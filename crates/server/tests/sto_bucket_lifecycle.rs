//! REQ-AMB-STO-BUCKET-LIFECYCLE-V2 的系统测试：以子进程运行
//! `amberlog`，对象存储用 `file://` 临时目录，测试直接读目录下的对象检查快照与桶 manifest。
//!
//! 「查询期间桶越过封口时刻不重不漏」不等真实整点：以 `fault-injection` 特性下的测试专用时钟偏移
//! `TESTBED_CLOCK_OFFSET` 把被测进程的时钟放到下一整点前 25 秒。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use common::api::{ingest_ok, now_us, poll};
use common::store::{FLUSH_1S, HOUR, bucket_label, hot_buckets, hour_of, metric, stop};
use common::{BEARER, Fixture, Kv, Server};
use serde_json::json;

const GRACE_1S: Kv = ("lifecycle.seal_grace", Some("\"1s\""));
const LAKE_2H: Kv = ("lifecycle.lake_watermark", Some("\"2h\""));
const LAKE_1H: Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
/// 冷层写入失败：对 `<storage.prefix>/_iceberg/` 之下对象的写请求以 HTTP 403 作答（`TESTBED_FAULT_STORE`），使入湖
/// 不能完成、桶留在热层。
const COLD_WRITE_403: &str = "write _iceberg/ 403\n";

/// 此刻在某整点后的前 `after` 之内，或在下一整点前的 `before` 之内时，等到不在这两段之内：使「此刻前 1 小时」的桶
/// 已过封口时刻，且「此刻」的桶在随后 `before` 之内不越过整点。
fn away_from_hour_edges(after: Duration, before: Duration) {
    let now = now_us();
    let into = now - hour_of(now);
    let left = HOUR - into;
    let (a, b) = (after.as_micros() as i64, before.as_micros() as i64);
    let wait = if into < a {
        a - into
    } else if left < b {
        left + a
    } else {
        0
    };
    std::thread::sleep(Duration::from_micros(wait as u64));
}

/// 等到 `/metrics` 出现该数据集的 `amberlog_hot_buckets`（至多 10 秒），返回三个状态的值。
fn wait_hot_buckets(s: &Server, stream: &str, want: [f64; 3]) -> [Option<f64>; 3] {
    let read = || ["open", "sealed", "pending_lake"].map(|state| hot_buckets(s, stream, state));
    let (got, _) = poll(Duration::from_secs(10), read, |v| {
        v.iter().zip(want).all(|(g, w)| *g == Some(w))
    });
    got
}

/// 读一次 `/metrics`，返回该数据集 `amberlog_hot_buckets` 三个状态（open、sealed、pending_lake）的值。
fn hot_buckets_once(s: &Server, stream: &str) -> [Option<f64>; 3] {
    let r = s.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    let text = r.text();
    ["open", "sealed", "pending_lake"].map(|state| {
        let key = format!(
            "amberlog_hot_buckets{{org=\"default\",stream=\"{stream}\",state=\"{state}\"}} "
        );
        text.lines()
            .find_map(|l| l.strip_prefix(&key))
            .map(|v| v.trim().parse().expect("样本值是数"))
    })
}

fn record_at(ts: i64, extra: &str) -> String {
    format!(r#"{{"_timestamp":{ts}{extra}}}"#)
}

#[test]
fn lifecycle_bucket_states() {
    let f = Fixture::new();
    let (s, faults) = f.start_with_store_faults(&[GRACE_1S, LAKE_2H, FLUSH_1S]);
    faults.set(COLD_WRITE_403);
    // 轮询至多 10 秒：整点前 15 秒之内开始的话，「此刻」的桶会在轮询期间越过整点
    away_from_hour_edges(Duration::from_secs(2), Duration::from_secs(15));
    let now = now_us();
    let body = format!(
        "[{},{},{}]",
        record_at(now, ""),
        record_at(now - HOUR, ""),
        record_at(now - 4 * HOUR, "")
    );
    ingest_ok(&s, "app", &body);
    assert_eq!(
        wait_hot_buckets(&s, "app", [1.0, 1.0, 1.0]),
        [Some(1.0), Some(1.0), Some(1.0)]
    );
}

#[test]
fn lifecycle_state_recomputed_after_restart() {
    let f = Fixture::new();
    let (mut s, faults) = f.start_with_store_faults(&[GRACE_1S, LAKE_2H, FLUSH_1S]);
    faults.set(COLD_WRITE_403);
    ingest_ok(
        &s,
        "app",
        &format!("[{}]", record_at(now_us() - 4 * HOUR, "")),
    );
    assert_eq!(
        wait_hot_buckets(&s, "app", [0.0, 0.0, 1.0]),
        [Some(0.0), Some(0.0), Some(1.0)]
    );
    let ds = f.dataset("app");
    let before: BTreeMap<String, Vec<u8>> = ds
        .keys()
        .into_iter()
        .map(|k| (k.clone(), ds.read(&k)))
        .collect();
    stop(&mut s);

    let (s, faults) = f.start_with_store_faults(&[
        GRACE_1S,
        ("lifecycle.lake_watermark", Some("\"10h\"")),
        FLUSH_1S,
    ]);
    faults.set(COLD_WRITE_403);
    // 指标一出现就检查取值，不轮询到取值等于期望
    let (got, seen) = poll(
        Duration::from_secs(10),
        || hot_buckets_once(&s, "app"),
        |v| v.iter().any(Option::is_some),
    );
    assert!(seen, "10 秒内没有出现该数据集的 amberlog_hot_buckets");
    assert_eq!(got[1], Some(1.0));
    assert_eq!(got[2], Some(0.0));
    for (k, bytes) in &before {
        assert!(ds.exists(k), "{k} 不见了");
        assert_eq!(&ds.read(k), bytes, "{k} 被改写");
    }
}

#[test]
fn lifecycle_disabled_dataset_no_gauge() {
    let f = Fixture::new();
    let mut s = f.start(&[GRACE_1S, FLUSH_1S]);
    let ts = now_us() - 2 * HOUR;
    for stream in ["app", "ok"] {
        ingest_ok(&s, stream, &format!("[{}]", record_at(ts, "")));
        f.dataset(stream).wait_records(1, Duration::from_secs(15));
    }
    stop(&mut s);
    let app = f.dataset("app");
    let head = app.head();
    let manifest = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == bucket_label(ts))
        .unwrap()["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    app.remove(&manifest);

    let s = f.start(&[GRACE_1S, FLUSH_1S]);
    let body = json!({"query": {
        "sql": "SELECT * FROM app",
        "start_time": hour_of(ts),
        "end_time": hour_of(ts) + HOUR,
    }})
    .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    let text = s.get_auth("/metrics").text();
    assert!(
        !text
            .lines()
            .any(|l| l.starts_with("amberlog_hot_buckets{") && l.contains("stream=\"app\"")),
        "{text}"
    );
    assert_eq!(hot_buckets(&s, "ok", "sealed"), Some(1.0), "{text}");
}

#[test]
fn lifecycle_late_records_routed() {
    let f = Fixture::new();
    let s = f.start(&[LAKE_2H, FLUSH_1S]);
    let now = now_us();
    let (a, b) = (now - 90 * 60_000_000, now - 5 * HOUR);
    let r = ingest_ok(
        &s,
        "app",
        &format!("[{},{}]", record_at(a, ""), record_at(b, "")),
    );
    assert_eq!(r["status"][0]["successful"], 2);
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": b, "end_time": a + 1}})
        .to_string();
    let (rows, ok) = poll(
        Duration::from_secs(10),
        || {
            let r = s.request(
                "POST",
                "/api/default/_search",
                &[("Authorization", BEARER)],
                body.as_bytes(),
            );
            assert_eq!(r.status, 200, "{}", r.text());
            r.json()["hits"].as_array().unwrap().len()
        },
        |n| *n == 2,
    );
    assert!(ok, "查询返回 {rows} 行");
    for (route, want) in [("sealed", 1.0), ("pending_lake", 1.0)] {
        assert_eq!(
            metric(
                &s,
                &format!(
                    "amberlog_late_records_total{{org=\"default\",stream=\"app\",route=\"{route}\"}}"
                )
            ),
            Some(want),
            "{route}"
        );
    }
}

#[test]
fn lifecycle_late_record_new_data_file() {
    let f = Fixture::new();
    let s = f.start(&[GRACE_1S, FLUSH_1S]);
    let ds = f.dataset("app");
    let ts = hour_of(now_us()) - 2 * HOUR + 60_000_000;
    let label = bucket_label(ts);
    ingest_ok(&s, "app", &format!("[{}]", record_at(ts, "")));
    let head = ds.wait_records(1, Duration::from_secs(15));
    let before: BTreeMap<String, Vec<u8>> = ds.bucket_manifest(&head, &label).unwrap()["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            let p = e["path"].as_str().unwrap().to_owned();
            let bytes = ds.read(&p);
            (p, bytes)
        })
        .collect();

    ingest_ok(&s, "app", &format!("[{}]", record_at(ts + 1, "")));
    std::thread::sleep(Duration::from_secs(5));
    let m = ds.bucket_manifest(&ds.head(), &label).unwrap();
    let paths: BTreeSet<&str> = m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap())
        .collect();
    assert!(
        paths.iter().any(|p| !before.contains_key(*p)),
        "没有新数据文件：{m}"
    );
    for (p, bytes) in &before {
        assert_eq!(&ds.read(p), bytes, "{p} 被改写");
    }
}

/// 以覆盖小时桶 `start` 的时间范围查询 `SELECT * FROM app`（`size` 为 1000），返回 (状态码, 行数)。
fn query_bucket(s: &Server, start: i64) -> (u16, usize) {
    let q = json!({"query": {
        "sql": "SELECT * FROM app",
        "start_time": start,
        "end_time": start + HOUR,
        "size": 1000,
    }})
    .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        q.as_bytes(),
    );
    let n = if r.status == 200 {
        r.json()["hits"].as_array().unwrap().len()
    } else {
        0
    };
    (r.status, n)
}

/// 当前快照的 `buckets` 中桶 `label` 不含存活数据文件（不存在该桶的元素，或其 `record_count` 为 0）。
fn no_live_files(f: &Fixture, label: &str) -> bool {
    f.dataset("app").head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label)
        .is_none_or(|b| b["record_count"] == 0)
}

/// `amberlog_late_records_total{org="default",stream="app",route="pending_lake"}`，没有该样本记为 0。
fn late_pending_lake(s: &Server) -> f64 {
    metric(
        s,
        "amberlog_late_records_total{org=\"default\",stream=\"app\",route=\"pending_lake\"}",
    )
    .unwrap_or(0.0)
}

#[test]
fn lifecycle_lake_entry_leaves_hot_query_complete() {
    let f = Fixture::new();
    let s = f.start(&[LAKE_1H, FLUSH_1S]);
    let ds = f.dataset("app");
    let start = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(start);
    let body: Vec<String> = (0..100)
        .map(|i| record_at(start + i * 1_000_000, &format!(r#","i":{i}"#)))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", body.join(",")));
    ds.wait_records(100, Duration::from_secs(15));
    // 时限 120 秒（REQ-AMB-STO-LAKE-ENTRY-DEADLINE）：轮询到该桶离开热层即止，至多 130 秒
    let (_, ok) = poll(
        Duration::from_secs(130),
        || no_live_files(&f, &label),
        |v| *v,
    );
    assert!(ok, "130 秒内该桶未离开热层：{}", ds.head());
    assert_eq!(hot_buckets(&s, "app", "pending_lake"), Some(0.0));
    assert_eq!(query_bucket(&s, start), (200, 100));

    let root = f.store_dir().join("amberlog");
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir).unwrap().flatten() {
            if e.file_type().unwrap().is_dir() {
                stack.push(e.path());
                continue;
            }
            let rel = e
                .path()
                .strip_prefix(&root)
                .unwrap()
                .to_string_lossy()
                .into_owned();
            assert!(
                rel.starts_with("default/app/")
                    || rel.starts_with("_iceberg/")
                    || rel.starts_with("_system/"),
                "根前缀下出现了 {rel}"
            );
        }
    }
    assert_eq!(s.get("/readyz").status, 200);
}

#[test]
fn lifecycle_lake_failure_keeps_hot() {
    let f = Fixture::new();
    let (s, faults) = f.start_with_store_faults(&[LAKE_1H, FLUSH_1S]);
    faults.set(COLD_WRITE_403);
    let ds = f.dataset("app");
    let start = hour_of(now_us()) - 3 * HOUR;
    let body: Vec<String> = (0..100)
        .map(|i| record_at(start + i * 1_000_000, &format!(r#","i":{i}"#)))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", body.join(",")));
    ds.wait_records(100, Duration::from_secs(15));
    std::thread::sleep(Duration::from_secs(120));

    let r = ingest_ok(
        &s,
        "app",
        &format!("[{}]", record_at(start + 500_000_000, "")),
    );
    assert_eq!(r["status"][0]["successful"], 1, "{r}");
    let (got, _) = poll(
        Duration::from_secs(10),
        || query_bucket(&s, start),
        |v| v.1 == 101,
    );
    assert_eq!(got, (200, 101));
    assert_eq!(hot_buckets(&s, "app", "pending_lake"), Some(1.0));
    assert_eq!(s.get("/readyz").status, 200);
}

#[test]
fn lifecycle_late_record_after_lake() {
    let f = Fixture::new();
    let s = f.start(&[LAKE_1H, FLUSH_1S]);
    let start = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(start);
    let body: Vec<String> = (0..10)
        .map(|i| record_at(start + i * 1_000_000, ""))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", body.join(",")));
    let (_, ok) = poll(
        Duration::from_secs(132),
        || no_live_files(&f, &label),
        |v| *v,
    );
    assert!(ok, "该桶未入湖：{}", f.dataset("app").head());

    let c = late_pending_lake(&s);
    let r = ingest_ok(
        &s,
        "app",
        &format!("[{}]", record_at(start + 500_000_000, "")),
    );
    assert_eq!(r["status"][0]["successful"], 1, "{r}");
    let (got, _) = poll(
        Duration::from_secs(10),
        || query_bucket(&s, start),
        |v| v.1 == 11,
    );
    assert_eq!(got, (200, 11));
    assert_eq!(late_pending_lake(&s), c + 1.0);
}

/// 时钟偏移只在 `fault-injection` 下编入；不带该特性时进程不会越过整点，本测试不编译。
#[cfg(feature = "fault-injection")]
#[test]
fn lifecycle_state_change_exactly_once() {
    // 把被测进程的时钟放到下一整点前 25 秒
    let real = now_us();
    let target = hour_of(real) + HOUR - 25_000_000;
    let offset_s = (target - real).div_euclid(1_000_000);
    let f = Fixture::new();
    let mut cmd = f.serve(&[GRACE_1S, FLUSH_1S]);
    cmd.env("TESTBED_CLOCK_OFFSET", offset_s.to_string());
    let s = Server::start(cmd);

    let acked: std::sync::Mutex<Vec<u64>> = std::sync::Mutex::default();
    let done = std::sync::atomic::AtomicBool::new(false);
    /// 摄取线程退出时（含断言失败）置位结束标志，查询线程不会一直空转。
    struct SetOnDrop<'a>(&'a std::sync::atomic::AtomicBool);
    impl Drop for SetOnDrop<'_> {
        fn drop(&mut self) {
            self.0.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let _done = SetOnDrop(&done);
            let deadline = std::time::Instant::now() + Duration::from_secs(45);
            let mut seq = 0u64;
            while std::time::Instant::now() < deadline {
                let r = ingest_ok(&s, "app", &format!(r#"[{{"seq":{seq}}}]"#));
                assert_eq!(r["status"][0]["successful"], 1);
                acked.lock().unwrap().push(seq);
                seq += 1;
                std::thread::sleep(Duration::from_millis(50));
            }
        });
        scope.spawn(|| {
            while !done.load(std::sync::atomic::Ordering::SeqCst) {
                let hits = seqs(&s);
                let unique: BTreeSet<u64> = hits.iter().copied().collect();
                assert_eq!(unique.len(), hits.len(), "查询结果有重复的 seq");
                std::thread::sleep(Duration::from_millis(100));
            }
        });
    });
    std::thread::sleep(Duration::from_secs(10));
    let mut got = seqs(&s);
    got.sort();
    let mut want = acked.into_inner().unwrap();
    want.sort();
    assert_eq!(got, want);
    // 确实越过了整点：已提交的记录分在两个小时桶里，且前一小时的桶封口后发生过合并
    let head = f.dataset("app").head();
    let labels: BTreeSet<&str> = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|b| b["bucket"].as_str().unwrap())
        .collect();
    assert_eq!(labels.len(), 2, "没有越过整点：{labels:?}");
    let compacted = metric(
        &s,
        "amberlog_compactions_total{org=\"default\",stream=\"app\",result=\"ok\"}",
    )
    .unwrap_or(0.0);
    assert!(compacted >= 1.0, "越过整点后没有发生合并");
}

#[cfg(feature = "fault-injection")]
fn seqs(s: &Server) -> Vec<u64> {
    let r = common::api::search_resp(s, "SELECT seq FROM app");
    if r.status != 200 {
        // 数据集或列尚不存在（第一条记录还没受理完）
        let reason = r.json()["reason"].clone();
        assert!(
            reason == "stream_not_found" || reason == "unknown_column",
            "{}",
            r.text()
        );
        return Vec::new();
    }
    r.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h: &serde_json::Value| h["seq"].as_u64().unwrap())
        .collect()
}
