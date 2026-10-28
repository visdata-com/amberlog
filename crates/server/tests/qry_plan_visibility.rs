//! REQ-AMB-QRY-PLAN-VISIBILITY 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查询。
//! flush 时点只靠 `flush.interval` 与 `flush.max_records`；
//! 「对象存储不可达」用 MinIO 台架加本进程里的 TCP 转发（见 `common::s3`）。

mod common;
mod qry_common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::s3::S3Instance;
use common::{BEARER, Fixture, Server};
use qry_common::{ALL, Chain, HOUR, MINUTE, column, eventually, hits, query};
use serde_json::{Value, json};

fn start_with(overrides: &[common::Kv]) -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(overrides);
    (f, s)
}

/// `SELECT seq FROM <stream>`（`size` 为 10000）返回的 `seq`，按 hit 顺序。
fn seqs(s: &Server, stream: &str) -> Vec<i64> {
    let r = s.search(&query(
        &format!("SELECT seq FROM {stream}"),
        ALL,
        &json!({"size": 10000}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    column(&r.json(), "seq")
        .iter()
        .map(|v| v.as_i64().expect("seq 是整数"))
        .collect()
}

// ---------------------------------------------------------------------------
// visibility
// ---------------------------------------------------------------------------

#[test]
fn visibility_within_10s() {
    let (_f, s) = start_with(&[]);
    s.ingest_ok("fresh", &json!([{"k": "fresh"}]));
    let acked = Instant::now();
    eventually(Duration::from_secs(10), "记录可查", || {
        let v = s.sql_ok("SELECT k FROM fresh WHERE k = 'fresh'");
        (hits(&v).len() == 1).then_some(())
    });
    assert!(acked.elapsed() < Duration::from_secs(10));
}

#[test]
fn visibility_memtable_and_files() {
    let (f, s) = start_with(&[
        ("flush.interval", Some("\"1h\"")),
        ("flush.max_records", Some("3")),
    ]);
    s.ingest_ok("mf", &json!([{"s": 1}, {"s": 2}, {"s": 3}]));
    Chain::file(&f).wait_record_count("mf", 3);
    s.ingest_ok("mf", &json!([{"s": 4}, {"s": 5}]));
    let r = s.search(&query("SELECT s FROM mf", ALL, &json!({"size": 100})));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    let got: BTreeSet<i64> = column(&v, "s")
        .iter()
        .map(|x| x.as_i64().unwrap())
        .collect();
    assert_eq!(got, (1..=5).collect());
    assert_eq!(hits(&v).len(), 5);
    assert_eq!(v["scan_files"], 1);
}

/// 摄取期间持续查询：每次查询不重复；摄取完成后 10 秒内恰返回全部 `seq`。
#[test]
fn visibility_flush_boundary_exactly_once() {
    let (_f, s) = start_with(&[("flush.max_records", Some("100"))]);
    let s = Arc::new(s);
    s.ingest_ok("fb", &json!([{"seq": -1}]));
    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (s, done) = (Arc::clone(&s), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut rounds = 0;
            while !done.load(Ordering::SeqCst) {
                let got = seqs(&s, "fb");
                let distinct: BTreeSet<i64> = got.iter().copied().collect();
                assert_eq!(distinct.len(), got.len(), "查询结果有重复的 seq");
                rounds += 1;
            }
            rounds
        })
    };
    for chunk in 0..100 {
        let batch: Vec<Value> = (0..10).map(|i| json!({"seq": chunk * 10 + i})).collect();
        s.ingest_ok("fb", &Value::Array(batch));
    }
    done.store(true, Ordering::SeqCst);
    let rounds = reader.join().expect("查询线程未失败");
    assert!(rounds > 0);
    let want: BTreeSet<i64> = (-1..1000).collect();
    eventually(Duration::from_secs(10), "全部 seq 可查", || {
        let got = seqs(&s, "fb");
        let distinct: BTreeSet<i64> = got.iter().copied().collect();
        assert_eq!(distinct.len(), got.len(), "查询结果有重复的 seq");
        (distinct == want).then_some(())
    });
}

/// 30 秒内持续摄取并持续查询：每次查询的 `seq` 集合包含之前任一次的集合。
#[test]
fn visibility_no_regression_during_flush() {
    let (_f, s) = start_with(&[
        ("flush.max_records", Some("50")),
        ("flush.interval", Some("\"1s\"")),
    ]);
    let s = Arc::new(s);
    s.ingest_ok("nr", &json!([{"seq": -1}]));
    let done = Arc::new(AtomicBool::new(false));
    let reader = {
        let (s, done) = (Arc::clone(&s), Arc::clone(&done));
        std::thread::spawn(move || {
            let mut seen: BTreeSet<i64> = BTreeSet::new();
            let mut rounds = 0;
            while !done.load(Ordering::SeqCst) {
                let got: BTreeSet<i64> = seqs(&s, "nr").into_iter().collect();
                let missing: Vec<_> = seen.difference(&got).take(5).collect();
                assert!(missing.is_empty(), "此前已可见的 seq 缺失：{missing:?}");
                seen = got;
                rounds += 1;
            }
            rounds
        })
    };
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut next = 0;
    while Instant::now() < deadline {
        let batch: Vec<Value> = (next..next + 7).map(|seq| json!({"seq": seq})).collect();
        s.ingest_ok("nr", &Value::Array(batch));
        next += 7;
        std::thread::sleep(Duration::from_millis(20));
    }
    done.store(true, Ordering::SeqCst);
    let rounds = reader.join().expect("查询线程未失败");
    assert!(rounds > 1, "{rounds}");
}

#[test]
fn visibility_after_object_store_recovery() {
    let inst = S3Instance::new();
    let s = inst.start(&[("flush.max_records", Some("1"))]);
    s.ingest_ok("app", &json!([{"k": "early"}]));
    Chain::s3(&inst).wait_record_count("app", 1);
    inst.relay.stop();
    s.ingest_ok("app", &json!([{"k": "late"}]));
    // 等到提交确实失败过（数据留在内存可见集、待恢复后重提）再恢复，场景要防的那条路径才被走到
    eventually(Duration::from_secs(60), "提交失败计数增加", || {
        (commits_failed(&s, "app") > 0.0).then_some(())
    });
    inst.relay.resume();
    let v = eventually(Duration::from_secs(60), "查询以 200 作答", || {
        let r = s.sql("SELECT k FROM app WHERE k = 'late'");
        (r.status == 200).then(|| r.json())
    });
    assert_eq!(hits(&v), [json!({"k": "late"})]);
}

/// `/metrics` 中 `stream` 的 `amberlog_manifest_commits_total{result="failed"}`；没有该样本行为 0。
fn commits_failed(s: &Server, stream: &str) -> f64 {
    let text = s
        .request("GET", "/metrics", &[("Authorization", BEARER)], b"")
        .text();
    let series = format!(
        "amberlog_manifest_commits_total{{org=\"default\",stream=\"{stream}\",result=\"failed\"}} "
    );
    text.lines()
        .find_map(|l| l.strip_prefix(&series))
        .map_or(0.0, |v| v.trim().parse().unwrap())
}

#[test]
fn visibility_lake_boundary_exactly_once() {
    let (_f, s) = start_with(&[("lifecycle.lake_watermark", Some("\"1h\""))]);
    let h = common::api::now_us() / HOUR * HOUR - 3 * HOUR;
    for chunk in 0..10i64 {
        let records: Vec<Value> = (chunk * 100..chunk * 100 + 100)
            .map(|i| json!({"_timestamp": h + 1 + i * 1_000_000, "seq": i}))
            .collect();
        s.ingest_ok("app", &Value::Array(records));
    }
    let acked = Instant::now();
    let tier_cold = || {
        let r = s.search(&query(
            "SELECT tier FROM _ledger WHERE dataset = 'default/app'",
            ALL,
            &json!({}),
        ));
        (r.status == 200).then(|| r.json()).filter(|v| {
            let hs = hits(v);
            hs.len() == 1 && hs[0]["tier"] == "cold"
        })
    };
    let check = |label: &str| -> u16 {
        let r = s.search(&query(
            "SELECT seq FROM app",
            (h, h + HOUR),
            &json!({"size": 10000}),
        ));
        if r.status == 200 {
            let seqs: BTreeSet<i64> = column(&r.json(), "seq")
                .iter()
                .map(|v| v.as_i64().unwrap())
                .collect();
            assert_eq!(hits(&r.json()).len(), 1000, "{label}");
            assert_eq!(seqs, (0..1000).collect::<BTreeSet<_>>(), "{label}");
        }
        r.status
    };
    loop {
        check("入湖之前或期间");
        if tier_cold().is_some() {
            break;
        }
        assert!(
            acked.elapsed() < Duration::from_secs(180),
            "180 秒内台账未显示 cold"
        );
        std::thread::sleep(Duration::from_secs(1));
    }
    for i in 0..10 {
        assert_eq!(check(&format!("入湖之后第 {i} 次")), 200);
        std::thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn visibility_no_flush_endpoint() {
    let (_f, s) = start_with(&[]);
    s.ingest_ok("fl", &json!([{"a": 1}]));
    for path in [
        "/api/default/fl/_flush",
        "/api/default/_flush",
        "/api/_flush",
    ] {
        s.request("POST", path, &[("Authorization", BEARER)], b"{}")
            .assert_envelope(404, "not_found");
    }
}

// ---------------------------------------------------------------------------
// pruning
// ---------------------------------------------------------------------------

/// 此刻前第 3 个 UTC 整点：它与其后一小时都在此刻前 24 小时之内，默认入湖水位 `72h` 下两个桶都不越过入湖水位时刻，
/// 数据都在热层（REQ-AMB-QRY-PLAN-VISIBILITY-V3 替代说明 ⑥；固定的早年时刻在入湖启用后会入湖）。
fn recent_hour() -> i64 {
    common::api::now_us() / HOUR * HOUR - 3 * HOUR
}

#[test]
fn pruning_scan_files() {
    let (f, s) = start_with(&[("flush.max_records", Some("1"))]);
    let chain = Chain::file(&f);
    let h1 = recent_hour();
    let hour1 = h1 + MINUTE;
    let hour2 = h1 + HOUR + MINUTE;
    s.ingest_ok("app", &json!([{"_timestamp": hour1}]));
    chain.wait_record_count("app", 1);
    s.ingest_ok("app", &json!([{"_timestamp": hour2}]));
    let snapshot = chain.wait_record_count("app", 2);
    assert_eq!(chain.files("app", &snapshot).len(), 2);
    let r = s.search(&query(
        "SELECT * FROM app",
        (h1 + HOUR, h1 + 2 * HOUR),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["scan_files"], 1);
}

#[test]
fn pruning_boundary_half_open() {
    let (f, s) = start_with(&[("flush.max_records", Some("2"))]);
    let h = recent_hour();
    let (a, b) = (h + 10 * MINUTE, h + 20 * MINUTE);
    s.ingest_ok("pb", &json!([{"_timestamp": a}, {"_timestamp": b}]));
    let chain = Chain::file(&f);
    let snapshot = chain.wait_record_count("pb", 2);
    let files = chain.files("pb", &snapshot);
    assert_eq!(files.len(), 1);
    assert_eq!(
        (
            files[0]["min_timestamp"].clone(),
            files[0]["max_timestamp"].clone()
        ),
        (json!(a), json!(b))
    );
    for (range, want) in [
        ((b, b + 1), 1),
        ((b + 1, b + 100), 0),
        ((a - 100, a), 0),
        ((a - 100, a + 1), 1),
    ] {
        let r = s.search(&query("SELECT * FROM pb", range, &json!({})));
        assert_eq!(r.status, 200, "{}", r.text());
        assert_eq!(r.json()["scan_files"], want, "{range:?}");
    }
}

#[test]
fn pruning_file_level_within_bucket() {
    let (f, s) = start_with(&[
        ("flush.interval", Some("\"1h\"")),
        ("flush.max_records", Some("2")),
    ]);
    let chain = Chain::file(&f);
    let h = recent_hour();
    s.ingest_ok(
        "pf",
        &json!([{"_timestamp": h + MINUTE}, {"_timestamp": h + 2 * MINUTE}]),
    );
    chain.wait_record_count("pf", 2);
    s.ingest_ok(
        "pf",
        &json!([{"_timestamp": h + 31 * MINUTE}, {"_timestamp": h + 32 * MINUTE}]),
    );
    let snapshot = chain.wait_record_count("pf", 4);
    assert_eq!(chain.files("pf", &snapshot).len(), 2);
    let r = s.search(&query(
        "SELECT * FROM pf",
        (h + 20 * MINUTE, h + 40 * MINUTE),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["scan_files"], 1);
    assert_eq!(hits(&v).len(), 2);
}
