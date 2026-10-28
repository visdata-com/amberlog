//! REQ-AMB-STO-SEAL-COMPACTION-V4 的系统测试：以子进程运行
//! `amberlog`，对象存储用 `file://` 临时目录，测试直接读写目录下的对象来检查桶 manifest 或替换数据文件。
//!
//! 「开放桶在先、封口在后」的场景以 `lifecycle.seal_grace` 为 `24h` 写出小文件（[`seed_small_files`]），再以 `1s`
//! 重启触发合并。

mod common;

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use common::api::{ingest_ok, now_us, poll};
use common::store::{
    Ds, FLUSH_1S, HOUR, bucket_label, has_mergeable_run, hour_of, metric,
    rewrite_parquet_uncompressed, seed_small_files, stop, three_ingests,
};
use common::{BEARER, Fixture, Kv, Server};
use serde_json::{Value, json};

const GRACE_1S: Kv = ("lifecycle.seal_grace", Some("\"1s\""));

/// 以覆盖 `[start, start + 1h)` 的时间范围执行 `sql`（`size` 为 10000），断言 200，返回 `hits`。
fn query_hour(s: &Server, sql: &str, start: i64) -> Vec<Value> {
    let body = json!({"query": {"sql": sql, "start_time": start, "end_time": start + HOUR, "size": 10000}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["hits"].as_array().unwrap().clone()
}

/// 当前快照中小时桶 `label` 的桶 manifest。
fn head_manifest(ds: &Ds, label: &str) -> Value {
    ds.bucket_manifest(&ds.head(), label)
        .unwrap_or_else(|| panic!("快照中没有桶 {label}"))
}

fn paths(entries: &Value) -> BTreeSet<String> {
    entries
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect()
}

fn compactions(s: &Server, result: &str) -> Option<f64> {
    metric(
        s,
        &format!(
            "amberlog_compactions_total{{org=\"default\",stream=\"app\",result=\"{result}\"}}"
        ),
    )
}

/// 此刻前第 2 小时的桶起点。
fn two_hours_ago() -> i64 {
    hour_of(now_us()) - 2 * HOUR
}

/// 桶 manifest `m` 中各数据文件的 `size_bytes`。
fn file_sizes(m: &Value) -> Vec<u64> {
    m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["size_bytes"].as_u64().unwrap())
        .collect()
}

fn sorted_rows(mut rows: Vec<Value>) -> Vec<String> {
    let mut out: Vec<String> = rows.drain(..).map(|r| r.to_string()).collect();
    out.sort();
    out
}

#[test]
fn compaction_sealed_bucket_merged() {
    let f = Fixture::new();
    let s = f.start(&[GRACE_1S, FLUSH_1S]);
    let ds = f.dataset("app");
    let start = two_hours_ago();
    let label = bucket_label(start);
    let mut originals = BTreeSet::new();
    for i in 0..3 {
        let records: Vec<String> = (0..10)
            .map(|j| {
                format!(
                    r#"{{"_timestamp":{},"i":{i},"j":{j}}}"#,
                    start + (i * 10 + j) * 1000
                )
            })
            .collect();
        ingest_ok(&s, "app", &format!("[{}]", records.join(",")));
        ds.wait_records((i as u64 + 1) * 10, Duration::from_secs(15));
        // 本次摄取产生的数据文件：含 10 条记录的条目
        for e in head_manifest(&ds, &label)["files"].as_array().unwrap() {
            if e["record_count"] == 10 {
                originals.insert(e["path"].as_str().unwrap().to_owned());
            }
        }
        std::thread::sleep(Duration::from_millis(1500));
    }
    assert_eq!(originals.len(), 3, "{originals:?}");
    let before = sorted_rows(query_hour(&s, "SELECT * FROM app", start));
    assert_eq!(before.len(), 30);
    // `before` 须真在合并之前取得：取得之后链头的该桶仍是这 3 个文件
    assert_eq!(
        paths(&head_manifest(&ds, &label)["files"]),
        originals,
        "取「合并之前」的查询结果时合并已经发生"
    );

    std::thread::sleep(Duration::from_secs(60));
    let m = head_manifest(&ds, &label);
    let files = m["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{m}");
    assert_eq!(files[0]["record_count"], 30);
    let compacted: BTreeSet<String> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    for p in &originals {
        assert!(compacted.contains(p), "{p} 不在 compaction 留痕中：{m}");
        assert!(ds.exists(p), "{p} 被删除");
    }
    assert_eq!(
        sorted_rows(query_hour(&s, "SELECT * FROM app", start)),
        before
    );
}

#[test]
fn compaction_open_bucket_untouched() {
    // 此刻距下一整点不足 5 分钟时等到下一整点之后
    let left = hour_of(now_us()) + HOUR - now_us();
    if left < 5 * 60_000_000 {
        std::thread::sleep(Duration::from_micros(left as u64 + 1_000_000));
    }
    let f = Fixture::new();
    let s = f.start(&[("lifecycle.seal_grace", Some("\"10m\"")), FLUSH_1S]);
    let ds = f.dataset("app");
    let label = bucket_label(now_us());
    for i in 0..3u64 {
        ingest_ok(
            &s,
            "app",
            r#"[{"a":1},{"a":2},{"a":3},{"a":4},{"a":5},{"a":6},{"a":7},{"a":8},{"a":9},{"a":10}]"#,
        );
        ds.wait_records((i + 1) * 10, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
    // 至少经过一个合并检查周期
    std::thread::sleep(Duration::from_secs(12));
    assert_eq!(bucket_label(now_us()), label, "测试跨过了整点");
    let m = head_manifest(&ds, &label);
    assert_eq!(m["files"].as_array().unwrap().len(), 3, "{m}");
    assert_eq!(m["deletions"], json!([]));
}

#[test]
fn compaction_oversized_converges() {
    let f = Fixture::new();
    let start = two_hours_ago();
    let label = bucket_label(start);
    seed_small_files(&f, "app", start, 10, 20);
    let ds = f.dataset("app");
    let sizes = file_sizes(&head_manifest(&ds, &label));
    assert_eq!(sizes.len(), 10);
    let (min, max) = (sizes.iter().min().unwrap(), sizes.iter().max().unwrap());
    assert!(max * 10 <= min * 11, "{sizes:?}");
    let total: u64 = sizes.iter().sum();
    let target = total.div_ceil(3);
    let target_s = target.to_string();

    let s = f.start(&[
        GRACE_1S,
        FLUSH_1S,
        ("compaction.target_file_bytes", Some(target_s.as_str())),
    ]);
    let (sizes, ok) = poll(
        Duration::from_secs(600),
        || file_sizes(&head_manifest(&ds, &label)),
        |sizes| !has_mergeable_run(sizes, target),
    );
    assert!(
        ok,
        "600 秒后仍有相邻可合并组：{sizes:?}（目标大小 {target}）"
    );
    let hits = query_hour(&s, "SELECT seq FROM app", start);
    let seqs: BTreeSet<u64> = hits.iter().map(|h| h["seq"].as_u64().unwrap()).collect();
    assert_eq!(hits.len(), 200, "合并输出有重复或缺失的行");
    assert_eq!(seqs.len(), 200);
    let ok = compactions(&s, "ok").unwrap_or(0.0);
    assert!(ok >= 3.0, "合并只做了 {ok} 次：{sizes:?}");
}

const GRACE_24H: Kv = ("lifecycle.seal_grace", Some("\"24h\""));

/// 200 个取自 94 个可打印 ASCII 字符的随机字符（几乎不可压缩，每个约 6.55 比特），`state` 为 xorshift 的状态。
fn random_pad(state: &mut u64) -> String {
    (0..200)
        .map(|_| {
            *state ^= *state << 13;
            *state ^= *state >> 7;
            *state ^= *state << 17;
            (b'!' + (*state % 94) as u8) as char
        })
        .collect()
}

/// 以一次请求摄取 `n` 条 `_timestamp` 自 `from` 起每条加 1 毫秒、带 [`random_pad`] 字段 `pad` 的记录。
fn ingest_padded(s: &Server, from: i64, n: i64, state: &mut u64) {
    let rows: Vec<Value> = (0..n)
        .map(|i| json!({"_timestamp": from + i * 1000, "pad": random_pad(state)}))
        .collect();
    ingest_ok(s, "app", &Value::Array(rows).to_string());
}

/// 当前快照中桶 `label` 的 `files` 里 `known` 之外的条目路径。
fn new_files(ds: &Ds, label: &str, known: &BTreeSet<String>) -> Vec<String> {
    paths(&head_manifest(ds, label)["files"])
        .into_iter()
        .filter(|p| !known.contains(p))
        .collect()
}

/// 2000 条带 `pad` 的记录约 330 KB，三个各 1 条的文件各约 1 KB：F 比三者之和大 2 倍以上，只有 S1、S2、S3 合并。
#[test]
fn compaction_sparse_files_spare_larger_file() {
    let f = Fixture::new();
    let start = two_hours_ago();
    let label = bucket_label(start);
    let ds = f.dataset("app");
    let mut s = f.start(&[GRACE_24H, FLUSH_1S]);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    ingest_padded(&s, start, 2000, &mut state);
    ds.wait_records(2000, Duration::from_secs(15));
    let mut known = BTreeSet::new();
    let big = new_files(&ds, &label, &known);
    assert_eq!(big.len(), 1, "{big:?}");
    let big = big[0].clone();
    known.insert(big.clone());
    let mut sparse = Vec::new();
    for i in 0..3 {
        std::thread::sleep(Duration::from_millis(1500));
        let body = json!([{"_timestamp": start + (2000 + i) * 1000, "n": i}]).to_string();
        ingest_ok(&s, "app", &body);
        ds.wait_records(2001 + i as u64, Duration::from_secs(15));
        let added = new_files(&ds, &label, &known);
        assert_eq!(added.len(), 1, "{added:?}");
        known.insert(added[0].clone());
        sparse.push(added[0].clone());
    }
    stop(&mut s);

    let m = head_manifest(&ds, &label);
    let size = |p: &str| {
        m["files"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["path"] == p)
            .unwrap()["size_bytes"]
            .as_u64()
            .unwrap()
    };
    let (f_size, s_sum) = (size(&big), sparse.iter().map(|p| size(p)).sum::<u64>());
    assert!(f_size * 2 < 134_217_728, "F {f_size} 字节");
    assert!(
        f_size > 2 * s_sum,
        "F {f_size} 字节，S1～S3 合计 {s_sum} 字节"
    );

    let s = f.start(&[GRACE_1S, FLUSH_1S]);
    std::thread::sleep(Duration::from_secs(150));
    let m = head_manifest(&ds, &label);
    let files = m["files"].as_array().unwrap();
    assert_eq!(files.len(), 2, "{m}");
    assert!(files.iter().any(|e| e["path"] == big.as_str()), "{m}");
    assert!(files.iter().any(|e| e["record_count"] == 3), "{m}");
    let deletions = m["deletions"].as_array().unwrap();
    let compacted: BTreeSet<&str> = deletions
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .map(|d| d["path"].as_str().unwrap())
        .collect();
    for p in &sparse {
        assert!(
            compacted.contains(p.as_str()),
            "{p} 不在 compaction 留痕中：{m}"
        );
    }
    assert!(!deletions.iter().any(|d| d["path"] == big.as_str()), "{m}");
    assert_eq!(query_hour(&s, "SELECT * FROM app", start).len(), 2003);
}

/// 4000 条带 `pad` 的记录约 660 KB，在 1 MiB 目标大小的一半与目标大小之间：两个文件都不是小文件。
#[test]
fn compaction_half_target_files_untouched() {
    const TARGET: Kv = ("compaction.target_file_bytes", Some("1048576"));
    let f = Fixture::new();
    let start = two_hours_ago();
    let label = bucket_label(start);
    let ds = f.dataset("app");
    let mut s = f.start(&[GRACE_24H, FLUSH_1S, TARGET]);
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    ingest_padded(&s, start, 4000, &mut state);
    ds.wait_records(4000, Duration::from_secs(15));
    std::thread::sleep(Duration::from_millis(1500));
    ingest_padded(&s, start + 4000 * 1000, 4000, &mut state);
    ds.wait_records(8000, Duration::from_secs(15));
    stop(&mut s);

    let before = head_manifest(&ds, &label);
    let sizes = file_sizes(&before);
    assert_eq!(sizes.len(), 2, "{before}");
    for n in &sizes {
        assert!((524_288..1_048_576).contains(n), "{sizes:?}");
    }

    let s = f.start(&[GRACE_1S, FLUSH_1S, TARGET]);
    std::thread::sleep(Duration::from_secs(60));
    let m = head_manifest(&ds, &label);
    assert_eq!(paths(&m["files"]), paths(&before["files"]), "{m}");
    assert_eq!(m["deletions"], json!([]));
    assert_eq!(compactions(&s, "ok").unwrap_or(0.0), 0.0);
}

/// 三个已提交的小文件、实例已停；返回它们的路径（按 `files` 顺序）与桶起点。
fn three_small_files(f: &Fixture) -> (Vec<String>, i64) {
    let start = two_hours_ago();
    seed_small_files(f, "app", start, 3, 10);
    let m = head_manifest(&f.dataset("app"), &bucket_label(start));
    let files: Vec<String> = m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(files.len(), 3);
    (files, start)
}

fn digest_mismatch_events(s: &Server) -> Vec<Value> {
    s.json_lines()
        .into_iter()
        .filter(|v| v["event"] == "digest_mismatch")
        .collect()
}

#[test]
fn compaction_digest_mismatch_aborts() {
    let f = Fixture::new();
    let (files, start) = three_small_files(&f);
    let ds = f.dataset("app");
    let tampered = &files[1];
    rewrite_parquet_uncompressed(&ds.path(tampered));
    let before = paths(&head_manifest(&ds, &bucket_label(start))["files"]);

    let s = f.start(&[GRACE_1S]);
    std::thread::sleep(Duration::from_secs(60));
    let events = digest_mismatch_events(&s);
    assert_eq!(events.len(), 1, "{:#?}", s.stderr_lines());
    assert_eq!(events[0]["dataset"], "default/app");
    assert_eq!(events[0]["path"], tampered.as_str());
    let m = head_manifest(&ds, &bucket_label(start));
    assert_eq!(paths(&m["files"]), before);
    assert_eq!(m["deletions"], json!([]));
    query_hour(&s, "SELECT * FROM app", start);
    assert_eq!(compactions(&s, "aborted"), Some(1.0));
}

#[test]
fn compaction_missing_input_aborts() {
    let f = Fixture::new();
    let (files, start) = three_small_files(&f);
    let ds = f.dataset("app");
    ds.remove(&files[0]);
    let before = paths(&head_manifest(&ds, &bucket_label(start))["files"]);

    let s = f.start(&[GRACE_1S]);
    std::thread::sleep(Duration::from_secs(120));
    assert!(
        digest_mismatch_events(&s).is_empty(),
        "{:#?}",
        s.stderr_lines()
    );
    let m = head_manifest(&ds, &bucket_label(start));
    assert_eq!(paths(&m["files"]), before);
    assert_eq!(m["deletions"], json!([]));
    assert_eq!(compactions(&s, "aborted"), Some(1.0));
}

#[test]
fn compaction_concurrent_query_exactly_once() {
    let f = Fixture::new();
    let start = two_hours_ago();
    let label = bucket_label(start);
    seed_small_files(&f, "app", start, 50, 20);
    let ds = f.dataset("app");
    let s = f.start(&[GRACE_1S, FLUSH_1S]);
    let deadline = Instant::now() + Duration::from_secs(90);
    let mut queries = 0;
    loop {
        let hits = query_hour(&s, "SELECT seq FROM app", start);
        let seqs: BTreeSet<u64> = hits.iter().map(|h| h["seq"].as_u64().unwrap()).collect();
        assert_eq!(seqs.len(), hits.len(), "查询结果有重复的 seq");
        assert_eq!(seqs.len(), 1000);
        queries += 1;
        if head_manifest(&ds, &label)["files"]
            .as_array()
            .unwrap()
            .len()
            == 1
        {
            break;
        }
        assert!(Instant::now() < deadline, "90 秒内未合并完");
    }
    assert!(queries > 1, "合并在第一次查询之前就已完成");
    // 合并之后再查一次
    let hits = query_hour(&s, "SELECT seq FROM app", start);
    assert_eq!(hits.len(), 1000);
}

/// 回归：合并决定是否合并之前读桶 manifest 不使数据集停用（REQ-AMB-STO-HOT-FSCK「停用只由查询、提交与装载触发」）。
/// 已封口桶的桶 manifest 被删除后重启，后台的合并检查只跳过该桶；对该桶发查询仍按 manifest 不可读停用。
#[test]
fn compaction_check_read_does_not_disable() {
    let f = Fixture::new();
    let start = two_hours_ago();
    let label = bucket_label(start);
    let ds = f.dataset("app");
    let mut s = f.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", &format!(r#"[{{"_timestamp":{start},"a":1}}]"#));
    ds.wait_records(1, Duration::from_secs(15));
    common::store::stop(&mut s);
    let head = ds.head();
    let bucket = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label.as_str())
        .unwrap();
    ds.remove(bucket["manifest"].as_str().unwrap());

    let s = f.start(&[FLUSH_1S]);
    std::thread::sleep(Duration::from_secs(16));
    let disabled = |s: &Server| {
        s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app")
    };
    assert!(!disabled(&s), "{:#?}", s.stderr_lines());
    let r = s.get_auth("/metrics");
    assert!(
        !r.text().contains("amberlog_dataset_disabled{"),
        "{}",
        r.text()
    );

    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": start + HOUR}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 500, "{}", r.text());
    let (_, seen) = poll(Duration::from_secs(5), || disabled(&s), |d| *d);
    assert!(seen, "{:#?}", s.stderr_lines());
}

/// 冷层写入失败（`TESTBED_FAULT_STORE`）：对 `<storage.prefix>/_iceberg/` 之下对象的写请求以 HTTP 403 作答。
const COLD_WRITE_403: &str = "write _iceberg/ 403\n";

#[test]
fn compaction_pending_lake_bucket_untouched() {
    let f = Fixture::new();
    let (s, faults) = f.start_with_store_faults(&[
        GRACE_1S,
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        FLUSH_1S,
    ]);
    faults.set(COLD_WRITE_403);
    let ds = f.dataset("app");
    let start = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(start);
    three_ingests(&s, &ds, start);
    std::thread::sleep(Duration::from_secs(60));
    let m = head_manifest(&ds, &label);
    assert_eq!(m["files"].as_array().unwrap().len(), 3, "{m}");
    assert!(
        !m["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["reason"] == "compaction"),
        "{m}"
    );
}

/// 时钟偏移与可放行的暂停点只在 `fault-injection` 下编入。
#[cfg(feature = "fault-injection")]
#[test]
fn compaction_abandon_when_lake_interleaves() {
    // 把被测进程的时钟放到下一整点后 5 秒
    let real = now_us();
    let hour = hour_of(real) + HOUR;
    let offset_s = (hour + 5_000_000 - real).div_euclid(1_000_000);
    let f = Fixture::new();
    let gate = f.root().join("fault-gate");
    std::fs::write(&gate, "hold").unwrap();
    let mut cmd = f.serve(&[
        GRACE_1S,
        ("lifecycle.lake_watermark", Some("\"2m\"")),
        FLUSH_1S,
    ]);
    cmd.env("TESTBED_CLOCK_OFFSET", offset_s.to_string())
        .env("TESTBED_FAULT_GATE", "compaction.before_commit")
        .env("TESTBED_FAULT_GATE_FILE", &gate);
    let s = Server::start(cmd);
    let ds = f.dataset("app");
    let start = hour - HOUR;
    let label = bucket_label(start);
    three_ingests(&s, &ds, start);
    let inputs = paths(&head_manifest(&ds, &label)["files"]);
    assert_eq!(inputs.len(), 3);
    // 合并已停在暂停点
    let (_, held) = poll(
        Duration::from_secs(60),
        || std::fs::read_to_string(&gate).is_ok_and(|g| g.trim() == "held"),
        |h| *h,
    );
    assert!(held, "合并未走到暂停点：{:#?}", s.stderr_lines());
    // 等到（进程时钟）整点后 2 分钟，再等 130 秒
    let wait = (hour + 120_000_000) - (now_us() + offset_s * 1_000_000);
    if wait > 0 {
        std::thread::sleep(Duration::from_micros(wait as u64));
    }
    std::thread::sleep(Duration::from_secs(130));
    std::fs::write(&gate, "go").unwrap();
    std::thread::sleep(Duration::from_secs(130));

    let m = head_manifest(&ds, &label);
    let deletions = m["deletions"].as_array().unwrap();
    let lake: BTreeSet<String> = deletions
        .iter()
        .filter(|d| d["reason"] == "lake")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(lake, inputs, "{m}");
    assert!(
        !deletions.iter().any(|d| d["reason"] == "compaction"),
        "{m}"
    );
    assert_eq!(query_hour(&s, "SELECT * FROM app", start).len(), 30);
}

/// 长时场景：设了 `TESTBED_SOAK_HOURS` 时按场景把被测进程放到某整点之后 30 秒（O 在该整点后 20 分 09 秒到期，约 21 分钟）；
/// 缺省跑缩短版，把被测进程放到该整点之后 19 分 30 秒（桶仍处于 `sealed`、O 尚未到期，约 2 分钟）。其余步骤与断言相同。
#[cfg(feature = "fault-injection")]
#[test]
fn compaction_abandon_when_input_expired_by_ttl() {
    let position = if std::env::var("TESTBED_SOAK_HOURS").is_ok() {
        30_000_000
    } else {
        (19 * 60 + 30) * 1_000_000
    };
    let real = now_us();
    let hour = hour_of(real) + HOUR;
    let offset_s = (hour + position - real).div_euclid(1_000_000);
    let offset = offset_s * 1_000_000;
    let f = Fixture::new();
    let gate = f.root().join("fault-gate");
    std::fs::write(&gate, "hold").unwrap();
    let mut cmd = f.serve(&[
        GRACE_1S,
        ("lifecycle.lake_watermark", Some("\"40m\"")),
        // 场景写的 `1h20m` 不合时长格式（REQ-AMB-CFG-STARTUP-V7：`<整数><单位>`），取等长的 `80m`
        ("retention.period", Some("\"80m\"")),
        ("retention.check_interval", Some("\"10s\"")),
        FLUSH_1S,
    ]);
    cmd.env("TESTBED_CLOCK_OFFSET", offset_s.to_string())
        .env("TESTBED_FAULT_GATE", "compaction.before_commit")
        .env("TESTBED_FAULT_GATE_FILE", &gate);
    let s = Server::start(cmd);
    let ds = f.dataset("app");
    let h = hour - HOUR;
    let label = bucket_label(h);
    let batch = |minute: i64| -> String {
        let rows: Vec<String> = (0..10i64)
            .map(|k| {
                format!(
                    r#"{{"_timestamp":{},"seq":{}}}"#,
                    h + minute * 60_000_000 + k * 1_000_000,
                    minute * 100 + k
                )
            })
            .collect();
        format!("[{}]", rows.join(","))
    };
    ingest_ok(&s, "app", &batch(0));
    ds.wait_records(10, Duration::from_secs(15));
    let o = paths(&head_manifest(&ds, &label)["files"]);
    std::thread::sleep(Duration::from_millis(1500));
    ingest_ok(&s, "app", &batch(50));
    ds.wait_records(20, Duration::from_secs(15));
    let files = paths(&head_manifest(&ds, &label)["files"]);
    let o: String = o.into_iter().next().unwrap();
    let l: String = files.iter().find(|p| **p != o).unwrap().clone();
    assert_eq!(files.len(), 2, "{files:?}");
    // 合并（输入 O 与 L）已停在暂停点
    let (_, held) = poll(
        Duration::from_secs(60),
        || std::fs::read_to_string(&gate).is_ok_and(|g| g.trim() == "held"),
        |h| *h,
    );
    assert!(held, "合并未走到暂停点：{:#?}", s.stderr_lines());

    // 等到 O 的 `ttl` 留痕，至多到（进程时钟）该整点后 25 分钟
    let limit = (hour + 25 * 60_000_000 - offset - now_us()).max(0);
    let (_, expired) = poll(
        Duration::from_micros(limit as u64),
        || {
            head_manifest(&ds, &label)["deletions"]
                .as_array()
                .unwrap()
                .iter()
                .any(|d| d["path"] == o.as_str() && d["reason"] == "ttl")
        },
        |e| *e,
    );
    assert!(expired, "O 未按 ttl 移出：{:#?}", s.stderr_lines());
    std::fs::write(&gate, "go").unwrap();
    std::thread::sleep(Duration::from_secs(60));

    let m = head_manifest(&ds, &label);
    assert_eq!(paths(&m["files"]), BTreeSet::from([l]), "{m}");
    assert!(
        !m["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["reason"] == "compaction"),
        "{m}"
    );
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": h, "end_time": h + HOUR, "size": 1000}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["total"], 10, "{}", r.text());
    assert!(
        compactions(&s, "aborted").is_none_or(|v| v == 0.0),
        "{:?}",
        compactions(&s, "aborted")
    );
    let text = s.get_auth("/metrics").text();
    assert!(
        !text
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled{") && l.contains("stream=\"app\"")),
        "{text}"
    );
}

#[test]
fn compaction_abandon_when_window_unknown() {
    let f = Fixture::new();
    let (s, faults) = f.start_with_store_faults(&[GRACE_1S, FLUSH_1S]);
    // 实例开始受理 `/api/` 请求之后，冷层元数据读取失败
    faults.set("get,head,list _iceberg/default/app/metadata/ 403\n");
    let ds = f.dataset("app");
    let start = two_hours_ago();
    let label = bucket_label(start);
    three_ingests(&s, &ds, start);
    std::thread::sleep(Duration::from_secs(60));
    let m = head_manifest(&ds, &label);
    assert_eq!(m["files"].as_array().unwrap().len(), 3, "{m}");
    assert!(
        !m["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .any(|d| d["reason"] == "compaction"),
        "{m}"
    );
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled"),
        "{:#?}",
        s.stderr_lines()
    );
    let r = common::api::ingest(&s, "app", &format!(r#"[{{"_timestamp":{}}}]"#, now_us()));
    assert_eq!(r.status, 200, "{}", r.text());
}
