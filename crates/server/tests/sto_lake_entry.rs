//! REQ-AMB-STO-LAKE-ENTRY 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录；测试直接读热层快照与桶 manifest、冷层元数据与清单、冷层 Parquet 文件，核对 stderr 的
//! JSON 日志与 `/metrics`。
//!
//! - 多个场景引用的前提「一个过去小时的桶入湖」由 [`main_flow`] 搭建。
//! - 「等待 N 秒」后断言某事已发生的，改为轮询到条件成立即止、上限取 N 秒；断言某事没有发生的，等满 N 秒。
//! - 故障注入：冷层写入失败为对象存储规则 `write _iceberg/ 403`，热层写入失败为 `write default/app/ 403`，冷层数据读取
//!   失败为 `get _iceberg/default/app/data/ 403`（`TESTBED_FAULT_STORE`）；暂停点 `lake.after_digest_check`、
//!   `lake.after_cold_commit`、`lake.before_recover_commit` 与 `shutdown.before_exit` 既可崩溃（`TESTBED_FAULT_PAUSE`）
//!   也可放行（`TESTBED_FAULT_GATE`）；重写失败为故障点 `lake.rewrite`（`TESTBED_FAULT_FAIL`，V2）；时钟偏移
//!   `TESTBED_CLOCK_OFFSET`。都只在 `fault-injection` 下生效。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::api::{ingest, ingest_ok, now_us, poll};
use common::lake::{ColdDir, alter_parquet_rows, ts_seq};
use common::store::{FLUSH_1S, HOUR, bucket_label, hour_of, metric, sha256, stop};
use common::{BEARER, Fixture, Kv, Server};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(130);
/// 两个 `flush.interval`（1 秒）加 130 秒。
const WAIT_FLUSH: Duration = Duration::from_secs(132);

const WATERMARK_1H: Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
const WATERMARK_72H: Kv = ("lifecycle.lake_watermark", Some("\"72h\""));

/// 此刻前第 `n` 小时所在小时桶的起点。
fn hours_ago(n: i64) -> i64 {
    hour_of(now_us()) - n * HOUR
}

/// `n` 条 `_timestamp` 落在小时桶 `start` 内、`seq` 自 `from` 起的记录（JSON 数组）。
fn records(start: i64, from: u64, n: u64) -> String {
    let r: Vec<String> = (from..from + n)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"seq":{i}}}"#,
                start + 1 + i as i64 * 1000
            )
        })
        .collect();
    format!("[{}]", r.join(","))
}

/// 向 `stream` 一次摄取 [`records`]。
fn ingest_bucket(server: &Server, stream: &str, start: i64, from: u64, n: u64) {
    let r = ingest_ok(server, stream, &records(start, from, n));
    assert_eq!(r["status"][0]["successful"], n, "{r}");
}

fn paths(entries: &[Value]) -> BTreeSet<String> {
    entries
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 桶 manifest `m` 中 `reason` 为 `lake` 的留痕。
fn lake_deletions(m: &Value) -> Vec<Value> {
    m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "lake")
        .cloned()
        .collect()
}

/// 以覆盖小时桶 `start` 的时间范围查询 `SELECT * FROM <stream>`（`size` 为 1000），返回行数。
fn rows_in(server: &Server, stream: &str, start: i64, end: i64) -> usize {
    let body = json!({"query": {"sql": format!("SELECT * FROM {stream}"), "start_time": start, "end_time": end, "size": 1000}});
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["hits"].as_array().unwrap().len()
}

/// 带环境变量启动。
fn start_env(f: &Fixture, overrides: &[Kv], envs: &[(&str, String)]) -> Server {
    let mut cmd = f.serve(overrides);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    Server::start(cmd)
}

/// 对象存储故障注入的控制文件，启动前写好初始规则。
struct Faults(PathBuf);

impl Faults {
    fn new(f: &Fixture, rules: &str) -> Faults {
        let p = f.root().join("fault-store");
        std::fs::write(&p, rules).unwrap();
        Faults(p)
    }

    fn set(&self, rules: &str) {
        std::fs::write(&self.0, rules).unwrap();
    }

    fn env(&self) -> (&'static str, String) {
        ("TESTBED_FAULT_STORE", self.0.display().to_string())
    }
}

const COLD_WRITE_403: &str = "write _iceberg/ 403\n";

/// 可放行暂停点的控制文件与环境变量（`TESTBED_FAULT_GATE` / `TESTBED_FAULT_GATE_FILE`）。
struct Hold(PathBuf);

impl Hold {
    fn new(f: &Fixture) -> Hold {
        let p = f.root().join("fault-gate");
        std::fs::write(&p, "hold").unwrap();
        Hold(p)
    }

    fn env(&self, step: &str) -> [(&'static str, String); 2] {
        [
            ("TESTBED_FAULT_GATE", step.to_owned()),
            ("TESTBED_FAULT_GATE_FILE", self.0.display().to_string()),
        ]
    }

    fn wait_held(&self, limit: Duration) {
        let deadline = Instant::now() + limit;
        while std::fs::read_to_string(&self.0).is_ok_and(|s| s.trim() == "hold") {
            assert!(
                Instant::now() < deadline,
                "{} 秒内未走到暂停点",
                limit.as_secs()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn release(&self) {
        std::fs::write(&self.0, "go").unwrap();
    }
}

/// 崩溃暂停点的环境变量与标记文件（`TESTBED_FAULT_PAUSE` / `TESTBED_FAULT_MARKER`）。
fn pause_env(f: &Fixture, step: &str) -> ([(&'static str, String); 2], PathBuf) {
    let marker = f.root().join("fault-marker");
    let _ = std::fs::remove_file(&marker);
    (
        [
            ("TESTBED_FAULT_PAUSE", step.to_owned()),
            ("TESTBED_FAULT_MARKER", marker.display().to_string()),
        ],
        marker,
    )
}

/// stderr 中 `event` 为 `event` 的 JSON 日志。
fn events(s: &Server, event: &str) -> Vec<Value> {
    s.json_lines()
        .into_iter()
        .filter(|v| v["event"] == event)
        .collect()
}

/// 冷层表目录下 `data/` 中的文件（不含目录本身与元数据）。
fn cold_files(f: &Fixture, stream: &str) -> Vec<String> {
    f.cold(stream)
        .keys()
        .into_iter()
        .filter(|k| !k.ends_with('/'))
        .collect()
}

/// 冷层当前版本中 `bucket` 的 `lake` 快照（按 `sequence-number` 升序）；尚无冷层表为空。
fn lake_snapshots(f: &Fixture, stream: &str, label: &str) -> Vec<Value> {
    let cold = f.cold(stream);
    match cold.version() {
        None => Vec::new(),
        Some(v) => ColdDir::lake_snapshots(&cold.metadata(v), label),
    }
}

/// 交接记录 `h` 的 `files` 所列冷层数据文件中的全部 (`_timestamp`, `seq`)。
fn handover_rows(f: &Fixture, stream: &str, h: &Value) -> Vec<(i64, Option<i64>)> {
    let cold = f.cold(stream);
    let mut out = Vec::new();
    for x in h["files"].as_array().unwrap() {
        out.extend(ts_seq(&cold.batches(x["path"].as_str().unwrap())));
    }
    out
}

// ---------------------------------------------------------------------------
// 场景「一个过去小时的桶入湖」
// ---------------------------------------------------------------------------

/// 重启之前：以 `seal_grace` 24h、`lake_watermark` 72h 分两次向桶 B 各写 50 条，B 有两个数据文件 F1、F2，SIGTERM 退出。
/// 返回 B 的起点与 F1、F2 的 `files` 条目。
fn main_flow_before(f: &Fixture) -> (i64, Vec<Value>) {
    let b = hours_ago(3);
    let mut s = f.start(&[
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"24h\"")),
        WATERMARK_72H,
    ]);
    let ds = f.dataset("app");
    ingest_bucket(&s, "app", b, 0, 50);
    ds.wait_records(50, Duration::from_secs(15));
    ingest_bucket(&s, "app", b, 50, 50);
    ds.wait_records(100, Duration::from_secs(15));
    stop(&mut s);
    let m = ds.bucket_now(&bucket_label(b)).expect("桶 B 在快照中");
    let files = m["files"].as_array().unwrap().clone();
    assert_eq!(files.len(), 2, "{m}");
    (b, files)
}

/// 以 `seal_grace` 10m、`lake_watermark` 1h、`storage.delete_delay` 1s 重启。
fn main_flow_config() -> Vec<Kv<'static>> {
    vec![
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"10m\"")),
        WATERMARK_1H,
        ("storage.delete_delay", Some("\"1s\"")),
    ]
}

/// 场景「一个过去小时的桶入湖」：搭建并等到 B 的交接完成。返回实例、B 的起点、入湖前 F1、F2 的 `files` 条目与 B 的桶 manifest。
fn main_flow(f: &Fixture) -> (Server, i64, Vec<Value>, Value) {
    let (b, files) = main_flow_before(f);
    let s = f.start(&main_flow_config());
    let m = f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    (s, b, files, m)
}

/// B 的那次 `lake` 冷层快照与当时的冷层元数据。
fn main_flow_cold(f: &Fixture, b: i64) -> (Value, Value) {
    let cold = f.cold("app");
    let meta = cold.current();
    let snaps = ColdDir::lake_snapshots(&meta, &bucket_label(b));
    assert_eq!(snaps.len(), 1, "{meta}");
    (meta, snaps[0].clone())
}

#[test]
fn lake_entry_main_flow() {
    let f = Fixture::new();
    let (s, b, files, m) = main_flow(&f);
    let label = bucket_label(b);
    let cold = f.cold("app");
    let meta = cold.current();
    let current = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == meta["current-snapshot-id"])
        .expect("冷层当前快照");
    assert_eq!(
        current["summary"]["amberlog.operation"], "lake",
        "{current}"
    );
    assert_eq!(current["summary"]["amberlog.bucket"], label.as_str());

    assert_eq!(m["files"], json!([]), "{m}");
    let inputs = paths(&files);
    assert_eq!(paths(&lake_deletions(&m)), inputs);
    assert_eq!(m["handovers"].as_array().unwrap().len(), 1);
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 100);

    // 1 秒加 1 小时之内物理删除
    let ds = f.dataset("app");
    let (_, gone) = poll(
        Duration::from_secs(3601),
        || inputs.iter().any(|p| ds.exists(p)),
        |any| !any,
    );
    assert!(gone, "入湖输入未删除");
}

#[test]
fn lake_entry_window_excludes_late_file() {
    let f = Fixture::new();
    let (s, b, _, _) = main_flow(&f);
    ingest_bucket(&s, "app", b, 100, 1);
    let m = f
        .dataset("app")
        .wait_handovers(&bucket_label(b), 2, WAIT_FLUSH);
    let h = m["handovers"].as_array().unwrap();
    assert_eq!(h.len(), 2, "{m}");
    assert_ne!(h[1]["iceberg_snapshot_id"], h[0]["iceberg_snapshot_id"]);
    let late: u64 = h[1]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(late, 1);
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 101);
}

// ---------------------------------------------------------------------------
// 触发、时限、停用、停机与补做优先
// ---------------------------------------------------------------------------

/// 使被测进程的时钟落在某整点之后 `after` 秒（`after` 为负即整点之前）的偏移（秒）；返回偏移与那个整点（进程时钟）。
fn offset_to_hour(after: i64) -> (i64, i64) {
    let real = now_us();
    let hour = hour_of(real) + HOUR;
    let target = hour + after * 1_000_000;
    ((target - real).div_euclid(1_000_000), hour)
}

#[test]
fn lake_entry_check_while_running() {
    let f = Fixture::new();
    let (offset, hour) = offset_to_hour(-25);
    let s = start_env(
        &f,
        &[
            FLUSH_1S,
            ("lifecycle.seal_grace", Some("\"1s\"")),
            ("lifecycle.lake_watermark", Some("\"1m\"")),
        ],
        &[("TESTBED_CLOCK_OFFSET", offset.to_string())],
    );
    // `_timestamp` 取此刻（进程时钟），落在整点之前的那个桶
    let ts = now_us() + offset * 1_000_000;
    let body: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{ts},"seq":{i}}}"#))
        .collect();
    let r = ingest_ok(&s, "app", &format!("[{}]", body.join(",")));
    assert_eq!(r["status"][0]["successful"], 10, "{r}");
    let label = bucket_label(hour - HOUR);
    // 整点后 1 分钟越过入湖水位；再 120 秒内完成交接（距此刻约 25 + 60 + 120 秒）
    f.dataset("app")
        .wait_handovers(&label, 1, Duration::from_secs(25 + 60 + 120));
}

#[test]
fn lake_entry_oldest_first() {
    let f = Fixture::new();
    let (b5, b4, b3) = (hours_ago(5), hours_ago(4), hours_ago(3));
    let mut s = f.start(&[FLUSH_1S, WATERMARK_72H]);
    let ds = f.dataset("app");
    for (i, b) in [b5, b4, b3].into_iter().enumerate() {
        ingest_bucket(&s, "app", b, i as u64 * 10, 10);
    }
    ds.wait_records(30, Duration::from_secs(15));
    stop(&mut s);
    let _s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let mut ids = Vec::new();
    for b in [b5, b4, b3] {
        let m = ds.wait_handovers(&bucket_label(b), 1, Duration::from_secs(400));
        ids.push(m["handovers"][0]["snapshot_id"].as_u64().unwrap());
    }
    assert!(ids.windows(2).all(|w| w[0] < w[1]), "{ids:?}");
    let meta = f.cold("app").current();
    let mut lakes: Vec<Value> = ColdDir::snapshots(&meta)
        .into_iter()
        .filter(|x| x["summary"]["amberlog.operation"] == "lake")
        .collect();
    lakes.sort_by_key(|x| x["sequence-number"].as_i64());
    let order: Vec<&str> = lakes
        .iter()
        .map(|x| x["summary"]["amberlog.bucket"].as_str().unwrap())
        .collect();
    assert_eq!(
        order,
        [bucket_label(b5), bucket_label(b4), bucket_label(b3)]
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>()
    );
}

#[test]
fn lake_entry_deadline() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let b = hours_ago(3);
    ingest_bucket(&s, "app", b, 0, 10);
    let t = now_us();
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT_FLUSH);
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let created = ds.snapshot(id)["created_at"].as_i64().unwrap();
    assert!(
        created <= t + 122_000_000,
        "交接晚于时限：{created} > {t} + 122s"
    );
}

/// 以 `lake_watermark` 72h 为 `app` 提交 10 条落在此刻前第 3 小时（桶 B）的记录后 SIGTERM；返回 B 的起点。
fn ten_in_b3_then_stop(f: &Fixture) -> i64 {
    let b = hours_ago(3);
    let mut s = f.start(&[FLUSH_1S, WATERMARK_72H]);
    ingest_bucket(&s, "app", b, 0, 10);
    f.dataset("app").wait_records(10, Duration::from_secs(15));
    stop(&mut s);
    b
}

#[test]
fn lake_entry_skip_disabled() {
    let f = Fixture::new();
    ten_in_b3_then_stop(&f);
    f.dataset("app").write("manifest/_current", b"not json");
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    std::thread::sleep(WAIT);
    assert_eq!(cold_files(&f, "app"), Vec::<String>::new());
    let r = ingest(&s, "app", r#"[{"seq":1}]"#);
    assert_eq!(r.status, 500, "{}", r.text());
    assert_eq!(r.json()["reason"], "internal");
}

#[test]
fn lake_entry_read_broken_disables() {
    let f = Fixture::new();
    let b = ten_in_b3_then_stop(&f);
    let ds = f.dataset("app");
    let head = ds.head();
    let r = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["bucket"] == bucket_label(b).as_str())
        .unwrap()
        .clone();
    ds.remove(r["manifest"].as_str().unwrap());
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let (disabled, ok) = poll(WAIT, || events(&s, "dataset_disabled"), |v| !v.is_empty());
    assert!(ok, "{:#?}", s.stderr_lines());
    assert_eq!(disabled[0]["dataset"], "default/app");
    assert_eq!(disabled[0]["state"], "manifest_unreadable");
    assert_eq!(cold_files(&f, "app"), Vec::<String>::new());
    let r = ingest(&s, "app", r#"[{"seq":1}]"#);
    assert_eq!(r.status, 500, "{}", r.text());
    assert_eq!(r.json()["reason"], "internal");
}

#[test]
fn lake_entry_no_start_in_shutdown() {
    let f = Fixture::new();
    let (offset, hour) = offset_to_hour(5);
    let (envs, marker) = pause_env(&f, "shutdown.before_exit");
    let mut all = envs.to_vec();
    all.push(("TESTBED_CLOCK_OFFSET", offset.to_string()));
    let mut s = start_env(
        &f,
        &[
            FLUSH_1S,
            ("lifecycle.seal_grace", Some("\"1s\"")),
            ("lifecycle.lake_watermark", Some("\"2m\"")),
        ],
        &all,
    );
    ingest_bucket(&s, "app", hour - HOUR, 0, 10);
    f.dataset("app").wait_records(10, Duration::from_secs(15));
    // 该桶在整点后 2 分钟越过入湖水位；此刻是整点后 5 秒多
    s.signal("TERM");
    assert!(
        common::wait_file(&marker, Duration::from_secs(60)),
        "未走到停机暂停点"
    );
    // 停机暂停 200 秒，其间越过入湖水位
    std::thread::sleep(Duration::from_secs(200));
    assert_eq!(cold_files(&f, "app"), Vec::<String>::new());
    s.sigkill();
}

/// 以 `lake_watermark` 1h 启动、要求进程停在 `step`，向 B（此刻前第 3 小时）写 100 条后等进程停住并 SIGKILL。返回 B 的起点。
fn crash_at(f: &Fixture, step: &str, extra: &[Kv]) -> i64 {
    let b = hours_ago(3);
    let (envs, marker) = pause_env(f, step);
    let mut config = vec![FLUSH_1S, WATERMARK_1H];
    config.extend_from_slice(extra);
    let mut s = start_env(f, &config, &envs);
    ingest_bucket(&s, "app", b, 0, 100);
    kill_at_marker_long(&mut s, &marker);
    b
}

/// 等暂停点标记出现后 SIGKILL（至多 130 秒：入湖每个检查周期才走到）。
fn kill_at_marker_long(s: &mut Server, marker: &std::path::Path) {
    assert!(
        common::wait_file(marker, WAIT),
        "未走到暂停点：{:#?}",
        s.stderr_lines()
    );
    s.sigkill();
}

#[test]
fn lake_entry_recover_before_new_lake() {
    let f = Fixture::new();
    let b = crash_at(&f, "lake.after_cold_commit", &[]);
    let hold = Hold::new(&f);
    let s = start_env(
        &f,
        &[FLUSH_1S, WATERMARK_1H],
        &hold.env("lake.before_recover_commit"),
    );
    hold.wait_held(WAIT);
    ingest_bucket(&s, "app", b, 100, 1);
    std::thread::sleep(WAIT_FLUSH);
    hold.release();
    let label = bucket_label(b);
    let (snaps, ok) = poll(WAIT, || lake_snapshots(&f, "app", &label), |v| v.len() == 2);
    assert!(ok, "{snaps:?}");
    let cold = f.cold("app");
    let meta = cold.current();
    let later: u64 = cold
        .added(&meta, &snaps[1])
        .iter()
        .map(|e| e.record_count)
        .sum();
    assert_eq!(later, 1);
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 101);
}

#[test]
fn lake_entry_recover_while_running() {
    let f = Fixture::new();
    let b = hours_ago(3);
    let hold = Hold::new(&f);
    let faults = Faults::new(&f, "");
    let mut envs = hold.env("lake.after_cold_commit").to_vec();
    envs.push(faults.env());
    let s = start_env(&f, &[FLUSH_1S, WATERMARK_1H], &envs);
    ingest_bucket(&s, "app", b, 0, 100);
    hold.wait_held(WAIT);
    faults.set("write default/app/ 403\n");
    hold.release();
    std::thread::sleep(Duration::from_secs(60));
    faults.set("");
    let label = bucket_label(b);
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&label, 1, WAIT);
    let snaps = lake_snapshots(&f, "app", &label);
    assert_eq!(snaps.len(), 1, "{snaps:?}");
    assert_eq!(
        m["handovers"][0]["iceberg_snapshot_id"],
        snaps[0]["snapshot-id"]
    );
    assert_eq!(m["files"], json!([]));
}

// ---------------------------------------------------------------------------
// 入湖前核对摘要
// ---------------------------------------------------------------------------

/// 以 `lake_watermark` 72h 为 B（此刻前第 3 小时）写出 `n` 个数据文件后 SIGTERM。返回 B 的起点与各文件路径（写出先后）。
fn files_in_b3(f: &Fixture, n: u64) -> (i64, Vec<String>) {
    let b = hours_ago(3);
    let mut s = f.start(&[FLUSH_1S, WATERMARK_72H]);
    let ds = f.dataset("app");
    for i in 0..n {
        ingest_bucket(&s, "app", b, i * 10, 10);
        ds.wait_records((i + 1) * 10, Duration::from_secs(15));
    }
    stop(&mut s);
    let mut entries = ds.bucket_now(&bucket_label(b)).unwrap()["files"]
        .as_array()
        .unwrap()
        .clone();
    entries.sort_by_key(|e| e["added_in"].as_u64());
    let files: Vec<String> = entries
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(files.len() as u64, n);
    (b, files)
}

/// 场景「全部入湖输入都核对」重启之前：3 个数据文件，F1、F3 被替换为内容不同的合法 Parquet 文件。
fn digest_setup(f: &Fixture) -> (i64, Vec<String>) {
    let (b, files) = files_in_b3(f, 3);
    let ds = f.dataset("app");
    alter_parquet_rows(&ds.path(&files[0]));
    alter_parquet_rows(&ds.path(&files[2]));
    (b, files)
}

fn mismatches(s: &Server) -> Vec<Value> {
    events(s, "digest_mismatch")
}

#[test]
fn lake_entry_digest_all_checked() {
    let f = Fixture::new();
    let (_, files) = digest_setup(&f);
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let (got, ok) = poll(WAIT, || mismatches(&s), |v| v.len() >= 2);
    assert!(ok, "{:#?}", s.stderr_lines());
    let reported: BTreeSet<&str> = got.iter().map(|v| v["path"].as_str().unwrap()).collect();
    assert_eq!(
        reported,
        BTreeSet::from([files[0].as_str(), files[2].as_str()])
    );
    assert!(got.iter().all(|v| v["dataset"] == "default/app"));
}

/// 前提为场景「全部入湖输入都核对」重启之前的实例与对象存储状态。
#[test]
fn lake_entry_digest_abort() {
    let f = Fixture::new();
    let (b, files) = digest_setup(&f);
    let _s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    std::thread::sleep(WAIT);
    assert_eq!(cold_files(&f, "app"), Vec::<String>::new());
    let m = f.dataset("app").bucket_now(&bucket_label(b)).unwrap();
    assert_eq!(
        paths(m["files"].as_array().unwrap()),
        files.iter().cloned().collect::<BTreeSet<_>>()
    );
    assert!(lake_deletions(&m).is_empty(), "{m}");
}

#[test]
fn lake_entry_digest_stop() {
    let f = Fixture::new();
    digest_setup(&f);
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let (_, ok) = poll(WAIT, || mismatches(&s), |v| v.len() >= 2);
    assert!(ok);
    std::thread::sleep(Duration::from_secs(300));
    assert_eq!(mismatches(&s).len(), 2);
    assert_eq!(cold_files(&f, "app"), Vec::<String>::new());
}

/// 场景「摘要不符时输出稳定事件」重启之后：2 个数据文件，F 被替换。返回实例、B 的起点与 F。
fn digest_event_setup(f: &Fixture) -> (Server, i64, String) {
    let (b, files) = files_in_b3(f, 2);
    alter_parquet_rows(&f.dataset("app").path(&files[0]));
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    (s, b, files[0].clone())
}

#[test]
fn lake_entry_digest_event() {
    let f = Fixture::new();
    let (s, _, file) = digest_event_setup(&f);
    let (_, ok) = poll(WAIT, || mismatches(&s), |v| !v.is_empty());
    assert!(ok);
    // 不再出现第二条
    std::thread::sleep(Duration::from_secs(30));
    let got = mismatches(&s);
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(got[0]["level"], "error");
    assert_eq!(got[0]["dataset"], "default/app");
    assert_eq!(got[0]["path"], file.as_str());
}

#[test]
fn lake_entry_digest_answers_unchanged() {
    let f = Fixture::new();
    let (s, b, _) = digest_event_setup(&f);
    let (_, ok) = poll(WAIT, || mismatches(&s), |v| !v.is_empty());
    assert!(ok);
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 20);
    let r = ingest(&s, "app", r#"[{"seq":1}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn lake_entry_rewrite_uses_checked_bytes() {
    let f = Fixture::new();
    let b = hours_ago(3);
    let hold = Hold::new(&f);
    let s = start_env(
        &f,
        &[FLUSH_1S, WATERMARK_1H],
        &hold.env("lake.after_digest_check"),
    );
    ingest_bucket(&s, "app", b, 0, 50);
    let ds = f.dataset("app");
    let entry = ds.wait_records(50, Duration::from_secs(15));
    let fpath = ds.file_entries(&entry)[0]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut expected: Vec<(i64, Option<i64>)> =
        (0..50).map(|i| (b + 1 + i * 1000, Some(i))).collect();
    hold.wait_held(WAIT);
    alter_parquet_rows(&ds.path(&fpath));
    hold.release();
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT);
    let mut got = handover_rows(&f, "app", &m["handovers"][0]);
    got.sort();
    expected.sort();
    assert_eq!(got, expected);
}

// ---------------------------------------------------------------------------
// 重写
// ---------------------------------------------------------------------------

#[test]
fn lake_entry_records_exact() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let b = hours_ago(3);
    // 带唯一 seq，部分记录的 note 为 null、部分没有 seq 以外的字段
    let recs: Vec<String> = (0..100)
        .map(|i| {
            let ts = b + 1 + i * 1000;
            match i % 3 {
                0 => format!(r#"{{"_timestamp":{ts},"seq":{i},"note":"n{i}"}}"#),
                1 => format!(r#"{{"_timestamp":{ts},"seq":{i},"note":null}}"#),
                _ => format!(r#"{{"_timestamp":{ts},"seq":{i}}}"#),
            }
        })
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
    let m = f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    let cold = f.cold("app");
    let mut got: Vec<(i64, i64, Option<String>)> = Vec::new();
    for x in m["handovers"][0]["files"].as_array().unwrap() {
        for batch in cold.batches(x["path"].as_str().unwrap()) {
            use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
            let ts = batch
                .column_by_name("_timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone();
            let seq = batch
                .column_by_name("seq")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            let note = batch
                .column_by_name("note")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            for i in 0..batch.num_rows() {
                got.push((
                    ts.value(i),
                    seq.value(i),
                    (!note.is_null(i)).then(|| note.value(i).to_owned()),
                ));
            }
        }
    }
    let mut expected: Vec<(i64, i64, Option<String>)> = (0..100)
        .map(|i| (b + 1 + i * 1000, i, (i % 3 == 0).then(|| format!("n{i}"))))
        .collect();
    got.sort();
    expected.sort();
    assert_eq!(got, expected);
}

/// 一个确定的「打乱」：0..n 的一个排列。
fn shuffled(n: u64) -> Vec<u64> {
    let mut v: Vec<u64> = (0..n).collect();
    let mut x: u64 = 0x9e37_79b9_7f4a_7c15;
    for i in (1..v.len()).rev() {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        v.swap(i, (x % (i as u64 + 1)) as usize);
    }
    v
}

#[test]
fn lake_entry_sorted() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let b = hours_ago(3);
    let recs: Vec<String> = shuffled(100)
        .into_iter()
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i as i64 * 1000))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
    let m = f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    let cold = f.cold("app");
    for x in m["handovers"][0]["files"].as_array().unwrap() {
        let ts: Vec<i64> = ts_seq(&cold.batches(x["path"].as_str().unwrap()))
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "{ts:?}");
    }
}

/// 一个 65536 字节、几乎不可压缩的字符串（确定的伪随机字母）。
fn noise(seed: u64) -> String {
    let mut x = seed.wrapping_mul(0x2545_f491_4f6c_dd1d) | 1;
    (0..65536)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (b'a' + (x % 26) as u8) as char
        })
        .collect()
}

/// 场景「冷层文件不超过目标大小」：以 `lake_watermark` 72h、`compaction.target_file_bytes` 1048576 为 B 提交 64 条各带一个
/// 65536 字节随机字符串字段、`_timestamp` 各不相同且顺序打乱的记录，再以 `lake_watermark` 1h 重启并等到交接完成。
fn big_records(f: &Fixture) -> (Server, i64, Value) {
    let b = hours_ago(3);
    let target = ("compaction.target_file_bytes", Some("1048576"));
    let mut s = f.start(&[FLUSH_1S, WATERMARK_72H, target]);
    for chunk in shuffled(64).chunks(16) {
        let recs: Vec<String> = chunk
            .iter()
            .map(|i| {
                format!(
                    r#"{{"_timestamp":{},"seq":{i},"blob":"{}"}}"#,
                    b + 1 + *i as i64 * 1000,
                    noise(*i)
                )
            })
            .collect();
        ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
    }
    let ds = f.dataset("app");
    ds.wait_records(64, Duration::from_secs(30));
    stop(&mut s);
    let s = f.start(&[FLUSH_1S, WATERMARK_1H, target]);
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT);
    (s, b, m)
}

#[test]
fn lake_entry_files_split_in_time_order() {
    let f = Fixture::new();
    let (_s, _, m) = big_records(&f);
    let cold = f.cold("app");
    let mut ranges: Vec<(i64, i64)> = m["handovers"][0]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| {
            let ts: Vec<i64> = ts_seq(&cold.batches(x["path"].as_str().unwrap()))
                .into_iter()
                .map(|(t, _)| t)
                .collect();
            (*ts.iter().min().unwrap(), *ts.iter().max().unwrap())
        })
        .collect();
    assert!(ranges.len() >= 2, "{ranges:?}");
    ranges.sort();
    assert!(ranges.windows(2).all(|w| w[0].1 <= w[1].0), "{ranges:?}");
}

#[test]
fn lake_entry_target_size() {
    let f = Fixture::new();
    let (_s, _, m) = big_records(&f);
    let files = m["handovers"][0]["files"].as_array().unwrap();
    assert!(
        files
            .iter()
            .all(|x| x["size_bytes"].as_u64().unwrap() <= 1_048_576),
        "{m}"
    );
    let n: u64 = files
        .iter()
        .map(|x| x["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(n, 64);
}

// ---------------------------------------------------------------------------
// 入湖的冷层提交
// ---------------------------------------------------------------------------

#[test]
fn lake_entry_one_cold_commit() {
    let f = Fixture::new();
    let (_s, b, m) = big_records(&f);
    let cold = f.cold("app");
    let meta = cold.current();
    let snaps = ColdDir::lake_snapshots(&meta, &bucket_label(b));
    assert_eq!(snaps.len(), 1);
    let added: BTreeSet<String> = cold
        .added(&meta, &snaps[0])
        .into_iter()
        .map(|e| e.path)
        .collect();
    assert_eq!(added, paths(m["handovers"][0]["files"].as_array().unwrap()));
    let id = snaps[0]["snapshot-id"].as_i64().unwrap();
    assert!(
        !cold
            .entries(&meta, &snaps[0])
            .iter()
            .any(|e| e.deleted && e.snapshot_id == id),
        "入湖的冷层提交不得移除文件"
    );
}

#[test]
fn lake_entry_summary_props() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    // UTC 2026-10-12 05 时（测试时钟晚于 2026-10-12 07:00）
    let b = 1_791_781_200_000_000;
    assert_eq!(bucket_label(b), "2026101205");
    assert!(now_us() > b + 2 * HOUR);
    ingest_bucket(&s, "app", b, 0, 10);
    f.dataset("app").wait_handovers("2026101205", 1, WAIT);
    let meta = f.cold("app").current();
    let current = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == meta["current-snapshot-id"])
        .unwrap();
    assert_eq!(current["summary"]["amberlog.operation"], "lake");
    assert_eq!(current["summary"]["amberlog.bucket"], "2026101205");
}

#[test]
fn lake_entry_hot_snapshot_id() {
    let f = Fixture::new();
    let (_s, b, files, m) = main_flow(&f);
    let (_, snap) = main_flow_cold(&f, b);
    let h = snap["summary"]["amberlog.hot_snapshot_id"]
        .as_str()
        .unwrap();
    assert!(
        !h.is_empty() && !h.starts_with('0') && h.bytes().all(|c| c.is_ascii_digit()),
        "{h}"
    );
    let h: u64 = h.parse().unwrap();
    let at_h = f.dataset("app").bucket_at(h, &bucket_label(b)).unwrap();
    assert_eq!(paths(at_h["files"].as_array().unwrap()), paths(&files));
    assert!(h < m["handovers"][0]["snapshot_id"].as_u64().unwrap());
}

#[test]
fn lake_entry_file_digests_property() {
    let f = Fixture::new();
    let (_s, b, _, _) = main_flow(&f);
    let (meta, snap) = main_flow_cold(&f, b);
    let cold = f.cold("app");
    let digests: BTreeMap<String, String> =
        serde_json::from_str(snap["summary"]["amberlog.file_digests"].as_str().unwrap()).unwrap();
    let added: BTreeSet<String> = cold
        .added(&meta, &snap)
        .into_iter()
        .map(|e| e.path)
        .collect();
    assert_eq!(digests.keys().cloned().collect::<BTreeSet<_>>(), added);
    for (path, d) in &digests {
        assert_eq!(*d, sha256(&cold.read(path)), "{path}");
    }
}

// ---------------------------------------------------------------------------
// 入湖交接提交与物理删除
// ---------------------------------------------------------------------------

#[test]
fn lake_entry_crash_before_cold_commit() {
    let f = Fixture::new();
    let b = crash_at(&f, "lake.after_digest_check", &[]);
    let m = f.dataset("app").bucket_now(&bucket_label(b)).unwrap();
    let n: u64 = m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(n, 100, "{m}");
    assert!(lake_deletions(&m).is_empty(), "{m}");
    assert_eq!(m["handovers"], json!([]));
}

#[test]
fn lake_entry_inputs_retired() {
    let f = Fixture::new();
    let (_s, _, files, m) = main_flow(&f);
    let now = paths(m["files"].as_array().unwrap());
    assert!(paths(&files).iter().all(|p| !now.contains(p)), "{m}");
}

#[test]
fn lake_entry_lake_trace() {
    let f = Fixture::new();
    let (_s, _, files, m) = main_flow(&f);
    let lake = lake_deletions(&m);
    assert_eq!(lake.len(), 2, "{m}");
    let got: BTreeSet<(String, String)> = lake
        .iter()
        .map(|d| {
            (
                d["path"].as_str().unwrap().to_owned(),
                d["content_digest"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    let want: BTreeSet<(String, String)> = files
        .iter()
        .map(|x| {
            (
                x["path"].as_str().unwrap().to_owned(),
                x["content_digest"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(got, want);
}

/// 停在 `step`（可放行）期间向 B 写 1 条迟到记录，记下新进入的文件 L 后放行；断言第 1 条交接记录所在快照中 B 只剩 L，
/// 查询返回 `total` 行。
fn late_file_stays(f: &Fixture, s: &Server, hold: &Hold, b: i64, from: u64, total: usize) {
    let ds = f.dataset("app");
    let label = bucket_label(b);
    let before = paths(ds.bucket_now(&label).unwrap()["files"].as_array().unwrap());
    ingest_bucket(s, "app", b, from, 1);
    let (now, ok) = poll(
        Duration::from_secs(15),
        || paths(ds.bucket_now(&label).unwrap()["files"].as_array().unwrap()),
        |p| p.len() > before.len(),
    );
    assert!(ok);
    let late: Vec<String> = now.difference(&before).cloned().collect();
    assert_eq!(late.len(), 1);
    hold.release();
    let m = ds.wait_handovers(&label, 1, WAIT);
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let at = ds.bucket_at(id, &label).unwrap();
    assert_eq!(
        paths(at["files"].as_array().unwrap()),
        late.into_iter().collect::<BTreeSet<_>>()
    );
    assert_eq!(rows_in(s, "app", b, b + HOUR), total);
}

#[test]
fn lake_entry_late_file_stays_hot() {
    let f = Fixture::new();
    let b = hours_ago(3);
    let hold = Hold::new(&f);
    let s = start_env(
        &f,
        &[FLUSH_1S, WATERMARK_1H],
        &hold.env("lake.after_digest_check"),
    );
    ingest_bucket(&s, "app", b, 0, 50);
    hold.wait_held(WAIT);
    late_file_stays(&f, &s, &hold, b, 50, 51);
}

#[test]
fn lake_entry_handover_refs() {
    let f = Fixture::new();
    let (_s, b, _, m) = main_flow(&f);
    let cold = f.cold("app");
    let n = cold.version().unwrap();
    let h = &m["handovers"][0];
    assert_eq!(h["metadata_version"], n);
    let (_, snap) = main_flow_cold(&f, b);
    assert_eq!(h["iceberg_snapshot_id"], snap["snapshot-id"]);
}

#[test]
fn lake_entry_handover_files() {
    let f = Fixture::new();
    let (_s, b, _, m) = main_flow(&f);
    let (meta, snap) = main_flow_cold(&f, b);
    let added: BTreeMap<String, (u64, u64)> = f
        .cold("app")
        .added(&meta, &snap)
        .into_iter()
        .map(|e| (e.path, (e.record_count, e.file_size_in_bytes)))
        .collect();
    let files = m["handovers"][0]["files"].as_array().unwrap();
    let got: BTreeMap<String, (u64, u64)> = files
        .iter()
        .map(|x| {
            (
                x["path"].as_str().unwrap().to_owned(),
                (
                    x["record_count"].as_u64().unwrap(),
                    x["size_bytes"].as_u64().unwrap(),
                ),
            )
        })
        .collect();
    assert_eq!(got, added);
    assert_eq!(got.values().map(|v| v.0).sum::<u64>(), 100);
}

#[test]
fn lake_entry_handover_digest() {
    let f = Fixture::new();
    let (_s, b, _, m) = main_flow(&f);
    let (_, snap) = main_flow_cold(&f, b);
    let digests: BTreeMap<String, String> =
        serde_json::from_str(snap["summary"]["amberlog.file_digests"].as_str().unwrap()).unwrap();
    let cold = f.cold("app");
    for x in m["handovers"][0]["files"].as_array().unwrap() {
        let p = x["path"].as_str().unwrap();
        assert_eq!(x["content_digest"], sha256(&cold.read(p)).as_str(), "{p}");
        assert_eq!(x["content_digest"], digests[p].as_str(), "{p}");
    }
}

#[test]
fn lake_entry_handover_time_bounds() {
    let f = Fixture::new();
    let (_s, _, _, m) = main_flow(&f);
    let cold = f.cold("app");
    for x in m["handovers"][0]["files"].as_array().unwrap() {
        let ts: Vec<i64> = ts_seq(&cold.batches(x["path"].as_str().unwrap()))
            .into_iter()
            .map(|(t, _)| t)
            .collect();
        assert!(
            x["min_timestamp"].is_i64() && x["max_timestamp"].is_i64(),
            "{x}"
        );
        assert_eq!(x["min_timestamp"], *ts.iter().min().unwrap());
        assert_eq!(x["max_timestamp"], *ts.iter().max().unwrap());
    }
}

#[test]
fn lake_entry_inputs_deleted_after_delay() {
    let f = Fixture::new();
    let s = f.start(&[
        FLUSH_1S,
        WATERMARK_1H,
        ("storage.delete_delay", Some("\"1m\"")),
    ]);
    let b = hours_ago(3);
    ingest_bucket(&s, "app", b, 0, 10);
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT_FLUSH);
    let file = lake_deletions(&m)[0]["path"].as_str().unwrap().to_owned();
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let created = ds.snapshot(id)["created_at"].as_i64().unwrap();
    // 交接提交成功后 30 秒仍在
    let at = created + 30_000_000;
    while now_us() < at {
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(ds.exists(&file), "delete_delay 未满就删了");
    // 其后 1 分钟加 1 小时之内删除
    let (_, gone) = poll(Duration::from_secs(60 + 3600), || ds.exists(&file), |e| !e);
    assert!(gone);
}

// ---------------------------------------------------------------------------
// 崩溃后补做交接
// ---------------------------------------------------------------------------

#[test]
fn lake_entry_recover_after_crash() {
    let f = Fixture::new();
    let b = crash_at(&f, "lake.after_cold_commit", &[]);
    let n1 = f.cold("app").version().expect("冷层提交已写成");
    let s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let label = bucket_label(b);
    let m = f.dataset("app").wait_handovers(&label, 1, WAIT);
    assert_eq!(m["handovers"].as_array().unwrap().len(), 1);
    assert_eq!(m["handovers"][0]["metadata_version"], n1);
    assert_eq!(lake_snapshots(&f, "app", &label).len(), 1);
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 100);
}

#[test]
fn lake_entry_recover_after_reactivation() {
    let f = Fixture::new();
    let b = hours_ago(3);
    let label = bucket_label(b);
    let config = [
        FLUSH_1S,
        WATERMARK_1H,
        ("storage.delete_delay", Some("\"1s\"")),
    ];
    let mut s = f.start(&config);
    ingest_bucket(&s, "app", b, 0, 100);
    let ds = f.dataset("app");
    ds.wait_handovers(&label, 1, WAIT_FLUSH);
    stop(&mut s);

    // 时钟推进 49 小时：B 移入留痕归档段（归档每分钟一轮）
    let offset = (49 * HOUR / 1_000_000).to_string();
    let (pause, marker) = pause_env(&f, "lake.after_cold_commit");
    let mut envs = pause.to_vec();
    envs.push(("TESTBED_CLOCK_OFFSET", offset.clone()));
    let mut s = start_env(&f, &config, &envs);
    let (_, ok) = poll(
        Duration::from_secs(150),
        || ds.bucket_now(&label).is_none(),
        |gone| *gone,
    );
    assert!(ok, "B 未移入留痕归档段");
    ingest_bucket(&s, "app", b, 100, 1);
    kill_at_marker_long(&mut s, &marker);

    let s = start_env(&f, &config, &[("TESTBED_CLOCK_OFFSET", offset)]);
    let m = ds.wait_handovers(&label, 1, WAIT);
    let snaps = lake_snapshots(&f, "app", &label);
    assert_eq!(snaps.len(), 2, "{snaps:?}");
    assert_eq!(m["handovers"].as_array().unwrap().len(), 1, "{m}");
    assert_eq!(
        m["handovers"][0]["iceberg_snapshot_id"],
        snaps[1]["snapshot-id"]
    );
    assert_eq!(m["files"], json!([]));
    assert_eq!(rows_in(&s, "app", b, b + HOUR), 101);
}

#[test]
fn lake_entry_recover_before_earlier_bucket() {
    let f = Fixture::new();
    let b3 = crash_at(&f, "lake.after_cold_commit", &[]);
    let b4 = hours_ago(4);
    let hold = Hold::new(&f);
    let s = start_env(
        &f,
        &[FLUSH_1S, WATERMARK_1H],
        &hold.env("lake.before_recover_commit"),
    );
    hold.wait_held(WAIT);
    ingest_bucket(&s, "app", b4, 1000, 10);
    std::thread::sleep(WAIT_FLUSH);
    assert!(
        lake_snapshots(&f, "app", &bucket_label(b4)).is_empty(),
        "补做之前就为 B4 入湖了"
    );
    hold.release();
    let ds = f.dataset("app");
    let m3 = ds.wait_handovers(&bucket_label(b3), 1, Duration::from_secs(260));
    let m4 = ds.wait_handovers(&bucket_label(b4), 1, Duration::from_secs(260));
    assert!(
        m3["handovers"][0]["snapshot_id"].as_u64() < m4["handovers"][0]["snapshot_id"].as_u64()
    );
    assert_eq!(rows_in(&s, "app", b4, b3 + HOUR), 110);
}

#[test]
fn lake_entry_recover_keeps_late_file() {
    let f = Fixture::new();
    let b = crash_at(&f, "lake.after_cold_commit", &[]);
    let hold = Hold::new(&f);
    let s = start_env(
        &f,
        &[FLUSH_1S, WATERMARK_1H],
        &hold.env("lake.before_recover_commit"),
    );
    hold.wait_held(WAIT);
    late_file_stays(&f, &s, &hold, b, 100, 101);
}

#[test]
fn lake_entry_recover_no_cold_data_read() {
    let f = Fixture::new();
    let b = crash_at(&f, "lake.after_cold_commit", &[]);
    let label = bucket_label(b);
    let snaps = lake_snapshots(&f, "app", &label);
    assert_eq!(snaps.len(), 1);
    let d: BTreeMap<String, String> = serde_json::from_str(
        snaps[0]["summary"]["amberlog.file_digests"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let faults = Faults::new(&f, "get _iceberg/default/app/data/ 403\n");
    let _s = start_env(&f, &[FLUSH_1S, WATERMARK_1H], &[faults.env()]);
    let m = f.dataset("app").wait_handovers(&label, 1, WAIT);
    let h = &m["handovers"][0];
    for x in h["files"].as_array().unwrap() {
        let p = x["path"].as_str().unwrap();
        assert_eq!(x["content_digest"], d[p].as_str(), "{p}");
    }
}

// ---------------------------------------------------------------------------
// 迟到记录
// ---------------------------------------------------------------------------

#[test]
fn lake_entry_late_relake() {
    let f = Fixture::new();
    let (s, b, _, _) = main_flow(&f);
    let label = bucket_label(b);
    ingest_bucket(&s, "app", b, 100, 1);
    let (snaps, ok) = poll(
        WAIT_FLUSH,
        || lake_snapshots(&f, "app", &label),
        |v| v.len() == 2,
    );
    assert!(ok, "{snaps:?}");
    let meta = f.cold("app").current();
    assert!(
        ColdDir::snapshots(&meta)
            .iter()
            .all(|x| x["summary"]["amberlog.operation"] == "lake"),
        "{meta}"
    );
    let (_, ok) = poll(WAIT, || rows_in(&s, "app", b, b + HOUR), |n| *n == 101);
    assert!(ok);
}

// ---------------------------------------------------------------------------
// 入湖失败不阻断摄取
// ---------------------------------------------------------------------------

/// 场景「冷层写入失败时照常受理」：冷层写入失败，分 5 个批次、间隔 60 秒向此刻前第 3 小时写入，每批 200。返回实例与批次所在桶。
fn failure_accepts_ingest(f: &Fixture) -> (Server, i64) {
    let faults = Faults::new(f, COLD_WRITE_403);
    let s = start_env(f, &[FLUSH_1S, WATERMARK_1H], &[faults.env()]);
    let b = hours_ago(3);
    for i in 0..5 {
        if i > 0 {
            std::thread::sleep(Duration::from_secs(60));
        }
        let r = ingest(&s, "app", &records(b, i * 10, 10));
        assert_eq!(r.status, 200, "{}", r.text());
    }
    (s, b)
}

#[test]
fn lake_entry_failure_accepts_ingest() {
    let f = Fixture::new();
    let (s, b) = failure_accepts_ingest(&f);
    let (_, ok) = poll(
        Duration::from_secs(10),
        || rows_in(&s, "app", b, b + HOUR),
        |n| *n == 50,
    );
    assert!(ok);
}

#[test]
fn lake_entry_failure_ready() {
    let f = Fixture::new();
    let (s, _) = failure_accepts_ingest(&f);
    assert_eq!(s.get("/readyz").status, 200);
}

// ---------------------------------------------------------------------------
// 重写失败（V2）
// ---------------------------------------------------------------------------

/// 场景「重写失败的桶本进程不再入湖，后面的桶照常入湖」重启之后的实例：以 `lake_watermark` 72h 为 `app` 提交
/// 此刻前第 4、3 小时（B4、B3）的记录各 10 条后 SIGTERM，以 1h 重启并注入一次重写失败（故障点 `lake.rewrite`），
/// 等待 300 秒后在时刻 `r` 读取 `/metrics`。
struct RewriteFailed {
    s: Server,
    b4: i64,
    b3: i64,
    r: i64,
    metrics: String,
}

fn rewrite_failed(f: &Fixture) -> RewriteFailed {
    // 两个桶只取一次：跨过整点时各步仍指同一个桶
    let hour = hour_of(now_us());
    let (b4, b3) = (hour - 4 * HOUR, hour - 3 * HOUR);
    let mut s = f.start(&[FLUSH_1S, WATERMARK_72H]);
    ingest_bucket(&s, "app", b4, 0, 10);
    ingest_bucket(&s, "app", b3, 10, 10);
    f.dataset("app").wait_records(20, Duration::from_secs(15));
    stop(&mut s);
    let s = start_env(
        f,
        &[FLUSH_1S, WATERMARK_1H],
        &[("TESTBED_FAULT_FAIL", "lake.rewrite".to_owned())],
    );
    std::thread::sleep(Duration::from_secs(300));
    let r = now_us();
    let resp = s.get_auth("/metrics");
    assert_eq!(resp.status, 200, "{}", resp.text());
    RewriteFailed {
        s,
        b4,
        b3,
        r,
        metrics: resp.text(),
    }
}

/// `/metrics` 文本中序列 `series` 的值。
fn sample(metrics: &str, series: &str) -> Option<f64> {
    metrics
        .lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map(|v| v.trim().parse().expect("样本值是数"))
}

#[test]
fn lake_entry_rewrite_fail_stops_bucket() {
    let f = Fixture::new();
    let x = rewrite_failed(&f);
    let lakes: Vec<Value> = ColdDir::snapshots(&f.cold("app").current())
        .into_iter()
        .filter(|v| v["summary"]["amberlog.operation"] == "lake")
        .collect();
    assert_eq!(lakes.len(), 1, "{lakes:#?}");
    assert_eq!(lakes[0]["summary"]["amberlog.bucket"], bucket_label(x.b3));
    let ds = f.dataset("app");
    let m4 = ds.bucket_now(&bucket_label(x.b4)).unwrap();
    assert!(!m4["files"].as_array().unwrap().is_empty(), "{m4}");
    assert!(lake_deletions(&m4).is_empty(), "{m4}");
    let m3 = ds.bucket_now(&bucket_label(x.b3)).unwrap();
    assert!(m3["files"].as_array().unwrap().is_empty(), "{m3}");
    assert_eq!(rows_in(&x.s, "app", x.b4, x.b4 + HOUR), 10);
    let lag = sample(
        &x.metrics,
        "amberlog_lake_lag_seconds{org=\"default\",stream=\"app\"}",
    )
    .unwrap();
    // B4 的入湖水位时刻：桶结束（起点加 1 小时）再加 1 小时水位
    let floor = (x.r - (x.b4 + 2 * HOUR)) as f64 / 1e6 - 1.0;
    assert!(lag >= floor, "{lag} < {floor}");
}

#[test]
fn lake_entry_rewrite_fail_retried_after_restart() {
    let f = Fixture::new();
    let mut x = rewrite_failed(&f);
    stop(&mut x.s);
    let _s = f.start(&[FLUSH_1S, WATERMARK_1H]);
    let label = bucket_label(x.b4);
    let ds = f.dataset("app");
    ds.wait_handovers(&label, 1, WAIT);
    assert_eq!(lake_snapshots(&f, "app", &label).len(), 1);
    let m4 = ds.bucket_now(&label).unwrap();
    assert!(m4["files"].as_array().unwrap().is_empty(), "{m4}");
}

#[test]
fn lake_entry_rewrite_fail_warn_log() {
    let f = Fixture::new();
    let x = rewrite_failed(&f);
    let label = bucket_label(x.b4);
    let lines: Vec<Value> =
        x.s.json_lines()
            .into_iter()
            .filter(|v| {
                v["level"] == "warn"
                    && v["dataset"] == "default/app"
                    && v["bucket"] == label.as_str()
                    && v.get("event").is_none()
            })
            .collect();
    assert_eq!(lines.len(), 1, "{:#?}", x.s.stderr_lines());
    assert!(
        lines[0]["reason"].as_str().is_some_and(|r| !r.is_empty()),
        "{}",
        lines[0]
    );
}

#[test]
fn lake_entry_rewrite_fail_metric() {
    let f = Fixture::new();
    let x = rewrite_failed(&f);
    assert_eq!(
        sample(
            &x.metrics,
            "amberlog_lake_rewrite_failures_total{org=\"default\",stream=\"app\"}"
        ),
        Some(1.0),
        "{}",
        x.metrics
    );
}

// ---------------------------------------------------------------------------
// 积压的指标与告警
// ---------------------------------------------------------------------------

fn lag(s: &Server) -> Option<f64> {
    metric(
        s,
        "amberlog_lake_lag_seconds{org=\"default\",stream=\"app\"}",
    )
}

#[test]
fn lake_entry_lag_metric() {
    let f = Fixture::new();
    let faults = Faults::new(&f, COLD_WRITE_403);
    let s = start_env(&f, &[FLUSH_1S, WATERMARK_1H], &[faults.env()]);
    let b = hours_ago(3);
    ingest_bucket(&s, "app", b, 0, 10);
    // 入湖水位时刻 W：桶终点加 1 小时
    let w = b + HOUR + HOUR;
    std::thread::sleep(Duration::from_secs(30));
    let before = now_us();
    let got = lag(&s).expect("有 lag 序列");
    let after = now_us();
    let lo = (before - w).div_euclid(1_000_000) - 1;
    let hi = (after - w).div_euclid(1_000_000);
    assert!(
        got >= lo as f64 && got <= hi as f64,
        "{got} 不在 [{lo}, {hi}]"
    );
    faults.set("");
    let (v, ok) = poll(WAIT, || lag(&s), |v| *v == Some(0.0));
    assert!(ok, "{v:?}");
}

/// 场景「积压越过阈值时告警」：`lake_alert_after` 1h、冷层写入失败，向此刻前第 4 小时写 10 条。
fn backlog_alert(f: &Fixture) -> (Server, Faults, i64) {
    let faults = Faults::new(f, COLD_WRITE_403);
    let s = start_env(
        f,
        &[
            FLUSH_1S,
            WATERMARK_1H,
            ("lifecycle.lake_alert_after", Some("\"1h\"")),
        ],
        &[faults.env()],
    );
    let b = hours_ago(4);
    ingest_bucket(&s, "app", b, 0, 10);
    (s, faults, b)
}

fn alerts(s: &Server) -> Vec<Value> {
    events(s, "lake_backlog_high")
}

#[test]
fn lake_entry_backlog_alert() {
    let f = Fixture::new();
    let (s, _faults, b) = backlog_alert(&f);
    let (got, ok) = poll(WAIT, || alerts(&s), |v| !v.is_empty());
    assert!(ok, "{:#?}", s.stderr_lines());
    std::thread::sleep(Duration::from_secs(20));
    let got2 = alerts(&s);
    assert_eq!(got2.len(), 1, "{got2:?}");
    let a = &got[0];
    assert_eq!(a["level"], "warn");
    assert_eq!(a["dataset"], "default/app");
    assert_eq!(a["bucket"], bucket_label(b).as_str());
    assert_eq!(a["alert_after_seconds"], 3600);
    assert!(a["lag_seconds"].as_i64().unwrap() > 3600, "{a}");
    assert_eq!(s.get("/readyz").status, 200);
}

#[test]
fn lake_entry_backlog_alert_once() {
    let f = Fixture::new();
    let (s, faults, b) = backlog_alert(&f);
    let (_, ok) = poll(WAIT, || alerts(&s), |v| !v.is_empty());
    assert!(ok);
    std::thread::sleep(Duration::from_secs(180));
    assert_eq!(alerts(&s).len(), 1);
    faults.set("");
    std::thread::sleep(WAIT);
    f.dataset("app")
        .wait_handovers(&bucket_label(b), 1, Duration::from_secs(1));
    faults.set(COLD_WRITE_403);
    ingest_bucket(&s, "app", hours_ago(5), 100, 10);
    let (got, ok) = poll(WAIT, || alerts(&s), |v| v.len() >= 2);
    assert!(ok, "{got:?}");
    std::thread::sleep(Duration::from_secs(20));
    assert_eq!(alerts(&s).len(), 2);
}
