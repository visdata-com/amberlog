//! REQ-AMB-QRY-CROSS-TIER 的系统测试：以子进程运行 `amberlog`，对象存储用
//! `file://` 临时目录；测试直接读热层快照与桶 manifest、冷层元数据与清单，经 `_search` 查询。
//!
//! 「等待某桶入湖完成」按块的定义轮询当前快照中该桶的桶 manifest 出现第 k 条交接记录（[`Ds::wait_handovers`]）。
//! 「等待 N 秒」一类的等待改为轮询到条件成立即止，上限取场景所给时长。块中写「与上一场景相同的实例与数据」或
//! 「上一场景的查询已返回 HTTP 503」的场景，各自的测试重复前一场景的准备步骤，不依赖测试执行次序。
//! 暂停点用 `fault::gate`（`TESTBED_FAULT_GATES` 可同时停住入湖与一次查询）与 `fault::pause`（标记后 SIGKILL），
//! 冷层读写失败用对象存储请求注入（`TESTBED_FAULT_STORE`，请求记录 `TESTBED_FAULT_STORE_LOG`）。

mod common;
mod qry_common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use common::api::now_us;
use common::lake::ColdDir;
use common::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of, kill_at_marker, stop};
use common::{BEARER, Fixture, Kv, Resp, Server, http_within};
use iceberg::spec::{FormatVersion, ManifestList};
use qry_common::{column, hits, query};
use serde_json::{Value, json};

/// 入湖完成的等待上限（入湖检查每 10 秒一轮，块的入湖时限远大于此）。
const WAIT: Duration = Duration::from_secs(150);
/// 延后物理删除（`storage.delete_delay` 为 `1s`，每 60 秒一轮）之后热层文件消失的等待上限。
const PURGE: Duration = Duration::from_secs(180);

/// 此刻前第 `n` 小时所在小时桶的起点。
fn hours_ago(n: i64) -> i64 {
    hour_of(now_us()) - n * HOUR
}

/// 场景共用的配置：`flush.interval` 1s、`lifecycle.seal_grace` 1s、`lifecycle.lake_watermark` 2s，另加 `extra`。
fn lake_cfg<'a>(extra: &[Kv<'a>]) -> Vec<Kv<'a>> {
    let mut all = vec![
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"2s\"")),
    ];
    all.extend_from_slice(extra);
    all
}

const DELETE_1S: Kv = ("storage.delete_delay", Some("\"1s\""));
const DELETE_1H: Kv = ("storage.delete_delay", Some("\"1h\""));

/// 以 `cfg` 启动，另设环境变量 `envs`。
fn start_env(f: &Fixture, cfg: &[Kv], envs: &[(&str, String)]) -> Server {
    let mut cmd = f.serve(cfg);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    Server::start(cmd)
}

/// 向 `stream` 摄取 `seq` 为 `seqs`、`_timestamp` 自 `start + 1` 起每条加 `step` 微秒的记录（一个请求），断言全部受理。
fn ingest_seq(s: &Server, stream: &str, start: i64, seqs: std::ops::Range<i64>, step: i64) {
    let records: Vec<Value> = seqs
        .clone()
        .map(|i| json!({"_timestamp": start + 1 + (i - seqs.start) * step, "seq": i}))
        .collect();
    let r = s.ingest(stream, &Value::Array(records));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], seqs.end - seqs.start);
}

/// 以 `[start, end)` 查询 `sql`（`size` 为 `size`）。
fn search(s: &Server, sql: &str, range: (i64, i64), size: u64) -> Resp {
    s.search(&query(sql, range, &json!({"size": size})))
}

fn ok(r: &Resp) -> Value {
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()
}

/// 小时桶 `h` 的时间范围。
fn hour(h: i64) -> (i64, i64) {
    (h, h + HOUR)
}

/// 响应中 `seq` 的取值，断言无重复，按升序返回。
fn unique_seqs(body: &Value) -> Vec<i64> {
    let all: Vec<i64> = column(body, "seq")
        .iter()
        .map(|v| v.as_i64().expect("seq 是整数"))
        .collect();
    let set: BTreeSet<i64> = all.iter().copied().collect();
    assert_eq!(set.len(), all.len(), "seq 有重复：{all:?}");
    set.into_iter().collect()
}

/// 桶 manifest `m` 的存活文件路径。
fn live_paths(m: &Value) -> Vec<String> {
    m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 桶 manifest `m` 中原因为 `lake` 的留痕的路径（入湖输入）。
fn lake_deleted(m: &Value) -> Vec<String> {
    m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "lake")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 等到 `paths`（相对数据集前缀）在对象存储中都不存在。
fn wait_gone(ds: &Ds, paths: &[String]) {
    let deadline = Instant::now() + PURGE;
    while paths.iter().any(|p| ds.exists(p)) {
        assert!(Instant::now() < deadline, "热层文件未物理删除：{paths:?}");
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// 向 `app` 摄取 `n` 条 `_timestamp` 落在小时桶 `b` 内、`seq` 自 `from` 起的记录，等该桶第 `k` 次入湖完成；
/// `purge` 时再等它的入湖输入都已物理删除。返回入湖后该桶的桶 manifest。
fn lake_bucket(
    f: &Fixture,
    s: &Server,
    b: i64,
    seqs: std::ops::Range<i64>,
    k: usize,
    purge: bool,
) -> Value {
    ingest_seq(s, "app", b, seqs, 1_000_000);
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&bucket_label(b), k, WAIT);
    if purge {
        wait_gone(&ds, &lake_deleted(&m));
    }
    m
}

/// `<store>/amberlog/_iceberg/default/app/data/` 之下以 `.parquet` 结尾的对象个数。
fn cold_parquet_count(f: &Fixture) -> usize {
    f.cold("app")
        .keys()
        .iter()
        .filter(|k| k.starts_with("data/") && k.ends_with(".parquet"))
        .count()
}

/// 冷层当前快照的元数据与当前快照。
fn cold_current(f: &Fixture) -> (ColdDir, Value, Value) {
    let cold = f.cold("app");
    let meta = cold.current();
    let current = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == meta["current-snapshot-id"])
        .expect("冷层当前快照");
    (cold, meta, current)
}

/// 冷层当前快照中存活（未标为已删除）的清单条目。
fn cold_live_entries(f: &Fixture) -> Vec<common::lake::Entry> {
    let (cold, meta, current) = cold_current(f);
    cold.entries(&meta, &current)
        .into_iter()
        .filter(|e| !e.deleted)
        .collect()
}

/// 冷层当前版本中出现桶 `b` 的第 `k` 个 `lake` 快照之前一直等待。
fn wait_cold_lake(f: &Fixture, b: i64, k: usize) {
    let cold = f.cold("app");
    let label = bucket_label(b);
    let deadline = Instant::now() + WAIT;
    loop {
        if cold.version().is_some() && ColdDir::lake_snapshots(&cold.current(), &label).len() >= k {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "冷层未出现桶 {label} 的第 {k} 个 lake 快照"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 同时停住几个步骤的闸门（`TESTBED_FAULT_GATES`），各用一个控制文件。
struct Gates {
    files: Vec<(&'static str, PathBuf)>,
}

impl Gates {
    /// `steps` 中各步骤的闸门；`armed` 为真的一开始就停，否则等 [`Gates::arm`]。
    fn new(f: &Fixture, steps: &[(&'static str, bool)]) -> Gates {
        let files = steps
            .iter()
            .map(|(step, armed)| {
                let p = f.root().join(format!("gate-{step}"));
                std::fs::write(&p, if *armed { "hold" } else { "idle" }).unwrap();
                (*step, p)
            })
            .collect();
        Gates { files }
    }

    fn env(&self) -> (&'static str, String) {
        let v: Vec<String> = self
            .files
            .iter()
            .map(|(s, p)| format!("{s}={}", p.display()))
            .collect();
        ("TESTBED_FAULT_GATES", v.join(";"))
    }

    fn file(&self, step: &str) -> &PathBuf {
        &self.files.iter().find(|(s, _)| *s == step).unwrap().1
    }

    fn arm(&self, step: &str) {
        std::fs::write(self.file(step), "hold").unwrap();
    }

    fn wait_held(&self, step: &str, s: &Server) {
        let deadline = Instant::now() + WAIT;
        while std::fs::read_to_string(self.file(step)).unwrap().trim() != "held" {
            assert!(
                Instant::now() < deadline,
                "未走到 {step}：{:#?}",
                s.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn release(&self, step: &str) {
        std::fs::write(self.file(step), "go").unwrap();
    }
}

/// 对象存储请求注入（`TESTBED_FAULT_STORE`）与请求记录（`TESTBED_FAULT_STORE_LOG`）。
struct Faults {
    control: PathBuf,
    log: PathBuf,
}

impl Faults {
    fn new(f: &Fixture) -> Faults {
        let control = f.root().join("fault-store");
        let log = f.root().join("fault-store.log");
        std::fs::write(&control, "").unwrap();
        Faults { control, log }
    }

    fn envs(&self) -> Vec<(&'static str, String)> {
        vec![
            ("TESTBED_FAULT_STORE", self.control.display().to_string()),
            ("TESTBED_FAULT_STORE_LOG", self.log.display().to_string()),
        ]
    }

    fn set(&self, rules: &str) {
        std::fs::write(&self.control, rules).unwrap();
    }

    fn clear_log(&self) {
        let _ = std::fs::remove_file(&self.log);
    }

    /// 记录下的请求（`<操作> <相对根前缀的键>`）。
    fn requests(&self) -> Vec<String> {
        std::fs::read_to_string(&self.log)
            .unwrap_or_default()
            .lines()
            .map(str::to_owned)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// A · 一次查询的数据范围
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_union_lake_and_open() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 100..105, 1);
    let r = search(
        &s,
        "SELECT * FROM app",
        (b - HOUR, hour_of(now) + 2 * HOUR),
        1000,
    );
    assert_eq!(ok(&r)["total"], 105);
}

#[test]
fn cross_tier_lake_failed_bucket_hot() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    faults.set("write _iceberg/ 403\n");
    let s = start_env(&f, &lake_cfg(&[]), &faults.envs());
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 0..50, 1_000_000);
    // 场景写「等待 150 秒」：等到入湖已向冷层写过（被拒）即可，至多 150 秒
    let deadline = Instant::now() + WAIT;
    while !faults
        .requests()
        .iter()
        .any(|r| (r.starts_with("put ") || r.starts_with("create ")) && r.contains(" _iceberg/"))
    {
        assert!(Instant::now() < deadline, "入湖未尝试写冷层");
        std::thread::sleep(Duration::from_millis(200));
    }
    let ds = f.dataset("app");
    let m = ds.bucket_now(&bucket_label(b)).expect("桶仍在热层");
    let body = ok(&search(&s, "SELECT * FROM app", hour(b), 1000));
    assert_eq!(body["total"], 50);
    assert_eq!(body["scan_files"], m["files"].as_array().unwrap().len());
}

#[test]
fn cross_tier_cold_files_answer() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[DELETE_1S]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, true);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b), 1000));
    assert_eq!(body["total"], 100);
    assert_eq!(body["scan_files"], cold_parquet_count(&f));
}

#[test]
fn cross_tier_no_cold_table() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S]);
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 0..10, 1);
    let snap = f.dataset("app").wait_records(10, Duration::from_secs(15));
    assert_eq!(snap["summary"]["cold"]["record_count"], 0, "{snap}");
    assert!(f.iceberg_keys().is_empty(), "{:?}", f.iceberg_keys());
    let body = ok(&search(
        &s,
        "SELECT * FROM app",
        (now - HOUR, now + HOUR),
        100,
    ));
    assert_eq!(body["total"], 10);
}

#[test]
fn cross_tier_cold_table_vanished_500() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 100..105, 1);
    assert_eq!(
        f.dataset("app").head()["summary"]["cold"]["record_count"],
        100
    );
    std::fs::remove_dir_all(f.cold("app").dir).unwrap();
    for range in [hour(b), (now - 60_000_000, now + 60_000_000)] {
        search(&s, "SELECT * FROM app", range, 1000).assert_envelope(500, "internal");
    }
}

#[test]
fn cross_tier_late_records_after_lake() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    ingest_seq(&s, "app", b + 30 * 60_000_000, 100..103, 1);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b), 1000));
    assert_eq!(body["total"], 103);
}

// ---------------------------------------------------------------------------
// 交接窗口
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_handover_window_once() {
    let f = Fixture::new();
    let gates = Gates::new(&f, &[("lake.after_cold_commit", true)]);
    let s = start_env(&f, &lake_cfg(&[]), &[gates.env()]);
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 0..100, 1_000_000);
    wait_cold_lake(&f, b, 1);
    gates.wait_held("lake.after_cold_commit", &s);
    let body = ok(&search(&s, "SELECT seq FROM app", hour(b), 1000));
    assert_eq!(unique_seqs(&body), (0..100).collect::<Vec<_>>());
    let m = f.dataset("app").bucket_now(&bucket_label(b)).unwrap();
    assert_eq!(body["scan_files"], m["files"].as_array().unwrap().len());
}

#[test]
fn cross_tier_handover_crash_restart_once() {
    let f = Fixture::new();
    let (mut s, marker) = f.start_paused(&lake_cfg(&[]), "lake.after_cold_commit");
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 0..100, 1_000_000);
    kill_at_marker(&mut s, &marker);
    drop(s);
    let cold = f.cold("app");
    assert_eq!(
        ColdDir::lake_snapshots(&cold.current(), &bucket_label(b)).len(),
        1
    );
    let s = f.start(&lake_cfg(&[]));
    s.wait_readyz(200, Duration::from_secs(30));
    let body = ok(&search(&s, "SELECT seq FROM app", hour(b), 1000));
    assert_eq!(unique_seqs(&body), (0..100).collect::<Vec<_>>());
}

#[test]
fn cross_tier_second_handover_window() {
    let f = Fixture::new();
    let gates = Gates::new(&f, &[("lake.after_cold_commit", false)]);
    let s = start_env(&f, &lake_cfg(&[]), &[gates.env()]);
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 1..101, 1, false);
    gates.arm("lake.after_cold_commit");
    ingest_seq(&s, "app", b + 30 * 60_000_000, 101..104, 1);
    wait_cold_lake(&f, b, 2);
    gates.wait_held("lake.after_cold_commit", &s);
    let body = ok(&search(&s, "SELECT seq FROM app", hour(b), 1000));
    assert_eq!(unique_seqs(&body), (1..104).collect::<Vec<_>>());
}

// ---------------------------------------------------------------------------
// 读的先后
// ---------------------------------------------------------------------------

/// 被暂停点拦住的查询的读超时：放行之前测试至多等一次入湖（`WAIT`）与一次物理删除（`PURGE`，每 60 秒一轮），
/// 读超时不早于这些等待各自的上限，另留余量。
const HELD_READ: Duration = Duration::from_secs(WAIT.as_secs() + PURGE.as_secs() + 60);

/// 在另一线程提交 `SELECT seq FROM app`（`size` 1000，范围 `range`），返回其句柄；读超时为 `HELD_READ`。
fn query_async(s: &Server, sql: &'static str, range: (i64, i64)) -> std::thread::JoinHandle<Resp> {
    let addr = s.addr;
    let body = query(sql, range, &json!({"size": 1000})).to_string();
    std::thread::spawn(move || {
        http_within(
            addr,
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            body.as_bytes(),
            HELD_READ,
        )
    })
}

#[test]
fn cross_tier_hot_then_cold_no_loss() {
    let f = Fixture::new();
    let gates = Gates::new(
        &f,
        &[
            ("lake.after_digest_check", true),
            ("query.after_hot_snapshot", true),
        ],
    );
    let s = start_env(&f, &lake_cfg(&[DELETE_1H]), &[gates.env()]);
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 1..101, 1_000_000);
    gates.wait_held("lake.after_digest_check", &s);
    let q = query_async(&s, "SELECT seq FROM app", hour(b));
    gates.wait_held("query.after_hot_snapshot", &s);
    gates.release("lake.after_digest_check");
    f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    gates.release("query.after_hot_snapshot");
    let r = q.join().unwrap();
    assert_eq!(unique_seqs(&ok(&r)), (1..101).collect::<Vec<_>>());
}

#[test]
fn cross_tier_cold_version_per_query() {
    let f = Fixture::new();
    let gates = Gates::new(&f, &[("lake.after_digest_check", true)]);
    let s = start_env(&f, &lake_cfg(&[DELETE_1S]), &[gates.env()]);
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 0..100, 1_000_000);
    gates.wait_held("lake.after_digest_check", &s);
    assert_eq!(
        ok(&search(&s, "SELECT * FROM app", hour(b), 1000))["total"],
        100
    );
    gates.release("lake.after_digest_check");
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT);
    wait_gone(&ds, &lake_deleted(&m));
    assert_eq!(
        ok(&search(&s, "SELECT * FROM app", hour(b), 1000))["total"],
        100
    );
}

// ---------------------------------------------------------------------------
// 不重不漏
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_exactly_once_during_lake() {
    const INGEST_FOR: Duration = Duration::from_secs(300);
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[DELETE_1H]));
    let b = hours_ago(3);
    // 已收到 200 的 seq 与收到的时刻
    let acked: Mutex<Vec<(i64, Instant)>> = Mutex::new(Vec::new());
    let done = AtomicBool::new(false);
    let started = Instant::now();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            let mut next = 0i64;
            while started.elapsed() < INGEST_FOR {
                // 在该小时内均匀分布：按已过时长在小时内的比例取时刻
                let frac = started.elapsed().as_secs_f64() / INGEST_FOR.as_secs_f64();
                let ts = b + (frac * (HOUR - 1_000_000) as f64) as i64;
                let records: Vec<Value> = (next..next + 20)
                    .map(|i| json!({"_timestamp": ts + (i - next), "seq": i}))
                    .collect();
                let r = s.ingest("app", &Value::Array(records));
                assert_eq!(r.status, 200, "{}", r.text());
                let at = Instant::now();
                acked
                    .lock()
                    .unwrap()
                    .extend((next..next + 20).map(|i| (i, at)));
                next += 20;
                // 每秒 20 条：5 分钟约 6000 条，不超过查询的 `size` 10000（缺省按 `_timestamp` 降序，超过即截掉最早的）
                std::thread::sleep(Duration::from_secs(1));
            }
            done.store(true, Ordering::SeqCst);
        });
        scope.spawn(|| {
            // 第一批摄取确认之前 stream 尚不存在，查询从此后开始
            while acked.lock().unwrap().is_empty() {
                std::thread::sleep(Duration::from_millis(50));
            }
            while !done.load(Ordering::SeqCst) {
                let submitted = Instant::now();
                let must: BTreeSet<i64> = acked
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(_, at)| submitted.duration_since(*at) >= Duration::from_secs(10))
                    .map(|(i, _)| *i)
                    .collect();
                let body = ok(&search(&s, "SELECT seq FROM app", hour(b), 10_000));
                let got: BTreeSet<i64> = unique_seqs(&body).into_iter().collect();
                assert!(got.len() < 10_000, "结果达到 size 上限，测试的摄取量过大");
                let missing: Vec<&i64> = must.difference(&got).take(10).collect();
                assert!(
                    missing.is_empty(),
                    "已确认 10 秒以上的记录缺失：{missing:?}"
                );
                std::thread::sleep(Duration::from_millis(500));
            }
        });
    });
    // 摄取停止后等该桶最后一次入湖完成：热层不再有记录、冷层记录数等于已确认的条数
    let all: BTreeSet<i64> = acked.lock().unwrap().iter().map(|(i, _)| *i).collect();
    let ds = f.dataset("app");
    let deadline = Instant::now() + WAIT;
    loop {
        let head = ds.head();
        if head["summary"]["record_count"] == 0
            && head["summary"]["cold"]["record_count"] == all.len()
        {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "最后一次入湖未完成：{}",
            head["summary"]
        );
        std::thread::sleep(Duration::from_millis(500));
    }
    let body = ok(&search(&s, "SELECT seq FROM app", hour(b), 10_000));
    let got: BTreeSet<i64> = unique_seqs(&body).into_iter().collect();
    assert_eq!(got, all);
}

// ---------------------------------------------------------------------------
// B · 列与 schema
// ---------------------------------------------------------------------------

/// `app` 依次摄取 `{"Level":"a"}` 与 `{"level":"b"}`（此刻前第 3 小时起点加 1 秒、加 2 秒），等该桶入湖完成。
fn case_fold_lake(f: &Fixture, s: &Server) -> i64 {
    let b = hours_ago(3);
    for (k, v, t) in [("Level", "a", 1), ("level", "b", 2)] {
        s.ingest_ok("app", &json!([{"_timestamp": b + t * 1_000_000, k: v}]));
    }
    f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    b
}

#[test]
fn cross_tier_case_fold_columns_by_field_id() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = case_fold_lake(&f, &s);
    let body = ok(&search(
        &s,
        "SELECT \"Level\", \"level\" FROM app ORDER BY _timestamp",
        hour(b),
        100,
    ));
    assert_eq!(hits(&body), [json!({"Level":"a"}), json!({"level":"b"})]);
}

#[test]
fn cross_tier_renamed_column_unknown() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = case_fold_lake(&f, &s);
    let meta = f.cold("app").current();
    let schema = meta["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["schema-id"] == meta["current-schema-id"])
        .unwrap()
        .clone();
    let names: Vec<&str> = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|x| x["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"level__2"), "{names:?}");
    search(&s, "SELECT \"level__2\" FROM app", hour(b), 100).assert_envelope(400, "unknown_column");
}

#[test]
fn cross_tier_missing_column_null() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = hours_ago(3);
    s.ingest_ok("app", &json!([{"_timestamp": b + 1_000_000, "msg": "old"}]));
    f.dataset("app").wait_handovers(&bucket_label(b), 1, WAIT);
    let now = now_us();
    s.ingest_ok(
        "app",
        &json!([{"_timestamp": now - 1_000_000, "msg": "new", "extra": 1}]),
    );
    let body = ok(&search(
        &s,
        "SELECT msg, extra FROM app ORDER BY _timestamp",
        (b, now + 60_000_000),
        100,
    ));
    assert_eq!(
        hits(&body),
        [json!({"msg":"old"}), json!({"msg":"new","extra":1})]
    );
}

// ---------------------------------------------------------------------------
// C · 扫描计数与剪枝
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_scan_files_both_tiers() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[DELETE_1S]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, true);
    let c = cold_parquet_count(&f);
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 100..105, 1);
    std::thread::sleep(Duration::from_secs(3));
    let ds = f.dataset("app");
    ds.wait_records(5, Duration::from_secs(15));
    let h = live_paths(&ds.bucket_now(&bucket_label(now - 5_000_000)).unwrap()).len();
    let body = ok(&search(&s, "SELECT * FROM app", (b, now + HOUR), 1000));
    assert_eq!(body["scan_files"], c + h);
}

/// `app` 中此刻前第 3 小时的 100 条已入湖完成、热层数据文件已物理删除。返回实例与该小时起点。
fn lake_purged(f: &Fixture) -> (Server, i64) {
    let s = f.start(&lake_cfg(&[DELETE_1S]));
    let b = hours_ago(3);
    lake_bucket(f, &s, b, 0..100, 1, true);
    (s, b)
}

#[test]
fn cross_tier_scan_bytes_cold() {
    let f = Fixture::new();
    let (s, b) = lake_purged(&f);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b), 1000));
    let entries = cold_live_entries(&f);
    let manifest_bytes: u64 = entries.iter().map(|e| e.file_size_in_bytes).sum();
    let cold = f.cold("app");
    let disk_bytes: u64 = entries
        .iter()
        .map(|e| std::fs::metadata(cold.path(&e.path)).unwrap().len())
        .sum();
    assert_eq!(body["scan_bytes"], manifest_bytes);
    assert_eq!(manifest_bytes, disk_bytes);
}

#[test]
fn cross_tier_scan_records_cold() {
    let f = Fixture::new();
    let (s, b) = lake_purged(&f);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b), 1000));
    assert_eq!(body["scan_records"], 100);
}

#[test]
fn cross_tier_scan_metadata_only_zero() {
    let f = Fixture::new();
    let (s, _) = lake_purged(&f);
    let body = ok(&search(&s, "SELECT * FROM app", hour(hours_ago(10)), 100));
    assert_eq!(
        (
            body["scan_files"].clone(),
            body["scan_bytes"].clone(),
            body["scan_records"].clone()
        ),
        (json!(0), json!(0), json!(0))
    );
}

#[test]
fn cross_tier_prune_time_cold() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[DELETE_1S]));
    let (b3, b30) = (hours_ago(3), hours_ago(30));
    lake_bucket(&f, &s, b30, 0..40, 1, true);
    lake_bucket(&f, &s, b3, 100..150, 1, true);
    let cold = f.cold("app");
    let meta = cold.current();
    let c: usize = ColdDir::lake_snapshots(&meta, &bucket_label(b3))
        .iter()
        .map(|snap| cold.added(&meta, snap).len())
        .sum();
    assert!(c > 0);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b3), 1000));
    assert_eq!(body["scan_files"], c);
}

// ---------------------------------------------------------------------------
// D · 冷层读取失败
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_cold_read_all_or_nothing() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    let s = start_env(&f, &lake_cfg(&[DELETE_1S]), &faults.envs());
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 100..105, 1);
    faults.set("get _iceberg/default/app/data/ 403\n");
    let r = search(&s, "SELECT * FROM app", (b, now + HOUR), 1000);
    assert!(!(200..300).contains(&r.status), "{}", r.text());
    assert!(r.json().get("hits").is_none(), "{}", r.text());
}

/// 冷层当前快照的清单列表所列、分区摘要中 `_timestamp_day` 的上下界都是 UTC 日 `day` 的清单（相对冷层表位置）。
fn manifests_of_day(f: &Fixture, day: i32) -> Vec<String> {
    let (cold, meta, current) = cold_current(f);
    let list_rel = ColdDir::rel(&meta, current["manifest-list"].as_str().unwrap());
    let list = ManifestList::parse_with_version(&cold.read(&list_rel), FormatVersion::V2)
        .expect("清单列表可解析");
    let as_day = |b: Option<&Vec<u8>>| {
        b.and_then(|b| <[u8; 4]>::try_from(b.as_slice()).ok())
            .map(i32::from_le_bytes)
    };
    list.entries()
        .iter()
        .filter(|m| {
            m.partitions
                .as_ref()
                .and_then(|p| p.first())
                .is_some_and(|p| {
                    as_day(p.lower_bound.as_deref()) == Some(day)
                        && as_day(p.upper_bound.as_deref()) == Some(day)
                })
        })
        .map(|m| ColdDir::rel(&meta, &m.manifest_path))
        .collect()
}

#[test]
fn cross_tier_unrelated_manifest_skipped() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    let s = start_env(&f, &lake_cfg(&[DELETE_1S]), &faults.envs());
    let (b50, b3) = (hours_ago(50), hours_ago(3));
    let (day50, day3) = (
        b50.div_euclid(86_400_000_000),
        b3.div_euclid(86_400_000_000),
    );
    assert_ne!(day50, day3);
    lake_bucket(&f, &s, b50, 0..10, 1, true);
    lake_bucket(&f, &s, b3, 100..120, 1, true);
    let first = manifests_of_day(&f, day50 as i32);
    assert!(!first.is_empty(), "第一组所在 UTC 日没有单独的清单");
    let rules: String = first
        .iter()
        .map(|m| format!("get _iceberg/default/app/{m} 403\n"))
        .collect();
    faults.set(&rules);
    let body = ok(&search(&s, "SELECT * FROM app", hour(b3), 1000));
    assert_eq!(body["total"], 20);
}

#[test]
fn cross_tier_cold_unavailable_503() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    let s = start_env(&f, &lake_cfg(&[]), &faults.envs());
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    faults.set("get _iceberg/default/app/metadata/ 403\n");
    search(&s, "SELECT * FROM app", hour(b), 1000).assert_envelope(503, "unavailable");
    faults.set("");
    assert_eq!(
        ok(&search(&s, "SELECT * FROM app", hour(b), 1000))["total"],
        100
    );
}

#[test]
fn cross_tier_cold_file_missing_500() {
    let f = Fixture::new();
    let s = f.start(&lake_cfg(&[]));
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    let entries = cold_live_entries(&f);
    let victim = &entries[0];
    std::fs::remove_file(f.cold("app").path(&victim.path)).unwrap();
    search(&s, "SELECT * FROM app", hour(b), 1000).assert_envelope(500, "internal");
}

/// 场景「在途查询读到已入湖并物理删除的热层文件时回 503」：返回实例与该小时起点。
fn moved_503(f: &Fixture) -> (Server, i64) {
    let gates = Gates::new(
        f,
        &[
            ("lake.after_digest_check", true),
            ("query.after_bind", true),
        ],
    );
    let s = start_env(f, &lake_cfg(&[DELETE_1S]), &[gates.env()]);
    let b = hours_ago(3);
    ingest_seq(&s, "app", b, 0..100, 1_000_000);
    gates.wait_held("lake.after_digest_check", &s);
    let q = query_async(&s, "SELECT * FROM app", hour(b));
    gates.wait_held("query.after_bind", &s);
    gates.release("lake.after_digest_check");
    let ds = f.dataset("app");
    let m = ds.wait_handovers(&bucket_label(b), 1, WAIT);
    wait_gone(&ds, &lake_deleted(&m));
    gates.release("query.after_bind");
    q.join().unwrap().assert_envelope(503, "unavailable");
    (s, b)
}

#[test]
fn cross_tier_moved_503() {
    let f = Fixture::new();
    moved_503(&f);
}

#[test]
fn cross_tier_moved_not_disabled() {
    let f = Fixture::new();
    let (s, b) = moved_503(&f);
    assert_eq!(
        ok(&search(&s, "SELECT * FROM app", hour(b), 1000))["total"],
        100
    );
    let metrics = s.get("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains("stream=\"app\"")),
        "{metrics}"
    );
    assert!(
        !s.json_lines()
            .iter()
            .any(|l| l["event"] == "dataset_disabled" && l["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

/// 场景「在途查询读到已被过期并清理的清单列表时回 503」：返回实例与该小时起点。冷层合并停在
/// `cold_compaction.before_run`（场景不涉冷层合并；两个桶落在已够格的日分区时也不合并）。
fn moved_manifest_list_503(f: &Fixture) -> (Server, i64) {
    let gates = Gates::new(
        f,
        &[
            ("query.after_cold_version", false),
            ("cold_compaction.before_run", true),
        ],
    );
    let cfg = lake_cfg(&[
        DELETE_1S,
        ("cold_retention.snapshots", Some("1")),
        ("cold_retention.age", Some("\"1s\"")),
        ("cold_retention.check_interval", Some("\"5s\"")),
        ("hot_fsck.interval", Some("\"5s\"")),
        ("hot_fsck.min_orphan_age", Some("\"1s\"")),
    ]);
    let s = start_env(f, &cfg, &[gates.env()]);
    let b = hours_ago(3);
    lake_bucket(f, &s, b, 0..100, 1, false);
    let (_, meta, s1) = cold_current(f);
    let list = ColdDir::rel(&meta, s1["manifest-list"].as_str().unwrap());

    gates.arm("query.after_cold_version");
    let q = query_async(&s, "SELECT * FROM app", hour(b));
    gates.wait_held("query.after_cold_version", &s);
    lake_bucket(f, &s, hours_ago(4), 100..110, 1, false);
    let cold = f.cold("app");
    let deadline = Instant::now() + Duration::from_secs(180);
    while cold.exists(&list) {
        assert!(
            Instant::now() < deadline,
            "S1 的清单列表 3 分钟内没有被删除：{:#?}",
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    gates.release("query.after_cold_version");
    q.join().unwrap().assert_envelope(503, "unavailable");
    (s, b)
}

#[test]
fn cross_tier_moved_manifest_list_503() {
    let f = Fixture::new();
    moved_manifest_list_503(&f);
}

#[test]
fn cross_tier_moved_manifest_list_not_disabled() {
    let f = Fixture::new();
    let (s, b) = moved_manifest_list_503(&f);
    assert_eq!(
        ok(&search(&s, "SELECT * FROM app", hour(b), 1000))["total"],
        100
    );
    let metrics = s.get("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains("stream=\"app\"")),
        "{metrics}"
    );
    assert!(
        !s.json_lines()
            .iter()
            .any(|l| l["event"] == "dataset_disabled" && l["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

// ---------------------------------------------------------------------------
// E · 停用的数据集
// ---------------------------------------------------------------------------

#[test]
fn cross_tier_disabled_no_cold_reads() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    let cfg = lake_cfg(&[]);
    let mut s = start_env(&f, &cfg, &faults.envs());
    let b = hours_ago(3);
    lake_bucket(&f, &s, b, 0..100, 1, false);
    // 另有一个带存活文件的桶；停机后删去它的桶 manifest，重启（不留本进程的缓存）后一次查询读到即进入 manifest 不可读情形
    let now = now_us();
    ingest_seq(&s, "app", now - 5_000_000, 100..105, 1);
    let ds = f.dataset("app");
    ds.wait_records(5, Duration::from_secs(15));
    stop(&mut s);
    drop(s);
    let head = ds.head();
    let open = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["bucket"] == bucket_label(now - 5_000_000))
        .unwrap()["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    ds.remove(&open);
    let s = start_env(&f, &cfg, &faults.envs());
    search(&s, "SELECT * FROM app", (now - HOUR, now + HOUR), 1000)
        .assert_envelope(500, "internal");

    faults.set("get,head,list _iceberg/ 403\n");
    faults.clear_log();
    search(&s, "SELECT * FROM app", hour(b), 1000).assert_envelope(500, "internal");
    let cold_reads: Vec<String> = faults
        .requests()
        .into_iter()
        .filter(|r| {
            let (op, key) = r.split_once(' ').unwrap();
            matches!(op, "get" | "head" | "list") && key.starts_with("_iceberg/default/app/")
        })
        .collect();
    assert!(cold_reads.is_empty(), "{cold_reads:?}");
}
