//! REQ-AMB-STO-TTL 的系统测试：以子进程运行 `amberlog`，对象存储用 `file://` 临时目录；
//! 测试直接读热层快照、桶 manifest 与冷层变更段、冷层元数据与清单，核对 `_search` 与 `/metrics`。
//!
//! - 各实例以 `retention.check_interval` 为 `10s` 启动（场景约定），`lifecycle.seal_grace` 与 `lifecycle.lake_watermark` 按场景。
//! - 「冷层写入失败」以对象存储注入（`TESTBED_FAULT_STORE`）对 `_iceberg/` 之下的写入回 403；暂停点经
//!   `TESTBED_FAULT_GATES`，每个步骤一个控制文件，可在运行中再写 `hold` 启用。
//! - 以「此刻前 5 小时 30 分」、`retention.period` 为 `6h` 起步的场景，入湖完成之后以测试专用时钟偏移
//!   `TESTBED_CLOCK_OFFSET` 为 1 小时重启实例，使记录在重启后已到期；时限按块条款从重启后开始受理请求
//!   的时刻起算，场景中的「此刻」与 `cutoff` 都按被测进程的时钟（真实时钟加偏移）计。
//! - 「等待 TTL」按块的定义轮询到断言成立即止，上限为时限（2 分钟）加 5 分钟。

// 多数场景要用测试专用的注入（对象存储注入、暂停点、时钟偏移），只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::api::{counter_file, ingest_ok, now_us};
use common::lake::ColdDir;
use common::store::{Ds, HOUR, bucket_label, hour_of, stop};
use common::{BEARER, Fixture, Server, wait_file};
use iceberg::spec::{DataContentType, FormatVersion, Manifest, ManifestContentType, ManifestList};
use serde_json::{Value, json};

const MINUTE: i64 = 60_000_000;
/// 「等待 TTL」的上限：时限 2 分钟加 5 分钟。
const TTL_WAIT: Duration = Duration::from_secs(7 * 60);
/// 等入湖完成的上限。
const LAKE_WAIT: Duration = Duration::from_secs(130);
/// 场景中的「等待 3 分钟」。
const THREE_MIN: Duration = Duration::from_secs(180);
/// 数据集建立之后，冷层合并必已对它读过一次冷层表的时长（每 30 秒一轮，取两轮）。
const COLD_COMPACTION_SETTLED: Duration = Duration::from_secs(60);
/// 重启时的时钟偏移（秒）。
const OFFSET_S: i64 = 3600;
const COLD_WRITE_FAILS: &str = "write _iceberg/ 403";

// ---------------------------------------------------------------------------
// 启动
// ---------------------------------------------------------------------------

/// 一次启动的配置与注入。
#[derive(Default, Clone)]
struct Run {
    /// `retention.period`；`None` 为不配置。
    retention: Option<String>,
    seal_grace: Option<String>,
    lake_watermark: Option<String>,
    extra: Vec<(&'static str, String)>,
    /// 启用的暂停点（其控制文件写为 `hold` 的才停）。
    gates: Vec<&'static str>,
    /// 崩溃暂停点。
    pause: Option<&'static str>,
    /// 时钟偏移（秒）。
    offset_s: i64,
}

impl Run {
    /// `retention.period` 为 `retention`、`lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`。
    fn ttl(retention: &str) -> Run {
        Run {
            retention: Some(retention.to_owned()),
            seal_grace: Some("1s".into()),
            lake_watermark: Some("2s".into()),
            ..Run::default()
        }
    }

    fn gates(mut self, gates: &[&'static str]) -> Run {
        self.gates = gates.to_vec();
        self
    }

    fn offset(mut self, s: i64) -> Run {
        self.offset_s = s;
        self
    }

    fn with(mut self, key: &'static str, toml: &str) -> Run {
        self.extra.push((key, toml.to_owned()));
        self
    }
}

/// 一个测试的对象存储注入与暂停点控制文件（跨重启共用）。
struct Ctl {
    root: PathBuf,
}

impl Ctl {
    fn new(f: &Fixture) -> Ctl {
        let root = f.root().join("ttl-ctl");
        std::fs::create_dir_all(root.join("gates")).unwrap();
        std::fs::write(root.join("store"), "").unwrap();
        Ctl { root }
    }

    /// 对象存储注入规则（每行一条，见 `amberlog_storage::fault::store`）；运行中改写即生效。
    fn faults(&self, rules: &str) {
        std::fs::write(self.root.join("store"), rules).unwrap();
    }

    fn gate_file(&self, step: &str) -> PathBuf {
        self.root.join("gates").join(step)
    }

    /// 启用暂停点 `step`：下一个走到它的调用停住。
    fn hold(&self, step: &str) {
        std::fs::write(self.gate_file(step), "hold").unwrap();
    }

    fn held(&self, step: &str) -> bool {
        std::fs::read_to_string(self.gate_file(step)).is_ok_and(|s| s.trim() == "held")
    }

    fn wait_held(&self, step: &str, server: &Server, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.held(step) {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有走到暂停点 {step}：{:#?}",
                limit.as_secs(),
                server.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    fn release(&self, step: &str) {
        std::fs::write(self.gate_file(step), "go").unwrap();
    }

    fn marker(&self) -> PathBuf {
        self.root.join("pause-marker")
    }

    fn start(&self, f: &Fixture, run: &Run) -> Server {
        Server::start(self.command(f, run))
    }

    fn command(&self, f: &Fixture, run: &Run) -> std::process::Command {
        let quoted = |v: &str| format!("\"{v}\"");
        let mut owned: Vec<(&str, String)> = vec![
            ("flush.interval", quoted("1s")),
            ("retention.check_interval", quoted("10s")),
        ];
        if let Some(r) = &run.retention {
            owned.push(("retention.period", quoted(r)));
        }
        if let Some(v) = &run.seal_grace {
            owned.push(("lifecycle.seal_grace", quoted(v)));
        }
        if let Some(v) = &run.lake_watermark {
            owned.push(("lifecycle.lake_watermark", quoted(v)));
        }
        owned.extend(run.extra.iter().cloned());
        let kv: Vec<common::Kv> = owned.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect();
        let mut cmd = f.serve(&kv);
        cmd.env("TESTBED_FAULT_STORE", self.root.join("store"));
        if !run.gates.is_empty() {
            let gates: Vec<String> = run
                .gates
                .iter()
                .map(|g| format!("{g}={}", self.gate_file(g).display()))
                .collect();
            cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
        }
        if let Some(step) = run.pause {
            let _ = std::fs::remove_file(self.marker());
            cmd.env("TESTBED_FAULT_PAUSE", step)
                .env("TESTBED_FAULT_MARKER", self.marker());
        }
        if run.offset_s != 0 {
            cmd.env("TESTBED_CLOCK_OFFSET", run.offset_s.to_string());
        }
        cmd
    }
}

// ---------------------------------------------------------------------------
// 数据
// ---------------------------------------------------------------------------

/// 此刻前第 `n` 小时所在小时桶的起点。
fn hours_ago(n: i64) -> i64 {
    hour_of(now_us()) - n * HOUR
}

/// 以一个请求摄取 `_timestamp` 依次为 `ts` 的记录，断言全部受理。
fn ingest(server: &Server, ts: &[i64]) {
    let records: Vec<String> = ts
        .iter()
        .enumerate()
        .map(|(i, t)| format!(r#"{{"_timestamp":{t},"seq":{i}}}"#))
        .collect();
    let r = ingest_ok(server, "app", &format!("[{}]", records.join(",")));
    assert_eq!(r["status"][0]["successful"], ts.len(), "{r}");
}

/// 自 `start` 起每隔 1 毫秒的 `n` 个时间戳。
fn series(start: i64, n: usize) -> Vec<i64> {
    (0..n as i64).map(|i| start + i * 1000).collect()
}

/// 小时桶 `start` 内的 `n` 个时间戳（自起点后 1 分钟起）。
fn in_hour(start: i64, n: usize) -> Vec<i64> {
    series(start + MINUTE, n)
}

/// `_search` 以 `[start, end)` 为时间范围执行 `SELECT * FROM app` 的 `total`（断言 200）。
fn total_in(server: &Server, start: i64, end: i64) -> u64 {
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": end, "size": 1000}});
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["total"].as_u64().unwrap()
}

fn paths(entries: &Value) -> BTreeSet<String> {
    entries
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["path"].as_str().unwrap().to_owned())
        .collect()
}

/// 桶 manifest `m` 中 `reason` 为 `reason` 的留痕。
fn deletions(m: &Value, reason: &str) -> Vec<Value> {
    m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == reason)
        .cloned()
        .collect()
}

/// 当前快照中桶 `label` 的 `files` 路径（桶不在快照中为空）。
fn live(ds: &Ds, label: &str) -> BTreeSet<String> {
    ds.bucket_now(label)
        .map(|m| paths(&m["files"]))
        .unwrap_or_default()
}

/// 当前快照经 `buckets` 可达的全部桶 manifest 中的 `ttl` 留痕。
fn hot_ttl(ds: &Ds) -> Vec<Value> {
    let head = ds.head();
    ds.bucket_manifests(&head)
        .iter()
        .flat_map(|m| deletions(m, "ttl"))
        .collect()
}

/// 当前快照经 `cold_deletion_segment` 沿 `previous` 可达的冷层变更段的全部 `deletions` 元素（自最新段起）。
fn cold_changes(ds: &Ds) -> Vec<Value> {
    let mut out = Vec::new();
    let mut next = ds.head()["cold_deletion_segment"].clone();
    while !next.is_null() {
        let seg = ds.json(next["path"].as_str().unwrap());
        out.extend(seg["deletions"].as_array().unwrap().iter().cloned());
        next = seg["previous"].clone();
    }
    out
}

/// 当前快照的完整删除台账来源中的桶 manifest：`buckets` 所引用的与全部留痕归档段所引用的。
fn ledger_manifests(ds: &Ds) -> Vec<Value> {
    let head = ds.head();
    let mut out = ds.bucket_manifests(&head);
    let mut next = head["retired_segment"].clone();
    while !next.is_null() {
        let seg = ds.json(next["path"].as_str().unwrap());
        for b in seg["buckets"].as_array().unwrap() {
            out.push(ds.restore(b["manifest"].as_str().unwrap()));
        }
        next = seg["previous"].clone();
    }
    out
}

/// 冷层当前快照的存活数据文件（相对表位置）；尚无冷层表为空。
fn cold_live(f: &Fixture) -> BTreeSet<String> {
    let cold = f.cold("app");
    if cold.version().is_none() {
        return BTreeSet::new();
    }
    let meta = cold.current();
    let Some(cur) = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|s| s["snapshot-id"] == meta["current-snapshot-id"])
    else {
        return BTreeSet::new();
    };
    cold.entries(&meta, &cur)
        .into_iter()
        .filter(|e| !e.deleted)
        .map(|e| e.path)
        .collect()
}

/// 冷层当前版本中摘要 `amberlog.operation` 为 `ttl` 的快照。
fn ttl_snapshots(f: &Fixture) -> Vec<Value> {
    let cold = f.cold("app");
    if cold.version().is_none() {
        return Vec::new();
    }
    ColdDir::snapshots(&cold.current())
        .into_iter()
        .filter(|s| s["summary"]["amberlog.operation"] == "ttl")
        .collect()
}

/// `ttl` 冷层提交 `snap` 标为已删除的数据文件。
fn removed_by(f: &Fixture, snap: &Value) -> BTreeSet<String> {
    let cold = f.cold("app");
    let meta = cold.current();
    let id = snap["snapshot-id"].as_i64().unwrap();
    cold.entries(&meta, snap)
        .into_iter()
        .filter(|e| e.deleted && e.snapshot_id == id)
        .map(|e| e.path)
        .collect()
}

/// 桶 manifest `m` 的交接记录所记的冷层数据文件（路径 → 元素）。
fn handed_files(m: &Value) -> BTreeMap<String, Value> {
    m["handovers"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|h| h["files"].as_array().unwrap().iter())
        .map(|x| (x["path"].as_str().unwrap().to_owned(), x.clone()))
        .collect()
}

fn wait_until(limit: Duration, what: &str, mut ok: impl FnMut() -> bool) {
    let deadline = Instant::now() + limit;
    while !ok() {
        assert!(
            Instant::now() < deadline,
            "{} 秒内未{what}",
            limit.as_secs()
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// `/metrics` 中是否有 `stream="app"` 的 `amberlog_dataset_disabled` 序列。
fn app_disabled(server: &Server) -> bool {
    let r = server.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
        .lines()
        .any(|l| l.starts_with("amberlog_dataset_disabled{") && l.contains("stream=\"app\""))
}

/// 被测进程的「此刻」（真实时钟加偏移）。
fn proc_now(offset_s: i64) -> i64 {
    now_us() + offset_s * 1_000_000
}

/// 搭建「冷层文件按时间戳上界判定到期」的实例与数据：保留期 6h，第一组 10 条在此刻前 5 小时 30 分、第二组 10 条在此刻前
/// 第 1 小时，两个桶都入湖完成后以时钟偏移重启（第一组随之到期，第二组仍未到期）。返回重启后的实例与两组所在桶的标签、
/// 两组的时间戳与交接记录所记的冷层文件。
struct ColdCase {
    server: Server,
    first: Vec<i64>,
    second: Vec<i64>,
    first_files: BTreeMap<String, Value>,
    second_files: BTreeSet<String>,
}

fn cold_case(f: &Fixture, ctl: &Ctl, restart: Run) -> ColdCase {
    let mut s = ctl.start(f, &Run::ttl("6h"));
    let ds = f.dataset("app");
    let first = series(now_us() - 5 * HOUR - 30 * MINUTE, 10);
    let second = in_hour(hours_ago(1), 10);
    ingest(&s, &first);
    ingest(&s, &second);
    let m1 = ds.wait_handovers(&bucket_label(first[0]), 1, LAKE_WAIT);
    let m2 = ds.wait_handovers(&bucket_label(second[0]), 1, LAKE_WAIT);
    stop(&mut s);
    let server = ctl.start(f, &restart);
    ColdCase {
        server,
        first,
        second,
        first_files: handed_files(&m1),
        second_files: handed_files(&m2).into_keys().collect(),
    }
}

/// 重启：保留期 6h、时钟偏移 1 小时。
fn shifted() -> Run {
    Run::ttl("6h").offset(OFFSET_S)
}

/// 等到第一组的冷层文件都离开冷层存活文件集。
fn wait_first_gone(f: &Fixture, c: &ColdCase) {
    wait_until(TTL_WAIT, "见第一组冷层文件离开存活集", || {
        let live = cold_live(f);
        c.first_files.keys().all(|p| !live.contains(p))
    });
}

// ---------------------------------------------------------------------------
// A · 适用范围与到期判据
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_expired_deleted_and_traced() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let run = Run::ttl("6h").with("storage.delete_delay", "\"1s\"");
    let mut s = ctl.start(&f, &run);
    let ds = f.dataset("app");
    let ts = series(now_us() - 5 * HOUR - 30 * MINUTE, 100);
    ingest(&s, &ts);
    let m = ds.wait_handovers(&bucket_label(ts[0]), 1, LAKE_WAIT);
    let files = handed_files(&m);
    stop(&mut s);

    let s = ctl.start(&f, &run.offset(OFFSET_S));
    wait_until(TTL_WAIT, "删除已入湖的到期数据", || {
        total_in(&s, ts[0], ts[99] + 1) == 0
            && ds.head()["cold_deletion_segment"] != Value::Null
            && {
                let ttl: BTreeSet<String> = cold_changes(&ds)
                    .iter()
                    .filter(|d| d["reason"] == "ttl")
                    .map(|d| d["path"].as_str().unwrap().to_owned())
                    .collect();
                files.keys().all(|p| ttl.contains(p))
            }
    });
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_no_start_on_shutdown() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    ctl.hold("ttl.before_run");
    let mut s = ctl.start(&f, &Run::ttl("3h").gates(&["ttl.before_run"]));
    ctl.wait_held("ttl.before_run", &s, Duration::from_secs(30));
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(6), 10);
    ingest(&s, &ts);
    ds.wait_records(10, Duration::from_secs(15));
    std::thread::sleep(Duration::from_secs(2));
    let label = bucket_label(ts[0]);
    let files = live(&ds, &label);
    assert_eq!(files.len(), 1);

    s.signal("TERM");
    ctl.release("ttl.before_run");
    assert_eq!(s.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    assert_eq!(live(&ds, &label), files);
    assert!(deletions(&ds.bucket_now(&label).unwrap(), "ttl").is_empty());
}

#[test]
fn ttl_unconfigured_keeps_data() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let run = Run {
        retention: None,
        ..Run::ttl("unused")
    };
    let s = ctl.start(&f, &run);
    let ds = f.dataset("app");
    let ts = series(now_us() - 400 * 24 * HOUR, 100);
    ingest(&s, &ts);
    ds.wait_handovers(&bucket_label(ts[0]), 1, LAKE_WAIT);
    std::thread::sleep(THREE_MIN);
    assert_eq!(total_in(&s, ts[0], ts[99] + 1), 100);
    assert!(ds.keys_under("manifest/cold-deletions/").is_empty());
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_hot_expired_by_max_timestamp() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let ds = f.dataset("app");
    let old = in_hour(hours_ago(6), 10);
    let fresh = series(now_us(), 10);
    ingest(&s, &old);
    ingest(&s, &fresh);
    ds.wait_records(20, Duration::from_secs(15));
    let label = bucket_label(old[0]);
    let files = ds
        .bucket_manifests(&ds.snapshot(ds.wait_current(Duration::from_secs(5))))
        .into_iter()
        .find(|m| m["bucket"] == label.as_str())
        .map(|m| paths(&m["files"]))
        .unwrap_or_default();

    wait_until(TTL_WAIT, "见第 6 小时那个桶的 files 为空", || {
        live(&ds, &label).is_empty()
    });
    let m = ds.bucket_now(&label).unwrap();
    let traced: BTreeSet<String> = deletions(&m, "ttl")
        .iter()
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    assert!(files.iter().all(|p| traced.contains(p)), "{m}");
    assert_eq!(total_in(&s, old[0], fresh[9] + 1), 10);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_expired_by_upper_bound() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let c = cold_case(&f, &ctl, shifted());
    wait_first_gone(&f, &c);
    let live = cold_live(&f);
    assert!(c.second_files.iter().all(|p| live.contains(p)));
    assert_eq!(total_in(&c.server, c.first[0], c.second[9] + 1), 10);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_partial_file_kept() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    // H 早于此刻 72 小时以上：保留期大于默认的入湖水位 72h，满足启动约束
    let h = hours_ago(80);
    let (r1, r2) = (h + MINUTE, h + 59 * MINUTE);
    // cutoff 此刻落在 H 起点加 30 分钟，约 29 分钟后才越过 r2
    let retention_s = (now_us() - (h + 30 * MINUTE)) / 1_000_000;
    let run = Run {
        retention: Some(format!("{retention_s}s")),
        ..Run::default()
    };
    let s = ctl.start(&f, &run);
    let ds = f.dataset("app");
    ingest(&s, &[r1, r2]);
    ds.wait_records(2, Duration::from_secs(15));
    let label = bucket_label(h);
    let file = live(&ds, &label);
    assert_eq!(file.len(), 1);
    // 让 TTL 至少执行两次
    std::thread::sleep(Duration::from_secs(25));
    assert_eq!(total_in(&s, h, h + HOUR), 2);
    assert_eq!(live(&ds, &label), file);
    assert!(
        now_us() - retention_s * 1_000_000 < r2,
        "测试超时：cutoff 已越过 r2"
    );
}

// ---------------------------------------------------------------------------
// B · 热层
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_hot_deadline() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("3h"));
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(6), 10);
    ingest(&s, &ts);
    ds.wait_records(10, Duration::from_secs(15));
    let label = bucket_label(ts[0]);
    let entry = ds.bucket_now(&label).unwrap()["files"][0].clone();
    let path = entry["path"].as_str().unwrap().to_owned();
    // t：该文件所在提交（快照 added_in）成功的时刻
    let t = ds.snapshot(entry["added_in"].as_u64().unwrap())["created_at"]
        .as_i64()
        .unwrap();
    let deadline = t + 2 * MINUTE;
    loop {
        if !live(&ds, &label).contains(&path) {
            break;
        }
        assert!(now_us() < deadline, "t 加 2 分钟之前未移出 {path}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_hot_one_commit() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let ds = f.dataset("app");
    let mut ts = Vec::new();
    for n in [6, 7, 8] {
        ts.extend(in_hour(hours_ago(n), 10));
    }
    ingest(&s, &ts);
    ds.wait_records(30, Duration::from_secs(15));
    let labels: Vec<String> = [6, 7, 8].map(|n| bucket_label(hours_ago(n))).to_vec();
    wait_until(TTL_WAIT, "见三个桶的 files 都为空", || {
        labels.iter().all(|l| live(&ds, l).is_empty())
    });
    let removed_in: BTreeSet<u64> = labels
        .iter()
        .flat_map(|l| deletions(&ds.bucket_now(l).unwrap(), "ttl"))
        .map(|d| d["removed_in"].as_u64().unwrap())
        .collect();
    assert_eq!(removed_in.len(), 1, "{removed_in:?}");
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_hot_trace_added_in() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    // TTL 先停在执行之前，记下 t0 再放行，使删除它的那次执行在 t0 之后开始
    ctl.hold("ttl.before_run");
    let s = ctl.start(&f, &Run::ttl("3h").gates(&["ttl.before_run"]));
    let ds = f.dataset("app");
    let old = in_hour(hours_ago(6), 10);
    ingest(&s, &old);
    ingest(&s, &series(now_us(), 10));
    ds.wait_records(20, Duration::from_secs(15));
    let label = bucket_label(old[0]);
    let entry = ds.bucket_now(&label).unwrap()["files"][0].clone();
    let t0 = now_us();
    ctl.release("ttl.before_run");
    wait_until(TTL_WAIT, "见该桶的 files 为空", || {
        live(&ds, &label).is_empty()
    });
    let t1 = now_us();
    let m = ds.bucket_now(&label).unwrap();
    let d = deletions(&m, "ttl")
        .into_iter()
        .find(|d| d["path"] == entry["path"])
        .unwrap_or_else(|| panic!("{m}"));
    assert_eq!(d["added_in"], entry["added_in"]);
    assert_eq!(d["content_digest"], entry["content_digest"]);
    assert_eq!(d["max_timestamp"], entry["max_timestamp"]);
    let cutoff = d["cutoff"].as_i64().unwrap();
    assert!(cutoff > entry["max_timestamp"].as_i64().unwrap());
    assert!(t0 - 3 * HOUR <= cutoff && cutoff <= t1 - 3 * HOUR, "{d}");
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_not_wait_lake() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(6), 10);
    ingest(&s, &ts);
    ds.wait_records(10, Duration::from_secs(15));
    let label = bucket_label(ts[0]);
    wait_until(TTL_WAIT, "见该桶的 files 为空", || {
        live(&ds, &label).is_empty()
    });
    assert!(
        f.cold("app").keys().iter().all(|k| !k.starts_with("data/")),
        "{:?}",
        f.cold("app").keys()
    );
}

// ---------------------------------------------------------------------------
// C · 冷层
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_deadline() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let mut s = ctl.start(&f, &Run::ttl("6h"));
    let ds = f.dataset("app");
    let ts = series(now_us() - 5 * HOUR - 30 * MINUTE, 10);
    ingest(&s, &ts);
    let files = handed_files(&ds.wait_handovers(&bucket_label(ts[0]), 1, LAKE_WAIT));
    stop(&mut s);
    // 重启后被测进程的时钟已越过到期时刻 e：时限从开始受理请求起算
    let s = ctl.start(&f, &shifted());
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let live = cold_live(&f);
        if files.keys().all(|p| !live.contains(p)) {
            break;
        }
        assert!(Instant::now() < deadline, "2 分钟内冷层文件未标为已删除");
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_commit_operation() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let c = cold_case(&f, &ctl, shifted());
    wait_first_gone(&f, &c);
    let cold = f.cold("app");
    let meta = cold.current();
    // 第一组各文件被标为已删除的那次提交
    let mut by: BTreeSet<i64> = BTreeSet::new();
    for snap in ColdDir::snapshots(&meta) {
        let id = snap["snapshot-id"].as_i64().unwrap();
        for e in cold.entries(&meta, &snap) {
            if e.deleted && e.snapshot_id == id && c.first_files.contains_key(&e.path) {
                by.insert(id);
            }
        }
    }
    assert_eq!(by.len(), 1, "{by:?}");
    let id = *by.first().unwrap();
    let snap = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == id)
        .unwrap();
    assert_eq!(snap["summary"]["amberlog.operation"], "ttl", "{snap}");
    let removed = removed_by(&f, &snap);
    assert!(c.first_files.keys().all(|p| removed.contains(p)));
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_commit_records_cutoff() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.hold("ttl.before_run");
    let c = cold_case(&f, &ctl, shifted().gates(&["ttl.before_run"]));
    let ds = f.dataset("app");
    ctl.wait_held("ttl.before_run", &c.server, Duration::from_secs(30));
    let t0 = proc_now(OFFSET_S);
    ctl.release("ttl.before_run");
    wait_until(TTL_WAIT, "见 cold_deletion_segment 不为 null", || {
        ds.head()["cold_deletion_segment"] != Value::Null
    });
    let t1 = proc_now(OFFSET_S);
    let snaps = ttl_snapshots(&f);
    assert_eq!(snaps.len(), 1, "{snaps:?}");
    let x: i64 = snaps[0]["summary"]["amberlog.ttl_cutoff"]
        .as_str()
        .expect("amberlog.ttl_cutoff")
        .parse()
        .expect("十进制整数");
    assert!(t0 - 6 * HOUR <= x && x <= t1 - 6 * HOUR, "{x}");
    let removed = removed_by(&f, &snaps[0]);
    let id = snaps[0]["snapshot-id"].as_i64().unwrap();
    for d in cold_changes(&ds) {
        if d["cold_removed_in"] == id && removed.contains(d["path"].as_str().unwrap()) {
            assert_eq!(d["cutoff"], x, "{d}");
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_no_row_deletes() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let c = cold_case(&f, &ctl, shifted());
    wait_first_gone(&f, &c);
    let cold = f.cold("app");
    let meta = cold.current();
    let cur = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|s| s["snapshot-id"] == meta["current-snapshot-id"])
        .unwrap();
    let list_rel = ColdDir::rel(&meta, cur["manifest-list"].as_str().unwrap());
    let list = ManifestList::parse_with_version(&cold.read(&list_rel), FormatVersion::V2).unwrap();
    for mf in list.entries() {
        assert_eq!(
            mf.content,
            ManifestContentType::Data,
            "{}",
            mf.manifest_path
        );
        let m = Manifest::parse_avro(&cold.read(&ColdDir::rel(&meta, &mf.manifest_path))).unwrap();
        for e in m.entries() {
            assert_eq!(e.content_type(), DataContentType::Data, "{}", e.file_path());
        }
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_deletion_segment() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let c = cold_case(&f, &ctl, shifted());
    let ds = f.dataset("app");
    wait_until(TTL_WAIT, "见 cold_deletion_segment 不为 null", || {
        ds.head()["cold_deletion_segment"] != Value::Null
    });
    let snaps = ttl_snapshots(&f);
    assert_eq!(snaps.len(), 1);
    let id = snaps[0]["snapshot-id"].as_i64().unwrap();
    let changes = cold_changes(&ds);
    for (path, rec) in &c.first_files {
        let d = changes
            .iter()
            .find(|d| d["path"] == path.as_str())
            .unwrap_or_else(|| panic!("冷层变更段中没有 {path}：{changes:?}"));
        assert_eq!(d["content_digest"], rec["content_digest"]);
        assert_eq!(d["reason"], "ttl");
        assert_eq!(d["cold_removed_in"], id);
        assert_eq!(d["max_timestamp"], rec["max_timestamp"]);
        assert!(d["cutoff"].as_i64().unwrap() > rec["max_timestamp"].as_i64().unwrap());
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_summary_updated() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.hold("ttl.before_run");
    let c = cold_case(&f, &ctl, shifted().gates(&["ttl.before_run"]));
    let ds = f.dataset("app");
    ctl.wait_held("ttl.before_run", &c.server, Duration::from_secs(30));
    assert_eq!(ds.head()["summary"]["cold"]["record_count"], 20);
    ctl.release("ttl.before_run");
    wait_until(TTL_WAIT, "见 cold_deletion_segment 不为 null", || {
        ds.head()["cold_deletion_segment"] != Value::Null
    });
    let head = ds.head();
    assert_eq!(head["summary"]["cold"]["record_count"], 10, "{head}");
    assert_eq!(
        head["summary"]["cold"]["time_min"],
        *c.second.iter().min().unwrap()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_trace_recover_after_crash() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let mut crash = shifted();
    crash.pause = Some("ttl.after_cold_commit");
    let mut c = cold_case(&f, &ctl, crash);
    let ds = f.dataset("app");
    assert!(
        wait_file(&ctl.marker(), TTL_WAIT),
        "未走到 ttl.after_cold_commit：{:#?}",
        c.server.stderr_lines()
    );
    c.server.sigkill();
    let snaps = ttl_snapshots(&f);
    assert_eq!(snaps.len(), 1);
    let id = snaps[0]["snapshot-id"].as_i64().unwrap();
    let removed = removed_by(&f, &snaps[0]);
    assert!(!removed.is_empty());
    let x: i64 = snaps[0]["summary"]["amberlog.ttl_cutoff"]
        .as_str()
        .unwrap()
        .parse()
        .unwrap();

    let s = ctl.start(&f, &shifted());
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    wait_until(Duration::from_secs(130), "补写冷层变更段", || {
        let changes = cold_changes(&ds);
        removed.iter().all(|p| {
            changes
                .iter()
                .any(|d| d["path"] == p.as_str() && d["cold_removed_in"] == id)
        })
    });
    let changes = cold_changes(&ds);
    for p in &removed {
        let mine: Vec<&Value> = changes
            .iter()
            .filter(|d| d["path"] == p.as_str() && d["reason"] == "ttl")
            .collect();
        assert_eq!(mine.len(), 1, "{p}：{mine:?}");
        assert_eq!(mine[0]["cutoff"], x);
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_trace_retry_before_next_run() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults("write default/app/manifest/cold-deletions/ 403");
    let c = cold_case(&f, &ctl, shifted());
    let ds = f.dataset("app");
    wait_until(TTL_WAIT, "见 ttl 冷层提交", || {
        !ttl_snapshots(&f).is_empty()
    });
    let c1 = ttl_snapshots(&f)[0].clone();
    let c1_id = c1["snapshot-id"].as_i64().unwrap();
    let c1_removed = removed_by(&f, &c1);

    let r = proc_now(OFFSET_S) - 10 * HOUR;
    ingest(&c.server, &[r]);
    let label = bucket_label(r);
    wait_until(Duration::from_secs(15), "见 r 提交", || {
        !live(&ds, &label).is_empty()
    });
    let r_hot = live(&ds, &label);
    std::thread::sleep(THREE_MIN);
    assert_eq!(total_in(&c.server, r, r + 1), 1);

    ctl.faults("");
    // r 所在数据文件：热层文件，或其桶入湖后交接记录所记的冷层文件
    let r_trace = |ds: &Ds| -> Option<Value> {
        let manifests = ledger_manifests(ds);
        let mut mine = r_hot.clone();
        for m in manifests.iter().filter(|m| m["bucket"] == label.as_str()) {
            mine.extend(handed_files(m).into_keys());
        }
        manifests
            .iter()
            .flat_map(|m| deletions(m, "ttl"))
            .chain(
                cold_changes(ds)
                    .into_iter()
                    .filter(|d| d["reason"] == "ttl"),
            )
            .find(|d| mine.contains(d["path"].as_str().unwrap()))
    };
    wait_until(TTL_WAIT, "见 r 所在数据文件的 ttl 留痕", || {
        r_trace(&ds).is_some()
    });
    let r_removed_in = r_trace(&ds).unwrap()["removed_in"].as_u64().unwrap();
    let changes = cold_changes(&ds);
    for p in &c1_removed {
        let mine: Vec<&Value> = changes
            .iter()
            .filter(|d| {
                d["path"] == p.as_str() && d["reason"] == "ttl" && d["cold_removed_in"] == c1_id
            })
            .collect();
        assert_eq!(mine.len(), 1, "{p}：{changes:?}");
        assert!(mine[0]["removed_in"].as_u64().unwrap() < r_removed_in);
    }
}

// ---------------------------------------------------------------------------
// D · 一次执行的提交
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_no_empty_hot_commit() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("6h"));
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(3), 10);
    ingest(&s, &ts);
    ds.wait_records(10, Duration::from_secs(15));
    std::thread::sleep(Duration::from_secs(3));
    let before = ds.current().unwrap();
    std::thread::sleep(THREE_MIN);
    assert_eq!(ds.current(), Some(before));
    assert!(deletions(&ds.bucket_now(&bucket_label(ts[0])).unwrap(), "ttl").is_empty());
}

#[test]
fn ttl_no_empty_cold_commit() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let s = ctl.start(&f, &Run::ttl("6h"));
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(3), 10);
    ingest(&s, &ts);
    ds.wait_handovers(&bucket_label(ts[0]), 1, LAKE_WAIT);
    std::thread::sleep(THREE_MIN);
    assert!(ttl_snapshots(&f).is_empty());
    assert_eq!(ds.head()["cold_deletion_segment"], Value::Null);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_order_hot_cold_trace() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.hold("ttl.before_run");
    let s = ctl.start(
        &f,
        &Run::ttl("6h").gates(&["ttl.before_run", "lake.after_digest_check"]),
    );
    let ds = f.dataset("app");
    let first = in_hour(hours_ago(10), 10);
    ingest(&s, &first);
    let first_files = handed_files(&ds.wait_handovers(&bucket_label(first[0]), 1, LAKE_WAIT));

    ctl.hold("lake.after_digest_check");
    let p = in_hour(hours_ago(14), 10);
    let second = in_hour(hours_ago(12), 10);
    ingest(&s, &[p.clone(), second.clone()].concat());
    ctl.wait_held("lake.after_digest_check", &s, LAKE_WAIT);
    let second_label = bucket_label(second[0]);
    let second_files = live(&ds, &second_label);
    assert!(!second_files.is_empty());

    ctl.release("ttl.before_run");
    wait_until(
        TTL_WAIT,
        "见第二组移出、第一组冷层文件移出、冷层变更段写出",
        || {
            live(&ds, &second_label).is_empty()
                && first_files.keys().all(|k| !cold_live(&f).contains(k))
                && ds.head()["cold_deletion_segment"] != Value::Null
        },
    );
    let s1: BTreeSet<u64> = deletions(&ds.bucket_now(&second_label).unwrap(), "ttl")
        .iter()
        .filter(|d| second_files.contains(d["path"].as_str().unwrap()))
        .map(|d| d["removed_in"].as_u64().unwrap())
        .collect();
    let s2: BTreeSet<u64> = cold_changes(&ds)
        .iter()
        .filter(|d| first_files.contains_key(d["path"].as_str().unwrap()))
        .map(|d| d["removed_in"].as_u64().unwrap())
        .collect();
    assert_eq!((s1.len(), s2.len()), (1, 1), "{s1:?} {s2:?}");
    let (s1, s2) = (*s1.first().unwrap(), *s2.first().unwrap());
    assert!(s1 < s2);
    let ttl = ttl_snapshots(&f);
    assert_eq!(ttl.len(), 1);
    let at = ttl[0]["timestamp-ms"].as_i64().unwrap();
    let ms = |id: u64| ds.snapshot(id)["created_at"].as_i64().unwrap() / 1000;
    assert!(ms(s1) <= at && at <= ms(s2), "{} {at} {}", ms(s1), ms(s2));
    ctl.release("lake.after_digest_check");
}

// ---------------------------------------------------------------------------
// E · 避让、放弃与不删除的对象
// ---------------------------------------------------------------------------

/// 两个入湖避让场景共用：保留期 3h，10 条记录 5 分钟后到期，入湖停在 `step`；到期后再等 3 分钟。返回实例、桶标签与入湖输入。
fn guard_case(f: &Fixture, ctl: &Ctl, step: &'static str) -> (Server, String, BTreeSet<String>) {
    ctl.hold(step);
    let s = ctl.start(f, &Run::ttl("3h").gates(&[step]));
    let ds = f.dataset("app");
    let ts = series(now_us() - 2 * HOUR - 55 * MINUTE, 10);
    ingest(&s, &ts);
    ctl.wait_held(step, &s, LAKE_WAIT);
    let label = bucket_label(ts[0]);
    let inputs = live(&ds, &label);
    assert!(!inputs.is_empty());
    let expiry = ts[9] + 3 * HOUR;
    let until = expiry + 3 * MINUTE;
    while now_us() < until {
        std::thread::sleep(Duration::from_secs(1));
    }
    (s, label, inputs)
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_lake_guard_inputs_kept() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let (_s, label, inputs) = guard_case(&f, &ctl, "lake.after_digest_check");
    let ds = f.dataset("app");
    assert!(inputs.is_subset(&live(&ds, &label)));
    assert!(deletions(&ds.bucket_now(&label).unwrap(), "ttl").is_empty());

    ctl.release("lake.after_digest_check");
    let m = ds.wait_handovers(&label, 1, LAKE_WAIT);
    for p in &inputs {
        let mine: Vec<Value> = m["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|d| d["path"] == p.as_str())
            .cloned()
            .collect();
        assert_eq!(mine.len(), 1, "{p}：{m}");
        assert_eq!(mine[0]["reason"], "lake");
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_lake_guard_window_kept() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let (_s, label, inputs) = guard_case(&f, &ctl, "lake.after_cold_commit");
    let ds = f.dataset("app");
    assert!(inputs.is_subset(&live(&ds, &label)));
    let cold = f.cold("app");
    let meta = cold.current();
    let lake = ColdDir::lake_snapshots(&meta, &label);
    assert_eq!(lake.len(), 1);
    let added = cold.added(&meta, &lake[0]);
    let live_cold = cold_live(&f);
    assert!(!added.is_empty());
    assert!(added.iter().all(|e| live_cold.contains(&e.path)));
    assert!(ds.keys_under("manifest/cold-deletions/").is_empty());
    ctl.release("lake.after_cold_commit");
}

/// 读取故障的场景共用：保留期 3h、冷层写入失败，以 `ts` 摄取后停机；重启时对 `blocked` 桶的桶 manifest 的读取回 403
/// （重启后的进程此前未读过它）。返回重启后的实例。
fn restart_with_blocked_manifest(f: &Fixture, ctl: &Ctl, ts: &[i64], blocked: &str) -> Server {
    ctl.faults(COLD_WRITE_FAILS);
    // 写入期间 TTL 不执行，重启后才开始判定
    ctl.hold("ttl.before_run");
    let mut s = ctl.start(f, &Run::ttl("3h").gates(&["ttl.before_run"]));
    let ds = f.dataset("app");
    ingest(&s, ts);
    ds.wait_records(ts.len() as u64, Duration::from_secs(15));
    stop(&mut s);
    let manifest = ds.bucket_now(blocked).unwrap();
    let rel = ds.head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == blocked)
        .map(|b| b["manifest"].as_str().unwrap().to_owned())
        .unwrap();
    assert!(manifest["files"].as_array().is_some());
    ctl.faults(&format!("{COLD_WRITE_FAILS}\nget default/app/{rel} 403"));
    let _ = std::fs::remove_file(ctl.gate_file("ttl.before_run"));
    ctl.start(f, &Run::ttl("3h"))
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_incomplete_read_abandons() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let six = in_hour(hours_ago(6), 5);
    let seven = in_hour(hours_ago(7), 5);
    let s = restart_with_blocked_manifest(
        &f,
        &ctl,
        &[six.clone(), seven.clone()].concat(),
        &bucket_label(seven[0]),
    );
    let ds = f.dataset("app");
    let before = ds.head();
    std::thread::sleep(THREE_MIN);
    let after = ds.head();
    assert_eq!(after["buckets"], before["buckets"]);
    assert!(hot_ttl(&ds).is_empty());
    drop(s);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_incomplete_unrelated_bucket_ignored() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let a = in_hour(hours_ago(6), 5);
    let b = in_hour(hours_ago(-5), 5);
    let _s = restart_with_blocked_manifest(
        &f,
        &ctl,
        &[a.clone(), b.clone()].concat(),
        &bucket_label(b[0]),
    );
    let ds = f.dataset("app");
    let label = bucket_label(a[0]);
    let files = live(&ds, &label);
    wait_until(TTL_WAIT, "见 A 的 files 为空", || {
        live(&ds, &label).is_empty()
    });
    let traced: BTreeSet<String> = deletions(&ds.bucket_now(&label).unwrap(), "ttl")
        .iter()
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    assert!(files.is_subset(&traced));
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_table_gone_abandons() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.hold("ttl.before_run");
    let s = ctl.start(
        &f,
        &Run::ttl("6h").gates(&["ttl.before_run", "lake.after_digest_check"]),
    );
    let ds = f.dataset("app");
    let fresh = in_hour(hours_ago(4), 10);
    ingest(&s, &fresh);
    let created = Instant::now();
    ds.wait_handovers(&bucket_label(fresh[0]), 1, LAKE_WAIT);
    assert!(
        ds.head()["summary"]["cold"]["record_count"]
            .as_u64()
            .unwrap()
            > 0
    );

    ctl.hold("lake.after_digest_check");
    let p = in_hour(hours_ago(14), 10);
    let q = in_hour(hours_ago(12), 10);
    ingest(&s, &[p, q.clone()].concat());
    ctl.wait_held("lake.after_digest_check", &s, LAKE_WAIT);
    let q_label = bucket_label(q[0]);
    let q_files = live(&ds, &q_label);
    // 冷层合并（REQ-AMB-STO-COLD-COMPACTION）每 30 秒一轮，在第一次见到 `app` 的那一轮读一次冷层表，读到冷层表整体消失即按
    // REQ-AMB-STO-COLD-TABLE-V2 停用 `app`，与 TTL 无关。删除之前等到 `app` 建立之后满两轮的间隔，使那次读取在删除之前
    if let Some(left) = COLD_COMPACTION_SETTLED.checked_sub(created.elapsed()) {
        std::thread::sleep(left);
    }
    std::fs::remove_dir_all(&f.cold("app").dir).unwrap();

    ctl.release("ttl.before_run");
    std::thread::sleep(THREE_MIN);
    assert_eq!(live(&ds, &q_label), q_files);
    assert!(hot_ttl(&ds).is_empty());
    assert!(!app_disabled(&s));
    ctl.release("lake.after_digest_check");
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_read_failure_not_disabled() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let ds = f.dataset("app");
    let a = in_hour(hours_ago(6), 5);
    let b = in_hour(hours_ago(-5), 5);
    ingest(&s, &[a, b.clone()].concat());
    ds.wait_records(10, Duration::from_secs(15));
    let b_label = bucket_label(b[0]);
    let rel = ds.head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["bucket"] == b_label.as_str())
        .map(|x| x["manifest"].as_str().unwrap().to_owned())
        .unwrap();
    ds.remove(&rel);
    std::thread::sleep(THREE_MIN);
    assert!(!app_disabled(&s));
    assert!(
        !s.json_lines()
            .iter()
            .any(|l| l["event"] == "dataset_disabled" && l["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_disabled_dataset_skipped() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    ctl.hold("ttl.before_run");
    let run = Run::ttl("3h").gates(&["ttl.before_run"]);
    let mut s = ctl.start(&f, &run);
    let ds = f.dataset("app");
    let old = in_hour(hours_ago(6), 5);
    let other = series(now_us(), 5);
    ingest(&s, &[old.clone(), other.clone()].concat());
    ds.wait_records(10, Duration::from_secs(15));
    stop(&mut s);
    let file = live(&ds, &bucket_label(old[0])).into_iter().next().unwrap();
    let before = ds.current().unwrap();
    // 另一个被引用的桶 manifest 被删除；重启后的进程未缓存它，查询读到它不存在即进入 manifest 不可读情形
    let other_label = bucket_label(other[0]);
    let rel = ds.head()["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|x| x["bucket"] == other_label.as_str())
        .map(|x| x["manifest"].as_str().unwrap().to_owned())
        .unwrap();
    ds.remove(&rel);
    ctl.hold("ttl.before_run");
    let s = ctl.start(&f, &run);
    ctl.wait_held("ttl.before_run", &s, Duration::from_secs(30));
    let r = common::api::search_resp(&s, "SELECT * FROM app");
    assert_eq!(r.status, 500, "{}", r.text());

    ctl.release("ttl.before_run");
    std::thread::sleep(THREE_MIN);
    assert!(ds.exists(&file));
    assert_eq!(ds.current(), Some(before));
}

#[test]
fn ttl_counters_kept() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let ts = in_hour(hours_ago(6), 10);
    let received = now_us();
    ingest(&s, &ts);
    let k = counter_file(&f, "app", hour_of(received));
    wait_until(Duration::from_secs(120), "见计数对象写出", || {
        k.exists()
    });
    wait_until(TTL_WAIT, "查询 total 为 0", || {
        total_in(&s, ts[0], ts[9] + 1) == 0
    });
    assert!(k.exists());
    let obj: Value = serde_json::from_slice(&std::fs::read(&k).unwrap()).unwrap();
    let durable: u64 = obj["windows"]
        .as_array()
        .unwrap()
        .iter()
        .map(|w| w["durable"].as_u64().unwrap())
        .sum();
    assert!(durable >= 10, "{obj}");
}

// ---------------------------------------------------------------------------
// F · 到达时已过保留期的记录
// ---------------------------------------------------------------------------

#[test]
fn ttl_late_expired_record_accepted() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let s = ctl.start(&f, &Run::ttl("3h"));
    let r = now_us() - 10 * HOUR;
    let body = ingest_ok(&s, "app", &format!(r#"[{{"_timestamp":{r}}}]"#));
    assert_eq!(body["status"][0]["successful"], 1, "{body}");
    wait_until(TTL_WAIT, "查询 total 为 0", || {
        total_in(&s, r, r + 1) == 0
    });
}

// ---------------------------------------------------------------------------
// REQ-AMB-STO-AUDIT-INVARIANTS-V3 中由 TTL 写出的留痕（块的两个场景，测试放在本文件）
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn audit_hot_ttl_tombstone_cutoff() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    // 记下 A 的条目之前 TTL 不执行
    ctl.hold("ttl.before_run");
    let s = ctl.start(&f, &Run::ttl("3h").gates(&["ttl.before_run"]));
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(6), 10);
    ingest(&s, &ts);
    ds.wait_records(10, Duration::from_secs(15));
    let label = bucket_label(ts[0]);
    let recorded: Vec<Value> = ds.bucket_now(&label).unwrap()["files"]
        .as_array()
        .unwrap()
        .clone();
    assert!(!recorded.is_empty());
    ctl.release("ttl.before_run");
    wait_until(TTL_WAIT, "见 A 的 files 为空", || {
        live(&ds, &label).is_empty()
    });

    let m = ds.bucket_now(&label).unwrap();
    for e in &recorded {
        let mine: Vec<Value> = deletions(&m, "ttl")
            .into_iter()
            .filter(|d| d["path"] == e["path"])
            .collect();
        assert_eq!(mine.len(), 1, "{m}");
        let d = &mine[0];
        for k in ["content_digest", "added_in", "max_timestamp"] {
            assert_eq!(d[k], e[k], "{k}：{d}");
        }
        let cutoff = d["cutoff"].as_i64().expect("cutoff 为 JSON 整数");
        assert!(cutoff > d["max_timestamp"].as_i64().unwrap(), "{d}");
        assert!(
            cutoff <= d["deleted_at"].as_i64().unwrap() - 3 * HOUR,
            "{d}"
        );
    }
}

#[test]
fn audit_cold_deletion_segment_ttl() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    // 场景「交接记录中的冷层文件摘要与冷层文件一致」的配置与写入：B 为此刻前 3 小时所在小时桶，100 条
    let base = Run {
        seal_grace: Some("10m".into()),
        lake_watermark: Some("1h".into()),
        ..Run::default()
    };
    let mut s = ctl.start(&f, &base);
    let ds = f.dataset("app");
    let b = hour_of(now_us() - 3 * HOUR);
    ingest(&s, &in_hour(b, 100));
    let m = ds.wait_handovers(&bucket_label(b), 1, Duration::from_secs(150));
    let recorded = handed_files(&m);
    assert!(!recorded.is_empty());
    stop(&mut s);

    let _s = ctl.start(
        &f,
        &Run {
            retention: Some("2h".into()),
            ..base
        },
    );
    wait_until(
        TTL_WAIT,
        "见冷层表当前版本不再含所记文件",
        || {
            let live = cold_live(&f);
            recorded.keys().all(|p| !live.contains(p))
        },
    );
    // 冷层变更段在冷层提交之后写出
    wait_until(Duration::from_secs(30), "见冷层变更段写出", || {
        let changes = cold_changes(&ds);
        recorded
            .keys()
            .all(|p| changes.iter().any(|d| d["path"] == p.as_str()))
    });
    let ttl = ttl_snapshots(&f);
    assert_eq!(ttl.len(), 1);
    let ttl_id = ttl[0]["snapshot-id"].as_i64().unwrap();
    let segments = ds.segments(&ds.head()["cold_deletion_segment"]);
    // 自链首起：首个引用某段的快照
    let mut chain = ds.chain();
    chain.reverse();
    for (path, rec) in &recorded {
        let mut hits = Vec::new();
        for seg in &segments {
            for d in seg["deletions"].as_array().unwrap() {
                if d["path"] == path.as_str() {
                    hits.push((seg.clone(), d.clone()));
                }
            }
        }
        assert_eq!(hits.len(), 1, "{path}");
        let (seg, d) = &hits[0];
        assert_eq!(d["content_digest"], rec["content_digest"]);
        assert_eq!(d["max_timestamp"], rec["max_timestamp"]);
        assert_eq!(d["reason"], "ttl");
        let cutoff = d["cutoff"].as_i64().expect("cutoff 为 JSON 整数");
        assert!(cutoff > d["max_timestamp"].as_i64().unwrap(), "{d}");
        assert!(
            cutoff <= d["deleted_at"].as_i64().unwrap() - 2 * HOUR,
            "{d}"
        );
        assert_eq!(seg["additions"], json!([]));
        assert_eq!(d["cold_removed_in"], ttl_id);
        let seg_path = layout_cold_change_path(seg);
        let first = chain
            .iter()
            .find(|s| s["cold_deletion_segment"]["path"] == seg_path.as_str())
            .expect("有快照引用该段");
        assert_eq!(d["removed_in"], first["snapshot_id"]);
    }
}

/// 冷层变更段的路径（`manifest/cold-deletions/<零填充 20 位段号>.json`）。
fn layout_cold_change_path(seg: &Value) -> String {
    amberlog_storage::layout::cold_change_segment_path(amberlog_storage::chain::SegmentId(
        seg["segment_id"].as_u64().unwrap(),
    ))
}

// ---------------------------------------------------------------------------
// H · TTL 热层提交按父快照重核（V2 第 30 条）
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_hot_commit_recheck_after_lake() {
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    ctl.faults(COLD_WRITE_FAILS);
    ctl.hold("ttl.before_hot_commit");
    let s = ctl.start(
        &f,
        &Run::ttl("3h").gates(&["ttl.before_hot_commit", "ttl.before_run"]),
    );
    let ds = f.dataset("app");
    let ts = in_hour(hours_ago(6), 10);
    ingest(&s, &ts);
    // 冷层写入失败使入湖完不成；该次执行判 F 到期后停在 `ttl.before_hot_commit`
    ctl.wait_held("ttl.before_hot_commit", &s, TTL_WAIT);
    let label = bucket_label(ts[0]);
    let files = live(&ds, &label);
    assert_eq!(files.len(), 1, "{files:?}");
    let f_path = files.into_iter().next().unwrap();

    ctl.faults("");
    ds.wait_handovers(&label, 1, LAKE_WAIT);
    let s_id = ds.head()["snapshot_id"].clone();
    // 挡住其后的执行，15 秒内只有被挡的那次执行能写提交
    ctl.hold("ttl.before_run");
    ctl.release("ttl.before_hot_commit");
    let released = Instant::now();
    // 下一轮停在 `ttl.before_run` 即被挡的那次执行已结束；观察期照场景满 15 秒
    ctl.wait_held("ttl.before_run", &s, Duration::from_secs(60));
    std::thread::sleep(Duration::from_secs(15).saturating_sub(released.elapsed()));

    let m = ds.bucket_now(&label).unwrap();
    let mine: Vec<&Value> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["path"] == f_path.as_str())
        .collect();
    assert_eq!(mine.len(), 1, "{m}");
    assert_eq!(mine[0]["reason"], "lake", "{m}");
    assert_eq!(ds.head()["snapshot_id"], s_id);
}

// ---------------------------------------------------------------------------
// V2 · 与冷层合并的配合（第 23、24 条与第 15 条的合并记录一支）
// ---------------------------------------------------------------------------

const DAY: i64 = 24 * HOUR;
const SECOND: i64 = 1_000_000;

/// 三个场景中 `cutoff` 越过给定时刻的时刻（自实例启动起）。长时场景：设了 `TESTBED_SOAK_HOURS`
/// 时按场景取 10 分钟；缺省跑缩短版，取 150 秒（两个桶入湖、冷层合并走到暂停点都在其前完成）。
fn merge_lead() -> i64 {
    if std::env::var("TESTBED_SOAK_HOURS").is_ok() {
        10 * MINUTE
    } else {
        150 * SECOND
    }
}

/// D 日（此刻所在 UTC 日的前两日）`h:m:s` 时刻（UTC 微秒）。
fn day_d(h: i64, m: i64, s: i64) -> i64 {
    now_us().div_euclid(DAY) * DAY - 2 * DAY + h * HOUR + m * MINUTE + s * SECOND
}

/// 自 `start` 起每秒一个、共 `n` 个时间戳。
fn per_second(start: i64, n: usize) -> Vec<i64> {
    (0..n as i64).map(|i| start + i * SECOND).collect()
}

/// 使 `cutoff` 在 `started` 之后 `lead` 时越过 `at` 的 `retention.period`（整秒，向上取整）。
fn period_crossing(started: i64, lead: i64, at: i64) -> String {
    format!("{}s", (started + lead - at + SECOND - 1).div_euclid(SECOND))
}

/// 当前快照经 `cold_deletion_segment` 沿 `previous` 可达的冷层变更段（自最新段起）。
fn cold_segments(ds: &Ds) -> Vec<Value> {
    let mut out = Vec::new();
    let mut next = ds.head()["cold_deletion_segment"].clone();
    while !next.is_null() {
        let seg = ds.json(next["path"].as_str().unwrap());
        next = seg["previous"].clone();
        out.push(seg);
    }
    out
}

/// 冷层当前版本中摘要 `amberlog.operation` 为 `compaction` 的快照。
fn compaction_snapshots(f: &Fixture) -> Vec<Value> {
    let cold = f.cold("app");
    if cold.version().is_none() {
        return Vec::new();
    }
    ColdDir::snapshots(&cold.current())
        .into_iter()
        .filter(|s| s["summary"]["amberlog.operation"] == "compaction")
        .collect()
}

/// 「合并记录尚未写出的合并产出不被 TTL 移走」的实例与数据：冷层合并停在 `cold_compaction.after_cold_commit`。
/// `m` 为 D 日 02:00:09。返回实例、控制、实例启动时刻与合并产出 O（冷层合并提交新增的冷层数据文件）。
struct MergeCase {
    server: Server,
    ctl: Ctl,
    started: i64,
    outputs: BTreeSet<String>,
}

const AFTER_MERGE: &str = "cold_compaction.after_cold_commit";

fn merge_case(f: &Fixture) -> MergeCase {
    let ctl = Ctl::new(f);
    let m = day_d(2, 0, 9);
    let started = now_us();
    let lead = merge_lead();
    ctl.hold(AFTER_MERGE);
    let run = Run::ttl(&period_crossing(started, lead, m))
        .with("cold_compaction.check_interval", "\"10s\"")
        .gates(&[AFTER_MERGE]);
    let server = ctl.start(f, &run);
    let first = per_second(day_d(1, 59, 50), 10);
    let second = per_second(day_d(2, 0, 0), 10);
    ingest(&server, &first);
    ingest(&server, &second);
    let ds = f.dataset("app");
    ds.wait_handovers(&bucket_label(first[0]), 1, LAKE_WAIT);
    ds.wait_handovers(&bucket_label(second[0]), 1, LAKE_WAIT);
    let left = Duration::from_micros(u64::try_from(started + lead - now_us()).unwrap_or(0));
    ctl.wait_held(AFTER_MERGE, &server, left);
    let compactions = compaction_snapshots(f);
    assert_eq!(compactions.len(), 1, "{compactions:?}");
    let id = compactions[0]["snapshot-id"].as_i64().unwrap();
    let cold = f.cold("app");
    let outputs: BTreeSet<String> = cold
        .entries(&cold.current(), &compactions[0])
        .into_iter()
        .filter(|e| !e.deleted && e.snapshot_id == id)
        .map(|e| e.path)
        .collect();
    assert!(!outputs.is_empty());
    MergeCase {
        server,
        ctl,
        started,
        outputs,
    }
}

/// 等到被测进程的 `cutoff`（此刻减保留期）越过 `at` 之后再等 `then`；保留期按 `started + lead` 时越过算出。
fn wait_cutoff_past(started: i64, lead: i64, then: Duration) {
    let crossing = started + lead;
    let wait = u64::try_from(crossing - now_us()).unwrap_or(0);
    std::thread::sleep(Duration::from_micros(wait) + then);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_compaction_guard_outputs_kept() {
    let f = Fixture::new();
    let c = merge_case(&f);
    wait_cutoff_past(c.started, merge_lead(), THREE_MIN);
    assert!(c.ctl.held(AFTER_MERGE), "其间合并保持暂停");
    let live = cold_live(&f);
    for o in &c.outputs {
        assert!(live.contains(o), "合并产出 {o} 应仍存活：{live:?}");
    }
    assert_eq!(ttl_snapshots(&f), Vec::<Value>::new());
    drop(c.server);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_trace_from_merge_record() {
    let f = Fixture::new();
    let c = merge_case(&f);
    c.ctl.release(AFTER_MERGE);
    let ds = f.dataset("app");
    let limit = Duration::from_micros(u64::try_from(merge_lead()).unwrap()) + TTL_WAIT;
    wait_until(
        limit,
        "见 TTL 移出合并产出并写出冷层变更段",
        || {
            let changes = cold_changes(&ds);
            c.outputs.iter().all(|o| {
                changes
                    .iter()
                    .any(|d| d["path"] == o.as_str() && d["reason"] == "ttl")
            })
        },
    );
    let segments = cold_segments(&ds);
    let snapshots = ColdDir::snapshots(&f.cold("app").current());
    for o in &c.outputs {
        let (merge_seg, rec) = segments
            .iter()
            .find_map(|s| {
                s["additions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|a| a["path"] == o.as_str())
                    .map(|a| (s, a))
            })
            .expect("有记着该次合并的冷层变更段");
        let d = segments
            .iter()
            .flat_map(|s| s["deletions"].as_array().unwrap().iter())
            .find(|d| d["path"] == o.as_str() && d["reason"] == "ttl")
            .expect("有 TTL 的冷层变更段元素");
        assert_eq!(d["content_digest"], rec["content_digest"], "{d} {rec}");
        assert_eq!(d["max_timestamp"], rec["max_timestamp"], "{d} {rec}");
        let ttl = snapshots
            .iter()
            .find(|s| s["snapshot-id"] == d["cold_removed_in"])
            .expect("cold_removed_in 所指快照在冷层当前版本中");
        assert_eq!(ttl["summary"]["amberlog.operation"], "ttl");
        let merged_ms = merge_seg["created_at"].as_i64().unwrap().div_euclid(1000);
        assert!(
            ttl["timestamp-ms"].as_i64().unwrap() >= merged_ms,
            "{ttl} {merge_seg}"
        );
    }
    drop(c.server);
}

#[cfg(feature = "fault-injection")]
#[test]
fn ttl_cold_commit_recheck_after_compaction() {
    const TTL_RUN: &str = "ttl.before_run";
    const MERGE_RUN: &str = "cold_compaction.before_run";
    const META_PUT: &str = "cold_commit.before_metadata_put";
    let f = Fixture::new();
    let ctl = Ctl::new(&f);
    let started = now_us();
    let lead = merge_lead();
    ctl.hold(TTL_RUN);
    ctl.hold(MERGE_RUN);
    let run = Run::ttl(&period_crossing(started, lead, day_d(1, 0, 9)))
        .with("cold_compaction.check_interval", "\"10s\"")
        .gates(&[TTL_RUN, MERGE_RUN, META_PUT]);
    let s = ctl.start(&f, &run);
    let a_ts = per_second(day_d(1, 0, 0), 10);
    let b_ts = per_second(day_d(2, 30, 0), 20);
    ingest(&s, &a_ts);
    ingest(&s, &b_ts);
    let ds = f.dataset("app");
    let a: Vec<String> = handed_files(&ds.wait_handovers(&bucket_label(a_ts[0]), 1, LAKE_WAIT))
        .into_keys()
        .collect();
    let b: Vec<String> = handed_files(&ds.wait_handovers(&bucket_label(b_ts[0]), 1, LAKE_WAIT))
        .into_keys()
        .collect();
    assert_eq!((a.len(), b.len()), (1, 1), "{a:?} {b:?}");
    // 两个桶入湖完成之后才启用：只停住冷层合并提交
    ctl.hold(META_PUT);
    ctl.release(MERGE_RUN);
    ctl.wait_held(META_PUT, &s, Duration::from_secs(60));
    // A 整体到期（超时上限按场景为启动后 12 分钟，缩短版同比）
    wait_cutoff_past(started, lead, Duration::ZERO);
    ctl.release(TTL_RUN);
    std::thread::sleep(Duration::from_secs(15));
    ctl.release(META_PUT);
    std::thread::sleep(THREE_MIN);

    let compactions = compaction_snapshots(&f);
    assert_eq!(compactions.len(), 1, "{compactions:?}");
    let c_id = compactions[0]["snapshot-id"].clone();
    let live = cold_live(&f);
    assert!(!live.contains(&a[0]) && !live.contains(&b[0]), "{live:?}");
    assert_eq!(ttl_snapshots(&f), Vec::<Value>::new());
    let of_a: Vec<Value> = cold_changes(&ds)
        .into_iter()
        .filter(|d| d["path"] == a[0].as_str())
        .collect();
    assert_eq!(of_a.len(), 1, "{of_a:?}");
    assert_eq!(of_a[0]["reason"], "compaction");
    assert_eq!(of_a[0]["cold_removed_in"], c_id);
}
