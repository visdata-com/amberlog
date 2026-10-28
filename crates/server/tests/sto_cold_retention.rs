//! REQ-AMB-STO-COLD-RETENTION 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录；测试直接读冷层元数据 JSON、Avro 清单列表与清单（iceberg-rust 按 Iceberg 规范解析，不经
//! 被测实现）、热层快照与冷层变更段，核对 `_search`、`_json` 的应答与 stderr 的 JSON 日志。
//!
//! - 场景通则（块）：实例以 `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、`cold_retention.snapshots`
//!   为 `1`、`cold_retention.age` 为 `1s`、`cold_retention.check_interval` 为 `5s`、`storage.delete_delay` 为 `1s`、
//!   `hot_fsck.interval` 为 `5s`、`hot_fsck.min_orphan_age` 为 `1s` 启动（[`Run::base`]），冷层合并在暂停点
//!   `cold_compaction.before_run` 暂停（[`Ctl::start`] 每次启动都重新启用它，[`Run::merging`] 除外）。「三次入湖的实例」由
//!   [`Ctl::three`] 搭建，「冷层快照集」为 [`Ctl::set`]。
//! - 「等待 N 秒」后断言某事已发生的，改为轮询到条件成立即止、上限取 N 秒；断言某事没有发生的，等满 N 秒。「等待至」
//!   「轮询直到」的超时上限为 3 分钟（[`WAIT`]）。
//! - 故障注入（只在 `fault-injection` 特性下生效）：暂停点经 `TESTBED_FAULT_GATES`（每个步骤一个控制文件，运行中写 `hold`
//!   即启用）；停机前的停顿经 `TESTBED_FAULT_PAUSE`；对象存储请求经 `TESTBED_FAULT_STORE`。

// 场景都要用测试专用的注入，只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::api::{ingest, now_us, poll};
use common::lake::ColdDir;
use common::store::{Ds, HOUR, bucket_label, hour_of, sha256, stop};
use common::{BEARER, Fixture, Kv, Server, wait_file};
use serde_json::{Value, json};

const SECOND: i64 = 1_000_000;
const MINUTE: i64 = 60 * SECOND;
const DAY: i64 = 24 * HOUR;
const APP: &str = "app";
/// 「等待至」「轮询直到」的超时上限。
const WAIT: Duration = Duration::from_secs(180);

// ---------------------------------------------------------------------------
// 启动与注入
// ---------------------------------------------------------------------------

/// 一次启动的配置（TOML 键与取值）与注入。
#[derive(Clone, Default)]
struct Run {
    kv: Vec<(&'static str, String)>,
    pause: Option<&'static str>,
    /// 冷层合并照常运行（不在 `cold_compaction.before_run` 暂停）。
    merging: bool,
}

impl Run {
    /// 块的场景通则；另以 `flush.interval` 为 `1s` 使摄取尽快提交。
    fn base() -> Run {
        Run::default()
            .with("flush.interval", "1s")
            .with("lifecycle.seal_grace", "1s")
            .with("lifecycle.lake_watermark", "2s")
            .raw("cold_retention.snapshots", "1")
            .with("cold_retention.age", "1s")
            .with("cold_retention.check_interval", "5s")
            .with("storage.delete_delay", "1s")
            .with("hot_fsck.interval", "5s")
            .with("hot_fsck.min_orphan_age", "1s")
    }

    /// 设字符串配置项（已有同名键则替换）。
    fn with(self, key: &'static str, value: &str) -> Run {
        self.raw(key, &format!("\"{value}\""))
    }

    /// 设配置项，取值按 TOML 原样写出。
    fn raw(mut self, key: &'static str, toml: &str) -> Run {
        self.kv.retain(|(k, _)| *k != key);
        self.kv.push((key, toml.to_owned()));
        self
    }

    fn pause(mut self, step: &'static str) -> Run {
        self.pause = Some(step);
        self
    }

    fn merging(mut self) -> Run {
        self.merging = true;
        self
    }
}

/// 测试用到的暂停点（每个一个控制文件，起初为空即不停）。
const GATES: &[&str] = &[
    "cold_compaction.before_run",
    "cold_compaction.after_cold_commit",
    "cold_retention.before_commit",
    "lake.after_cold_commit",
    "ttl.before_run",
    "ttl.after_cold_commit",
];

/// 被测实例的环境：临时目录与跨重启共用的注入控制文件。
struct Ctl {
    f: Fixture,
    root: PathBuf,
}

/// 「三次入湖的实例」的三个入湖快照（S1、S2、S3）。
struct Three {
    s: [i64; 3],
}

impl Ctl {
    fn new() -> Ctl {
        let f = Fixture::new();
        let root = f.root().join("retention-ctl");
        std::fs::create_dir_all(root.join("gates")).unwrap();
        std::fs::write(root.join("store"), "").unwrap();
        for g in GATES {
            std::fs::write(root.join("gates").join(g), "").unwrap();
        }
        Ctl { f, root }
    }

    fn cold(&self) -> ColdDir {
        self.f.cold(APP)
    }

    fn ds(&self) -> Ds {
        self.f.dataset(APP)
    }

    /// 对象存储注入规则（每行一条，写法见 `amberlog_storage::fault::store`）；运行中改写即生效。
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

    fn start(&self, run: &Run) -> Server {
        if !run.merging {
            self.hold("cold_compaction.before_run");
        }
        let kv: Vec<Kv> = run.kv.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect();
        let mut cmd = self.f.serve(&kv);
        cmd.env("TESTBED_FAULT_STORE", self.root.join("store"));
        let gates: Vec<String> = GATES
            .iter()
            .map(|g| format!("{g}={}", self.gate_file(g).display()))
            .collect();
        cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
        if let Some(step) = run.pause {
            let _ = std::fs::remove_file(self.marker());
            cmd.env("TESTBED_FAULT_PAUSE", step)
                .env("TESTBED_FAULT_MARKER", self.marker());
        }
        Server::start(cmd)
    }

    /// 向 `app` 摄取 10 条 `_timestamp` 落在此刻前第 `n` 小时的记录，等该桶入湖完成，返回入湖快照号。
    fn lake_hours_ago(&self, server: &Server, n: i64) -> i64 {
        let start = hours_ago(n);
        ingest10(server, start);
        self.wait_laked(start, server).0
    }

    /// 等时刻 `ts` 所在小时桶的第一条交接记录写入热层，返回其冷层提交的 Iceberg 快照号与唯一的冷层数据文件。
    fn wait_laked(&self, ts: i64, server: &Server) -> (i64, String) {
        let label = bucket_label(ts);
        let ds = self.ds();
        let (m, ok) = poll(
            WAIT,
            || ds.bucket_now(&label),
            |m| {
                m.as_ref()
                    .is_some_and(|m| m["handovers"].as_array().is_some_and(|h| !h.is_empty()))
            },
        );
        assert!(ok, "桶 {label} 没有入湖完成：{:#?}", server.stderr_lines());
        let h = m.unwrap()["handovers"][0].clone();
        let files = h["files"].as_array().unwrap();
        assert_eq!(files.len(), 1, "{h}");
        (
            h["iceberg_snapshot_id"].as_i64().unwrap(),
            files[0]["path"].as_str().unwrap().to_owned(),
        )
    }

    /// 「三次入湖的实例」：以 `run` 启动，向 `app` 依次摄取此刻前第 5、4、3 小时的记录各 10 条，每次等该桶入湖完成。
    fn three(&self, run: &Run) -> (Server, Three) {
        let s = self.start(run);
        let s1 = self.lake_hours_ago(&s, 5);
        let s2 = self.lake_hours_ago(&s, 4);
        let s3 = self.lake_hours_ago(&s, 3);
        (s, Three { s: [s1, s2, s3] })
    }

    /// 冷层快照集：冷层当前版本元数据 `snapshots` 中各快照的 `snapshot-id`。
    fn set(&self) -> BTreeSet<i64> {
        ColdDir::snapshots(&self.cold().current())
            .iter()
            .map(|s| s["snapshot-id"].as_i64().unwrap())
            .collect()
    }

    /// 轮询到冷层快照集等于 `want`（至多 `limit`）。
    fn wait_set(&self, want: &[i64], limit: Duration, server: &Server) {
        let want: BTreeSet<i64> = want.iter().copied().collect();
        let (got, ok) = poll(limit, || self.set(), |s| *s == want);
        assert!(
            ok,
            "{} 秒内冷层快照集没有成为 {want:?}，仍为 {got:?}：{:#?}",
            limit.as_secs(),
            server.stderr_lines()
        );
    }

    /// 冷层当前版本 N。
    fn version(&self) -> u64 {
        self.cold().version().expect("已有冷层表")
    }

    /// 某个版本元数据中快照 `id`（任一列有它的版本，自新到旧找；版本文件一律保留）。
    fn snapshot(&self, id: i64) -> Value {
        let cold = self.cold();
        for n in (1..=self.version()).rev() {
            if let Some(s) = ColdDir::snapshots(&cold.metadata(n))
                .into_iter()
                .find(|s| s["snapshot-id"] == id)
            {
                return s;
            }
        }
        panic!("没有哪个版本列有快照 {id}");
    }

    /// 快照 `id` 的清单列表（相对冷层表位置）。
    fn manifest_list(&self, id: i64) -> String {
        let meta = self.cold().metadata(1);
        ColdDir::rel(&meta, self.snapshot(id)["manifest-list"].as_str().unwrap())
    }

    /// 快照 `id` 的 `timestamp-ms`（UTC 微秒）。
    fn timestamp_us(&self, id: i64) -> i64 {
        self.snapshot(id)["timestamp-ms"].as_i64().unwrap() * 1000
    }

    /// 热层当前快照经 `cold_deletion_segment` 可达的各冷层变更段的 `deletions` 所记路径。
    fn traced_paths(&self) -> BTreeSet<String> {
        let ds = self.ds();
        if ds.current().is_none() {
            return BTreeSet::new();
        }
        ds.segments(&ds.head()["cold_deletion_segment"])
            .iter()
            .flat_map(|s| s["deletions"].as_array().unwrap().clone())
            .map(|d| d["path"].as_str().unwrap().to_owned())
            .collect()
    }

    /// 等到冷层变更段记下 `paths` 中的每个路径（至多 `limit`）。
    fn wait_traced(&self, paths: &[&str], limit: Duration, server: &Server) {
        let (got, ok) = poll(
            limit,
            || self.traced_paths(),
            |t| paths.iter().all(|p| t.contains(*p)),
        );
        assert!(
            ok,
            "{} 秒内冷层变更段没有记下 {paths:?}（已记 {got:?}）：{:#?}",
            limit.as_secs(),
            server.stderr_lines()
        );
    }

    /// 冷层当前版本中 `amberlog.operation` 为 `op` 的快照号（按 `sequence-number` 升序）。
    fn of_operation(&self, op: &str) -> Vec<i64> {
        let mut v: Vec<Value> = ColdDir::snapshots(&self.cold().current())
            .into_iter()
            .filter(|s| s["summary"]["amberlog.operation"] == op)
            .collect();
        v.sort_by_key(|s| s["sequence-number"].as_i64());
        v.iter()
            .map(|s| s["snapshot-id"].as_i64().unwrap())
            .collect()
    }

    fn current_snapshot_id(&self) -> i64 {
        self.cold().current()["current-snapshot-id"]
            .as_i64()
            .unwrap()
    }

    /// 冷层当前版本中桶 `ts` 的 `lake` 快照中最新的一个。
    fn lake_snapshot(&self, ts: i64) -> i64 {
        ColdDir::lake_snapshots(&self.cold().current(), &bucket_label(ts))
            .last()
            .expect("该桶的 lake 快照")["snapshot-id"]
            .as_i64()
            .unwrap()
    }
}

// ---------------------------------------------------------------------------
// 数据与读取
// ---------------------------------------------------------------------------

/// 此刻前第 `n` 小时所在小时桶中的一个时刻（桶起点加 1 分钟）。
fn hours_ago(n: i64) -> i64 {
    hour_of(now_us()) - n * HOUR + MINUTE
}

/// 以一个请求向 `app` 摄取 10 条记录：第 `i` 条 `_timestamp` 为 `start` 加 `i` 秒、`seq` 为 `i`。断言全部受理。
fn ingest10(server: &Server, start: i64) {
    let body: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, start + i * SECOND))
        .collect();
    let r = ingest(server, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 10, "{}", r.text());
}

/// `/metrics` 中是否有 `stream="app"` 的 `amberlog_dataset_disabled` 序列。
fn app_disabled(server: &Server) -> bool {
    let r = server.get_auth("/metrics");
    r.text()
        .lines()
        .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains(r#"stream="app""#))
}

/// 以 `[start, end)` 提交 `SELECT * FROM app`（`size` 为 1000）。
fn query(server: &Server, start: i64, end: i64) -> common::Resp {
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": start, "end_time": end, "size": 1000}});
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    )
}

fn ids(v: &[i64]) -> BTreeSet<i64> {
    v.iter().copied().collect()
}

// ---------------------------------------------------------------------------
// 定期执行，首次执行有上界
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_interval() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base());
    c.wait_set(&[t.s[2]], WAIT, &s);
    let s4 = c.lake_hours_ago(&s, 2);
    let deadline = c.timestamp_us(s4) + 30 * SECOND;
    let limit = Duration::from_micros(u64::try_from(deadline - now_us()).unwrap_or(0));
    c.wait_set(&[s4], limit, &s);
}

/// 以 `cold_retention.age` 为 `1h` 运行三次入湖的实例（按时长保留），以 SIGTERM 退出后以 `check_interval` 重启，返回
/// 重启后的实例、`GET /readyz` 首次返回 200 的时刻与三个入湖快照。
fn restart_after_age(c: &Ctl, check_interval: &str) -> (Server, Instant, Three) {
    let (mut s, t) = c.three(&Run::base().with("cold_retention.age", "1h"));
    std::thread::sleep(Duration::from_secs(6));
    assert_eq!(c.set(), ids(&t.s));
    stop(&mut s);
    let s = c.start(&Run::base().with("cold_retention.check_interval", check_interval));
    let r = s.wait_readyz(200, Duration::from_secs(60));
    assert_eq!(r.status, 200, "{}", r.text());
    (s, Instant::now(), t)
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_first_run_after_start() {
    let c = Ctl::new();
    let (s, t0, t) = restart_after_age(&c, "1h");
    let want = ids(&t.s[2..]);
    loop {
        if c.set() == want {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(12 * 60),
            "重启后 12 分钟内冷层快照集没有成为 {want:?}：{:#?}",
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_secs(10));
    }
}

// ---------------------------------------------------------------------------
// 不再受保护的快照在时限内被过期
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_deadline() {
    let c = Ctl::new();
    let (s, t0, t) = restart_after_age(&c, "10s");
    let want = ids(&t.s[2..]);
    loop {
        if c.set() == want {
            break;
        }
        assert!(
            t0.elapsed() < Duration::from_secs(140),
            "t0 加 140 秒之前冷层快照集没有成为 {want:?}：{:#?}",
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_secs(5));
    }
}

// ---------------------------------------------------------------------------
// 恰好一次冷层过期提交移除全部不受保护的快照
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_expire_unprotected() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base());
    c.wait_set(&[t.s[2]], Duration::from_secs(30), &s);
    assert_eq!(c.current_snapshot_id(), t.s[2]);
    let cold = c.cold();
    for n in 1..=c.version() {
        assert!(cold.exists(&format!("metadata/v{n}.metadata.json")), "v{n}");
    }
}

// ---------------------------------------------------------------------------
// 不移除受保护的快照
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_by_count() {
    let c = Ctl::new();
    let (_s, t) = c.three(&Run::base().raw("cold_retention.snapshots", "2"));
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.set(), ids(&t.s[1..]));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_by_age() {
    let c = Ctl::new();
    let (_s, t) = c.three(&Run::base().with("cold_retention.age", "1h"));
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.set(), ids(&t.s));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_within_delete_delay() {
    let c = Ctl::new();
    let (_s, t) = c.three(&Run::base().with("storage.delete_delay", "1h"));
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.set(), ids(&t.s));
}

/// 先以未配置 `retention.period` 运行，摄取此刻前第 30 小时的 10 条记录并等其入湖（返回 X 与 S1），以 SIGTERM 退出。
fn laked_30h(c: &Ctl) -> (String, i64) {
    let mut s = c.start(&Run::base());
    let ts = hours_ago(30);
    ingest10(&s, ts);
    let (s1, x) = c.wait_laked(ts, &s);
    stop(&mut s);
    (x, s1)
}

/// 以 `retention.period` 为 `10h`、`retention.check_interval` 为 `10s` 重启的配置。
fn ttl_10h() -> Run {
    Run::base()
        .with("retention.period", "10h")
        .with("retention.check_interval", "10s")
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_window_lake() {
    let c = Ctl::new();
    let (x, _s1) = laked_30h(&c);
    c.hold("ttl.before_run");
    c.hold("lake.after_cold_commit");
    let s = c.start(&ttl_10h());
    c.wait_held("ttl.before_run", &s, WAIT);
    let ts = hours_ago(3);
    ingest10(&s, ts);
    c.wait_held("lake.after_cold_commit", &s, WAIT);
    let l = c.lake_snapshot(ts);

    c.release("ttl.before_run");
    c.wait_traced(&[&x], WAIT, &s);
    assert_eq!(c.of_operation("ttl").last(), Some(&c.current_snapshot_id()));
    std::thread::sleep(Duration::from_secs(30));
    assert!(c.set().contains(&l), "{:?}", c.set());

    c.release("lake.after_cold_commit");
    c.wait_laked(ts, &s);
    let (got, ok) = poll(Duration::from_secs(30), || c.set(), |set| !set.contains(&l));
    assert!(ok, "交接之后 30 秒内 L 仍在：{got:?}");
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_untraced_ttl() {
    let c = Ctl::new();
    let (x, s1) = laked_30h(&c);
    c.hold("ttl.after_cold_commit");
    let s = c.start(&ttl_10h());
    c.wait_held("ttl.after_cold_commit", &s, WAIT);
    let t = c.current_snapshot_id();
    assert_eq!(c.of_operation("ttl"), [t]);
    c.lake_hours_ago(&s, 3);
    assert_ne!(c.current_snapshot_id(), t);

    std::thread::sleep(Duration::from_secs(30));
    let set = c.set();
    assert!(set.contains(&t) && set.contains(&s1), "{set:?}");

    c.release("ttl.after_cold_commit");
    c.wait_traced(&[&x], WAIT, &s);
    let (got, ok) = poll(
        Duration::from_secs(30),
        || c.set(),
        |set| !set.contains(&t) && !set.contains(&s1),
    );
    assert!(ok, "冷层变更段写出之后 30 秒内 T 或 S1 仍在：{got:?}");
}

/// 此刻所在 UTC 日的前两日 `h` 时 1 分。
fn two_days_ago_at(h: i64) -> i64 {
    now_us().div_euclid(DAY) * DAY - 2 * DAY + h * HOUR + MINUTE
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_keep_untraced_compaction() {
    let c = Ctl::new();
    c.hold("cold_compaction.after_cold_commit");
    let s = c.start(
        &Run::base()
            .with("cold_compaction.check_interval", "10s")
            .merging(),
    );
    for h in [1, 2] {
        let ts = two_days_ago_at(h);
        ingest10(&s, ts);
        c.wait_laked(ts, &s);
    }
    c.wait_held("cold_compaction.after_cold_commit", &s, WAIT);
    let merged = c.of_operation("compaction");
    assert_eq!(merged.len(), 1, "{merged:?}");
    let cs = merged[0];
    c.lake_hours_ago(&s, 3);
    assert_ne!(c.current_snapshot_id(), cs);

    std::thread::sleep(Duration::from_secs(30));
    assert!(c.set().contains(&cs), "{:?}", c.set());

    c.release("cold_compaction.after_cold_commit");
    let ds = c.ds();
    let (_, ok) = poll(
        WAIT,
        || {
            ds.segments(&ds.head()["cold_deletion_segment"])
                .iter()
                .any(|seg| seg["additions"].as_array().is_some_and(|a| !a.is_empty()))
        },
        |b| *b,
    );
    assert!(ok, "记有 C 的冷层变更段没有写出：{:#?}", s.stderr_lines());
    let (got, ok) = poll(
        Duration::from_secs(30),
        || c.set(),
        |set| !set.contains(&cs),
    );
    assert!(ok, "合并记录写出之后 30 秒内 C 仍在：{got:?}");
}

// ---------------------------------------------------------------------------
// 没有可移除的快照时不写
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_no_empty_commit() {
    let c = Ctl::new();
    let s = c.start(&Run::base());
    c.lake_hours_ago(&s, 3);
    let n = c.version();
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.version(), n);
    assert!(
        !c.cold()
            .exists(&format!("metadata/v{}.metadata.json", n + 1))
    );
}

// ---------------------------------------------------------------------------
// 只移除快照，snapshot-log 按参考实现去掉条目
// ---------------------------------------------------------------------------

fn snapshot_ids(meta: &Value) -> BTreeSet<i64> {
    ColdDir::snapshots(meta)
        .iter()
        .map(|s| s["snapshot-id"].as_i64().unwrap())
        .collect()
}

fn log_ids(meta: &Value) -> Vec<i64> {
    meta["snapshot-log"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["snapshot-id"].as_i64().unwrap())
        .collect()
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_commit_shape() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base());
    c.wait_set(&[t.s[2]], WAIT, &s);
    let cold = c.cold();
    let m = (2..=c.version())
        .rev()
        .find(|&m| {
            snapshot_ids(&cold.metadata(m)).len() < snapshot_ids(&cold.metadata(m - 1)).len()
        })
        .expect("有写出移除的版本");
    let (before, after) = (cold.metadata(m - 1), cold.metadata(m));
    let (b_ids, a_ids) = (snapshot_ids(&before), snapshot_ids(&after));
    assert!(a_ids.is_subset(&b_ids), "{b_ids:?} {a_ids:?}");
    for key in [
        "current-snapshot-id",
        "refs",
        "schemas",
        "partition-specs",
        "properties",
    ] {
        assert_eq!(before[key], after[key], "{key}");
    }
    let log = log_ids(&before);
    let cut = log
        .iter()
        .rposition(|id| !a_ids.contains(id))
        .map_or(0, |i| i + 1);
    assert_eq!(log_ids(&after), log[cut..].to_vec());
}

/// 场景「过期较新的快照时其之前的 snapshot-log 条目一并去掉」中 `cutoff` 越过 h 时 58 分 59 秒的时刻（自实例启动起）。
/// 长时场景：设了 `TESTBED_SOAK_HOURS` 时按场景取 5 分钟；缺省跑缩短版，取 150 秒（搭建在其前
/// 完成；`cutoff` 越过 h+1 时 00 分 59 秒仍在其后约 2 分钟）。
fn truncate_lead() -> i64 {
    if std::env::var("TESTBED_SOAK_HOURS").is_ok() {
        5 * MINUTE
    } else {
        150 * SECOND
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_snapshot_log_truncated() {
    let c = Ctl::new();
    let started = now_us();
    let h = hour_of(started - 30 * HOUR);
    let w1 = h + 58 * MINUTE + 50 * SECOND;
    let w2 = h + HOUR + 50 * SECOND;
    // cutoff = 此刻减保留期：启动后 lead 时越过 W1 的上界（h 时 58 分 59 秒）
    let period_s = (started + truncate_lead() - (w1 + 9 * SECOND)) / SECOND;
    c.hold("ttl.before_run");
    let s = c.start(
        &Run::base()
            .with("retention.period", &format!("{period_s}s"))
            .with("retention.check_interval", "10s"),
    );
    c.wait_held("ttl.before_run", &s, WAIT);
    ingest10(&s, w1);
    let (_sw1, f1) = c.wait_laked(w1, &s);
    ingest10(&s, w2);
    let (_sw2, f2) = c.wait_laked(w2, &s);
    c.hold("lake.after_cold_commit");
    let ts = hours_ago(3);
    ingest10(&s, ts);
    c.wait_held("lake.after_cold_commit", &s, WAIT);
    let sl = c.lake_snapshot(ts);
    assert!(
        now_us() < started + truncate_lead(),
        "搭建超过了 cutoff 越过 W1 的时刻"
    );

    c.release("ttl.before_run");
    let limit = Duration::from_micros(u64::try_from(started + 12 * MINUTE - now_us()).unwrap_or(0));
    c.wait_traced(&[&f1, &f2], limit, &s);
    let ttl = c.of_operation("ttl");
    assert_eq!(ttl.len(), 2, "{ttl:?}");
    let t2 = ttl[1];
    assert_eq!(c.current_snapshot_id(), t2);
    c.wait_set(&[sl, t2], Duration::from_secs(30), &s);
    assert_eq!(log_ids(&c.cold().current()), [t2]);
}

// ---------------------------------------------------------------------------
// 重试耗尽时停用数据集
// ---------------------------------------------------------------------------

/// 以 `v<n>.metadata.json` 为基础另写的一份合法 v2 表元数据：只改 `last-updated-ms`，并把 `v<n>` 记入 `metadata-log`。
fn foreign_metadata(cold: &ColdDir, n: u64) -> Value {
    let mut meta = cold.metadata(n);
    let ms = now_us() / 1000;
    let file = format!(
        "{}/metadata/v{n}.metadata.json",
        meta["location"].as_str().unwrap()
    );
    let previous = meta["last-updated-ms"].clone();
    meta["last-updated-ms"] = json!(ms);
    meta["metadata-log"]
        .as_array_mut()
        .unwrap()
        .push(json!({"metadata-file": file, "timestamp-ms": previous}));
    meta
}

/// 「三次入湖的实例」先以 `cold_retention.age` 为 `1h` 搭建（三个快照都按时长保留，冷层过期提交尚未发生）、以 SIGTERM 退出，
/// 再以 `storage.commit_max_retries` 为 0、暂停点 `cold_retention.before_commit` 启用重启：重启后第一次执行停在该点。
#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_retry_exhausted() {
    let c = Ctl::new();
    let (mut s, _t) = c.three(&Run::base().with("cold_retention.age", "1h"));
    stop(&mut s);
    c.hold("cold_retention.before_commit");
    let s = c.start(&Run::base().raw("storage.commit_max_retries", "0"));
    c.wait_held("cold_retention.before_commit", &s, WAIT);
    let n = c.version();
    let cold = c.cold();
    let next = cold.path(&format!("metadata/v{}.metadata.json", n + 1));
    std::fs::write(
        &next,
        serde_json::to_vec(&foreign_metadata(&cold, n)).unwrap(),
    )
    .unwrap();
    c.release("cold_retention.before_commit");
    std::thread::sleep(Duration::from_secs(30));
    let now = now_us();
    let r = ingest(&s, APP, &format!(r#"[{{"_timestamp":{now},"seq":0}}]"#));
    r.assert_envelope(500, "internal");
}

// ---------------------------------------------------------------------------
// 不写热层、不删除对象
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_no_hot_write() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base());
    let ds = c.ds();
    let hot = ds.current().expect("热层当前快照");
    c.wait_set(&[t.s[2]], WAIT, &s);
    assert_eq!(ds.current(), Some(hot));
    assert!(ds.keys_under("manifest/cold-deletions/").is_empty());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_no_direct_delete() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base().with("hot_fsck.interval", "1h"));
    let list = c.manifest_list(t.s[0]);
    c.wait_set(&[t.s[2]], WAIT, &s);
    std::thread::sleep(Duration::from_secs(30));
    assert!(c.cold().exists(&list), "{list}");
}

// ---------------------------------------------------------------------------
// 被释放的清单列表的清理与重建
// ---------------------------------------------------------------------------

/// 场景「被释放的清单列表由冷层孤儿清理删除，当前版本照常可读」的准备与第一条断言：三次入湖的实例，等冷层快照集等于
/// {S3}，再至多 60 秒等 S1 与 S2 的清单列表被删除。
fn released(c: &Ctl) -> (Server, Three) {
    let (s, t) = c.three(&Run::base());
    let lists = [c.manifest_list(t.s[0]), c.manifest_list(t.s[1])];
    c.wait_set(&[t.s[2]], WAIT, &s);
    let cold = c.cold();
    let (_, ok) = poll(
        Duration::from_secs(60),
        || lists.iter().any(|l| cold.exists(l)),
        |left| !*left,
    );
    assert!(ok, "60 秒内 S1、S2 的清单列表没有删除：{lists:?}");
    (s, t)
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_released_cleaned() {
    use arrow::array::{Array, Int64Array, TimestampMicrosecondArray};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::table::StaticTable;

    let c = Ctl::new();
    let first = hours_ago(5);
    let (s, t) = released(&c);
    let cold = c.cold();
    let meta = cold.current();
    let s3 = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == t.s[2])
        .unwrap();
    let list = ColdDir::rel(&meta, s3["manifest-list"].as_str().unwrap());
    assert!(cold.exists(&list), "{list}");
    let list_bytes = cold.read(&list);
    let manifests = iceberg::spec::ManifestList::parse_with_version(
        &list_bytes,
        iceberg::spec::FormatVersion::V2,
    )
    .unwrap();
    for m in manifests.entries() {
        assert!(
            cold.exists(&ColdDir::rel(&meta, &m.manifest_path)),
            "{}",
            m.manifest_path
        );
    }
    for e in cold.entries(&meta, &s3) {
        assert!(cold.exists(&e.path), "{}", e.path);
    }
    for n in 1..=c.version() {
        assert!(cold.exists(&format!("metadata/v{n}.metadata.json")), "v{n}");
    }
    assert!(cold.exists("metadata/version-hint.text"));

    // 不经 Amberlog：按提示文件找当前版本，iceberg-rust 自带的读取器读出全表
    let hint = String::from_utf8(cold.read("metadata/version-hint.text")).unwrap();
    let location = meta["location"].as_str().unwrap().to_owned();
    let metadata = format!("{location}/metadata/v{}.metadata.json", hint.trim());
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();
    let batches: Vec<RecordBatch> = rt.block_on(async {
        let table = StaticTable::from_metadata_file(
            &metadata,
            TableIdent::from_strs(["default", "app"]).unwrap(),
            FileIO::new_with_fs(),
        )
        .await
        .unwrap();
        table
            .scan()
            .select_all()
            .build()
            .unwrap()
            .to_arrow()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap()
    });
    let mut got = Vec::new();
    for b in &batches {
        let ts = b
            .column_by_name("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        let seq = b
            .column_by_name("seq")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        for i in 0..b.num_rows() {
            assert!(!seq.is_null(i));
            got.push((ts.value(i), seq.value(i)));
        }
    }
    got.sort_unstable();
    let mut want: Vec<(i64, i64)> = (0..3)
        .flat_map(|k| (0..10).map(move |i| (first + k * HOUR + i * SECOND, i)))
        .collect();
    want.sort_unstable();
    assert_eq!(got, want);

    let r = query(&s, first - MINUTE, first + 3 * HOUR);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["total"], 30, "{}", r.text());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_rebuild_from_hot_chain() {
    let c = Ctl::new();
    let (_s, _t) = released(&c);
    let ds = c.ds();
    let head = ds.head();
    // 已记入冷层文件集：交接记录（`buckets` 与留痕归档段引用的桶 manifest），自链首依次累加冷层变更段
    let mut set: BTreeMap<String, String> = BTreeMap::new();
    let mut add = |m: &Value| {
        for h in m["handovers"].as_array().unwrap() {
            for f in h["files"].as_array().unwrap() {
                set.insert(
                    f["path"].as_str().unwrap().to_owned(),
                    f["content_digest"].as_str().unwrap().to_owned(),
                );
            }
        }
    };
    for m in ds.bucket_manifests(&head) {
        add(&m);
    }
    for seg in ds.segments(&head["retired_segment"]) {
        for b in seg["buckets"].as_array().unwrap() {
            add(&ds.restore(b["manifest"].as_str().unwrap()));
        }
    }
    for seg in ds.segments(&head["cold_deletion_segment"]).iter().rev() {
        for d in seg["deletions"].as_array().unwrap() {
            set.remove(d["path"].as_str().unwrap());
        }
        for a in seg["additions"].as_array().unwrap() {
            set.insert(
                a["path"].as_str().unwrap().to_owned(),
                a["content_digest"].as_str().unwrap().to_owned(),
            );
        }
    }

    let cold = c.cold();
    let meta = cold.current();
    let current = ColdDir::snapshots(&meta)
        .into_iter()
        .find(|x| x["snapshot-id"] == meta["current-snapshot-id"])
        .unwrap();
    let live: BTreeSet<String> = cold
        .entries(&meta, &current)
        .into_iter()
        .filter(|e| !e.deleted)
        .map(|e| e.path)
        .collect();
    assert_eq!(set.keys().cloned().collect::<BTreeSet<_>>(), live);
    for (path, digest) in &set {
        assert_eq!(digest, &sha256(&cold.read(path)), "{path}");
    }
}

// ---------------------------------------------------------------------------
// 不再是当前快照之后 storage.delete_delay 内不删清单列表
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_reader_window() {
    let c = Ctl::new();
    let s = c.start(&Run::base().with("storage.delete_delay", "1m"));
    let s1 = c.lake_hours_ago(&s, 5);
    let s2 = c.lake_hours_ago(&s, 4);
    let list = c.manifest_list(s1);
    let until = c.timestamp_us(s2) + 50 * SECOND;
    let cold = c.cold();
    while now_us() < until {
        assert!(cold.exists(&list), "{list} 在 S2 之后 50 秒内被删除");
        std::thread::sleep(Duration::from_secs(5));
    }
    assert!(cold.exists(&list), "{list}");
}

// ---------------------------------------------------------------------------
// 读不全时放弃本次、不停用
// ---------------------------------------------------------------------------

const METADATA_403: &str = "get _iceberg/default/app/metadata/ 403\n";

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_read_abort() {
    let c = Ctl::new();
    let (s, t) = c.three(&Run::base().with("storage.delete_delay", "30s"));
    c.faults(METADATA_403);
    let n = c.version();
    std::thread::sleep(Duration::from_secs(60));
    assert!(
        !c.cold()
            .exists(&format!("metadata/v{}.metadata.json", n + 1))
    );
    assert!(!app_disabled(&s));
    c.faults("");
    c.wait_set(&[t.s[2]], Duration::from_secs(30), &s);
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_read_abort_warn() {
    let c = Ctl::new();
    let (s, _t) = c.three(&Run::base().with("storage.delete_delay", "30s"));
    c.faults(METADATA_403);
    let n = s.stderr_lines().len();
    let warned = || {
        s.stderr_lines()[n..].iter().any(|l| {
            serde_json::from_str::<Value>(l).is_ok_and(|v| v["level"] == "warn")
                && l.contains("default")
                && l.contains("app")
        })
    };
    let (_, ok) = poll(Duration::from_secs(60), warned, |b| *b);
    assert!(ok, "{:#?}", &s.stderr_lines()[n..]);
}

// ---------------------------------------------------------------------------
// 数据集停用与停机
// ---------------------------------------------------------------------------

/// 运行中的实例缓存读过的桶 manifest：停下后删除、再以同一配置重启，查询才读到这次删除（与审计不变量测试同类场景的做法相同）。重启后立即
/// 查询，赶在冷层保留第一次执行（开始受理后 2 秒）之前使数据集停用。
#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_disabled_skipped() {
    let c = Ctl::new();
    let run = Run::base().with("storage.delete_delay", "30s");
    let (mut s, _t) = c.three(&run);
    let now = now_us();
    let r = ingest(&s, APP, &format!(r#"[{{"_timestamp":{now},"seq":0}}]"#));
    assert_eq!(r.status, 200, "{}", r.text());
    let ds = c.ds();
    let label = bucket_label(now);
    let (_, ok) = poll(
        Duration::from_secs(30),
        || ds.bucket_now(&label),
        Option::is_some,
    );
    assert!(ok, "此刻的桶没有提交");
    stop(&mut s);
    let head = ds.head();
    let r = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label.as_str())
        .unwrap()
        .clone();
    ds.remove(r["manifest"].as_str().unwrap());
    let s = c.start(&run);
    let q = query(&s, now - SECOND, now + SECOND);
    assert_eq!(q.status, 500, "{}", q.text());
    let n = c.version();
    std::thread::sleep(Duration::from_secs(60));
    assert!(
        !c.cold()
            .exists(&format!("metadata/v{}.metadata.json", n + 1))
    );
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_retention_no_start_on_shutdown() {
    let c = Ctl::new();
    let mut s = c.start(
        &Run::base()
            .with("storage.delete_delay", "30s")
            .pause("shutdown.before_exit"),
    );
    let s1 = c.lake_hours_ago(&s, 5);
    let s2 = c.lake_hours_ago(&s, 4);
    let n = c.version();
    let t = c.timestamp_us(s2);
    assert!(now_us() < t + 10 * SECOND, "搭建超过了 S2 之后 10 秒");
    s.signal("TERM");
    assert!(
        wait_file(&c.marker(), Duration::from_secs(60)),
        "{:#?}",
        s.stderr_lines()
    );
    // 停在退出之前：S1 自 S2 之后 30 秒起不再受保护，其间若开始了新的执行，检查间隔 5 秒内就会写出冷层过期提交
    while now_us() < t + 50 * SECOND {
        std::thread::sleep(Duration::from_secs(1));
    }
    s.sigkill();
    assert!(
        !c.cold()
            .exists(&format!("metadata/v{}.metadata.json", n + 1))
    );
    assert!(c.set().contains(&s1));
}
