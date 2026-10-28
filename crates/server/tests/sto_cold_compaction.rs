//! REQ-AMB-STO-COLD-COMPACTION 的系统测试：以子进程运行
//! `amberlog`，对象存储用 `file://` 临时目录；测试直接读冷层元数据 JSON、Avro 清单列表与清单（iceberg-rust 按 Iceberg 规范
//! 解析，不经被测实现）、冷层 Parquet、热层快照与冷层变更段，核对 `_search` 的应答与 stderr 的 JSON 日志。
//!
//! - 场景通则（块）：实例以 `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、
//!   `cold_compaction.check_interval` 为 `10s`、`storage.delete_delay` 为 `1h` 启动（[`Run::base`]）；D 为此刻所在 UTC 日的
//!   前两日。「两个小文件的实例」由 [`Ctl::two_small`] 搭建，「解除合并的暂停」为 [`Ctl::release_merge`]，「等待合并完成」为
//!   [`Ctl::wait_merge`]（超时 3 分钟）。
//! - 「等待 N 秒」后断言某事已发生的，改为轮询到条件成立即止、上限取 N 秒；断言某事没有发生的，等满 N 秒。
//! - 故障注入（只在 `fault-injection` 特性下生效）：暂停点经 `TESTBED_FAULT_GATES`（每个步骤一个控制文件，运行中写 `hold`
//!   即启用，放行后再写 `hold` 对下一次走到生效）；崩溃点经 `TESTBED_FAULT_PAUSE`；对象存储请求经 `TESTBED_FAULT_STORE`。

// 多数场景要用测试专用的注入，只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use arrow::array::{Array, Int64Array, TimestampMicrosecondArray};
use common::api::{ingest, now_us, poll};
use common::lake::ColdDir;
use common::store::{Ds, HOUR, bucket_label, sha256, stop};
use common::{BEARER, Fixture, Kv, Server, wait_file};
use serde_json::{Value, json};

const DAY: i64 = 24 * HOUR;
const SECOND: i64 = 1_000_000;
const MINUTE: i64 = 60 * SECOND;
const APP: &str = "app";
/// 场景中「等待至」「轮询直到」与「等待合并完成」的超时上限。
const WAIT: Duration = Duration::from_secs(180);

// ---------------------------------------------------------------------------
// 启动与注入
// ---------------------------------------------------------------------------

/// 一次启动的配置（TOML 键与取值）与崩溃点。
#[derive(Clone, Default)]
struct Run {
    kv: Vec<(&'static str, String)>,
    pause: Option<&'static str>,
    /// 测试专用时钟偏移（秒，`TESTBED_CLOCK_OFFSET`）。
    offset: Option<i64>,
}

impl Run {
    /// 块的场景通则：`lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、`cold_compaction.check_interval`
    /// 为 `10s`、`storage.delete_delay` 为 `1h`；另以 `flush.interval` 为 `1s` 使摄取尽快提交。
    fn base() -> Run {
        Run::default()
            .with("flush.interval", "1s")
            .with("lifecycle.seal_grace", "1s")
            .with("lifecycle.lake_watermark", "2s")
            .with("cold_compaction.check_interval", "10s")
            .with("storage.delete_delay", "1h")
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

    fn offset(mut self, seconds: i64) -> Run {
        self.offset = Some(seconds);
        self
    }
}

/// 测试用到的暂停点（每个一个控制文件，起初为空即不停）。
const GATES: &[&str] = &[
    "cold_compaction.before_run",
    "cold_compaction.after_select",
    "cold_compaction.after_digest_check",
    "cold_compaction.after_upload",
    "cold_compaction.after_cold_commit",
    "lake.after_cold_commit",
    "ttl.before_run",
    "shutdown.before_exit",
];

/// 被测实例的环境：临时目录与跨重启共用的注入控制文件。
struct Ctl {
    f: Fixture,
    root: PathBuf,
}

/// 「两个小文件的实例」中的两个冷层数据文件（相对冷层表位置）与它们的交接记录 `files` 元素。
struct Small {
    a: String,
    b: String,
    a_entry: Value,
    b_entry: Value,
}

impl Ctl {
    fn new() -> Ctl {
        let f = Fixture::new();
        let root = f.root().join("compaction-ctl");
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

    fn wait_held(&self, step: &str, server: &Server) {
        let deadline = Instant::now() + WAIT;
        while !self.held(step) {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有走到暂停点 {step}：{:#?}",
                WAIT.as_secs(),
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
        let kv: Vec<Kv> = run.kv.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect();
        let mut cmd = self.f.serve(&kv);
        cmd.env("TESTBED_FAULT_STORE", self.root.join("store"));
        let gates: Vec<String> = GATES
            .iter()
            .map(|g| format!("{g}={}", self.gate_file(g).display()))
            .collect();
        cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
        if let Some(o) = run.offset {
            cmd.env("TESTBED_CLOCK_OFFSET", o.to_string());
        }
        if let Some(step) = run.pause {
            let _ = std::fs::remove_file(self.marker());
            cmd.env("TESTBED_FAULT_PAUSE", step)
                .env("TESTBED_FAULT_MARKER", self.marker());
        }
        Server::start(cmd)
    }

    /// 「两个小文件的实例」：冷层合并在 `cold_compaction.before_run` 暂停，以 `run` 启动，向 `app` 摄取 A（D 日 01:00:00 至
    /// 01:00:09、`seq` 1～10）与 B（D 日 02:30:00 至 02:30:19、`seq` 11～30），等两个桶入湖完成。`extra(seq)` 给每条
    /// 记录另加的字段（空串或以逗号开头）。
    fn two_small_with(&self, run: &Run, extra: impl Fn(u64) -> String) -> (Server, Small) {
        self.hold("cold_compaction.before_run");
        let s = self.start(run);
        ingest_seq(&s, d_at(1, 0, 0), 1, 10, &extra);
        ingest_seq(&s, d_at(2, 30, 0), 11, 20, &extra);
        let (a, a_entry) = self.wait_laked(d_at(1, 0, 0));
        let (b, b_entry) = self.wait_laked(d_at(2, 30, 0));
        (
            s,
            Small {
                a,
                b,
                a_entry,
                b_entry,
            },
        )
    }

    fn two_small(&self, run: &Run) -> (Server, Small) {
        self.two_small_with(run, |_| String::new())
    }

    /// 等时刻 `ts` 所在小时桶的第一条交接记录写入热层，返回其唯一的冷层数据文件与 `files` 元素。
    fn wait_laked(&self, ts: i64) -> (String, Value) {
        let m = self.ds().wait_handovers(&bucket_label(ts), 1, WAIT);
        let files = m["handovers"][0]["files"].as_array().unwrap().clone();
        assert_eq!(files.len(), 1, "{files:?}");
        (
            files[0]["path"].as_str().unwrap().to_owned(),
            files[0].clone(),
        )
    }

    fn release_merge(&self) {
        self.release("cold_compaction.before_run");
    }

    /// 当前快照经 `cold_deletion_segment` 沿 `previous` 可达的冷层变更段（自最新起）。
    fn segments(&self) -> Vec<Value> {
        let ds = self.ds();
        if ds.current().is_none() {
            return Vec::new();
        }
        ds.segments(&ds.head()["cold_deletion_segment"])
    }

    /// 记有合并的冷层变更段（`additions` 不为空），自最新起。
    fn merge_segments(&self) -> Vec<Value> {
        self.segments()
            .into_iter()
            .filter(|s| s["additions"].as_array().is_some_and(|a| !a.is_empty()))
            .collect()
    }

    /// 等待合并完成：轮询直到出现 `additions` 不为空的段（超时 3 分钟），返回最新的那一个。
    fn wait_merge(&self, server: &Server) -> Value {
        self.wait_merges(1, server).remove(0)
    }

    /// 等到记有合并的冷层变更段不少于 `k` 个，返回它们（自最新起）。
    fn wait_merges(&self, k: usize, server: &Server) -> Vec<Value> {
        let (segs, ok) = poll(WAIT, || self.merge_segments(), |v| v.len() >= k);
        assert!(
            ok,
            "{} 秒内没有第 {k} 次合并：{:#?}",
            WAIT.as_secs(),
            server.stderr_lines()
        );
        segs
    }

    /// 冷层当前版本中 `amberlog.operation` 为 `compaction` 的快照。
    fn compactions(&self) -> Vec<Value> {
        let cold = self.cold();
        if cold.version().is_none() {
            return Vec::new();
        }
        ColdDir::snapshots(&cold.current())
            .into_iter()
            .filter(|s| s["summary"]["amberlog.operation"] == "compaction")
            .collect()
    }

    /// 冷层当前快照的存活数据文件（相对冷层表位置）。
    fn live(&self) -> BTreeSet<String> {
        let cold = self.cold();
        let meta = cold.current();
        let Some(current) = ColdDir::snapshots(&meta)
            .into_iter()
            .find(|s| s["snapshot-id"] == meta["current-snapshot-id"])
        else {
            return BTreeSet::new();
        };
        cold.entries(&meta, &current)
            .into_iter()
            .filter(|e| !e.deleted)
            .map(|e| e.path)
            .collect()
    }

    /// 冷层快照 `snapshot` 新增的冷层数据文件（相对冷层表位置）。
    fn added_by(&self, snapshot: &Value) -> BTreeSet<String> {
        let cold = self.cold();
        let meta = cold.current();
        cold.added(&meta, snapshot)
            .into_iter()
            .map(|e| e.path)
            .collect()
    }
}

// ---------------------------------------------------------------------------
// 数据
// ---------------------------------------------------------------------------

/// D 日（此刻所在 UTC 日的前两日）的起点。
fn day_d() -> i64 {
    now_us().div_euclid(DAY) * DAY - 2 * DAY
}

/// D 日 `h:m:s` 时刻（UTC 微秒）。
fn d_at(h: i64, m: i64, s: i64) -> i64 {
    day_d() + h * HOUR + m * MINUTE + s * SECOND
}

/// D 日的 `YYYY-MM-DD` 写法。
fn d_name() -> String {
    let days = day_d().div_euclid(DAY);
    // 1970-01-01 起的天数换算成公历日期（Howard Hinnant 的 civil_from_days）
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", if m <= 2 { y + 1 } else { y })
}

/// 以一个请求向 `app` 摄取 `n` 条记录：第 `i` 条 `_timestamp` 为 `start` 加 `i` 秒、`seq` 为 `first + i`，另加
/// `extra(seq)` 的字段；断言全部受理。
fn ingest_seq(server: &Server, start: i64, first: u64, n: u64, extra: &impl Fn(u64) -> String) {
    let body: Vec<String> = (0..n)
        .map(|i| {
            let seq = first + i;
            format!(
                r#"{{"_timestamp":{},"seq":{seq}{}}}"#,
                start + i as i64 * SECOND,
                extra(seq)
            )
        })
        .collect();
    let r = ingest(server, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], n, "{}", r.text());
}

/// 以覆盖 D 日的时间范围提交 `SELECT seq FROM app`（`size` 为 1000）。
fn query_d(server: &Server) -> common::Resp {
    let body = json!({"query": {
        "sql": "SELECT seq FROM app",
        "start_time": day_d(),
        "end_time": day_d() + DAY,
        "size": 1000
    }});
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    )
}

/// 同 [`query_d`]，断言 200，返回 `seq` 取值（排序）。
fn seqs_d(server: &Server) -> Vec<i64> {
    let r = query_d(server);
    assert_eq!(r.status, 200, "{}", r.text());
    let mut v: Vec<i64> = r.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| h["seq"].as_i64().unwrap())
        .collect();
    v.sort_unstable();
    v
}

fn range(a: i64, b: i64) -> Vec<i64> {
    (a..=b).collect()
}

/// 冷层数据文件 `rel` 中各记录的（`_timestamp`, `seq`）（按行序；`seq` 缺列或为 NULL 时为 `None`）。
fn ts_seq(cold: &ColdDir, rel: &str) -> Vec<(i64, Option<i64>)> {
    common::lake::ts_seq(&cold.batches(rel))
}

/// 冷层数据文件 `rel` 中某整数列的取值（按行序；缺列为全 `None`）。
fn int_column(cold: &ColdDir, rel: &str, name: &str) -> Vec<Option<i64>> {
    let mut out = Vec::new();
    for b in cold.batches(rel) {
        match b.column_by_name(name) {
            Some(col) => {
                let a = col.as_any().downcast_ref::<Int64Array>().unwrap();
                out.extend((0..a.len()).map(|i| (!a.is_null(i)).then(|| a.value(i))));
            }
            None => out.extend(std::iter::repeat_n(None, b.num_rows())),
        }
    }
    out
}

/// 冷层数据文件 `rel` 中 `_timestamp` 的取值（按行序）。
fn timestamps(cold: &ColdDir, rel: &str) -> Vec<i64> {
    let mut out = Vec::new();
    for b in cold.batches(rel) {
        let a = b
            .column_by_name("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap();
        out.extend(a.values().iter().copied());
    }
    out
}

/// 冷层变更段 `seg` 中 `reason` 为 `compaction` 的元素所记路径（排序）。
fn compaction_paths(seg: &Value) -> Vec<String> {
    let mut v: Vec<String> = seg["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "compaction")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    v.sort();
    v
}

/// 冷层变更段 `seg` 的 `additions` 所列路径（排序）。
fn addition_paths(seg: &Value) -> Vec<String> {
    let mut v: Vec<String> = seg["additions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["path"].as_str().unwrap().to_owned())
        .collect();
    v.sort();
    v
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

/// stderr 中 `tier` 为 `cold` 的 `digest_mismatch` 日志。
fn cold_mismatches(server: &Server) -> Vec<Value> {
    server
        .json_lines()
        .into_iter()
        .filter(|l| l["event"] == "digest_mismatch" && l["tier"] == "cold")
        .collect()
}

/// `/metrics` 中是否有 `stream="app"` 的 `amberlog_dataset_disabled` 序列。
fn app_disabled(server: &Server) -> bool {
    let r = server.get_auth("/metrics");
    r.text()
        .lines()
        .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains(r#"stream="app""#))
}

/// 把冷层数据文件 `rel` 改写为记录相同、字节不同的合法 Parquet 文件（换成不压缩、另一种页大小写出）。
fn rewrite_same_records(cold: &ColdDir, rel: &str) {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;

    let batches = cold.batches(rel);
    let before = std::fs::read(cold.path(rel)).unwrap();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_data_page_size_limit(64)
        .build();
    let mut w = ArrowWriter::try_new(Vec::new(), batches[0].schema(), Some(props)).unwrap();
    for b in &batches {
        w.write(b).unwrap();
    }
    let bytes = w.into_inner().unwrap();
    assert_ne!(sha256(&bytes), sha256(&before), "改写后字节应不同");
    std::fs::write(cold.path(rel), bytes).unwrap();
}

// ---------------------------------------------------------------------------
// 满足合并条件的分区以一次合并吃下全部合并候选文件
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_all_candidates_one_merge() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    ingest_seq(&s, d_at(3, 0, 0), 31, 10, &|_| String::new());
    let (cc, _) = c.wait_laked(d_at(3, 0, 0));
    c.release_merge();
    c.wait_merge(&s);
    let compactions = c.compactions();
    assert_eq!(compactions.len(), 1, "{compactions:?}");
    let live = c.live();
    for f in [&small.a, &small.b, &cc] {
        assert!(!live.contains(f), "{f} 应已离开冷层存活文件集");
    }
    let outputs = c.added_by(&compactions[0]);
    let prefix = format!("data/_timestamp_day={}/", d_name());
    let day_live: BTreeSet<String> = live
        .into_iter()
        .filter(|p| p.starts_with(&prefix))
        .collect();
    assert_eq!(day_live, outputs);
    let cold = c.cold();
    let mut seqs: Vec<i64> = outputs
        .iter()
        .flat_map(|o| ts_seq(&cold, o))
        .map(|(_, s)| s.unwrap())
        .collect();
    seqs.sort_unstable();
    assert_eq!(seqs, range(1, 40));
}

// ---------------------------------------------------------------------------
// 文件摘要、合并记录与条数守恒
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_file_digests() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    c.wait_merge(&s);
    let compactions = c.compactions();
    assert_eq!(compactions.len(), 1);
    let raw = compactions[0]["summary"]["amberlog.file_digests"]
        .as_str()
        .expect("有 amberlog.file_digests");
    let obj: BTreeMap<String, String> = serde_json::from_str(raw).unwrap();
    let outputs = c.added_by(&compactions[0]);
    assert_eq!(obj.keys().cloned().collect::<BTreeSet<_>>(), outputs);
    let cold = c.cold();
    for (path, digest) in &obj {
        assert_eq!(digest, &sha256(&cold.read(path)));
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_trace_segment() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    c.release_merge();
    let seg = c.wait_merge(&s);
    let compaction = c.compactions().remove(0);
    let id = compaction["snapshot-id"].clone();
    let dels = seg["deletions"].as_array().unwrap();
    assert_eq!(dels.len(), 2, "{dels:?}");
    for (path, entry) in [(&small.a, &small.a_entry), (&small.b, &small.b_entry)] {
        let d = dels
            .iter()
            .find(|d| d["path"] == path.as_str())
            .unwrap_or_else(|| panic!("段里没有 {path}：{dels:?}"));
        assert_eq!(d["content_digest"], entry["content_digest"]);
        assert_eq!(d["max_timestamp"], entry["max_timestamp"]);
        assert_eq!(d["reason"], "compaction");
        assert_eq!(d["cutoff"], Value::Null);
        assert_eq!(d["cold_removed_in"], id);
    }
    assert_eq!(
        seg["additions"].as_array().unwrap().len(),
        c.added_by(&compaction).len()
    );
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_merge_record_values() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    let seg = c.wait_merge(&s);
    let compaction = c.compactions().remove(0);
    let digests: BTreeMap<String, String> = serde_json::from_str(
        compaction["summary"]["amberlog.file_digests"]
            .as_str()
            .unwrap(),
    )
    .unwrap();
    let cold = c.cold();
    let meta = cold.current();
    let entries = cold.entries(&meta, &compaction);
    for a in seg["additions"].as_array().unwrap() {
        let keys: BTreeSet<&str> = a.as_object().unwrap().keys().map(String::as_str).collect();
        assert_eq!(
            keys,
            BTreeSet::from([
                "path",
                "content_digest",
                "record_count",
                "size_bytes",
                "min_timestamp",
                "max_timestamp"
            ])
        );
        let path = a["path"].as_str().unwrap();
        assert_eq!(
            a["content_digest"].as_str(),
            digests.get(path).map(String::as_str)
        );
        let e = entries.iter().find(|e| e.path == path).expect("清单条目");
        assert_eq!(a["record_count"].as_u64(), Some(e.record_count));
        assert_eq!(a["size_bytes"].as_u64(), Some(e.file_size_in_bytes));
        let ts = timestamps(&cold, path);
        assert_eq!(a["min_timestamp"].as_i64(), ts.iter().min().copied());
        assert_eq!(a["max_timestamp"].as_i64(), ts.iter().max().copied());
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_record_count_conserved() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    c.release_merge();
    let seg = c.wait_merge(&s);
    let entries = BTreeMap::from([
        (small.a.clone(), small.a_entry.clone()),
        (small.b.clone(), small.b_entry.clone()),
    ]);
    let removed: u64 = compaction_paths(&seg)
        .iter()
        .map(|p| entries[p]["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(removed, 30);
    let added: u64 = seg["additions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(added, 30);
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_summary_cold() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    let before = c.ds().head()["summary"]["cold"].clone();
    assert_eq!(before["record_count"], 30, "{before}");
    c.release_merge();
    let seg = c.wait_merge(&s);
    let after = c.ds().head()["summary"]["cold"].clone();
    assert_eq!(after["record_count"], 30);
    assert_eq!(after["time_min"], before["time_min"]);
    assert_eq!(after["time_max"], before["time_max"]);
    let size: u64 = seg["additions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|a| a["size_bytes"].as_u64().unwrap())
        .sum();
    assert_eq!(after["size_bytes"].as_u64(), Some(size));
}

// ---------------------------------------------------------------------------
// 合并产出：记录不变、同一日分区、按时间排序、一次提交
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_records_preserved() {
    let c = Ctl::new();
    let (s, _small) = c.two_small_with(&Run::base(), |seq| {
        if seq > 10 {
            format!(r#","x":{}"#, seq * 7)
        } else {
            String::new()
        }
    });
    c.release_merge();
    let seg = c.wait_merge(&s);
    let cold = c.cold();
    let mut got: Vec<(i64, Option<i64>, Option<i64>)> = Vec::new();
    for o in addition_paths(&seg) {
        let ts_seqs = ts_seq(&cold, &o);
        let xs = int_column(&cold, &o, "x");
        for ((ts, seq), x) in ts_seqs.into_iter().zip(xs) {
            got.push((ts, seq, x));
        }
    }
    got.sort();
    let mut want: Vec<(i64, Option<i64>, Option<i64>)> = (1..=10)
        .map(|i| (d_at(1, 0, 0) + (i - 1) * SECOND, Some(i), None))
        .chain((11..=30).map(|i| (d_at(2, 30, 0) + (i - 11) * SECOND, Some(i), Some(i * 7))))
        .collect();
    want.sort();
    assert_eq!(got, want);
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_same_day() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    let seg = c.wait_merge(&s);
    let prefix = format!("data/_timestamp_day={}/", d_name());
    for p in addition_paths(&seg) {
        assert!(p.starts_with(&prefix), "{p}");
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_sorted() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    let seg = c.wait_merge(&s);
    let cold = c.cold();
    for p in addition_paths(&seg) {
        let ts = timestamps(&cold, &p);
        assert!(ts.windows(2).all(|w| w[0] <= w[1]), "{p}：{ts:?}");
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_one_commit() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    ingest_seq(&s, d_at(5, 0, 0) - DAY, 41, 10, &|_| String::new());
    let (e, _) = c.wait_laked(d_at(5, 0, 0) - DAY);
    let cold = c.cold();
    let n = cold.version().unwrap();
    let live_before = c.live();
    c.release_merge();
    c.wait_merge(&s);
    let (before, after) = (cold.metadata(n), cold.metadata(n + 1));
    let ids = |m: &Value| -> BTreeSet<i64> {
        ColdDir::snapshots(m)
            .iter()
            .map(|s| s["snapshot-id"].as_i64().unwrap())
            .collect()
    };
    let new: Vec<i64> = ids(&after).difference(&ids(&before)).copied().collect();
    assert_eq!(new.len(), 1, "v{} 应比 v{n} 恰多一个快照", n + 1);
    let snap = ColdDir::snapshots(&after)
        .into_iter()
        .find(|s| s["snapshot-id"] == new[0])
        .unwrap();
    assert_eq!(snap["summary"]["amberlog.operation"], "compaction");
    let current = ColdDir::snapshots(&after)
        .into_iter()
        .find(|s| s["snapshot-id"] == after["current-snapshot-id"])
        .unwrap();
    let live_after: BTreeSet<String> = cold
        .entries(&after, &current)
        .into_iter()
        .filter(|x| !x.deleted)
        .map(|x| x.path)
        .collect();
    let mut want = live_before.clone();
    want.remove(&small.a);
    want.remove(&small.b);
    want.extend(cold.added(&after, &snap).into_iter().map(|x| x.path));
    assert_eq!(live_after, want);
    assert!(live_after.contains(&e));
}

// ---------------------------------------------------------------------------
// 够格分区满足合并条件之后的时限
// ---------------------------------------------------------------------------

#[test]
fn cold_compaction_deadline() {
    let c = Ctl::new();
    let s = c.start(&Run::base());
    s.wait_readyz(200, WAIT);
    ingest_seq(&s, d_at(1, 0, 0), 1, 10, &|_| String::new());
    ingest_seq(&s, d_at(2, 0, 0), 11, 10, &|_| String::new());
    c.wait_laked(d_at(1, 0, 0));
    c.wait_laked(d_at(2, 0, 0));
    let m = c.ds().bucket_now(&bucket_label(d_at(2, 0, 0))).unwrap();
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    let t = c.ds().snapshot(id)["created_at"].as_i64().unwrap();
    let deadline = t + 2 * 60 * SECOND;
    let (segs, ok) = poll(
        Duration::from_secs(150),
        || c.merge_segments(),
        |v| !v.is_empty(),
    );
    assert!(ok, "两分钟内没有合并：{:#?}", s.stderr_lines());
    let seg = segs.last().unwrap();
    assert!(
        seg["created_at"].as_i64().unwrap() < deadline,
        "合并晚于时限：{seg}"
    );
    assert_eq!(c.compactions().len(), 1);
}

// ---------------------------------------------------------------------------
// 先冷层合并提交，后写冷层变更段，最后物理删除输入
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_step_order() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base().with("storage.delete_delay", "1s"));
    c.hold("cold_compaction.after_cold_commit");
    c.release_merge();
    c.wait_held("cold_compaction.after_cold_commit", &s);
    let compactions = c.compactions();
    assert_eq!(compactions.len(), 1, "暂停期间应已有冷层合并快照");
    let ms = compactions[0]["timestamp-ms"].as_i64().unwrap();
    assert_eq!(c.ds().head()["cold_deletion_segment"], Value::Null);
    let cold = c.cold();
    assert!(cold.exists(&small.a) && cold.exists(&small.b));
    c.release("cold_compaction.after_cold_commit");
    let seg = c.wait_merge(&s);
    for d in seg["deletions"].as_array().unwrap() {
        let id = d["removed_in"].as_u64().unwrap();
        let created = c.ds().snapshot(id)["created_at"].as_i64().unwrap();
        assert!(created >= ms * 1000, "removed_in 所指快照早于冷层合并快照");
    }
    let deadline = seg["deletions"][0]["deleted_at"].as_i64().unwrap() + SECOND + HOUR;
    let (_, ok) = poll(
        Duration::from_secs(240),
        || cold.exists(&small.a) || cold.exists(&small.b),
        |e| !e,
    );
    assert!(
        ok && now_us() < deadline,
        "合并输入没有被物理删除：{:#?}",
        s.stderr_lines()
    );
}

// ---------------------------------------------------------------------------
// 合并输入的摘要核对：字节不同即放弃、停止、事件与查询应答
// ---------------------------------------------------------------------------

/// 两个小文件的实例以 SIGTERM 退出，`replace` 所列文件换成记录相同、字节不同的合法 Parquet 文件，再以同一配置重启
/// （冷层合并不暂停）。
fn replaced_and_restarted(c: &Ctl, replace: impl Fn(&Small) -> Vec<String>) -> (Server, Small) {
    let (mut s, small) = c.two_small(&Run::base());
    stop(&mut s);
    let cold = c.cold();
    for p in replace(&small) {
        rewrite_same_records(&cold, &p);
    }
    c.release_merge();
    (c.start(&Run::base()), small)
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_all_inputs() {
    let c = Ctl::new();
    let (s, small) = replaced_and_restarted(&c, |x| vec![x.a.clone(), x.b.clone()]);
    let (lines, ok) = poll(
        Duration::from_secs(60),
        || cold_mismatches(&s),
        |v| v.len() >= 2,
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    let paths: BTreeSet<String> = lines
        .iter()
        .inspect(|l| assert_eq!(l["dataset"], "default/app"))
        .map(|l| l["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(paths, BTreeSet::from([small.a, small.b]));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_abort() {
    let c = Ctl::new();
    let (s, small) = replaced_and_restarted(&c, |x| vec![x.a.clone(), x.b.clone()]);
    let (_, ok) = poll(
        Duration::from_secs(60),
        || cold_mismatches(&s),
        |v| v.len() >= 2,
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    assert!(c.compactions().is_empty());
    let live = c.live();
    assert!(live.contains(&small.a) && live.contains(&small.b));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_stop() {
    let c = Ctl::new();
    let (s, _small) = replaced_and_restarted(&c, |x| vec![x.a.clone(), x.b.clone()]);
    let (_, ok) = poll(
        Duration::from_secs(60),
        || cold_mismatches(&s),
        |v| v.len() >= 2,
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    std::thread::sleep(Duration::from_secs(60));
    assert_eq!(cold_mismatches(&s).len(), 2, "{:#?}", s.stderr_lines());
    assert!(c.compactions().is_empty());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_event() {
    let c = Ctl::new();
    let (s, small) = replaced_and_restarted(&c, |x| vec![x.a.clone()]);
    let (_, ok) = poll(
        Duration::from_secs(60),
        || cold_mismatches(&s),
        |v| !v.is_empty(),
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    std::thread::sleep(Duration::from_secs(15));
    let lines = cold_mismatches(&s);
    assert_eq!(lines.len(), 1, "{lines:?}");
    let l = &lines[0];
    assert_eq!(l["level"], "error");
    assert_eq!(l["dataset"], "default/app");
    assert_eq!(l["tier"], "cold");
    assert_eq!(l["path"], small.a.as_str());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_answer_unchanged() {
    let c = Ctl::new();
    let (s, _small) = replaced_and_restarted(&c, |x| vec![x.a.clone()]);
    let (_, ok) = poll(
        Duration::from_secs(60),
        || cold_mismatches(&s),
        |v| !v.is_empty(),
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    assert_eq!(seqs_d(&s), range(1, 30));
    let r = ingest(&s, APP, &format!(r#"[{{"_timestamp":{}}}]"#, now_us()));
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_digest_bytes_used() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    let cold = c.cold();
    let mut want: Vec<(i64, Option<i64>)> = ts_seq(&cold, &small.a)
        .into_iter()
        .chain(ts_seq(&cold, &small.b))
        .collect();
    want.sort();
    c.hold("cold_compaction.after_digest_check");
    c.release_merge();
    c.wait_held("cold_compaction.after_digest_check", &s);
    common::lake::alter_parquet_rows(&cold.path(&small.a));
    c.release("cold_compaction.after_digest_check");
    let seg = c.wait_merge(&s);
    let mut got: Vec<(i64, Option<i64>)> = addition_paths(&seg)
        .iter()
        .flat_map(|o| ts_seq(&cold, o))
        .collect();
    got.sort();
    assert_eq!(got, want);
}

// ---------------------------------------------------------------------------
// 多个合并产出按时间顺序切分、不超过目标大小
// ---------------------------------------------------------------------------

/// 不易压缩的随机字符串（64 个字符的字母表，按 `seed` 确定）。
fn pad(seed: u64, len: usize) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            ALPHABET[(x % 64) as usize] as char
        })
        .collect()
}

/// 以一个请求向 `app` 摄取 `n` 条带 `pad`（长 `len`）的记录：`_timestamp` 自 `start` 起每隔 1 毫秒，`seq` 自 `first` 起。
fn ingest_padded(server: &Server, start: i64, first: u64, n: u64, len: usize) {
    let body: Vec<String> = (0..n)
        .map(|i| {
            let seq = first + i;
            format!(
                r#"{{"_timestamp":{},"seq":{seq},"pad":"{}"}}"#,
                start + i as i64 * 1000,
                pad(seq, len)
            )
        })
        .collect();
    let r = ingest(server, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], n, "{}", r.text());
}

/// 「多个合并产出按时间顺序切分」的实例：以 `compaction.target_file_bytes` 为 1 MiB 启动，A 与 B 各含 3000 条带随机字符串
/// `pad` 的记录（A、B 各小于 1 MiB，两者之和大于 1 MiB），等两个桶入湖完成。
fn two_padded(c: &Ctl) -> (Server, Small) {
    c.hold("cold_compaction.before_run");
    let s = c.start(&Run::base().raw("compaction.target_file_bytes", "1048576"));
    ingest_padded(&s, d_at(1, 0, 0), 1, 3000, 360);
    ingest_padded(&s, d_at(2, 30, 0), 3001, 3000, 360);
    let (a, a_entry) = c.wait_laked(d_at(1, 0, 0));
    let (b, b_entry) = c.wait_laked(d_at(2, 30, 0));
    for e in [&a_entry, &b_entry] {
        assert!(e["size_bytes"].as_u64().unwrap() < 1_048_576, "{e}");
    }
    assert!(
        a_entry["size_bytes"].as_u64().unwrap() + b_entry["size_bytes"].as_u64().unwrap()
            > 1_048_576
    );
    (
        s,
        Small {
            a,
            b,
            a_entry,
            b_entry,
        },
    )
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_sorted_across() {
    let c = Ctl::new();
    let (s, _small) = two_padded(&c);
    c.release_merge();
    let seg = c.wait_merge(&s);
    let cold = c.cold();
    let mut spans: Vec<(i64, i64)> = addition_paths(&seg)
        .iter()
        .map(|o| {
            let ts = timestamps(&cold, o);
            (*ts.iter().min().unwrap(), *ts.iter().max().unwrap())
        })
        .collect();
    assert!(spans.len() >= 2, "{spans:?}");
    spans.sort();
    assert!(spans.windows(2).all(|w| w[0].1 <= w[1].0), "{spans:?}");
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.compactions().len(), 1);
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_output_size() {
    let c = Ctl::new();
    let (s, _small) = two_padded(&c);
    c.release_merge();
    let seg = c.wait_merge(&s);
    let cold = c.cold();
    for o in addition_paths(&seg) {
        let n = cold.read(&o).len();
        assert!(n <= 1_048_576, "{o} 有 {n} 字节");
    }
}

// ---------------------------------------------------------------------------
// 只有冷层合并的冷层变更段带合并记录
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_additions_only_compaction() {
    let c = Ctl::new();
    let (mut s, _small) = c.two_small(&Run::base());
    let old = now_us() - 30 * DAY;
    ingest_seq(&s, old, 101, 10, &|_| String::new());
    let (f, _) = c.wait_laked(old);
    stop(&mut s);
    c.hold("cold_compaction.before_run");
    let run = Run::base()
        .with("retention.period", "10d")
        .with("retention.check_interval", "10s");
    let s = c.start(&run);
    c.release_merge();
    c.wait_merge(&s);
    let (_, ok) = poll(
        WAIT,
        || {
            c.segments().iter().any(|g| {
                g["deletions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|d| d["reason"] == "ttl" && d["path"] == f.as_str())
            })
        },
        |x| *x,
    );
    assert!(ok, "TTL 没有移出 F：{:#?}", s.stderr_lines());
    let compaction = c.compactions().remove(0);
    let outputs: Vec<String> = c.added_by(&compaction).into_iter().collect();
    for g in c.segments() {
        let reasons: BTreeSet<&str> = g["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|d| d["reason"].as_str().unwrap())
            .collect();
        if reasons.contains("ttl") {
            assert_eq!(g["additions"], json!([]), "{g}");
        }
        if reasons.contains("compaction") {
            assert_eq!(addition_paths(&g), outputs);
        }
    }
}

// ---------------------------------------------------------------------------
// 合并的任一阶段查询都恰返回每条记录一次
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_exactly_once() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base().with("storage.delete_delay", "1s"));
    c.hold("cold_compaction.after_upload");
    c.hold("cold_compaction.after_cold_commit");
    c.release_merge();
    c.wait_held("cold_compaction.after_upload", &s);
    assert_eq!(seqs_d(&s), range(1, 30), "上传之后、冷层合并提交之前");
    c.release("cold_compaction.after_upload");
    c.wait_held("cold_compaction.after_cold_commit", &s);
    assert_eq!(seqs_d(&s), range(1, 30), "冷层合并提交之后、写段之前");
    c.release("cold_compaction.after_cold_commit");
    c.wait_merge(&s);
    assert_eq!(seqs_d(&s), range(1, 30), "合并完成之后");
    let cold = c.cold();
    let (_, ok) = poll(
        Duration::from_secs(240),
        || cold.exists(&small.a) || cold.exists(&small.b),
        |e| !e,
    );
    assert!(ok, "合并输入没有被物理删除");
    assert_eq!(seqs_d(&s), range(1, 30), "合并输入物理删除之后");
}

// ---------------------------------------------------------------------------
// 数据集停用与停机
// ---------------------------------------------------------------------------

/// D 日分区之下的 `.parquet` 文件（相对冷层表位置）。
fn day_files(c: &Ctl) -> BTreeSet<String> {
    let prefix = format!("data/_timestamp_day={}/", d_name());
    c.cold()
        .keys()
        .into_iter()
        .filter(|k| k.starts_with(&prefix) && k.ends_with(".parquet"))
        .collect()
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_disabled_skipped() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base());
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
    // 运行中的实例缓存读过的桶 manifest：停下后删除、再以同一配置重启，查询才读到这次删除（与审计不变量测试同类场景的做法相同）
    let head = ds.head();
    let r = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label.as_str())
        .unwrap()
        .clone();
    ds.remove(r["manifest"].as_str().unwrap());
    c.hold("cold_compaction.before_run");
    let s = c.start(&Run::base());
    let body = json!({"query": {"sql": "SELECT * FROM app", "start_time": now - SECOND, "end_time": now + SECOND, "size": 10}});
    let q = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(q.status, 500, "{}", q.text());
    c.release_merge();
    std::thread::sleep(Duration::from_secs(60));
    assert!(c.compactions().is_empty());
    assert_eq!(day_files(&c), BTreeSet::from([small.a, small.b]));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_no_start_on_shutdown() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base().pause("shutdown.before_exit"));
    s.signal("TERM");
    c.release_merge();
    assert!(
        wait_file(&c.marker(), Duration::from_secs(60)),
        "{:#?}",
        s.stderr_lines()
    );
    // 停在退出之前：其间若开始了新的冷层合并，检查间隔 10 秒内就会写出冷层合并提交
    std::thread::sleep(Duration::from_secs(30));
    s.sigkill();
    assert!(c.compactions().is_empty());
    assert_eq!(day_files(&c), BTreeSet::from([small.a, small.b]));
}

// ---------------------------------------------------------------------------
// 定期检查，首次检查有上界
// ---------------------------------------------------------------------------

/// 时刻 `ts` 所在小时桶的交接记录所在快照的 `created_at`。
fn handover_created_at(c: &Ctl, ts: i64) -> i64 {
    let m = c.ds().bucket_now(&bucket_label(ts)).unwrap();
    let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
    c.ds().snapshot(id)["created_at"].as_i64().unwrap()
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_repeated_checks() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    let first = c.wait_merge(&s);
    let o1 = addition_paths(&first);
    assert_eq!(o1.len(), 1, "{first}");
    ingest_seq(&s, d_at(3, 0, 0), 31, 10, &|_| String::new());
    ingest_seq(&s, d_at(4, 0, 0), 41, 10, &|_| String::new());
    let (c3, _) = c.wait_laked(d_at(3, 0, 0));
    let (c4, _) = c.wait_laked(d_at(4, 0, 0));
    let t = handover_created_at(&c, d_at(4, 0, 0));
    let deadline = t + 2 * MINUTE;
    let (segs, ok) = poll(
        Duration::from_secs(150),
        || c.merge_segments(),
        |v| {
            v.len() >= 2
                && v.iter()
                    .flat_map(compaction_paths)
                    .collect::<BTreeSet<_>>()
                    .is_superset(&BTreeSet::from([o1[0].clone(), c3.clone(), c4.clone()]))
        },
    );
    assert!(ok, "两次够格的合并没有都完成：{segs:#?}");
    for g in &segs[..segs.len() - 1] {
        assert!(g["created_at"].as_i64().unwrap() < deadline, "{g}");
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_first_check_after_start() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base());
    stop(&mut s);
    c.release_merge();
    let s = c.start(&Run::base().with("cold_compaction.check_interval", "1h"));
    s.wait_readyz(200, WAIT);
    let t0 = now_us();
    let (segs, ok) = poll(
        Duration::from_secs(13 * 60),
        || c.merge_segments(),
        |v| !v.is_empty(),
    );
    assert!(ok, "13 分钟内没有合并：{:#?}", s.stderr_lines());
    assert!(segs[0]["created_at"].as_i64().unwrap() < t0 + 12 * MINUTE);
    let compactions = c.compactions();
    assert_eq!(compactions.len(), 1);
    let live = c.live();
    assert!(!live.contains(&small.a) && !live.contains(&small.b));
}

// ---------------------------------------------------------------------------
// 只合并够格日分区中满足合并条件时的合并候选文件
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_ineligible_day_skipped() {
    let c = Ctl::new();
    // 测试台架没有可控时钟：以时钟偏移把被测进程的此刻放在所在 UTC 日的 12:00（不早于 04:00，不必等），
    // 只改执行方式、不改断言
    let real = now_us();
    let noon = real.div_euclid(DAY) * DAY + 12 * HOUR;
    let offset_s = (noon - real).div_euclid(SECOND);
    let s = c.start(&Run::base().offset(offset_s));
    let now = real + offset_s * SECOND;
    let h3 = now - 3 * HOUR;
    let h2 = now - 2 * HOUR;
    ingest_seq(&s, h3, 1, 10, &|_| String::new());
    ingest_seq(&s, h2, 11, 10, &|_| String::new());
    c.wait_laked(h3);
    c.wait_laked(h2);
    std::thread::sleep(Duration::from_secs(60));
    assert!(c.compactions().is_empty(), "{:?}", c.compactions());
    assert!(c.ds().keys_under("manifest/cold-deletions/").is_empty());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_half_target_output_skipped() {
    let c = Ctl::new();
    c.hold("cold_compaction.before_run");
    let s = c.start(&Run::base().raw("compaction.target_file_bytes", "1048576"));
    ingest_padded(&s, d_at(1, 0, 0), 1, 4000, 200);
    ingest_seq(&s, d_at(2, 30, 0), 4001, 20, &|_| String::new());
    c.wait_laked(d_at(1, 0, 0));
    c.wait_laked(d_at(2, 30, 0));
    c.release_merge();
    let first = c.wait_merge(&s);
    let o = addition_paths(&first);
    assert_eq!(o.len(), 1, "{first}");
    let size = c.cold().read(&o[0]).len();
    assert!(
        (524_288..1_048_576).contains(&size),
        "O 有 {size} 字节，应在 [524288, 1048576) 内（调整 pad 的长度）"
    );
    ingest_seq(&s, d_at(5, 0, 0), 5001, 10, &|_| String::new());
    let (cc, _) = c.wait_laked(d_at(5, 0, 0));
    std::thread::sleep(Duration::from_secs(30));
    assert_eq!(c.compactions().len(), 1, "D 日分区的合并候选文件只有 C");
    ingest_seq(&s, d_at(6, 0, 0), 6001, 10, &|_| String::new());
    let (e, _) = c.wait_laked(d_at(6, 0, 0));
    let (segs, ok) = poll(
        Duration::from_secs(120),
        || c.merge_segments(),
        |v| v.len() >= 2,
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    assert_eq!(c.compactions().len(), 2);
    assert_eq!(compaction_paths(&segs[0]), sorted(vec![cc, e]));
    assert!(c.live().contains(&o[0]));
}

#[test]
fn cold_compaction_large_files_skipped() {
    let c = Ctl::new();
    let s = c.start(&Run::base().raw("compaction.target_file_bytes", "1"));
    ingest_seq(&s, d_at(1, 0, 0), 1, 2, &|_| String::new());
    ingest_seq(&s, d_at(2, 0, 0), 3, 2, &|_| String::new());
    for ts in [d_at(1, 0, 0), d_at(2, 0, 0)] {
        c.ds().wait_handovers(&bucket_label(ts), 1, WAIT);
    }
    std::thread::sleep(Duration::from_secs(60));
    assert!(c.compactions().is_empty());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_window_file_skipped() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    c.hold("lake.after_cold_commit");
    ingest_seq(&s, d_at(3, 0, 0), 31, 10, &|_| String::new());
    c.wait_held("lake.after_cold_commit", &s);
    let cc = window_file(&c);
    c.release_merge();
    let seg = c.wait_merge(&s);
    assert_eq!(compaction_paths(&seg), sorted(vec![small.a, small.b]));
    assert!(c.live().contains(&cc));
}

/// 处于交接窗口的那次入湖冷层提交新增的冷层数据文件（D 日 03 时的桶，入湖停在 `lake.after_cold_commit`）。
fn window_file(c: &Ctl) -> String {
    let cold = c.cold();
    let meta = cold.current();
    let l = ColdDir::lake_snapshots(&meta, &bucket_label(d_at(3, 0, 0)))
        .pop()
        .expect("D 日 03 时的入湖冷层提交");
    let added = cold.added(&meta, &l);
    assert_eq!(added.len(), 1, "{added:?}");
    added[0].path.clone()
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_expired_file_skipped() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base());
    ingest_seq(&s, d_at(23, 0, 0), 31, 10, &|_| String::new());
    let (cc, _) = c.wait_laked(d_at(23, 0, 0));
    stop(&mut s);
    // cutoff 落在 A 与 B 的最大 `_timestamp` 之间（D 日 02:00）
    let period_s = (now_us() - d_at(2, 0, 0)).div_euclid(SECOND);
    c.hold("cold_compaction.before_run");
    c.hold("ttl.before_run");
    let s = c.start(&Run::base().with("retention.period", &format!("{period_s}s")));
    c.release_merge();
    let seg = c.wait_merge(&s);
    assert_eq!(compaction_paths(&seg), sorted(vec![small.b, cc]));
    assert!(c.live().contains(&small.a));
}

// ---------------------------------------------------------------------------
// 提交前按所基于的冷层当前版本重核合并输入仍存活
// ---------------------------------------------------------------------------

/// 使 A 在此后 `secs` 秒时整体到期、B 在此后 30 分钟内不整体到期的 `retention.period`。
fn a_expires_in(small: &Small, secs: i64) -> String {
    let a_max = small.a_entry["max_timestamp"].as_i64().unwrap();
    format!("{}s", (now_us() + secs * SECOND - a_max).div_euclid(SECOND))
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_recheck_abandons() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base());
    stop(&mut s);
    c.hold("cold_compaction.before_run");
    c.hold("cold_compaction.after_upload");
    let run = Run::base()
        .with("retention.period", &a_expires_in(&small, 60))
        .with("retention.check_interval", "10s")
        .with("hot_fsck.interval", "5s")
        .with("hot_fsck.min_orphan_age", "1s");
    let s = c.start(&run);
    c.release_merge();
    c.wait_held("cold_compaction.after_upload", &s);
    let (_, ok) = poll(
        Duration::from_secs(150),
        || c.live().contains(&small.a),
        |l| !l,
    );
    assert!(ok, "A 没有被 TTL 移出：{:#?}", s.stderr_lines());
    c.release("cold_compaction.after_upload");
    std::thread::sleep(Duration::from_secs(60));
    assert!(c.compactions().is_empty());
    assert!(c.live().contains(&small.b));
    let others: BTreeSet<String> = day_files(&c)
        .into_iter()
        .filter(|p| *p != small.a && *p != small.b)
        .collect();
    assert!(
        others.is_empty(),
        "合并产出应已被冷层孤儿清理删除：{others:?}"
    );
    assert!(!app_disabled(&s));
}

// ---------------------------------------------------------------------------
// 改写清单时其余条目保留原快照号与序号
// ---------------------------------------------------------------------------

/// 冷层快照 `snapshot` 的清单中路径为 `rel` 的条目的（`snapshot_id`, `sequence_number`, `file_sequence_number`），
/// 未写的按清单文件继承（Iceberg 规范）。
fn entry_ids(cold: &ColdDir, meta: &Value, snapshot: &Value, rel: &str) -> (i64, i64, i64) {
    use iceberg::spec::{FormatVersion, Manifest, ManifestList};
    let list_rel = ColdDir::rel(meta, snapshot["manifest-list"].as_str().unwrap());
    let list = ManifestList::parse_with_version(&cold.read(&list_rel), FormatVersion::V2).unwrap();
    for f in list.entries() {
        let m = Manifest::parse_avro(&cold.read(&ColdDir::rel(meta, &f.manifest_path))).unwrap();
        for e in m.entries() {
            if ColdDir::rel(meta, e.file_path()) == rel && e.is_alive() {
                let seq = e.sequence_number.unwrap_or(f.sequence_number);
                return (
                    e.snapshot_id.unwrap_or(f.added_snapshot_id),
                    seq,
                    e.file_sequence_number.unwrap_or(seq),
                );
            }
        }
    }
    panic!("{rel} 不在快照 {} 的清单中", snapshot["snapshot-id"]);
}

fn current_snapshot(meta: &Value) -> Value {
    ColdDir::snapshots(meta)
        .into_iter()
        .find(|s| s["snapshot-id"] == meta["current-snapshot-id"])
        .unwrap()
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_keep_entries_window() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.hold("lake.after_cold_commit");
    ingest_seq(&s, d_at(3, 0, 0), 31, 10, &|_| String::new());
    c.wait_held("lake.after_cold_commit", &s);
    let cc = window_file(&c);
    let cold = c.cold();
    let meta = cold.current();
    let l = ColdDir::lake_snapshots(&meta, &bucket_label(d_at(3, 0, 0)))
        .pop()
        .unwrap();
    let before = entry_ids(&cold, &meta, &current_snapshot(&meta), &cc);
    c.release_merge();
    c.wait_merge(&s);
    let meta = cold.current();
    let after = entry_ids(&cold, &meta, &current_snapshot(&meta), &cc);
    assert_eq!(after, before);
    assert_eq!(Some(after.0), l["snapshot-id"].as_i64());
    c.release("lake.after_cold_commit");
    let m = c.ds().wait_handovers(&bucket_label(d_at(3, 0, 0)), 1, WAIT);
    let files: Vec<&str> = m["handovers"][0]["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap())
        .collect();
    assert_eq!(files, vec![cc.as_str()]);
    assert_eq!(seqs_d(&s), range(1, 40));
}

// ---------------------------------------------------------------------------
// 两次提交都成功之后合并输入延后物理删除
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_inputs_deleted_after_delay() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base().with("storage.delete_delay", "1m"));
    c.release_merge();
    let seg = c.wait_merge(&s);
    let t = seg["deletions"][0]["deleted_at"].as_i64().unwrap();
    let cold = c.cold();
    let wait_until = |at: i64| {
        let d = at - now_us();
        if d > 0 {
            std::thread::sleep(Duration::from_micros(d as u64));
        }
    };
    wait_until(t + 30 * SECOND);
    assert!(
        cold.exists(&small.a) && cold.exists(&small.b),
        "未到可删时刻就删除了合并输入"
    );
    let (_, ok) = poll(
        Duration::from_secs(240),
        || cold.exists(&small.a) || cold.exists(&small.b),
        |e| !e,
    );
    assert!(ok && now_us() < t + MINUTE + HOUR, "合并输入没有被物理删除");
}

// ---------------------------------------------------------------------------
// 补写冷层变更段
// ---------------------------------------------------------------------------

/// 冷层变更段中 `cold_removed_in` 为 `id` 的元素所记路径（排序）。
fn removed_in(segs: &[Value], id: &Value) -> Vec<String> {
    let mut v: Vec<String> = segs
        .iter()
        .flat_map(|g| g["deletions"].as_array().unwrap().clone())
        .filter(|d| &d["cold_removed_in"] == id)
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    v.sort();
    v
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_recover_after_crash() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base().pause("cold_compaction.after_cold_commit"));
    c.release_merge();
    common::store::kill_at_marker(&mut s, &c.marker());
    let compactions = c.compactions();
    assert_eq!(compactions.len(), 1, "崩溃前应已写出冷层合并提交");
    let cc = compactions[0].clone();
    let id = cc["snapshot-id"].clone();
    let digests: BTreeMap<String, String> =
        serde_json::from_str(cc["summary"]["amberlog.file_digests"].as_str().unwrap()).unwrap();
    let s = c.start(&Run::base());
    s.wait_readyz(200, WAIT);
    let (segs, ok) = poll(
        Duration::from_secs(130),
        || c.segments(),
        |v| !removed_in(v, &id).is_empty(),
    );
    assert!(ok, "重启后 130 秒内没有补写：{:#?}", s.stderr_lines());
    assert_eq!(removed_in(&segs, &id), sorted(vec![small.a, small.b]));
    let seg = segs
        .iter()
        .find(|g| !removed_in(std::slice::from_ref(*g), &id).is_empty())
        .unwrap();
    for d in seg["deletions"].as_array().unwrap() {
        assert_eq!(d["reason"], "compaction");
    }
    let outputs = c.added_by(&cc);
    assert_eq!(
        addition_paths(seg).into_iter().collect::<BTreeSet<_>>(),
        outputs
    );
    for a in seg["additions"].as_array().unwrap() {
        assert_eq!(
            a["content_digest"].as_str(),
            digests.get(a["path"].as_str().unwrap()).map(String::as_str)
        );
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_retry_before_next_merge() {
    let c = Ctl::new();
    let (s, small) = c.two_small(&Run::base());
    c.faults("write default/app/manifest/cold-deletions/ 403\n");
    c.release_merge();
    let (cs, ok) = poll(WAIT, || c.compactions(), |v| !v.is_empty());
    assert!(ok, "{:#?}", s.stderr_lines());
    let c1 = cs[0]["snapshot-id"].clone();
    ingest_seq(&s, d_at(5, 0, 0), 51, 10, &|_| String::new());
    ingest_seq(&s, d_at(6, 0, 0), 61, 10, &|_| String::new());
    c.wait_laked(d_at(5, 0, 0));
    c.wait_laked(d_at(6, 0, 0));
    std::thread::sleep(Duration::from_secs(60));
    let cs = c.compactions();
    assert_eq!(cs.len(), 1, "补写未成功时不应写出新的冷层合并提交：{cs:?}");
    c.faults("");
    let lifted = Instant::now();
    let (segs, ok) = poll(
        Duration::from_secs(140),
        || c.segments(),
        |v| removed_in(v, &c1) == sorted(vec![small.a.clone(), small.b.clone()]),
    );
    assert!(
        ok && lifted.elapsed() <= Duration::from_secs(140),
        "{:#?}",
        s.stderr_lines()
    );
    let traced = segs
        .iter()
        .find(|g| !removed_in(std::slice::from_ref(*g), &c1).is_empty())
        .unwrap();
    let traced_ms = traced["created_at"].as_i64().unwrap() / 1000;
    std::thread::sleep(Duration::from_secs(40));
    for x in c.compactions() {
        if x["snapshot-id"] != c1 {
            assert!(x["timestamp-ms"].as_i64().unwrap() >= traced_ms, "{x}");
        }
    }
}

// ---------------------------------------------------------------------------
// 对象存储不可访问、读到已移走的文件与热层读取
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_inaccessible_abandons() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.hold("cold_compaction.after_digest_check");
    c.release_merge();
    c.wait_held("cold_compaction.after_digest_check", &s);
    c.faults("* * 503\n");
    c.release("cold_compaction.after_digest_check");
    std::thread::sleep(Duration::from_secs(30));
    c.faults("");
    std::thread::sleep(Duration::from_secs(180));
    assert!(!app_disabled(&s));
    let cs = c.compactions();
    assert_eq!(cs.len(), 1, "{cs:?}");
    assert!(
        !removed_in(&c.segments(), &cs[0]["snapshot-id"]).is_empty(),
        "记有它的冷层变更段应已写出"
    );
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_input_moved_abandons() {
    let c = Ctl::new();
    let (mut s, small) = c.two_small(&Run::base());
    stop(&mut s);
    c.hold("cold_compaction.before_run");
    c.hold("cold_compaction.after_select");
    let run = Run::base()
        .with("retention.period", &a_expires_in(&small, 60))
        .with("retention.check_interval", "10s")
        .with("storage.delete_delay", "1s");
    let s = c.start(&run);
    c.release_merge();
    c.wait_held("cold_compaction.after_select", &s);
    let (_, ok) = poll(
        Duration::from_secs(150),
        || c.live().contains(&small.a),
        |l| !l,
    );
    assert!(ok, "A 没有被 TTL 移出：{:#?}", s.stderr_lines());
    let cold = c.cold();
    if cold.exists(&small.a) {
        std::fs::remove_file(cold.path(&small.a)).unwrap();
    }
    c.release("cold_compaction.after_select");
    std::thread::sleep(Duration::from_secs(30));
    assert!(c.compactions().is_empty());
    assert!(!app_disabled(&s));
    assert_eq!(seqs_d(&s), range(11, 30));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_compaction_hot_read_abandons() {
    let c = Ctl::new();
    let (s, _small) = c.two_small(&Run::base());
    c.release_merge();
    let g1 = c.wait_merge(&s);
    let o1 = addition_paths(&g1);
    assert_eq!(o1.len(), 1);
    c.hold("cold_compaction.before_run");
    ingest_seq(&s, d_at(3, 0, 0), 31, 10, &|_| String::new());
    let (cc, _) = c.wait_laked(d_at(3, 0, 0));
    let ds = c.ds();
    let g1_path = ds.head()["cold_deletion_segment"]["path"]
        .as_str()
        .unwrap()
        .to_owned();
    ds.remove(&g1_path);
    let n = s.stderr_lines().len();
    c.release_merge();
    std::thread::sleep(Duration::from_secs(60));
    assert_eq!(c.compactions().len(), 1);
    let live = c.live();
    assert!(live.contains(&o1[0]) && live.contains(&cc));
    assert!(!app_disabled(&s));
    let warned = s.stderr_lines()[n..].iter().any(|l| {
        serde_json::from_str::<Value>(l)
            .is_ok_and(|v| v["level"] == "warn" && l.contains("default") && l.contains("app"))
    });
    assert!(warned, "{:#?}", &s.stderr_lines()[n..]);
}
