//! REQ-AMB-STO-COLD-TABLE 的系统测试：以子进程运行 `amberlog`，对象存储默认
//! 用 `file://` 临时目录；测试直接读冷层元数据 JSON、Avro 清单列表与清单（iceberg-rust 按 Iceberg 规范解析，不经被测实现）、
//! 冷层与热层 Parquet 页脚、热层快照与桶 manifest，核对 `_search`、`_json` 的应答与 stderr 的 JSON 日志。
//!
//! - 前提「已入湖的实例」由 [`Ctl::laked`] 搭建：以 `lifecycle.lake_watermark` 为 `1h` 启动，向 `app` 一次摄取 100 条
//!   `_timestamp` 落在此刻前第 3 小时的记录，等到该桶的交接记录写入热层。
//! - 「等待 N 秒」后断言某事已发生的，改为轮询到条件成立即止、上限取 N 秒；断言某事没有发生的，等满 N 秒。
//! - 「以 `retention.period` 为 `2h` 重启」时 `retention.check_interval` 取缺省的 `1h`：TTL 在开始受理后立即执行一次，此后
//!   1 小时内不再执行。
//! - 故障注入（只在 `fault-injection` 特性下生效）：对象存储请求经 `TESTBED_FAULT_STORE` 的控制文件（「以超时作答、客户端
//!   重试时得到已存在」为 `create … after-409`，即照常创建再以「已存在」作答；「对象存储被停止」为 `* * 503`）；暂停点经
//!   `TESTBED_FAULT_GATES`（每个步骤一个控制文件，运行中写 `hold` 即启用）；崩溃点经 `TESTBED_FAULT_PAUSE`，同一个点在
//!   一次运行里走到多次时以 `TESTBED_FAULT_PAUSE_NTH` 选第 k 次。
//! - 场景「表位置写成不带端点地址的 URI」用 MinIO 台架（`tools/testbed.sh`）。

// 多数场景要用测试专用的注入，只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::api::{ingest, now_us, poll};
use common::lake::ColdDir;
use common::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of, parquet_columns, sha256, stop};
use common::{BEARER, Fixture, Kv, Server, wait_file};
use iceberg::spec::{
    DataFile, Datum, FormatVersion, Manifest, ManifestContentType, ManifestList, ManifestStatus,
    PrimitiveLiteral,
};
use serde_json::{Value, json};

/// 场景中的「等待 130 秒」（「等待 260 秒」为两个桶依次各等一次）。
const WAIT: Duration = Duration::from_secs(130);
/// 等 TTL 的冷层提交：开始受理后立即执行，上限取 TTL 时限 2 分钟加 5 分钟。
const TTL_WAIT: Duration = Duration::from_secs(7 * 60);
const MINUTE: i64 = 60_000_000;
const APP: &str = "app";

// ---------------------------------------------------------------------------
// 启动与注入
// ---------------------------------------------------------------------------

/// 一次启动的配置（TOML 键与取值）与注入。
#[derive(Clone, Default)]
struct Run {
    kv: Vec<(&'static str, String)>,
    /// 崩溃点与其第几次（`TESTBED_FAULT_PAUSE`、`TESTBED_FAULT_PAUSE_NTH`）。
    pause: Option<(&'static str, u32)>,
}

impl Run {
    /// `flush.interval` 为 `1s`、`lifecycle.lake_watermark` 为 `1h`。
    fn lake() -> Run {
        Run::default()
            .with("flush.interval", "1s")
            .with("lifecycle.lake_watermark", "1h")
    }

    /// 同 [`Run::lake`]，另以 `retention.period` 为 `period`。
    fn ttl(period: &str) -> Run {
        Run::lake().with("retention.period", period)
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

    fn pause(mut self, step: &'static str, nth: u32) -> Run {
        self.pause = Some((step, nth));
        self
    }
}

/// 冷层孤儿清理每 5 秒一轮、孤儿最小年龄 1 秒。
fn orphans_fast(run: Run) -> Run {
    run.with("hot_fsck.interval", "5s")
        .with("hot_fsck.min_orphan_age", "1s")
}

/// 被测实例的环境：临时目录与跨重启共用的注入控制文件。
struct Ctl {
    f: Fixture,
    root: PathBuf,
    /// 测试开始时所在小时桶的起点：「此刻前第 n 小时」都从它算，测试跨过整点时各步仍指同一个桶。
    hour: i64,
}

/// 测试用到的暂停点（每个一个控制文件，起初为空即不停）。
const GATES: &[&str] = &[
    "cold_commit.before_metadata_put",
    "lake.after_digest_check",
    "lake.after_cold_upload",
    "ttl.before_run",
    "delete.before_physical",
    "cold_orphan.before_delete",
];

impl Ctl {
    fn new() -> Ctl {
        let f = Fixture::new();
        let root = f.root().join("cold-ctl");
        std::fs::create_dir_all(root.join("gates")).unwrap();
        std::fs::write(root.join("store"), "").unwrap();
        for g in GATES {
            std::fs::write(root.join("gates").join(g), "").unwrap();
        }
        Ctl {
            f,
            root,
            hour: hour_of(now_us()),
        }
    }

    /// 测试开始时前第 `n` 小时所在小时桶的起点。
    fn hours_ago(&self, n: i64) -> i64 {
        self.hour - n * HOUR
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
        let kv: Vec<Kv> = run.kv.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect();
        let mut cmd = self.f.serve(&kv);
        cmd.env("TESTBED_FAULT_STORE", self.root.join("store"));
        let gates: Vec<String> = GATES
            .iter()
            .map(|g| format!("{g}={}", self.gate_file(g).display()))
            .collect();
        cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
        if let Some((step, nth)) = run.pause {
            let _ = std::fs::remove_file(self.marker());
            cmd.env("TESTBED_FAULT_PAUSE", step)
                .env("TESTBED_FAULT_PAUSE_NTH", nth.to_string())
                .env("TESTBED_FAULT_MARKER", self.marker());
        }
        Server::start(cmd)
    }

    /// 前提「已入湖的实例」：以 `run`（须使此刻前第 3 小时的桶越过入湖水位）启动，向 `app` 一次摄取 100 条记录
    /// （[`rec`] 加上 `extra(i)` 的字段），等到该桶的交接记录写入热层。
    fn laked(&self, run: &Run, extra: impl Fn(u64) -> String) -> Server {
        let s = self.start(run);
        ingest_records(&s, APP, self.hours_ago(3), 0, 100, extra);
        self.wait_lake(self.hours_ago(3), &s);
        s
    }

    /// 等到 `app` 中起点为 `bucket` 的小时桶有交接记录（至多 [`WAIT`]），返回该桶的桶 manifest。
    fn wait_lake(&self, bucket: i64, server: &Server) -> Value {
        self.wait_lake_in(APP, bucket, 1, server)
    }

    fn wait_lake_in(&self, stream: &str, bucket: i64, k: usize, server: &Server) -> Value {
        let ds = self.f.dataset(stream);
        let label = bucket_label(bucket);
        let (m, ok) = poll(
            WAIT,
            || {
                ds.current()
                    .and_then(|_| ds.bucket_now(&label))
                    .filter(|m| m["handovers"].as_array().map_or(0, Vec::len) >= k)
            },
            Option::is_some,
        );
        assert!(
            ok,
            "{} 秒内桶 {label} 没有第 {k} 条交接记录：{:#?}",
            WAIT.as_secs(),
            server.stderr_lines()
        );
        m.unwrap()
    }

    /// 等到冷层当前版本中出现 `amberlog.operation` 为 `ttl` 的快照（至多 [`TTL_WAIT`]），返回它。
    fn wait_ttl(&self, server: &Server) -> Value {
        let cold = self.cold();
        let (s, ok) = poll(
            TTL_WAIT,
            || {
                cold.version().and_then(|_| {
                    ColdDir::snapshots(&cold.current())
                        .into_iter()
                        .find(|s| s["summary"]["amberlog.operation"] == "ttl")
                })
            },
            Option::is_some,
        );
        assert!(ok, "冷层 TTL 没有提交：{:#?}", server.stderr_lines());
        s.unwrap()
    }
}

// ---------------------------------------------------------------------------
// 数据
// ---------------------------------------------------------------------------

/// 小时桶 `start` 内第 `i` 条记录：`_timestamp` 为起点后 1 分钟加 `i` 毫秒，`seq` 为 `i`。
fn rec(start: i64, i: u64) -> String {
    format!(
        r#""_timestamp":{},"seq":{i}"#,
        start + MINUTE + i as i64 * 1000
    )
}

/// 以一个请求向 `stream` 摄取 `from..from+n` 号记录（[`rec`] 加 `extra(i)`，`extra` 为空串或以逗号开头的字段），
/// 断言全部受理。
fn ingest_records(
    server: &Server,
    stream: &str,
    start: i64,
    from: u64,
    n: u64,
    extra: impl Fn(u64) -> String,
) {
    let body: Vec<String> = (from..from + n)
        .map(|i| format!("{{{}{}}}", rec(start, i), extra(i)))
        .collect();
    let r = ingest(server, stream, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], n, "{}", r.text());
}

/// 以一个请求向 `stream` 摄取 `n` 条只含 `_timestamp`（小时桶 `start` 起点后 1 分钟加 `i` 毫秒）与 `field(i)` 所给字段
/// （形如 `"b":"b0"`）的记录，断言全部受理。
fn ingest_only(server: &Server, stream: &str, start: i64, n: u64, field: impl Fn(u64) -> String) {
    let body: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},{}}}"#,
                start + MINUTE + i as i64 * 1000,
                field(i)
            )
        })
        .collect();
    let r = ingest(server, stream, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], n, "{}", r.text());
}

fn plain(_: u64) -> String {
    String::new()
}

/// 以 `[start, end)` 为时间范围对 `stream` 执行 `SELECT *`（`size` 为 1000）。
fn query(server: &Server, stream: &str, start: i64, end: i64) -> common::Resp {
    let body = json!({"query": {"sql": format!("SELECT * FROM {stream}"), "start_time": start, "end_time": end, "size": 1000}});
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    )
}

/// 同 [`query`]，断言 200，返回行数。
fn rows(server: &Server, stream: &str, start: i64, end: i64) -> usize {
    let r = query(server, stream, start, end);
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["hits"].as_array().unwrap().len()
}

/// 向 `app` 摄取一条 `_timestamp` 为此刻的记录。
fn ingest_one(server: &Server) -> common::Resp {
    ingest(server, APP, &format!(r#"[{{"_timestamp":{}}}]"#, now_us()))
}

// ---------------------------------------------------------------------------
// 冷层对象
// ---------------------------------------------------------------------------

/// 元数据的当前 schema 的各列。
fn schema_fields(meta: &Value) -> Vec<Value> {
    let id = &meta["current-schema-id"];
    meta["schemas"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| &s["schema-id"] == id)
        .expect("当前 schema")["fields"]
        .as_array()
        .unwrap()
        .clone()
}

fn field_names(meta: &Value) -> Vec<String> {
    schema_fields(meta)
        .iter()
        .map(|f| f["name"].as_str().unwrap().to_owned())
        .collect()
}

/// 当前 schema 中名为 `name` 的列。
fn field<'a>(fields: &'a [Value], name: &str) -> &'a Value {
    fields
        .iter()
        .find(|f| f["name"] == name)
        .unwrap_or_else(|| panic!("冷层 schema 没有 {name}：{fields:?}"))
}

/// 元数据中的当前快照。
fn current_snapshot(meta: &Value) -> Value {
    ColdDir::snapshots(meta)
        .into_iter()
        .find(|s| s["snapshot-id"] == meta["current-snapshot-id"])
        .expect("冷层当前快照")
}

/// 快照 `snap` 的清单列表中的清单，与各清单中的条目（相对表位置的路径、状态、所记快照号与数据文件）。
struct RawEntry {
    path: String,
    status: ManifestStatus,
    snapshot_id: i64,
    file: DataFile,
}

fn manifest_list(cold: &ColdDir, meta: &Value, snap: &Value) -> ManifestList {
    let rel = ColdDir::rel(meta, snap["manifest-list"].as_str().unwrap());
    ManifestList::parse_with_version(&cold.read(&rel), FormatVersion::V2).expect("清单列表可解析")
}

fn raw_entries(cold: &ColdDir, meta: &Value, snap: &Value) -> Vec<RawEntry> {
    let mut out = Vec::new();
    for m in manifest_list(cold, meta, snap).entries() {
        let manifest = Manifest::parse_avro(&cold.read(&ColdDir::rel(meta, &m.manifest_path)))
            .expect("清单可解析");
        for e in manifest.entries() {
            out.push(RawEntry {
                path: ColdDir::rel(meta, e.file_path()),
                status: e.status(),
                snapshot_id: e.snapshot_id().unwrap_or(m.added_snapshot_id),
                file: e.data_file().clone(),
            });
        }
    }
    out
}

/// 冷层当前快照的存活条目（状态不为「已删除」）。
fn live_entries(cold: &ColdDir) -> Vec<RawEntry> {
    let meta = cold.current();
    raw_entries(cold, &meta, &current_snapshot(&meta))
        .into_iter()
        .filter(|e| e.status != ManifestStatus::Deleted)
        .collect()
}

/// 冷层当前版本（存在时）各快照引用的清单列表、清单与数据文件中不存在的（相对表位置）。
fn dangling(cold: &ColdDir) -> Vec<String> {
    let Some(n) = cold.version() else {
        return Vec::new();
    };
    let meta = cold.metadata(n);
    let mut missing = Vec::new();
    for snap in ColdDir::snapshots(&meta) {
        let list = ColdDir::rel(&meta, snap["manifest-list"].as_str().unwrap());
        if !cold.exists(&list) {
            missing.push(list);
            continue;
        }
        for m in manifest_list(cold, &meta, &snap).entries() {
            let rel = ColdDir::rel(&meta, &m.manifest_path);
            if !cold.exists(&rel) {
                missing.push(rel);
                continue;
            }
            let manifest = Manifest::parse_avro(&cold.read(&rel)).expect("清单可解析");
            for e in manifest.entries() {
                let p = ColdDir::rel(&meta, e.file_path());
                if !cold.exists(&p) {
                    missing.push(p);
                }
            }
        }
    }
    missing
}

/// `data/` 之下的对象（相对表位置）。
fn data_files(cold: &ColdDir) -> BTreeSet<String> {
    cold.keys()
        .into_iter()
        .filter(|k| k.starts_with("data/"))
        .collect()
}

/// 一个冷层数据文件内的 `_timestamp`。
fn file_timestamps(cold: &ColdDir, rel: &str) -> Vec<i64> {
    common::store::parquet_timestamps(&cold.path(rel))
}

/// `us` 所在 UTC 日的 `YYYY-MM-DD` 写法。
fn day_of(us: i64) -> String {
    let label = bucket_label(us);
    format!("{}-{}-{}", &label[0..4], &label[4..6], &label[6..8])
}

/// 冷层提示文件的内容。
fn hint(cold: &ColdDir) -> String {
    String::from_utf8(cold.read("metadata/version-hint.text")).unwrap()
}

fn set_hint(cold: &ColdDir, text: &str) {
    std::fs::write(cold.path("metadata/version-hint.text"), text).unwrap();
}

/// 以 `v<n>.metadata.json` 为基础另加一个不改变数据文件集合的快照（沿用当前快照的清单列表）写成的合法 v2 表元数据，
/// 返回其 JSON 与新快照号。
fn foreign_metadata(cold: &ColdDir, n: u64) -> (Value, i64) {
    let mut meta = cold.metadata(n);
    let cur = current_snapshot(&meta);
    let s: i64 = 7_000_000_000_000_000_001;
    let seq = meta["last-sequence-number"].as_i64().unwrap() + 1;
    let ms = now_us() / 1000;
    let snap = json!({
        "snapshot-id": s,
        "parent-snapshot-id": cur["snapshot-id"],
        "sequence-number": seq,
        "timestamp-ms": ms,
        "manifest-list": cur["manifest-list"],
        "summary": {"operation": "append"},
        "schema-id": meta["current-schema-id"],
    });
    meta["snapshots"].as_array_mut().unwrap().push(snap);
    meta["current-snapshot-id"] = json!(s);
    meta["last-sequence-number"] = json!(seq);
    meta["last-updated-ms"] = json!(ms);
    meta["refs"] = json!({"main": {"snapshot-id": s, "type": "branch"}});
    meta["snapshot-log"]
        .as_array_mut()
        .unwrap()
        .push(json!({"snapshot-id": s, "timestamp-ms": ms}));
    (meta, s)
}

/// 桶 manifest `m` 的第 `i` 条交接记录。
fn handover(m: &Value, i: usize) -> &Value {
    &m["handovers"][i]
}

/// stderr 中 `event` 为 `dataset_disabled` 的日志行。
fn disabled_events(server: &Server) -> Vec<Value> {
    server
        .json_lines()
        .into_iter()
        .filter(|l| l["event"] == "dataset_disabled")
        .collect()
}

/// 等到 stderr 出现 `dataset_disabled`（至多 `limit`）。
fn wait_disabled(server: &Server, limit: Duration) {
    let (_, ok) = poll(limit, || disabled_events(server).len(), |n| *n > 0);
    assert!(ok, "数据集没有停用：{:#?}", server.stderr_lines());
}

// ---------------------------------------------------------------------------
// 表的位置
// ---------------------------------------------------------------------------

#[test]
fn cold_table_dir() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let keys = c.f.iceberg_keys();
    assert!(
        keys.contains(&"default/app/metadata/v1.metadata.json".to_owned()),
        "{keys:?}"
    );
    assert!(
        keys.iter().all(|k| k.starts_with("default/app/")),
        "{keys:?}"
    );
}

#[test]
fn cold_table_subdirs() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let keys = c.cold().keys();
    assert!(!keys.is_empty());
    for k in &keys {
        assert!(
            k.starts_with("metadata/") || k.starts_with("data/"),
            "{k}：{keys:?}"
        );
    }
}

#[test]
fn cold_table_iceberg_only() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    ingest_records(&s, "web", c.hours_ago(3), 0, 100, plain);
    c.wait_lake_in("web", c.hours_ago(3), 1, &s);
    let keys = c.f.iceberg_keys();
    assert!(
        keys.iter().any(|k| k.starts_with("default/web/")),
        "{keys:?}"
    );
    for k in &keys {
        assert!(
            k.starts_with("default/app/") || k.starts_with("default/web/"),
            "{k}"
        );
    }
}

#[test]
fn cold_table_location_uri() {
    use common::s3::S3Instance;
    use object_store::ObjectStoreExt;

    let inst = S3Instance::new();
    let url = inst
        .config(&[])
        .into_iter()
        .find(|(k, _)| *k == "object_store.url")
        .and_then(|(_, v)| v)
        .unwrap();
    let url = url.trim_matches('"').to_owned();
    let s = inst.start(&[FLUSH_1S, ("lifecycle.lake_watermark", Some("\"1h\""))]);
    ingest_records(&s, APP, hour_of(now_us()) - 3 * HOUR, 0, 100, plain);
    let store = inst.store();
    let path = ["_iceberg", "default", "app", "metadata", "v1.metadata.json"]
        .iter()
        .fold(store.root.clone(), |p, part| p.join(*part));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (bytes, ok) = poll(
        WAIT,
        || {
            rt.block_on(store.inner.get(&path))
                .ok()
                .map(|g| rt.block_on(g.bytes()).unwrap())
        },
        Option::is_some,
    );
    assert!(ok, "v1.metadata.json 没有出现：{:#?}", s.stderr_lines());
    let meta: Value = serde_json::from_slice(&bytes.unwrap()).unwrap();
    assert_eq!(
        meta["location"],
        format!("{url}/amberlog/_iceberg/default/app").as_str(),
        "{meta}"
    );
}

// ---------------------------------------------------------------------------
// 表格式、分区与快照摘要
// ---------------------------------------------------------------------------

#[test]
fn cold_table_standard_reader() {
    use arrow::array::{Array, Int64Array, StringArray, TimestampMicrosecondArray};
    use arrow::record_batch::RecordBatch;
    use futures::TryStreamExt;
    use iceberg::TableIdent;
    use iceberg::io::FileIO;
    use iceberg::table::StaticTable;

    let c = Ctl::new();
    let mut s = c.laked(&Run::lake(), |i| format!(r#","msg":"m{i}""#));
    stop(&mut s);
    let start = c.hours_ago(3);
    let mut want: Vec<(i64, i64, String)> = (0..100u64)
        .map(|i| (start + MINUTE + i as i64 * 1000, i as i64, format!("m{i}")))
        .collect();
    want.sort();

    // 不经 Amberlog：按提示文件找当前版本，iceberg-rust 自带的读取器读出全表
    let cold = c.cold();
    let location = cold.current()["location"].as_str().unwrap().to_owned();
    let metadata = format!("{location}/metadata/v{}.metadata.json", hint(&cold));
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
        let msg = b
            .column_by_name("msg")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..b.num_rows() {
            assert!(!seq.is_null(i) && !msg.is_null(i));
            got.push((ts.value(i), seq.value(i), msg.value(i).to_owned()));
        }
    }
    got.sort();
    assert_eq!(got, want);
}

#[test]
fn cold_table_day_partition() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let meta = c.cold().current();
    let spec = meta["partition-specs"]
        .as_array()
        .unwrap()
        .iter()
        .find(|p| p["spec-id"] == meta["default-spec-id"])
        .expect("默认分区规格")
        .clone();
    let fields = spec["fields"].as_array().unwrap();
    assert_eq!(fields.len(), 1, "{spec}");
    assert_eq!(fields[0]["name"], "_timestamp_day");
    assert_eq!(fields[0]["source-id"], 1);
    assert_eq!(fields[0]["transform"], "day");
}

/// 「已入湖的实例」以 SIGTERM 退出，再以 `retention.period` 为 `2h`（其余配置不变）重启，等到冷层 TTL 提交一次。
fn laked_then_ttl(c: &Ctl) -> Server {
    let mut s = c.laked(&Run::lake(), plain);
    stop(&mut s);
    let s = c.start(&Run::ttl("2h"));
    c.wait_ttl(&s);
    s
}

#[test]
fn cold_table_no_row_delete() {
    let c = Ctl::new();
    let _s = laked_then_ttl(&c);
    let cold = c.cold();
    let meta = cold.current();
    let snaps = ColdDir::snapshots(&meta);
    assert!(snaps.len() >= 2, "{meta}");
    for snap in &snaps {
        for m in manifest_list(&cold, &meta, snap).entries() {
            assert_eq!(m.content, ManifestContentType::Data, "{}", m.manifest_path);
        }
    }
}

#[test]
fn cold_table_no_name_props() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), |i| {
        format!(r#","level":"info","status":{}"#, 200 + i % 3)
    });
    let meta = c.cold().current();
    assert!(field_names(&meta).contains(&"level".to_owned()));
    let props = meta["properties"].as_object().cloned().unwrap_or_default();
    for k in props.keys() {
        assert!(!k.contains("level") && !k.contains("status"), "{k}");
    }
}

#[test]
fn cold_table_one_snapshot() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert!(!cold.exists(&format!("metadata/v{}.metadata.json", n + 2)));
    let (old, new) = (cold.metadata(n), cold.metadata(n + 1));
    let (a, b) = (ColdDir::snapshots(&old), ColdDir::snapshots(&new));
    assert_eq!(b.len(), a.len() + 1);
    let added: Vec<&Value> = b.iter().filter(|x| !a.contains(x)).collect();
    assert_eq!(added.len(), 1, "{new}");
    assert_eq!(added[0]["snapshot-id"], new["current-snapshot-id"]);
}

#[test]
fn cold_table_summary_operation() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    for snap in ColdDir::snapshots(&c.cold().current()) {
        assert_eq!(snap["summary"]["amberlog.operation"], "lake", "{snap}");
    }
}

/// 冷层当前版本中 `amberlog.operation` 为 `op` 的快照的 `summary`。
fn summaries(c: &Ctl, op: &str) -> Vec<Value> {
    ColdDir::snapshots(&c.cold().current())
        .into_iter()
        .filter(|s| s["summary"]["amberlog.operation"] == op)
        .map(|s| s["summary"].clone())
        .collect()
}

#[test]
fn cold_table_summary_hot_snapshot_id_lake_only() {
    let c = Ctl::new();
    let _s = laked_then_ttl(&c);
    let (lake, ttl) = (summaries(&c, "lake"), summaries(&c, "ttl"));
    assert!(!lake.is_empty() && !ttl.is_empty());
    assert!(
        lake.iter()
            .all(|s| s.get("amberlog.hot_snapshot_id").is_some()),
        "{lake:?}"
    );
    assert!(
        ttl.iter()
            .all(|s| s.get("amberlog.hot_snapshot_id").is_none()),
        "{ttl:?}"
    );
}

#[test]
fn cold_table_summary_file_digests_lake_not_ttl() {
    let c = Ctl::new();
    let _s = laked_then_ttl(&c);
    let (lake, ttl) = (summaries(&c, "lake"), summaries(&c, "ttl"));
    assert!(!lake.is_empty() && !ttl.is_empty());
    let digest = |v: &Value| {
        v.as_str().is_some_and(|s| {
            s.strip_prefix("sha256:").is_some_and(|h| {
                h.len() == 64
                    && h.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            })
        })
    };
    for s in &lake {
        let raw = s["amberlog.file_digests"]
            .as_str()
            .expect("有 file_digests");
        let obj: Value = serde_json::from_str(raw).unwrap();
        let obj = obj.as_object().expect("JSON 对象");
        assert!(!obj.is_empty());
        assert!(obj.values().all(digest), "{raw}");
    }
    assert!(
        ttl.iter().all(|s| s.get("amberlog.file_digests").is_none()),
        "{ttl:?}"
    );
}

#[test]
fn cold_table_summary_standard_operation() {
    let c = Ctl::new();
    let _s = laked_then_ttl(&c);
    let (lake, ttl) = (summaries(&c, "lake"), summaries(&c, "ttl"));
    assert!(!lake.is_empty() && !ttl.is_empty());
    assert!(lake.iter().all(|s| s["operation"] == "append"), "{lake:?}");
    assert!(ttl.iter().all(|s| s["operation"] == "delete"), "{ttl:?}");
}

/// 场景「冷层合并快照的标准 operation 为 replace」「文件摘要属性出现在 compaction 快照」的实例：以 `lifecycle.seal_grace`
/// 为 `1s`、`lifecycle.lake_watermark` 为 `2s`、`cold_compaction.check_interval` 为 `10s` 启动，向 `app` 摄取此刻所在 UTC 日
/// 前两日 01 时与 02 时的记录各 10 条，等到冷层合并完成（出现 `compaction` 快照且记有它的冷层变更段已写出，至多 3 分钟）。
fn compacted(c: &Ctl) -> Server {
    let run = Run::lake()
        .with("lifecycle.seal_grace", "1s")
        .with("lifecycle.lake_watermark", "2s")
        .with("cold_compaction.check_interval", "10s");
    let s = c.start(&run);
    let day = now_us().div_euclid(24 * HOUR) * 24 * HOUR - 2 * 24 * HOUR;
    ingest_records(&s, APP, day + HOUR, 0, 10, plain);
    ingest_records(&s, APP, day + 2 * HOUR, 10, 10, plain);
    let ds = c.ds();
    let (_, ok) = poll(
        Duration::from_secs(180),
        || {
            let traced = ds.current().is_some_and(|_| {
                ds.segments(&ds.head()["cold_deletion_segment"])
                    .iter()
                    .any(|g| g["additions"].as_array().is_some_and(|a| !a.is_empty()))
            });
            traced && !summaries(c, "compaction").is_empty()
        },
        |x| *x,
    );
    assert!(ok, "三分钟内冷层合并没有完成：{:#?}", s.stderr_lines());
    s
}

#[test]
fn cold_table_summary_standard_operation_replace() {
    let c = Ctl::new();
    let _s = compacted(&c);
    let compaction = summaries(&c, "compaction");
    assert!(
        compaction.iter().all(|s| s["operation"] == "replace"),
        "{compaction:?}"
    );
}

#[test]
fn cold_table_summary_file_digests_compaction() {
    let c = Ctl::new();
    let _s = compacted(&c);
    for s in summaries(&c, "compaction") {
        let raw = s["amberlog.file_digests"]
            .as_str()
            .expect("有 file_digests");
        let obj: Value = serde_json::from_str(raw).unwrap();
        let obj = obj.as_object().expect("JSON 对象");
        assert!(!obj.is_empty());
        assert!(
            obj.values().all(|v| v.as_str().is_some_and(|d| {
                d.strip_prefix("sha256:").is_some_and(|h| {
                    h.len() == 64
                        && h.bytes()
                            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
            })),
            "{raw}"
        );
        assert!(
            s.get("amberlog.hot_snapshot_id").is_none() && s.get("amberlog.ttl_cutoff").is_none()
        );
    }
}

#[test]
fn cold_table_summary_ttl_cutoff_ttl_only() {
    let c = Ctl::new();
    let _s = laked_then_ttl(&c);
    let (lake, ttl) = (summaries(&c, "lake"), summaries(&c, "ttl"));
    assert!(!lake.is_empty() && !ttl.is_empty());
    for s in &ttl {
        let v = s["amberlog.ttl_cutoff"].as_str().expect("有 ttl_cutoff");
        let digits = v.strip_prefix('-').unwrap_or(v);
        assert!(
            !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit()),
            "{v}"
        );
    }
    assert!(
        lake.iter().all(|s| s.get("amberlog.ttl_cutoff").is_none()),
        "{lake:?}"
    );
}

#[test]
fn cold_table_commit_added_files() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    let (b5, b3) = (c.hours_ago(5), c.hours_ago(3));
    let body: Vec<String> = (0..10)
        .map(|i| format!("{{{}}}", rec(b5, i)))
        .chain((0..20).map(|i| format!("{{{}}}", rec(b3, i))))
        .collect();
    let r = ingest(&s, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    c.wait_lake(b5, &s);
    let m3 = c.wait_lake(b3, &s);
    let label3 = bucket_label(b3);
    let s3 = ColdDir::lake_snapshots(&c.cold().current(), &label3);
    assert_eq!(s3.len(), 1);
    let snap_s = s3[0]["snapshot-id"].as_i64().unwrap();
    let mut s = s;
    stop(&mut s);

    let s = c.start(&Run::ttl("4h"));
    c.wait_ttl(&s);
    let cold = c.cold();
    let from_s: Vec<RawEntry> = live_entries(&cold)
        .into_iter()
        .filter(|e| e.snapshot_id == snap_s)
        .collect();
    assert_eq!(
        from_s.iter().map(|e| e.file.record_count()).sum::<u64>(),
        20
    );
    for e in &from_s {
        for t in file_timestamps(&cold, &e.path) {
            assert_eq!(hour_of(t), b3, "{}", e.path);
        }
    }
    // 第 5 小时的桶的文件已被移出
    assert!(live_entries(&cold).iter().all(|e| e.snapshot_id == snap_s));
    let listed: BTreeSet<String> = handover(&m3, 0)["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    let entries: BTreeSet<String> = from_s.into_iter().map(|e| e.path).collect();
    assert_eq!(listed, entries);
}

// ---------------------------------------------------------------------------
// schema 与列名
// ---------------------------------------------------------------------------

/// 热层链尾快照的 `schema`：列名 → 字段编号。
fn hot_field_ids(ds: &Ds) -> BTreeMap<String, i64> {
    ds.head()["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_owned(),
                c["field_id"].as_i64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn cold_table_field_id() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), |i| {
        format!(r#","kubernetes.pod.name":"p{i}","X-Request-Id":"r{i}","status":200"#)
    });
    let fields = schema_fields(&c.cold().current());
    let hot = hot_field_ids(&c.ds());
    assert!(hot.contains_key("kubernetes.pod.name"), "{hot:?}");
    for (name, id) in &hot {
        assert_eq!(field(&fields, name)["id"], *id, "{name}");
    }
}

#[test]
fn cold_table_types() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), |i| {
        format!(r#","n":{i},"x":{i}.5,"ok":true,"msg":"m{i}""#)
    });
    let fields = schema_fields(&c.cold().current());
    for (name, ty) in [
        ("n", "long"),
        ("x", "double"),
        ("ok", "boolean"),
        ("msg", "string"),
    ] {
        let f = field(&fields, name);
        assert_eq!(f["type"], ty, "{f}");
        assert_eq!(f["required"], false, "{f}");
    }
    let ts = field(&fields, "_timestamp");
    assert_eq!(ts["type"], "timestamptz", "{ts}");
    assert_eq!(ts["required"], true, "{ts}");
}

#[test]
fn cold_table_schema_grows() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), |i| format!(r#","a":"a{i}""#));
    let cold = c.cold();
    let n = cold.version().unwrap();
    ingest_only(&s, APP, c.hours_ago(4), 10, |i| format!(r#""b":"b{i}""#));
    c.wait_lake(c.hours_ago(4), &s);
    let (old, new) = (
        schema_fields(&cold.metadata(n)),
        schema_fields(&cold.current()),
    );
    field(&new, "b");
    let (a0, a1) = (field(&old, "a"), field(&new, "a"));
    for k in ["id", "name", "type"] {
        assert_eq!(a0[k], a1[k], "{k}");
    }
}

/// 场景「入湖时冷层 schema 覆盖热层当前 schema」的实例：先摄取 10 条只含 `c`、`_timestamp` 为此刻的记录，等两个
/// `flush.interval`，再摄取 10 条只含 `a`、落在此刻前第 3 小时的记录，等到该桶入湖。
fn covers_hot(c: &Ctl) -> Server {
    let s = c.start(&Run::lake());
    let now = now_us();
    let body: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"c":"c{i}"}}"#, now - i))
        .collect();
    let r = ingest(&s, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    std::thread::sleep(Duration::from_secs(2));
    let body: Vec<String> = (0..10)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"a":"a{i}"}}"#,
                c.hours_ago(3) + MINUTE + i
            )
        })
        .collect();
    let r = ingest(&s, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    c.wait_lake(c.hours_ago(3), &s);
    s
}

#[test]
fn cold_table_schema_covers_hot() {
    let c = Ctl::new();
    let _s = covers_hot(&c);
    let names = field_names(&c.cold().current());
    for n in ["_timestamp", "a", "c"] {
        assert!(names.contains(&n.to_owned()), "{names:?}");
    }
}

#[test]
fn cold_table_schema_append_order() {
    let c = Ctl::new();
    let s = covers_hot(&c);
    let hot = hot_field_ids(&c.ds());
    assert_eq!(
        (hot["_timestamp"], hot["c"], hot["a"]),
        (1, 2, 3),
        "{hot:?}"
    );
    let cold = c.cold();
    let n = cold.version().unwrap();
    ingest_only(&s, APP, c.hours_ago(4), 10, |i| format!(r#""b":"b{i}""#));
    c.wait_lake(c.hours_ago(4), &s);
    assert_eq!(field_names(&cold.metadata(n)), ["_timestamp", "c", "a"]);
    assert_eq!(
        field_names(&cold.metadata(n + 1)),
        ["_timestamp", "c", "a", "b"]
    );
}

#[test]
fn cold_table_original_names() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    let b = c.hours_ago(3);
    // 同名字段先为整数、后为字符串：后者落进影子列 `status__utf8`
    ingest_records(&s, APP, b, 0, 50, |i| {
        format!(r#","kubernetes.pod.name":"p{i}","X-Request-Id":"r{i}","status":200"#)
    });
    ingest_records(&s, APP, b, 50, 50, |_| r#","status":"ok""#.to_owned());
    let cold = c.cold();
    let want = ["kubernetes.pod.name", "X-Request-Id", "status__utf8"];
    let (names, ok) = poll(
        WAIT,
        || {
            cold.version()
                .map(|_| field_names(&cold.current()))
                .unwrap_or_default()
        },
        |names| want.iter().all(|w| names.iter().any(|n| n == w)),
    );
    assert!(ok, "{names:?}：{:#?}", s.stderr_lines());
}

/// 场景「只差大小写的列在冷层改名」的实例：向 `case` 摄取 10 条落在此刻前第 3 小时、依次含 `Foo`、`foo`、`FOO` 的
/// 记录，等到该桶入湖。
fn case_renamed(c: &Ctl) -> Server {
    let s = c.start(&Run::lake());
    let names = ["Foo", "foo", "FOO"];
    ingest_only(&s, "case", c.hours_ago(3), 10, |i| {
        format!(r#""{}":"v{i}""#, names[i as usize % 3])
    });
    c.wait_lake_in("case", c.hours_ago(3), 1, &s);
    let hot = hot_field_ids(&c.f.dataset("case"));
    assert_eq!((hot["Foo"], hot["foo"], hot["FOO"]), (2, 3, 4), "{hot:?}");
    s
}

/// `case` 冷层当前 schema：字段编号 → 列。
fn case_fields(c: &Ctl) -> BTreeMap<i64, Value> {
    schema_fields(&c.f.cold("case").current())
        .into_iter()
        .map(|f| (f["id"].as_i64().unwrap(), f))
        .collect()
}

#[test]
fn cold_table_case_rename() {
    let c = Ctl::new();
    let _s = case_renamed(&c);
    let fields = case_fields(&c);
    assert_eq!(fields[&2]["name"], "Foo");
    assert_eq!(fields[&3]["name"], "foo__2");
    assert_eq!(fields[&4]["name"], "FOO__3");
}

#[test]
fn cold_table_rename_doc() {
    let c = Ctl::new();
    let _s = case_renamed(&c);
    let fields = case_fields(&c);
    assert_eq!(fields[&3]["doc"], "foo");
    assert_eq!(fields[&4]["doc"], "FOO");
}

#[test]
fn cold_table_hot_names_unchanged() {
    let c = Ctl::new();
    let s = case_renamed(&c);
    let ds = c.f.dataset("case");
    let now = now_us();
    let body: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"foo":"x{i}"}}"#, now - i))
        .collect();
    let r = ingest(&s, "case", &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    let label = bucket_label(now);
    let (m, ok) = poll(
        Duration::from_secs(15),
        || ds.bucket_now(&label),
        |m| {
            m.as_ref()
                .is_some_and(|m| !m["files"].as_array().unwrap().is_empty())
        },
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    let head = ds.head();
    let col3 = head["schema"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["field_id"] == 3)
        .unwrap()
        .clone();
    assert_eq!(col3["name"], "foo", "{head}");
    let file = m.unwrap()["files"][0]["path"].as_str().unwrap().to_owned();
    let cols = parquet_columns(&ds.path(&file));
    let phys = cols
        .iter()
        .find(|c| c.field_id == 3)
        .expect("字段编号 3 的列");
    assert_eq!(phys.name, "foo");
}

// ---------------------------------------------------------------------------
// 数据文件
// ---------------------------------------------------------------------------

#[test]
fn cold_table_data_path() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let meta = cold.current();
    let location = meta["location"].as_str().unwrap();
    let prefix = format!("{location}/data/_timestamp_day={}/", day_of(c.hours_ago(3)));
    let list = manifest_list(&cold, &meta, &current_snapshot(&meta));
    let mut n = 0;
    for m in list.entries() {
        let manifest =
            Manifest::parse_avro(&cold.read(&ColdDir::rel(&meta, &m.manifest_path))).unwrap();
        for e in manifest.entries() {
            let path = e.file_path();
            let name = path
                .strip_prefix(&prefix)
                .unwrap_or_else(|| panic!("{path} 不以 {prefix} 开头"));
            let ulid = name.strip_suffix(".parquet").expect(".parquet 结尾");
            assert_eq!(ulid.len(), 26, "{name}");
            assert!(
                ulid.bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()),
                "{name}"
            );
            assert!(cold.exists(&ColdDir::rel(&meta, path)), "{path}");
            n += 1;
        }
    }
    assert!(n > 0);
}

#[test]
fn cold_table_data_one_day() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    // 当日 00 时须已越过入湖水位（00 时的桶 01:00 结束，加 1 小时水位）：此刻早于 UTC 02:10 时取前一日作「当日」
    let now = now_us();
    let day = 24 * HOUR;
    let today = now.div_euclid(day) * day;
    let d = if now - today >= 2 * HOUR + 10 * MINUTE {
        today
    } else {
        today - day
    };
    let (late, early) = (d - HOUR, d);
    let body: Vec<String> = (0..10)
        .map(|i| format!("{{{}}}", rec(late, i)))
        .chain((0..10).map(|i| format!("{{{}}}", rec(early, i))))
        .collect();
    let r = ingest(&s, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    c.wait_lake(late, &s);
    c.wait_lake(early, &s);
    let cold = c.cold();
    let files = data_files(&cold);
    assert!(files.len() >= 2, "{files:?}");
    for rel in &files {
        let day_part = rel
            .strip_prefix("data/_timestamp_day=")
            .and_then(|r| r.split('/').next())
            .unwrap();
        for t in file_timestamps(&cold, rel) {
            assert_eq!(day_of(t), day_part, "{rel}");
        }
    }
}

#[test]
fn cold_table_parquet_field_id() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), |i| format!(r#","msg":"m{i}","n":{i}"#));
    let cold = c.cold();
    let fields = schema_fields(&cold.current());
    let files = data_files(&cold);
    assert!(!files.is_empty());
    for rel in &files {
        for col in parquet_columns(&cold.path(rel)) {
            assert_eq!(
                field(&fields, &col.name)["id"],
                col.field_id,
                "{rel}：{}",
                col.name
            );
        }
    }
}

#[test]
fn cold_table_physical_names() {
    let c = Ctl::new();
    let _s = case_renamed(&c);
    let cold = c.f.cold("case");
    let files = data_files(&cold);
    assert!(!files.is_empty());
    for rel in &files {
        let cols: BTreeMap<i32, String> = parquet_columns(&cold.path(rel))
            .into_iter()
            .map(|c| (c.field_id, c.name))
            .collect();
        assert_eq!(cols[&2], "Foo", "{rel}");
        assert_eq!(cols[&3], "foo__2", "{rel}");
        assert_eq!(cols[&4], "FOO__3", "{rel}");
    }
}

#[test]
fn cold_table_immutable() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let before: BTreeMap<String, String> = cold
        .keys()
        .into_iter()
        .filter(|k| k != "metadata/version-hint.text")
        .map(|k| (k.clone(), sha256(&cold.read(&k))))
        .collect();
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    for (k, digest) in &before {
        assert!(cold.exists(k), "{k} 不见了");
        assert_eq!(&sha256(&cold.read(k)), digest, "{k} 被改写");
    }
}

/// 「已入湖的实例」此后又有两个桶（此刻前第 4、5 小时）完成入湖。
fn three_buckets(c: &Ctl, run: &Run) -> Server {
    let s = c.laked(run, plain);
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    ingest_records(&s, APP, c.hours_ago(5), 0, 10, plain);
    c.wait_lake(c.hours_ago(5), &s);
    s
}

#[test]
fn cold_table_no_accel_files_in_table_dir() {
    let c = Ctl::new();
    let _s = three_buckets(&c, &Run::lake());
    let cold = c.cold();
    for k in cold.keys() {
        if let Some(name) = k.strip_prefix("metadata/") {
            assert!(
                name.ends_with(".metadata.json")
                    || name.ends_with(".avro")
                    || name == "version-hint.text",
                "{k}"
            );
        } else if let Some(name) = k.strip_prefix("data/") {
            assert!(name.ends_with(".parquet"), "{k}");
        }
    }
    let meta = cold.current();
    for key in ["statistics", "partition-statistics"] {
        let v = &meta[key];
        assert!(
            v.is_null() || v.as_array().is_some_and(Vec::is_empty),
            "{key}：{v}"
        );
    }
}

// ---------------------------------------------------------------------------
// 清单条目与列统计
// ---------------------------------------------------------------------------

#[test]
fn cold_table_entry_size() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let entries = live_entries(&cold);
    let mut total = 0;
    for e in &entries {
        assert_eq!(
            e.file.file_size_in_bytes(),
            cold.read(&e.path).len() as u64,
            "{}",
            e.path
        );
        let n: usize = cold.batches(&e.path).iter().map(|b| b.num_rows()).sum();
        assert_eq!(e.file.record_count(), n as u64, "{}", e.path);
        total += n;
    }
    assert_eq!(total, 100);
}

#[test]
fn cold_table_entry_ts_bounds() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let entries = live_entries(&cold);
    assert!(!entries.is_empty());
    for e in &entries {
        let ts = file_timestamps(&cold, &e.path);
        let (min, max) = (*ts.iter().min().unwrap(), *ts.iter().max().unwrap());
        assert_eq!(
            e.file.lower_bounds()[&1],
            Datum::timestamptz_micros(min),
            "{}",
            e.path
        );
        assert_eq!(
            e.file.upper_bounds()[&1],
            Datum::timestamptz_micros(max),
            "{}",
            e.path
        );
    }
}

#[test]
fn cold_table_entry_bounds_exact() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    // 1000 字节的字符串，首字节各不相同
    let msgs: Vec<String> = (0..10).map(|i| format!("{i}{}", "z".repeat(999))).collect();
    let m2 = msgs.clone();
    ingest_records(&s, APP, c.hours_ago(3), 0, 10, move |i| {
        format!(r#","msg":"{}""#, m2[i as usize])
    });
    c.wait_lake(c.hours_ago(3), &s);
    let cold = c.cold();
    let id = field(&schema_fields(&cold.current()), "msg")["id"]
        .as_i64()
        .unwrap() as i32;
    let (min, max) = (msgs.iter().min().unwrap(), msgs.iter().max().unwrap());
    let text = |d: &Datum| match d.literal() {
        PrimitiveLiteral::String(s) => s.clone(),
        other => panic!("{other:?}"),
    };
    for e in live_entries(&cold) {
        if let Some(lo) = e.file.lower_bounds().get(&id) {
            assert!(text(lo).as_str() <= min.as_str(), "{}", text(lo));
        }
        if let Some(hi) = e.file.upper_bounds().get(&id) {
            assert!(text(hi).as_str() >= max.as_str(), "{}", text(hi));
        }
    }
}

#[test]
fn cold_table_entry_null_counts() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    ingest_records(&s, APP, c.hours_ago(3), 0, 20, |i| {
        if i < 5 {
            String::new()
        } else {
            format!(r#","pod":"p{i}""#)
        }
    });
    c.wait_lake(c.hours_ago(3), &s);
    let cold = c.cold();
    let id = field(&schema_fields(&cold.current()), "pod")["id"]
        .as_i64()
        .unwrap() as i32;
    let nulls: u64 = live_entries(&cold)
        .iter()
        .map(|e| e.file.null_value_counts().get(&id).copied().unwrap_or(0))
        .sum();
    assert_eq!(nulls, 5);
}

// ---------------------------------------------------------------------------
// 元数据文件与提示
// ---------------------------------------------------------------------------

#[test]
fn cold_table_metadata_names() {
    let c = Ctl::new();
    let _s = three_buckets(&c, &Run::lake());
    let cold = c.cold();
    let n = cold.version().unwrap();
    let got: Vec<String> = cold
        .keys()
        .into_iter()
        .filter_map(|k| k.strip_prefix("metadata/").map(str::to_owned))
        .filter(|k| k.ends_with(".metadata.json"))
        .collect();
    let want: BTreeSet<String> = (1..=n).map(|i| format!("v{i}.metadata.json")).collect();
    assert_eq!(got.into_iter().collect::<BTreeSet<_>>(), want);
}

#[test]
fn cold_table_hint_content() {
    let c = Ctl::new();
    let _s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    assert_eq!(hint(&cold), cold.version().unwrap().to_string());
}

#[test]
fn cold_table_keep_metadata() {
    let c = Ctl::new();
    let mut s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    let before: BTreeMap<String, String> = (1..=n)
        .map(|i| {
            let k = format!("metadata/v{i}.metadata.json");
            let d = sha256(&cold.read(&k));
            (k, d)
        })
        .collect();
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    ingest_records(&s, APP, c.hours_ago(5), 0, 10, plain);
    c.wait_lake(c.hours_ago(5), &s);
    stop(&mut s);
    let s = c.start(&Run::ttl("2h"));
    c.wait_ttl(&s);
    for (k, d) in &before {
        assert!(cold.exists(k), "{k}");
        assert_eq!(&sha256(&cold.read(k)), d, "{k}");
    }
}

/// 场景「元数据文件的 metadata-log 至多 100 条」入湖的小时桶数：设了 `TESTBED_SOAK_HOURS`（跑满时长的长时运行）时为块写的
/// 102 个（入湖每轮只入一个桶，约 17 分钟），否则为 12 个的缩短版（只核对 `metadata-log` 随版本增长、末项为前一版本）。
fn metadata_log_buckets() -> i64 {
    if std::env::var_os("TESTBED_SOAK_HOURS").is_some() {
        102
    } else {
        12
    }
}

#[test]
fn cold_table_metadata_log_capped() {
    let c = Ctl::new();
    let s = c.start(&Run::lake());
    let k = metadata_log_buckets();
    for h in 3..3 + k {
        ingest_records(&s, APP, c.hours_ago(h), h as u64, 1, plain);
    }
    let (n, ok) = poll(
        Duration::from_secs(k as u64 * 10 + 300),
        || c.cold().version().unwrap_or(0),
        |n| *n >= k as u64,
    );
    assert!(ok, "冷层当前版本只到 {n}：{:#?}", s.stderr_lines());
    let log = c.cold().metadata(n)["metadata-log"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(log.len() <= 100, "metadata-log 有 {} 条", log.len());
    assert_eq!(log.len() as u64, (n - 1).min(100));
    let previous = format!("/metadata/v{}.metadata.json", n - 1);
    assert!(
        log.iter().any(|e| e["metadata-file"]
            .as_str()
            .is_some_and(|f| f.ends_with(&previous))),
        "{log:?}"
    );
}

// ---------------------------------------------------------------------------
// 提交
// ---------------------------------------------------------------------------

#[test]
fn cold_table_commit_sequence() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    let (b4, b5) = (c.hours_ago(4), c.hours_ago(5));
    let body: Vec<String> = (0..10)
        .map(|i| format!("{{{}}}", rec(b4, i)))
        .chain((0..10).map(|i| format!("{{{}}}", rec(b5, i))))
        .collect();
    let r = ingest(&s, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    let m5 = c.wait_lake(b5, &s);
    let m4 = c.wait_lake(b4, &s);
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 2)));
    assert_eq!(handover(&m5, 0)["metadata_version"], n + 1, "{m5}");
    assert_eq!(handover(&m4, 0)["metadata_version"], n + 2, "{m4}");
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_commit_order_crash() {
    let c = Ctl::new();
    let b = c.hours_ago(3);
    // 先只写入、不入湖（水位 72 小时），使 `app` 有一个待入湖的桶
    let mut s = c.start(&Run::lake().with("lifecycle.lake_watermark", "72h"));
    ingest_records(&s, APP, b, 0, 100, plain);
    c.ds().wait_records(100, Duration::from_secs(15));
    stop(&mut s);

    let cold = c.cold();
    let label = bucket_label(b);
    for k in 1..=30u32 {
        let mut s = c.start(&Run::lake().pause("cold_commit.after_each_put", k));
        let ds = c.ds();
        let (done, ok) = poll(
            WAIT,
            || {
                if c.marker().exists() {
                    Some(false)
                } else if ds
                    .bucket_now(&label)
                    .is_some_and(|m| !m["handovers"].as_array().unwrap().is_empty())
                {
                    Some(true)
                } else {
                    None
                }
            },
            Option::is_some,
        );
        assert!(
            ok,
            "第 {k} 次运行既未崩溃也未完成入湖：{:#?}",
            s.stderr_lines()
        );
        if done == Some(true) {
            assert!(k > 1, "一次都没有走到崩溃点");
            stop(&mut s);
            assert!(dangling(&cold).is_empty(), "{:?}", dangling(&cold));
            return;
        }
        s.sigkill();
        // 崩溃之后、重启之前：冷层当前版本各快照引用的对象都在
        let missing = dangling(&cold);
        assert!(
            missing.is_empty(),
            "第 {k} 个写入请求之后崩溃，悬空引用：{missing:?}"
        );
    }
    panic!("30 次重启后仍未完成入湖");
}

/// 「已入湖的实例」（冷层当前版本 N = 1），并在此后对 `v<N+1>.metadata.json` 施加 `rules`（`{v}` 换成该对象的键）。
fn laked_with_faults(c: &Ctl, run: &Run, rules: &str) -> (Server, u64) {
    let s = c.laked(run, plain);
    let n = c.cold().version().unwrap();
    let key = format!("_iceberg/default/app/metadata/v{}.metadata.json", n + 1);
    c.faults(&rules.replace("{v}", &key));
    (s, n)
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_exists_same() {
    let c = Ctl::new();
    // 照常创建，再以「已存在」作答一次：即请求超时后客户端重试时得到的应答
    let (s, n) = laked_with_faults(&c, &Run::lake(), "create {v} after-409 times=1\n");
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    let m = c.wait_lake(c.hours_ago(4), &s);
    let cold = c.cold();
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert!(!cold.exists(&format!("metadata/v{}.metadata.json", n + 2)));
    assert_eq!(handover(&m, 0)["metadata_version"], n + 1, "{m}");
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_same_number_same_content() {
    let c = Ctl::new();
    let (s, n) = laked_with_faults(
        &c,
        &Run::lake(),
        "create {v} after-409 times=1\nget {v} 503 times=1\n",
    );
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    let m = c.wait_lake(c.hours_ago(4), &s);
    let cold = c.cold();
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert!(!cold.exists(&format!("metadata/v{}.metadata.json", n + 2)));
    assert_eq!(
        ColdDir::snapshots(&cold.metadata(n + 1)).len(),
        ColdDir::snapshots(&cold.metadata(n)).len() + 1
    );
    assert_eq!(handover(&m, 0)["metadata_version"], n + 1, "{m}");
}

/// 已入湖的实例在下一次冷层提交创建 `v<N+1>.metadata.json` 之前停住，测试台架以另一份合法的 v2 表元数据抢先创建它；
/// 返回实例、N 与抢先写入的快照号。
fn conflict(c: &Ctl, run: &Run) -> (Server, u64, i64) {
    let s = c.laked(run, plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    c.hold("cold_commit.before_metadata_put");
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_held("cold_commit.before_metadata_put", &s, WAIT);
    let (meta, snap) = foreign_metadata(&cold, n);
    std::fs::write(
        cold.path(&format!("metadata/v{}.metadata.json", n + 1)),
        serde_json::to_vec(&meta).unwrap(),
    )
    .unwrap();
    (s, n, snap)
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_exists_conflict() {
    let c = Ctl::new();
    let (s, n, snap) = conflict(&c, &Run::lake());
    c.release("cold_commit.before_metadata_put");
    let m = c.wait_lake(c.hours_ago(4), &s);
    let meta = c.cold().metadata(n + 2);
    let snaps = ColdDir::snapshots(&meta);
    assert!(snaps.iter().any(|x| x["snapshot-id"] == snap), "{meta}");
    assert_eq!(
        current_snapshot(&meta)["parent-snapshot-id"],
        snap,
        "{meta}"
    );
    assert_eq!(handover(&m, 0)["metadata_version"], n + 2, "{m}");
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_serial_lake_and_ttl() {
    let c = Ctl::new();
    // 冷层文件 F：此刻前第 3 小时的桶入湖
    let mut s = c.laked(&Run::lake(), plain);
    stop(&mut s);
    let run = Run::default()
        .with("flush.interval", "1s")
        .with("lifecycle.seal_grace", "1s")
        .with("lifecycle.lake_watermark", "2s")
        .with("retention.period", "3s")
        .raw("storage.commit_max_retries", "0");
    c.hold("ttl.before_run");
    c.hold("lake.after_digest_check");
    let s = c.start(&run);
    c.wait_held("ttl.before_run", &s, WAIT);
    // 另一个待入湖桶
    ingest_records(&s, APP, c.hours_ago(2), 0, 10, plain);
    c.wait_held("lake.after_digest_check", &s, WAIT);
    let cold = c.cold();
    let n = cold.version().unwrap();
    c.release("ttl.before_run");
    c.release("lake.after_digest_check");
    let (v, ok) = poll(WAIT, || cold.version().unwrap(), |v| *v >= n + 2);
    assert!(ok, "冷层版本停在 {v}：{:#?}", s.stderr_lines());
    let r = ingest_one(&s);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(cold.version(), Some(n + 2));
    let (m1, m2) = (cold.metadata(n + 1), cold.metadata(n + 2));
    let (a, b) = (current_snapshot(&m1), current_snapshot(&m2));
    let ops: BTreeSet<String> = [&a, &b]
        .iter()
        .map(|x| {
            x["summary"]["amberlog.operation"]
                .as_str()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(ops, BTreeSet::from(["lake".to_owned(), "ttl".to_owned()]));
    assert_eq!(b["parent-snapshot-id"], a["snapshot-id"]);
    assert!(disabled_events(&s).is_empty(), "{:#?}", s.stderr_lines());
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_retry_exhausted() {
    let c = Ctl::new();
    let run = Run::lake().raw("storage.commit_max_retries", "0");
    let (s, _, _) = conflict(&c, &run);
    c.release("cold_commit.before_metadata_put");
    wait_disabled(&s, WAIT);
    ingest_one(&s).assert_envelope(500, "internal");
}

#[test]
fn cold_table_hint_after_commit() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    assert_eq!(hint(&cold), (n + 1).to_string());
}

/// 两个桶入湖（冷层当前版本 N 不小于 2）后以 SIGTERM 退出，测试台架把提示改为 `1`。返回 N。
fn lagging_hint(c: &Ctl) -> u64 {
    let mut s = c.laked(&Run::lake(), plain);
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    stop(&mut s);
    let cold = c.cold();
    let n = cold.version().unwrap();
    assert!(n >= 2);
    set_hint(&cold, "1");
    n
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_hint_before_create() {
    let c = Ctl::new();
    let n = lagging_hint(&c);
    let mut s = c.start(&Run::lake().pause("cold_commit.after_metadata_put", 1));
    ingest_records(&s, APP, c.hours_ago(5), 0, 10, plain);
    assert!(wait_file(&c.marker(), WAIT), "{:#?}", s.stderr_lines());
    s.sigkill();
    let cold = c.cold();
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    let h: u64 = hint(&cold).parse().unwrap();
    assert!(h >= n, "提示为 {h}，N 为 {n}");
}

const HINT_WRITE_403: &str = "write _iceberg/default/app/metadata/version-hint.text 403\n";

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_hint_write_fails() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let cold = c.cold();
    let n = cold.version().unwrap();
    c.faults(HINT_WRITE_403);
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    let m = c.wait_lake(c.hours_ago(4), &s);
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert_eq!(handover(&m, 0)["metadata_version"], n + 1, "{m}");
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_hint_lag_write_fails() {
    let c = Ctl::new();
    let n = lagging_hint(&c);
    c.faults(HINT_WRITE_403);
    let s = c.start(&Run::lake());
    ingest_records(&s, APP, c.hours_ago(5), 0, 10, plain);
    let m = c.wait_lake(c.hours_ago(5), &s);
    let cold = c.cold();
    assert!(cold.exists(&format!("metadata/v{}.metadata.json", n + 1)));
    assert_eq!(handover(&m, 0)["metadata_version"], n + 1, "{m}");
    assert_eq!(hint(&cold), "1");
}

// ---------------------------------------------------------------------------
// 确定冷层当前版本
// ---------------------------------------------------------------------------

/// 场景「提示落后时按探测找到当前版本」的对象存储状态：`app` 的三个桶（此刻前第 3、4、5 小时，各 10 条）各自完成入湖，
/// 实例以 SIGTERM 退出。返回冷层当前版本 N。
fn three_laked_stopped(c: &Ctl) -> u64 {
    let mut s = c.start(&Run::lake());
    for h in [3, 4, 5] {
        ingest_records(&s, APP, c.hours_ago(h), 0, 10, plain);
        c.wait_lake(c.hours_ago(h), &s);
    }
    stop(&mut s);
    c.cold().version().unwrap()
}

/// 覆盖三个桶的时间范围。
fn three_range(c: &Ctl) -> (i64, i64) {
    (c.hours_ago(5), c.hours_ago(2))
}

#[test]
fn cold_table_current_hint_lag() {
    let c = Ctl::new();
    let n = three_laked_stopped(&c);
    set_hint(&c.cold(), &(n - 2).to_string());
    let s = c.start(&Run::lake());
    let (a, b) = three_range(&c);
    assert_eq!(rows(&s, APP, a, b), 30);
}

#[test]
fn cold_table_current_hint_missing_or_corrupt() {
    let c = Ctl::new();
    three_laked_stopped(&c);
    let cold = c.cold();
    let (a, b) = three_range(&c);
    for case in ["missing", "abc"] {
        match case {
            "missing" => std::fs::remove_file(cold.path("metadata/version-hint.text")).unwrap(),
            _ => set_hint(&cold, "abc"),
        }
        let mut s = c.start(&Run::lake());
        assert_eq!(rows(&s, APP, a, b), 30, "提示{case}");
        let r = ingest_one(&s);
        assert_eq!(r.status, 200, "提示{case}：{}", r.text());
        stop(&mut s);
    }
}

#[test]
fn cold_table_current_hint_ahead_disables() {
    let c = Ctl::new();
    let n = three_laked_stopped(&c);
    let cold = c.cold();
    let (a, b) = three_range(&c);
    set_hint(&cold, &(n + 6).to_string());
    let s = c.start(&Run::lake());
    query(&s, APP, a, b).assert_envelope(500, "internal");
    ingest_one(&s).assert_envelope(500, "internal");
    for k in n + 1..=n + 7 {
        assert!(
            !cold.path(&format!("metadata/v{k}.metadata.json")).exists(),
            "不应写出 v{k}"
        );
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_current_hint_inaccessible_503() {
    let c = Ctl::new();
    three_laked_stopped(&c);
    let s = c.start(&Run::lake());
    let (a, b) = three_range(&c);
    c.faults("get _iceberg/default/app/metadata/version-hint.text 503\n");
    query(&s, APP, a, b).assert_envelope(503, "unavailable");
    c.faults("");
    assert_eq!(rows(&s, APP, a, b), 30);
}

// ---------------------------------------------------------------------------
// 冷层对象的读取与停用
// ---------------------------------------------------------------------------

/// 「已入湖的实例」以 SIGTERM 退出，测试台架改动冷层对象（`damage`），再以同一配置重启；以覆盖该桶的时间范围查询
/// `app`，再向 `app` 摄取一条记录：两者都回 500 `internal`。返回重启后的实例。
fn damaged_disables(c: &Ctl, damage: impl FnOnce(&ColdDir)) -> Server {
    let mut s = c.laked(&Run::lake(), plain);
    stop(&mut s);
    damage(&c.cold());
    let s = c.start(&Run::lake());
    let b = c.hours_ago(3);
    query(&s, APP, b, b + HOUR).assert_envelope(500, "internal");
    ingest_one(&s).assert_envelope(500, "internal");
    s
}

#[test]
fn cold_table_unparseable_manifest() {
    let c = Ctl::new();
    damaged_disables(&c, |cold| {
        let meta = cold.current();
        let list = manifest_list(cold, &meta, &current_snapshot(&meta));
        let rel = ColdDir::rel(&meta, &list.entries()[0].manifest_path);
        std::fs::write(cold.path(&rel), b"not avro").unwrap();
    });
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_read_inaccessible() {
    let c = Ctl::new();
    let s = c.laked(&Run::lake(), plain);
    let b = c.hours_ago(3);
    c.faults("* * 503\n");
    query(&s, APP, b, b + HOUR).assert_envelope(503, "unavailable");
    c.faults("");
    assert_eq!(rows(&s, APP, b, b + HOUR), 100);
}

#[test]
fn cold_table_read_missing_data_file() {
    let c = Ctl::new();
    damaged_disables(&c, |cold| {
        let live = live_entries(cold);
        std::fs::remove_file(cold.path(&live[0].path)).unwrap();
    });
}

/// 两个桶（此刻前第 5、3 小时）都以 `lifecycle.lake_watermark` 为 `1h` 入湖；S1 是第 5 小时那个桶的入湖快照。
#[test]
fn cold_table_retention_read_not_disabled() {
    let c = Ctl::new();
    let keep = Run::lake()
        .raw("cold_retention.snapshots", "1")
        .with("cold_retention.age", "1s")
        .with("cold_retention.check_interval", "5s");
    let mut s = c.start(&keep.clone().with("storage.delete_delay", "1h"));
    ingest_records(&s, APP, c.hours_ago(5), 0, 10, plain);
    c.wait_lake(c.hours_ago(5), &s);
    ingest_records(&s, APP, c.hours_ago(3), 10, 10, plain);
    c.wait_lake(c.hours_ago(3), &s);
    stop(&mut s);
    let cold = c.cold();
    let meta = cold.current();
    let mut snaps = ColdDir::snapshots(&meta);
    assert_eq!(snaps.len(), 2, "{meta}");
    snaps.sort_by_key(|x| x["sequence-number"].as_i64());
    let list = ColdDir::rel(&meta, snaps[0]["manifest-list"].as_str().unwrap());
    std::fs::remove_file(cold.path(&list)).unwrap();

    let s = c.start(&keep.with("storage.delete_delay", "1s"));
    std::thread::sleep(Duration::from_secs(30));
    let metrics = s.get_auth("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains(r#"stream="app""#)),
        "{metrics}"
    );
    assert_eq!(rows(&s, APP, c.hours_ago(5), c.hours_ago(2)), 20);
}

#[test]
fn cold_table_table_gone_disables() {
    let c = Ctl::new();
    let ds = c.ds();
    let mut s = c.laked(&Run::lake(), plain);
    stop(&mut s);
    assert_eq!(
        ds.head()["summary"]["cold"]["record_count"],
        100,
        "{}",
        ds.head()
    );
    let s = damaged_disables_after_stop(&c, |cold| std::fs::remove_dir_all(&cold.dir).unwrap());
    let events = disabled_events(&s);
    assert!(
        events
            .iter()
            .any(|e| e["dataset"] == "default/app" && e["state"] == "manifest_unreadable"),
        "{:#?}",
        s.stderr_lines()
    );
}

/// 同 [`damaged_disables`]，但实例已入湖并已停止。
fn damaged_disables_after_stop(c: &Ctl, damage: impl FnOnce(&ColdDir)) -> Server {
    damage(&c.cold());
    let s = c.start(&Run::lake());
    let b = c.hours_ago(3);
    query(&s, APP, b, b + HOUR).assert_envelope(500, "internal");
    ingest_one(&s).assert_envelope(500, "internal");
    s
}

/// 「已入湖的实例」（`storage.delete_delay` 为 `1s`，等到入湖输入的热层文件已物理删除）以 SIGTERM 退出，再以
/// `retention.period` 为 `2h`、`storage.delete_delay` 为 `delay`（其余配置不变）重启，等到冷层 TTL 把该桶的冷层数据文件
/// F 移出冷层存活文件集。`before_restart` 在重启之前调用。返回实例、F 与 F 的冷层变更段留痕。
fn ttl_removes_f(c: &Ctl, delay: &str, before_restart: impl FnOnce()) -> (Server, String, Value) {
    let base = Run::lake().with("storage.delete_delay", "1s");
    let mut s = c.laked(&base, plain);
    let ds = c.ds();
    let m = ds.bucket_now(&bucket_label(c.hours_ago(3))).unwrap();
    let inputs: Vec<String> = m["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|d| d["reason"] == "lake")
        .map(|d| d["path"].as_str().unwrap().to_owned())
        .collect();
    let (_, gone) = poll(
        Duration::from_secs(300),
        || inputs.iter().any(|p| ds.exists(p)),
        |any| !any,
    );
    assert!(gone, "入湖输入没有物理删除：{:#?}", s.stderr_lines());
    let f = live_entries(&c.cold())[0].path.clone();
    stop(&mut s);
    before_restart();

    let s = c.start(&Run::ttl("2h").with("storage.delete_delay", delay));
    c.wait_ttl(&s);
    let (entry, ok) = poll(TTL_WAIT, || cold_change(&ds, &f), Option::is_some);
    assert!(ok, "F 的冷层删除留痕没有写出：{:#?}", s.stderr_lines());
    (s, f, entry.unwrap())
}

/// 当前快照经 `cold_deletion_segment` 可达的冷层变更段中路径为 `path` 的留痕。
fn cold_change(ds: &Ds, path: &str) -> Option<Value> {
    let mut next = ds.head()["cold_deletion_segment"].clone();
    while !next.is_null() {
        let seg = ds.json(next["path"].as_str().unwrap());
        if let Some(d) = seg["deletions"]
            .as_array()
            .unwrap()
            .iter()
            .find(|d| d["path"] == path)
        {
            return Some(d.clone());
        }
        next = seg["previous"].clone();
    }
    None
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_disabled_no_delete() {
    let c = Ctl::new();
    let (s, f, _) = ttl_removes_f(&c, "1s", || c.hold("delete.before_physical"));
    c.wait_held("delete.before_physical", &s, Duration::from_secs(180));
    // 删去热层链尾快照引用的一个桶 manifest，以覆盖该桶的查询使 `app` 停用。本进程已缓存现有快照引用的桶 manifest，
    // 所以先在链尾追加一个快照，让它改引用该桶 manifest 的一份副本，再删去副本：查询找链尾时读到它不存在
    let ds = c.ds();
    let now = now_us();
    let r = ingest_one(&s);
    assert_eq!(r.status, 200, "{}", r.text());
    let label = bucket_label(now);
    let (_, ok) = poll(
        Duration::from_secs(15),
        || ds.bucket_now(&label),
        |m| {
            m.as_ref()
                .is_some_and(|m| !m["files"].as_array().unwrap().is_empty())
        },
    );
    assert!(ok, "{:#?}", s.stderr_lines());
    let head = ds.head();
    let i = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .position(|b| b["bucket"] == label.as_str())
        .unwrap();
    let old = head["buckets"][i]["manifest"].as_str().unwrap().to_owned();
    let copy = format!("manifest/buckets/{label}-01jzzzzzzzzzzzzzzzzzzzzzzz.json");
    ds.write(&copy, &ds.read(&old));
    ds.append_snapshot(|next| next["buckets"][i]["manifest"] = json!(copy));
    ds.remove(&copy);
    let start = amberlog_storage::layout::HourBucket::parse(&label)
        .unwrap()
        .start_us();
    query(&s, APP, start, start + HOUR).assert_envelope(500, "internal");
    wait_disabled(&s, Duration::from_secs(10));
    c.release("delete.before_physical");
    std::thread::sleep(Duration::from_secs(60));
    assert!(c.cold().exists(&f), "停用的数据集删除了冷层文件 {f}");
}

// ---------------------------------------------------------------------------
// 冷层文件的物理删除
// ---------------------------------------------------------------------------

/// 场景「持续运行中不删除冷层对象」的时长：设了 `TESTBED_SOAK_HOURS`（跑满时长的长时运行）时跑满 10 分钟，否则跑 2 分钟
/// 的缩短版（同审计不变量测试的长时场景做法）。
fn delete_scope_duration() -> Duration {
    if std::env::var_os("TESTBED_SOAK_HOURS").is_some() {
        Duration::from_secs(600)
    } else {
        Duration::from_secs(120)
    }
}

#[test]
fn cold_table_delete_scope() {
    let c = Ctl::new();
    let run = Run::lake()
        .with("hot_fsck.interval", "10s")
        .with("hot_fsck.min_orphan_age", "1s");
    let s = c.start(&run);
    let cold = c.cold();
    let deadline = Instant::now() + delete_scope_duration();
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut next_list = Instant::now();
    let mut i = 0u64;
    while Instant::now() < deadline {
        let h = 3 + (i % 4) as i64;
        ingest_records(&s, APP, c.hours_ago(h), i * 10, 10, plain);
        i += 1;
        if Instant::now() >= next_list {
            let now: BTreeSet<String> = cold.keys().into_iter().collect();
            let lost: Vec<&String> = seen.difference(&now).collect();
            assert!(lost.is_empty(), "冷层对象被删除：{lost:?}");
            seen = now;
            next_list += Duration::from_secs(10);
        }
        std::thread::sleep(Duration::from_millis(500));
    }
    assert!(
        !seen.is_empty(),
        "期间没有任何桶入湖：{:#?}",
        s.stderr_lines()
    );
}

#[test]
fn cold_table_delete_delay() {
    let c = Ctl::new();
    let (s, f, entry) = ttl_removes_f(&c, "1m", || ());
    let t = entry["deleted_at"].as_i64().unwrap();
    let wait_until = |us: i64| {
        let left = us - now_us();
        if left > 0 {
            std::thread::sleep(Duration::from_micros(left as u64));
        }
    };
    wait_until(t + 30_000_000);
    assert!(c.cold().exists(&f), "T 加 30 秒时 F 已删除");
    let limit = (t + MINUTE + 60 * MINUTE - now_us()).max(0) as u64;
    let (_, gone) = poll(Duration::from_micros(limit), || c.cold().exists(&f), |e| !e);
    assert!(
        gone,
        "T 加 1 分钟加 1 小时之内 F 未删除：{:#?}",
        s.stderr_lines()
    );
}

#[test]
fn cold_table_delete_after_restart() {
    let c = Ctl::new();
    let (mut s, f, entry) = ttl_removes_f(&c, "1m", || ());
    let t = entry["deleted_at"].as_i64().unwrap();
    let kill_at = t + 10_000_000 - now_us();
    if kill_at > 0 {
        std::thread::sleep(Duration::from_micros(kill_at as u64));
    }
    s.sigkill();
    assert!(c.cold().exists(&f), "SIGKILL 之前 F 已删除");
    let s = c.start(&Run::ttl("2h").with("storage.delete_delay", "1m"));
    let accepted = now_us();
    let limit = (accepted.max(t + MINUTE) + 60 * MINUTE - now_us()).max(0) as u64;
    let (_, gone) = poll(Duration::from_micros(limit), || c.cold().exists(&f), |e| !e);
    assert!(gone, "重启后 F 未删除：{:#?}", s.stderr_lines());
}

// ---------------------------------------------------------------------------
// 停机与冷层孤儿
// ---------------------------------------------------------------------------

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_no_delete_in_shutdown() {
    let c = Ctl::new();
    let base = Run::ttl("2h").with("storage.delete_delay", "1m");
    // 先入湖（不配 retention），再以 TTL 配置运行、停机时停在 `shutdown.before_exit`
    let mut s = c.laked(&Run::lake(), plain);
    let f = live_entries(&c.cold())[0].path.clone();
    stop(&mut s);
    let mut s = c.start(&base.clone().pause("shutdown.before_exit", 1));
    let (_, ok) = poll(
        TTL_WAIT,
        || live_entries(&c.cold()).iter().any(|e| e.path == f),
        |l| !l,
    );
    assert!(ok, "F 没有离开冷层存活文件集：{:#?}", s.stderr_lines());
    s.signal("TERM");
    assert!(
        wait_file(&c.marker(), Duration::from_secs(60)),
        "{:#?}",
        s.stderr_lines()
    );
    // 停在退出之前 200 秒：其间越过 F 的删除时刻（`deleted_at` 加 1 分钟）
    std::thread::sleep(Duration::from_secs(200));
    s.sigkill();
    assert!(c.cold().exists(&f), "停机过程中删除了 F");
}

/// 冷层孤儿清理加快（每 5 秒、最小年龄 1 秒）的入湖配置。
fn orphan_run() -> Run {
    orphans_fast(Run::lake())
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_orphan_cleaned() {
    let c = Ctl::new();
    let mut s = c.laked(&orphan_run(), plain);
    let cold = c.cold();
    let before = data_files(&cold);
    stop(&mut s);
    let mut s = c.start(&orphan_run().pause("lake.after_cold_upload", 1));
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    assert!(wait_file(&c.marker(), WAIT), "{:#?}", s.stderr_lines());
    s.sigkill();
    let g: Vec<String> = data_files(&cold).difference(&before).cloned().collect();
    assert!(!g.is_empty(), "崩溃前没有上传冷层数据文件");

    let s = c.start(&orphan_run());
    let (_, gone) = poll(
        Duration::from_secs(150),
        || g.iter().any(|p| cold.exists(p)),
        |any| !any,
    );
    assert!(gone, "孤儿 {g:?} 未被清理：{:#?}", s.stderr_lines());
    c.wait_lake(c.hours_ago(4), &s);
    assert!(dangling(&cold).is_empty(), "{:?}", dangling(&cold));
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_orphan_inflight_kept() {
    let c = Ctl::new();
    let s = c.laked(&orphan_run(), plain);
    let cold = c.cold();
    let before = data_files(&cold);
    c.hold("lake.after_cold_upload");
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_held("lake.after_cold_upload", &s, WAIT);
    let g: Vec<String> = data_files(&cold).difference(&before).cloned().collect();
    assert!(!g.is_empty());
    let until = Instant::now() + Duration::from_secs(30);
    while Instant::now() < until {
        assert!(g.iter().all(|p| cold.exists(p)), "暂停期间 G 被删除");
        std::thread::sleep(Duration::from_millis(500));
    }
    c.release("lake.after_cold_upload");
    c.wait_lake(c.hours_ago(4), &s);
    std::thread::sleep(Duration::from_secs(60));
    let live: BTreeSet<String> = live_entries(&cold).into_iter().map(|e| e.path).collect();
    for p in &g {
        assert!(live.contains(p), "{p} 不是冷层存活数据文件");
    }
}

#[test]
#[cfg(feature = "fault-injection")]
fn cold_table_orphan_newly_referenced_kept() {
    let c = Ctl::new();
    let s = c.laked(&orphan_run(), plain);
    let cold = c.cold();
    let before = data_files(&cold);
    c.hold("lake.after_cold_upload");
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_held("lake.after_cold_upload", &s, WAIT);
    let g: Vec<String> = data_files(&cold).difference(&before).cloned().collect();
    assert!(!g.is_empty());
    std::thread::sleep(Duration::from_secs(10));
    c.hold("cold_orphan.before_delete");
    c.wait_held("cold_orphan.before_delete", &s, Duration::from_secs(30));
    c.release("lake.after_cold_upload");
    c.wait_lake(c.hours_ago(4), &s);
    c.release("cold_orphan.before_delete");
    std::thread::sleep(Duration::from_secs(30));
    let live: BTreeSet<String> = live_entries(&cold).into_iter().map(|e| e.path).collect();
    for p in &g {
        assert!(cold.exists(p) && live.contains(p), "{p}");
    }
    assert!(dangling(&cold).is_empty(), "{:?}", dangling(&cold));
}

#[test]
fn cold_table_orphan_abort() {
    let c = Ctl::new();
    // 两个桶入湖（当前版本有两个快照），此时孤儿清理按缺省的 1 小时周期、不会运行
    let mut s = c.laked(&Run::lake(), plain);
    ingest_records(&s, APP, c.hours_ago(4), 0, 10, plain);
    c.wait_lake(c.hours_ago(4), &s);
    stop(&mut s);
    let cold = c.cold();
    let meta = cold.current();
    let snaps = ColdDir::snapshots(&meta);
    assert!(snaps.len() >= 2);
    // 冷层孤儿 G，并删去一个非当前快照的清单列表
    let g = format!(
        "data/_timestamp_day={}/01jzzzzzzzzzzzzzzzzzzzzzzz.parquet",
        day_of(c.hours_ago(3))
    );
    std::fs::write(cold.path(&g), b"orphan").unwrap();
    let old = snaps
        .iter()
        .find(|x| x["snapshot-id"] != meta["current-snapshot-id"])
        .unwrap();
    std::fs::remove_file(cold.path(&ColdDir::rel(&meta, old["manifest-list"].as_str().unwrap())))
        .unwrap();
    std::thread::sleep(Duration::from_secs(2));

    let s = c.start(&orphan_run());
    std::thread::sleep(Duration::from_secs(15));
    assert!(cold.exists(&g), "读不到清单时仍清理了孤儿");
    let r = query(&s, APP, c.hours_ago(4), c.hours_ago(2));
    assert_eq!(r.status, 200, "{}", r.text());
}
