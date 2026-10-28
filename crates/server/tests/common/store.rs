//! `file://` 对象存储一侧的检查与构造：读提示、当前快照与桶 manifest，列对象、读 Parquet，等提交，在暂停点崩溃
//! （REQ-AMB-DM-NAMESPACE、REQ-AMB-STO-* 的系统测试共用）。测试直接读写临时目录下的对象。
//!
//! 桶 manifest 由清单片组成（REQ-AMB-STO-HOT-LAYOUT-V8）：[`restore_with`] 是测试一侧按块「还原」各条独立写的还原，
//! 读桶 manifest 的辅助函数都经它，不直接把头片当整份桶 manifest。

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::array::{Array, TimestampMicrosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{LogicalType, Repetition, Type as PhysicalType};
use serde_json::Value;

use super::{Fixture, Kv, Server, wait_file};

/// 2026-08-22T10:00:00Z（UTC 微秒）。
pub const H10: i64 = 1_787_392_800_000_000;
pub const HOUR: i64 = 3_600_000_000;

/// `flush.interval` 为 `1s`。
pub const FLUSH_1S: Kv = ("flush.interval", Some("\"1s\""));

/// 完整字节的 `sha256:<hex>`。
pub fn sha256(bytes: &[u8]) -> String {
    amberlog_storage::chain::Digest::of(bytes).to_string()
}

/// 快照 `id` 相对数据集前缀的路径（`manifest/` 加零填充 20 位编号）。
pub fn snapshot_rel(id: u64) -> String {
    amberlog_storage::layout::snapshot_path(amberlog_storage::chain::SnapshotId(id))
}

/// 按 REQ-AMB-STO-HOT-LAYOUT-V8「还原」，由 `json` 读出的清单片还原头片 `head` 所指的桶 manifest，得
/// `{"format_version","bucket","files","deletions","handovers"}`（`deletions` 按写出的先后：留痕链自最早的全量片起，
/// 再接增量链上各增量片）。不核对摘要。
pub fn restore_with(json: impl Fn(&str) -> Value, head: &str) -> Value {
    let mut deltas = Vec::new();
    let mut p = json(head);
    while p["kind"] == "delta" {
        let previous = p["previous"]["path"].as_str().unwrap().to_owned();
        deltas.push(p);
        p = json(&previous);
    }
    assert_eq!(p["kind"], "full", "{head} 的增量链应止于全量片");
    let mut ledger = Vec::new();
    let mut lp = p["ledger_previous"].clone();
    while !lp.is_null() {
        let q = json(lp["path"].as_str().unwrap());
        lp = q["ledger_previous"].clone();
        ledger.push(q);
    }
    let array = |v: &Value, k: &str| v[k].as_array().unwrap().clone();
    let mut deletions: Vec<Value> = ledger
        .iter()
        .rev()
        .flat_map(|q| array(q, "deletions"))
        .collect();
    deletions.extend(array(&p, "deletions"));
    let mut files = array(&p, "files");
    let mut handovers = array(&p, "handovers");
    for d in deltas.iter().rev() {
        let gone: Vec<Value> = array(d, "deletions");
        files.retain(|f| !gone.iter().any(|g| g["path"] == f["path"]));
        files.extend(array(d, "files_added"));
        deletions.extend(gone);
        handovers.extend(array(d, "handovers_added"));
    }
    serde_json::json!({
        "format_version": p["format_version"],
        "bucket": p["bucket"],
        "files": files,
        "deletions": deletions,
        "handovers": handovers,
    })
}

/// 头片 `head` 所指的桶 manifest 的全部清单片路径：增量链（自头片起到基准片），再接基准片的留痕链。
pub fn pieces_with(json: impl Fn(&str) -> Value, head: &str) -> Vec<String> {
    let mut out = vec![head.to_owned()];
    let mut p = json(head);
    while p["kind"] == "delta" {
        let previous = p["previous"]["path"].as_str().unwrap().to_owned();
        out.push(previous.clone());
        p = json(&previous);
    }
    let mut lp = p["ledger_previous"].clone();
    while !lp.is_null() {
        let path = lp["path"].as_str().unwrap().to_owned();
        out.push(path.clone());
        lp = json(&path)["ledger_previous"].clone();
    }
    out
}

/// 一个数据集前缀 `<store>/amberlog/default/<stream>/`。
pub struct Ds {
    pub dir: PathBuf,
}

impl Fixture {
    pub fn dataset(&self, stream: &str) -> Ds {
        Ds {
            dir: self
                .store_dir()
                .join("amberlog")
                .join("default")
                .join(stream),
        }
    }

    /// 以给定配置启动，要求进程停在暂停点 `step`（`TESTBED_FAULT_PAUSE`），返回进程与标记文件路径。
    pub fn start_paused(&self, overrides: &[Kv], step: &str) -> (Server, PathBuf) {
        let marker = self.root().join("fault-marker");
        let _ = std::fs::remove_file(&marker);
        let mut cmd = self.serve(overrides);
        cmd.env("TESTBED_FAULT_PAUSE", step)
            .env("TESTBED_FAULT_MARKER", &marker);
        (Server::start(cmd), marker)
    }
}

/// 可放行暂停点的控制文件（`TESTBED_FAULT_GATE` / `TESTBED_FAULT_GATE_FILE`，见 `amberlog_storage::fault::gate`）。
pub struct Gate(PathBuf);

impl Fixture {
    /// 以给定配置启动，要求第一次走到步骤 `step` 的提交停住，直到 [`Gate::release`]。
    pub fn start_gated(&self, overrides: &[Kv], step: &str) -> (Server, Gate) {
        let control = self.root().join("fault-gate");
        std::fs::write(&control, "hold").unwrap();
        let mut cmd = self.serve(overrides);
        cmd.env("TESTBED_FAULT_GATE", step)
            .env("TESTBED_FAULT_GATE_FILE", &control);
        (Server::start(cmd), Gate(control))
    }
}

impl Gate {
    /// 是否已有提交停在暂停点上（或停过）。
    pub fn held(&self) -> bool {
        std::fs::read_to_string(&self.0).is_ok_and(|s| s.trim() != "hold")
    }

    /// 等到有提交停在暂停点上（至多 `limit`）。
    pub fn wait_held(&self, server: &Server, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.held() {
            assert!(
                Instant::now() < deadline,
                "{} 秒内没有提交走到暂停点：{:#?}",
                limit.as_secs(),
                server.stderr_lines()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 放行停住的提交。
    pub fn release(&self) {
        std::fs::write(&self.0, "go").unwrap();
    }
}

/// 对象存储请求的故障注入控制文件（`TESTBED_FAULT_STORE`，规则写法见 `amberlog_storage::fault::store`；路径前缀相对
/// `<storage.prefix>/`，如 `default/app/manifest/_current`）。被测进程每个请求前读一次，改写即生效。
pub struct StoreFaults(PathBuf);

impl Fixture {
    /// 以给定配置启动并等到 `ready`，被测进程的对象存储请求按控制文件注入故障（起初为空，照常转发）。
    pub fn start_with_store_faults(&self, overrides: &[Kv]) -> (Server, StoreFaults) {
        let control = self.root().join("fault-store");
        std::fs::write(&control, "").unwrap();
        let mut cmd = self.serve(overrides);
        cmd.env("TESTBED_FAULT_STORE", &control);
        (Server::start(cmd), StoreFaults(control))
    }
}

impl StoreFaults {
    /// 把规则改为 `rules`（每行一条）。
    pub fn set(&self, rules: &str) {
        std::fs::write(&self.0, rules).unwrap();
    }
}

/// 等暂停点标记出现后 SIGKILL（至多 30 秒）。
pub fn kill_at_marker(server: &mut Server, marker: &Path) {
    assert!(
        wait_file(marker, Duration::from_secs(30)),
        "未走到暂停点：{:#?}",
        server.stderr_lines()
    );
    server.sigkill();
}

impl Ds {
    pub fn path(&self, rel: &str) -> PathBuf {
        self.dir.join(rel)
    }

    pub fn exists(&self, rel: &str) -> bool {
        self.path(rel).exists()
    }

    pub fn read(&self, rel: &str) -> Vec<u8> {
        std::fs::read(self.path(rel)).unwrap_or_else(|e| panic!("读 {rel}：{e}"))
    }

    pub fn json(&self, rel: &str) -> Value {
        serde_json::from_slice(&self.read(rel)).unwrap_or_else(|e| panic!("{rel} 不是 JSON：{e}"))
    }

    pub fn write(&self, rel: &str, bytes: &[u8]) {
        let p = self.path(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(p, bytes).unwrap();
    }

    pub fn write_json(&self, rel: &str, v: &Value) {
        self.write(rel, v.to_string().as_bytes());
    }

    pub fn remove(&self, rel: &str) {
        std::fs::remove_file(self.path(rel)).unwrap_or_else(|e| panic!("删 {rel}：{e}"));
    }

    /// 提示 `manifest/_current` 所记的快照号；提示不存在或读不出整数时为 `None`。
    pub fn hint(&self) -> Option<u64> {
        let bytes = std::fs::read(self.path("manifest/_current")).ok()?;
        serde_json::from_slice::<Value>(&bytes).ok()?["snapshot_id"].as_u64()
    }

    /// 当前快照号：读提示得 h（不存在为 0），再从 h+1 起逐个确认快照文件是否存在，取存在的最大编号
    /// （找链尾，REQ-AMB-STO-MANIFEST-COMMIT-V3）；尚无任何快照为 `None`。
    pub fn current(&self) -> Option<u64> {
        let mut tail = self.hint();
        while self.exists(&snapshot_rel(tail.map_or(1, |t| t + 1))) {
            tail = Some(tail.map_or(1, |t| t + 1));
        }
        tail
    }

    pub fn snapshot(&self, id: u64) -> Value {
        self.json(&snapshot_rel(id))
    }

    /// 当前快照。
    pub fn head(&self) -> Value {
        self.snapshot(self.current().expect("当前快照存在"))
    }

    /// 以当前快照 N 为父写出快照 N+1（复制 N 的内容，`snapshot_id`、`parent_snapshot_id` 与 `parent_digest` 按 N 改写，
    /// 再经 `edit` 修改），使之成为链尾；不改提示。返回 N+1。
    pub fn append_snapshot(&self, edit: impl FnOnce(&mut Value)) -> u64 {
        let n = self.current().expect("当前快照存在");
        let parent = self.read(&snapshot_rel(n));
        let mut snap: Value = serde_json::from_slice(&parent).unwrap();
        snap["snapshot_id"] = serde_json::json!(n + 1);
        snap["parent_snapshot_id"] = serde_json::json!(n);
        snap["parent_digest"] = serde_json::json!(sha256(&parent));
        edit(&mut snap);
        self.write_json(&snapshot_rel(n + 1), &snap);
        n + 1
    }

    /// 自当前快照沿 `parent_snapshot_id` 回溯得到的快照（链头在前；父快照已被快照回收时到此为止）。
    pub fn chain(&self) -> Vec<Value> {
        let mut out = Vec::new();
        let mut next = self.current();
        while let Some(id) = next {
            if !self.exists(&snapshot_rel(id)) {
                break;
            }
            let s = self.snapshot(id);
            next = s["parent_snapshot_id"].as_u64();
            out.push(s);
        }
        out
    }

    /// 数据集前缀下全部对象的相对键（排序）。`object_store` 本地后端写入时的暂存文件（名字带 `#`，写完改名）
    /// 不是对象，不列出；进程运行中列举时文件可能恰好消失，跳过即可。
    pub fn keys(&self) -> Vec<String> {
        fn walk(base: &Path, p: &Path, out: &mut Vec<String>) {
            let Ok(rd) = std::fs::read_dir(p) else {
                return;
            };
            for e in rd.flatten() {
                let Ok(ft) = e.file_type() else {
                    continue;
                };
                if ft.is_dir() {
                    walk(base, &e.path(), out);
                } else if !e.file_name().to_string_lossy().contains('#') {
                    let rel = e.path().strip_prefix(base).unwrap().to_owned();
                    out.push(rel.to_string_lossy().into_owned());
                }
            }
        }
        let mut out = Vec::new();
        walk(&self.dir, &self.dir, &mut out);
        out.sort();
        out
    }

    /// `prefix` 之下的对象键。
    pub fn keys_under(&self, prefix: &str) -> Vec<String> {
        self.keys()
            .into_iter()
            .filter(|k| k.starts_with(prefix))
            .collect()
    }

    /// `manifest/` 下一级的快照编号（升序）。
    pub fn snapshot_ids(&self) -> Vec<u64> {
        let mut ids: Vec<u64> = self
            .keys_under("manifest/")
            .iter()
            .filter_map(|k| amberlog_storage::layout::parse_snapshot_path(k).map(|id| id.0))
            .collect();
        ids.sort();
        ids
    }

    /// 头片 `head` 所指的桶 manifest（按块还原，见 [`restore_with`]）。
    pub fn restore(&self, head: &str) -> Value {
        restore_with(|rel| self.json(rel), head)
    }

    /// 头片 `head` 所指的桶 manifest 的全部清单片路径（见 [`pieces_with`]）。
    pub fn pieces(&self, head: &str) -> Vec<String> {
        pieces_with(|rel| self.json(rel), head)
    }

    /// 快照 `s` 的 `buckets` 所引用的桶 manifest（与 `buckets` 同序，按块还原）。
    pub fn bucket_manifests(&self, s: &Value) -> Vec<Value> {
        s["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .map(|b| self.restore(b["manifest"].as_str().unwrap()))
            .collect()
    }

    /// 快照 `s` 经 `buckets` 可达的全部 `files` 条目。
    pub fn file_entries(&self, s: &Value) -> Vec<Value> {
        self.bucket_manifests(s)
            .into_iter()
            .flat_map(|m| m["files"].as_array().unwrap().clone())
            .collect()
    }

    /// 等当前快照的 `summary.record_count` 不小于 `n`（至多 `limit`），返回该快照。
    pub fn wait_records(&self, n: u64, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(id) = self.current() {
                let s = self.snapshot(id);
                if s["summary"]["record_count"].as_u64().unwrap() >= n {
                    return s;
                }
            }
            assert!(
                Instant::now() < deadline,
                "{} 内未提交到 {n} 条记录（当前快照 = {:?}）",
                limit.as_secs(),
                self.current()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    /// 等提示与当前快照相等（快照创建成功后提示随即写出，二者之间有一小段间隔；至多 `limit`），返回该编号。
    pub fn wait_hint_at_tail(&self, limit: Duration) -> u64 {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(n) = self.current()
                && self.hint() == Some(n)
            {
                return n;
            }
            assert!(
                Instant::now() < deadline,
                "{} 内提示未追上当前快照（提示 {:?}，当前快照 {:?}）",
                limit.as_secs(),
                self.hint(),
                self.current()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 等快照 `id` 出现（至多 `limit`）。
    pub fn wait_snapshot(&self, id: u64, limit: Duration) {
        let deadline = Instant::now() + limit;
        while !self.exists(&snapshot_rel(id)) {
            assert!(
                Instant::now() < deadline,
                "{} 内未见快照 {id}",
                limit.as_secs()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 快照 `id` 新增的 `files` 条目（`added_in` 等于 `id`）。
    pub fn added_entries(&self, id: u64) -> Vec<Value> {
        self.file_entries(&self.snapshot(id))
            .into_iter()
            .filter(|f| f["added_in"] == id)
            .collect()
    }

    /// 等当前快照出现（至多 `limit`），返回其快照号。
    pub fn wait_current(&self, limit: Duration) -> u64 {
        let deadline = Instant::now() + limit;
        loop {
            if let Some(id) = self.current() {
                return id;
            }
            assert!(Instant::now() < deadline, "{} 内未见快照", limit.as_secs());
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

/// Parquet 文件的一列：列名、`field_id`、物理类型、逻辑类型与重复级别。
#[derive(Debug, Clone)]
pub struct ParquetColumn {
    pub name: String,
    pub field_id: i32,
    pub physical: PhysicalType,
    pub logical: Option<LogicalType>,
    pub repetition: Repetition,
}

/// 以 Parquet 读取器读出文件的各顶层列。
pub fn parquet_columns(path: &Path) -> Vec<ParquetColumn> {
    let f = std::fs::File::open(path).unwrap();
    let b = ParquetRecordBatchReaderBuilder::try_new(f).unwrap();
    let descr = b.parquet_schema();
    (0..descr.num_columns())
        .map(|i| {
            let c = descr.column(i);
            let info = c.self_type().get_basic_info();
            ParquetColumn {
                name: c.name().to_owned(),
                field_id: info.id(),
                physical: c.physical_type(),
                logical: c.logical_type_ref().cloned(),
                repetition: info.repetition(),
            }
        })
        .collect()
}

/// 名为 `name` 的列。
pub fn parquet_column(path: &Path, name: &str) -> ParquetColumn {
    parquet_columns(path)
        .into_iter()
        .find(|c| c.name == name)
        .unwrap_or_else(|| panic!("{} 没有列 {name}", path.display()))
}

/// Parquet 文件的 `_timestamp` 列（UTC 微秒）。
pub fn parquet_timestamps(path: &Path) -> Vec<i64> {
    let f = std::fs::File::open(path).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(f)
        .unwrap()
        .build()
        .unwrap();
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let col = batch
            .column_by_name("_timestamp")
            .unwrap()
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .clone();
        out.extend((0..col.len()).map(|i| col.value(i)));
    }
    out
}

/// 对象存储 `store` 上一个数据集前缀的读写（测试进程自己的客户端，如 [`super::s3::S3Instance::store`]）。
pub struct Remote {
    store: amberlog_storage::store::Store,
    dataset: amberlog_common::dataset::Dataset,
    rt: tokio::runtime::Runtime,
}

impl Remote {
    pub fn new(store: amberlog_storage::store::Store, stream: &str) -> Remote {
        Remote {
            store,
            dataset: amberlog_common::dataset::Dataset::new("default", stream),
            rt: tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap(),
        }
    }

    pub fn read(&self, rel: &str) -> Option<Vec<u8>> {
        let path = self.store.object_path(&self.dataset, rel);
        self.rt
            .block_on(self.store.read(&path))
            .unwrap()
            .map(|f| f.bytes.to_vec())
    }

    pub fn json(&self, rel: &str) -> Value {
        serde_json::from_slice(&self.read(rel).unwrap_or_else(|| panic!("{rel} 不存在"))).unwrap()
    }

    /// 头片 `head` 所指的桶 manifest（按块还原，见 [`restore_with`]）。
    pub fn restore(&self, head: &str) -> Value {
        restore_with(|rel| self.json(rel), head)
    }

    pub fn write_json(&self, rel: &str, v: &Value) {
        use object_store::ObjectStoreExt;
        let path = self.store.object_path(&self.dataset, rel);
        let bytes = bytes_of(v);
        self.rt
            .block_on(self.store.inner.put(&path, bytes.into()))
            .unwrap();
    }

    /// 提示所记的快照号；不存在时为 `None`。
    pub fn hint(&self) -> Option<u64> {
        let bytes = self.read("manifest/_current")?;
        serde_json::from_slice::<Value>(&bytes).ok()?["snapshot_id"].as_u64()
    }

    /// 当前快照号（找链尾，同 [`Ds::current`]）；尚无任何快照为 `None`。
    pub fn current(&self) -> Option<u64> {
        let mut tail = self.hint();
        while self
            .read(&snapshot_rel(tail.map_or(1, |t| t + 1)))
            .is_some()
        {
            tail = Some(tail.map_or(1, |t| t + 1));
        }
        tail
    }

    /// 把数据集前缀下的全部对象复制到本地目录 `dest`，返回可同 `file://` 一样检查的 [`Ds`]。
    pub fn mirror(&self, dest: &Path) -> Ds {
        let prefix = self.store.dataset_prefix(&self.dataset);
        let ds = Ds {
            dir: dest.to_owned(),
        };
        for meta in self.rt.block_on(self.store.list(&prefix)).unwrap() {
            let rel = meta
                .location
                .as_ref()
                .strip_prefix(&format!("{}/", prefix.as_ref()))
                .unwrap()
                .to_owned();
            let got = self
                .rt
                .block_on(self.store.read(&meta.location))
                .unwrap()
                .unwrap();
            ds.write(&rel, &got.bytes);
        }
        ds
    }
}

fn bytes_of(v: &Value) -> Vec<u8> {
    v.to_string().into_bytes()
}

/// `/metrics` 中 `amberlog_manifest_commits_total{org="default",stream=<stream>,result=<result>}` 的值（没有该行为 0）。
pub fn commits_total(server: &Server, stream: &str, result: &str) -> u64 {
    let r = server.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    let key = format!(
        "amberlog_manifest_commits_total{{org=\"default\",stream=\"{stream}\",result=\"{result}\"}} "
    );
    r.text()
        .lines()
        .find_map(|l| l.strip_prefix(&key))
        .map_or(0, |v| v.trim().parse().unwrap())
}

/// `/metrics` 中样本 `series`（指标名连同标签，写法与渲染一致）的值；没有该样本为 `None`。
pub fn metric(server: &Server, series: &str) -> Option<f64> {
    let r = server.get_auth("/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    r.text()
        .lines()
        .find_map(|l| l.strip_prefix(series)?.strip_prefix(' '))
        .map(|v| v.trim().parse().expect("样本值是数"))
}

/// `amberlog_hot_buckets{org="default",stream=<stream>,state=<state>}` 的值。
pub fn hot_buckets(server: &Server, stream: &str, state: &str) -> Option<f64> {
    metric(
        server,
        &format!("amberlog_hot_buckets{{org=\"default\",stream=\"{stream}\",state=\"{state}\"}}"),
    )
}

/// 以 SIGTERM 停止并断言退出码 0（至多 60 秒）。
pub fn stop(server: &mut Server) {
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(60)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// `us` 所在 UTC 小时的起点。
pub fn hour_of(us: i64) -> i64 {
    us.div_euclid(HOUR) * HOUR
}

impl Ds {
    /// 等当前快照中小时桶 `label` 的桶 manifest 的 `deletions` 含 `reason` 为 `compaction` 的条目（至多 `limit`），
    /// 返回该桶 manifest。
    pub fn wait_compaction(&self, label: &str, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            if self.current().is_some()
                && let Some(m) = self.bucket_manifest(&self.head(), label)
                && m["deletions"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|d| d["reason"] == "compaction")
            {
                return m;
            }
            assert!(
                Instant::now() < deadline,
                "{} 内桶 {label} 未被合并",
                limit.as_secs()
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// 快照 `s` 中小时桶 `label`（`YYYYMMDDHH`）的桶 manifest（按块还原）；该桶不在 `buckets` 中为 `None`。
    pub fn bucket_manifest(&self, s: &Value, label: &str) -> Option<Value> {
        s["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|b| b["bucket"] == label)
            .map(|b| self.restore(b["manifest"].as_str().unwrap()))
    }
}

/// 分三次（每次等提交后再隔 1.5 秒，超过 1 秒的 `flush.interval`）各向 stream `app` 摄取 10 条记录，`_timestamp`
/// 依次为 `ts` 起的 30 个微秒、`seq` 为 0～29：在 `ts` 所在小时桶写出 3 个数据文件。
pub fn three_ingests(s: &Server, ds: &Ds, ts: i64) {
    for i in 0..3u64 {
        let body: Vec<String> = (0..10u64)
            .map(|k| {
                let n = i * 10 + k;
                format!(r#"{{"_timestamp":{},"seq":{n}}}"#, ts + n as i64)
            })
            .collect();
        super::api::ingest_ok(s, "app", &format!("[{}]", body.join(",")));
        ds.wait_records((i + 1) * 10, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
}

/// 分 `n` 次向 stream `app` 摄取（每次 1 条、`seq` 自 `from` 起递增；等它提交后再隔 1.5 秒，超过 1 秒的
/// `flush.interval`）。返回最后的当前快照编号。
pub fn ingest_times(s: &Server, ds: &Ds, from: u64, n: u64) -> u64 {
    let base = ds.current().map_or(0, |id| {
        ds.snapshot(id)["summary"]["record_count"].as_u64().unwrap()
    });
    for i in 0..n {
        super::api::ingest_ok(s, "app", &format!(r#"[{{"seq":{}}}]"#, from + i));
        ds.wait_records(base + i + 1, Duration::from_secs(15));
        std::thread::sleep(Duration::from_millis(1500));
    }
    ds.current().unwrap()
}

/// 等快照 `ids` 都不存在（热层快照回收，至多 `limit`）。
pub fn wait_snapshots_gone(ds: &Ds, ids: impl IntoIterator<Item = u64> + Clone, limit: Duration) {
    let deadline = Instant::now() + limit;
    while ids
        .clone()
        .into_iter()
        .any(|id| ds.exists(&snapshot_rel(id)))
    {
        assert!(
            Instant::now() < deadline,
            "{} 秒内快照未被回收：{:?}",
            limit.as_secs(),
            ds.snapshot_ids()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// `us` 所在小时桶的 `YYYYMMDDHH`。
pub fn bucket_label(us: i64) -> String {
    amberlog_storage::layout::HourBucket::of(us).to_string()
}

/// 以同样的行、不压缩的方式重写 Parquet 文件（字节改变、内容不变）。
pub fn rewrite_parquet_uncompressed(path: &Path) {
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    let original = std::fs::read(path).unwrap();
    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    let schema = reader.schema().clone();
    let batches: Vec<_> = reader.build().unwrap().map(|b| b.unwrap()).collect();
    let mut out = Vec::new();
    let props = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .build();
    let mut w = ArrowWriter::try_new(&mut out, schema, Some(props)).unwrap();
    for b in &batches {
        w.write(b).unwrap();
    }
    w.close().unwrap();
    assert_ne!(out, original);
    std::fs::write(path, out).unwrap();
}

/// 大小为 `sizes` 的数据文件中有没有相邻可合并组（REQ-AMB-STO-SEAL-COMPACTION-V4）：小文件（小于 `target` 的一半）按
/// 大小升序排成一列，位置相连的若干个（不少于 2 个）之和不大于 `target`、其中最大的一个不大于其余之和的 2 倍。
pub fn has_mergeable_run(sizes: &[u64], target: u64) -> bool {
    let mut small: Vec<u64> = sizes.iter().copied().filter(|&n| n * 2 < target).collect();
    small.sort_unstable();
    (0..small.len()).any(|l| {
        let mut sum = small[l];
        small[l + 1..].iter().any(|&n| {
            let ok = sum + n <= target && n <= 2 * sum;
            sum += n;
            ok
        })
    })
}

/// 为 `stream` 在 `bucket_start` 所在小时写出 `files` 个各含 `per_file` 条带唯一 `seq` 记录的小文件后以 SIGTERM 停止
/// （`lifecycle.seal_grace` 为 `24h`，该桶在写出期间不封口；每批达到 `flush.max_records` 即单独 flush 成一个文件）。
pub fn seed_small_files(f: &Fixture, stream: &str, bucket_start: i64, files: u64, per_file: u64) {
    let per = per_file.to_string();
    let mut s = f.start(&[
        ("lifecycle.seal_grace", Some("\"24h\"")),
        ("flush.max_records", Some(per.as_str())),
        FLUSH_1S,
    ]);
    let ds = f.dataset(stream);
    for i in 0..files {
        let records: Vec<String> = (0..per_file)
            .map(|j| {
                let seq = i * per_file + j;
                format!(
                    r#"{{"_timestamp":{},"seq":{seq}}}"#,
                    bucket_start + seq as i64 * 1000
                )
            })
            .collect();
        super::api::ingest_ok(&s, stream, &format!("[{}]", records.join(",")));
        ds.wait_records((i + 1) * per_file, Duration::from_secs(15));
    }
    stop(&mut s);
}
