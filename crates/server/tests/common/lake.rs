//! 入湖一侧的检查：冷层表目录（`<store>/amberlog/_iceberg/default/<stream>/`）下的元数据、清单列表与清单、冷层数据文件，
//! 以及热层桶 manifest 的交接记录（REQ-AMB-STO-LAKE-ENTRY、REQ-AMB-STO-AUDIT-INVARIANTS-V3 等的系统测试共用）。
//!
//! 元数据按 JSON 直接读；清单列表与清单是 Avro，用 iceberg-rust 的解析（按 Iceberg 规范独立读，不经被测实现）。

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use arrow::record_batch::RecordBatch;
use iceberg::spec::{FormatVersion, Manifest, ManifestList, ManifestStatus};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::Value;

use super::api::{ingest_ok, now_us, poll};
use super::store::{Ds, HOUR, bucket_label, hour_of, stop};
use super::{Fixture, Kv, Server};

/// 一张冷层表的目录。
pub struct ColdDir {
    pub dir: PathBuf,
}

/// 冷层快照清单中的一个条目。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// 相对冷层表位置。
    pub path: String,
    /// 清单条目所记的快照号（`snapshot_id`，未写时承自清单的 `added_snapshot_id`）。
    pub snapshot_id: i64,
    pub added: bool,
    pub deleted: bool,
    pub record_count: u64,
    pub file_size_in_bytes: u64,
}

impl Fixture {
    /// stream `stream` 的冷层表目录。
    pub fn cold(&self, stream: &str) -> ColdDir {
        ColdDir {
            dir: self
                .store_dir()
                .join("amberlog")
                .join("_iceberg")
                .join("default")
                .join(stream),
        }
    }

    /// 冷层前缀 `<store>/amberlog/_iceberg/` 之下的全部对象（相对该前缀）。
    pub fn iceberg_keys(&self) -> Vec<String> {
        keys(&self.store_dir().join("amberlog").join("_iceberg"))
    }
}

/// 目录 `base` 之下全部对象的相对键（排序；跳过写入中的暂存文件）。
pub fn keys(base: &Path) -> Vec<String> {
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
                out.push(
                    e.path()
                        .strip_prefix(base)
                        .unwrap()
                        .to_string_lossy()
                        .into_owned(),
                );
            }
        }
    }
    let mut out = Vec::new();
    walk(base, base, &mut out);
    out.sort();
    out
}

impl ColdDir {
    pub fn path(&self, rel: &str) -> PathBuf {
        self.dir.join(rel)
    }

    pub fn exists(&self, rel: &str) -> bool {
        self.path(rel).exists()
    }

    pub fn read(&self, rel: &str) -> Vec<u8> {
        std::fs::read(self.path(rel)).unwrap_or_else(|e| panic!("读冷层 {rel}：{e}"))
    }

    /// 表目录下全部对象。
    pub fn keys(&self) -> Vec<String> {
        keys(&self.dir)
    }

    /// 冷层当前版本：自 1 起逐个确认 `metadata/v<N>.metadata.json` 存在，取存在的最大 N；尚无冷层表为 `None`。
    pub fn version(&self) -> Option<u64> {
        let mut n = 0;
        while self.exists(&format!("metadata/v{}.metadata.json", n + 1)) {
            n += 1;
        }
        (n > 0).then_some(n)
    }

    pub fn metadata(&self, n: u64) -> Value {
        serde_json::from_slice(&self.read(&format!("metadata/v{n}.metadata.json"))).unwrap()
    }

    /// 当前版本的元数据。
    pub fn current(&self) -> Value {
        self.metadata(self.version().expect("已有冷层表"))
    }

    /// 元数据所列的快照。
    pub fn snapshots(meta: &Value) -> Vec<Value> {
        meta["snapshots"].as_array().cloned().unwrap_or_default()
    }

    /// 元数据中摘要 `amberlog.operation` 为 `lake`、`amberlog.bucket` 为 `bucket` 的快照（按 `sequence-number` 升序）。
    pub fn lake_snapshots(meta: &Value, bucket: &str) -> Vec<Value> {
        let mut out: Vec<Value> = Self::snapshots(meta)
            .into_iter()
            .filter(|s| s["summary"]["amberlog.operation"] == "lake")
            .filter(|s| s["summary"]["amberlog.bucket"] == bucket)
            .collect();
        out.sort_by_key(|s| s["sequence-number"].as_i64());
        out
    }

    /// 元数据的 `location` 之下的 URI 换成相对表位置的路径。
    pub fn rel(meta: &Value, uri: &str) -> String {
        let location = meta["location"].as_str().unwrap();
        uri.strip_prefix(location)
            .and_then(|r| r.strip_prefix('/'))
            .unwrap_or_else(|| panic!("{uri} 不在表位置 {location} 之下"))
            .to_owned()
    }

    /// 快照 `snapshot`（元数据 `meta` 所列）的清单中的全部条目。
    pub fn entries(&self, meta: &Value, snapshot: &Value) -> Vec<Entry> {
        let list_rel = Self::rel(meta, snapshot["manifest-list"].as_str().unwrap());
        let list = ManifestList::parse_with_version(&self.read(&list_rel), FormatVersion::V2)
            .expect("清单列表可解析");
        let mut out = Vec::new();
        for f in list.entries() {
            let m = Manifest::parse_avro(&self.read(&Self::rel(meta, &f.manifest_path)))
                .expect("清单可解析");
            for e in m.entries() {
                out.push(Entry {
                    path: Self::rel(meta, e.file_path()),
                    snapshot_id: e.snapshot_id().unwrap_or(f.added_snapshot_id),
                    added: e.status() == ManifestStatus::Added,
                    deleted: e.status() == ManifestStatus::Deleted,
                    record_count: e.record_count(),
                    file_size_in_bytes: e.file_size_in_bytes(),
                });
            }
        }
        out
    }

    /// 快照 `snapshot` 的清单中状态为「已加入」、且由它加入的条目（该快照新增的冷层数据文件）。
    pub fn added(&self, meta: &Value, snapshot: &Value) -> Vec<Entry> {
        let id = snapshot["snapshot-id"].as_i64().unwrap();
        self.entries(meta, snapshot)
            .into_iter()
            .filter(|e| e.added && e.snapshot_id == id)
            .collect()
    }

    /// 读一个冷层数据文件（相对表位置）的全部记录。
    pub fn batches(&self, rel: &str) -> Vec<RecordBatch> {
        let f = std::fs::File::open(self.path(rel)).unwrap();
        ParquetRecordBatchReaderBuilder::try_new(f)
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }
}

impl Ds {
    /// 当前快照中桶 `label` 的桶 manifest；该桶不在 `buckets` 中为 `None`。
    pub fn bucket_now(&self, label: &str) -> Option<Value> {
        let head = self.head();
        self.bucket_manifest(&head, label)
    }

    /// 等当前快照中桶 `label` 的桶 manifest 的 `handovers` 不少于 `k` 条（至多 `limit`），返回该桶 manifest。
    /// 桶已移入留痕归档段（不在 `buckets` 中）时不算。
    pub fn wait_handovers(&self, label: &str, k: usize, limit: Duration) -> Value {
        let deadline = Instant::now() + limit;
        loop {
            if self.current().is_some()
                && let Some(m) = self.bucket_now(label)
                && m["handovers"].as_array().map_or(0, Vec::len) >= k
            {
                return m;
            }
            assert!(
                Instant::now() < deadline,
                "{} 秒内桶 {label} 未见第 {k} 条交接记录：{:?}",
                limit.as_secs(),
                self.bucket_now(label)
            );
            std::thread::sleep(Duration::from_millis(200));
        }
    }

    /// 快照 `id` 中桶 `label` 的桶 manifest。
    pub fn bucket_at(&self, id: u64, label: &str) -> Option<Value> {
        let s = self.snapshot(id);
        self.bucket_manifest(&s, label)
    }
}

/// 把 Parquet 文件替换为内容不同的合法 Parquet 文件：schema（含字段编号）不变，`seq` 列每个值加 1000000。
pub fn alter_parquet_rows(path: &Path) {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int64Array};
    use parquet::arrow::ArrowWriter;

    let reader =
        ParquetRecordBatchReaderBuilder::try_new(std::fs::File::open(path).unwrap()).unwrap();
    let schema = reader.schema().clone();
    let batches: Vec<RecordBatch> = reader.build().unwrap().map(|b| b.unwrap()).collect();
    let seq = schema.index_of("seq").expect("文件含 seq 列");
    let mut out = Vec::new();
    let mut w = ArrowWriter::try_new(&mut out, schema.clone(), None).unwrap();
    for b in &batches {
        let old = b
            .column(seq)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("seq 为 Int64");
        let new: Int64Array = old.iter().map(|v| v.map(|x| x + 1_000_000)).collect();
        let mut columns: Vec<ArrayRef> = b.columns().to_vec();
        columns[seq] = Arc::new(new);
        w.write(&RecordBatch::try_new(schema.clone(), columns).unwrap())
            .unwrap();
    }
    w.close().unwrap();
    std::fs::write(path, out).unwrap();
}

/// 记录批次中各行的 (`_timestamp`, `seq`)（`seq` 为 NULL 记作 `None`），按列名取。
pub fn ts_seq(batches: &[RecordBatch]) -> Vec<(i64, Option<i64>)> {
    use arrow::array::{Array, Int64Array, TimestampMicrosecondArray};
    let mut out = Vec::new();
    for b in batches {
        let ts = b
            .column_by_name("_timestamp")
            .expect("_timestamp 列")
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .expect("_timestamp 为微秒时间戳");
        let seq = b
            .column_by_name("seq")
            .and_then(|c| c.as_any().downcast_ref::<Int64Array>().cloned());
        for i in 0..b.num_rows() {
            let s = seq
                .as_ref()
                .and_then(|c| (!c.is_null(i)).then(|| c.value(i)));
            out.push((ts.value(i), s));
        }
    }
    out
}

/// 留痕归档场景重启时的时钟偏移（秒）：已入湖的桶变空满 1 小时才归档，以偏移 2 小时重启即越过。
pub const RETIRE_OFFSET_S: i64 = 7200;

/// 一个已入湖、又已移入留痕归档段的桶。
pub struct Retired {
    /// 桶起点（此刻前第 3 小时）。
    pub bucket: i64,
    pub label: String,
    /// 入湖之前最后一个快照中该桶的 `files` 条目。
    pub inputs: Vec<Value>,
    /// 该桶最后一份桶 manifest（含 `lake` 留痕、被归档段引用的那份）的路径。
    pub manifest: String,
    /// 归档后当前快照的 `retired_segment`。
    pub segment: Value,
}

impl Fixture {
    /// 以时钟偏移 `offset_s` 秒（`TESTBED_CLOCK_OFFSET`）与额外的环境变量启动。
    pub fn start_offset(&self, overrides: &[Kv], offset_s: i64, envs: &[(&str, String)]) -> Server {
        let mut cmd = self.serve(overrides);
        cmd.env("TESTBED_CLOCK_OFFSET", offset_s.to_string());
        for (k, v) in envs {
            cmd.env(k, v);
        }
        Server::start(cmd)
    }

    /// 以 `overrides`（须使此刻前第 3 小时的桶 B 越过入湖水位，如 `lifecycle.lake_watermark` 为 `1h`）启动，向 `app`
    /// 的 B 写 `n` 条记录（`{"_timestamp":B+1+i*1000,"seq":i}`）并等到交接完成（至多 150 秒），以 SIGTERM 停止；再以
    /// [`RETIRE_OFFSET_S`] 的时钟偏移、同一配置重启，等到当前快照的 `buckets` 不含 B（至多 150 秒，归档每 60 秒一轮）。
    /// 返回运行中的实例。
    pub fn retire_bucket(&self, overrides: &[Kv], n: u64) -> (Server, Retired) {
        let b = hour_of(now_us()) - 3 * HOUR;
        let label = bucket_label(b);
        let mut s = self.start(overrides);
        let recs: Vec<String> = (0..n)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i as i64 * 1000))
            .collect();
        ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
        let ds = self.dataset("app");
        let m = ds.wait_handovers(&label, 1, Duration::from_secs(150));
        let id = m["handovers"][0]["snapshot_id"].as_u64().unwrap();
        let inputs = ds.bucket_at(id - 1, &label).expect("入湖前快照中有 B")["files"]
            .as_array()
            .unwrap()
            .clone();
        stop(&mut s);
        let head = ds.head();
        let manifest = head["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|r| r["bucket"] == label.as_str())
            .unwrap()["manifest"]
            .as_str()
            .unwrap()
            .to_owned();

        let s = self.start_offset(overrides, RETIRE_OFFSET_S, &[]);
        let (_, ok) = poll(
            Duration::from_secs(150),
            || ds.bucket_now(&label).is_none(),
            |gone| *gone,
        );
        assert!(ok, "150 秒内 B 未移入留痕归档段：{:#?}", s.stderr_lines());
        let segment = ds.head()["retired_segment"].clone();
        assert!(segment.is_object(), "{}", ds.head());
        (
            s,
            Retired {
                bucket: b,
                label,
                inputs,
                manifest,
                segment,
            },
        )
    }
}

impl Ds {
    /// 当前快照的完整删除台账：`buckets` 所引用桶 manifest 的 `deletions`，并上沿 `retired_segment` 与 `previous` 可达的
    /// 各留痕归档段所引用桶 manifest 的 `deletions`，并上沿 `cold_deletion_segment` 与 `previous` 可达的各冷层变更段的
    /// `deletions`。
    pub fn ledger(&self) -> Vec<Value> {
        let head = self.head();
        let mut out: Vec<Value> = self
            .bucket_manifests(&head)
            .into_iter()
            .flat_map(|m| m["deletions"].as_array().unwrap().clone())
            .collect();
        for seg in self.segments(&head["retired_segment"]) {
            for b in seg["buckets"].as_array().unwrap() {
                let m = self.restore(b["manifest"].as_str().unwrap());
                out.extend(m["deletions"].as_array().unwrap().iter().cloned());
            }
        }
        for seg in self.segments(&head["cold_deletion_segment"]) {
            out.extend(seg["deletions"].as_array().unwrap().iter().cloned());
        }
        out
    }

    /// 自段引用 `r`（`{path, content_digest}` 或 null）沿 `previous` 可达的各段。
    pub fn segments(&self, r: &Value) -> Vec<Value> {
        let mut out = Vec::new();
        let mut r = r.clone();
        while let Some(path) = r["path"].as_str() {
            let seg = self.json(path);
            r = seg["previous"].clone();
            out.push(seg);
        }
        out
    }
}

/// 冷层文件到期场景的配置：`flush.interval` 为 `1s`、`retention.period` 为 `6h`、`retention.check_interval` 为 `10s`、
/// `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `2s`。
pub const COLD_TTL: [Kv<'static>; 5] = [
    ("flush.interval", Some("\"1s\"")),
    ("retention.period", Some("\"6h\"")),
    ("retention.check_interval", Some("\"10s\"")),
    ("lifecycle.seal_grace", Some("\"1s\"")),
    ("lifecycle.lake_watermark", Some("\"2s\"")),
];

/// [`Fixture::lake_expiring`] 之后重启时的时钟偏移（秒）：记录随之早于保留期；被测进程的时钟只前进 20 分钟，入湖后
/// 变空的桶未满 1 小时、不会被归档（归档与冷层变更段共用暂停点 `commit.after_segment_write`）。
pub const COLD_TTL_OFFSET_S: i64 = 1200;

impl Fixture {
    /// 以 [`COLD_TTL`] 启动，向 `app` 写 10 条 `_timestamp` 自此刻前 5 小时 50 分起、每隔 1 毫秒的记录（`seq` 为 0～9），
    /// 等其所在桶的交接完成（至多 150 秒）后以 SIGTERM 停止。返回交接记录所记的冷层文件（相对冷层表位置的路径 → 元素）。
    /// 此后以 [`COLD_TTL_OFFSET_S`] 的时钟偏移重启，这些冷层文件内的记录即已全部早于保留期。
    pub fn lake_expiring(&self) -> std::collections::BTreeMap<String, Value> {
        let start = now_us() - 5 * HOUR - 50 * 60_000_000;
        let mut s = self.start(&COLD_TTL);
        let recs: Vec<String> = (0..10)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, start + i * 1000))
            .collect();
        ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
        let m =
            self.dataset("app")
                .wait_handovers(&bucket_label(start), 1, Duration::from_secs(150));
        stop(&mut s);
        m["handovers"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|h| h["files"].as_array().unwrap().iter())
            .map(|x| (x["path"].as_str().unwrap().to_owned(), x.clone()))
            .collect()
    }
}
