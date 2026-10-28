//! 存储集成测试共用的台架：两类对象存储后端、提交者、数据文件与对快照链的原样读取。
//!
//! `s3://` 后端用 `tools/testbed.sh <标签> up` 起的 MinIO/Silo，经 `TESTBED_S3_*` 取地址与凭据；每个测试取一个新的
//! `storage.prefix`，互不干扰。未起台架时这些测试失败而不是跳过（跳过会被当作通过）。

#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_storage::bloom::BloomColumns;
use amberlog_storage::chain::{BucketManifest, Snapshot, SnapshotId};
use amberlog_storage::committer::{CommitRequest, StoreCommitter};
use amberlog_storage::flush::{DataFile, write_data_files};
use amberlog_storage::format::{self, SnapshotContent};
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::layout;
use amberlog_storage::reader::StoreChain;
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::schema::{SchemaColumn, StreamColumns};
use amberlog_storage::store::{Location, S3Options, Store};
use amberlog_storage::testing::FakeWal;
use amberlog_storage::wal::WalPosition;
use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use object_store::ObjectStoreExt;

/// 2026-08-22T10:00:00Z
pub const H10: i64 = 1_787_392_800_000_000;
pub const HOUR: i64 = 3_600_000_000;

/// 一个后端：名字、打开的对象存储与（`file://` 时）其目录。
pub struct Backend {
    pub name: &'static str,
    pub store: Store,
    pub dir: Option<TempDir>,
}

pub struct TempDir(pub PathBuf);

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn unique(tag: &str) -> String {
    static N: AtomicU32 = AtomicU32::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    format!(
        "{tag}-{}-{}-{nanos}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    )
}

pub fn file_backend() -> Backend {
    let dir = std::env::temp_dir().join(unique("amberlog-storage-it"));
    std::fs::create_dir_all(&dir).unwrap();
    let store = Store::open(
        &Location::File { dir: dir.clone() },
        &S3Options::default(),
        "amberlog",
    )
    .unwrap();
    Backend {
        name: "file",
        store,
        dir: Some(TempDir(dir)),
    }
}

pub fn s3_backend() -> Backend {
    let var = |k: &str| {
        std::env::var(k).unwrap_or_else(|_| {
            panic!(
                "{k} 未设置：这些测试要用 MinIO/Silo 台架，先 eval \"$(tools/testbed.sh <标签> up)\""
            )
        })
    };
    let store = Store::open(
        &Location::S3 {
            bucket: var("TESTBED_S3_BUCKET"),
            path: Vec::new(),
        },
        &S3Options {
            endpoint: Some(var("TESTBED_S3_ENDPOINT")),
            region: "us-east-1".into(),
            access_key_id: Some(var("TESTBED_S3_ACCESS_KEY_ID")),
            secret_access_key: Some(var("TESTBED_S3_SECRET_ACCESS_KEY")),
            force_path_style: true,
            ..S3Options::default()
        },
        &unique("it"),
    )
    .unwrap();
    Backend {
        name: "s3",
        store,
        dir: None,
    }
}

/// `file://` 与 `s3://`（MinIO/Silo）各一个。
pub fn backends() -> Vec<Backend> {
    vec![file_backend(), s3_backend()]
}

pub fn app() -> Dataset {
    Dataset::new("default", "app")
}

/// 同一对象存储上的一个提交者（模拟一个进程：自有的登记表、闸门、缓存与 WAL）。
pub struct Committer {
    pub committer: StoreCommitter,
    pub wal: Arc<FakeWal>,
    pub chain: Arc<StoreChain>,
    pub gates: Arc<DatasetGates>,
    pub registry: Arc<DatasetRegistry>,
}

pub fn committer(store: &Store, max_retries: u32) -> Committer {
    let registry = Arc::new(DatasetRegistry::new());
    let gates = Arc::new(DatasetGates::new());
    let chain = Arc::new(StoreChain::new(
        store.clone(),
        Arc::clone(&registry),
        Arc::clone(&gates),
    ));
    let wal = Arc::new(FakeWal::new());
    let committer = StoreCommitter::new(Arc::clone(&chain), wal.clone(), max_retries);
    Committer {
        committer,
        wal,
        chain,
        gates,
        registry,
    }
}

/// 列 `_timestamp`（字段 id 1）与 `n`（Int64，字段 id 2）。
pub fn columns() -> StreamColumns {
    StreamColumns::from_columns(vec![
        SchemaColumn {
            name: TIMESTAMP_COLUMN.into(),
            column_type: ColumnType::Int64,
            field_id: 1,
        },
        SchemaColumn {
            name: "n".into(),
            column_type: ColumnType::Int64,
            field_id: 2,
        },
    ])
}

/// 一个批次：各行 `_timestamp` 取 `ts`，`n` 取 `value`。
pub fn batch(ts: &[i64], value: i64) -> RecordBatch {
    let cols = columns();
    let schema = Schema::new(vec![
        cols.get(TIMESTAMP_COLUMN).unwrap().arrow_field(),
        cols.get("n").unwrap().arrow_field(),
    ]);
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(TimestampMicrosecondArray::from(ts.to_vec()).with_timezone("UTC")),
        Arc::new(Int64Array::from(vec![value; ts.len()])),
    ];
    RecordBatch::try_new(Arc::new(schema), arrays).unwrap()
}

/// 由若干行（`_timestamp`）写出的数据文件，每个小时桶一个。
pub fn data_files(ts: &[i64], value: i64) -> Vec<DataFile> {
    write_data_files(&columns(), &[batch(ts, value)], &BloomColumns::none()).unwrap()
}

pub fn request(files: Vec<DataFile>) -> CommitRequest {
    CommitRequest {
        files,
        through: WalPosition(1),
        through_partial: None,
        bind: None,
    }
}

// ---------------------------------------------------------------------------
// 对快照链的原样读取（不经被测实现的缓存）
// ---------------------------------------------------------------------------

pub async fn raw(store: &Store, dataset: &Dataset, rel: &str) -> Option<bytes::Bytes> {
    let path = store.object_path(dataset, rel);
    match store.inner.get(&path).await {
        Ok(r) => Some(r.bytes().await.unwrap()),
        Err(object_store::Error::NotFound { .. }) => None,
        Err(e) => panic!("read {path}: {e}"),
    }
}

pub async fn exists(store: &Store, dataset: &Dataset, rel: &str) -> bool {
    let path = store.object_path(dataset, rel);
    match store.inner.head(&path).await {
        Ok(_) => true,
        Err(object_store::Error::NotFound { .. }) => false,
        Err(e) => panic!("head {path}: {e}"),
    }
}

/// 当前快照：读提示后按编号逐个探测，直到某编号不存在（找链尾，REQ-AMB-STO-MANIFEST-COMMIT-V3）。
pub async fn current(store: &Store, dataset: &Dataset) -> Option<SnapshotId> {
    let mut tail = hint(store, dataset).await;
    loop {
        let next = SnapshotId(tail.map_or(1, |t| t.0 + 1));
        if raw(store, dataset, &layout::snapshot_path(next))
            .await
            .is_none()
        {
            return tail;
        }
        tail = Some(next);
    }
}

/// 提示 `_current` 所记的编号；不存在为 `None`。
pub async fn hint(store: &Store, dataset: &Dataset) -> Option<SnapshotId> {
    raw(store, dataset, layout::CURRENT)
        .await
        .map(|b| format::decode_current(&b).unwrap())
}

pub async fn snapshot(store: &Store, dataset: &Dataset, id: SnapshotId) -> Snapshot {
    let bytes = raw(store, dataset, &layout::snapshot_path(id))
        .await
        .unwrap_or_else(|| panic!("快照 {id} 应存在"));
    match format::decode_snapshot(&bytes, dataset, id).unwrap() {
        SnapshotContent::Supported(s) => *s,
        other => panic!("快照 {id} 格式版本过高：{other:?}"),
    }
}

/// 快照引用的全部桶 manifest（按 `buckets` 顺序），逐片核对摘要后按 REQ-AMB-STO-HOT-LAYOUT-V8 还原。
pub async fn manifests(store: &Store, dataset: &Dataset, s: &Snapshot) -> Vec<BucketManifest> {
    let mut out = Vec::new();
    for b in &s.buckets {
        let r = amberlog_storage::chain::read_restored(store, dataset, b, s.format_version)
            .await
            .unwrap()
            .unwrap_or_else(|e| panic!("桶 manifest {} 应可还原：{e}", b.manifest));
        out.push((*r.manifest).clone());
    }
    out
}

/// 快照经 `buckets` 可达的全部数据文件路径。
pub async fn files(store: &Store, dataset: &Dataset, s: &Snapshot) -> Vec<String> {
    manifests(store, dataset, s)
        .await
        .into_iter()
        .flat_map(|m| m.files.into_iter().map(|f| f.path))
        .collect()
}
