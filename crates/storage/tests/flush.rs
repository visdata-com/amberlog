//! 物化的触发与编排在存储层的核对：可见集（真实实现）→ 冻结 → Parquet → 提交 → 回执 → 段回收，以及四个需求块
//! （命名空间、热层布局、快照提交、审计不变量）里依赖 flush 的行为在存储层的对应核对。
//!
//! 不对应块场景：块场景经 `_json`、`_search` 端到端验收，在 amberlog-server 的系统测试里。
//! 对象存储用 `file://` 临时目录；WAL 用 FakeWal（只收提交意图与段回收通知）。

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::{Duration, Instant};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::{FieldValue, Record};
use amberlog_storage::audit::verify_data_files;
use amberlog_storage::chain::{Digest, SnapshotId};
use amberlog_storage::commit::{Committer as _, Existence};
use amberlog_storage::committer::CommitError;
use amberlog_storage::flusher::{
    CommitObserver, CommitTotals, FlushConfig, FlushFailure, FlushOutcome, Flusher,
};
use amberlog_storage::gate::DatasetState;
use amberlog_storage::memtable::{Memtable, MemtableView, MemtableWriter};
use amberlog_storage::wal::{Release, WalPosition};
use arrow::array::{Array, Int64Array, TimestampMicrosecondArray};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use common::*;

struct Rig {
    backend: Backend,
    c: Committer,
    memtable: Arc<Memtable>,
    flusher: Arc<Flusher>,
    next: u64,
}

fn rig(interval: Duration, max_records: u64) -> Rig {
    rig_with(interval, max_records, 8)
}

fn rig_with(interval: Duration, max_records: u64, retries: u32) -> Rig {
    rig_on(file_backend(), interval, max_records, retries, None)
}

fn rig_on(
    backend: Backend,
    interval: Duration,
    max_records: u64,
    retries: u32,
    observer: Option<CommitObserver>,
) -> Rig {
    let c = committer(&backend.store, 8);
    let memtable = Arc::new(Memtable::new());
    let store_committer = Arc::new(amberlog_storage::committer::StoreCommitter::new(
        Arc::clone(&c.chain),
        c.wal.clone(),
        retries,
    ));
    let mut flusher = Flusher::new(
        store_committer,
        Arc::clone(&memtable),
        FlushConfig {
            interval,
            max_records,
        },
    );
    if let Some(observer) = observer {
        flusher = flusher.with_commit_observer(observer);
    }
    Rig {
        backend,
        c,
        memtable,
        flusher: Arc::new(flusher),
        next: 1,
    }
}

fn rec(ts: i64, fields: &[(&str, FieldValue)]) -> Record {
    Record {
        timestamp_us: ts,
        fields: fields
            .iter()
            .map(|(k, v)| ((*k).to_owned(), v.clone()))
            .collect(),
    }
}

impl Rig {
    /// 模拟摄取：确保存在、登记进目录、写入（WAL 位置由这里递增）。
    async fn ingest(&mut self, d: &Dataset, records: Vec<Record>) -> WalPosition {
        assert_eq!(self.flusher.ensure_dataset(d).await, Existence::Committed);
        self.flusher
            .register(d, amberlog_storage::schema::StreamColumns::genesis());
        let pos = WalPosition(self.next);
        self.next += 1;
        self.flusher.write(d, pos, records);
        pos
    }

    fn store(&self) -> &amberlog_storage::store::Store {
        &self.backend.store
    }

    fn file(&self, d: &Dataset, rel: &str) -> std::path::PathBuf {
        self.backend
            .dir
            .as_ref()
            .unwrap()
            .0
            .join("amberlog")
            .join(&d.org)
            .join(&d.stream)
            .join(rel)
    }
}

fn ints(n: i64) -> Vec<Record> {
    (0..n)
        .map(|i| rec(H10 + i, &[("n", FieldValue::Int64(i))]))
        .collect()
}

/// flush 一次：冻结 → 物化 → 提交 → 回执 → 段回收；无新记录时不写新快照。
#[tokio::test]
async fn flush_commits_retires_and_releases() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = app();
    let pos = r.ingest(&d, ints(3)).await;
    assert_eq!(r.memtable.candidates(&d).participating(None).len(), 1);

    let out = r.flusher.flush_dataset(&d).await;
    assert_eq!(out, FlushOutcome::Committed(SnapshotId(2)));
    assert_eq!(current(r.store(), &d).await, Some(SnapshotId(2)));
    let s = snapshot(r.store(), &d, SnapshotId(2)).await;
    assert_eq!(s.summary.record_count, 3);
    assert!(
        r.memtable.candidates(&d).participating(None).is_empty(),
        "回执后从可见集移除"
    );
    assert!(r.c.wal.releases().contains(&Release::Records {
        dataset: d.clone(),
        through: pos
    }));
    // 没有新记录：不写新快照
    let before = std::fs::read_dir(r.file(&d, "manifest")).unwrap().count();
    assert_eq!(r.flusher.flush_dataset(&d).await, FlushOutcome::Idle);
    assert_eq!(
        std::fs::read_dir(r.file(&d, "manifest")).unwrap().count(),
        before
    );
    assert_eq!(
        r.flusher.commit_totals()[&d],
        CommitTotals { ok: 2, failed: 0 },
        "创世快照与一次数据提交"
    );
}

/// 一次 flush 跨小时按小时桶拆分，文件内 `_timestamp` 落在路径所示小时内。
#[tokio::test]
async fn flush_splits_by_hour() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = Dataset::new("default", "hb");
    r.ingest(
        &d,
        vec![rec(H10 + HOUR - 1_000_000, &[]), rec(H10 + HOUR, &[])],
    )
    .await;
    r.flusher.flush_dataset(&d).await;
    for (dir, hour) in [
        ("data/2026/08/22/10", H10),
        ("data/2026/08/22/11", H10 + HOUR),
    ] {
        let files: Vec<_> = std::fs::read_dir(r.file(&d, dir))
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect();
        assert_eq!(files.len(), 1, "{dir}");
        let bytes = bytes::Bytes::from(std::fs::read(&files[0]).unwrap());
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .unwrap()
            .build()
            .unwrap();
        for b in reader {
            let b = b.unwrap();
            let ts = b
                .column_by_name("_timestamp")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .clone();
            assert!(ts.values().iter().all(|t| (hour..hour + HOUR).contains(t)));
        }
    }
}

/// 字段 id 按首次进入 schema 的顺序分配、与引用新列的文件同快照提交；文件列的 field id 与快照一致；列可空。
#[tokio::test]
async fn flush_field_ids_follow_schema() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = Dataset::new("default", "fid");
    let mut first_b = None;
    for fields in [
        vec![("a", FieldValue::Int64(1))],
        vec![("b", FieldValue::Utf8("x".into()))],
        vec![
            ("c", FieldValue::Boolean(true)),
            ("a", FieldValue::Int64(2)),
        ],
    ] {
        r.ingest(&d, vec![rec(H10, &fields)]).await;
        let FlushOutcome::Committed(p) = r.flusher.flush_dataset(&d).await else {
            panic!("应提交");
        };
        let s = snapshot(r.store(), &d, p).await;
        if first_b.is_none() && s.schema.iter().any(|c| c.name == "b") {
            first_b = Some(p);
        }
    }
    let top = current(r.store(), &d).await.unwrap();
    let s = snapshot(r.store(), &d, top).await;
    let ids: Vec<(String, u32)> = s
        .schema
        .iter()
        .map(|c| (c.name.clone(), c.field_id))
        .collect();
    assert_eq!(
        ids,
        [("_timestamp", 1), ("a", 2), ("b", 3), ("c", 4)].map(|(n, i)| (n.to_owned(), i))
    );
    let first_b = first_b.unwrap();
    let ms = manifests(r.store(), &d, &s).await;
    let entry = ms[0].files.iter().find(|f| f.added_in == first_b).unwrap();
    let bytes = bytes::Bytes::from(std::fs::read(r.file(&d, &entry.path)).unwrap());
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let fields = reader.parquet_schema().root_schema().get_fields().to_vec();
    let got: Vec<(String, i32)> = fields
        .iter()
        .map(|f| (f.name().to_owned(), f.get_basic_info().id()))
        .collect();
    assert_eq!(got, [("_timestamp".to_owned(), 1), ("b".to_owned(), 3)]);
    assert!(fields[1].is_optional());
}

/// 同一记录引入的多个新列按出现顺序分配字段 id；创世快照只含 `_timestamp`。
#[tokio::test]
async fn flush_field_ids_within_record_and_genesis() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = Dataset::new("default", "app6");
    r.ingest(
        &d,
        vec![rec(
            H10,
            &[
                ("host", FieldValue::Utf8("a".into())),
                ("level", FieldValue::Utf8("info".into())),
                ("msg", FieldValue::Utf8("x".into())),
            ],
        )],
    )
    .await;
    let genesis = snapshot(r.store(), &d, SnapshotId(1)).await;
    assert_eq!(genesis.schema.len(), 1);
    assert_eq!(genesis.parent_snapshot_id, None);
    assert!(genesis.buckets.is_empty());
    r.flusher.flush_dataset(&d).await;
    let s = snapshot(r.store(), &d, current(r.store(), &d).await.unwrap()).await;
    let ids: Vec<u32> = s.schema.iter().skip(1).map(|c| c.field_id).collect();
    assert_eq!(ids, [2, 3, 4]);
}

/// flush.max_records 达到即 flush（周期设为 1 小时，只有记录数能触发）。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_triggered_by_max_records() {
    let mut r = rig(Duration::from_secs(3600), 5);
    let d = app();
    let task = r.flusher.spawn();
    r.ingest(&d, ints(3)).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        current(r.store(), &d).await,
        Some(SnapshotId(1)),
        "未达上限不 flush"
    );
    r.ingest(&d, ints(2)).await;
    assert!(
        wait_until(Duration::from_secs(5), || async {
            current(r.store(), &d).await == Some(SnapshotId(2))
        })
        .await
    );
    assert_eq!(
        snapshot(r.store(), &d, SnapshotId(2))
            .await
            .summary
            .record_count,
        5
    );
    task.abort();
}

/// 按 flush.interval 周期 flush，并补提创世快照。
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_on_interval_and_settles_genesis() {
    let mut r = rig(Duration::from_millis(200), 1_000_000);
    let d = app();
    r.ingest(&d, ints(2)).await;
    // WAL 写不进时新数据集的创世快照不写出；恢复后由周期任务补提
    let late = Dataset::new("default", "late");
    r.c.wal.set_failing(true);
    assert_eq!(r.flusher.ensure_dataset(&late).await, Existence::Pending);
    r.c.wal.set_failing(false);
    let task = r.flusher.spawn();
    // 只是等待上限，满足即返回：5 秒在执行器负载高时等不到，放宽到 30 秒
    assert!(
        wait_until(Duration::from_secs(30), || async {
            current(r.store(), &d).await == Some(SnapshotId(2))
                && current(r.store(), &late).await == Some(SnapshotId(1))
        })
        .await
    );
    task.abort();
}

/// 提交失败时冻结代保留、下一次 flush 连同新冻结的代重提，每条记录恰好入链一次。
#[tokio::test]
async fn flush_failure_keeps_generations_and_retries_once() {
    let mut r = rig_with(Duration::from_secs(3600), 1_000_000, 2);
    let d = app();
    r.ingest(&d, ints(3)).await;
    let root = r.backend.dir.as_ref().unwrap().0.clone();
    let moved = root.with_extension("away");
    std::fs::rename(&root, &moved).unwrap();
    let out = r.flusher.flush_dataset(&d).await;
    assert_eq!(
        out,
        FlushOutcome::Failed(FlushFailure::Commit(CommitError::Unavailable))
    );
    assert_eq!(
        r.c.gates.state(&d),
        DatasetState::Normal,
        "对象存储不可访问不停用"
    );
    assert_eq!(
        r.memtable.candidates(&d).participating(None).len(),
        1,
        "冻结代仍在可见集"
    );
    std::fs::rename(&moved, &root).unwrap();
    r.ingest(&d, ints(2)).await;
    loop {
        match r.flusher.flush_dataset(&d).await {
            FlushOutcome::Idle => break,
            FlushOutcome::Committed(_) => {}
            other => panic!("{other:?}"),
        }
    }
    let s = snapshot(r.store(), &d, current(r.store(), &d).await.unwrap()).await;
    assert_eq!(s.summary.record_count, 5);
    assert!(r.memtable.candidates(&d).participating(None).is_empty());
    assert_eq!(r.flusher.commit_totals()[&d].failed, 1);
}

/// 停用的数据集不冻结、不上传；有未提交记录时记失败。其他数据集照常。
#[tokio::test]
async fn flush_skips_disabled_dataset() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = app();
    let web = Dataset::new("default", "web");
    r.ingest(&d, ints(1)).await;
    r.ingest(&web, ints(1)).await;
    r.c.gates.enter(&d, DatasetState::IncompatibleFormat);
    assert_eq!(
        r.flusher.flush_dataset(&d).await,
        FlushOutcome::Failed(FlushFailure::Commit(CommitError::Disabled(
            DatasetState::IncompatibleFormat
        )))
    );
    assert!(!r.file(&d, "data").exists(), "停用后不再上传数据文件");
    assert_eq!(
        r.memtable.candidates(&d).participating(None).len(),
        1,
        "记录保留"
    );
    assert!(matches!(
        r.flusher.flush_dataset(&web).await,
        FlushOutcome::Committed(_)
    ));
}

/// 装载不计提交（提示缺失而链上只有 1 号快照是合法状态，不前滚也不补写）；同一周期内的多次失败只计一次。
#[tokio::test]
async fn flush_observer_counts_failures_once_per_period() {
    let first = rig(Duration::from_secs(3600), 1_000_000);
    let d = app();
    assert_eq!(first.flusher.ensure_dataset(&d).await, Existence::Committed);
    std::fs::remove_file(first.file(&d, "manifest/_current")).unwrap();
    let Rig { backend, .. } = first;

    let seen: Arc<std::sync::Mutex<Vec<(Dataset, bool)>>> = Arc::default();
    let sink = Arc::clone(&seen);
    let mut r = rig_on(
        backend,
        Duration::from_secs(3600),
        1_000_000,
        0,
        Some(Arc::new(move |d: &Dataset, ok| {
            sink.lock().unwrap().push((d.clone(), ok))
        })),
    );
    r.flusher.load().await.unwrap();
    assert_eq!(current(r.store(), &d).await, Some(SnapshotId(1)));
    assert!(seen.lock().unwrap().is_empty());

    r.ingest(&d, ints(1)).await;
    let root = r.backend.dir.as_ref().unwrap().0.clone();
    let moved = root.with_extension("away");
    std::fs::rename(&root, &moved).unwrap();
    for _ in 0..3 {
        assert!(matches!(
            r.flusher.flush_dataset(&d).await,
            FlushOutcome::Failed(_)
        ));
    }
    std::fs::rename(&moved, &root).unwrap();
    assert_eq!(*seen.lock().unwrap(), [(d.clone(), false)]);
    assert_eq!(
        r.flusher.commit_totals()[&d],
        CommitTotals { ok: 0, failed: 1 }
    );
}

/// 停机的 flush_all：每个数据集都提交完，停用的除外。
#[tokio::test]
async fn flush_all_commits_every_dataset() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let names = ["a1", "a2", "a3"];
    for n in names {
        r.ingest(&Dataset::new("default", n), ints(2)).await;
    }
    r.flusher.flush_all().await;
    for n in names {
        let d = Dataset::new("default", n);
        assert_eq!(current(r.store(), &d).await, Some(SnapshotId(2)), "{n}");
    }
}

/// 摘要在写入时固定、同一文件条目在各版桶 manifest 中不变、deletions 为空、快照单链、`_current` 只前进、
/// 已提交对象不被覆盖或删除、离线查验无误报——多次 flush 跨多个小时桶。
#[tokio::test]
async fn flush_audit_invariants_over_many_flushes() {
    let mut r = rig(Duration::from_secs(3600), 1_000_000);
    let d = Dataset::new("default", "nd");
    let mut seen_keys: BTreeSet<std::path::PathBuf> = BTreeSet::new();
    let mut hashes = std::collections::HashMap::new();
    let mut last = SnapshotId(0);
    for round in 0..12i64 {
        let hour = H10 + (round % 4) * HOUR;
        r.ingest(
            &d,
            vec![rec(hour + round, &[("n", FieldValue::Int64(round))])],
        )
        .await;
        r.flusher.flush_dataset(&d).await;
        let now = current(r.store(), &d).await.unwrap();
        assert!(now > last, "_current 只前进");
        last = now;
        for path in walk(&r.file(&d, "")) {
            if path.ends_with("_current") {
                continue;
            }
            let digest = Digest::of(&std::fs::read(&path).unwrap());
            if let Some(prev) = hashes.insert(path.clone(), digest) {
                assert_eq!(prev, digest, "{} 被改写", path.display());
            }
            seen_keys.insert(path);
        }
    }
    for k in &seen_keys {
        assert!(k.exists(), "{} 被删除", k.display());
    }
    let mut id = last;
    let mut entries = std::collections::HashMap::new();
    loop {
        let s = snapshot(r.store(), &d, id).await;
        for m in manifests(r.store(), &d, &s).await {
            assert!(m.deletions.is_empty());
            for f in m.files {
                let bytes = std::fs::read(r.file(&d, &f.path)).unwrap();
                assert_eq!(f.content_digest, Digest::of(&bytes));
                assert_eq!(f.size_bytes, bytes.len() as u64);
                let prev = entries.insert(f.path.clone(), (f.content_digest, f.added_in));
                if let Some(prev) = prev {
                    assert_eq!(prev, (f.content_digest, f.added_in));
                }
            }
        }
        match s.parent_snapshot_id {
            None => break,
            Some(p) => {
                assert_eq!(p.0, id.0 - 1);
                id = p;
            }
        }
    }
    assert_eq!(id, SnapshotId(1));
    assert_eq!(entries.len(), 12);
    assert!(!r.file(&d, "_system").exists());
    assert_eq!(verify_data_files(r.store(), &d).await, Ok(Vec::new()));
    // 快照 summary 与各级合计自洽
    let s = snapshot(r.store(), &d, last).await;
    assert_eq!(s.summary.record_count, 12);
    assert_eq!(s.buckets.len(), 4);
    let col = |b: &arrow::record_batch::RecordBatch| {
        b.column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .len()
    };
    let mut rows = 0;
    for m in manifests(r.store(), &d, &s).await {
        for f in m.files {
            let bytes = bytes::Bytes::from(std::fs::read(r.file(&d, &f.path)).unwrap());
            for b in ParquetRecordBatchReaderBuilder::try_new(bytes)
                .unwrap()
                .build()
                .unwrap()
            {
                rows += col(&b.unwrap());
            }
        }
    }
    assert_eq!(rows, 12);
}

fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
    let mut out = Vec::new();
    for e in std::fs::read_dir(dir).unwrap() {
        let p = e.unwrap().path();
        if p.is_dir() {
            out.extend(walk(&p));
        } else {
            out.push(p);
        }
    }
    out
}

async fn wait_until<F, Fut>(limit: Duration, mut f: F) -> bool
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    let end = Instant::now() + limit;
    while Instant::now() < end {
        if f().await {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}
