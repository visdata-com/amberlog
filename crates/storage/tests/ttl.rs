//! TTL 驱动（`amberlog_storage::ttl`）经真实的快照提交与段链读取跑通（`file://` 后端；冷层读写用替身）：
//! 热层 `ttl` 留痕的取值、冷层变更段的写出与补写、读取失败不停用数据集。REQ-AMB-STO-TTL 的场景由
//! `amberlog-server` 的系统测试验证，这里不对应块场景。

mod common;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_storage::chain::{
    ChainReader, ColdDeletionReason, ColdFileEntry, DeletionReason, Digest, HandoverDraft,
    SegmentReader, Snapshot, SnapshotId,
};
use amberlog_storage::cold::{
    ColdDataFile, ColdDeleted, ColdOperation, ColdReader, ColdSnapshot, ColdState, ColdTable,
    ColdWriter,
};
use amberlog_storage::commit::{Existence, HandoverRequest, HotCommits};
use amberlog_storage::committer::StoreCommitter;
use amberlog_storage::format;
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::layout::HourBucket;
use amberlog_storage::lifecycle;
use amberlog_storage::maintenance::LakeInputs;
use amberlog_storage::reader::StoreChain;
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::store::Store;
use amberlog_storage::testing::{FakeColdReader, FakeColdWriter, FakeWal};
use amberlog_storage::ttl::{CommitTally, QuietChain, Ttl, TtlConfig, TtlDeps};

use common::*;

const RETENTION_HOURS: i64 = 3;

/// 提交计数不在这里核对（由系统测试经 `/metrics` 核对）。
struct NoTally;

impl CommitTally for NoTally {
    fn ok(&self, _: &Dataset) {}
    fn failed(&self, _: &Dataset) {}
    fn unavailable(&self, _: &Dataset) {}
}

/// 一个进程：自有的登记表、闸门、缓存与提交者，TTL 用真实的快照提交与段链读取、替身冷层读写。
struct Proc {
    gates: Arc<DatasetGates>,
    committer: Arc<StoreCommitter>,
    cold_reader: Arc<FakeColdReader>,
    cold_writer: Arc<FakeColdWriter>,
    ttl: Ttl,
}

fn process(store: &Store) -> Proc {
    let registry = Arc::new(DatasetRegistry::new());
    let gates = Arc::new(DatasetGates::new());
    let chain = Arc::new(StoreChain::new(
        store.clone(),
        Arc::clone(&registry),
        Arc::clone(&gates),
    ));
    let committer = Arc::new(StoreCommitter::new(
        Arc::clone(&chain),
        Arc::new(FakeWal::new()),
        3,
    ));
    let cold_reader = Arc::new(FakeColdReader::new());
    let cold_writer = Arc::new(FakeColdWriter::new());
    let ttl = Ttl::new(
        TtlDeps {
            registry,
            gates: Arc::clone(&gates),
            chain: Arc::new(QuietChain(Arc::clone(&chain))) as Arc<dyn ChainReader>,
            segments: Arc::clone(&chain) as Arc<dyn SegmentReader>,
            commits: Arc::clone(&committer) as Arc<dyn HotCommits>,
            cold_reader: Arc::clone(&cold_reader) as Arc<dyn ColdReader>,
            cold_writer: Arc::clone(&cold_writer) as Arc<dyn ColdWriter>,
            lake_inputs: Arc::new(LakeInputs::new()),
            tally: Arc::new(NoTally),
        },
        TtlConfig {
            retention: Duration::from_secs(RETENTION_HOURS as u64 * 3600),
            check_interval: Duration::from_secs(10),
        },
    );
    Proc {
        gates,
        committer,
        cold_reader,
        cold_writer,
        ttl,
    }
}

impl Proc {
    async fn ingest(&self, ts: &[i64]) {
        self.committer
            .commit(&app(), &request(data_files(ts, 1)))
            .await
            .unwrap();
    }

    /// 跑一轮，返回这一轮前后的「此刻减保留期」，作为 `cutoff` 的上下界。
    async fn run(&self) -> (i64, i64) {
        let before = lifecycle::now_us() - RETENTION_HOURS * HOUR;
        self.ttl.run_once().await;
        (before, lifecycle::now_us() - RETENTION_HOURS * HOUR)
    }
}

async fn latest(store: &Store) -> Snapshot {
    snapshot(store, &app(), current(store, &app()).await.unwrap()).await
}

/// 此刻前第 `hours_ago` 小时那个桶的起点加一分钟。
fn hours_ago(now: i64, hours_ago: i64) -> i64 {
    HourBucket::of(now - hours_ago * HOUR).start_us() + 60_000_000
}

fn label(ts: i64) -> String {
    HourBucket::of(ts).to_string()
}

async fn bucket_manifest(
    store: &Store,
    s: &Snapshot,
    ts: i64,
) -> amberlog_storage::chain::BucketManifest {
    let r = s
        .buckets
        .iter()
        .find(|b| b.bucket == label(ts))
        .unwrap_or_else(|| panic!("桶 {} 应在快照中", label(ts)));
    let restored = amberlog_storage::chain::read_restored(store, &app(), r, s.format_version)
        .await
        .unwrap()
        .unwrap();
    (*restored.manifest).clone()
}

fn on_disk(b: &Backend, dataset: &Dataset, rel: &str) -> std::path::PathBuf {
    b.dir
        .as_ref()
        .unwrap()
        .0
        .join(b.store.object_path(dataset, rel).as_ref())
}

#[tokio::test]
async fn ttl_hot_commit_records_added_in_max_timestamp_and_cutoff() {
    let b = file_backend();
    let p = process(&b.store);
    assert_eq!(
        p.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    let now = lifecycle::now_us();
    let old = hours_ago(now, 6);
    p.ingest(&[old, old + 1]).await;
    p.ingest(&[now]).await;
    let before = latest(&b.store).await;
    let entry = bucket_manifest(&b.store, &before, old).await.files[0].clone();

    let (lo, hi) = p.run().await;
    let after = latest(&b.store).await;
    assert_eq!(after.snapshot_id.0, before.snapshot_id.0 + 1);
    let m = bucket_manifest(&b.store, &after, old).await;
    assert!(m.files.is_empty());
    let d = m
        .deletions
        .iter()
        .find(|d| d.path == entry.path)
        .expect("应有该文件的留痕");
    assert_eq!(d.reason, DeletionReason::Ttl);
    assert_eq!(d.content_digest, entry.content_digest);
    assert_eq!(d.added_in, Some(entry.added_in));
    assert_eq!(d.max_timestamp, Some(entry.max_timestamp));
    assert_eq!(d.removed_in, after.snapshot_id);
    let cutoff = d.cutoff.expect("ttl 留痕带 cutoff");
    assert!(lo <= cutoff && cutoff <= hi && cutoff > entry.max_timestamp);
    // 此刻的桶不受影响
    assert_eq!(bucket_manifest(&b.store, &after, now).await.files.len(), 1);

    // 再跑一轮：没有要移走的文件，不写提交
    p.run().await;
    assert_eq!(latest(&b.store).await.snapshot_id, after.snapshot_id);
}

#[tokio::test]
async fn ttl_read_failures_do_not_disable() {
    // 到期桶的桶 manifest 不存在：本次不删，也不停用
    let b = file_backend();
    let p = process(&b.store);
    assert_eq!(
        p.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    let now = lifecycle::now_us();
    let (a, gone) = (hours_ago(now, 6), hours_ago(now, 7));
    p.ingest(&[a, gone]).await;
    let s = latest(&b.store).await;
    let r = s.buckets.iter().find(|r| r.bucket == label(gone)).unwrap();
    std::fs::remove_file(on_disk(&b, &app(), &r.manifest)).unwrap();
    // 另一个进程（缓存里没有该桶 manifest）
    let q = process(&b.store);
    q.committer.load().await.unwrap();
    q.run().await;
    assert_eq!(latest(&b.store).await.snapshot_id, s.snapshot_id);
    assert_eq!(q.gates.state(&app()), DatasetState::Normal);

    // 与到期无关的桶（此刻后第 5 小时，未到期）的桶 manifest 不存在：照常删除，也不停用
    let b = file_backend();
    let p = process(&b.store);
    assert_eq!(
        p.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    let future = now + 5 * HOUR;
    p.ingest(&[a, future]).await;
    let s = latest(&b.store).await;
    let r = s
        .buckets
        .iter()
        .find(|r| r.bucket == label(future))
        .unwrap();
    std::fs::remove_file(on_disk(&b, &app(), &r.manifest)).unwrap();
    let q = process(&b.store);
    q.committer.load().await.unwrap();
    q.run().await;
    let after = latest(&b.store).await;
    assert_eq!(after.snapshot_id.0, s.snapshot_id.0 + 1);
    assert!(bucket_manifest(&b.store, &after, a).await.files.is_empty());
    assert_eq!(q.gates.state(&app()), DatasetState::Normal);
}

/// 已入湖的桶（此刻前第 9 小时）：入湖交接经真实提交写出，冷层表由替身给出。返回 (冷层文件, 交接记录所在快照)。
async fn lake(p: &Proc, store: &Store, now: i64) -> (ColdFileEntry, SnapshotId) {
    assert_eq!(
        p.committer.ensure_dataset(&app()).await,
        Existence::Committed
    );
    let ts = hours_ago(now, 9);
    p.ingest(&[ts, ts + 1]).await;
    let h = current(store, &app()).await.unwrap();
    let file = ColdFileEntry {
        path: "data/_timestamp_day=2026-10-13/c1.parquet".into(),
        min_timestamp: ts,
        max_timestamp: ts + 1,
        record_count: 2,
        size_bytes: 300,
        content_digest: Digest::of(b"c1"),
    };
    let committed = p
        .committer
        .commit_handover(
            &app(),
            HandoverRequest {
                bucket: HourBucket::of(ts),
                hot_snapshot_id: h,
                handover: HandoverDraft {
                    table_location: "file:///x/amberlog/_iceberg/default/app".into(),
                    metadata_version: 1,
                    iceberg_snapshot_id: 100,
                    files: vec![file.clone()],
                },
            },
        )
        .await
        .unwrap();
    (file, committed.snapshot_id)
}

fn cold_table(snapshots: Vec<ColdSnapshot>, files: Vec<ColdDataFile>) -> ColdState {
    ColdState::Table(Arc::new(ColdTable {
        location: "file:///x/amberlog/_iceberg/default/app".into(),
        metadata_version: 2,
        current_snapshot_id: snapshots.last().map(|s| s.snapshot_id),
        fields: Vec::new(),
        snapshots,
        files,
    }))
}

fn lake_snapshot(ts: i64, h: SnapshotId) -> ColdSnapshot {
    ColdSnapshot {
        snapshot_id: 100,
        parent_snapshot_id: None,
        timestamp_ms: 0,
        operation: ColdOperation::Lake {
            bucket: HourBucket::of(ts),
            hot_snapshot_id: h,
            file_digests: BTreeMap::new(),
        },
    }
}

fn data_file(f: &ColdFileEntry) -> ColdDataFile {
    ColdDataFile {
        path: f.path.clone(),
        added_snapshot_id: 100,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        day: 0,
    }
}

async fn cold_change(store: &Store, s: &Snapshot) -> amberlog_storage::chain::ColdChangeSegment {
    let r = s.cold_deletion_segment.as_ref().expect("应引用冷层变更段");
    let bytes = raw(store, &app(), &r.path).await.unwrap();
    let id = amberlog_storage::layout::parse_cold_change_segment_path(&r.path).unwrap();
    format::decode_cold_change_segment(&bytes, id).unwrap()
}

#[tokio::test]
async fn ttl_cold_change_segment_takes_digest_and_max_timestamp_from_handover() {
    let b = file_backend();
    let p = process(&b.store);
    let now = lifecycle::now_us();
    let (file, handed) = lake(&p, &b.store, now).await;
    assert_eq!(latest(&b.store).await.summary.cold.record_count, 2);
    let h = SnapshotId(handed.0 - 1);
    p.cold_reader.set_table(
        &app(),
        Ok(cold_table(
            vec![lake_snapshot(file.min_timestamp, h)],
            vec![data_file(&file)],
        )),
    );
    p.cold_writer.push_delete(Ok(ColdDeleted {
        metadata_version: 3,
        iceberg_snapshot_id: 101,
        removed: vec![file.path.clone()],
    }));

    let (lo, hi) = p.run().await;
    let s = latest(&b.store).await;
    assert_eq!(s.snapshot_id.0, handed.0 + 1, "只有冷层变更段提交");
    assert_eq!(s.summary.cold.record_count, 0);
    let segment = cold_change(&b.store, &s).await;
    assert_eq!(segment.deletions.len(), 1);
    let d = &segment.deletions[0];
    assert_eq!(d.path, file.path);
    assert_eq!(d.content_digest, file.content_digest);
    assert_eq!(d.max_timestamp, file.max_timestamp);
    assert_eq!(d.reason, ColdDeletionReason::Ttl);
    assert_eq!(d.cold_removed_in, 101);
    assert_eq!(d.removed_in, s.snapshot_id);
    let cutoff = d.cutoff.unwrap();
    assert!(lo <= cutoff && cutoff <= hi && cutoff > d.max_timestamp);

    // 冷层当前版本此后有该次 ttl 冷层提交：已记入，不再补写
    p.cold_reader.set_table(
        &app(),
        Ok(cold_table(
            vec![
                lake_snapshot(file.min_timestamp, h),
                ColdSnapshot {
                    snapshot_id: 101,
                    parent_snapshot_id: Some(100),
                    timestamp_ms: 0,
                    operation: ColdOperation::Ttl { cutoff },
                },
            ],
            Vec::new(),
        )),
    );
    p.run().await;
    assert_eq!(latest(&b.store).await.snapshot_id, s.snapshot_id);
}

#[tokio::test]
async fn ttl_recovers_unrecorded_cold_commit_with_its_cutoff() {
    let b = file_backend();
    let p = process(&b.store);
    let now = lifecycle::now_us();
    let (file, handed) = lake(&p, &b.store, now).await;
    let h = SnapshotId(handed.0 - 1);
    // 上一个进程在 TTL 冷层提交 101 之后、冷层变更段提交之前终止；新进程装载后补写
    let recorded_cutoff = file.max_timestamp + 7;
    let q = process(&b.store);
    q.committer.load().await.unwrap();
    q.cold_reader.set_table(
        &app(),
        Ok(cold_table(
            vec![
                lake_snapshot(file.min_timestamp, h),
                ColdSnapshot {
                    snapshot_id: 101,
                    parent_snapshot_id: Some(100),
                    timestamp_ms: 0,
                    operation: ColdOperation::Ttl {
                        cutoff: recorded_cutoff,
                    },
                },
            ],
            Vec::new(),
        )),
    );
    q.cold_reader
        .set_snapshot_files(&app(), 100, vec![data_file(&file)]);
    q.cold_reader.set_snapshot_files(&app(), 101, Vec::new());
    q.run().await;

    let s = latest(&b.store).await;
    assert_eq!(s.snapshot_id.0, handed.0 + 1);
    let segment = cold_change(&b.store, &s).await;
    assert_eq!(segment.deletions.len(), 1);
    let d = &segment.deletions[0];
    assert_eq!(d.path, file.path);
    assert_eq!(d.cold_removed_in, 101);
    assert_eq!(d.cutoff, Some(recorded_cutoff));
    // 补写不经冷层写入
    assert!(q.cold_writer.calls().is_empty());

    // 再跑：已记入，不重复补写
    q.run().await;
    assert_eq!(latest(&b.store).await.snapshot_id, s.snapshot_id);
}
