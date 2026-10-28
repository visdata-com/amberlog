use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use super::*;
use crate::chain::{
    BucketManifest, BucketRef, ColdChangeSegment, ColdDeletion, ColdSummary, Digest, FileEntry,
    SegmentId, Snapshot, SnapshotId, Summary,
};
use crate::cold::{ColdDataFile, ColdDeleted, ColdOperation, ColdSnapshot, ColdState, ColdTable};
use crate::committer::CommitError;
use crate::gate::{DatasetGates, DatasetState};
use crate::layout::HourBucket;
use crate::lifecycle;
use crate::maintenance::LakeInputs;
use crate::registry::{DatasetRegistry, Persistence};
use crate::testing::{
    ColdWriteCall, FakeChain, FakeColdReader, FakeColdWriter, FakeHotCommits, FakeSegments,
    HotCommitCall,
};

const HOUR: i64 = 3_600_000_000;

/// 记下计数：(ok, failed, unavailable)。
#[derive(Default)]
struct Tally(std::sync::Mutex<(u32, u32, u32)>);

impl Tally {
    fn get(&self) -> (u32, u32, u32) {
        *self.0.lock().unwrap()
    }
}

impl CommitTally for Tally {
    fn ok(&self, _: &Dataset) {
        self.0.lock().unwrap().0 += 1;
    }

    fn failed(&self, _: &Dataset) {
        self.0.lock().unwrap().1 += 1;
    }

    fn unavailable(&self, _: &Dataset) {
        self.0.lock().unwrap().2 += 1;
    }
}
const PERIOD_HOURS: i64 = 3;

struct Rig {
    dataset: Dataset,
    gates: Arc<DatasetGates>,
    chain: Arc<FakeChain>,
    segments: Arc<FakeSegments>,
    commits: Arc<FakeHotCommits>,
    cold_reader: Arc<FakeColdReader>,
    cold_writer: Arc<FakeColdWriter>,
    lake_inputs: Arc<LakeInputs>,
    tally: Arc<Tally>,
    ttl: Ttl,
    /// 准备数据时的此刻（UTC 微秒）。
    now: i64,
}

fn rig() -> Rig {
    let dataset = Dataset::new("default", "app");
    let registry = Arc::new(DatasetRegistry::new());
    registry.record(&dataset, Persistence::Stored);
    let gates = Arc::new(DatasetGates::new());
    let chain = Arc::new(FakeChain::new());
    let segments = Arc::new(FakeSegments::new());
    let commits = Arc::new(FakeHotCommits::new());
    let cold_reader = Arc::new(FakeColdReader::new());
    let cold_writer = Arc::new(FakeColdWriter::new());
    let lake_inputs = Arc::new(LakeInputs::new());
    let tally = Arc::new(Tally::default());
    let ttl = Ttl::new(
        TtlDeps {
            registry,
            gates: Arc::clone(&gates),
            chain: Arc::clone(&chain) as Arc<dyn ChainReader>,
            segments: Arc::clone(&segments) as Arc<dyn SegmentReader>,
            commits: Arc::clone(&commits) as Arc<dyn HotCommits>,
            cold_reader: Arc::clone(&cold_reader) as Arc<dyn ColdReader>,
            cold_writer: Arc::clone(&cold_writer) as Arc<dyn ColdWriter>,
            lake_inputs: Arc::clone(&lake_inputs),
            tally: Arc::clone(&tally) as Arc<dyn CommitTally>,
        },
        TtlConfig {
            retention: Duration::from_secs(PERIOD_HOURS as u64 * 3600),
            check_interval: Duration::from_secs(10),
        },
    );
    Rig {
        dataset,
        gates,
        chain,
        segments,
        commits,
        cold_reader,
        cold_writer,
        lake_inputs,
        tally,
        ttl,
        now: lifecycle::now_us(),
    }
}

/// 一个热层数据文件：`[min, max]` 为时间戳范围。
fn file(path: &str, min: i64, max: i64, added_in: u64) -> FileEntry {
    FileEntry {
        path: path.into(),
        min_timestamp: min,
        max_timestamp: max,
        record_count: 10,
        size_bytes: 100,
        content_digest: Digest::of(path.as_bytes()),
        added_in: SnapshotId(added_in),
        column_stats: None,
    }
}

fn cold_file(path: &str, max: i64, added_snapshot_id: i64) -> ColdDataFile {
    ColdDataFile {
        path: path.into(),
        added_snapshot_id,
        record_count: 10,
        size_bytes: 100,
        min_timestamp: max - HOUR / 2,
        max_timestamp: max,
        day: i32::try_from(max.div_euclid(24 * HOUR)).unwrap(),
    }
}

fn lake_snapshot(id: i64, bucket: HourBucket, h: u64) -> ColdSnapshot {
    ColdSnapshot {
        snapshot_id: id,
        parent_snapshot_id: None,
        timestamp_ms: 0,
        operation: ColdOperation::Lake {
            bucket,
            hot_snapshot_id: SnapshotId(h),
            file_digests: BTreeMap::new(),
        },
    }
}

fn table(snapshots: Vec<ColdSnapshot>, files: Vec<ColdDataFile>) -> ColdState {
    ColdState::Table(Arc::new(ColdTable {
        location: "file:///x/amberlog/_iceberg/default/app".into(),
        metadata_version: 3,
        current_snapshot_id: snapshots.last().map(|s| s.snapshot_id),
        fields: Vec::new(),
        snapshots,
        files,
    }))
}

impl Rig {
    /// 快照 7：各桶的桶 manifest 预置进替身（`unset` 中的桶不预置，读到即失败），`summary.cold.record_count` 为 `cold`。
    fn hot(&self, buckets: &[(HourBucket, Vec<FileEntry>)], unset: &[HourBucket], cold: u64) {
        let mut refs = Vec::new();
        for (hour, files) in buckets {
            let m = BucketManifest {
                format_version: 2,
                bucket: hour.to_string(),
                files: files.clone(),
                deletions: Vec::new(),
                handovers: Vec::new(),
            };
            let path = format!("manifest/buckets/{hour}-m.json");
            if !unset.contains(hour) {
                self.chain
                    .put_bucket_manifest(&self.dataset, &path, m.clone());
            }
            refs.push(BucketRef::of(
                &m,
                path,
                Digest::of(hour.to_string().as_bytes()),
            ));
        }
        let cold = ColdSummary {
            record_count: cold,
            ..ColdSummary::EMPTY
        };
        let snapshot = Snapshot {
            format_version: 2,
            snapshot_id: SnapshotId(7),
            parent_snapshot_id: Some(SnapshotId(6)),
            parent_digest: None,
            created_at: 0,
            schema: Vec::new(),
            summary: Summary::of(&self.dataset, &refs, cold),
            buckets: refs,
            retired_segment: None,
            cold_deletion_segment: None,
        };
        self.chain.put_snapshot(&self.dataset, snapshot);
        self.chain
            .set_current(&self.dataset, Ok(Some(SnapshotId(7))));
    }

    fn hour(&self, hours_ago: i64) -> HourBucket {
        HourBucket::of(self.now - hours_ago * HOUR)
    }

    /// 此刻前第 `hours_ago` 小时那个桶里的一个文件（整体落在该小时内）。
    fn old_file(&self, path: &str, hours_ago: i64, added_in: u64) -> FileEntry {
        let start = self.hour(hours_ago).start_us();
        file(path, start + 60_000_000, start + 120_000_000, added_in)
    }

    fn ttl_hot_calls(&self) -> Vec<TtlHotRequest> {
        self.commits
            .calls()
            .into_iter()
            .filter_map(|c| match c {
                HotCommitCall::TtlHot(_, r) => Some(r),
                _ => None,
            })
            .collect()
    }

    fn cold_change_calls(&self) -> Vec<ColdChangeRequest> {
        self.commits
            .calls()
            .into_iter()
            .filter_map(|c| match c {
                HotCommitCall::ColdChange(_, r) => Some(r),
                _ => None,
            })
            .collect()
    }

    fn deletes(&self) -> Vec<ColdDelete> {
        self.cold_writer
            .calls()
            .into_iter()
            .filter_map(|c| match c {
                ColdWriteCall::Delete(_, r) => Some(r),
                _ => None,
            })
            .collect()
    }

    /// 跑一轮，返回这一轮前后的「此刻减保留期」，作为 `cutoff` 的上下界。
    async fn run(&self) -> (i64, i64) {
        let before = lifecycle::now_us() - PERIOD_HOURS * HOUR;
        self.ttl.run_once().await;
        (before, lifecycle::now_us() - PERIOD_HOURS * HOUR)
    }
}

#[tokio::test]
async fn hot_files_expire_by_max_timestamp_in_one_commit() {
    let r = rig();
    let b6 = r.hour(6);
    let b7 = r.hour(7);
    let now_bucket = r.hour(0);
    // 跨 cutoff 的文件：最早的记录早于 cutoff、最晚的不早于
    let straddle = r.hour(PERIOD_HOURS);
    let s = straddle.start_us();
    r.hot(
        &[
            (b7, vec![r.old_file("data/b7a.parquet", 7, 2)]),
            (
                b6,
                vec![
                    r.old_file("data/b6a.parquet", 6, 3),
                    r.old_file("data/b6b.parquet", 6, 4),
                ],
            ),
            (
                straddle,
                vec![file("data/straddle.parquet", s - HOUR, s + HOUR - 1, 5)],
            ),
            (now_bucket, vec![file("data/now.parquet", r.now, r.now, 6)]),
        ],
        // 此刻的桶 time_min 不早于 cutoff：不读其桶 manifest（读到即失败、整个执行放弃）
        &[now_bucket],
        0,
    );
    let (lo, hi) = r.run().await;
    let calls = r.ttl_hot_calls();
    assert_eq!(calls.len(), 1, "{:?}", r.commits.calls());
    assert!(lo <= calls[0].cutoff && calls[0].cutoff <= hi);
    assert_eq!(
        calls[0].expired,
        vec![
            (b7, "data/b7a.parquet".to_owned()),
            (b6, "data/b6a.parquet".to_owned()),
            (b6, "data/b6b.parquet".to_owned()),
        ]
    );
    // 尚无冷层表：不做冷层提交
    assert!(r.deletes().is_empty());
    assert!(r.cold_change_calls().is_empty());
    assert_eq!(r.tally.get(), (1, 0, 0));
}

#[tokio::test]
async fn nothing_expired_writes_no_commit() {
    let r = rig();
    r.hot(
        &[(r.hour(1), vec![r.old_file("data/a.parquet", 1, 2)])],
        &[],
        0,
    );
    r.cold_reader.set_table(
        &r.dataset,
        Ok(table(
            vec![lake_snapshot(10, r.hour(2), 1)],
            vec![cold_file("data/c.parquet", r.now - 2 * HOUR, 10)],
        )),
    );
    r.run().await;
    assert!(r.commits.calls().is_empty());
    assert!(r.cold_writer.calls().is_empty());
}

#[tokio::test]
async fn incomplete_reads_remove_nothing() {
    // 到期桶的桶 manifest 读不到
    let r = rig();
    let b6 = r.hour(6);
    let b7 = r.hour(7);
    r.hot(
        &[
            (b6, vec![r.old_file("data/a.parquet", 6, 2)]),
            (b7, vec![r.old_file("data/b.parquet", 7, 3)]),
        ],
        &[b7],
        0,
    );
    r.run().await;
    assert!(r.commits.calls().is_empty());

    // 冷层表整体消失：热层记入过冷层文件，冷层读到「尚无冷层表」
    let r = rig();
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        10,
    );
    r.run().await;
    assert!(r.commits.calls().is_empty());

    // 冷层读取不可访问
    let r = rig();
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        0,
    );
    r.cold_reader.set_table(
        &r.dataset,
        Err(crate::cold::ColdReadError::Unavailable("403".into())),
    );
    r.run().await;
    assert!(r.commits.calls().is_empty());
    assert!(r.cold_writer.calls().is_empty());
    assert_eq!(r.gates.state(&r.dataset), DatasetState::Normal);
}

#[tokio::test]
async fn lake_inputs_and_handover_window_are_kept() {
    let r = rig();
    let b5 = r.hour(5);
    let b6 = r.hour(6);
    let b9 = r.hour(9);
    r.hot(
        &[
            // 进行中的入湖已确定的输入 a，另有不是入湖输入的 b
            (
                b6,
                vec![
                    r.old_file("data/a.parquet", 6, 3),
                    r.old_file("data/b.parquet", 6, 4),
                ],
            ),
            // 处于交接窗口的冷层提交 21（桶 b5，H = 5）的入湖输入 w
            (b5, vec![r.old_file("data/w.parquet", 5, 5)]),
        ],
        &[],
        10,
    );
    assert!(
        r.lake_inputs
            .begin(&r.dataset, ["data/a.parquet".to_owned()])
    );
    r.cold_reader.set_table(
        &r.dataset,
        Ok(table(
            vec![lake_snapshot(20, b9, 1), lake_snapshot(21, b5, 5)],
            vec![
                // 交接已写的旧冷层文件，已到期
                cold_file("data/old.parquet", r.now - 9 * HOUR, 20),
                // 交接窗口中的冷层提交新增的文件，已到期
                cold_file("data/new.parquet", r.now - 5 * HOUR, 21),
            ],
        )),
    );
    r.cold_writer.push_delete(Ok(ColdDeleted {
        metadata_version: 4,
        iceberg_snapshot_id: 22,
        removed: vec!["data/old.parquet".into()],
    }));
    let (lo, hi) = r.run().await;

    let hot = r.ttl_hot_calls();
    assert_eq!(hot.len(), 1);
    assert_eq!(hot[0].expired, vec![(b6, "data/b.parquet".to_owned())]);
    let deletes = r.deletes();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].paths, vec!["data/old.parquet".to_owned()]);
    assert_eq!(deletes[0].cutoff, hot[0].cutoff);
    let changes = r.cold_change_calls();
    assert_eq!(
        changes,
        vec![ColdChangeRequest {
            cold_removed_in: 22,
            reason: ColdDeletionReason::Ttl,
            cutoff: Some(hot[0].cutoff),
            paths: vec!["data/old.parquet".into()],
            additions: Vec::new(),
        }]
    );
    assert!(lo <= hot[0].cutoff && hot[0].cutoff <= hi);
    // 依次：TTL 热层提交、冷层变更段提交（冷层删除在两者之间，见上）
    assert!(matches!(
        r.commits.calls().as_slice(),
        [HotCommitCall::TtlHot(..), HotCommitCall::ColdChange(..)]
    ));
    // 守卫已释放：入湖可以开始
    assert!(
        r.lake_inputs
            .begin(&r.dataset, ["data/x.parquet".to_owned()])
    );
}

#[tokio::test]
async fn cold_files_expire_by_upper_bound_and_failures_stop_later_commits() {
    let r = rig();
    let b9 = r.hour(9);
    r.hot(&[], &[], 20);
    let cutoff_now = r.now - PERIOD_HOURS * HOUR;
    let state = table(
        vec![lake_snapshot(20, b9, 1)],
        vec![
            cold_file("data/expired.parquet", cutoff_now - HOUR, 20),
            // 上界不早于 cutoff（下界早于）：不到期
            cold_file("data/straddle.parquet", cutoff_now + HOUR, 20),
        ],
    );
    r.cold_reader.set_table(&r.dataset, Ok(state));
    // 冷层删除提交不可访问：不做冷层变更段提交
    r.cold_writer
        .push_delete(Err(crate::cold::ColdWriteError::Unavailable("503".into())));
    r.run().await;
    assert!(r.ttl_hot_calls().is_empty());
    let deletes = r.deletes();
    assert_eq!(deletes.len(), 1);
    assert_eq!(deletes[0].paths, vec!["data/expired.parquet".to_owned()]);
    assert!(r.cold_change_calls().is_empty());

    // TTL 热层提交未成功：不做冷层删除
    let r = rig();
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        20,
    );
    r.cold_reader.set_table(
        &r.dataset,
        Ok(table(
            vec![lake_snapshot(20, b9, 1)],
            vec![cold_file("data/expired.parquet", cutoff_now - HOUR, 20)],
        )),
    );
    r.commits.fail_next(CommitError::Unavailable);
    r.run().await;
    assert_eq!(r.ttl_hot_calls().len(), 1);
    assert!(r.cold_writer.calls().is_empty());
    // 一轮因对象存储不可访问而未完成：就地计一次失败
    assert_eq!(r.tally.get(), (0, 0, 1));
}

#[tokio::test]
async fn unrecorded_ttl_cold_commit_is_written_before_removing() {
    let r = rig();
    let b9 = r.hour(9);
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        10,
    );
    let ttl_snapshot = ColdSnapshot {
        snapshot_id: 31,
        parent_snapshot_id: Some(30),
        timestamp_ms: 0,
        operation: ColdOperation::Ttl { cutoff: -5 },
    };
    // 链上已记入的只有更早的 ttl 冷层提交 29
    let recorded = ColdSnapshot {
        snapshot_id: 29,
        parent_snapshot_id: Some(20),
        timestamp_ms: 0,
        operation: ColdOperation::Ttl { cutoff: -9 },
    };
    r.cold_reader.set_table(
        &r.dataset,
        Ok(table(
            vec![
                lake_snapshot(20, b9, 1),
                recorded,
                lake_snapshot(30, b9, 1),
                ttl_snapshot,
            ],
            vec![cold_file("data/y.parquet", r.now, 30)],
        )),
    );
    r.segments.set_cold_changes(
        &r.dataset,
        Ok(vec![ColdChangeSegment {
            format_version: 2,
            segment_id: SegmentId(1),
            previous: None,
            created_at: 0,
            deletions: vec![ColdDeletion {
                path: "data/gone.parquet".into(),
                content_digest: Digest::of(b"gone"),
                max_timestamp: -10,
                reason: ColdDeletionReason::Ttl,
                cutoff: Some(-9),
                deleted_at: 0,
                removed_in: SnapshotId(5),
                cold_removed_in: 29,
            }],
            additions: Vec::new(),
        }]),
    );
    r.cold_reader.set_snapshot_files(
        &r.dataset,
        30,
        vec![
            cold_file("data/x.parquet", r.now - 9 * HOUR, 30),
            cold_file("data/w.parquet", r.now - 9 * HOUR, 30),
            cold_file("data/y.parquet", r.now, 30),
        ],
    );
    r.cold_reader
        .set_snapshot_files(&r.dataset, 31, vec![cold_file("data/y.parquet", r.now, 30)]);
    r.run().await;
    let calls = r.commits.calls();
    assert_eq!(calls.len(), 2, "{calls:?}");
    assert_eq!(
        calls[0],
        HotCommitCall::ColdChange(
            r.dataset.clone(),
            ColdChangeRequest {
                cold_removed_in: 31,
                reason: ColdDeletionReason::Ttl,
                cutoff: Some(-5),
                paths: vec!["data/w.parquet".into(), "data/x.parquet".into()],
                additions: Vec::new(),
            }
        )
    );
    assert!(matches!(calls[1], HotCommitCall::TtlHot(..)));

    // 补写未成功：本次不移走任何文件
    let r2 = rig();
    r2.hot(
        &[(r2.hour(6), vec![r2.old_file("data/a.parquet", 6, 2)])],
        &[],
        10,
    );
    r2.cold_reader.set_table(
        &r2.dataset,
        Ok(table(
            vec![
                lake_snapshot(30, b9, 1),
                ColdSnapshot {
                    snapshot_id: 31,
                    parent_snapshot_id: Some(30),
                    timestamp_ms: 0,
                    operation: ColdOperation::Ttl { cutoff: -5 },
                },
            ],
            vec![cold_file("data/old.parquet", r2.now - 9 * HOUR, 30)],
        )),
    );
    r2.cold_reader.set_snapshot_files(
        &r2.dataset,
        30,
        vec![
            cold_file("data/x.parquet", r2.now - 9 * HOUR, 30),
            cold_file("data/old.parquet", r2.now - 9 * HOUR, 30),
        ],
    );
    r2.cold_reader.set_snapshot_files(
        &r2.dataset,
        31,
        vec![cold_file("data/old.parquet", r2.now - 9 * HOUR, 30)],
    );
    r2.commits.fail_next(CommitError::Exhausted);
    r2.run().await;
    assert_eq!(r2.tally.get(), (0, 1, 0));
    assert!(matches!(
        r2.commits.calls().as_slice(),
        [HotCommitCall::ColdChange(..)]
    ));
    assert!(r2.cold_writer.calls().is_empty());
}

#[tokio::test]
async fn disabled_or_stopping_does_not_run() {
    let r = rig();
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        0,
    );
    r.gates.enter(&r.dataset, DatasetState::ManifestUnreadable);
    r.run().await;
    assert_eq!(r.chain.reads(), 0);
    assert!(r.commits.calls().is_empty());

    let r = rig();
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        0,
    );
    BucketTask::stop(&r.ttl);
    r.run().await;
    assert_eq!(r.chain.reads(), 0);
    assert!(r.commits.calls().is_empty());
}

#[test]
fn first_round_starts_at_once() {
    let r = rig();
    assert_eq!(r.ttl.first_delay(), Duration::ZERO);
    assert_eq!(r.ttl.period(), Duration::from_secs(10));
}

#[tokio::test]
async fn hot_commit_rechecks_under_the_guard() {
    let r = rig();
    let b6 = r.hour(6);
    r.hot(
        &[(
            b6,
            vec![
                r.old_file("data/a.parquet", 6, 2),
                r.old_file("data/b.parquet", 6, 3),
            ],
        )],
        &[],
        0,
    );
    let cutoff = r.now - PERIOD_HOURS * HOUR;
    let judged = vec![
        (b6, "data/a.parquet".to_owned()),
        (b6, "data/b.parquet".to_owned()),
    ];
    // 判定之后：a 已被入湖移出，新出现的到期文件 c 留给下一次执行
    r.hot(
        &[(
            b6,
            vec![
                r.old_file("data/b.parquet", 6, 3),
                r.old_file("data/c.parquet", 6, 8),
            ],
        )],
        &[],
        0,
    );
    assert!(r.ttl.commit_hot(&r.dataset, cutoff, judged.clone()).await);
    let calls = r.ttl_hot_calls();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].expired, vec![(b6, "data/b.parquet".to_owned())]);
    // 守卫已释放
    assert!(
        r.lake_inputs
            .begin(&r.dataset, ["data/b.parquet".to_owned()])
    );
    // 进行中入湖的输入不移出；没有要移出的就不写
    assert!(r.ttl.commit_hot(&r.dataset, cutoff, judged).await);
    assert_eq!(r.ttl_hot_calls().len(), 1);
}

#[tokio::test]
async fn stale_hot_commit_still_goes_on_to_cold() {
    let r = rig();
    let b9 = r.hour(9);
    r.hot(
        &[(r.hour(6), vec![r.old_file("data/a.parquet", 6, 2)])],
        &[],
        20,
    );
    r.cold_reader.set_table(
        &r.dataset,
        Ok(table(
            vec![lake_snapshot(20, b9, 1)],
            vec![cold_file("data/c.parquet", r.now - 9 * HOUR, 20)],
        )),
    );
    // 按父快照重核后没有要移出的热层文件：快照提交不写，算作略过
    r.commits.fail_next(CommitError::Stale);
    r.cold_writer.push_delete(Ok(ColdDeleted {
        metadata_version: 4,
        iceberg_snapshot_id: 22,
        removed: vec!["data/c.parquet".into()],
    }));
    r.run().await;
    assert_eq!(r.ttl_hot_calls().len(), 1);
    assert_eq!(r.deletes().len(), 1);
    assert_eq!(r.cold_change_calls().len(), 1);
    // Stale 不计；冷层变更段提交成功计 ok
    assert_eq!(r.tally.get(), (1, 0, 0));
}
