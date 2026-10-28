//! 桶生命周期中几项周期工作在存储层的核对：延后物理删除（[`Purge`]）、留痕归档（[`Archiver`]）与巡检对留痕归档段和
//! 已留痕文件的处置（[`HotFsck`]）。入湖交接提交直接经 [`HotCommits`] 发出（冷层一侧以假的交接记录代替）。
//!
//! 不对应块场景：块场景要用真实入湖与可控时钟，经 `amberlog` 二进制在 amberlog-server 的系统测试里验收
//! （REQ-AMB-STO-AUDIT-INVARIANTS-V3、REQ-AMB-STO-HOT-LAYOUT-V4、REQ-AMB-STO-HOT-FSCK-V2）。

mod common;

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_storage::archive::Archiver;
use amberlog_storage::chain::{
    ChainReader, ColdDeletionReason, ColdFileEntry, ColdSummary, Digest, HandoverDraft,
    SegmentReader,
};
use amberlog_storage::cold;
use amberlog_storage::commit::{ColdChangeRequest, HandoverRequest, HotCommits};
use amberlog_storage::committer::StoreCommitter;
use amberlog_storage::flusher::{FlushConfig, Flusher};
use amberlog_storage::format;
use amberlog_storage::gate::DatasetState;
use amberlog_storage::hot_fsck::{FsckConfig, FsckEvents, HotFsck};
use amberlog_storage::layout::{self, HourBucket};
use amberlog_storage::maintenance::BucketTask;
use amberlog_storage::memtable::Memtable;
use amberlog_storage::purge::Purge;
use amberlog_storage::snapshot_gc::RetentionConfig;
use amberlog_storage::store::Store;

use common::*;

/// 一个进程：提交者、flush 一侧（归档与巡检经它记提交结果）与快照链。
struct Proc {
    c: Committer,
    sc: Arc<StoreCommitter>,
    flusher: Arc<Flusher>,
}

fn proc(store: &Store) -> Proc {
    let c = committer(store, 8);
    let sc = Arc::new(StoreCommitter::new(Arc::clone(&c.chain), c.wal.clone(), 8));
    let flusher = Arc::new(Flusher::new(
        Arc::clone(&sc),
        Arc::new(Memtable::new()),
        FlushConfig {
            interval: Duration::from_secs(3600),
            max_records: 1000,
        },
    ));
    Proc { c, sc, flusher }
}

/// 向小时桶 `ts` 提交一个数据文件，再以入湖交接提交使它离开存活集（记 `lake` 留痕）。返回该数据文件的路径。
async fn lake_one(p: &Proc, d: &Dataset, ts: i64) -> String {
    lake_one_cold(p, d, ts).await.0
}

/// 同 [`lake_one`]，另返回交接记录 `files` 中那个（假的）冷层数据文件。
async fn lake_one_cold(p: &Proc, d: &Dataset, ts: i64) -> (String, ColdFileEntry) {
    let files = data_files(&[ts], 1);
    let path = files[0].path.clone();
    let id = p.sc.commit(d, &request(files)).await.unwrap();
    let day = format!("data/_timestamp_day={}", layout::HourBucket::of(ts));
    let cold_file = ColdFileEntry {
        path: format!("{day}/{}.parquet", layout::new_object_id()),
        min_timestamp: ts,
        max_timestamp: ts,
        record_count: 1,
        size_bytes: 10,
        content_digest: Digest::of(b"cold"),
    };
    p.sc.commit_handover(
        d,
        HandoverRequest {
            bucket: HourBucket::of(ts),
            hot_snapshot_id: id,
            handover: HandoverDraft {
                table_location: "file:///cold".into(),
                metadata_version: 1,
                iceberg_snapshot_id: 1,
                files: vec![cold_file.clone()],
            },
        },
    )
    .await
    .unwrap();
    (path, cold_file)
}

fn exists(b: &Backend, d: &Dataset, rel: &str) -> bool {
    b.dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog")
        .join(&d.org)
        .join(&d.stream)
        .join(rel)
        .exists()
}

#[derive(Default)]
struct Deleted(AtomicUsize);

impl FsckEvents for Deleted {
    fn deleted(&self, _: &Dataset, data_file: bool) {
        if data_file {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
}

fn fsck(p: &Proc, delete_delay: Duration, events: Arc<Deleted>) -> HotFsck {
    HotFsck::new(
        Arc::clone(&p.flusher),
        FsckConfig {
            interval: Duration::from_secs(3600),
            min_orphan_age: Duration::ZERO,
            delete_delay,
            retention: RetentionConfig {
                snapshots: 1000,
                age: Duration::from_secs(168 * 3600),
            },
        },
        events,
    )
}

#[tokio::test]
async fn purge_waits_for_delay_and_skips_disabled_dataset() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let f = lake_one(&p, &d, H10 + 1).await;
    assert!(exists(&b, &d, &f));

    Purge::new(Arc::clone(&p.c.chain), Duration::from_secs(3600))
        .run_once()
        .await;
    assert!(exists(&b, &d, &f), "delete_delay 未满就删了");

    p.c.gates.enter(&d, DatasetState::ManifestUnreadable);
    Purge::new(Arc::clone(&p.c.chain), Duration::ZERO)
        .run_once()
        .await;
    assert!(exists(&b, &d, &f), "停用的数据集不得物理删除");
}

#[tokio::test]
async fn purge_deletes_due_lake_tombstoned_file() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let f = lake_one(&p, &d, H10 + 1).await;
    let purge = Purge::new(Arc::clone(&p.c.chain), Duration::ZERO);
    purge.run_once().await;
    assert!(!exists(&b, &d, &f), "已到删除时刻的入湖输入未删");
    // 再一轮不出错（已处理的不再删）
    purge.run_once().await;
}

#[tokio::test]
async fn archive_once_per_day_and_purge_sweeps_retired_bucket_after_restart() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let f10 = lake_one(&p, &d, H10 + 1).await;
    let archiver = Archiver::new(Arc::clone(&p.flusher), Duration::ZERO);
    archiver.run_once().await;
    let head = p.c.chain.latest_snapshot(&d).unwrap();
    assert!(head.retired_segment.is_some(), "空桶未归档");
    assert!(head.buckets.iter().all(|x| x.bucket != "2026082210"));
    let segment = head.retired_segment.clone().unwrap();
    let raw_segment = raw(&b.store, &d, &segment.path).await.unwrap();
    let decoded = format::decode_retired_segment(
        &raw_segment,
        layout::parse_retired_segment_path(&segment.path).unwrap(),
    )
    .unwrap();
    assert_eq!(decoded.buckets.len(), 1);
    assert_eq!(decoded.buckets[0].bucket, "2026082210");
    assert_eq!(decoded.created_at, head.created_at);

    // 同一 UTC 日不再写第二段：另一个变空的桶留在 `buckets` 中
    lake_one(&p, &d, H10 + HOUR + 1).await;
    archiver.run_once().await;
    let head = p.c.chain.latest_snapshot(&d).unwrap();
    assert_eq!(head.retired_segment, Some(segment));
    assert!(head.buckets.iter().any(|x| x.bucket == "2026082211"));

    // 「重启」：新进程装载后第一轮列一次 `data/`，到归档段所引用的桶 manifest 里找到 F 的留痕并补删
    assert!(exists(&b, &d, &f10));
    let p2 = proc(&b.store);
    p2.sc.load().await.unwrap();
    Purge::new(Arc::clone(&p2.c.chain), Duration::ZERO)
        .run_once()
        .await;
    assert!(!exists(&b, &d, &f10), "归档桶中已留痕的文件重启后未补删");
}

#[tokio::test]
async fn fsck_redeletes_lake_file_after_delay_only() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let f = lake_one(&p, &d, H10 + 1).await;

    let events = Arc::new(Deleted::default());
    fsck(&p, Duration::from_secs(3600), Arc::clone(&events))
        .run_once()
        .await;
    assert!(exists(&b, &d, &f), "delete_delay 未满就补删了");

    fsck(&p, Duration::ZERO, Arc::clone(&events))
        .run_once()
        .await;
    assert!(!exists(&b, &d, &f), "巡检未补删已留痕的入湖文件");
    assert_eq!(events.0.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn fsck_abandons_when_retired_segment_missing() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    lake_one(&p, &d, H10 + 1).await;
    Archiver::new(Arc::clone(&p.flusher), Duration::ZERO)
        .run_once()
        .await;
    let segment =
        p.c.chain
            .latest_snapshot(&d)
            .unwrap()
            .retired_segment
            .clone()
            .expect("已归档");
    let seg_file = b
        .dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog/default/app")
        .join(&segment.path);
    let seg_bytes = std::fs::read(&seg_file).unwrap();
    std::fs::remove_file(&seg_file).unwrap();

    // 一个孤儿数据文件
    let orphan = layout::data_file_path(HourBucket::of(H10 + HOUR), &layout::new_object_id());
    let orphan_file = b
        .dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog/default/app")
        .join(&orphan);
    std::fs::create_dir_all(orphan_file.parent().unwrap()).unwrap();
    std::fs::write(&orphan_file, b"orphan").unwrap();
    std::thread::sleep(Duration::from_millis(10));

    let events = Arc::new(Deleted::default());
    let f = fsck(&p, Duration::ZERO, Arc::clone(&events));
    f.run_once().await;
    assert!(orphan_file.exists(), "段缺失时巡检未放弃");
    assert_eq!(
        p.c.gates.state(&d),
        DatasetState::Normal,
        "巡检的读取使数据集停用了"
    );

    std::fs::write(&seg_file, seg_bytes).unwrap();
    f.run_once().await;
    assert!(!orphan_file.exists(), "段放回后巡检仍未删除孤儿");
}

/// 巡检缓存随清单片与快照的个数增长，不随「版本数 × 每版文件数」增长：同一个桶提交很多次后，缓存中清单片的条目数
/// 远小于各版本桶 manifest 的文件数之和，快照只记与父快照的差异；巡检照常删除孤儿数据文件，没有新提交时再巡检缓存不变。
#[tokio::test]
async fn fsck_cache_grows_with_pieces_not_versions() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let events = Arc::new(Deleted::default());
    let f = fsck(&p, Duration::from_secs(3600), Arc::clone(&events));
    // 每次提交一个数据文件：四分之三进同一个桶，其余轮流进另外三个桶；每 32 次巡检一次（缓存逐次接起）
    let n = 512usize;
    let mut main = 0usize;
    for i in 0..n {
        let bucket = if i % 4 == 0 { 1 + (i / 4) % 3 } else { 0 };
        main += usize::from(bucket == 0);
        let ts = H10 + bucket as i64 * HOUR + i as i64;
        p.sc.commit(&d, &request(data_files(&[ts], i as i64)))
            .await
            .unwrap();
        if i % 32 == 31 {
            f.run_once().await;
        }
    }
    let size = f.cache_size(&d);
    let snapshots = p.c.chain.latest_snapshot(&d).unwrap().snapshot_id.0 as usize;
    assert_eq!(size.snapshots, snapshots, "{size:?}");
    // 每个快照只记与父快照不同的那一个桶（新桶也算一条），链底不记
    assert!(size.snapshot_changes < snapshots, "{size:?}");
    // 按版本整份存时，主桶各版本的文件数之和为 1 + 2 + … + main
    let per_version = main * (main + 1) / 2;
    // 增量片各一条，每 64 片一份全量片重列当时的全部存活文件
    let bound = n + (main / 64) * main;
    assert!(
        size.piece_entries <= bound && size.piece_entries * 20 < per_version,
        "{size:?}，上界 {bound}，按版本整份存 {per_version}"
    );
    assert_eq!(size.pieces, n, "{size:?}");

    // 一个孤儿数据文件照常留痕后删除；再巡检，缓存规模不变
    let orphan = layout::data_file_path(HourBucket::of(H10 + 5 * HOUR), &layout::new_object_id());
    let orphan_file = b
        .dir
        .as_ref()
        .unwrap()
        .0
        .join("amberlog/default/app")
        .join(&orphan);
    std::fs::create_dir_all(orphan_file.parent().unwrap()).unwrap();
    std::fs::write(&orphan_file, b"orphan").unwrap();
    std::thread::sleep(Duration::from_millis(10));
    f.run_once().await;
    assert!(!orphan_file.exists(), "巡检未删除孤儿数据文件");
    assert_eq!(events.0.load(Ordering::SeqCst), 1);
    // 留痕提交多出一个快照，下一次巡检才读到
    f.run_once().await;
    let after = f.cache_size(&d);
    f.run_once().await;
    assert_eq!(f.cache_size(&d), after);
}

/// 冷层变更段提交带合并记录（`additions`，REQ-AMB-STO-HOT-LAYOUT-V5 的段格式）：段里写出 `additions`，
/// `summary.cold` 先去掉移除的、再加入合并产出；此后合并产出作为已记入冷层文件集的文件可再被移除，其摘要取自合并记录。
#[tokio::test]
async fn cold_change_segment_carries_additions() {
    let b = file_backend();
    let p = proc(&b.store);
    let d = app();
    let (_, a) = lake_one_cold(&p, &d, H10 + 1).await;
    let (_, c) = lake_one_cold(&p, &d, H10 + 2).await;
    let o = ColdFileEntry {
        path: "data/_timestamp_day=merged/o.parquet".into(),
        min_timestamp: a.min_timestamp,
        max_timestamp: c.max_timestamp,
        record_count: 2,
        size_bytes: 15,
        content_digest: Digest::of(b"merged"),
    };
    let merged =
        p.sc.commit_cold_change(
            &d,
            ColdChangeRequest {
                cold_removed_in: 7,
                reason: ColdDeletionReason::Compaction,
                cutoff: None,
                paths: vec![a.path.clone(), c.path.clone()],
                additions: vec![o.clone()],
            },
        )
        .await
        .unwrap();
    let chain = &p.c.chain;
    let snapshot = chain.snapshot(&d, merged.snapshot_id).await.unwrap();
    assert_eq!(snapshot.summary.cold, ColdSummary::of([&o]));
    let segments = chain.cold_change_segments(&d, &snapshot).await.unwrap();
    assert_eq!(segments.len(), 1);
    assert_eq!(segments[0].additions, vec![o.clone()]);
    let removed: Vec<_> = segments[0]
        .deletions
        .iter()
        .map(|x| (x.path.clone(), x.content_digest))
        .collect();
    assert_eq!(
        removed,
        vec![
            (a.path.clone(), a.content_digest),
            (c.path.clone(), c.content_digest)
        ]
    );
    assert_eq!(cold::traced(&segments), [7].into());

    let ttl =
        p.sc.commit_cold_change(
            &d,
            ColdChangeRequest {
                cold_removed_in: 8,
                reason: ColdDeletionReason::Ttl,
                cutoff: Some(H10 + 3),
                paths: vec![o.path.clone()],
                additions: Vec::new(),
            },
        )
        .await
        .unwrap();
    let snapshot = chain.snapshot(&d, ttl.snapshot_id).await.unwrap();
    assert_eq!(snapshot.summary.cold, ColdSummary::EMPTY);
    let segments = chain.cold_change_segments(&d, &snapshot).await.unwrap();
    assert_eq!(segments[0].deletions[0].content_digest, o.content_digest);
    assert_eq!(cold::traced(&segments), [7, 8].into());
}
