use std::sync::atomic::{AtomicU32, Ordering};

use amberlog_common::record::{FieldValue, Record};

use super::*;
use crate::chain::{Digest, SnapshotId};
use crate::commit::CommitIntent;

struct Dir(PathBuf);

impl Dir {
    fn new() -> Dir {
        static N: AtomicU32 = AtomicU32::new(0);
        let p = std::env::temp_dir().join(format!(
            "amberlog-wal-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = fs::remove_dir_all(&p);
        fs::create_dir_all(&p).unwrap();
        Dir(p)
    }

    fn segments(&self) -> Vec<PathBuf> {
        let mut v: Vec<PathBuf> = fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "wal"))
            .collect();
        v.sort();
        v
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn app() -> Dataset {
    Dataset::new("default", "app")
}

fn rec(seq: i64) -> WalEntry {
    WalEntry::Records {
        dataset: app(),
        records: vec![Record {
            timestamp_us: 1_000_000,
            fields: vec![("seq".into(), FieldValue::Int64(seq))],
        }],
    }
}

fn seq_of(e: &WalEntry) -> Option<i64> {
    match e {
        WalEntry::Records { records, .. } => match records[0].fields[0].1 {
            FieldValue::Int64(v) => Some(v),
            _ => None,
        },
        _ => None,
    }
}

/// 可在运行中开关的故障钩子：集合里的操作一律失败。
#[derive(Clone, Default)]
struct Faults(Arc<Mutex<Vec<&'static str>>>);

impl Faults {
    fn set(&self, ops: &[&'static str]) {
        *self.0.lock().unwrap() = ops.to_vec();
    }

    fn hook(&self) -> FaultHook {
        let me = self.clone();
        Arc::new(move |op| {
            if me.0.lock().unwrap().contains(&op) {
                Err(io::Error::other(format!("test {op} failure")))
            } else {
                Ok(())
            }
        })
    }
}

fn open(dir: &Dir) -> (FileWal, Vec<(u64, WalEntry)>) {
    open_with(dir, &Faults::default(), SEGMENT_BYTES)
}

fn open_with(dir: &Dir, faults: &Faults, segment_bytes: u64) -> (FileWal, Vec<(u64, WalEntry)>) {
    let wal = FileWal::with_options(&dir.0, faults.hook(), segment_bytes);
    let mut seen = Vec::new();
    wal.replay(&mut |p, e| seen.push((p.0, e))).unwrap();
    (wal, seen)
}

fn replayed(dir: &Dir) -> Result<Vec<(u64, WalEntry)>, WalError> {
    let wal = FileWal::open(&dir.0);
    let mut seen = Vec::new();
    wal.replay(&mut |p, e| seen.push((p.0, e)))?;
    Ok(seen)
}

fn seqs(seen: &[(u64, WalEntry)]) -> Vec<i64> {
    seen.iter().filter_map(|(_, e)| seq_of(e)).collect()
}

#[tokio::test]
async fn appends_survive_reopen_in_order_with_contiguous_positions() {
    let dir = Dir::new();
    let (wal, seen) = open(&dir);
    assert!(seen.is_empty());
    assert_eq!(
        wal.append(vec![WalEntry::DatasetExists { dataset: app() }, rec(0)])
            .await,
        Ok(WalPosition(2))
    );
    assert_eq!(wal.append(vec![rec(1)]).await, Ok(WalPosition(3)));
    drop(wal);

    let (wal, seen) = open(&dir);
    assert_eq!(seen.iter().map(|(p, _)| *p).collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(seqs(&seen), [0, 1]);
    // 重放之后的第一次写入新开一段，位置接续
    assert_eq!(wal.append(vec![rec(2)]).await, Ok(WalPosition(4)));
    drop(wal);
    assert_eq!(dir.segments().len(), 2);
    assert_eq!(seqs(&replayed(&dir).unwrap()), [0, 1, 2]);
}

/// 按位置读回已确认的条目（溢出记录的提交，REQ-AMB-ING-HOT-BACKPRESSURE）：跨段、跨重启都读得到，读不全即错误。
#[tokio::test]
async fn read_back_by_position_across_segments() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 64);
    for seq in 0..4 {
        wal.append(vec![rec(seq)]).await.unwrap();
    }
    assert!(dir.segments().len() > 1, "小段：每次追加都换段");
    let got = wal
        .read(vec![WalPosition(4), WalPosition(2)])
        .await
        .unwrap();
    assert_eq!(
        got.iter()
            .map(|(p, e)| (p.0, seq_of(e)))
            .collect::<Vec<_>>(),
        [(2, Some(1)), (4, Some(3))]
    );
    assert!(wal.read(vec![WalPosition(9)]).await.is_err());
    drop(wal);

    let (wal, _) = open(&dir);
    let got = wal.read(vec![WalPosition(1)]).await.unwrap();
    assert_eq!(seq_of(&got[0].1), Some(0));
}

/// 运行期读回时所需条目所在帧校验失败：[`WalError::Corrupt`]（REQ-AMB-ING-HOT-BACKPRESSURE-V2「溢出记录读不出时」）。
#[tokio::test]
async fn read_back_of_a_corrupt_frame_is_corrupt() {
    let dir = Dir::new();
    let (wal, _) = open(&dir);
    wal.append(vec![rec(0)]).await.unwrap();
    let segment = dir.segments().pop().unwrap();
    let before = fs::metadata(&segment).unwrap().len() as usize;
    wal.append(vec![rec(1)]).await.unwrap();
    wal.append(vec![rec(2)]).await.unwrap();
    let mut bytes = fs::read(&segment).unwrap();
    bytes[before + 20] ^= 0xFF;
    fs::write(&segment, &bytes).unwrap();
    assert_eq!(
        seq_of(&wal.read(vec![WalPosition(1)]).await.unwrap()[0].1),
        Some(0)
    );
    for p in [2, 3] {
        assert!(
            matches!(
                wal.read(vec![WalPosition(p)]).await,
                Err(WalError::Corrupt(_))
            ),
            "位置 {p}"
        );
    }
}

#[tokio::test]
async fn concurrent_appends_get_distinct_positions() {
    let dir = Dir::new();
    let (wal, _) = open(&dir);
    let wal = Arc::new(wal);
    let mut tasks = Vec::new();
    for i in 0..64 {
        let wal = Arc::clone(&wal);
        tasks.push(tokio::spawn(async move { wal.append(vec![rec(i)]).await }));
    }
    let mut positions = Vec::new();
    for t in tasks {
        positions.push(t.await.unwrap().unwrap().0);
    }
    positions.sort_unstable();
    assert_eq!(positions, (1..=64).collect::<Vec<_>>());
    drop(wal);
    let mut s = seqs(&replayed(&dir).unwrap());
    s.sort_unstable();
    assert_eq!(s, (0..64).collect::<Vec<_>>());
}

#[tokio::test]
async fn torn_tail_is_truncated_and_earlier_records_kept() {
    let dir = Dir::new();
    let (wal, _) = open(&dir);
    for i in 0..3 {
        wal.append(vec![rec(i)]).await.unwrap();
    }
    drop(wal);
    let seg = &dir.segments()[0];
    let len = fs::metadata(seg).unwrap().len();
    let f = OpenOptions::new().write(true).open(seg).unwrap();
    f.set_len(len - 3).unwrap();

    let (wal, seen) = open(&dir);
    assert_eq!(seqs(&seen), [0, 1]);
    // 截断后位置接续被截掉的那一帧
    assert_eq!(wal.append(vec![rec(9)]).await, Ok(WalPosition(3)));
    drop(wal);
    assert_eq!(seqs(&replayed(&dir).unwrap()), [0, 1, 9]);
}

#[tokio::test]
async fn corruption_with_intact_frames_after_it_is_fatal() {
    let dir = Dir::new();
    let (wal, _) = open(&dir);
    for i in 0..10 {
        wal.append(vec![rec(i)]).await.unwrap();
    }
    drop(wal);
    let seg = dir.segments()[0].clone();
    let mut bytes = fs::read(&seg).unwrap();
    let mid = bytes.len() / 2;
    bytes[mid] ^= 0xff;
    fs::write(&seg, &bytes).unwrap();
    assert!(matches!(replayed(&dir), Err(WalError::Corrupt(_))));
    // 失败的重放不改动段文件
    assert_eq!(fs::read(&seg).unwrap(), bytes);
}

#[tokio::test]
async fn damaged_frame_at_the_end_of_an_earlier_segment_is_fatal() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    for i in 0..3 {
        wal.append(vec![rec(i)]).await.unwrap();
    }
    drop(wal);
    let segs = dir.segments();
    assert_eq!(segs.len(), 3, "段大小为 1 字节时每次写入一段");
    let mut bytes = fs::read(&segs[1]).unwrap();
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    fs::write(&segs[1], &bytes).unwrap();
    assert!(matches!(replayed(&dir), Err(WalError::Corrupt(_))));
    assert_eq!(fs::read(&segs[1]).unwrap(), bytes, "失败的重放不截断");
}

#[tokio::test]
async fn a_missing_segment_is_fatal() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    for i in 0..3 {
        wal.append(vec![rec(i)]).await.unwrap();
    }
    drop(wal);
    fs::remove_file(&dir.segments()[1]).unwrap();
    assert!(matches!(replayed(&dir), Err(WalError::Corrupt(_))));
}

#[tokio::test]
async fn a_torn_new_segment_header_is_discarded() {
    let dir = Dir::new();
    let (wal, _) = open(&dir);
    wal.append(vec![rec(0)]).await.unwrap();
    drop(wal);
    fs::write(dir.0.join(format!("{:020}.wal", 99)), b"AMBWAL").unwrap();
    let (wal, seen) = open(&dir);
    assert_eq!(seqs(&seen), [0]);
    assert_eq!(wal.append(vec![rec(1)]).await, Ok(WalPosition(2)));
}

#[tokio::test]
async fn failed_write_is_not_replayed_and_later_appends_succeed() {
    let dir = Dir::new();
    let faults = Faults::default();
    let (wal, _) = open_with(&dir, &faults, SEGMENT_BYTES);
    wal.append(vec![rec(0)]).await.unwrap();
    for op in ["write", "fsync"] {
        faults.set(&[op]);
        assert!(matches!(
            wal.append(vec![rec(-1)]).await,
            Err(WalError::Write(_))
        ));
    }
    faults.set(&[]);
    assert_eq!(wal.append(vec![rec(1)]).await, Ok(WalPosition(2)));
    drop(wal);
    assert_eq!(seqs(&replayed(&dir).unwrap()), [0, 1]);
}

/// 收下 [`WalEvents::append_failed`] 的每次报出。
#[derive(Default)]
struct Reported(Mutex<Vec<(String, String, u64)>>);

impl WalEvents for Reported {
    fn append_failed(&self, op: &str, reason: &str, failures: u64) {
        self.0
            .lock()
            .unwrap()
            .push((op.to_owned(), reason.to_owned(), failures));
    }
}

#[tokio::test]
async fn append_failures_are_reported_with_op_and_reason_once_a_second() {
    let dir = Dir::new();
    let faults = Faults::default();
    let reported = Arc::new(Reported::default());
    let wal = FileWal::with_options(&dir.0, faults.hook(), SEGMENT_BYTES)
        .with_events(Arc::clone(&reported) as Arc<dyn WalEvents>);
    wal.replay(&mut |_, _| {}).unwrap();
    // 还没有活动段：第一次追加要打开新段
    faults.set(&["write"]);
    assert!(wal.append(vec![rec(0)]).await.is_err());
    faults.set(&[]);
    wal.append(vec![rec(0)]).await.unwrap();
    // 同一秒内同一操作同一原因的失败只报第一次
    faults.set(&["fsync"]);
    for _ in 0..3 {
        assert!(wal.append(vec![rec(-1)]).await.is_err());
    }
    let want = |op: &str, failures| (op.to_owned(), format!("test {op} failure"), failures);
    assert_eq!(
        *reported.0.lock().unwrap(),
        [
            (
                "open_segment".to_owned(),
                "test write failure".to_owned(),
                1
            ),
            want("fsync", 1),
        ]
    );
}

#[test]
fn failure_throttle_counts_the_failures_it_held_back() {
    let mut t = FailureThrottle::default();
    let t0 = Instant::now();
    let ms = |n| t0 + Duration::from_millis(n);
    assert_eq!(t.hit("write", "disk full", t0), Some(1));
    assert_eq!(t.hit("write", "disk full", ms(300)), None);
    assert_eq!(t.hit("write", "disk full", ms(600)), None);
    // 另一操作、另一原因各自计
    assert_eq!(t.hit("fsync", "disk full", ms(600)), Some(1));
    assert_eq!(t.hit("write", "too many open files", ms(600)), Some(1));
    assert_eq!(t.hit("write", "disk full", ms(1000)), Some(3));
    assert_eq!(t.hit("write", "disk full", ms(1500)), None);
    assert_eq!(t.hit("write", "disk full", ms(2100)), Some(2));
}

/// fsync 失败而回截也失败：失败的帧可能已在文件里，作废帧落盘之后它不再复活，即使此后没有新的追加。
#[tokio::test]
async fn failed_batch_that_cannot_be_rolled_back_is_voided() {
    let dir = Dir::new();
    let faults = Faults::default();
    let (wal, _) = open_with(&dir, &faults, SEGMENT_BYTES);
    wal.append(vec![rec(0)]).await.unwrap();
    faults.set(&["fsync", "truncate"]);
    assert!(wal.append(vec![rec(-1), rec(-2)]).await.is_err());
    faults.set(&[]);
    // 同步点：写线程已尝试把作废帧落盘（后台重试也可能先一步写下）
    assert!(wal.barrier().await, "作废帧已落盘");
    assert_eq!(dir.segments().len(), 2, "截不回去之后换了新段");
    // 模拟 SIGKILL：不经关闭直接读目录
    let seen = replayed(&dir).unwrap();
    assert_eq!(seqs(&seen), [0]);
    // 被作废的位置不复用
    assert_eq!(wal.append(vec![rec(1)]).await, Ok(WalPosition(4)));
    drop(wal);
    assert_eq!(seqs(&replayed(&dir).unwrap()), [0, 1]);
}

/// 作废帧也没来得及落盘就崩溃：失败的帧若残缺即按尾部截断，位置重新分配，不留空洞。
#[tokio::test]
async fn crash_before_the_void_is_written_leaves_a_consistent_log() {
    let dir = Dir::new();
    let faults = Faults::default();
    let (wal, _) = open_with(&dir, &faults, SEGMENT_BYTES);
    wal.append(vec![rec(0)]).await.unwrap();
    faults.set(&["write", "truncate"]);
    assert!(wal.append(vec![rec(-1)]).await.is_err());
    // 故障持续：后台重试写不下作废帧
    std::thread::sleep(Duration::from_millis(250));
    let (wal2, seen) = open(&dir);
    assert_eq!(seqs(&seen), [0]);
    assert_eq!(wal2.append(vec![rec(1)]).await, Ok(WalPosition(2)));
    drop((wal, wal2));
}

#[tokio::test]
async fn segments_are_deleted_only_after_every_kind_is_released() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    let hour = 7 * HOUR_US;
    wal.append(vec![WalEntry::DatasetExists { dataset: app() }, rec(0)])
        .await
        .unwrap(); // 段 1：位置 1、2
    wal.append(vec![WalEntry::Counts {
        dataset: app(),
        window_start_us: hour + 60_000_000,
        received: 1,
        durable: 1,
        rejected: 0,
        source: None,
        durable_ts: Some(vec![(hour + 60_000_000, 1)]),
    }])
    .await
    .unwrap(); // 段 2：位置 3
    wal.append(vec![rec(1)]).await.unwrap(); // 段 3（活动段）：位置 4
    assert_eq!(dir.segments().len(), 3);

    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(4),
    });
    wal.append(vec![rec(2)]).await.unwrap(); // 位置 5；追加在回收之后处理
    assert_eq!(dir.segments().len(), 4, "存在性尚未体现，段 1 不删");

    wal.release(Release::Existence { dataset: app() });
    wal.append(vec![rec(3)]).await.unwrap();
    assert_eq!(
        dir.segments().len(),
        4,
        "段 1 可删，但计数未写出挡住段 2 之后按序删除之外的段"
    );

    wal.release(Release::Counts {
        dataset: app(),
        hour_start_us: hour,
        through: WalPosition(3),
    });
    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(6),
    });
    wal.append(vec![rec(4)]).await.unwrap(); // 位置 7，新开一段
    // 除活动段外全部可删（写后的那次回收在回复追加之后进行，以同步点等它做完）
    wal.barrier().await;
    assert_eq!(dir.segments().len(), 1);
    drop(wal);
    let seen = replayed(&dir).unwrap();
    assert_eq!(seqs(&seen), [4]);
    assert_eq!(seen[0].0, 7);
}

/// 计数增量按记录时间戳涉及别的小时时（REQ-AMB-OBS-INGEST-COUNTERS-V3），接收小时与所涉各小时都写出之后段才可删。
#[tokio::test]
async fn counts_pin_every_hour_they_touch() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    let hour = 7 * HOUR_US;
    wal.append(vec![WalEntry::Counts {
        dataset: app(),
        window_start_us: hour + 60_000_000,
        received: 2,
        durable: 2,
        rejected: 0,
        source: None,
        durable_ts: Some(vec![(hour - 2 * HOUR_US, 1), (hour + 60_000_000, 1)]),
    }])
    .await
    .unwrap(); // 段 1：位置 1
    wal.append(vec![rec(0)]).await.unwrap(); // 段 2（活动段）：位置 2
    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(2),
    });
    wal.release(Release::Counts {
        dataset: app(),
        hour_start_us: hour,
        through: WalPosition(1),
    });
    wal.append(vec![rec(1)]).await.unwrap();
    wal.barrier().await;
    assert_eq!(
        dir.segments().len(),
        3,
        "按记录时间戳涉及的小时还没写出，段 1 不删"
    );

    wal.release(Release::Counts {
        dataset: app(),
        hour_start_us: hour - 2 * HOUR_US,
        through: WalPosition(1),
    });
    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(3),
    });
    wal.append(vec![rec(2)]).await.unwrap();
    wal.barrier().await;
    assert_eq!(dir.segments().len(), 1);
}

#[tokio::test]
async fn intents_outlive_their_segment_while_the_dataset_has_entries() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    let intent = |p: u64, b: u64| {
        WalEntry::CommitIntent(CommitIntent {
            dataset: app(),
            snapshot_id: SnapshotId(p),
            through: WalPosition(b),
            through_partial: None,
            digest: Digest([p as u8; 32]),
            files: Vec::new(),
        })
    };
    let baseline = WalEntry::Baseline {
        dataset: app(),
        snapshot_id: Some(SnapshotId(1)),
    };
    wal.append(vec![baseline.clone(), rec(0)]).await.unwrap(); // 1、2
    wal.append(vec![intent(2, 2)]).await.unwrap(); // 3
    wal.append(vec![rec(1)]).await.unwrap(); // 4
    wal.append(vec![intent(3, 4)]).await.unwrap(); // 5
    wal.append(vec![intent(4, 4)]).await.unwrap(); // 6
    wal.append(vec![rec(2)]).await.unwrap(); // 7（活动段）
    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(4),
    });
    wal.append(vec![rec(3)]).await.unwrap(); // 8
    // 段 1～5 删去，基线（位置 1）与两条最近的意图（位置 5、6）转存；位置 3 的旧意图不再保留
    drop(wal);
    let seen = replayed(&dir).unwrap();
    let positions: Vec<u64> = seen.iter().map(|(p, _)| *p).collect();
    assert_eq!(positions, [1, 5, 6, 7, 8]);
    assert_eq!(seen[0].1, baseline);
    assert_eq!(seen[1].1, intent(3, 4));
    assert_eq!(seqs(&seen), [2, 3]);

    // 数据集的全部条目都回收之后，转存也一并清掉
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    wal.release(Release::Records {
        dataset: app(),
        through: WalPosition(8),
    });
    wal.append(vec![WalEntry::DatasetExists {
        dataset: Dataset::new("default", "other"),
    }])
    .await
    .unwrap();
    drop(wal);
    assert!(!dir.0.join(RETAINED).exists());
    let seen = replayed(&dir).unwrap();
    assert_eq!(seen.len(), 1);
}

#[tokio::test]
async fn a_discard_marker_releases_the_records_it_covers() {
    let dir = Dir::new();
    let (wal, _) = open_with(&dir, &Faults::default(), 1);
    wal.append(vec![rec(0)]).await.unwrap(); // 1
    wal.append(vec![rec(1)]).await.unwrap(); // 2
    wal.append(vec![
        WalEntry::Discard {
            dataset: app(),
            through: WalPosition(2),
        },
        rec(1),
    ])
    .await
    .unwrap(); // 3、4
    wal.append(vec![rec(2)]).await.unwrap(); // 5，此时段 1、2 可删
    assert_eq!(dir.segments().len(), 2);
    drop(wal);
    let seen = replayed(&dir).unwrap();
    assert_eq!(seen.iter().map(|(p, _)| *p).collect::<Vec<_>>(), [3, 4, 5]);
}
