//! 耐久缓冲的文件实现：`wal.dir` 下的分段日志（REQ-AMB-LIFE-DURABILITY、ADR-0015、ADR-0021 第 4 条）。
//!
//! - **追加**：由一个专用写线程串行执行；同时到达的多次追加合并为一次写入与一次 fsync（组提交），
//!   fsync 返回成功之后才回复调用方。每次追加是一帧，重放时要么整帧出现、要么整帧不出现。
//! - **写入或 fsync 失败**：本组追加一律失败，并当场把文件截回本组写入之前的长度；截不回去时
//!   这些位置作废（记入待写的作废帧，随下一次写入或后台重试落盘），此后换新段写，不在可能残缺的字节之后追加。
//!   作废的位置在重放中不出现（ADR-0021 前提③）。
//! - **换段**：活动段写满 8 MiB 之后，下一次写入前换新段。段文件名为递增的段序号，段头记本段起始位置。
//! - **重放**：依次读各段，逐帧核对校验和。遇到损坏时向后搜索完整帧：搜得到即非尾部损坏
//!   （[`WalError::Corrupt`]，启动以退出码 3 失败）；搜不到即尾部，截断之。截断之后各段的帧与作废区间
//!   须连续覆盖自首段起始位置到最后位置的全部位置，有空洞同样属非尾部损坏（整个段丢失、段中间被截短等）。
//!   重放之后的第一次写入总是新开一段。
//! - **段回收**：按 [`Release`] 三种通知累计水位，只从最旧的段起按顺序删除条目全部被覆盖、且不是活动段的段。
//!   提交意图、基线快照号与计数意图不阻止删段；删段之前把仍需保留的这几类条目连同原位置转存到 `retained`
//!   （ADR-0021 第 4 条「意图不得先于它所覆盖、仍可被重放的记录丢失」），重放时按位置并回。
//! - **读回**：按位置读回已确认的条目（溢出记录的提交，REQ-AMB-ING-HOT-BACKPRESSURE），不经写线程：读者取写线程公布的
//!   各段起始位置与路径，在阻塞线程池里按段读取，读的同时写线程照常追加与 fsync（REQ-AMB-ING-HOT-BACKPRESSURE-V5「读回溢出记录
//!   不挡确认」）。已确认的帧落盘后不再改写（失败回截与作废只涉及其后的字节），活动段读到的只是此刻文件的长度，末尾未写完的帧
//!   不完整、读到即停。要读的条目尚未被回收通知覆盖（调用方保证），它们所在的段不会在读的同时被删除。段读不出或帧校验失败使
//!   所需条目读不到时为 [`WalError::Corrupt`]（REQ-AMB-ING-HOT-BACKPRESSURE-V2「溢出记录读不出时」，调用方以退出码 3 退出）。

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use amberlog_common::dataset::Dataset;
use tokio::sync::oneshot;

use super::codec::{
    Frame, SEGMENT_HEADER_LEN, decode_frame, encode_frame, find_next_frame, parse_segment_header,
    segment_header,
};
use super::{Release, Wal, WalEntry, WalError, WalPosition};
use crate::BoxFuture;

/// 活动段写满这么多字节后换新段（不对外）。
pub const SEGMENT_BYTES: u64 = 8 * 1024 * 1024;

const SEGMENT_SUFFIX: &str = ".wal";
const RETAINED: &str = "retained";
const RETAINED_TMP: &str = "retained.tmp";
const RETAINED_MAGIC: &[u8; 8] = b"AMBRET01";
const HOUR_US: i64 = 3_600_000_000;
/// 作废区间没能落盘时的后台重试间隔。
const VOID_RETRY: Duration = Duration::from_millis(100);

/// 作用于 WAL 文件写入调用的故障钩子：`op` 为 `write`、`fsync` 或 `truncate`。
type FaultHook = Arc<dyn Fn(&str) -> io::Result<()> + Send + Sync>;

/// 同一操作同一原因的追加失败，在这段时间内至多报一次。
const FAILURE_REPORT_INTERVAL: Duration = Duration::from_secs(1);

/// WAL 追加失败的可观察结果，由调用方接到日志。
pub trait WalEvents: Send + Sync {
    /// 打开新段、写入或 fsync 失败（这一组追加都以 [`WalError::Write`] 回复）：`op` 为 `open_segment`、`write` 或 `fsync`，
    /// `reason` 为操作系统给出的原因，`failures` 为自上次报出以来同一 `op` 同一 `reason` 的失败次数（含本次）。
    /// 同一 `op` 同一 `reason` 每秒至多报一次。
    fn append_failed(&self, op: &str, reason: &str, failures: u64);
}

/// 不接日志时的 [`WalEvents`]。
struct NoEvents;

impl WalEvents for NoEvents {
    fn append_failed(&self, _: &str, _: &str, _: u64) {}
}

/// `wal.dir` 下的分段日志。
pub struct FileWal {
    dir: PathBuf,
    fault: FaultHook,
    events: Arc<dyn WalEvents>,
    segment_bytes: u64,
    tx: Mutex<Option<mpsc::Sender<Cmd>>>,
    /// 写线程公布的各段（起始位置, 路径），自最旧的段起；读回据此找段。
    layout: Layout,
}

/// 写线程公布给读者的段列表。
type Layout = Arc<Mutex<Vec<(u64, PathBuf)>>>;

enum Cmd {
    Append(
        Vec<WalEntry>,
        oneshot::Sender<Result<WalPosition, WalError>>,
    ),
    Release(Release),
    /// 封住活动段并删除能删的段，回复剩下的段数（见 [`FileWal::drain`]）。
    Drain(oneshot::Sender<Result<usize, WalError>>),
    /// 测试用同步点：此前的命令都处理完、并已尝试把待写的作废帧落盘之后回复，`true` 即不再有待写的作废帧。
    #[cfg(test)]
    Barrier(oneshot::Sender<bool>),
}

impl FileWal {
    /// 打开 `dir`（须已存在且可写，由启动校验保证）。此时不读任何文件：先调用 [`Wal::replay`]。
    pub fn open(dir: &Path) -> FileWal {
        FileWal::with_options(dir, Arc::new(crate::fault::wal_io), SEGMENT_BYTES)
    }

    fn with_options(dir: &Path, fault: FaultHook, segment_bytes: u64) -> FileWal {
        FileWal {
            dir: dir.to_owned(),
            fault,
            events: Arc::new(NoEvents),
            segment_bytes,
            tx: Mutex::new(None),
            layout: Layout::default(),
        }
    }

    /// 追加失败报给 `events`（须在 [`Wal::replay`] 之前设定）。
    pub fn with_events(mut self, events: Arc<dyn WalEvents>) -> FileWal {
        self.events = events;
        self
    }

    /// 测试用同步点，见 `Cmd::Barrier`。
    #[cfg(test)]
    async fn barrier(&self) -> bool {
        let (reply, rx) = oneshot::channel();
        self.sender()
            .expect("已重放")
            .send(Cmd::Barrier(reply))
            .expect("写线程在运行");
        rx.await.expect("写线程回复")
    }

    fn sender(&self) -> Option<mpsc::Sender<Cmd>> {
        self.tx.lock().expect("WAL 锁未中毒").clone()
    }

    /// 优雅退役（REQ-AMB-LIFE-RETIREMENT-REPLAY）：此前发出的回收通知都处理完之后，封住活动段（此后的写入另开新段），
    /// 按段回收的同一条件删除条目都已被覆盖的段，返回剩下的段数（承载停用数据集记录的段删不掉，照常留下）。
    /// 转存 `retained` 或删除段文件失败为 [`WalError::Write`]。须在 [`Wal::replay`] 之后调用。
    pub async fn drain(&self) -> Result<usize, WalError> {
        let (reply, rx) = oneshot::channel();
        let sent = match self.sender() {
            Some(tx) => tx.send(Cmd::Drain(reply)).is_ok(),
            None => false,
        };
        if !sent {
            return Err(WalError::Write("WAL is not open".into()));
        }
        rx.await
            .unwrap_or_else(|_| Err(WalError::Write("WAL writer stopped".into())))
    }
}

impl Wal for FileWal {
    fn append(&self, entries: Vec<WalEntry>) -> BoxFuture<'_, Result<WalPosition, WalError>> {
        assert!(!entries.is_empty(), "追加的条目不得为空");
        let (reply, rx) = oneshot::channel();
        let sent = match self.sender() {
            Some(tx) => tx.send(Cmd::Append(entries, reply)).is_ok(),
            None => false,
        };
        Box::pin(async move {
            if !sent {
                return Err(WalError::Write("WAL is not open for appending".into()));
            }
            rx.await
                .unwrap_or_else(|_| Err(WalError::Write("WAL writer stopped".into())))
        })
    }

    fn replay(&self, sink: &mut dyn FnMut(WalPosition, WalEntry)) -> Result<(), WalError> {
        let mut tx = self.tx.lock().expect("WAL 锁未中毒");
        assert!(tx.is_none(), "replay 只调用一次");
        let mut writer = recover(&self.dir, Arc::clone(&self.fault), sink)?;
        writer.segment_bytes = self.segment_bytes;
        writer.layout = Arc::clone(&self.layout);
        writer.events = Arc::clone(&self.events);
        writer.publish();
        let (send, recv) = mpsc::channel();
        std::thread::Builder::new()
            .name("amberlog-wal".into())
            .spawn(move || writer.run(recv))
            .map_err(|e| WalError::Write(format!("cannot start the WAL writer: {e}")))?;
        *tx = Some(send);
        Ok(())
    }

    fn release(&self, release: Release) {
        if let Some(tx) = self.sender() {
            let _ = tx.send(Cmd::Release(release));
        }
    }

    fn read(
        &self,
        positions: Vec<WalPosition>,
    ) -> BoxFuture<'_, Result<Vec<(WalPosition, WalEntry)>, WalError>> {
        let open = self.sender().is_some();
        Box::pin(async move {
            if !open {
                return Err(WalError::Write("WAL is not open for reading".into()));
            }
            let segments = self.layout.lock().expect("WAL 段列表锁未中毒").clone();
            // 读出开始之后、读完之前的测试暂停点（REQ-AMB-ING-HOT-BACKPRESSURE-V5）
            crate::fault::gate("overflow.read_pause").await;
            tokio::task::spawn_blocking(move || read_entries(&segments, &positions))
                .await
                .unwrap_or_else(|e| Err(WalError::Write(format!("WAL reader failed: {e}"))))
        })
    }
}

// ---------------------------------------------------------------------------
// 段与水位的簿记
// ---------------------------------------------------------------------------

/// 一段中阻止删段的条目：每类取该段内的最大位置。
#[derive(Debug, Default)]
struct Pins {
    records: HashMap<Dataset, u64>,
    existence: HashSet<Dataset>,
    counts: HashMap<(Dataset, i64), u64>,
    /// 段内有记录、存在性、计数或 `_sent` 条目的数据集（意图类条目的转存以此判断数据集是否仍有条目）。
    datasets: HashSet<Dataset>,
}

impl Pins {
    fn add(&mut self, pos: u64, entry: &WalEntry) {
        match entry {
            WalEntry::Records { dataset, .. } => {
                raise(&mut self.records, dataset.clone(), pos);
                self.datasets.insert(dataset.clone());
            }
            WalEntry::DatasetExists { dataset } => {
                self.existence.insert(dataset.clone());
                self.datasets.insert(dataset.clone());
            }
            // 计数增量涉及接收小时与按记录时间戳的各小时（REQ-AMB-OBS-INGEST-COUNTERS-V3），各自登记
            WalEntry::Counts {
                dataset,
                window_start_us,
                durable_ts,
                ..
            } => {
                let timestamped = durable_ts.iter().flatten().map(|(w, _)| *w);
                for window in std::iter::once(*window_start_us).chain(timestamped) {
                    raise(&mut self.counts, (dataset.clone(), hour_of(window)), pos);
                }
                self.datasets.insert(dataset.clone());
            }
            WalEntry::Sent { dataset, elements } => {
                for e in elements {
                    raise(
                        &mut self.counts,
                        (dataset.clone(), hour_of(e.window_start_us)),
                        pos,
                    );
                }
                self.datasets.insert(dataset.clone());
            }
            // 作废标记不阻止删段：它所覆盖的记录都在它之前，按序删段时总先于它被删去
            WalEntry::CommitIntent(_)
            | WalEntry::Baseline { .. }
            | WalEntry::CountIntent { .. }
            | WalEntry::Discard { .. } => {}
        }
    }
}

fn raise<K: std::hash::Hash + Eq>(m: &mut HashMap<K, u64>, key: K, pos: u64) {
    let v = m.entry(key).or_insert(pos);
    *v = (*v).max(pos);
}

fn hour_of(us: i64) -> i64 {
    us.div_euclid(HOUR_US) * HOUR_US
}

/// 已收到的段回收通知（各水位只增不减）。
#[derive(Debug, Default)]
struct Released {
    records: HashMap<Dataset, u64>,
    existence: HashSet<Dataset>,
    counts: HashMap<(Dataset, i64), u64>,
}

impl Released {
    fn apply(&mut self, r: Release) {
        match r {
            Release::Records { dataset, through } => {
                let v = self.records.entry(dataset).or_insert(0);
                *v = (*v).max(through.0);
            }
            Release::Counts {
                dataset,
                hour_start_us,
                through,
            } => {
                let v = self
                    .counts
                    .entry((dataset, hour_of(hour_start_us)))
                    .or_insert(0);
                *v = (*v).max(through.0);
            }
            Release::Existence { dataset } => {
                self.existence.insert(dataset);
            }
        }
    }

    /// 作废标记同时是其覆盖记录的回收通知。
    fn note(&mut self, entry: &WalEntry) {
        if let WalEntry::Discard { dataset, through } = entry {
            self.apply(Release::Records {
                dataset: dataset.clone(),
                through: *through,
            });
        }
    }

    fn covers(&self, pins: &Pins) -> bool {
        pins.records
            .iter()
            .all(|(d, p)| self.records.get(d).is_some_and(|t| t >= p))
            && pins.existence.iter().all(|d| self.existence.contains(d))
            && pins
                .counts
                .iter()
                .all(|(k, p)| self.counts.get(k).is_some_and(|t| t >= p))
    }
}

/// 须跨段保留的意图类条目：每个数据集最近两条提交意图（最近一条已判明的与至多一条未判明的）、
/// 最近一条基线快照号，每个 (数据集, 小时) 最近两条计数意图。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum RetainKey {
    Commit(Dataset),
    Baseline(Dataset),
    Count(Dataset, i64),
}

impl RetainKey {
    fn of(entry: &WalEntry) -> Option<RetainKey> {
        match entry {
            WalEntry::CommitIntent(i) => Some(RetainKey::Commit(i.dataset.clone())),
            WalEntry::Baseline { dataset, .. } => Some(RetainKey::Baseline(dataset.clone())),
            WalEntry::CountIntent {
                dataset,
                hour_start_us,
                ..
            } => Some(RetainKey::Count(dataset.clone(), *hour_start_us)),
            _ => None,
        }
    }

    fn keep(&self) -> usize {
        match self {
            RetainKey::Baseline(_) => 1,
            RetainKey::Commit(_) | RetainKey::Count(..) => 2,
        }
    }

    fn dataset(&self) -> &Dataset {
        match self {
            RetainKey::Commit(d) | RetainKey::Baseline(d) | RetainKey::Count(d, _) => d,
        }
    }
}

#[derive(Debug, Default)]
struct Retained(BTreeMap<RetainKey, VecDeque<(u64, WalEntry)>>);

impl Retained {
    fn add(&mut self, pos: u64, entry: &WalEntry) {
        if let Some(key) = RetainKey::of(entry) {
            let keep = key.keep();
            let q = self.0.entry(key).or_default();
            q.push_back((pos, entry.clone()));
            while q.len() > keep {
                q.pop_front();
            }
        }
    }
}

struct Segment {
    first: u64,
    path: PathBuf,
    pins: Pins,
}

struct Active {
    file: File,
    len: u64,
}

// ---------------------------------------------------------------------------
// 重放
// ---------------------------------------------------------------------------

fn corrupt(what: impl std::fmt::Display) -> WalError {
    WalError::Corrupt(what.to_string())
}

fn segment_seq(name: &str) -> Option<u64> {
    let digits = name.strip_suffix(SEGMENT_SUFFIX)?;
    (digits.len() == 20 && digits.bytes().all(|b| b.is_ascii_digit()))
        .then(|| digits.parse().ok())
        .flatten()
}

fn segment_path(dir: &Path, seq: u64) -> PathBuf {
    dir.join(format!("{seq:020}{SEGMENT_SUFFIX}"))
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

fn read_retained(dir: &Path) -> Result<Vec<(u64, WalEntry)>, WalError> {
    let path = dir.join(RETAINED);
    let bytes = match fs::read(&path) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(corrupt(format!("cannot read {}: {e}", path.display()))),
    };
    let bad = || corrupt(format!("{} is damaged", path.display()));
    if bytes.get(..8) != Some(&RETAINED_MAGIC[..]) {
        return Err(bad());
    }
    let mut out = Vec::new();
    let mut off = 8;
    while off < bytes.len() {
        match decode_frame(&bytes[off..]) {
            Some((Frame::Batch { first, entries }, n)) if entries.len() == 1 => {
                out.extend(entries.into_iter().map(|e| (first.0, e)));
                off += n;
            }
            _ => return Err(bad()),
        }
    }
    Ok(out)
}

enum Repair {
    Remove(PathBuf),
    Truncate(PathBuf, u64),
}

impl Repair {
    fn apply(self) -> io::Result<()> {
        match self {
            Repair::Remove(path) => fs::remove_file(path),
            Repair::Truncate(path, len) => {
                let f = OpenOptions::new().write(true).open(path)?;
                f.set_len(len)?;
                f.sync_all()
            }
        }
    }
}

/// 读出 `dir` 中的全部段，截断损坏的尾部，把未作废的条目按位置交给 `sink`，并建立写线程的状态。
fn recover(
    dir: &Path,
    fault: FaultHook,
    sink: &mut dyn FnMut(WalPosition, WalEntry),
) -> Result<Writer, WalError> {
    let io_err = |what: &str, e: io::Error| corrupt(format!("{what}: {e}"));
    match fs::remove_file(dir.join(RETAINED_TMP)) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => {
            return Err(io_err("cannot remove the stale retained.tmp", e));
        }
        _ => {}
    }
    let retained_file = read_retained(dir)?;

    let mut seqs: Vec<u64> = fs::read_dir(dir)
        .map_err(|e| io_err("cannot list wal.dir", e))?
        .filter_map(|e| e.ok()?.file_name().to_str().and_then(segment_seq))
        .collect();
    seqs.sort_unstable();

    let mut segments = Vec::new();
    // (起, 止, 段下标, 条目)；作废区间单列
    let mut batches: Vec<(u64, u64, usize, Vec<WalEntry>)> = Vec::new();
    let mut voids: Vec<(u64, u64)> = Vec::new();
    let mut headers: Vec<u64> = Vec::new();
    // 截断与删除都等覆盖检查通过之后才做：以退出码 3 失败时不改动任何文件
    let mut repairs = Vec::new();
    for (i, &seq) in seqs.iter().enumerate() {
        let path = segment_path(dir, seq);
        let is_last = i + 1 == seqs.len();
        let bytes = fs::read(&path).map_err(|e| io_err("cannot read a WAL segment", e))?;
        let Some(first) = parse_segment_header(&bytes) else {
            if is_last && bytes.len() <= SEGMENT_HEADER_LEN {
                // 新建段时崩溃：段头尚未写完，段内没有任何帧
                repairs.push(Repair::Remove(path));
                continue;
            }
            return Err(corrupt(format!(
                "WAL segment {} has a damaged header",
                path.display()
            )));
        };
        let index = segments.len();
        let mut off = SEGMENT_HEADER_LEN;
        while off < bytes.len() {
            match decode_frame(&bytes[off..]) {
                Some((frame, n)) => {
                    match frame {
                        Frame::Batch { first, entries } => {
                            if !entries.is_empty() {
                                let last = first.0 + entries.len() as u64 - 1;
                                batches.push((first.0, last, index, entries));
                            }
                        }
                        Frame::Void(ranges) => voids.extend(ranges.iter().map(|(a, b)| (a.0, b.0))),
                    }
                    off += n;
                }
                None => {
                    if find_next_frame(&bytes, off + 1).is_some() {
                        return Err(corrupt(format!(
                            "WAL segment {} is damaged at byte {off} with intact records after it",
                            path.display()
                        )));
                    }
                    // 尾部：不完整或校验不符的最后一段字节
                    repairs.push(Repair::Truncate(path.clone(), off as u64));
                    break;
                }
            }
        }
        headers.push(first.0);
        segments.push(Segment {
            first: first.0,
            path,
            pins: Pins::default(),
        });
    }

    // 覆盖检查：自首段起始位置起，帧与作废区间连续覆盖到最后位置；后续各段段头声明其之前的位置均已覆盖
    let mut next = 1;
    if let Some(&start) = headers.first() {
        let mut ranges: Vec<(u64, u64, bool)> = batches
            .iter()
            .map(|(a, b, ..)| (*a, *b, true))
            .chain(voids.iter().map(|(a, b)| (*a, *b, false)))
            .filter(|(_, b, _)| *b >= start)
            .collect();
        ranges.sort_unstable();
        let mut covered = start - 1;
        let mut last_batch_end = 0;
        for (a, b, is_batch) in ranges {
            if a > covered + 1 {
                return Err(corrupt(format!(
                    "WAL positions {}..{} are missing",
                    covered + 1,
                    a - 1
                )));
            }
            if is_batch {
                if a <= last_batch_end {
                    return Err(corrupt(format!("WAL position {a} appears twice")));
                }
                last_batch_end = b;
            }
            covered = covered.max(b);
        }
        let claimed = headers.iter().map(|h| h - 1).max().unwrap_or(0);
        if claimed > covered {
            return Err(corrupt(format!(
                "WAL positions {}..{claimed} are missing",
                covered + 1
            )));
        }
        next = covered + 1;
    }

    for repair in repairs {
        repair
            .apply()
            .map_err(|e| io_err("cannot repair a torn WAL tail", e))?;
    }
    if !headers.is_empty() || !seqs.is_empty() {
        sync_dir(dir).map_err(|e| io_err("cannot sync wal.dir", e))?;
    }

    // 交出条目：先转存的（位置早于首段、且不与段内重复），再各段；作废的位置跳过
    let voided = |p: u64| voids.iter().any(|(a, b)| (*a..=*b).contains(&p));
    let seg_start = headers.first().copied().unwrap_or(u64::MAX);
    let mut retained = Retained::default();
    let mut released = Released::default();
    for (pos, entry) in retained_file.iter().filter(|(p, _)| *p < seg_start) {
        retained.add(*pos, entry);
        next = next.max(pos + 1);
        sink(WalPosition(*pos), entry.clone());
    }
    let sidecar: Vec<(u64, WalEntry)> = retained_file
        .into_iter()
        .filter(|(p, _)| *p < seg_start)
        .collect();
    batches.sort_unstable_by_key(|b| b.0);
    for (first, _, index, entries) in batches {
        for (k, entry) in entries.into_iter().enumerate() {
            let pos = first + k as u64;
            if voided(pos) {
                continue;
            }
            segments[index].pins.add(pos, &entry);
            retained.add(pos, &entry);
            released.note(&entry);
            sink(WalPosition(pos), entry);
        }
    }

    Ok(Writer {
        dir: dir.to_owned(),
        next_seq: seqs.last().map_or(1, |s| s + 1),
        segments,
        active: None,
        next,
        voids: Vec::new(),
        needs_rotate: false,
        released,
        retained,
        sidecar,
        fault,
        segment_bytes: SEGMENT_BYTES,
        layout: Layout::default(),
        events: Arc::new(NoEvents),
        failures: FailureThrottle::default(),
    })
}

// ---------------------------------------------------------------------------
// 写线程
// ---------------------------------------------------------------------------

struct Writer {
    dir: PathBuf,
    next_seq: u64,
    /// 自最旧的段起；有活动段时它是最后一个。
    segments: Vec<Segment>,
    active: Option<Active>,
    /// 下一个可分配的位置。
    next: u64,
    /// 已作废、作废帧尚未落盘的位置区间。
    voids: Vec<(u64, u64)>,
    needs_rotate: bool,
    released: Released,
    retained: Retained,
    /// `retained` 文件此刻的内容。
    sidecar: Vec<(u64, WalEntry)>,
    fault: FaultHook,
    segment_bytes: u64,
    /// 公布给读者的段列表（与 `segments` 同步更新）。
    layout: Layout,
    events: Arc<dyn WalEvents>,
    failures: FailureThrottle,
}

/// 追加失败的节流：按（操作, 原因）记上次报出的时刻与此后没报的次数。
#[derive(Default)]
struct FailureThrottle {
    seen: HashMap<(&'static str, String), (Instant, u64)>,
}

impl FailureThrottle {
    /// 记一次失败；该报出时返回自上次报出以来的失败次数（含本次）。
    fn hit(&mut self, op: &'static str, reason: &str, now: Instant) -> Option<u64> {
        if self.seen.len() > 64 {
            self.seen
                .retain(|_, (at, _)| now.duration_since(*at) < FAILURE_REPORT_INTERVAL);
        }
        match self.seen.get_mut(&(op, reason.to_owned())) {
            Some((at, unreported)) if now.duration_since(*at) < FAILURE_REPORT_INTERVAL => {
                *unreported += 1;
                None
            }
            Some((at, unreported)) => {
                let failures = *unreported + 1;
                *at = now;
                *unreported = 0;
                Some(failures)
            }
            None => {
                self.seen.insert((op, reason.to_owned()), (now, 0));
                Some(1)
            }
        }
    }
}

type Pending = (
    Vec<WalEntry>,
    oneshot::Sender<Result<WalPosition, WalError>>,
);

impl Writer {
    fn run(mut self, rx: mpsc::Receiver<Cmd>) {
        loop {
            let first = if self.voids.is_empty() {
                match rx.recv() {
                    Ok(c) => c,
                    Err(_) => return,
                }
            } else {
                match rx.recv_timeout(VOID_RETRY) {
                    Ok(c) => c,
                    Err(RecvTimeoutError::Timeout) => {
                        self.write_group(Vec::new());
                        let _ = self.collect();
                        continue;
                    }
                    Err(RecvTimeoutError::Disconnected) => return,
                }
            };
            let mut appends = Vec::new();
            let mut released = false;
            let mut drains = Vec::new();
            #[cfg(test)]
            let mut barriers: Vec<oneshot::Sender<bool>> = Vec::new();
            for cmd in std::iter::once(first).chain(rx.try_iter()) {
                match cmd {
                    Cmd::Append(entries, reply) => appends.push((entries, reply)),
                    Cmd::Release(r) => {
                        self.released.apply(r);
                        released = true;
                    }
                    Cmd::Drain(reply) => drains.push(reply),
                    #[cfg(test)]
                    Cmd::Barrier(reply) => barriers.push(reply),
                }
            }
            if released {
                let _ = self.collect();
            }
            if !appends.is_empty() {
                self.write_group(appends);
                let _ = self.collect();
            }
            for reply in drains {
                let _ = reply.send(self.drain());
            }
            #[cfg(test)]
            if !barriers.is_empty() {
                self.write_group(Vec::new());
                let _ = self.collect();
                for reply in barriers {
                    let _ = reply.send(self.voids.is_empty());
                }
            }
        }
    }

    fn fault(&self, op: &str) -> io::Result<()> {
        (self.fault)(op)
    }

    /// 一次追加失败，按 [`FailureThrottle`] 节流后报给 `events`。
    fn report_failure(&mut self, op: &'static str, e: &io::Error) {
        let reason = e.to_string();
        if let Some(failures) = self.failures.hit(op, &reason, Instant::now()) {
            self.events.append_failed(op, &reason, failures);
        }
    }

    /// 把此刻的段列表公布给读者。
    fn publish(&self) {
        *self.layout.lock().expect("WAL 段列表锁未中毒") = self
            .segments
            .iter()
            .map(|s| (s.first, s.path.clone()))
            .collect();
    }

    /// 需要时新开一段：尚无活动段、活动段已满，或上次失败后活动段尾部可能残缺。
    fn ensure_segment(&mut self) -> io::Result<()> {
        if let Some(a) = &self.active
            && !self.needs_rotate
            && a.len < self.segment_bytes
        {
            return Ok(());
        }
        // 段头的起始位置 = 最小的尚未落盘覆盖的位置：作废帧若随本段一同丢失，重放时这些位置按未分配处理
        let first = self.voids.iter().map(|v| v.0).min().unwrap_or(self.next);
        let seq = self.next_seq;
        let path = segment_path(&self.dir, seq);
        self.fault("write")?;
        let mut file = OpenOptions::new()
            .create_new(true)
            .append(true)
            .open(&path)?;
        let init = file
            .write_all(&segment_header(WalPosition(first)))
            .and_then(|()| {
                self.fault("fsync")?;
                file.sync_all()
            })
            .and_then(|()| sync_dir(&self.dir));
        if let Err(e) = init {
            let _ = fs::remove_file(&path);
            return Err(e);
        }
        self.next_seq += 1;
        self.segments.push(Segment {
            first,
            path,
            pins: Pins::default(),
        });
        self.publish();
        self.active = Some(Active {
            file,
            len: SEGMENT_HEADER_LEN as u64,
        });
        self.needs_rotate = false;
        Ok(())
    }

    /// 写一组追加（可以为空：只补写作废帧），一次 fsync。
    fn write_group(&mut self, appends: Vec<Pending>) {
        let fail = |appends: Vec<Pending>, e: &io::Error| {
            for (_, reply) in appends {
                let _ = reply.send(Err(WalError::Write(e.to_string())));
            }
        };
        if appends.is_empty() && self.voids.is_empty() {
            return;
        }
        if let Err(e) = self.ensure_segment() {
            self.report_failure("open_segment", &e);
            fail(appends, &e);
            return;
        }
        let mut bytes = Vec::new();
        if !self.voids.is_empty() {
            let ranges = self
                .voids
                .iter()
                .map(|(a, b)| (WalPosition(*a), WalPosition(*b)))
                .collect();
            encode_frame(&Frame::Void(ranges), &mut bytes);
        }
        let mut pos = self.next;
        let mut lasts = Vec::with_capacity(appends.len());
        for (entries, _) in &appends {
            encode_frame(
                &Frame::Batch {
                    first: WalPosition(pos),
                    entries: entries.clone(),
                },
                &mut bytes,
            );
            pos += entries.len() as u64;
            lasts.push(pos - 1);
        }

        let fault = Arc::clone(&self.fault);
        let active = self.active.as_mut().expect("ensure_segment 之后有活动段");
        let start_len = active.len;
        let written = fault("write")
            .and_then(|()| active.file.write_all(&bytes))
            .map_err(|e| ("write", e))
            .and_then(|()| {
                fault("fsync")
                    .and_then(|()| active.file.sync_data())
                    .map_err(|e| ("fsync", e))
            });
        if let Err((op, e)) = written {
            let rolled_back = fault("truncate")
                .and_then(|()| active.file.set_len(start_len))
                .and_then(|()| active.file.sync_data());
            if rolled_back.is_err() && pos > self.next {
                // 截不回去：这些位置作废，此后换新段
                self.voids.push((self.next, pos - 1));
                self.next = pos;
                self.needs_rotate = true;
            } else if rolled_back.is_err() {
                self.needs_rotate = true;
            }
            self.report_failure(op, &e);
            fail(appends, &e);
            return;
        }
        active.len += bytes.len() as u64;
        self.voids.clear();
        let segment = self.segments.last_mut().expect("活动段在列表末尾");
        let mut p = self.next;
        for (entries, _) in &appends {
            for entry in entries {
                segment.pins.add(p, entry);
                self.retained.add(p, entry);
                self.released.note(entry);
                p += 1;
            }
        }
        self.next = pos;
        for ((_, reply), last) in appends.into_iter().zip(lasts) {
            let _ = reply.send(Ok(WalPosition(last)));
        }
    }

    /// 封住活动段（作废帧先落盘），再按 [`Writer::collect`] 删段；返回剩下的段数。
    fn drain(&mut self) -> Result<usize, WalError> {
        self.write_group(Vec::new());
        if !self.voids.is_empty() {
            return Err(WalError::Write(
                "voided WAL positions could not be recorded".into(),
            ));
        }
        self.active = None;
        self.collect()
            .map_err(|e| WalError::Write(format!("cannot delete a WAL segment: {e}")))?;
        Ok(self.segments.len())
    }

    /// 从最旧的段起删除已被回收通知全部覆盖的段；删段之前转存仍需保留的意图类条目。转存或删除失败时停下（下次再试）
    /// 并返回该错误。
    fn collect(&mut self) -> io::Result<()> {
        let sealed = self.segments.len() - usize::from(self.active.is_some());
        let k = self.segments[..sealed]
            .iter()
            .take_while(|s| self.released.covers(&s.pins))
            .count();
        if k == 0 {
            return Ok(());
        }
        let boundary = self.segments.get(k).map_or(self.next, |s| s.first);
        let live: HashSet<&Dataset> = self.segments[k..]
            .iter()
            .flat_map(|s| s.pins.datasets.iter())
            .collect();
        let mut sidecar: Vec<(u64, WalEntry)> = self
            .retained
            .0
            .iter()
            .filter(|(key, _)| live.contains(key.dataset()))
            .flat_map(|(_, q)| q.iter().filter(|(p, _)| *p < boundary).cloned())
            .collect();
        sidecar.sort_unstable_by_key(|(p, _)| *p);
        if sidecar != self.sidecar {
            // 转存不成功就不删段，下次再试
            self.write_retained(&sidecar)?;
            self.sidecar = sidecar;
        }
        let mut removed = 0;
        let mut failed = None;
        for s in &self.segments[..k] {
            if let Err(e) = self.fault("delete").and_then(|()| fs::remove_file(&s.path)) {
                failed = Some(e);
                break;
            }
            removed += 1;
        }
        if removed > 0 {
            let _ = sync_dir(&self.dir);
        }
        self.segments.drain(..removed);
        if removed > 0 {
            self.publish();
        }
        failed.map_or(Ok(()), Err)
    }

    fn write_retained(&self, entries: &[(u64, WalEntry)]) -> io::Result<()> {
        let path = self.dir.join(RETAINED);
        if entries.is_empty() {
            match fs::remove_file(&path) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
            return sync_dir(&self.dir);
        }
        let mut bytes = RETAINED_MAGIC.to_vec();
        for (p, e) in entries {
            encode_frame(
                &Frame::Batch {
                    first: WalPosition(*p),
                    entries: vec![e.clone()],
                },
                &mut bytes,
            );
        }
        let tmp = self.dir.join(RETAINED_TMP);
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, &path)?;
        sync_dir(&self.dir)
    }
}

/// 读回这些位置上的条目，按位置升序（读者一侧，见模块说明「读回」）。`segments` 是写线程公布的段列表。位置须已确认、其段
/// 尚未删除：段读不出、或所在帧校验失败而读不到的位置都是 [`WalError::Corrupt`]（这些已确认的条目无法无损读出）。
fn read_entries(
    segments: &[(u64, PathBuf)],
    positions: &[WalPosition],
) -> Result<Vec<(WalPosition, WalEntry)>, WalError> {
    let mut wanted: Vec<u64> = positions.iter().map(|p| p.0).collect();
    wanted.sort_unstable();
    wanted.dedup();
    let mut out = Vec::with_capacity(wanted.len());
    for (i, (first, path)) in segments.iter().enumerate() {
        let end = segments.get(i + 1).map_or(u64::MAX, |s| s.0);
        let here: HashSet<u64> = wanted
            .iter()
            .copied()
            .filter(|p| (*first..end).contains(p))
            .collect();
        if here.is_empty() {
            continue;
        }
        let bytes = fs::read(path)
            .map_err(|e| WalError::Corrupt(format!("cannot read a WAL segment: {e}")))?;
        let mut off = SEGMENT_HEADER_LEN;
        while off < bytes.len() {
            let Some((frame, n)) = decode_frame(&bytes[off..]) else {
                break;
            };
            if let Frame::Batch { first, entries } = frame {
                for (k, entry) in entries.into_iter().enumerate() {
                    let pos = first.0 + k as u64;
                    if here.contains(&pos) {
                        out.push((WalPosition(pos), entry));
                    }
                }
            }
            off += n;
        }
    }
    out.sort_unstable_by_key(|(p, _)| *p);
    out.dedup_by_key(|(p, _)| *p);
    if out.len() != wanted.len() {
        return Err(WalError::Corrupt(format!(
            "{} of {} WAL positions could not be read back",
            wanted.len() - out.len(),
            wanted.len()
        )));
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
