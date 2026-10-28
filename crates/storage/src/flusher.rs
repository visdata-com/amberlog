//! 物化（Flush）的触发与编排，以及快照提交对其他元素的接口 [`Committer`] 的实现
//! （ADR-0021 第 1、2 条，ADR-0052 第 4 条）。
//!
//! - 触发：每个 `flush.interval` 为每个数据集 flush 一次；某数据集尚未冻结的积压达到 `flush.max_records` 条时立即为它 flush。
//!   为此 [`Flusher`] 包在可见集的写入一侧外面（实现 [`MemtableWriter`]，转交给可见集并按数据集计数）。
//! - 数据集之间互不等待（REQ-AMB-STO-MANIFEST-COMMIT-V5）：每个数据集一个提交任务，同一数据集的 flush 串行，不同数据集的并发；
//!   周期节拍与即时触发只是唤醒各数据集的任务，不等任何一次提交返回。Parquet 编码、摘要与列统计在阻塞线程池里做，
//!   同时进行的编码至多为可用核数。
//! - 一次 flush 逐次写入提交，直到开始时已有的积压都已提交（其后到达的积压仍达到上限的接着提交）：每次按确认的先后从可见集冻结
//!   至多 `flush.max_records` 条的前缀（所含溢出记录读回后的字节数达到内存上限即提前截断）→ 物化成按小时桶拆分的 Parquet →
//!   提交 → 回执（从可见集移除这一代、归还它占用的内存额度）；段回收通知由提交发出（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。
//!   同一数据集的写入提交串行、以前一次的结果为父快照，所以各前缀依次进入已提交快照。
//! - 提交失败时冻结代留在可见集与 WAL 中，下一次 flush 重提。它的提交意图已记入 WAL（结果可能不明）时，先以同一份快照字节
//!   判明，不并入此后的记录（[`CommitProgress`]）；意图尚未记入（如上传数据文件时对象存储就不可访问）且不足一次的上限时，
//!   退回可见集，下一次按当时的积压重新冻结前缀。
//! - 读到数据集停用之后不再为它冻结、物化或上传；有未提交记录时按周期记一次提交失败。
//! - 物化读出溢出记录时 WAL 损坏（[`WalError::Corrupt`]）交给 [`Flusher::with_corruption_handler`] 的处置（进程里以退出码 3
//!   退出）；含它们的一代退回可见集之首，该数据集其后的记录不越过它提交。
//! - 每个周期同时补完各数据集待办的创世快照（[`StoreCommitter::settle`]）。
//!
//! 提交结果按数据集计入 `amberlog_manifest_commits_total`（REQ-AMB-OBS-HEALTH-METRICS）：成功每次计一次，失败在同一个
//! `flush.interval` 内至多计一次；创世快照的提交同样计入。提交因对象存储不可访问、
//! WAL 写入失败而没做成的数据集（「卡住」），自失败起到下一次成功或进入停用为止每个 `flush.interval` 计一次失败；停用且有
//! 未提交记录的数据集同样每个周期计一次。这两种按周期的计数由独立的节拍完成，不随一次提交尝试（含重试与退避）的耗时拖后。
//! 每计一次调用一次 [`Flusher::with_commit_observer`] 给定的观察者，另按数据集累计（[`Flusher::commit_totals`]）。

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_common::record::Record;
use amberlog_common::schema::ColumnType;

use crate::BoxFuture;
use crate::bloom::BloomColumns;
use crate::chain::SnapshotId;
use crate::commit::{Baseline, CommitIntent, Committer, Existence, IntentOutcome};
use crate::committer::{CommitError, CommitProgress, CommitRequest, StoreCommitter};
use crate::fault;
use crate::flush::{DataFile, FlushError, write_data_files};
use crate::gate::DatasetState;
use crate::memtable::{
    FrozenGeneration, MemtableGenerations, MemtableView, MemtableWriter, records_to_batch,
};
use crate::registry::Persistence;
use crate::schema::StreamColumns;
use crate::store::Unavailable;
use crate::wal::{WalEntry, WalError, WalPosition};

/// flush 的触发条件（`flush.interval`、`flush.max_records`）。
#[derive(Debug, Clone, Copy)]
pub struct FlushConfig {
    pub interval: Duration,
    /// 既是触发写入提交的积压条数，也是一次写入提交的记录数上限（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。
    pub max_records: u64,
}

/// 一次 flush 的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushOutcome {
    /// 没有待提交的记录，没有写新快照。
    Idle,
    /// 提交成功，最后一次写入提交创建了这个快照。
    Committed(SnapshotId),
    /// 没有提交成功；冻结代与 WAL 保留，下一次 flush 重提。
    Failed(FlushFailure),
}

/// flush 没有提交成功的原因。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlushFailure {
    Commit(CommitError),
    /// 冻结代与 stream schema 不符，写不成 Parquet（实现缺陷），或溢出记录读不出。
    Materialize(FlushError),
}

/// 一个数据集按提交结果累计的次数。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CommitTotals {
    pub ok: u64,
    pub failed: u64,
}

/// 提交结果的观察者：每计入一次 `amberlog_manifest_commits_total` 调用一次，`ok` 为成功。
pub type CommitObserver = Arc<dyn Fn(&Dataset, bool) + Send + Sync>;

/// 读出溢出记录时 WAL 损坏的处置：参数是原因。进程里由启动流程给定，以退出码 3 退出
/// （REQ-AMB-ING-HOT-BACKPRESSURE-V2「溢出记录读不出时」）。
pub type CorruptionHandler = Arc<dyn Fn(&str) + Send + Sync>;

/// 物化的触发与编排；同时是快照提交对外的 [`Committer`]。
pub struct Flusher {
    committer: Arc<StoreCommitter>,
    view: Arc<dyn MemtableView>,
    writer: Arc<dyn MemtableWriter>,
    generations: Arc<dyn MemtableGenerations>,
    config: FlushConfig,
    slots: Mutex<HashMap<Dataset, Arc<Slot>>>,
    /// [`Flusher::spawn`] 之后：请调度任务为某数据集起提交任务。
    starter: Mutex<Option<tokio::sync::mpsc::UnboundedSender<Dataset>>>,
    /// 同时进行的 Parquet 编码的上限（可用核数）。
    encoders: Arc<tokio::sync::Semaphore>,
    totals: Mutex<Totals>,
    observer: Option<CommitObserver>,
    on_corrupt: Option<CorruptionHandler>,
}

#[derive(Default)]
struct Totals {
    by_dataset: HashMap<Dataset, CommitTotals>,
    /// 本个 `flush.interval` 内已计过失败的数据集。
    failed_this_interval: HashSet<Dataset>,
    /// 最近一次写入提交或创世快照的提交因对象存储不可访问、WAL 写入失败等没做成、这两种提交尚未再成功的数据集：
    /// 每个周期计一次失败。
    stuck: HashSet<Dataset>,
}

/// 一个数据集的 flush 状态。
#[derive(Default)]
struct Slot {
    /// 尚未冻结的积压条数；写入、冻结与退回都在这把锁下，计数与可见集一致。
    active: Mutex<u64>,
    /// 同一数据集的 flush 串行。
    work: tokio::sync::Mutex<Work>,
    /// 唤醒该数据集的提交任务（周期节拍或积压达到上限）。
    wake: tokio::sync::Notify,
    /// 该数据集的提交任务已起（或已请调度任务起）。
    started: AtomicBool,
}

#[derive(Default)]
struct Work {
    /// 已冻结、尚未提交成功的一代（一次写入提交的内容）。
    inflight: Option<Batch>,
}

struct Batch {
    generation: FrozenGeneration,
    /// 物化出的数据文件；物化之前为 `None`。
    files: Option<Vec<DataFile>>,
    progress: CommitProgress,
}

impl Slot {
    /// 有已确认尚未提交的记录（尚未冻结的积压或已冻结的一代）。正在 flush 时视为有。
    fn backlog(&self) -> bool {
        match self.work.try_lock() {
            Ok(work) => work.inflight.is_some() || *lock(&self.active) > 0,
            Err(_) => true,
        }
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("flush 状态锁未中毒")
}

impl Flusher {
    /// 以同一个可见集的三个侧面构造。
    pub fn new<M>(committer: Arc<StoreCommitter>, memtable: Arc<M>, config: FlushConfig) -> Flusher
    where
        M: MemtableView + MemtableWriter + MemtableGenerations + 'static,
    {
        let cores = std::thread::available_parallelism().map_or(1, |n| n.get());
        Flusher {
            committer,
            view: memtable.clone(),
            writer: memtable.clone(),
            generations: memtable,
            config,
            slots: Mutex::default(),
            starter: Mutex::default(),
            encoders: Arc::new(tokio::sync::Semaphore::new(cores)),
            totals: Mutex::default(),
            observer: None,
            on_corrupt: None,
        }
    }

    /// 设定提交结果的观察者（如 `/metrics` 的计数器）。
    pub fn with_commit_observer(mut self, observer: CommitObserver) -> Flusher {
        self.observer = Some(observer);
        self
    }

    /// 设定读出溢出记录时 WAL 损坏的处置（进程里以退出码 3 退出）。不设定时该次 flush 失败，含这些溢出记录的一代
    /// 退回可见集之首，此后该数据集的任何记录都不越过它提交。
    pub fn with_corruption_handler(mut self, handler: CorruptionHandler) -> Flusher {
        self.on_corrupt = Some(handler);
        self
    }

    pub fn committer(&self) -> &Arc<StoreCommitter> {
        &self.committer
    }

    fn slot(&self, dataset: &Dataset) -> Arc<Slot> {
        Arc::clone(lock(&self.slots).entry(dataset.clone()).or_default())
    }

    /// 各数据集按提交结果累计的次数。
    pub fn commit_totals(&self) -> HashMap<Dataset, CommitTotals> {
        lock(&self.totals).by_dataset.clone()
    }

    /// 写入提交或创世快照的提交成功：计一次成功，按周期的失败计数到此为止（REQ-AMB-OBS-HEALTH-METRICS-V4）。
    fn record_commit_ok(&self, dataset: &Dataset) {
        lock(&self.totals).stuck.remove(dataset);
        self.record_ok(dataset);
    }

    /// 一次提交成功：计一次成功。后台任务（合并、巡检留痕、入湖交接、归档等）的提交成功只走这里，不使按周期的
    /// 失败计数停止——那一条只看写入提交与创世快照的提交（REQ-AMB-OBS-HEALTH-METRICS-V4）。
    pub(crate) fn record_ok(&self, dataset: &Dataset) {
        lock(&self.totals)
            .by_dataset
            .entry(dataset.clone())
            .or_default()
            .ok += 1;
        if let Some(observe) = &self.observer {
            observe(dataset, true);
        }
    }

    pub(crate) fn record_failed(&self, dataset: &Dataset) {
        {
            let mut t = lock(&self.totals);
            if !t.failed_this_interval.insert(dataset.clone()) {
                return;
            }
            t.by_dataset.entry(dataset.clone()).or_default().failed += 1;
        }
        if let Some(observe) = &self.observer {
            observe(dataset, false);
        }
    }

    /// 后台任务（入湖交接与补做、TTL 热层、归档与冷层变更段）的一轮提交以对象存储不可访问结束：就地计一次失败，
    /// 与按周期的计数互不去重（REQ-AMB-OBS-HEALTH-METRICS-V4）。
    pub(crate) fn record_background_unavailable(&self, dataset: &Dataset) {
        lock(&self.totals)
            .by_dataset
            .entry(dataset.clone())
            .or_default()
            .failed += 1;
        if let Some(observe) = &self.observer {
            observe(dataset, false);
        }
    }

    /// 一次提交没做成：计一次失败；数据集仍为正常状态（不是因停用而放弃）时，此后每个周期再计一次，直到成功。
    fn record_stuck(&self, dataset: &Dataset) {
        if self.committer.chain().gates.state(dataset) == DatasetState::Normal {
            lock(&self.totals).stuck.insert(dataset.clone());
        }
        self.record_failed(dataset);
    }

    /// 新周期开始：清掉上个周期的去重，为卡住的数据集与停用且有未提交记录的数据集各计一次失败。
    fn tick_failures(&self) {
        let gates = &self.committer.chain().gates;
        let stuck: Vec<Dataset> = {
            let mut t = lock(&self.totals);
            t.failed_this_interval.clear();
            // 进入停用即不再按「卡住」计，改由停用一条计
            t.stuck.retain(|d| gates.state(d) == DatasetState::Normal);
            t.stuck.iter().cloned().collect()
        };
        for d in &stuck {
            self.record_failed(d);
        }
        let slots: Vec<(Dataset, Arc<Slot>)> = lock(&self.slots)
            .iter()
            .map(|(d, s)| (d.clone(), Arc::clone(s)))
            .collect();
        for (d, slot) in slots {
            if gates.state(&d) != DatasetState::Normal && slot.backlog() {
                self.record_failed(&d);
            }
        }
    }

    /// 已知的全部数据集：存在性登记表中的与写入过的。
    fn datasets(&self) -> HashSet<Dataset> {
        let mut all: HashSet<Dataset> = self
            .committer
            .chain()
            .registry
            .datasets()
            .into_iter()
            .collect();
        all.extend(lock(&self.slots).keys().cloned());
        all
    }

    /// 启动按周期与按记录数的 flush。返回的任务一直运行，停机时由调用方在 [`Committer::flush_all`] 之前中止；
    /// 各数据集的提交任务归它所有，随它一并中止。
    pub fn spawn(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
        let me = Arc::clone(self);
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        *lock(&self.starter) = Some(tx);
        tokio::spawn(async move {
            let period = me.config.interval;
            let start = tokio::time::Instant::now() + period;
            // 按周期的失败计数单独走一个节拍：一个数据集的 flush 可能因重试与退避跨过好几个周期
            let counting = async {
                let mut tick = tokio::time::interval_at(start, period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tick.tick().await;
                    me.tick_failures();
                }
            };
            let scheduling = async {
                let mut workers = tokio::task::JoinSet::new();
                let mut started = HashSet::new();
                let mut tick = tokio::time::interval_at(start, period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tokio::select! {
                        _ = tick.tick() => {
                            for d in me.datasets() {
                                me.start_worker(&mut workers, &mut started, &d);
                                me.slot(&d).wake.notify_one();
                            }
                        }
                        Some(d) = rx.recv() => me.start_worker(&mut workers, &mut started, &d),
                    }
                }
            };
            tokio::join!(counting, scheduling);
        })
    }

    /// 为数据集起提交任务（已起则不做）：每次被唤醒，补完待办的创世快照，再 flush 一次。
    fn start_worker(
        self: &Arc<Self>,
        workers: &mut tokio::task::JoinSet<()>,
        started: &mut HashSet<Dataset>,
        dataset: &Dataset,
    ) {
        if !started.insert(dataset.clone()) {
            return;
        }
        let slot = self.slot(dataset);
        slot.started.store(true, Ordering::SeqCst);
        let me = Arc::clone(self);
        let dataset = dataset.clone();
        workers.spawn(async move {
            loop {
                slot.wake.notified().await;
                me.settle(&dataset).await;
                me.flush_dataset(&dataset).await;
            }
        });
    }

    async fn settle(&self, dataset: &Dataset) {
        match self.committer.settle(dataset).await {
            Some(Ok(_)) => self.record_commit_ok(dataset),
            Some(Err(_)) => self.record_stuck(dataset),
            None => {}
        }
    }

    /// 把冻结代退回可见集之首，计回尚未冻结的积压。
    fn thaw(&self, dataset: &Dataset, slot: &Slot, generation: &FrozenGeneration) {
        let mut active = lock(&slot.active);
        self.generations.thaw(dataset, generation.id);
        *active += generation.records;
    }

    /// 为数据集 flush 一次：逐次写入提交，直到开始时已有的积压都已提交，且余下的不足一次的上限。
    pub async fn flush_dataset(&self, dataset: &Dataset) -> FlushOutcome {
        let slot = self.slot(dataset);
        let mut work = slot.work.lock().await;
        let state = self.committer.chain().gates.state(dataset);
        if state != DatasetState::Normal {
            // 停用之后不冻结、不物化、不上传；有未提交的记录即按周期记一次失败
            if work.inflight.is_none() && *lock(&slot.active) == 0 {
                return FlushOutcome::Idle;
            }
            self.record_failed(dataset);
            return FlushOutcome::Failed(FlushFailure::Commit(CommitError::Disabled(state)));
        }
        let max = self.config.max_records;
        // 开始时已有的积压：这些都要在本次提交
        let mut owed =
            *lock(&slot.active) + work.inflight.as_ref().map_or(0, |b| b.generation.records);
        let mut outcome = FlushOutcome::Idle;
        loop {
            if work.inflight.is_none() {
                let frozen = {
                    let mut active = lock(&slot.active);
                    if *active == 0 || (owed == 0 && *active < max) {
                        return outcome;
                    }
                    let Some(g) = self.generations.freeze(dataset, max) else {
                        *active = 0;
                        return outcome;
                    };
                    *active = active.saturating_sub(g.records);
                    g
                };
                work.inflight = Some(Batch {
                    generation: frozen,
                    files: None,
                    progress: CommitProgress::default(),
                });
            }
            let batch = work.inflight.as_mut().expect("刚定下");
            if batch.files.is_none() {
                match self.materialize(dataset, &batch.generation).await {
                    Ok(files) => batch.files = Some(files),
                    Err(e) => {
                        // 留在队首：退回可见集之首，其后的记录不越过它提交
                        let batch = work.inflight.take().expect("刚定下");
                        self.thaw(dataset, &slot, &batch.generation);
                        self.record_failed(dataset);
                        return FlushOutcome::Failed(FlushFailure::Materialize(e));
                    }
                }
            }
            let g = &batch.generation;
            let generations = Arc::clone(&self.generations);
            let (d, ids) = (dataset.clone(), vec![g.id]);
            let request = CommitRequest {
                files: batch.files.clone().expect("已物化"),
                through: g.last,
                through_partial: g.last_partial,
                bind: Some(Box::new(move |p| generations.bind(&d, &ids, p))),
            };
            match self
                .committer
                .commit_with(dataset, &request, &mut batch.progress)
                .await
            {
                Ok(p) => {
                    self.generations.retire(dataset, &[g.id]);
                    owed = owed.saturating_sub(g.records);
                    work.inflight = None;
                    self.record_commit_ok(dataset);
                    outcome = FlushOutcome::Committed(p);
                    // 一次写入提交成功（快照已创建、额度已归还）之后、开始下一次之前的测试暂停点（REQ-AMB-ING-HOT-BACKPRESSURE-V5）
                    fault::gate("flush.after_write_commit").await;
                }
                Err(e) => {
                    let full = g.records >= max || g.spill_capped;
                    if !batch.progress.intent_written() && !full {
                        // 没有发出过快照创建、也不足一次的上限：退回，下一次按当时的积压重新冻结
                        let batch = work.inflight.take().expect("刚定下");
                        self.thaw(dataset, &slot, &batch.generation);
                    }
                    self.record_stuck(dataset);
                    return FlushOutcome::Failed(FlushFailure::Commit(e));
                }
            }
        }
    }

    /// 把冻结代物化成待提交的数据文件，按小时桶拆分。溢出记录从 WAL 读回（REQ-AMB-ING-HOT-BACKPRESSURE），读回所用内存
    /// 是物化的临时占用；编码在阻塞线程池里做。
    async fn materialize(
        &self,
        dataset: &Dataset,
        frozen: &FrozenGeneration,
    ) -> Result<Vec<DataFile>, FlushError> {
        let columns = self
            .view
            .schema(dataset)
            .unwrap_or_else(StreamColumns::genesis);
        let mut batches = frozen.batches.clone();
        if !frozen.spilled.is_empty() {
            let positions: Vec<WalPosition> = frozen.spilled.iter().map(|s| s.position).collect();
            let entries = match self.committer.wal().read(positions).await {
                Ok(entries) => entries,
                Err(e) => {
                    let why = format!("cannot read spilled records of {dataset} back: {e}");
                    // 读不出的已确认记录不跳过：交给处置（以退出码 3 退出），不在本进程里继续提交
                    if let (WalError::Corrupt(_), Some(handle)) = (&e, &self.on_corrupt) {
                        handle(&why);
                    }
                    return Err(FlushError(why));
                }
            };
            let by_position: HashMap<WalPosition, Vec<Record>> = entries
                .into_iter()
                .filter_map(|(p, entry)| match entry {
                    WalEntry::Records { records, .. } => Some((p, records)),
                    _ => None,
                })
                .collect();
            for s in &frozen.spilled {
                let records = by_position
                    .get(&s.position)
                    .and_then(|r| r.get(s.skip as usize..(s.skip + s.count) as usize))
                    .ok_or_else(|| {
                        FlushError(format!(
                            "WAL position {} of {dataset} does not hold the spilled records",
                            s.position.0
                        ))
                    })?;
                batches.push(records_to_batch(&columns, records));
            }
        }
        let permit = Arc::clone(&self.encoders)
            .acquire_owned()
            .await
            .map_err(|e| FlushError(e.to_string()))?;
        tokio::task::spawn_blocking(move || {
            let _permit = permit;
            write_data_files(&columns, &batches, &BloomColumns::none())
        })
        .await
        .map_err(|e| FlushError(format!("encoding task failed: {e}")))?
    }

    /// 为全部数据集补完待办的创世快照，再各 flush 一次（各数据集并发）。
    async fn flush_all_datasets(&self) {
        let all = self.datasets();
        futures::future::join_all(all.iter().map(|d| async move {
            self.settle(d).await;
            self.flush_dataset(d).await
        }))
        .await;
    }
}

impl Flusher {
    /// 在该数据集的计数锁内执行写入 `write`，尚未冻结的积压加 `n` 条，达到 `flush.max_records` 即唤醒它的提交任务。
    fn counted(&self, dataset: &Dataset, n: u64, write: impl FnOnce()) {
        let slot = self.slot(dataset);
        let due = {
            let mut active = lock(&slot.active);
            write();
            *active += n;
            *active >= self.config.max_records
        };
        if due {
            slot.wake.notify_one();
            if !slot.started.load(Ordering::SeqCst)
                && let Some(tx) = lock(&self.starter).as_ref()
            {
                let _ = tx.send(dataset.clone());
            }
        }
    }
}

/// 写入一侧的包装：转交给可见集，并按数据集计尚未冻结的积压条数，达到 `flush.max_records` 即安排 flush。
impl MemtableWriter for Flusher {
    fn register(&self, dataset: &Dataset, baseline: StreamColumns) {
        self.writer.register(dataset, baseline);
    }

    fn write(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.counted(dataset, records.len() as u64, || {
            self.writer.write(dataset, position, records)
        });
    }

    fn write_resident(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.counted(dataset, records.len() as u64, || {
            self.writer.write_resident(dataset, position, records)
        });
    }

    fn write_tail(
        &self,
        dataset: &Dataset,
        position: WalPosition,
        skip: u32,
        records: Vec<Record>,
    ) {
        self.counted(dataset, records.len() as u64, || {
            self.writer.write_tail(dataset, position, skip, records)
        });
    }

    fn reserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        self.writer.reserve(dataset, columns);
    }

    fn unreserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        self.writer.unreserve(dataset, columns);
    }
}

impl Committer for Flusher {
    fn load(&self) -> BoxFuture<'_, Result<Vec<Baseline>, Unavailable>> {
        Box::pin(self.committer.load())
    }

    fn resolve_intent(
        &self,
        intent: CommitIntent,
    ) -> BoxFuture<'_, Result<IntentOutcome, Unavailable>> {
        Box::pin(async move { self.committer.resolve_intent(intent).await })
    }

    fn ensure_dataset<'a>(&'a self, dataset: &'a Dataset) -> BoxFuture<'a, Existence> {
        Box::pin(async move {
            let chain = self.committer.chain();
            let settled = chain.registry.persistence(dataset) == Some(Persistence::Stored)
                && self.committer.has_snapshot(dataset);
            let existence = self.committer.ensure_dataset(dataset).await;
            if !settled {
                // 这次做了一次创世快照提交的尝试
                match existence {
                    Existence::Committed => self.record_commit_ok(dataset),
                    Existence::Pending => self.record_stuck(dataset),
                }
            }
            existence
        })
    }

    fn flush_all(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.flush_all_datasets())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gate::DatasetGates;
    use crate::memtable::Memtable;
    use crate::reader::StoreChain;
    use crate::registry::DatasetRegistry;
    use crate::store::{Location, S3Options, Store};
    use crate::testing::FakeWal;

    fn flusher(dir: &std::path::Path) -> Flusher {
        std::fs::create_dir_all(dir).unwrap();
        let store = Store::open(
            &Location::File {
                dir: dir.to_path_buf(),
            },
            &S3Options::default(),
            "amberlog",
        )
        .unwrap();
        let chain = Arc::new(StoreChain::new(
            store,
            Arc::new(DatasetRegistry::new()),
            Arc::new(DatasetGates::new()),
        ));
        let committer = Arc::new(StoreCommitter::new(chain, Arc::new(FakeWal::new()), 8));
        Flusher::new(
            committer,
            Arc::new(Memtable::new()),
            FlushConfig {
                interval: Duration::from_secs(3600),
                max_records: 1_000_000,
            },
        )
    }

    /// 卡住的数据集：后台提交成功不使按周期的失败计数停止，写入提交或创世快照的提交成功才停止。
    #[test]
    fn background_ok_keeps_counting_stuck_dataset() {
        let dir = std::env::temp_dir().join(format!(
            "amberlog-flusher-{}-{}",
            std::process::id(),
            rand::random::<u64>()
        ));
        let f = flusher(&dir);
        let d = Dataset::new("default", "app");
        let failed = |f: &Flusher| f.commit_totals()[&d].failed;

        f.record_stuck(&d);
        assert_eq!(failed(&f), 1);
        f.record_ok(&d);
        f.tick_failures();
        assert_eq!(failed(&f), 2, "后台提交成功之后仍按周期计");
        f.record_commit_ok(&d);
        f.tick_failures();
        assert_eq!(failed(&f), 2, "写入提交成功之后不再计");
        assert_eq!(f.commit_totals()[&d].ok, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
