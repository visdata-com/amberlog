//! 测试用替身：[`Wal`]、[`MemtableView`]、[`Committer`]、[`ChainReader`]、[`SegmentReader`]、[`HotCommits`]、[`ColdReader`]
//! 与 [`ColdWriter`] 各一个内存实现。
//!
//! 只在本 crate 的测试与打开 `testing` feature 的构建里编入；其他 crate 在 `[dev-dependencies]` 里打开它。
//! 替身不实现任何块的行为（不校验、不持久化、不判明），只按测试预先设定的内容作答并记下调用，
//! 让调用方的测试不依赖真实实现。

use std::collections::{HashMap, HashSet, VecDeque};
use std::future::ready;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::Record;
use amberlog_common::schema::ColumnType;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

use crate::BoxFuture;
use crate::chain::{
    BucketManifest, BucketRef, ChainError, ChainReader, ColdChangeSegment, HandoverDraft,
    RetiredSegment, SegmentReader, Snapshot, SnapshotId,
};
use crate::cold::{
    ColdAppend, ColdBatches, ColdDataFile, ColdDelete, ColdDeleted, ColdExpire, ColdExpired,
    ColdReadError, ColdReader, ColdReplace, ColdReplaced, ColdScope, ColdState, ColdTable,
    ColdUpkeep, ColdWriteError, ColdWriter,
};
use crate::commit::{
    ArchiveRequest, Baseline, ColdChangeRequest, CommitIntent, Committed, Committer, Existence,
    HandoverRequest, HotCommits, IntentOutcome, TtlHotRequest,
};
use crate::committer::CommitError;
use crate::in_use::{InUseSnapshots, SnapshotPin};
use crate::memtable::{
    Binding, CandidateGeneration, Candidates, FrozenGeneration, MemtableGenerations, MemtableView,
    MemtableWriter,
};
use crate::schema::{SchemaColumn, StreamColumns};
use crate::store::Unavailable;
use crate::wal::{Release, Wal, WalEntry, WalError, WalPosition};

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("替身的锁未中毒")
}

// ---------------------------------------------------------------------------
// WAL
// ---------------------------------------------------------------------------

/// 内存里的 WAL：追加的条目按位置保存，重放原样交出；可设为写入失败。
#[derive(Debug, Default)]
pub struct FakeWal {
    inner: Mutex<FakeWalInner>,
}

#[derive(Debug, Default)]
struct FakeWalInner {
    entries: Vec<(WalPosition, WalEntry)>,
    failing: bool,
    releases: Vec<Release>,
}

impl FakeWal {
    pub fn new() -> Self {
        Self::default()
    }

    /// 预置重放内容（位置自 1 起连续）。
    pub fn with_entries(entries: Vec<WalEntry>) -> Self {
        let wal = Self::new();
        lock(&wal.inner).entries = entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| (WalPosition(i as u64 + 1), e))
            .collect();
        wal
    }

    /// 此后的追加一律以 [`WalError::Write`] 失败，失败的条目不保存。
    pub fn set_failing(&self, failing: bool) {
        lock(&self.inner).failing = failing;
    }

    /// 已保存的全部条目。
    pub fn entries(&self) -> Vec<(WalPosition, WalEntry)> {
        lock(&self.inner).entries.clone()
    }

    /// 收到的段回收通知，按收到的顺序。
    pub fn releases(&self) -> Vec<Release> {
        lock(&self.inner).releases.clone()
    }
}

impl Wal for FakeWal {
    fn append(&self, entries: Vec<WalEntry>) -> BoxFuture<'_, Result<WalPosition, WalError>> {
        assert!(!entries.is_empty(), "追加的条目不得为空");
        let mut inner = lock(&self.inner);
        let result = if inner.failing {
            Err(WalError::Write("FakeWal set to fail".into()))
        } else {
            let mut last = inner.entries.last().map_or(0, |(p, _)| p.0);
            for entry in entries {
                last += 1;
                inner.entries.push((WalPosition(last), entry));
            }
            Ok(WalPosition(last))
        };
        Box::pin(ready(result))
    }

    fn replay(&self, sink: &mut dyn FnMut(WalPosition, WalEntry)) -> Result<(), WalError> {
        for (position, entry) in self.entries() {
            sink(position, entry);
        }
        Ok(())
    }

    fn release(&self, release: Release) {
        lock(&self.inner).releases.push(release);
    }

    fn read(
        &self,
        positions: Vec<WalPosition>,
    ) -> BoxFuture<'_, Result<Vec<(WalPosition, WalEntry)>, WalError>> {
        let found: Vec<(WalPosition, WalEntry)> = self
            .entries()
            .into_iter()
            .filter(|(p, _)| positions.contains(p))
            .collect();
        Box::pin(ready(Ok(found)))
    }
}

// ---------------------------------------------------------------------------
// 可见集
// ---------------------------------------------------------------------------

/// 代编号；原先定义在这里，现与接口共用 [`crate::memtable::GenerationId`]。
pub use crate::memtable::GenerationId;

/// 内存里的可见集：由测试直接登记数据集、推入代、绑定与移除；也实现写入与物化两侧的接口，
/// 只按下面各 `impl` 注释所述的简化方式作答并记下调用。
#[derive(Debug, Default)]
pub struct FakeMemtable {
    streams: Mutex<HashMap<Dataset, FakeStream>>,
    written: Mutex<Vec<Written>>,
    retired: Mutex<Vec<(Dataset, GenerationId)>>,
}

#[derive(Debug)]
struct FakeStream {
    columns: StreamColumns,
    /// 在途登记、尚未确认的列与其引用数（同 [`crate::memtable::Memtable`]）。
    pending: HashMap<String, usize>,
    generations: Vec<(GenerationId, Arc<Binding>, Vec<RecordBatch>)>,
    next: u64,
    /// [`FakeMemtable::stage_frozen`] 排入、尚未被 [`MemtableGenerations::freeze`] 取走的代。
    staged: VecDeque<FrozenGeneration>,
    /// 已被 `freeze` 取走、尚未回执的代（`thaw` 据此放回）。
    frozen: Vec<FrozenGeneration>,
}

impl FakeStream {
    fn new(columns: StreamColumns) -> FakeStream {
        FakeStream {
            columns,
            pending: HashMap::new(),
            generations: Vec::new(),
            next: 0,
            staged: VecDeque::new(),
            frozen: Vec::new(),
        }
    }
}

impl FakeMemtable {
    pub fn new() -> Self {
        Self::default()
    }

    /// 把数据集放进进程内目录，当前 schema 为 `columns`；已存在时只替换 schema。
    pub fn add_dataset(&self, dataset: &Dataset, columns: StreamColumns) {
        let mut streams = lock(&self.streams);
        match streams.get_mut(dataset) {
            Some(s) => s.columns = columns,
            None => {
                streams.insert(dataset.clone(), FakeStream::new(columns));
            }
        }
    }

    /// 推入一个未绑定的代。数据集须已登记。
    pub fn push_generation(&self, dataset: &Dataset, batches: Vec<RecordBatch>) -> GenerationId {
        let mut streams = lock(&self.streams);
        let s = streams.get_mut(dataset).expect("先 add_dataset");
        let id = GenerationId(s.next);
        s.next += 1;
        s.generations
            .push((id, Arc::new(Binding::default()), batches));
        id
    }

    /// 把代绑定到快照号 `p`（模拟快照提交在条件写之前的绑定）。
    pub fn bind(&self, dataset: &Dataset, generation: GenerationId, p: SnapshotId) {
        self.with_generation(dataset, generation, |b| b.bind(p));
    }

    /// 移除一代（模拟回执之后的移除）；已取出的候选视图不受影响。
    pub fn remove(&self, dataset: &Dataset, generation: GenerationId) {
        if let Some(s) = lock(&self.streams).get_mut(dataset) {
            s.generations.retain(|(id, _, _)| *id != generation);
        }
    }

    fn with_generation(
        &self,
        dataset: &Dataset,
        generation: GenerationId,
        f: impl FnOnce(&Binding),
    ) {
        let streams = lock(&self.streams);
        let (_, binding, _) = streams
            .get(dataset)
            .and_then(|s| s.generations.iter().find(|(id, _, _)| *id == generation))
            .expect("代存在");
        f(binding);
    }
}

impl MemtableView for FakeMemtable {
    fn schema(&self, dataset: &Dataset) -> Option<StreamColumns> {
        lock(&self.streams).get(dataset).map(|s| s.columns.clone())
    }

    fn candidates(&self, dataset: &Dataset) -> Candidates {
        let streams = lock(&self.streams);
        let generations = streams.get(dataset).map_or_else(Vec::new, |s| {
            s.generations
                .iter()
                .map(|(_, binding, batches)| {
                    CandidateGeneration::new(Arc::clone(binding), batches.clone())
                })
                .collect()
        });
        Candidates::new(generations)
    }
}

/// [`FakeMemtable`] 收到的一次 [`MemtableWriter::write`]。
#[derive(Debug, Clone, PartialEq)]
pub struct Written {
    pub dataset: Dataset,
    pub position: WalPosition,
    pub records: Vec<Record>,
    /// 经 [`MemtableWriter::write_resident`] 写入。
    pub resident: bool,
    /// 经 [`MemtableWriter::write_tail`] 写入时为其 `skip`，否则为 0。
    pub skip: u32,
}

impl FakeMemtable {
    /// 收到的写入，按顺序。
    pub fn written(&self) -> Vec<Written> {
        lock(&self.written).clone()
    }

    /// 排入一个待冻结的代：推入一个未绑定的代（参与候选视图），下一次 `freeze` 按排入顺序把它交出。数据集须已登记。
    pub fn stage_frozen(
        &self,
        dataset: &Dataset,
        first: WalPosition,
        last: WalPosition,
        batches: Vec<RecordBatch>,
    ) -> GenerationId {
        let id = self.push_generation(dataset, batches.clone());
        let mut streams = lock(&self.streams);
        let s = streams.get_mut(dataset).expect("先 add_dataset");
        let records = batches.iter().map(|b| b.num_rows() as u64).sum();
        s.staged.push_back(FrozenGeneration {
            id,
            first,
            last,
            last_partial: None,
            records,
            batches,
            spilled: Vec::new(),
            spill_capped: false,
        });
        id
    }

    /// 记下一次写入；新列按出现顺序取最大字段 id + 1 并入 schema。
    fn record_write(
        &self,
        dataset: &Dataset,
        position: WalPosition,
        records: Vec<Record>,
        resident: bool,
    ) {
        {
            let mut streams = lock(&self.streams);
            let s = streams
                .entry(dataset.clone())
                .or_insert_with(|| FakeStream::new(StreamColumns::genesis()));
            let mut columns = s.columns.columns().to_vec();
            for record in &records {
                for (name, value) in &record.fields {
                    s.pending.remove(name);
                    if !columns.iter().any(|c| &c.name == name) {
                        let field_id = columns.iter().map(|c| c.field_id).max().unwrap_or(0) + 1;
                        columns.push(SchemaColumn {
                            name: name.clone(),
                            column_type: value.column_type(),
                            field_id,
                        });
                    }
                }
            }
            s.columns = StreamColumns::from_columns(columns);
        }
        lock(&self.written).push(Written {
            dataset: dataset.clone(),
            position,
            records,
            resident,
            skip: 0,
        });
    }

    /// 收到回执而移除的代，按顺序。
    pub fn retired(&self) -> Vec<(Dataset, GenerationId)> {
        lock(&self.retired).clone()
    }
}

/// 替身的写入：记下写入；新列按出现顺序取最大字段 id + 1 并入 schema。不生成代，候选视图只含测试推入的代。
impl MemtableWriter for FakeMemtable {
    fn register(&self, dataset: &Dataset, baseline: StreamColumns) {
        lock(&self.streams)
            .entry(dataset.clone())
            .or_insert_with(|| FakeStream::new(baseline));
    }

    fn write(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.record_write(dataset, position, records, false);
    }

    fn write_resident(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.record_write(dataset, position, records, true);
    }

    fn write_tail(
        &self,
        dataset: &Dataset,
        position: WalPosition,
        skip: u32,
        records: Vec<Record>,
    ) {
        self.record_write(dataset, position, records, false);
        if let Some(w) = lock(&self.written).last_mut() {
            w.skip = skip;
        }
    }

    fn reserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        let mut streams = lock(&self.streams);
        let s = streams
            .entry(dataset.clone())
            .or_insert_with(|| FakeStream::new(StreamColumns::genesis()));
        let mut all = s.columns.columns().to_vec();
        for (name, column_type) in columns {
            if let Some(refs) = s.pending.get_mut(name) {
                *refs += 1;
            } else if s.columns.get(name).is_none() {
                s.pending.insert(name.clone(), 1);
                let field_id = all.iter().map(|c| c.field_id).max().unwrap_or(0) + 1;
                all.push(SchemaColumn {
                    name: name.clone(),
                    column_type: *column_type,
                    field_id,
                });
            }
        }
        s.columns = StreamColumns::from_columns(all);
    }

    fn unreserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        let mut streams = lock(&self.streams);
        let Some(s) = streams.get_mut(dataset) else {
            return;
        };
        for (name, _) in columns {
            if let Some(refs) = s.pending.get_mut(name) {
                *refs -= 1;
                if *refs == 0 {
                    s.pending.remove(name);
                    let kept = s
                        .columns
                        .columns()
                        .iter()
                        .filter(|c| &c.name != name)
                        .cloned()
                        .collect();
                    s.columns = StreamColumns::from_columns(kept);
                }
            }
        }
    }
}

/// 替身的物化一侧：`freeze` 按排入顺序逐个交出 [`FakeMemtable::stage_frozen`] 排入的代（不论条数上限）、`thaw` 把它放回队首；
/// `bind` 与 `retire` 作用于同编号的代。
impl MemtableGenerations for FakeMemtable {
    fn freeze(&self, dataset: &Dataset, _max_records: u64) -> Option<FrozenGeneration> {
        let mut streams = lock(&self.streams);
        let s = streams.get_mut(dataset)?;
        let g = s.staged.pop_front()?;
        s.frozen.push(g.clone());
        Some(g)
    }

    fn thaw(&self, dataset: &Dataset, generation: GenerationId) {
        let mut streams = lock(&self.streams);
        if let Some(s) = streams.get_mut(dataset)
            && let Some(i) = s.frozen.iter().position(|g| g.id == generation)
        {
            let g = s.frozen.remove(i);
            s.staged.push_front(g);
        }
    }

    fn bind(&self, dataset: &Dataset, generations: &[GenerationId], p: SnapshotId) {
        for g in generations {
            self.with_generation(dataset, *g, |b| b.bind(p));
        }
    }

    fn retire(&self, dataset: &Dataset, generations: &[GenerationId]) {
        for g in generations {
            if let Some(s) = lock(&self.streams).get_mut(dataset) {
                s.frozen.retain(|f| f.id != *g);
            }
            self.remove(dataset, *g);
            lock(&self.retired).push((dataset.clone(), *g));
        }
    }
}

// ---------------------------------------------------------------------------
// 快照提交
// ---------------------------------------------------------------------------

/// 按预设作答的快照提交：`load` 默认返回空列表，`ensure_dataset` 默认 [`Existence::Committed`]，
/// `resolve_intent` 默认 [`IntentOutcome::Absent`]。
#[derive(Debug, Default)]
pub struct FakeCommitter {
    inner: Mutex<FakeCommitterInner>,
}

#[derive(Debug, Default)]
struct FakeCommitterInner {
    baselines: Option<Result<Vec<Baseline>, Unavailable>>,
    pending: Vec<Dataset>,
    outcomes: HashMap<(Dataset, SnapshotId), IntentOutcome>,
    ensured: Vec<Dataset>,
    resolved: Vec<CommitIntent>,
    flushes: usize,
}

impl FakeCommitter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_baselines(&self, baselines: Result<Vec<Baseline>, Unavailable>) {
        lock(&self.inner).baselines = Some(baselines);
    }

    /// 此后对 `dataset` 的 `ensure_dataset` 返回 [`Existence::Pending`]（`false` 恢复为 `Committed`）。
    pub fn set_pending(&self, dataset: &Dataset, pending: bool) {
        let mut inner = lock(&self.inner);
        inner.pending.retain(|d| d != dataset);
        if pending {
            inner.pending.push(dataset.clone());
        }
    }

    pub fn set_intent_outcome(&self, dataset: &Dataset, p: SnapshotId, outcome: IntentOutcome) {
        lock(&self.inner)
            .outcomes
            .insert((dataset.clone(), p), outcome);
    }

    /// `ensure_dataset` 的调用，按顺序。
    pub fn ensured(&self) -> Vec<Dataset> {
        lock(&self.inner).ensured.clone()
    }

    /// `resolve_intent` 收到的意图，按顺序。
    pub fn resolved(&self) -> Vec<CommitIntent> {
        lock(&self.inner).resolved.clone()
    }

    /// `flush_all` 的调用次数。
    pub fn flushes(&self) -> usize {
        lock(&self.inner).flushes
    }
}

impl Committer for FakeCommitter {
    fn load(&self) -> BoxFuture<'_, Result<Vec<Baseline>, Unavailable>> {
        let result = lock(&self.inner)
            .baselines
            .clone()
            .unwrap_or(Ok(Vec::new()));
        Box::pin(ready(result))
    }

    fn resolve_intent(
        &self,
        intent: CommitIntent,
    ) -> BoxFuture<'_, Result<IntentOutcome, Unavailable>> {
        let mut inner = lock(&self.inner);
        let outcome = inner
            .outcomes
            .get(&(intent.dataset.clone(), intent.snapshot_id))
            .copied()
            .unwrap_or(IntentOutcome::Absent);
        inner.resolved.push(intent);
        Box::pin(ready(Ok(outcome)))
    }

    fn ensure_dataset<'a>(&'a self, dataset: &'a Dataset) -> BoxFuture<'a, Existence> {
        let mut inner = lock(&self.inner);
        inner.ensured.push(dataset.clone());
        let existence = if inner.pending.contains(dataset) {
            Existence::Pending
        } else {
            Existence::Committed
        };
        Box::pin(ready(existence))
    }

    fn flush_all(&self) -> BoxFuture<'_, ()> {
        lock(&self.inner).flushes += 1;
        Box::pin(ready(()))
    }
}

// ---------------------------------------------------------------------------
// 快照链读取
// ---------------------------------------------------------------------------

/// 按预设作答的快照链：未预设的读取一律返回 [`ChainError::Integrity`]；不读闸门状态，也不写。
/// [`ChainReader::reading`] 登记进它自己的在用快照登记（[`FakeChain::in_use`]），供测试核对登记与注销。
#[derive(Debug, Default)]
pub struct FakeChain {
    datasets: Mutex<HashMap<Dataset, FakeChainDataset>>,
    reads: AtomicUsize,
    in_use: Arc<InUseSnapshots>,
}

#[derive(Debug, Default)]
struct FakeChainDataset {
    current: Option<Result<Option<SnapshotId>, ChainError>>,
    snapshots: HashMap<SnapshotId, Result<Arc<Snapshot>, ChainError>>,
    manifests: HashMap<String, Result<Arc<BucketManifest>, ChainError>>,
}

impl FakeChain {
    pub fn new() -> Self {
        Self::default()
    }

    /// 经 [`ChainReader::reading`] 所作的登记。
    pub fn in_use(&self) -> &Arc<InUseSnapshots> {
        &self.in_use
    }

    pub fn set_current(&self, dataset: &Dataset, current: Result<Option<SnapshotId>, ChainError>) {
        self.with_dataset(dataset, |d| d.current = Some(current));
    }

    /// 预置快照，按其 `snapshot_id` 取用。
    pub fn put_snapshot(&self, dataset: &Dataset, snapshot: Snapshot) {
        self.with_dataset(dataset, |d| {
            d.snapshots
                .insert(snapshot.snapshot_id, Ok(Arc::new(snapshot)));
        });
    }

    pub fn fail_snapshot(&self, dataset: &Dataset, id: SnapshotId, error: ChainError) {
        self.with_dataset(dataset, |d| {
            d.snapshots.insert(id, Err(error));
        });
    }

    /// 预置桶 manifest，按 [`BucketRef::manifest`] 路径取用。
    pub fn put_bucket_manifest(&self, dataset: &Dataset, path: &str, manifest: BucketManifest) {
        self.with_dataset(dataset, |d| {
            d.manifests.insert(path.to_owned(), Ok(Arc::new(manifest)));
        });
    }

    pub fn fail_bucket_manifest(&self, dataset: &Dataset, path: &str, error: ChainError) {
        self.with_dataset(dataset, |d| {
            d.manifests.insert(path.to_owned(), Err(error));
        });
    }

    /// 三个读取方法被调用的总次数（用来断言「不读对象存储」）。
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    fn with_dataset(&self, dataset: &Dataset, f: impl FnOnce(&mut FakeChainDataset)) {
        f(lock(&self.datasets).entry(dataset.clone()).or_default());
    }

    fn read<T: Clone>(
        &self,
        dataset: &Dataset,
        what: &str,
        pick: impl FnOnce(&FakeChainDataset) -> Option<&Result<T, ChainError>>,
    ) -> Result<T, ChainError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        lock(&self.datasets)
            .get(dataset)
            .and_then(pick)
            .cloned()
            .unwrap_or_else(|| {
                Err(ChainError::Integrity(format!(
                    "FakeChain: {what} of {dataset} not set"
                )))
            })
    }
}

impl ChainReader for FakeChain {
    fn current<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<SnapshotId>, ChainError>> {
        Box::pin(ready(
            self.read(dataset, "_current", |d| d.current.as_ref()),
        ))
    }

    fn snapshot<'a>(
        &'a self,
        dataset: &'a Dataset,
        id: SnapshotId,
    ) -> BoxFuture<'a, Result<Arc<Snapshot>, ChainError>> {
        Box::pin(ready(
            self.read(dataset, "snapshot", |d| d.snapshots.get(&id)),
        ))
    }

    fn bucket_manifest<'a>(
        &'a self,
        dataset: &'a Dataset,
        _snapshot: &'a Snapshot,
        bucket: &'a BucketRef,
    ) -> BoxFuture<'a, Result<Arc<BucketManifest>, ChainError>> {
        Box::pin(ready(self.read(dataset, "bucket manifest", |d| {
            d.manifests.get(&bucket.manifest)
        })))
    }

    fn reading(&self, dataset: &Dataset) -> SnapshotPin {
        self.in_use.begin(dataset, None)
    }
}

/// 按预设作答的段链读取：未预设的数据集读作两条链都为空。按数据集预设，不看 `snapshot`。
#[derive(Debug, Default)]
pub struct FakeSegments {
    datasets: Mutex<HashMap<Dataset, FakeSegmentChains>>,
}

#[derive(Debug, Default, Clone)]
struct FakeSegmentChains {
    retired: Option<Result<Vec<Arc<RetiredSegment>>, ChainError>>,
    cold: Option<Result<Vec<Arc<ColdChangeSegment>>, ChainError>>,
}

impl FakeSegments {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_retired(&self, dataset: &Dataset, chain: Result<Vec<RetiredSegment>, ChainError>) {
        lock(&self.datasets)
            .entry(dataset.clone())
            .or_default()
            .retired = Some(chain.map(|v| v.into_iter().map(Arc::new).collect()));
    }

    pub fn set_cold_changes(
        &self,
        dataset: &Dataset,
        chain: Result<Vec<ColdChangeSegment>, ChainError>,
    ) {
        lock(&self.datasets)
            .entry(dataset.clone())
            .or_default()
            .cold = Some(chain.map(|v| v.into_iter().map(Arc::new).collect()));
    }
}

impl SegmentReader for FakeSegments {
    fn retired_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        _snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<RetiredSegment>>, ChainError>> {
        let chains = lock(&self.datasets)
            .get(dataset)
            .cloned()
            .unwrap_or_default();
        Box::pin(ready(chains.retired.unwrap_or(Ok(Vec::new()))))
    }

    fn cold_change_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        _snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<ColdChangeSegment>>, ChainError>> {
        let chains = lock(&self.datasets)
            .get(dataset)
            .cloned()
            .unwrap_or_default();
        Box::pin(ready(chains.cold.unwrap_or(Ok(Vec::new()))))
    }
}

// ---------------------------------------------------------------------------
// 桶生命周期的四种热层提交
// ---------------------------------------------------------------------------

/// [`FakeHotCommits`] 收到的一次提交。
#[derive(Debug, Clone, PartialEq)]
pub enum HotCommitCall {
    Handover(Dataset, HandoverRequest),
    TtlHot(Dataset, TtlHotRequest),
    ColdChange(Dataset, ColdChangeRequest),
    Archive(Dataset, ArchiveRequest),
}

/// 记下调用的热层提交：默认每次都成功，快照号按数据集自 1 起递增（[`FakeHotCommits::set_tail`] 可改起点），
/// `created_at` 取 [`FakeHotCommits::set_now`] 所设（默认 0）；[`FakeHotCommits::fail_next`] 让下一次以给定的错误失败。
/// 不校验请求，不写任何对象。
#[derive(Debug, Default)]
pub struct FakeHotCommits {
    inner: Mutex<FakeHotCommitsInner>,
}

#[derive(Debug, Default)]
struct FakeHotCommitsInner {
    tails: HashMap<Dataset, u64>,
    now: i64,
    failures: VecDeque<CommitError>,
    calls: Vec<HotCommitCall>,
}

impl FakeHotCommits {
    pub fn new() -> Self {
        Self::default()
    }

    /// 数据集 `dataset` 此刻的链尾为 `tail`：下一次成功的提交创建 `tail + 1`。
    pub fn set_tail(&self, dataset: &Dataset, tail: SnapshotId) {
        lock(&self.inner).tails.insert(dataset.clone(), tail.0);
    }

    /// 此后成功的提交所报的 `created_at`。
    pub fn set_now(&self, now_us: i64) {
        lock(&self.inner).now = now_us;
    }

    /// 让接下来的一次提交（不论种类）以 `error` 失败；可连续预设多次，按先后用掉。
    pub fn fail_next(&self, error: CommitError) {
        lock(&self.inner).failures.push_back(error);
    }

    /// 收到的提交（含失败的），按顺序。
    pub fn calls(&self) -> Vec<HotCommitCall> {
        lock(&self.inner).calls.clone()
    }

    fn commit(
        &self,
        dataset: &Dataset,
        call: HotCommitCall,
    ) -> BoxFuture<'_, Result<Committed, CommitError>> {
        let mut inner = lock(&self.inner);
        inner.calls.push(call);
        let result = match inner.failures.pop_front() {
            Some(error) => Err(error),
            None => {
                let now = inner.now;
                let tail = inner.tails.entry(dataset.clone()).or_default();
                *tail += 1;
                Ok(Committed {
                    snapshot_id: SnapshotId(*tail),
                    created_at: now,
                })
            }
        };
        Box::pin(ready(result))
    }
}

impl HotCommits for FakeHotCommits {
    fn commit_handover<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: HandoverRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        self.commit(dataset, HotCommitCall::Handover(dataset.clone(), request))
    }

    fn commit_ttl_hot<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: TtlHotRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        self.commit(dataset, HotCommitCall::TtlHot(dataset.clone(), request))
    }

    fn commit_cold_change<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdChangeRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        self.commit(dataset, HotCommitCall::ColdChange(dataset.clone(), request))
    }

    fn commit_archive<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ArchiveRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        self.commit(dataset, HotCommitCall::Archive(dataset.clone(), request))
    }
}

// ---------------------------------------------------------------------------
// 冷层表
// ---------------------------------------------------------------------------

/// 按预设作答的冷层读取：未预设表的数据集读作 [`ColdState::NoTable`]；`snapshot_files`、`referenced`、`file_bytes` 与
/// `read_file` 未预设时返回 [`ColdReadError::Integrity`]。`table` 不按 [`ColdScope`] 筛清单，原样返回预设的表。
/// `version` 由预设的表推出；`table_at` 在版本号与预设的表相同时返回它，否则返回 [`ColdReadError::Integrity`]。
#[derive(Debug, Default)]
pub struct FakeColdReader {
    datasets: Mutex<HashMap<Dataset, FakeColdDataset>>,
    reads: AtomicUsize,
}

#[derive(Debug, Default)]
struct FakeColdDataset {
    table: Option<Result<ColdState, ColdReadError>>,
    snapshot_files: HashMap<i64, Vec<ColdDataFile>>,
    snapshot_references: HashMap<i64, HashSet<String>>,
    referenced: Option<HashSet<String>>,
    files: HashMap<String, Result<Vec<RecordBatch>, ColdReadError>>,
    bytes: HashMap<String, Result<Bytes, ColdReadError>>,
}

impl FakeColdReader {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_table(&self, dataset: &Dataset, table: Result<ColdState, ColdReadError>) {
        self.with_dataset(dataset, |d| d.table = Some(table));
    }

    pub fn set_snapshot_files(
        &self,
        dataset: &Dataset,
        snapshot_id: i64,
        files: Vec<ColdDataFile>,
    ) {
        self.with_dataset(dataset, |d| {
            d.snapshot_files.insert(snapshot_id, files);
        });
    }

    pub fn set_snapshot_references(
        &self,
        dataset: &Dataset,
        snapshot_id: i64,
        refs: HashSet<String>,
    ) {
        self.with_dataset(dataset, |d| {
            d.snapshot_references.insert(snapshot_id, refs);
        });
    }

    pub fn set_referenced(&self, dataset: &Dataset, referenced: HashSet<String>) {
        self.with_dataset(dataset, |d| d.referenced = Some(referenced));
    }

    /// 预置 `read_file` 对路径 `path` 的结果（替身不按 `target` 转换，原样返回）。
    pub fn put_file(
        &self,
        dataset: &Dataset,
        path: &str,
        batches: Result<Vec<RecordBatch>, ColdReadError>,
    ) {
        self.with_dataset(dataset, |d| {
            d.files.insert(path.to_owned(), batches);
        });
    }

    /// 预置 `file_bytes` 对路径 `path` 的结果。
    pub fn put_file_bytes(
        &self,
        dataset: &Dataset,
        path: &str,
        bytes: Result<Bytes, ColdReadError>,
    ) {
        self.with_dataset(dataset, |d| {
            d.bytes.insert(path.to_owned(), bytes);
        });
    }

    /// 各读取方法被调用的总次数（用来断言「不读冷层对象」）。
    pub fn reads(&self) -> usize {
        self.reads.load(Ordering::SeqCst)
    }

    fn with_dataset(&self, dataset: &Dataset, f: impl FnOnce(&mut FakeColdDataset)) {
        f(lock(&self.datasets).entry(dataset.clone()).or_default());
    }

    fn read<T>(
        &self,
        dataset: &Dataset,
        what: &str,
        pick: impl FnOnce(&FakeColdDataset) -> Option<Result<T, ColdReadError>>,
    ) -> Result<T, ColdReadError> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        lock(&self.datasets)
            .get(dataset)
            .and_then(pick)
            .unwrap_or_else(|| {
                Err(ColdReadError::Integrity(format!(
                    "FakeColdReader: {what} of {dataset} not set"
                )))
            })
    }
}

impl ColdReader for FakeColdReader {
    fn table<'a>(
        &'a self,
        dataset: &'a Dataset,
        _scope: ColdScope,
    ) -> BoxFuture<'a, Result<ColdState, ColdReadError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        let state = lock(&self.datasets)
            .get(dataset)
            .and_then(|d| d.table.clone())
            .unwrap_or(Ok(ColdState::NoTable));
        Box::pin(ready(state))
    }

    fn version<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<u64>, ColdReadError>> {
        let state = self.table(dataset, ColdScope::All);
        Box::pin(async move {
            Ok(match state.await? {
                ColdState::NoTable => None,
                ColdState::Table(t) => Some(t.metadata_version),
            })
        })
    }

    fn table_at<'a>(
        &'a self,
        dataset: &'a Dataset,
        version: u64,
        _scope: ColdScope,
    ) -> BoxFuture<'a, Result<Arc<ColdTable>, ColdReadError>> {
        Box::pin(ready(self.read(dataset, "table", |d| match &d.table {
            Some(Ok(ColdState::Table(t))) if t.metadata_version == version => {
                Some(Ok(Arc::clone(t)))
            }
            Some(Err(e)) => Some(Err(e.clone())),
            _ => None,
        })))
    }

    fn snapshot_files<'a>(
        &'a self,
        dataset: &'a Dataset,
        _table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<Vec<ColdDataFile>, ColdReadError>> {
        Box::pin(ready(self.read(dataset, "snapshot files", |d| {
            d.snapshot_files.get(&snapshot_id).cloned().map(Ok)
        })))
    }

    fn snapshot_references<'a>(
        &'a self,
        dataset: &'a Dataset,
        _table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>> {
        Box::pin(ready(self.read(dataset, "snapshot references", |d| {
            d.snapshot_references.get(&snapshot_id).cloned().map(Ok)
        })))
    }

    fn referenced<'a>(
        &'a self,
        dataset: &'a Dataset,
        _table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>> {
        Box::pin(ready(self.read(dataset, "referenced objects", |d| {
            d.referenced.clone().map(Ok)
        })))
    }

    fn file_bytes<'a>(
        &'a self,
        dataset: &'a Dataset,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Bytes, ColdReadError>> {
        Box::pin(ready(
            self.read(dataset, "file bytes", |d| d.bytes.get(path).cloned()),
        ))
    }

    fn read_file<'a>(
        &'a self,
        dataset: &'a Dataset,
        _table: &'a ColdTable,
        file: &'a ColdDataFile,
        _target: SchemaRef,
        _time: std::ops::Range<i64>,
    ) -> BoxFuture<'a, Result<ColdBatches, ColdReadError>> {
        let read = self.read(dataset, "data file", |d| d.files.get(&file.path).cloned());
        Box::pin(ready(read.map(|batches| {
            Box::new(batches.into_iter().map(Ok)) as ColdBatches
        })))
    }
}

/// [`FakeColdWriter`] 收到的一次调用。
#[derive(Debug, Clone)]
pub enum ColdWriteCall {
    Append(Dataset, ColdAppend),
    Delete(Dataset, ColdDelete),
    Replace(Dataset, ColdReplace),
    /// 冷层过期提交：替身以预设的表调一次 `select`，记下它选出的快照号（`select` 失败时不记这一次调用）。
    Expire(Dataset, Vec<i64>),
}

/// 按预设作答的冷层写入：`append`、`delete`、`replace` 与 `expire` 依次用掉为之预设的结果，没有预设时返回
/// [`ColdWriteError::Unavailable`]；`in_flight` 返回预设的集合（默认空）。不写任何对象。
///
/// `expire` 先以 [`FakeColdWriter::set_expire_table`] 所设的表调一次 `request.select`（未设时不调，记为空选），
/// `select` 失败时按 [`ColdUpkeep::expire`] 的约定返回 `Unavailable` 或 `Integrity`；选出为空时返回 `Ok(None)`、
/// 不用掉预设。
#[derive(Debug, Default)]
pub struct FakeColdWriter {
    inner: Mutex<FakeColdWriterInner>,
}

#[derive(Debug, Default)]
struct FakeColdWriterInner {
    appends: VecDeque<Result<HandoverDraft, ColdWriteError>>,
    deletes: VecDeque<Result<ColdDeleted, ColdWriteError>>,
    replaces: VecDeque<Result<ColdReplaced, ColdWriteError>>,
    expires: VecDeque<Result<ColdExpired, ColdWriteError>>,
    expire_table: Option<Arc<ColdTable>>,
    in_flight: HashMap<Dataset, HashSet<String>>,
    calls: Vec<ColdWriteCall>,
}

impl FakeColdWriter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_append(&self, result: Result<HandoverDraft, ColdWriteError>) {
        lock(&self.inner).appends.push_back(result);
    }

    pub fn push_delete(&self, result: Result<ColdDeleted, ColdWriteError>) {
        lock(&self.inner).deletes.push_back(result);
    }

    pub fn push_replace(&self, result: Result<ColdReplaced, ColdWriteError>) {
        lock(&self.inner).replaces.push_back(result);
    }

    pub fn push_expire(&self, result: Result<ColdExpired, ColdWriteError>) {
        lock(&self.inner).expires.push_back(result);
    }

    /// `expire` 调 `select` 时所传的冷层当前版本。
    pub fn set_expire_table(&self, table: Arc<ColdTable>) {
        lock(&self.inner).expire_table = Some(table);
    }

    pub fn set_in_flight(&self, dataset: &Dataset, paths: HashSet<String>) {
        lock(&self.inner).in_flight.insert(dataset.clone(), paths);
    }

    /// 收到的调用，按顺序。
    pub fn calls(&self) -> Vec<ColdWriteCall> {
        lock(&self.inner).calls.clone()
    }
}

fn not_preset(what: &str) -> ColdWriteError {
    ColdWriteError::Unavailable(format!("FakeColdWriter: no {what} result preset"))
}

impl ColdWriter for FakeColdWriter {
    fn append<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdAppend,
    ) -> BoxFuture<'a, Result<HandoverDraft, ColdWriteError>> {
        let mut inner = lock(&self.inner);
        inner
            .calls
            .push(ColdWriteCall::Append(dataset.clone(), request));
        let result = inner
            .appends
            .pop_front()
            .unwrap_or_else(|| Err(not_preset("append")));
        Box::pin(ready(result))
    }

    fn delete<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdDelete,
    ) -> BoxFuture<'a, Result<ColdDeleted, ColdWriteError>> {
        let mut inner = lock(&self.inner);
        inner
            .calls
            .push(ColdWriteCall::Delete(dataset.clone(), request));
        let result = inner
            .deletes
            .pop_front()
            .unwrap_or_else(|| Err(not_preset("delete")));
        Box::pin(ready(result))
    }

    fn in_flight(&self, dataset: &Dataset) -> HashSet<String> {
        lock(&self.inner)
            .in_flight
            .get(dataset)
            .cloned()
            .unwrap_or_default()
    }
}

impl ColdUpkeep for FakeColdWriter {
    fn replace<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdReplace,
    ) -> BoxFuture<'a, Result<ColdReplaced, ColdWriteError>> {
        let mut inner = lock(&self.inner);
        inner
            .calls
            .push(ColdWriteCall::Replace(dataset.clone(), request));
        let result = inner
            .replaces
            .pop_front()
            .unwrap_or_else(|| Err(not_preset("replace")));
        Box::pin(ready(result))
    }

    fn expire<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdExpire,
    ) -> BoxFuture<'a, Result<Option<ColdExpired>, ColdWriteError>> {
        Box::pin(async move {
            let table = lock(&self.inner).expire_table.clone();
            let selected = match table {
                Some(t) => match request.select.select(dataset, &t).await {
                    Ok(ids) => ids,
                    Err(ColdReadError::Unavailable(e)) => {
                        return Err(ColdWriteError::Unavailable(e));
                    }
                    Err(ColdReadError::Integrity(e) | ColdReadError::NotFound(e)) => {
                        return Err(ColdWriteError::Integrity(e));
                    }
                },
                None => Vec::new(),
            };
            let mut inner = lock(&self.inner);
            inner
                .calls
                .push(ColdWriteCall::Expire(dataset.clone(), selected.clone()));
            if selected.is_empty() {
                return Ok(None);
            }
            inner
                .expires
                .pop_front()
                .unwrap_or_else(|| Err(not_preset("expire")))
                .map(Some)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_common::record::Record;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn app() -> Dataset {
        Dataset::new("default", "app")
    }

    fn records(n: usize) -> WalEntry {
        WalEntry::Records {
            dataset: app(),
            records: vec![
                Record {
                    timestamp_us: 1,
                    fields: Vec::new(),
                };
                n
            ],
        }
    }

    #[tokio::test]
    async fn fake_wal_positions_failure_and_replay() {
        let fake = Arc::new(FakeWal::new());
        let wal: Arc<dyn Wal> = fake.clone();
        let exists = WalEntry::DatasetExists { dataset: app() };
        assert_eq!(
            wal.append(vec![exists, records(2)]).await,
            Ok(WalPosition(2))
        );
        fake.set_failing(true);
        assert!(matches!(
            wal.append(vec![records(1)]).await,
            Err(WalError::Write(_))
        ));
        fake.set_failing(false);
        assert_eq!(wal.append(vec![records(1)]).await, Ok(WalPosition(3)));

        let mut seen = Vec::new();
        wal.replay(&mut |p, _| seen.push(p.0)).unwrap();
        assert_eq!(seen, [1, 2, 3], "失败的追加不在重放中出现");

        wal.release(Release::Existence { dataset: app() });
        assert_eq!(fake.releases().len(), 1);
    }

    #[test]
    fn fake_memtable_candidates_survive_removal() {
        let fake = FakeMemtable::new();
        let view: &dyn MemtableView = &fake;
        assert!(view.schema(&app()).is_none(), "未登记即不在进程内目录");

        fake.add_dataset(&app(), StreamColumns::genesis());
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        let batch = |v: i64| {
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(Int64Array::from(vec![v]))],
            )
            .unwrap()
        };
        let g1 = fake.push_generation(&app(), vec![batch(1)]);
        fake.push_generation(&app(), vec![batch(2)]);

        let candidates = view.candidates(&app());
        fake.bind(&app(), g1, SnapshotId(7));
        fake.remove(&app(), g1);
        // 候选取定之后 g1 才绑定并移除：候选仍持有它，按判定时的绑定号排除
        assert_eq!(
            candidates.clone().participating(Some(SnapshotId(7))).len(),
            1
        );
        assert_eq!(candidates.participating(Some(SnapshotId(6))).len(), 2);
        assert_eq!(view.candidates(&app()).participating(None).len(), 1);
    }

    #[tokio::test]
    async fn fake_committer_and_chain_answer_as_set() {
        let committer = FakeCommitter::new();
        assert_eq!(committer.ensure_dataset(&app()).await, Existence::Committed);
        committer.set_pending(&app(), true);
        assert_eq!(committer.ensure_dataset(&app()).await, Existence::Pending);
        assert_eq!(committer.ensured().len(), 2);
        assert_eq!(committer.load().await, Ok(Vec::new()));

        let chain = FakeChain::new();
        assert!(matches!(
            chain.current(&app()).await,
            Err(ChainError::Integrity(_))
        ));
        chain.set_current(&app(), Ok(None));
        assert_eq!(chain.current(&app()).await, Ok(None));
        assert_eq!(chain.reads(), 2);
    }

    #[test]
    fn fake_memtable_writer_and_generations() {
        let fake = FakeMemtable::new();
        fake.register(&app(), StreamColumns::genesis());
        fake.write(
            &app(),
            WalPosition(3),
            vec![Record {
                timestamp_us: 1,
                fields: vec![
                    ("b".into(), amberlog_common::record::FieldValue::Int64(1)),
                    (
                        "a".into(),
                        amberlog_common::record::FieldValue::Boolean(true),
                    ),
                ],
            }],
        );
        let schema = fake.schema(&app()).unwrap();
        assert_eq!(
            schema.get("b").unwrap().field_id,
            2,
            "同一记录内按字段出现顺序取 id"
        );
        assert_eq!(schema.get("a").unwrap().field_id, 3);
        assert_eq!(fake.written()[0].position, WalPosition(3));

        assert!(fake.freeze(&app(), 10).is_none());
        let g = fake.stage_frozen(&app(), WalPosition(1), WalPosition(3), Vec::new());
        let frozen = fake.freeze(&app(), 10).unwrap();
        assert_eq!((frozen.id, frozen.last), (g, WalPosition(3)));
        fake.thaw(&app(), g);
        assert_eq!(fake.freeze(&app(), 10).unwrap().id, g, "退回后再次交出");
        let generations: &dyn MemtableGenerations = &fake;
        generations.bind(&app(), &[g], SnapshotId(2));
        generations.retire(&app(), &[g]);
        assert_eq!(fake.retired(), [(app(), g)]);
        assert!(fake.candidates(&app()).participating(None).is_empty());
    }

    #[tokio::test]
    async fn fake_hot_commits_number_per_dataset_and_record_calls() {
        let fake = FakeHotCommits::new();
        let commits: &dyn HotCommits = &fake;
        fake.set_tail(&app(), SnapshotId(4));
        fake.set_now(7);
        let archive = ArchiveRequest {
            buckets: vec![crate::layout::HourBucket::of(0)],
        };
        assert_eq!(
            commits.commit_archive(&app(), archive.clone()).await,
            Ok(Committed {
                snapshot_id: SnapshotId(5),
                created_at: 7
            })
        );
        fake.fail_next(CommitError::Unavailable);
        assert_eq!(
            commits
                .commit_ttl_hot(
                    &app(),
                    TtlHotRequest {
                        cutoff: 1,
                        expired: Vec::new()
                    }
                )
                .await,
            Err(CommitError::Unavailable)
        );
        let web = Dataset::new("default", "web");
        assert_eq!(
            commits
                .commit_archive(&web, archive.clone())
                .await
                .map(|c| c.snapshot_id),
            Ok(SnapshotId(1))
        );
        assert_eq!(fake.calls().len(), 3);
        assert_eq!(fake.calls()[0], HotCommitCall::Archive(app(), archive));
    }

    #[tokio::test]
    async fn fake_cold_reader_and_writer_answer_as_preset() {
        let reader = FakeColdReader::new();
        let cold: &dyn ColdReader = &reader;
        assert_eq!(
            cold.table(&app(), ColdScope::All).await,
            Ok(ColdState::NoTable)
        );
        let table = ColdTable {
            location: "s3://b/amberlog/_iceberg/default/app".into(),
            metadata_version: 1,
            current_snapshot_id: None,
            fields: Vec::new(),
            snapshots: Vec::new(),
            files: Vec::new(),
        };
        reader.set_table(&app(), Ok(ColdState::Table(Arc::new(table.clone()))));
        assert!(matches!(
            cold.table(&app(), ColdScope::Days(0..=1)).await,
            Ok(ColdState::Table(_))
        ));
        assert!(matches!(
            cold.referenced(&app(), &table).await,
            Err(ColdReadError::Integrity(_))
        ));
        assert_eq!(reader.reads(), 3);
        assert_eq!(cold.version(&app()).await, Ok(Some(1)));
        assert_eq!(
            cold.table_at(&app(), 1, ColdScope::All)
                .await
                .unwrap()
                .as_ref(),
            &table
        );
        assert!(matches!(
            cold.table_at(&app(), 2, ColdScope::All).await,
            Err(ColdReadError::Integrity(_))
        ));

        let writer = FakeColdWriter::new();
        let w: &dyn ColdWriter = &writer;
        assert!(w.in_flight(&app()).is_empty());
        writer.push_delete(Ok(ColdDeleted {
            metadata_version: 2,
            iceberg_snapshot_id: 9,
            removed: vec!["data/x.parquet".into()],
        }));
        let delete = ColdDelete {
            cutoff: 5,
            paths: vec!["data/x.parquet".into()],
        };
        assert_eq!(
            w.delete(&app(), delete.clone())
                .await
                .map(|d| d.iceberg_snapshot_id),
            Ok(9)
        );
        assert!(matches!(
            w.delete(&app(), delete).await,
            Err(ColdWriteError::Unavailable(_))
        ));
        assert_eq!(writer.calls().len(), 2);
    }

    /// 只选出不是当前快照的那些。
    struct NotCurrent;

    impl crate::cold::ExpireSelect for NotCurrent {
        fn select<'a>(
            &'a self,
            _dataset: &'a Dataset,
            table: &'a ColdTable,
        ) -> BoxFuture<'a, Result<Vec<i64>, ColdReadError>> {
            let ids = table
                .snapshots
                .iter()
                .map(|s| s.snapshot_id)
                .filter(|id| Some(*id) != table.current_snapshot_id)
                .collect();
            Box::pin(ready(Ok(ids)))
        }
    }

    #[tokio::test]
    async fn fake_cold_upkeep_replaces_and_expires_as_preset() {
        let writer = FakeColdWriter::new();
        let w: &dyn ColdUpkeep = &writer;
        let replace = ColdReplace {
            day: 20_000,
            inputs: vec![("data/a.parquet".into(), Bytes::from_static(b"a"))],
            bloom: crate::bloom::BloomColumns::none(),
        };
        assert!(matches!(
            w.replace(&app(), replace.clone()).await,
            Err(ColdWriteError::Unavailable(_))
        ));
        writer.push_replace(Err(ColdWriteError::Stale));
        assert_eq!(w.replace(&app(), replace).await, Err(ColdWriteError::Stale));

        let expire = ColdExpire {
            select: Arc::new(NotCurrent),
        };
        // 未设表：空选，不写
        assert_eq!(w.expire(&app(), expire.clone()).await, Ok(None));
        let snap = |id| crate::cold::ColdSnapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            timestamp_ms: 0,
            operation: crate::cold::ColdOperation::Other,
        };
        writer.set_expire_table(Arc::new(ColdTable {
            location: "s3://b/amberlog/_iceberg/default/app".into(),
            metadata_version: 3,
            current_snapshot_id: Some(3),
            fields: Vec::new(),
            snapshots: vec![snap(1), snap(2), snap(3)],
            files: Vec::new(),
        }));
        let expired = ColdExpired {
            metadata_version: 4,
            removed: vec![1, 2],
        };
        writer.push_expire(Ok(expired.clone()));
        assert_eq!(w.expire(&app(), expire).await, Ok(Some(expired)));
        let calls: Vec<_> = writer
            .calls()
            .into_iter()
            .filter_map(|c| match c {
                ColdWriteCall::Expire(_, ids) => Some(ids),
                _ => None,
            })
            .collect();
        assert_eq!(calls, vec![Vec::new(), vec![1, 2]]);
    }
}
