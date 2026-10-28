//! 可见集本体：按数据集分代保存已确认的记录，持有 stream 当前 schema 的权威副本（ADR-0021、ADR-0022）。
//!
//! - 每个数据集同一时刻恰有一个活动代，即尚未冻结的积压；每次写入是活动代末尾的一段（一个 WAL 条目的记录，留在内存时转成
//!   一个 Arrow 批次，列为该批记录出现过的列加 `_timestamp`）。各段按确认的先后（WAL 中的先后）排列。
//! - 冻结从活动代之首取一个前缀成为冻结代：至多 `flush.max_records` 条，所含溢出记录的字节数达到内存上限即截断，必要时把
//!   一段切开（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。冻结代照常参与候选视图，直到回执后移除；没有绑定过的冻结代可以退回活动代之首。
//! - 新列在在途批次登记时（[`MemtableWriter::reserve`]）或重放写入时并入当前 schema，各取当前 schema 中最大字段 id + 1；
//!   在途登记的列按引用它的在途批次计数，写入即确认，引用全部撤回时退出当前 schema（REQ-AMB-DM-SCHEMA-DRIFT）。
//! - 每个数据集一把锁：写入、冻结、绑定、回执与取候选视图互斥，候选视图取的是那一刻各代批次的快照（批次本身不可变）。
//! - 内存中的积压有全局上限（`ingest.max_backlog_memory_bytes`，按记录在 WAL 中所占的字节计，REQ-AMB-ING-HOT-BACKPRESSURE）：
//!   写入时整批放得下就整批留在内存，放不下就整批成为溢出记录——段里只记下承载它的 WAL 位置，物化时由 flush 从 WAL 读回；
//!   溢出记录不进候选视图。WAL 中没有原样承载的批次（[`MemtableWriter::write_resident`]）无从读回，不论上限留在内存。
//!   冻结代被回执时归还它所含、留在内存中的记录占用的额度（一次写入提交成功即归还，REQ-AMB-ING-HOT-BACKPRESSURE-V5）；
//!   切开的一段按记录数分摊它的字节数。此刻占用的额度即内存积压字节数（[`Memtable::backlog_memory_bytes`]）。

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::{FieldValue, Record};
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;

use super::{
    Binding, CandidateGeneration, Candidates, FrozenGeneration, GenerationId, MemtableGenerations,
    MemtableView, MemtableWriter, SpilledRecords,
};
use crate::chain::SnapshotId;
use crate::schema::{SchemaColumn, StreamColumns};
use crate::wal::{WalPosition, records_len};

/// 进程内的可见集。
#[derive(Debug, Default)]
pub struct Memtable {
    streams: RwLock<HashMap<Dataset, Arc<Mutex<Stream>>>>,
    budget: Budget,
}

/// 内存中积压的全局额度。
#[derive(Debug)]
struct Budget {
    limit: u64,
    used: AtomicU64,
}

impl Default for Budget {
    fn default() -> Budget {
        Budget {
            limit: u64::MAX,
            used: AtomicU64::new(0),
        }
    }
}

impl Budget {
    /// 留得下 `n` 字节就占用并返回 `true`。
    fn take(&self, n: u64) -> bool {
        self.used
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |used| {
                used.checked_add(n).filter(|&total| total <= self.limit)
            })
            .is_ok()
    }

    /// 不论上限，占用 `n` 字节。
    fn force(&self, n: u64) {
        self.used.fetch_add(n, Ordering::SeqCst);
    }

    fn give_back(&self, n: u64) {
        self.used.fetch_sub(n, Ordering::SeqCst);
    }
}

#[derive(Debug)]
struct Stream {
    columns: StreamColumns,
    /// 在途登记、尚未确认的列：列名 → 引用它的在途批次数。
    pending: HashMap<String, usize>,
    active: Generation,
    /// 已冻结、尚未回执的代，按冻结顺序。
    frozen: Vec<Generation>,
    next_id: u64,
}

/// 一代中的一段：一个 WAL 条目的记录，或切开后的一部分。
#[derive(Debug, Clone)]
struct Piece {
    position: WalPosition,
    /// 该条目中排在本段之前的记录数（在更早的段里）。
    skip: u32,
    count: u32,
    /// 该条目的记录总数。
    entry_len: u32,
    /// 本段记录在 WAL 中所占的字节（切开的段按记录数分摊）。
    bytes: u64,
    /// 留在内存中的记录；溢出时为 `None`。
    batch: Option<RecordBatch>,
    /// 经 [`MemtableWriter::write_resident`] 写入：WAL 中该位置并不原样承载这些记录，不切开。
    resident: bool,
}

impl Piece {
    /// 切成前 `k` 条与其余（0 < k < count）。
    fn split(self, k: u32) -> (Piece, Piece) {
        debug_assert!(k > 0 && k < self.count);
        let head_bytes = (u128::from(self.bytes) * u128::from(k) / u128::from(self.count)) as u64;
        let (head_batch, tail_batch) = match &self.batch {
            Some(b) => (
                Some(b.slice(0, k as usize)),
                Some(b.slice(k as usize, (self.count - k) as usize)),
            ),
            None => (None, None),
        };
        let head = Piece {
            count: k,
            bytes: head_bytes,
            batch: head_batch,
            ..self.clone()
        };
        let tail = Piece {
            skip: self.skip + k,
            count: self.count - k,
            bytes: self.bytes - head_bytes,
            batch: tail_batch,
            ..self
        };
        (head, tail)
    }

    fn memory_bytes(&self) -> u64 {
        if self.batch.is_some() { self.bytes } else { 0 }
    }
}

#[derive(Debug)]
struct Generation {
    id: GenerationId,
    binding: Arc<Binding>,
    pieces: VecDeque<Piece>,
}

impl Generation {
    fn new(id: u64) -> Generation {
        Generation {
            id: GenerationId(id),
            binding: Arc::new(Binding::default()),
            pieces: VecDeque::new(),
        }
    }

    fn memory_bytes(&self) -> u64 {
        self.pieces.iter().map(Piece::memory_bytes).sum()
    }

    fn batches(&self) -> Vec<RecordBatch> {
        self.pieces.iter().filter_map(|p| p.batch.clone()).collect()
    }

    fn frozen(&self, spill_capped: bool) -> FrozenGeneration {
        let first = self.pieces.front().expect("冻结代至少一段");
        let last = self.pieces.back().expect("冻结代至少一段");
        let end = last.skip + last.count;
        FrozenGeneration {
            id: self.id,
            first: first.position,
            last: last.position,
            last_partial: (end < last.entry_len).then_some(end),
            records: self.pieces.iter().map(|p| u64::from(p.count)).sum(),
            batches: self.batches(),
            spilled: self
                .pieces
                .iter()
                .filter(|p| p.batch.is_none())
                .map(|p| SpilledRecords {
                    position: p.position,
                    skip: p.skip,
                    count: p.count,
                })
                .collect(),
            spill_capped,
        }
    }

    fn candidate(&self) -> CandidateGeneration {
        CandidateGeneration::new(Arc::clone(&self.binding), self.batches())
    }
}

impl Stream {
    fn new(columns: StreamColumns) -> Stream {
        Stream {
            columns,
            pending: HashMap::new(),
            active: Generation::new(1),
            frozen: Vec::new(),
            next_id: 2,
        }
    }

    fn frozen_mut(&mut self, id: GenerationId) -> Option<&mut Generation> {
        self.frozen.iter_mut().find(|g| g.id == id)
    }
}

fn lock(m: &Mutex<Stream>) -> MutexGuard<'_, Stream> {
    m.lock().expect("可见集的锁未中毒")
}

impl Memtable {
    pub fn new() -> Memtable {
        Memtable::default()
    }

    /// 内存中积压（全部数据集合计，按记录在 WAL 中所占的字节计）的上限：`ingest.max_backlog_memory_bytes`。
    pub fn with_backlog_limit(limit: u64) -> Memtable {
        Memtable {
            budget: Budget {
                limit,
                used: AtomicU64::new(0),
            },
            ..Memtable::default()
        }
    }

    /// 内存积压字节数：此刻留在内存中的积压记录在 WAL 中所占的字节之和，全部数据集合计，溢出记录不计
    /// （稳定指标 `amberlog_backlog_memory_bytes`，REQ-AMB-ING-HOT-BACKPRESSURE-V5）。
    pub fn backlog_memory_bytes(&self) -> u64 {
        self.budget.used.load(Ordering::SeqCst)
    }

    /// 写入一个已确认的批次（`position` 上的条目自第 `skip` 条起的记录）：`resident` 为真时不论上限整批留在内存，
    /// 否则放不下即整批溢出。
    fn append(
        &self,
        dataset: &Dataset,
        position: WalPosition,
        skip: u32,
        records: Vec<Record>,
        resident: bool,
    ) {
        if records.is_empty() {
            return;
        }
        // 约定先 register；没有登记的按创世 schema 登记，不丢记录
        let s = self.stream_or_register(dataset, StreamColumns::genesis());
        let mut s = lock(&s);
        let mut added = Vec::new();
        for record in &records {
            for (name, value) in &record.fields {
                s.pending.remove(name);
                if s.columns.get(name).is_none() && !added.iter().any(|(n, _)| n == name) {
                    added.push((name.clone(), value.column_type()));
                }
            }
        }
        s.add_columns(added);
        let bytes = records_len(dataset, &records);
        let in_memory = if resident {
            self.budget.force(bytes);
            true
        } else {
            self.budget.take(bytes)
        };
        let count = u32::try_from(records.len()).expect("一个批次的记录数在 u32 之内");
        let batch = in_memory.then(|| to_batch(&s.columns, &records));
        s.active.pieces.push_back(Piece {
            position,
            skip,
            count,
            entry_len: skip + count,
            bytes,
            batch,
            resident,
        });
    }

    fn stream(&self, dataset: &Dataset) -> Option<Arc<Mutex<Stream>>> {
        self.streams
            .read()
            .expect("可见集目录的锁未中毒")
            .get(dataset)
            .cloned()
    }

    fn stream_or_register(&self, dataset: &Dataset, baseline: StreamColumns) -> Arc<Mutex<Stream>> {
        if let Some(s) = self.stream(dataset) {
            return s;
        }
        let mut streams = self.streams.write().expect("可见集目录的锁未中毒");
        Arc::clone(
            streams
                .entry(dataset.clone())
                .or_insert_with(|| Arc::new(Mutex::new(Stream::new(baseline)))),
        )
    }
}

impl MemtableView for Memtable {
    fn schema(&self, dataset: &Dataset) -> Option<StreamColumns> {
        let s = self.stream(dataset)?;
        let columns = lock(&s).columns.clone();
        Some(columns)
    }

    fn candidates(&self, dataset: &Dataset) -> Candidates {
        let Some(s) = self.stream(dataset) else {
            return Candidates::default();
        };
        let s = lock(&s);
        Candidates::new(
            s.frozen
                .iter()
                .chain(std::iter::once(&s.active))
                .filter(|g| g.pieces.iter().any(|p| p.batch.is_some()))
                .map(Generation::candidate)
                .collect(),
        )
    }
}

impl MemtableWriter for Memtable {
    fn register(&self, dataset: &Dataset, baseline: StreamColumns) {
        self.stream_or_register(dataset, baseline);
    }

    fn write(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.append(dataset, position, 0, records, false);
    }

    fn write_resident(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>) {
        self.append(dataset, position, 0, records, true);
    }

    fn write_tail(
        &self,
        dataset: &Dataset,
        position: WalPosition,
        skip: u32,
        records: Vec<Record>,
    ) {
        self.append(dataset, position, skip, records, false);
    }

    fn reserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        let s = self.stream_or_register(dataset, StreamColumns::genesis());
        let mut s = lock(&s);
        let mut added = Vec::new();
        for (name, column_type) in columns {
            if let Some(refs) = s.pending.get_mut(name) {
                *refs += 1;
            } else if s.columns.get(name).is_none() {
                s.pending.insert(name.clone(), 1);
                added.push((name.clone(), *column_type));
            }
        }
        s.add_columns(added);
    }

    fn unreserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]) {
        let Some(s) = self.stream(dataset) else {
            return;
        };
        let mut s = lock(&s);
        let mut withdrawn = Vec::new();
        for (name, _) in columns {
            if let Some(refs) = s.pending.get_mut(name) {
                *refs -= 1;
                if *refs == 0 {
                    s.pending.remove(name);
                    withdrawn.push(name.as_str());
                }
            }
        }
        if !withdrawn.is_empty() {
            let kept = s
                .columns
                .columns()
                .iter()
                .filter(|c| !withdrawn.contains(&c.name.as_str()))
                .cloned()
                .collect();
            s.columns = StreamColumns::from_columns(kept);
        }
    }
}

impl Stream {
    /// 新列依次并入当前 schema，各取当前最大字段 id + 1。
    fn add_columns(&mut self, added: Vec<(String, ColumnType)>) {
        if added.is_empty() {
            return;
        }
        let mut columns = self.columns.columns().to_vec();
        let first_id = columns.iter().map(|c| c.field_id).max().unwrap_or(0) + 1;
        for (field_id, (name, column_type)) in (first_id..).zip(added) {
            columns.push(SchemaColumn {
                name,
                column_type,
                field_id,
            });
        }
        self.columns = StreamColumns::from_columns(columns);
    }
}

impl MemtableGenerations for Memtable {
    fn freeze(&self, dataset: &Dataset, max_records: u64) -> Option<FrozenGeneration> {
        let s = self.stream(dataset)?;
        let mut s = lock(&s);
        if s.active.pieces.is_empty() {
            return None;
        }
        let mut g = Generation::new(s.next_id);
        let (mut records, mut spilled) = (0u64, 0u64);
        let mut spill_capped = false;
        while let Some(piece) = s.active.pieces.pop_front() {
            let room = max_records.saturating_sub(records);
            let piece = if u64::from(piece.count) <= room || piece.resident {
                if piece.resident && u64::from(piece.count) > room && !g.pieces.is_empty() {
                    // 不切开的一批放不下：留给下一次
                    s.active.pieces.push_front(piece);
                    break;
                }
                piece
            } else {
                let (head, tail) = piece.split(room as u32);
                s.active.pieces.push_front(tail);
                head
            };
            records += u64::from(piece.count);
            if piece.batch.is_none() {
                spilled += piece.bytes;
            }
            g.pieces.push_back(piece);
            if spilled >= self.budget.limit {
                spill_capped = true;
                break;
            }
            if records >= max_records {
                break;
            }
        }
        s.next_id += 1;
        let frozen = g.frozen(spill_capped);
        s.frozen.push(g);
        Some(frozen)
    }

    fn thaw(&self, dataset: &Dataset, generation: GenerationId) {
        let Some(s) = self.stream(dataset) else {
            return;
        };
        let mut s = lock(&s);
        let Some(i) = s.frozen.iter().position(|g| g.id == generation) else {
            return;
        };
        debug_assert!(i + 1 == s.frozen.len(), "只退回最近冻结的一代");
        debug_assert!(s.frozen[i].binding.get().is_none(), "绑定过的代不退回");
        let g = s.frozen.remove(i);
        for piece in g.pieces.into_iter().rev() {
            s.active.pieces.push_front(piece);
        }
    }

    fn bind(&self, dataset: &Dataset, generations: &[GenerationId], p: SnapshotId) {
        let Some(s) = self.stream(dataset) else {
            return;
        };
        let mut s = lock(&s);
        for id in generations {
            if let Some(g) = s.frozen_mut(*id) {
                g.binding.bind(p);
            }
        }
    }

    fn retire(&self, dataset: &Dataset, generations: &[GenerationId]) {
        let Some(s) = self.stream(dataset) else {
            return;
        };
        let mut s = lock(&s);
        let mut released = 0;
        s.frozen.retain(|g| {
            let retired = generations.contains(&g.id);
            if retired {
                released += g.memory_bytes();
            }
            !retired
        });
        self.budget.give_back(released);
    }
}

/// 一批记录转成一个 Arrow 批次：`_timestamp` 加这批记录出现过的列，按字段 id 升序；缺失的字段为 null。
/// 记录的列须都在 `columns` 中（flush 读回溢出记录时同样用它）。
pub fn to_batch(columns: &StreamColumns, records: &[Record]) -> RecordBatch {
    let used: Vec<&SchemaColumn> = columns
        .columns()
        .iter()
        .filter(|c| {
            c.name == TIMESTAMP_COLUMN
                || records
                    .iter()
                    .any(|r| r.fields.iter().any(|(n, _)| n == &c.name))
        })
        .collect();
    let arrays: Vec<ArrayRef> = used.iter().map(|c| column_array(c, records)).collect();
    let schema = Schema::new(used.iter().map(|c| c.arrow_field()).collect::<Vec<_>>());
    RecordBatch::try_new(Arc::new(schema), arrays).expect("列与 schema 一一对应")
}

fn field<'r>(r: &'r Record, name: &str) -> Option<&'r FieldValue> {
    r.fields.iter().find(|(n, _)| n == name).map(|(_, v)| v)
}

fn column_array(column: &SchemaColumn, records: &[Record]) -> ArrayRef {
    if column.name == TIMESTAMP_COLUMN {
        return Arc::new(
            TimestampMicrosecondArray::from(
                records.iter().map(|r| r.timestamp_us).collect::<Vec<_>>(),
            )
            .with_timezone("UTC"),
        );
    }
    // 类型已在受理时判定过，不会与列类型不同；万一不同按 null 处理
    match column.column_type {
        ColumnType::Int64 => {
            let mut b = Int64Builder::with_capacity(records.len());
            for r in records {
                b.append_option(match field(r, &column.name) {
                    Some(FieldValue::Int64(v)) => Some(*v),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        ColumnType::Float64 => {
            let mut b = Float64Builder::with_capacity(records.len());
            for r in records {
                b.append_option(match field(r, &column.name) {
                    Some(FieldValue::Float64(v)) => Some(*v),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        ColumnType::Boolean => {
            let mut b = BooleanBuilder::with_capacity(records.len());
            for r in records {
                b.append_option(match field(r, &column.name) {
                    Some(FieldValue::Boolean(v)) => Some(*v),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
        ColumnType::Utf8 => {
            let mut b = StringBuilder::new();
            for r in records {
                b.append_option(match field(r, &column.name) {
                    Some(FieldValue::Utf8(v)) => Some(v.as_str()),
                    _ => None,
                });
            }
            Arc::new(b.finish())
        }
    }
}

#[cfg(test)]
mod tests;
