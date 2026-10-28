//! 可见集（Memtable）的只读视图：查询取「一致对」中的内存一半（ADR-0021 第 1、3 条）。
//!
//! 可见集按 stream 分代：每个 stream 同一时刻恰有一个活动代（尚未冻结的积压），flush 时按确认的先后从中冻结一个前缀
//! 成为冻结代（至多 `flush.max_records` 条，REQ-AMB-ING-HOT-BACKPRESSURE-V5），冻结代按顺序提交，一个冻结代即一次写入提交。
//! 快照提交在创建快照 P 之前把所含各代的绑定快照号置为 P（[`Binding`]），快照 P 写成、发回执之后才把代移除。
//!
//! 三个接口按调用方分开：
//! - [`MemtableView`]：查询读当前 schema 与候选视图；
//! - [`MemtableWriter`]：摄取管道与生命周期编排登记数据集、登记在途批次的列、写入已确认的记录（新列登记与字段 id 分配在此，ADR-0022）；
//! - [`MemtableGenerations`]：物化与快照提交冻结活动代、绑定快照号、回执后移除。flush 何时触发属物化，不在可见集。
//!
//! 查询按三步取用（ADR-0021 第 3 条）：
//! 1. [`MemtableView::candidates`] 取该 stream 各代的不可变候选视图；
//! 2. 经 [`crate::chain::ChainReader::current`] 得快照号 Q；
//! 3. [`Candidates::participating`] 只留「未绑定，或绑定号大于 Q」的代——与快照 Q 引用的文件互补、不重不漏。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use amberlog_common::dataset::Dataset;
use amberlog_common::record::Record;
use amberlog_common::schema::ColumnType;
use arrow::record_batch::RecordBatch;

use crate::chain::SnapshotId;
use crate::schema::StreamColumns;
use crate::wal::WalPosition;

/// 一代的绑定快照号：未绑定为 `None`；只增不减（ADR-0021 第 2 条）。
#[derive(Debug, Default)]
pub struct Binding(AtomicU64);

impl Binding {
    pub fn get(&self) -> Option<SnapshotId> {
        match self.0.load(Ordering::SeqCst) {
            0 => None,
            p => Some(SnapshotId(p)),
        }
    }

    /// 置为 `p`；已绑定到更大的号时不变。
    pub(crate) fn bind(&self, p: SnapshotId) {
        self.0.fetch_max(p.0, Ordering::SeqCst);
    }
}

/// 候选视图中的一代：持有该代的绑定与取候选那一刻的全部批次。
///
/// 批次的列是该 stream 当前 schema 的子集，每列按 [`crate::schema::SchemaColumn::arrow_field`] 的写法
/// （带字段 id 元数据，`_timestamp` 为不可空的 `Timestamp(Microsecond, "UTC")`）；查询把它们对齐到表 schema 后再合并。
#[derive(Debug, Clone)]
pub struct CandidateGeneration {
    binding: Arc<Binding>,
    batches: Vec<RecordBatch>,
}

impl CandidateGeneration {
    pub(crate) fn new(binding: Arc<Binding>, batches: Vec<RecordBatch>) -> Self {
        CandidateGeneration { binding, batches }
    }
}

/// 一个 stream 的候选视图（ADR-0021 第 3 条①）。此后被移除的代对本视图仍可读。
#[derive(Debug, Clone, Default)]
pub struct Candidates {
    generations: Vec<CandidateGeneration>,
}

impl Candidates {
    pub(crate) fn new(generations: Vec<CandidateGeneration>) -> Self {
        Candidates { generations }
    }

    /// 按查询读到的快照号 Q 过滤（ADR-0021 第 3 条③），返回参与本查询的各代的批次。
    /// `q` 为 `None` 表示按 0 计（见 [`crate::chain::ChainReader::current`]）。判定只做一次，所以取走 `self`。
    pub fn participating(self, q: Option<SnapshotId>) -> Vec<RecordBatch> {
        self.generations
            .into_iter()
            .filter(|g| match g.binding.get() {
                None => true,
                Some(p) => q.is_none_or(|q| p > q),
            })
            .flat_map(|g| g.batches)
            .collect()
    }
}

/// 可见集的只读视图。
pub trait MemtableView: Send + Sync {
    /// 该 stream 的当前 schema（已提交部分 ∪ 未提交部分，带字段 id）。
    ///
    /// 可见集持有进程内目录中每个 stream 的当前 schema，所以返回 `None` 即该 stream 不在进程内目录中
    /// （查询据此回 400 `stream_not_found`，不读对象存储）。处于停用状态的数据集仍在目录中。
    fn schema(&self, dataset: &Dataset) -> Option<StreamColumns>;

    /// 取该 stream 当前各代的不可变候选视图（含活动代此刻的内容）；stream 不存在时为空。
    fn candidates(&self, dataset: &Dataset) -> Candidates;
}

/// 一代在其数据集内的编号：按创建顺序递增、不复用。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GenerationId(pub u64);

/// 可见集的写入一侧：摄取管道在批次确认之后、生命周期编排在启动与重放中调用。
pub trait MemtableWriter: Send + Sync {
    /// 把数据集放进进程内目录，以 `baseline` 为其当前 schema 的已提交部分：启动时自当前快照（找链尾所得）
    /// 读入的 `schema`，新建或创世快照待补提的数据集为 [`StreamColumns::genesis`]。
    /// 数据集已在目录中时不变。须在该数据集的第一次 [`MemtableWriter::write`] 之前调用。
    fn register(&self, dataset: &Dataset, baseline: StreamColumns);

    /// 写入一个已确认的批次：`position` 是承载这些记录的 WAL 条目位置，记录写进该数据集的活动代。
    /// 返回时这些记录已对查询可见；它们落入的在途列（[`MemtableWriter::reserve`]）就此确认、不再撤回，
    /// 未经登记的新列（重放）按记录顺序、同一记录内按字段出现的顺序并入当前 schema，
    /// 并各取「当前 schema 中最大字段 id + 1」（ADR-0022、REQ-AMB-STO-HOT-LAYOUT-V2）。
    /// 调用方保证：同一数据集的写入按 WAL 位置递增的顺序串行进行，且记录的字段名即目标列名、取值类型与该列相同。
    fn write(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>);

    /// 同 [`MemtableWriter::write`]，但不论内存上限整批留在内存（照常占用额度），不成为溢出记录：
    /// 只用于 `position` 上的 WAL 条目并不原样承载这些记录的批次（过时 `wal.dir` 重判后追加 WAL 失败），
    /// 它们溢出后无从读回；冻结时这一批也不切开（见 [`MemtableGenerations::freeze`]）。
    fn write_resident(&self, dataset: &Dataset, position: WalPosition, records: Vec<Record>);

    /// 同 [`MemtableWriter::write`]，但 `position` 上的 WAL 条目的前 `skip` 条记录已进入已提交快照，`records` 是其余的
    /// （WAL 重放：分段提交切在该条目中间，REQ-AMB-ING-HOT-BACKPRESSURE-V5）。溢出时读回该条目的第 `skip` 条起。
    fn write_tail(&self, dataset: &Dataset, position: WalPosition, skip: u32, records: Vec<Record>);

    /// 一个在途批次（已确定目标列、尚未应答）登记它的记录落入的列（REQ-AMB-DM-SCHEMA-DRIFT）：
    /// 当前 schema 中没有的列自此属于当前 schema（取「最大字段 id + 1」），尚未确认的列记下该批次的一次引用；
    /// 已确认的列不受影响。须在该批次追加 WAL 之前、与确定目标列在同一把锁内调用。
    fn reserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]);

    /// 该在途批次收到 503：撤回它对这些列的引用。尚未确认、且不再被任何在途批次引用的列退出当前 schema。
    fn unreserve(&self, dataset: &Dataset, columns: &[(String, ColumnType)]);
}

/// 冻结的一代：物化的输入，一次写入提交的全部记录；快照提交按它绑定与回执。
#[derive(Debug, Clone)]
pub struct FrozenGeneration {
    pub id: GenerationId,
    /// 本代记录所在 WAL 条目的首末位置：该数据集记录子序列中连续的一段（提交意图的 b 取 `last`）。
    pub first: WalPosition,
    pub last: WalPosition,
    /// `last` 上的条目只有前 k 条在本代及更早的代里时为 `Some(k)`（分段切在该条目中间）；整条都在为 `None`。
    pub last_partial: Option<u32>,
    /// 本代的记录数（内存中的与溢出的合计）。
    pub records: u64,
    /// 本代留在内存中的记录。列是冻结时该数据集当前 schema 的子集，写法同 [`CandidateGeneration`] 的批次
    /// （[`crate::schema::SchemaColumn::arrow_field`]：带字段 id，`_timestamp` 为不可空的 UTC 微秒时间戳）。
    pub batches: Vec<RecordBatch>,
    /// 本代的溢出记录（REQ-AMB-ING-HOT-BACKPRESSURE）：物化时从 WAL 读回。
    pub spilled: Vec<SpilledRecords>,
    /// 冻结时因溢出记录读回后的字节数达到内存上限而截断（少于条数上限也不再加长）。
    pub spill_capped: bool,
}

/// 冻结代中一段溢出记录：WAL 位置 `position` 上的记录条目自第 `skip` 条起的 `count` 条。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpilledRecords {
    pub position: WalPosition,
    pub skip: u32,
    pub count: u32,
}

/// 可见集的物化一侧：物化与快照提交调用（ADR-0021 第 1、2 条）。
pub trait MemtableGenerations: Send + Sync {
    /// 按确认的先后（WAL 中的先后，内存中的记录与溢出记录同样按此）从该数据集的活动代冻结一个前缀，成为新的冻结代：
    /// 至多 `max_records` 条，所含溢出记录读回后的字节数达到内存上限即截断（REQ-AMB-ING-HOT-BACKPRESSURE-V5）；
    /// 前缀可以切在一个 WAL 条目中间（经 [`MemtableWriter::write_resident`] 写入的一批除外，它整批冻结，
    /// 为前缀的第一批时可多于 `max_records`）。活动代没有记录时返回 `None`。
    /// 冻结代按冻结顺序提交，冻结后照常参与候选视图，直到回执。
    fn freeze(&self, dataset: &Dataset, max_records: u64) -> Option<FrozenGeneration>;

    /// 把最近冻结、尚未绑定的那一代退回活动代之首（它的提交没有发出过任何快照创建，换一个前缀重新冻结）。
    fn thaw(&self, dataset: &Dataset, generation: GenerationId);

    /// 把这些冻结代的绑定快照号置为 `p` 并使查询可见：在创建快照 `p` 之前调用；以新号重绑时再次调用（只增不减）。
    fn bind(&self, dataset: &Dataset, generations: &[GenerationId], p: SnapshotId);

    /// 回执：快照创建成功之后把这些代从可见集移除并归还它们所占的内存额度；已取出的候选视图仍持有它们。
    fn retire(&self, dataset: &Dataset, generations: &[GenerationId]);
}

mod table;

pub use table::{Memtable, to_batch as records_to_batch};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn batch(v: i64) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("v", DataType::Int64, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int64Array::from(vec![v]))]).unwrap()
    }

    fn value(b: &RecordBatch) -> i64 {
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0)
    }

    /// 候选取定之后，按绑定号与 Q 的大小决定该代是否从内存出（ADR-0021）。
    #[test]
    fn participating_follows_binding_and_q() {
        let unbound = Arc::new(Binding::default());
        let bound_5 = Arc::new(Binding::default());
        bound_5.bind(SnapshotId(5));
        let candidates = || {
            Candidates::new(vec![
                CandidateGeneration::new(Arc::clone(&bound_5), vec![batch(5)]),
                CandidateGeneration::new(Arc::clone(&unbound), vec![batch(0)]),
            ])
        };
        let values = |q| -> Vec<i64> { candidates().participating(q).iter().map(value).collect() };
        // Q = 0（只在本地 WAL 等情形）：全部参与
        assert_eq!(values(None), [5, 0]);
        // Q < P：绑定号 P > Q，从内存出
        assert_eq!(values(Some(SnapshotId(4))), [5, 0]);
        // Q ≥ P（快照 P 已写成，回执到否都一样）：只从文件出
        assert_eq!(values(Some(SnapshotId(5))), [0]);
        assert_eq!(values(Some(SnapshotId(6))), [0]);
    }

    /// 绑定号读的是判定那一刻的值：候选取定之后才绑定、快照写成的代同样被排除（ADR-0021）。
    #[test]
    fn binding_read_at_filter_time() {
        let g = Arc::new(Binding::default());
        let c = Candidates::new(vec![CandidateGeneration::new(
            Arc::clone(&g),
            vec![batch(1)],
        )]);
        g.bind(SnapshotId(3));
        g.bind(SnapshotId(2));
        assert_eq!(g.get(), Some(SnapshotId(3)), "绑定号只增不减");
        assert!(c.participating(Some(SnapshotId(3))).is_empty());
    }
}
