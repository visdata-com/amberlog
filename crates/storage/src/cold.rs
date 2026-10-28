//! 冷层表（`<storage.prefix>/_iceberg/<org>/<stream>/`，REQ-AMB-STO-COLD-TABLE）的读写接口。
//!
//! 职责分工：
//! - **冷层写入**（Iceberg）是冷层表目录下对象的唯一写者，由桶生命周期调用：入湖时重写并提交（[`ColdWriter::append`]），
//!   TTL 时做冷层删除提交（[`ColdWriter::delete`]），冷层孤儿清理时询问在途上传（[`ColdWriter::in_flight`]）；
//!   另有冷层合并提交（[`ColdUpkeep::replace`]）与冷层过期提交（[`ColdUpkeep::expire`]），其编排同样归桶生命周期。
//!   它不调用快照提交：冷层结果由桶生命周期带回，再交给 [`crate::commit::HotCommits`]。
//! - **冷层读取**（[`ColdReader`]）是读写共用的代码：跨层查询的冷侧直接读冷层表、不经冷层写入，
//!   桶生命周期（TTL 判到期、补做交接与补写冷层变更段、冷层孤儿清理）与冷层写入自己也用它。
//!
//! 读写共用的 Iceberg 代码放在 `amberlog-storage`，不放进 `amberlog-query`。
//! 本模块定接口与数据模型；对象存储上的实现在下面的子模块里，测试替身在 [`crate::testing`]。

use std::collections::{BTreeMap, HashSet};
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::ColumnType;
use arrow::datatypes::SchemaRef;
use bytes::Bytes;

use crate::BoxFuture;
use crate::bloom::{BloomColumns, FileBlooms, Pick};
use crate::chain::{
    ColdChangeSegment, ColdFileEntry, Digest, FileEntry, HandoverDraft, SnapshotId,
};
use crate::gate::DatasetState;
use crate::layout::HourBucket;
use crate::scan::Batches;
use crate::schema::SchemaColumn;

// 对象存储上的实现：表目录布局、列名与 schema 换算、冷层数据文件、确定冷层当前版本、冷层提交、读取、
// 入湖的冷层一半与冷层孤儿清理。上游只用 apache iceberg-rust 的核心 crate；`v<N>.metadata.json` 的只创建即提交、
// 页脚到清单条目的换算与清单改写自写（ADR-0042 第 13 条）。
/// 一个冷层数据文件逐批读出的批次（[`ColdReader::read_file`]）。
pub type ColdBatches = Batches<ColdReadError>;

pub mod commit;
pub mod compact;
pub mod current;
pub mod datafile;
pub mod error;
pub mod lake;
pub mod layout;
pub mod manifests;
pub mod names;
pub mod orphan;
pub mod read;
mod sorted;
pub mod store;
#[cfg(test)]
mod store_tests;

/// 冷层 schema 的一列：字段编号与热层相同，名字为冷层列名（只差大小写的后到者为 `<原名>__<k>`，ADR-0041）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdField {
    pub field_id: u32,
    /// 冷层列名，即冷层 Parquet 的物理列名。
    pub name: String,
    pub column_type: ColumnType,
}

/// 冷层快照的摘要属性 `amberlog.*` 所记的操作（REQ-AMB-STO-COLD-TABLE-V2、REQ-AMB-STO-LAKE-ENTRY、REQ-AMB-STO-TTL、
/// REQ-AMB-STO-COLD-COMPACTION）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdOperation {
    /// `amberlog.operation` 为 `lake`。
    Lake {
        /// `amberlog.bucket`
        bucket: HourBucket,
        /// `amberlog.hot_snapshot_id`：确定入湖输入时所读的热层快照号 H。
        hot_snapshot_id: SnapshotId,
        /// `amberlog.file_digests`：本次新增的冷层数据文件（相对冷层表位置）→ 写出时算的摘要。
        file_digests: BTreeMap<String, Digest>,
    },
    /// `amberlog.operation` 为 `ttl`。
    Ttl {
        /// `amberlog.ttl_cutoff`（UTC 微秒）。
        cutoff: i64,
    },
    /// `amberlog.operation` 为 `compaction`。
    Compaction {
        /// `amberlog.file_digests`：本次新增的合并产出（相对冷层表位置）→ 写出时算的摘要。
        file_digests: BTreeMap<String, Digest>,
    },
    /// 没有 `amberlog.operation`、取值不是 `lake`、`ttl` 或 `compaction`，或 `amberlog.*` 的写法不合（例如
    /// `amberlog.file_digests` 不是合法 JSON）：不是本系统写出的快照，交接窗口与补写冷层变更段都不认它。这不属内容无法解析
    /// （该判定只管元数据、清单列表、清单与数据文件本身，REQ-AMB-STO-COLD-TABLE），不报 [`ColdReadError::Integrity`]。
    Other,
}

/// 冷层当前版本元数据所列的一个快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdSnapshot {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    /// 快照的 `timestamp-ms`（冷层保留按它判按时长保留与第 ⑤ 类，REQ-AMB-STO-COLD-RETENTION）。
    pub timestamp_ms: i64,
    pub operation: ColdOperation,
}

/// 冷层的一个存活数据文件（清单条目）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdDataFile {
    /// 相对冷层表位置，不以 `/` 开头（`data/_timestamp_day=YYYY-MM-DD/<file_id>.parquet`）。
    pub path: String,
    /// 新增它的冷层快照（清单条目的 `snapshot_id`）：跨层查询据此判定它是否属交接窗口内的冷层提交。
    pub added_snapshot_id: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    /// 清单条目中 `_timestamp` 的下界与上界（两端均含）：TTL 按上界判到期，查询按两者剪枝。
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// 分区 `_timestamp_day`：自 1970-01-01 起的天数。
    pub day: i32,
}

/// 一张冷层表在某个版本下的内容。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdTable {
    /// 表位置（元数据的 `location`），与交接记录的 `table_location` 相同。
    pub location: String,
    /// 当前版本 N（`metadata/v<N>.metadata.json`）。
    pub metadata_version: u64,
    /// 当前快照；表已建而尚无快照时为 `None`。
    pub current_snapshot_id: Option<i64>,
    pub fields: Vec<ColdField>,
    /// 当前版本元数据所列的全部快照，按提交先后（序号从小到大）排列。
    pub snapshots: Vec<ColdSnapshot>,
    /// 当前快照的存活数据文件（按 [`ColdScope`] 只读了部分清单时，只含这些清单里的）。
    pub files: Vec<ColdDataFile>,
}

impl ColdTable {
    /// 当前版本中 `lake` 快照所记的桶（判定交接窗口要读热层当前快照中这些桶的桶 manifest）。
    pub fn lake_buckets(&self) -> Vec<HourBucket> {
        let mut buckets: Vec<_> = self
            .snapshots
            .iter()
            .filter_map(|s| match &s.operation {
                ColdOperation::Lake { bucket, .. } => Some(*bucket),
                _ => None,
            })
            .collect();
        buckets.sort();
        buckets.dedup();
        buckets
    }

    /// 处于交接窗口的冷层提交（REQ-AMB-STO-LAKE-ENTRY-WINDOW）：当前版本中摘要为 `lake`、桶 B、H 的快照，
    /// 热层当前快照中桶 B 仍有 `added_in` 不大于 H 的存活数据文件。`live` 给出热层当前快照中各桶的存活文件
    /// （桶 manifest 的 `files`），须含 [`ColdTable::lake_buckets`] 中仍在热层当前快照 `buckets` 里的每个桶；
    /// 不在 `live` 中的桶按没有存活文件处理。
    ///
    /// 跨层查询的去重、入湖的补做交接与 TTL 的避让共用这一判定（各块所称交接窗口都按 LAKE-ENTRY-WINDOW）。
    /// 该冷层提交新增的冷层数据文件按 [`ColdDataFile::added_snapshot_id`] 等于 [`WindowCommit::iceberg_snapshot_id`] 判定。
    pub fn handover_window(
        &self,
        live: &BTreeMap<HourBucket, Vec<FileEntry>>,
    ) -> Vec<WindowCommit> {
        self.snapshots
            .iter()
            .filter_map(|s| {
                let ColdOperation::Lake {
                    bucket,
                    hot_snapshot_id,
                    ..
                } = &s.operation
                else {
                    return None;
                };
                let inputs: Vec<String> = live
                    .get(bucket)?
                    .iter()
                    .filter(|f| f.added_in <= *hot_snapshot_id)
                    .map(|f| f.path.clone())
                    .collect();
                (!inputs.is_empty()).then_some(WindowCommit {
                    iceberg_snapshot_id: s.snapshot_id,
                    bucket: *bucket,
                    hot_snapshot_id: *hot_snapshot_id,
                    inputs,
                })
            })
            .collect()
    }

    /// 当前版本中冷层变更段尚未写出的 `ttl` 与 `compaction` 快照，按提交先后：快照号不在 `traced`（[`traced`]）中。
    ///
    /// TTL 不移出合并记录尚未写出的合并产出（REQ-AMB-STO-TTL-V2）、冷层保留不过期这些快照（REQ-AMB-STO-COLD-RETENTION
    /// 第 ④ 类）、冷层合并与 TTL 补写冷层变更段（REQ-AMB-STO-COLD-COMPACTION、REQ-AMB-STO-TTL）共用这一判定。
    /// 某个 `compaction` 快照的合并产出按 [`ColdDataFile::added_snapshot_id`] 等于其快照号判定（改写清单时未被合并的
    /// 条目保留原快照号，REQ-AMB-STO-COLD-COMPACTION）。
    pub fn untraced(&self, traced: &HashSet<i64>) -> Vec<&ColdSnapshot> {
        self.snapshots
            .iter()
            .filter(|s| {
                matches!(
                    s.operation,
                    ColdOperation::Ttl { .. } | ColdOperation::Compaction { .. }
                ) && !traced.contains(&s.snapshot_id)
            })
            .collect()
    }
}

/// 冷层变更段 `segments` 各元素的 `cold_removed_in` 之集：传入热层当前快照经 `cold_deletion_segment` 可达的全部段时，
/// 即已写出冷层变更段的冷层提交（[`ColdTable::untraced`]）。
pub fn traced(segments: &[impl std::ops::Deref<Target = ColdChangeSegment>]) -> HashSet<i64> {
    segments
        .iter()
        .flat_map(|s| s.deletions.iter().map(|d| d.cold_removed_in))
        .collect()
}

/// 一个处于交接窗口的冷层提交。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WindowCommit {
    pub iceberg_snapshot_id: i64,
    pub bucket: HourBucket,
    /// H（`amberlog.hot_snapshot_id`）。
    pub hot_snapshot_id: SnapshotId,
    /// 其入湖输入：热层当前快照中桶 B 里 `added_in` 不大于 H 的存活数据文件（相对数据集前缀）。
    pub inputs: Vec<String>,
}

/// 读取的结果。
#[derive(Debug, Clone, PartialEq)]
pub enum ColdState {
    /// 尚无冷层表：从 1 号起探测，`metadata/v1.metadata.json` 不存在（与提示存不存在无关；提示缺失、无法解析或所指版本
    /// 不存在时都从 1 号起探测，REQ-AMB-STO-COLD-TABLE）。
    NoTable,
    Table(Arc<ColdTable>),
}

/// 读哪些清单。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdScope {
    /// 当前快照的全部清单（TTL 不按分区摘要跳过，ADR-0044）。
    All,
    /// 只读清单列表的分区摘要与这些 UTC 日（自 1970-01-01 起的天数）有交集的清单（跨层查询）。
    Days(RangeInclusive<i32>),
}

/// 冷层读取失败，按 REQ-AMB-STO-HOT-LAYOUT 与 REQ-AMB-QRY-CROSS-TIER 的分类。
///
/// 读取**不改变数据集状态**：使数据集进入 manifest 不可读情形的是调用方（跨层查询按 REQ-AMB-QRY-CROSS-TIER；
/// TTL 与冷层孤儿清理的读取不使数据集停用，REQ-AMB-STO-TTL、REQ-AMB-STO-COLD-TABLE）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdReadError {
    /// 对象存储不可访问。
    Unavailable(String),
    /// 所需对象不存在或内容无法解析。
    Integrity(String),
    /// 只由 [`ColdReader::read_file`] 返回：该冷层数据文件对象不存在。跨层查询对它重判（REQ-AMB-QRY-CROSS-TIER），
    /// 其余调用方按 [`ColdReadError::Integrity`] 处理。
    NotFound(String),
}

/// 冷层表的读取（读写共用）。
pub trait ColdReader: Send + Sync {
    /// 找冷层当前版本（读提示后逐个探测，REQ-AMB-STO-COLD-TABLE），读其元数据与 `scope` 所含的清单：
    /// 即 [`ColdReader::version`] 之后接 [`ColdReader::table_at`]。
    fn table<'a>(
        &'a self,
        dataset: &'a Dataset,
        scope: ColdScope,
    ) -> BoxFuture<'a, Result<ColdState, ColdReadError>>;

    /// 只确定冷层当前版本（读提示后逐个 HEAD 探测），不读元数据；`None` 即尚无冷层表。
    /// 跨层查询把这一步与读元数据、清单列表、清单分开报错（REQ-AMB-QRY-SEARCH-ENDPOINT-V6 的报告顺序）。
    fn version<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<u64>, ColdReadError>>;

    /// 读已写出的版本 `version`（由 [`ColdReader::version`] 得到）的元数据与 `scope` 所含的清单。
    fn table_at<'a>(
        &'a self,
        dataset: &'a Dataset,
        version: u64,
        scope: ColdScope,
    ) -> BoxFuture<'a, Result<Arc<ColdTable>, ColdReadError>>;

    /// `table` 所列快照 `snapshot_id` 下的全部存活数据文件（TTL 补写冷层变更段时，与其父快照的相减得出该次移除的文件）。
    fn snapshot_files<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<Vec<ColdDataFile>, ColdReadError>>;

    /// `table` 所列快照 `snapshot_id` 经其清单列表与清单引用的冷层数据文件（相对冷层表位置；清单条目为任何状态都算），
    /// 供冷层保留判第 ⑥ 类（REQ-AMB-STO-COLD-RETENTION）。
    fn snapshot_references<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
        snapshot_id: i64,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>>;

    /// `table` 当前版本中任一快照引用的全部对象（数据文件、清单与清单列表，相对冷层表位置），供冷层孤儿判定。
    ///
    /// 冷层孤儿清理的判定顺序必须是：列举冷层表目录 → 取 [`ColdWriter::in_flight`] → 读当前版本（[`ColdReader::table`]）
    /// 与本方法。反过来（先读本方法再取在途）时，其间完成的冷层提交新增的文件两边都不在，会被误删（REQ-AMB-STO-COLD-TABLE）。
    fn referenced<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<HashSet<String>, ColdReadError>>;

    /// 读一个冷层数据文件（`path` 相对冷层表位置）的完整字节：冷层合并据此核对写入时摘要，重写也只用这一份
    /// （REQ-AMB-STO-COLD-COMPACTION）。对象不存在时返回 [`ColdReadError::NotFound`]。
    fn file_bytes<'a>(
        &'a self,
        dataset: &'a Dataset,
        path: &'a str,
    ) -> BoxFuture<'a, Result<Bytes, ColdReadError>>;

    /// 冷层数据文件 `file` 中 `picks` 各列逐行组的等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM，只读尾部元数据与 bloom）；
    /// 读不到或尾部元数据无法解析为 `None`，即不下结论。缺省为 `None`（替身不提供 bloom）。
    fn blooms<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a ColdDataFile,
        picks: &'a [Pick],
    ) -> BoxFuture<'a, Option<FileBlooms>> {
        let _ = (dataset, file, picks);
        Box::pin(async { None })
    }

    /// 按字段编号逐批读一个冷层数据文件：每批的 schema 恰为 `target`，其每列按该列 `PARQUET:field_id` 元数据对到文件中
    /// 同一字段编号的列，文件中没有的为空值（REQ-AMB-STO-COLD-TABLE、REQ-AMB-QRY-CROSS-TIER）。只需解码 `target` 所含的
    /// 列；`time`（UTC 微秒，左闭右开）之外的记录可以不交出，也可以照常交出（由调用方过滤）。打开时的失败在返回之前
    /// 报出，解码中途的失败作为迭代中的一项报出。
    fn read_file<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
        file: &'a ColdDataFile,
        target: SchemaRef,
        time: Range<i64>,
    ) -> BoxFuture<'a, Result<ColdBatches, ColdReadError>>;
}

/// 一次入湖要写进冷层的内容（REQ-AMB-STO-LAKE-ENTRY）。
#[derive(Debug, Clone)]
pub struct ColdAppend {
    pub bucket: HourBucket,
    /// H：确定入湖输入时所读的热层快照号（记为 `amberlog.hot_snapshot_id`）。
    pub hot_snapshot_id: SnapshotId,
    /// 入湖开始时的热层 schema（字段编号与类型），冷层 schema 按它追加。
    pub schema: Vec<SchemaColumn>,
    /// 全部入湖输入的完整字节（已核对摘要的那一份）：热层 Parquet 文件。
    pub inputs: Vec<Bytes>,
    /// 写等值 bloom 的列（按 `schema` 与本进程的索引提示算出，REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ②）。
    /// 不参与「同一次提交」的认定：同一进程的同一请求所算出的相同。
    pub bloom: BloomColumns,
}

/// 一次冷层删除提交的内容（REQ-AMB-STO-TTL）。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdDelete {
    /// 该次 TTL 执行的截止时刻（记为 `amberlog.ttl_cutoff`）。
    pub cutoff: i64,
    /// 判为到期的冷层数据文件，相对冷层表位置。
    pub paths: Vec<String>,
}

/// 一次冷层删除提交的结果。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdDeleted {
    pub metadata_version: u64,
    /// 该次冷层提交的 Iceberg 快照号（冷层变更段各元素的 `cold_removed_in`）。
    pub iceberg_snapshot_id: i64,
    /// 被标为已删除的文件，相对冷层表位置。
    pub removed: Vec<String>,
}

/// 冷层写入没有完成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdWriteError {
    /// 数据集处于停用状态：不写也不删。
    Disabled(DatasetState),
    /// 重试耗尽、各次请求都未出现对象存储不可访问：数据集已进入 manifest 不可读情形（实现已把状态记入
    /// [`crate::gate::DatasetGates`]）。
    Exhausted,
    /// 对象存储不可访问，本次放弃。
    Unavailable(String),
    /// 所需冷层对象不存在或内容无法解析（是否停用见 REQ-AMB-STO-COLD-TABLE，实现已记入闸门的随之为 [`Self::Disabled`]）。
    Integrity(String),
    /// 删除请求所列文件一个都不在冷层当前存活集中，或合并请求的某个输入已不在其中，本次放弃。
    Stale,
    /// 入湖把入湖输入重写为冷层数据文件时失败，未写出冷层提交（REQ-AMB-STO-LAKE-ENTRY-V2-REWRITE-FAIL）。
    Rewrite(String),
}

/// 冷层写入（`_iceberg/` 的唯一写者）。同一数据集的冷层提交在进程内依次进行（REQ-AMB-STO-COLD-TABLE）。
pub trait ColdWriter: Send + Sync {
    /// 入湖：把 `request.inputs` 的全部记录重写为冷层 Parquet，追加冷层 schema，做恰好一次冷层提交；
    /// 返回写交接记录所需的内容。
    ///
    /// 返回 [`ColdWriteError::Unavailable`] 时提交结果可能不明（`v<N+1>` 已创建而读回不可访问）：调用方下一周期以**同一请求**
    /// 再调，实现按数据集保留进度，以 (`bucket`, `hot_snapshot_id`, 各输入的摘要) 认作同一次提交，用同号、逐字节相同的
    /// 元数据续做（REQ-AMB-STO-COLD-TABLE 同号同内容）；请求变了按新的一次提交处理。
    fn append<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdAppend,
    ) -> BoxFuture<'a, Result<HandoverDraft, ColdWriteError>>;

    /// 冷层删除提交：把 `request.paths` 中仍在冷层当前存活集里的文件整个标为已删除（不写行级删除文件，不物理删除），
    /// [`ColdDeleted::removed`] 列出实际标为已删除的；一个都不存活时返回 [`ColdWriteError::Stale`]。
    ///
    /// 返回 [`ColdWriteError::Unavailable`] 后的续做同 [`ColdWriter::append`]，以 (`cutoff`, `paths`) 认作同一次提交。
    fn delete<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdDelete,
    ) -> BoxFuture<'a, Result<ColdDeleted, ColdWriteError>>;

    /// 本进程正在进行的冷层提交已上传、尚未被已写出的版本引用的对象（相对冷层表位置）：冷层孤儿清理不删它们。
    /// 取用的先后见 [`ColdReader::referenced`]。
    fn in_flight(&self, dataset: &Dataset) -> HashSet<String>;
}

/// 一次冷层合并要写进冷层的内容（REQ-AMB-STO-COLD-COMPACTION）。
#[derive(Debug, Clone)]
pub struct ColdReplace {
    /// 合并输入所在的日分区 `_timestamp_day`（自 1970-01-01 起的天数），合并产出写在同一分区。
    pub day: i32,
    /// 全部合并输入：(相对冷层表位置的路径, 核对写入时摘要时读出的完整字节)。重写只用这些字节、不另读。
    pub inputs: Vec<(String, Bytes)>,
    /// 写等值 bloom 的列（按热层快照 `schema` 与本进程的索引提示算出，REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ③）。
    pub bloom: BloomColumns,
}

/// 一次冷层合并提交的结果。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdReplaced {
    pub metadata_version: u64,
    /// 该次冷层合并提交的 Iceberg 快照号（冷层变更段各 `compaction` 元素的 `cold_removed_in`）。
    pub iceberg_snapshot_id: i64,
    /// 全部合并产出，即写进冷层变更段 `additions` 的合并记录（摘要与 `amberlog.file_digests` 中的相同）。
    pub outputs: Vec<ColdFileEntry>,
}

/// 冷层保留在一次冷层过期提交的每次尝试中判定要移除的快照（REQ-AMB-STO-COLD-RETENTION）。
pub trait ExpireSelect: Send + Sync {
    /// `table` 是该次尝试所基于的冷层当前版本（读了全部清单）；返回其中要移除的快照号，空即不写。
    /// 受保护与否按 `table` 与实现自行重新读取的热层当前快照判定。读取未全部成功时返回 `Err`，本次执行放弃。
    fn select<'a>(
        &'a self,
        dataset: &'a Dataset,
        table: &'a ColdTable,
    ) -> BoxFuture<'a, Result<Vec<i64>, ColdReadError>>;
}

/// 一次冷层过期提交的请求。
#[derive(Clone)]
pub struct ColdExpire {
    pub select: Arc<dyn ExpireSelect>,
}

/// 一次冷层过期提交的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColdExpired {
    pub metadata_version: u64,
    /// 被移除的快照号。
    pub removed: Vec<i64>,
}

/// 冷层写入的另两种冷层提交（编排归桶生命周期，提交经冷层写入）。与 [`ColdWriter`] 的提交同属
/// 一个数据集进程内的同一串冷层提交，依次进行，不与其他冷层提交合写（REQ-AMB-STO-COLD-TABLE-V2）。
pub trait ColdUpkeep: Send + Sync {
    /// 冷层合并提交：把 `request.inputs` 的字节重写为按 `_timestamp` 有序、同一日分区、不超过
    /// `compaction.target_file_bytes` 的合并产出并上传，然后做恰好一次冷层提交：合并输入的清单条目标为 `status` 2，
    /// 其余条目保留 `snapshot_id`、`sequence_number` 与 `file_sequence_number`；快照摘要 `amberlog.operation` 为
    /// `compaction`、带 `amberlog.file_digests`，标准 `operation` 为 `replace`。合并产出自上传第一个文件起登记为在途
    /// （[`ColdWriter::in_flight`]）。
    ///
    /// 每次尝试都按所基于的冷层当前版本重核：任一合并输入已不存活即返回 [`ColdWriteError::Stale`]，不写提交，
    /// 已上传的合并产出撤出在途、留给冷层孤儿清理。返回 [`ColdWriteError::Unavailable`] 后的续做同
    /// [`ColdWriter::append`]，以 (各输入的路径与摘要) 认作同一次提交。
    fn replace<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdReplace,
    ) -> BoxFuture<'a, Result<ColdReplaced, ColdWriteError>>;

    /// 冷层过期提交：每次尝试以所基于的冷层当前版本调用 `request.select`（换号重试时重新调用），把选出的快照从元数据中
    /// 移除；`snapshot-log` 去掉指向已不存在的快照的条目及其之前的全部条目。不新增快照，不改 `current-snapshot-id`、
    /// `refs`、schema、分区规格与表属性，不删除任何对象。选出为空时不写，返回 `Ok(None)`。
    ///
    /// 读取（含 `select`）未全部成功时只放弃本次，不使数据集停用（REQ-AMB-STO-COLD-TABLE-V2）：对象存储不可访问返回
    /// [`ColdWriteError::Unavailable`]，其余返回 [`ColdWriteError::Integrity`]。重试耗尽按冷层提交的通则返回
    /// [`ColdWriteError::Exhausted`]。返回 `Unavailable` 时提交结果可能不明，下一次调用先以同号、逐字节相同的元数据续做。
    fn expire<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdExpire,
    ) -> BoxFuture<'a, Result<Option<ColdExpired>, ColdWriteError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, added_in: u64) -> FileEntry {
        FileEntry {
            path: path.into(),
            min_timestamp: 0,
            max_timestamp: 0,
            record_count: 1,
            size_bytes: 1,
            content_digest: Digest::of(path.as_bytes()),
            added_in: SnapshotId(added_in),
            column_stats: None,
        }
    }

    fn lake(id: i64, bucket: HourBucket, h: u64) -> ColdSnapshot {
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

    #[test]
    fn untraced_lists_ttl_and_compaction_without_segment() {
        let snap = |id, operation| ColdSnapshot {
            snapshot_id: id,
            parent_snapshot_id: None,
            timestamp_ms: 0,
            operation,
        };
        let table = ColdTable {
            location: "file:///x/amberlog/_iceberg/default/app".into(),
            metadata_version: 5,
            current_snapshot_id: Some(5),
            fields: Vec::new(),
            snapshots: vec![
                lake(1, HourBucket::of(0), 3),
                snap(2, ColdOperation::Ttl { cutoff: 0 }),
                snap(
                    3,
                    ColdOperation::Compaction {
                        file_digests: BTreeMap::new(),
                    },
                ),
                snap(4, ColdOperation::Other),
                snap(5, ColdOperation::Ttl { cutoff: 9 }),
            ],
            files: Vec::new(),
        };
        let ids = |traced: &HashSet<i64>| -> Vec<i64> {
            table
                .untraced(traced)
                .iter()
                .map(|s| s.snapshot_id)
                .collect()
        };
        assert_eq!(ids(&HashSet::new()), vec![2, 3, 5]);
        assert_eq!(ids(&[2, 5].into()), vec![3]);
        assert_eq!(ids(&[1, 2, 3, 4, 5].into()), Vec::<i64>::new());
    }

    #[test]
    fn handover_window_follows_added_in_and_h() {
        let b1 = HourBucket::of(0);
        let b2 = HourBucket::of(3_600_000_000);
        let b3 = HourBucket::of(7_200_000_000);
        let table = ColdTable {
            location: "file:///x/amberlog/_iceberg/default/app".into(),
            metadata_version: 4,
            current_snapshot_id: Some(4),
            fields: Vec::new(),
            snapshots: vec![
                lake(1, b1, 5),
                lake(2, b2, 7),
                ColdSnapshot {
                    snapshot_id: 3,
                    parent_snapshot_id: Some(2),
                    timestamp_ms: 0,
                    operation: ColdOperation::Ttl { cutoff: 0 },
                },
                lake(4, b3, 9),
            ],
            files: Vec::new(),
        };
        assert_eq!(table.lake_buckets(), vec![b1, b2, b3]);
        let live = BTreeMap::from([
            // b1：交接已写，只剩入湖开始之后进来的迟到文件
            (b1, vec![file("data/late.parquet", 6)]),
            // b2：交接未写，入湖输入仍在，另有迟到文件
            (
                b2,
                vec![
                    file("data/a.parquet", 3),
                    file("data/b.parquet", 7),
                    file("data/late2.parquet", 8),
                ],
            ),
            // b3 不在热层当前快照中
        ]);
        assert_eq!(
            table.handover_window(&live),
            vec![WindowCommit {
                iceberg_snapshot_id: 2,
                bucket: b2,
                hot_snapshot_id: SnapshotId(7),
                inputs: vec!["data/a.parquet".into(), "data/b.parquet".into()],
            }]
        );
    }
}
