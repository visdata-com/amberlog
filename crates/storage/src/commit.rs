//! 快照提交（Commit）面向其他元素的接口：启动时的基线读取、提交意图的判明、数据集存在性与停机 flush。
//!
//! 提交流程本身（上传数据文件 → 找链尾 → 桶 manifest 与段 → 意图 → 绑定 → 快照「仅当不存在时创建」→ 提示 → 回执）、
//! 物化的触发、与可见集之间的绑定与回执、向 WAL 发段回收通知，都在 amberlog-storage 内部完成
//! （REQ-AMB-STO-MANIFEST-COMMIT、REQ-AMB-STO-HOT-LAYOUT），不在本接口内。
//! 查询读取快照链走 [`crate::chain::ChainReader`]，不经本接口。
//!
//! 另有四种热层提交（REQ-AMB-STO-MANIFEST-COMMIT-V3「何时提交」）由桶生命周期发出，经 [`HotCommits`]：
//! 入湖交接提交、TTL 热层提交、冷层变更段提交与归档提交。它们与写入提交、合并提交、巡检留痕提交
//! 同属该数据集进程内的同一条提交队列，按同一协议提交。

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{ColdDeletionReason, ColdFileEntry, Digest, HandoverDraft, SnapshotId};
use crate::committer::CommitError;
use crate::layout::HourBucket;
use crate::schema::StreamColumns;
use crate::store::Unavailable;
use crate::wal::WalPosition;

/// 提交意图 (stream, P, b, d)（ADR-0021 第 4 条）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitIntent {
    pub dataset: Dataset,
    /// P：即将写出的快照号。
    pub snapshot_id: SnapshotId,
    /// b：本次所含最后一代在该 stream 记录子序列中的末位 WAL 位置（创世快照取该 stream 记录子序列的起点）。
    pub through: WalPosition,
    /// b 所指条目只有前 k 条在本次及更早的提交里时为 `Some(k)`（分段提交切在该条目中间，REQ-AMB-ING-HOT-BACKPRESSURE-V5）；
    /// 其余记录留给下一次提交。整条都在时为 `None`。
    pub through_partial: Option<u32>,
    /// d：快照 P 完整字节的摘要。
    pub digest: Digest,
    /// 本次提交上传的数据文件路径（相对数据集前缀；写入提交与合并提交记，其余提交为空）。快照 P 已被快照回收时，
    /// 按完整删除台账中 `added_in` 等于 P 的条目是否含这些路径判明（REQ-AMB-STO-MANIFEST-COMMIT-V4）。
    pub files: Vec<String>,
}

/// 启动时读到的一个已存在数据集（REQ-AMB-CFG-STARTUP-V2、REQ-AMB-DM-NAMESPACE）。
#[derive(Debug, Clone, PartialEq)]
pub struct Baseline {
    pub dataset: Dataset,
    /// schema 基线所在的快照号：按找链尾所得的当前快照；尚无快照时为 `None`。
    pub snapshot_id: Option<SnapshotId>,
    /// 该快照的 `schema`。数据集已处于停用状态时为 `None`（它仍在进程内目录中，但其 schema 不得用于判定）。
    pub columns: Option<StreamColumns>,
}

/// [`Committer::ensure_dataset`] 的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Existence {
    /// 该数据集已有 1 号快照（含本次刚提交成功的创世快照）。
    Committed,
    /// 创世快照尚未提交成功：调用方须把 [`crate::wal::WalEntry::DatasetExists`] 随本批一同追加（空批次亦然），
    /// 创世快照按普通提交失败继续重试；追加失败则本请求以 5xx 结束（REQ-AMB-DM-NAMESPACE）。
    Pending,
}

/// 一条未判明的提交意图的判明结果（ADR-0021 第 4 条经 ADR-0043 第 6 条改写，按摘要分三种，另加停用）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntentOutcome {
    /// 快照 P 存在且摘要等于 d：快照创建成功即提交完成，(上一水位, b] 的记录已提交。
    Committed,
    /// 快照 P 存在但摘要不等于 d（他人所写）：(上一水位, b] 是滞留记录，按 `wal.dir` 过时条款重判。
    Foreign,
    /// 快照 P 不存在：解绑，这些记录按未提交代重新物化、提交。
    Absent,
    /// 判明过程中数据集进入了停用状态：滞留记录原样保留，不作重判。
    Disabled,
}

/// 快照提交。
pub trait Committer: Send + Sync {
    /// 开始受理 `/api/` 请求之前，读入全部在对象存储中存在的数据集的基线（按找链尾所得的当前快照）。
    /// 读到的停用状态记入 [`crate::gate::DatasetGates`]。
    /// 列举或读取因对象存储不可访问而失败时返回 `Err`，启动以退出码 3 失败。
    fn load(&self) -> BoxFuture<'_, Result<Vec<Baseline>, Unavailable>>;

    /// 判明 WAL 重放中遇到的一条未判明的提交意图。
    fn resolve_intent(
        &self,
        intent: CommitIntent,
    ) -> BoxFuture<'_, Result<IntentOutcome, Unavailable>>;

    /// 确保数据集存在：已知 1 号快照存在则直接返回；否则把数据集放进进程内目录，
    /// 尝试提交创世快照（其意图先记入 WAL），成功返回 [`Existence::Committed`]，失败返回 [`Existence::Pending`]
    /// 且不改变应答。调用方须已通过停机闸门与数据集状态闸门。
    fn ensure_dataset<'a>(&'a self, dataset: &'a Dataset) -> BoxFuture<'a, Existence>;

    /// 停机时的 flush 与提交：冻结全部活动代并提交，处于两种停用状态的数据集除外；
    /// 返回时每个数据集的这一次提交都已结束（成功或重试用尽）。
    fn flush_all(&self) -> BoxFuture<'_, ()>;
}

/// 一次热层提交创建的快照。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Committed {
    pub snapshot_id: SnapshotId,
    /// 该快照的 `created_at`（UTC 微秒）：本次留痕的 `deleted_at`，延后物理删除据此起算（REQ-AMB-STO-AUDIT-INVARIANTS-V3）。
    pub created_at: i64,
}

/// 入湖交接提交的内容（REQ-AMB-STO-LAKE-ENTRY）。
#[derive(Debug, Clone, PartialEq)]
pub struct HandoverRequest {
    pub bucket: HourBucket,
    /// H：确定入湖输入时所读的热层快照号。父快照中该桶 `added_in` 不大于 H 的存活文件（且只有它们）离开存活集，
    /// 各记 `lake` 留痕；一个也没有时返回 [`CommitError::Stale`]（交接已写过）。正常交接与崩溃后的补做用同一条规则。
    pub hot_snapshot_id: SnapshotId,
    /// 该次冷层提交的结果，写成该桶 `handovers` 末尾的一条交接记录。
    pub handover: HandoverDraft,
}

/// TTL 热层提交的内容（REQ-AMB-STO-TTL）：同一次执行判为到期的全部热层数据文件，一次提交。
#[derive(Debug, Clone, PartialEq)]
pub struct TtlHotRequest {
    /// 该次执行的截止时刻（UTC 微秒），记进各 `ttl` 留痕的 `cutoff`。
    pub cutoff: i64,
    /// (路径所示小时桶, 相对数据集前缀的路径)。父快照（判定之后可能已因合并或入湖交接而变）中只移除仍存活的那些；
    /// 一个都不存活时返回 [`CommitError::Stale`]。各 `ttl` 留痕的 `added_in`、`max_timestamp` 与 `content_digest` 由快照提交
    /// 取自父快照中该文件的 `files` 条目。
    pub expired: Vec<(HourBucket, String)>,
}

/// 冷层变更段提交的内容（REQ-AMB-STO-AUDIT-INVARIANTS-V4、REQ-AMB-STO-TTL、REQ-AMB-STO-COLD-COMPACTION）：写出一个新冷层
/// 变更段并以 `cold_deletion_segment` 引用它，`summary.cold` 随之更新（先去掉 `paths`，再加入 `additions`）。
///
/// `deletions` 各元素的 `content_digest` 与 `max_timestamp` 由快照提交取自该文件所在的交接记录 `files` 元素或合并记录
/// （已记入冷层文件集），调用方只给路径；某路径不在父快照的已记入冷层文件集中时返回 [`CommitError::Stale`]。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdChangeRequest {
    /// 移除这些文件的冷层提交的 Iceberg 快照号（各元素的 `cold_removed_in`）。
    pub cold_removed_in: i64,
    pub reason: ColdDeletionReason,
    /// `reason` 为 `ttl` 时为该次 TTL 执行的截止时刻，否则为 `None`。
    pub cutoff: Option<i64>,
    /// 被移除的冷层数据文件，相对冷层表位置。
    pub paths: Vec<String>,
    /// 写进段 `additions` 的合并记录：`reason` 为 `compaction` 时为该次合并的全部合并产出，否则为空。
    pub additions: Vec<ColdFileEntry>,
}

/// 归档提交的内容（REQ-AMB-STO-HOT-LAYOUT-V4「移入留痕归档段」）：写出一个新留痕归档段，其 `buckets` 含这些桶在父快照中
/// 的桶 manifest 引用，新快照不再列入这些桶。任一桶已不在父快照的 `buckets` 中、或仍有存活数据文件时返回
/// [`CommitError::Stale`]。
#[derive(Debug, Clone, PartialEq)]
pub struct ArchiveRequest {
    pub buckets: Vec<HourBucket>,
}

/// 桶生命周期发出的四种热层提交（由 `amberlog-storage` 的快照提交实现，桶生命周期调用）。
///
/// 共同约定：
/// - 每次调用是一轮提交：创建成功返回 [`Committed`]；数据集处于停用状态返回 [`CommitError::Disabled`]，不写任何对象；
///   某次尝试遇对象存储不可访问即结束本轮、返回 [`CommitError::Unavailable`]，调用方下一周期重新判定后再发。
///   每次尝试都按所绑定的父快照重新核对请求，已生效的请求再发时得到 [`CommitError::Stale`]。
/// - 段号、`previous`、`removed_in`、`deleted_at`、交接记录的 `snapshot_id` 与 `summary.cold` 都由快照提交在绑定快照号时
///   定下，真冲突换号时随之重写（REQ-AMB-STO-MANIFEST-COMMIT-V3）。
pub trait HotCommits: Send + Sync {
    /// 入湖交接提交。
    fn commit_handover<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: HandoverRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>>;

    /// TTL 热层提交。
    fn commit_ttl_hot<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: TtlHotRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>>;

    /// 冷层变更段提交。
    fn commit_cold_change<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdChangeRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>>;

    /// 归档提交。
    fn commit_archive<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ArchiveRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>>;
}
