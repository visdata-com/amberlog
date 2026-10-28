//! 耐久缓冲（WAL）的接口：耐久写入、重放与段回收。
//!
//! 实现（分段日志、逐条 CRC32C、合并 fsync、截断损坏尾部，REQ-AMB-LIFE-DURABILITY）在子模块 `file`。
//! 条目种类取自 ADR-0021 第 4 条与 ADR-0023 第 1、4 条；
//! 新增条目种类时在 [`WalEntry`] 上增补，并在 `wal/codec.rs` 分配种类码。

use std::fmt;

use amberlog_common::dataset::Dataset;
use amberlog_common::record::Record;

use crate::BoxFuture;
use crate::chain::{Digest, SnapshotId};
use crate::commit::CommitIntent;

/// 条目在 WAL 中的位置：全序、自 1 起，一次追加的各条目取得连续的位置。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct WalPosition(pub u64);

impl WalPosition {
    /// 用于段回收时表示「该类条目此后一律不阻止删段」（例如计数对象已是更高格式版本的小时，ADR-0023 第 5 条）。
    pub const MAX: WalPosition = WalPosition(u64::MAX);
}

/// 一个 WAL 条目。
#[derive(Debug, Clone, PartialEq)]
pub enum WalEntry {
    /// 一个 `_json` 请求中被接受的全部记录，按 `index` 升序（重放时不再施加受理校验）。
    Records {
        dataset: Dataset,
        records: Vec<Record>,
    },
    /// 数据集的存在（REQ-AMB-DM-NAMESPACE）：[`crate::commit::Existence::Pending`] 时随本批一同追加，空批次亦然。
    DatasetExists { dataset: Dataset },
    /// 一个被确认（200）的 `_json` 批次的计数增量，与记录同一次追加（ADR-0023 第 1 条及其注记）。
    Counts {
        dataset: Dataset,
        /// 服务端接收时刻所在整分钟的起点，UTC 微秒。
        window_start_us: i64,
        received: u64,
        durable: u64,
        /// 本批响应的 `failed`（REQ-AMB-OBS-INGEST-COUNTERS-V2 的 `rejected`）。
        rejected: u64,
        /// 本请求合法的来源标识（`X-Amberlog-Source`）；未带时为 `None`。
        source: Option<String>,
        /// 本批被接受的记录按 `_timestamp` 所在整分钟分组的条数（窗口起点 UTC 微秒升序，REQ-AMB-OBS-INGEST-COUNTERS-V3
        /// 的 `durable_ts`）；`None` 只出现在读出早期版本写下的条目时，其分组要按紧随其后的记录条目重算。
        durable_ts: Option<Vec<(i64, u64)>>,
    },
    /// 一个 `_sent` 请求的全部元素（ADR-0023 第 1 条）。
    Sent {
        dataset: Dataset,
        elements: Vec<SentElement>,
    },
    /// 提交意图 (stream, P, b, d)，在创建快照 P 之前追加（ADR-0021 第 4 条）。
    CommitIntent(CommitIntent),
    /// 数据集在本目录第一次受理记录时的 schema 基线快照号；尚无任何快照时为 `None`（ADR-0021 第 4 条）。
    Baseline {
        dataset: Dataset,
        snapshot_id: Option<SnapshotId>,
    },
    /// 计数意图 (数据集, 小时, x, 待写内容的摘要)，在覆盖写计数对象之前追加（ADR-0023 第 4 条）。
    CountIntent {
        dataset: Dataset,
        /// 该小时的起点，UTC 微秒。
        hour_start_us: i64,
        through: WalPosition,
        digest: Digest,
    },
    /// 该数据集位置不超过 `through` 的记录条目作废：重放按 `wal.dir` 过时条款重判其滞留记录之后追加，
    /// 相容者随后以新位置重新追加，冲突者由此丢弃、不再复活（REQ-AMB-LIFE-DURABILITY）。
    /// 对段回收而言，它同时就是该数据集位置不超过 `through` 的记录的回收通知。
    Discard {
        dataset: Dataset,
        through: WalPosition,
    },
}

/// `_sent` 的一个元素（REQ-AMB-OBS-INGEST-COUNTERS-V3）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SentElement {
    pub source: String,
    /// UTC 微秒，已对齐到整分钟。
    pub window_start_us: i64,
    pub sent: u64,
    pub basis: Basis,
}

/// `_sent` 元素的 `basis`：发送数按什么归分钟（REQ-AMB-OBS-INGEST-COUNTERS-V3）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub enum Basis {
    /// 按送达（缺省），计入 `sent`。
    #[default]
    Delivery,
    /// 按记录时间戳，计入 `sent_ts`。
    Timestamp,
}

/// 段回收的通知（ADR-0021 第 4 条、ADR-0023 第 5 条）。
/// 一个段的全部条目都被某条通知覆盖之后，WAL 才可以删除它；提交意图与基线快照号另按 ADR-0021 第 4 条保留。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Release {
    /// ① 该数据集位置不超过 `through` 的记录都已进入已提交快照，或已按 `wal.dir` 过时条款被丢弃。
    Records {
        dataset: Dataset,
        through: WalPosition,
    },
    /// ② 该数据集该小时位置不超过 `through` 的计数增量与 `_sent` 元素都已写入计数对象
    /// （计数对象已是更高格式版本或内容无法解析的小时，以 [`WalPosition::MAX`] 通知）。
    Counts {
        dataset: Dataset,
        hour_start_us: i64,
        through: WalPosition,
    },
    /// ③ 该数据集的 `_current` 已存在：它的存在性条目都已体现。
    Existence { dataset: Dataset },
}

/// WAL 的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalError {
    /// 写入或 fsync 失败：本次追加的条目作废，重放时不复活（ADR-0021 前提③）；摄取向上表达为 503，不得降级为 200。
    Write(String),
    /// 重放遇到非尾部损坏且无法无损恢复：启动以退出码 3 失败；运行期读回已确认的条目读不出（段读不出或帧校验失败）：
    /// 以退出码 3 退出（REQ-AMB-ING-HOT-BACKPRESSURE-V2）。
    Corrupt(String),
}

impl fmt::Display for WalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            WalError::Write(m) => write!(f, "WAL write failed: {m}"),
            WalError::Corrupt(m) => write!(f, "WAL is corrupt: {m}"),
        }
    }
}

impl std::error::Error for WalError {}

/// 耐久缓冲。
pub trait Wal: Send + Sync {
    /// 耐久写入：按给出的顺序追加 `entries`（不得为空），返回最后一个条目的位置。
    /// 位置在调用时即排定：先调用者的条目位置在前，不论返回的 future 何时被等待（摄取管道据此在判定锁内排定 WAL 顺序）。
    ///
    /// 返回 `Ok` 之后，进程以任何方式终止并以同一 `wal.dir` 重启，这些条目必然在重放中出现；
    /// 返回 `Err` 时这些条目一个也不会在重放中出现。
    fn append(&self, entries: Vec<WalEntry>) -> BoxFuture<'_, Result<WalPosition, WalError>>;

    /// 重放：把全部已确认的条目按位置顺序交给 `sink`，截断的损坏尾部不含任何已确认条目。
    /// 在首次 [`Wal::append`] 之前调用一次。
    fn replay(&self, sink: &mut dyn FnMut(WalPosition, WalEntry)) -> Result<(), WalError>;

    /// 段回收通知。收到足以覆盖某段全部条目的通知之前，不得删除该段。
    fn release(&self, release: Release);

    /// 读回这些位置上已确认的条目，按位置升序（溢出记录的提交，REQ-AMB-ING-HOT-BACKPRESSURE）。
    /// 位置须已确认、尚未被回收；读不全时为 [`WalError::Corrupt`]。在 [`Wal::replay`] 之后调用。
    fn read(
        &self,
        positions: Vec<WalPosition>,
    ) -> BoxFuture<'_, Result<Vec<(WalPosition, WalEntry)>, WalError>>;
}

mod codec;
mod crc;
mod file;

pub use codec::records_len;
pub use file::{FileWal, SEGMENT_BYTES, WalEvents};
