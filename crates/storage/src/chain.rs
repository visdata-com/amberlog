//! 快照链的读取一侧：提示、快照与桶 manifest（REQ-AMB-STO-HOT-LAYOUT-V4），供查询规划与执行取「一致对」
//! 中的快照一半并做两级剪枝（ADR-0021 第 3 条②）。
//!
//! **查询不调用快照提交。** 查询与快照提交之间只有共享的持久状态，
//! 所以读取接口单独成 [`ChainReader`]，不挂在 [`crate::commit::Committer`] 上；两者共用本模块的数据模型。
//!
//! 本模块的类型是持久格式在内存中的形态，字段与块逐一对应；JSON 的编解码（忽略未知字段、`parent_snapshot_id`
//! 为 null 也写出键等）在 [`crate::format`]。
//!
//! 快照与清单片为第 4 版格式（REQ-AMB-STO-HOT-LAYOUT-V8，ADR-0057）：桶 manifest 是逻辑对象，由清单片（[`Piece`]，全量片或
//! 增量片）组成，快照与留痕归档段引用其头片；[`walk_pieces`] 自头片沿 `previous`、`ledger_previous` 逐片读取并核对摘要，
//! [`restore`] 按块「还原」各条合成 [`BucketManifest`]（`files`、`deletions`、`handovers` 全集）。读取、提交、巡检、证据导出与
//! 查验共用这两个函数，各自按自己的块处置核对结果（[`PieceProblem`]）。

use std::fmt;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::gate::DatasetState;
use crate::in_use::SnapshotPin;
use crate::schema::SchemaColumn;

/// 快照号：自 1 起单调递增。「尚无任何快照」（找链尾时按 0 计）用 `Option::None` 表示。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SnapshotId(pub u64);

impl fmt::Display for SnapshotId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 完整字节的 SHA-256；持久格式里写作 `sha256:<64 位小写十六进制>`（REQ-AMB-STO-AUDIT-INVARIANTS）。
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub struct Digest(pub [u8; 32]);

impl fmt::Display for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("sha256:")?;
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl Digest {
    /// `bytes` 的 SHA-256。
    pub fn of(bytes: &[u8]) -> Digest {
        use sha2::{Digest as _, Sha256};
        Digest(Sha256::digest(bytes).into())
    }

    /// 解析 `sha256:<64 位小写十六进制>`；不合此格式为 `None`。
    pub fn parse(s: &str) -> Option<Digest> {
        let hex = s.strip_prefix("sha256:")?.as_bytes();
        if hex.len() != 64 {
            return None;
        }
        let nibble = |b: u8| match b {
            b'0'..=b'9' => Some(b - b'0'),
            b'a'..=b'f' => Some(b - b'a' + 10),
            _ => None,
        };
        let mut out = [0u8; 32];
        for (i, pair) in hex.as_chunks::<2>().0.iter().enumerate() {
            out[i] = nibble(pair[0])? << 4 | nibble(pair[1])?;
        }
        Some(Digest(out))
    }
}

impl fmt::Debug for Digest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// 一个快照（`<数据集前缀>/manifest/<snapshot_id>.json`）。时刻一律为 UTC 微秒。
#[derive(Debug, Clone, PartialEq)]
pub struct Snapshot {
    pub format_version: u64,
    pub snapshot_id: SnapshotId,
    /// 创世快照为 `None`（JSON `null`）。
    pub parent_snapshot_id: Option<SnapshotId>,
    /// 父快照完整字节的摘要（第 2 版）；创世快照为 `None`（JSON `null`）。
    pub parent_digest: Option<Digest>,
    pub created_at: i64,
    pub schema: Vec<SchemaColumn>,
    pub buckets: Vec<BucketRef>,
    pub summary: Summary,
    /// 最新留痕归档段的引用（第 2 版）；尚无时为 `None`（JSON `null`）。
    pub retired_segment: Option<SegmentRef>,
    /// 最新冷层变更段的引用（第 2 版）；尚无时为 `None`（JSON `null`）。
    pub cold_deletion_segment: Option<SegmentRef>,
}

/// 快照 `buckets` 的一个元素：一个 UTC 小时桶及其桶 manifest 的引用。
#[derive(Debug, Clone, PartialEq)]
pub struct BucketRef {
    /// `YYYYMMDDHH`
    pub bucket: String,
    /// 桶 manifest 的路径，相对数据集前缀。
    pub manifest: String,
    /// 桶 manifest 完整字节的摘要。
    pub content_digest: Digest,
    pub record_count: u64,
    pub size_bytes: u64,
    /// 无存活数据文件的墓碑桶为 `None`；剪枝时视为与任何时间范围无交集。
    pub time_min: Option<i64>,
    pub time_max: Option<i64>,
}

impl BucketRef {
    /// 引用路径为 `manifest`、完整字节摘要为 `content_digest` 的桶 manifest `m`：
    /// 合计与极值取自 `m.files`，无存活文件时两个合计为 0、两个极值为 null（REQ-AMB-STO-HOT-LAYOUT-V2）。
    pub fn of(m: &BucketManifest, manifest: String, content_digest: Digest) -> BucketRef {
        BucketRef {
            bucket: m.bucket.clone(),
            manifest,
            content_digest,
            record_count: m.files.iter().map(|f| f.record_count).sum(),
            size_bytes: m.files.iter().map(|f| f.size_bytes).sum(),
            time_min: m.files.iter().map(|f| f.min_timestamp).min(),
            time_max: m.files.iter().map(|f| f.max_timestamp).max(),
        }
    }
}

impl Summary {
    /// 数据集 `dataset` 在 `buckets` 下、已记入冷层文件集为 `cold` 的 `summary`：两个合计取各元素之和，
    /// 两个极值取各元素非 null 值的极值。
    pub fn of(dataset: &Dataset, buckets: &[BucketRef], cold: ColdSummary) -> Summary {
        Summary {
            dataset: dataset.to_string(),
            record_count: buckets.iter().map(|b| b.record_count).sum(),
            size_bytes: buckets.iter().map(|b| b.size_bytes).sum(),
            time_min: buckets.iter().filter_map(|b| b.time_min).min(),
            time_max: buckets.iter().filter_map(|b| b.time_max).max(),
            cold,
        }
    }
}

/// 快照 `summary`。
#[derive(Debug, Clone, PartialEq)]
pub struct Summary {
    /// `<org>/<stream>`
    pub dataset: String,
    pub record_count: u64,
    pub size_bytes: u64,
    pub time_min: Option<i64>,
    pub time_max: Option<i64>,
    /// `summary.cold`（第 2 版）：该快照的已记入冷层文件集的合计与极值。
    pub cold: ColdSummary,
}

/// 快照 `summary.cold`（REQ-AMB-STO-HOT-LAYOUT-V4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ColdSummary {
    pub record_count: u64,
    pub size_bytes: u64,
    /// 已记入冷层文件集为空时为 `None`。
    pub time_min: Option<i64>,
    pub time_max: Option<i64>,
}

impl ColdSummary {
    /// 已记入冷层文件集为空。
    pub const EMPTY: ColdSummary = ColdSummary {
        record_count: 0,
        size_bytes: 0,
        time_min: None,
        time_max: None,
    };

    /// 冷层数据文件集 `files` 的合计与极值（每个文件取记录其新增的交接记录 `files` 元素或 `additions` 元素）。
    pub fn of<'a>(files: impl IntoIterator<Item = &'a ColdFileEntry>) -> ColdSummary {
        files
            .into_iter()
            .fold(ColdSummary::EMPTY, |acc, f| ColdSummary {
                record_count: acc.record_count + f.record_count,
                size_bytes: acc.size_bytes + f.size_bytes,
                time_min: Some(
                    acc.time_min
                        .map_or(f.min_timestamp, |t| t.min(f.min_timestamp)),
                ),
                time_max: Some(
                    acc.time_max
                        .map_or(f.max_timestamp, |t| t.max(f.max_timestamp)),
                ),
            })
    }
}

/// 一个桶 manifest：快照或留痕归档段 `buckets` 元素所指的头片（`<数据集前缀>/manifest/buckets/<YYYYMMDDHH>-<manifest_id>.json`）
/// 按 REQ-AMB-STO-HOT-LAYOUT-V8「还原」所得的逻辑对象（[`restore`]）。
#[derive(Debug, Clone, PartialEq)]
pub struct BucketManifest {
    /// 读到它的快照的 `format_version`（各清单片与之相等）。
    pub format_version: u64,
    pub bucket: String,
    /// 该桶全部存活数据文件（全集）。
    pub files: Vec<FileEntry>,
    /// 该桶的删除留痕（REQ-AMB-STO-AUDIT-INVARIANTS）。
    pub deletions: Vec<Deletion>,
    /// 交接记录（第 2 版），按写入的先后；该桶尚无入湖交接时为空。
    pub handovers: Vec<Handover>,
}

/// 增量链与留痕链的上限（REQ-AMB-STO-HOT-LAYOUT-V8）。
pub const CHAIN_LIMIT: usize = 64;

/// 对一个清单片的引用：快照与留痕归档段 `buckets` 元素所指的头片，增量片的 `previous` 与全量片的 `ledger_previous`。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PieceRef {
    /// 清单片的路径，相对数据集前缀（`manifest/buckets/<YYYYMMDDHH>-<manifest_id>.json`）。
    pub path: String,
    /// 清单片完整字节的摘要。
    pub content_digest: Digest,
}

/// 一个清单片怎样接到它之前的清单片上（REQ-AMB-STO-HOT-LAYOUT-V8 第 4 版）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PieceLink {
    /// 全量片（`kind` 为 `full`）：`ledger_previous` 为另一份全量片的引用，或 `None`（JSON `null`）。
    Full { ledger_previous: Option<PieceRef> },
    /// 增量片（`kind` 为 `delta`）：`previous` 为父快照中该桶头片的引用。
    Delta { previous: PieceRef },
}

/// 一个清单片（`<数据集前缀>/manifest/buckets/<YYYYMMDDHH>-<manifest_id>.json`）。桶 manifest 是由头片按
/// [`restore`] 还原所得的逻辑对象（[`BucketManifest`]）。
#[derive(Debug, Clone, PartialEq)]
pub struct Piece {
    pub format_version: u64,
    pub bucket: String,
    pub link: PieceLink,
    /// 全量片的 `files`（存活文件全集）或增量片的 `files_added`（本次新增的文件条目）。
    pub files: Vec<FileEntry>,
    /// 全量片的 `deletions` 或增量片的 `deletions`（本次新记入的留痕）。
    pub deletions: Vec<Deletion>,
    /// 全量片的 `handovers`（全集）或增量片的 `handovers_added`（本次新追加的）。
    pub handovers: Vec<Handover>,
}

/// 还原所得的桶 manifest 与其头片的链况：提交器据此按「写哪一种清单片」选片（REQ-AMB-STO-HOT-LAYOUT-V8）。
#[derive(Debug, Clone, PartialEq)]
pub struct Restored {
    pub manifest: Arc<BucketManifest>,
    /// 头片的引用。
    pub head: PieceRef,
    /// 头片增量链的片数（含头片与基准片）。
    pub delta_len: usize,
    /// 增量链的基准片。
    pub base: PieceRef,
    /// 基准片自身的 `deletions` 非空。
    pub base_has_deletions: bool,
    /// 基准片的 `ledger_previous`。
    pub base_ledger_previous: Option<PieceRef>,
    /// 基准片留痕链的片数（含基准片）。
    pub base_ledger_len: usize,
    /// `manifest.deletions` 中属基准片还原所得的条数：此后的是增量链上各增量片的留痕（按写出的先后）。
    pub base_deletions: usize,
    /// 组成该桶 manifest 的全部清单片路径：增量链（自头片起到基准片），再接基准片的留痕链（不含基准片）。
    pub pieces: Vec<String>,
}

/// 还原途中读到的一个清单片。
#[derive(Debug, Clone)]
pub struct PieceVisit {
    /// 引用它的地方所记的路径与摘要。
    pub reference: PieceRef,
    /// 取得的完整字节；不存在为 `None`。
    pub bytes: Option<bytes::Bytes>,
    /// 解码所得；不存在或无法解析为 `None`。
    pub piece: Option<Piece>,
    /// 该片的问题；没有为 `None`。
    pub problem: Option<PieceProblem>,
}

/// 一个清单片的核对结果（不符时仍解码，由调用方决定是否用其内容）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PieceProblem {
    /// 不存在。
    Missing,
    /// 完整字节的 SHA-256 与引用它的地方所记不符（仍解码）。
    DigestMismatch,
    /// 内容无法解析：不合字段与类型、版本与快照不等、`bucket` 与引用者不同、`ledger_previous` 指向增量片、链超过 64 片。
    Unparseable(String),
}

/// 自一个头片起读到的清单片：增量链（头片在先），到基准片后再接留痕链；读到问题使链无法往下读时即止。
#[derive(Debug, Clone, Default)]
pub struct PieceWalk {
    pub visits: Vec<PieceVisit>,
    /// 增量链读到一份已还原过的清单片（调用方所给）即止：还原从它开始。
    pub cached: Option<Arc<Restored>>,
}

impl PieceWalk {
    /// 第一个有问题的清单片的问题与路径。
    pub fn problem(&self) -> Option<(&PieceProblem, &str)> {
        self.visits
            .iter()
            .find_map(|v| v.problem.as_ref().map(|p| (p, v.reference.path.as_str())))
    }
}

/// 自头片 `head` 起沿 `previous` 读到基准片，再沿基准片的 `ledger_previous` 读到底（REQ-AMB-STO-HOT-LAYOUT-V8「还原」）。
///
/// 每片按引用者所记的摘要核对、按快照的 `format_version`（`version`）与桶 `bucket` 解码；不符照常解码、往下读，
/// 不存在或无法解析即止。增量链上遇到 `cached` 给出的已还原清单片（摘要须与引用者所记相等）即止，不再往下读。
/// `fetch` 取一个相对数据集前缀的路径的完整字节，不存在为 `None`。不改任何状态。
pub async fn walk_pieces<E, F, Fut>(
    head: &PieceRef,
    version: u64,
    bucket: &str,
    mut fetch: F,
    cached: impl Fn(&PieceRef) -> Option<Arc<Restored>>,
) -> Result<PieceWalk, E>
where
    F: FnMut(String) -> Fut,
    Fut: std::future::Future<Output = Result<Option<bytes::Bytes>, E>>,
{
    let mut walk = PieceWalk::default();
    let mut next = Some(head.clone());
    // 增量链上已读的片数；读到基准片之后为 `None`，改数留痕链
    let mut delta_len = Some(0usize);
    let mut ledger_len = 0usize;
    while let Some(reference) = next.take() {
        if delta_len.is_some_and(|n| n > 0)
            && let Some(c) = cached(&reference).filter(|c| c.head == reference)
        {
            walk.cached = Some(c);
            break;
        }
        let Some(bytes) = fetch(reference.path.clone()).await? else {
            walk.visits.push(PieceVisit {
                reference,
                bytes: None,
                piece: None,
                problem: Some(PieceProblem::Missing),
            });
            break;
        };
        let mismatch = Digest::of(&bytes) != reference.content_digest;
        let decoded = crate::format::decode_piece(&bytes, version, bucket)
            .map_err(|e| e.0)
            .and_then(|p| {
                match (&p.link, delta_len) {
                    (_, Some(n)) if n + 1 > CHAIN_LIMIT => {
                        return Err(format!("the delta chain exceeds {CHAIN_LIMIT} pieces"));
                    }
                    (PieceLink::Delta { .. }, None) => {
                        return Err("ledger_previous refers to a delta piece".to_owned());
                    }
                    (PieceLink::Full { .. }, None) if ledger_len + 1 > CHAIN_LIMIT => {
                        return Err(format!("the ledger chain exceeds {CHAIN_LIMIT} pieces"));
                    }
                    _ => {}
                }
                Ok(p)
            });
        match decoded {
            Err(e) => {
                walk.visits.push(PieceVisit {
                    reference,
                    bytes: Some(bytes),
                    piece: None,
                    problem: Some(PieceProblem::Unparseable(e)),
                });
                break;
            }
            Ok(piece) => {
                match (&piece.link, delta_len) {
                    (PieceLink::Delta { previous }, Some(n)) => {
                        delta_len = Some(n + 1);
                        next = Some(previous.clone());
                    }
                    (PieceLink::Full { ledger_previous }, _) => {
                        delta_len = None;
                        ledger_len += 1;
                        next = ledger_previous.clone();
                    }
                    (PieceLink::Delta { .. }, None) => unreachable!("上面已判"),
                }
                walk.visits.push(PieceVisit {
                    reference,
                    bytes: Some(bytes),
                    piece: Some(piece),
                    problem: mismatch.then_some(PieceProblem::DigestMismatch),
                });
            }
        }
    }
    Ok(walk)
}

/// 按 REQ-AMB-STO-HOT-LAYOUT-V8「还原」各条，由 [`walk_pieces`] 读到的清单片合成桶 manifest（`format_version` 取
/// `version`）。不看各片的摘要核对结果（由调用方决定）；有清单片不存在或无法解析、或增量链（含 `cached` 的）超过 64 片
/// 为 `Err`，附原因。
pub fn restore(walk: &PieceWalk, version: u64, bucket: &str) -> Result<Restored, String> {
    let pieces: Vec<(&PieceRef, &Piece)> = walk
        .visits
        .iter()
        .map(|v| match (&v.piece, &v.problem) {
            (Some(p), _) => Ok((&v.reference, p)),
            (None, Some(PieceProblem::Missing)) => {
                Err(format!("piece {} does not exist", v.reference.path))
            }
            (None, problem) => Err(format!("piece {}: {problem:?}", v.reference.path)),
        })
        .collect::<Result<_, _>>()?;
    // 增量链上的增量片（自头片起）与其后的基准片、留痕链
    let deltas = pieces
        .iter()
        .take_while(|(_, p)| matches!(p.link, PieceLink::Delta { .. }))
        .count();
    let mut out = match &walk.cached {
        Some(c) => {
            if deltas != pieces.len() {
                return Err("a cached piece is followed by more pieces".to_owned());
            }
            let mut r = (**c).clone();
            r.manifest = Arc::new((*c.manifest).clone());
            r
        }
        None => {
            let Some((base_ref, base)) = pieces.get(deltas) else {
                return Err("the delta chain does not reach a full piece".to_owned());
            };
            let ledger = &pieces[deltas + 1..];
            let mut deletions: Vec<Deletion> = Vec::new();
            for (_, p) in ledger.iter().rev() {
                deletions.extend(p.deletions.iter().cloned());
            }
            deletions.extend(base.deletions.iter().cloned());
            let PieceLink::Full { ledger_previous } = &base.link else {
                unreachable!("take_while 已判")
            };
            Restored {
                head: (*base_ref).clone(),
                delta_len: 1,
                base: (*base_ref).clone(),
                base_has_deletions: !base.deletions.is_empty(),
                base_ledger_previous: ledger_previous.clone(),
                base_ledger_len: 1 + ledger.len(),
                base_deletions: deletions.len(),
                pieces: std::iter::once(base_ref)
                    .chain(ledger.iter().map(|(r, _)| r))
                    .map(|r| r.path.clone())
                    .collect(),
                manifest: Arc::new(BucketManifest {
                    format_version: version,
                    bucket: bucket.to_owned(),
                    files: base.files.clone(),
                    deletions,
                    handovers: base.handovers.clone(),
                }),
            }
        }
    };
    // 自最早的增量片起依次应用
    for (r, p) in pieces[..deltas].iter().rev() {
        out = out.then_delta((*r).clone(), p);
    }
    if out.delta_len > CHAIN_LIMIT {
        return Err(format!("the delta chain exceeds {CHAIN_LIMIT} pieces"));
    }
    Ok(out)
}

/// 同 [`restore`]，但任一清单片有问题（含摘要不符）即为 `Err`，附原因：据以作答或提交的读取用它。
pub fn restore_checked(walk: &PieceWalk, version: u64, bucket: &str) -> Result<Restored, String> {
    if let Some((problem, path)) = walk.problem() {
        return Err(match problem {
            PieceProblem::Missing => format!("piece {path} does not exist"),
            PieceProblem::DigestMismatch => {
                format!("piece {path} does not match the content_digest recorded for it")
            }
            PieceProblem::Unparseable(e) => format!("piece {path}: {e}"),
        });
    }
    restore(walk, version, bucket)
}

/// 自对象存储读出 `snapshot`（格式版本 `version`）`buckets` 元素 `bucket` 所指的桶 manifest，逐片核对并还原（不经缓存、
/// 不改任何状态）。内层 `Err` 为无法使用的原因（见 [`restore_checked`]）。离线查验与测试用。
pub async fn read_restored(
    store: &crate::store::Store,
    dataset: &Dataset,
    bucket: &BucketRef,
    version: u64,
) -> Result<Result<Restored, String>, crate::store::Unavailable> {
    let head = PieceRef {
        path: bucket.manifest.clone(),
        content_digest: bucket.content_digest,
    };
    let walk = walk_pieces(
        &head,
        version,
        &bucket.bucket,
        |rel| async move {
            store
                .read(&store.object_path(dataset, &rel))
                .await
                .map(|f| f.map(|f| f.bytes))
        },
        |_| None,
    )
    .await?;
    Ok(restore_checked(&walk, version, &bucket.bucket))
}

impl Restored {
    /// 在本桶 manifest 之上接一份增量片 `p`（其引用为 `head`）所得。
    pub fn then_delta(mut self, head: PieceRef, p: &Piece) -> Restored {
        let m = Arc::make_mut(&mut self.manifest);
        m.files
            .retain(|f| !p.deletions.iter().any(|d| d.path == f.path));
        m.files.extend(p.files.iter().cloned());
        m.deletions.extend(p.deletions.iter().cloned());
        m.handovers.extend(p.handovers.iter().cloned());
        self.pieces.insert(0, head.path.clone());
        self.head = head;
        self.delta_len += 1;
        self
    }
}

/// 桶 manifest `files` 的一个元素。
#[derive(Debug, Clone, PartialEq)]
pub struct FileEntry {
    /// 数据文件路径，相对数据集前缀（`data/YYYY/MM/DD/HH/<file_id>.parquet`）。
    pub path: String,
    /// 文件内 `_timestamp` 的最小值与最大值，两端均含。
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    pub content_digest: Digest,
    /// 首次引用该文件的快照号。
    pub added_in: SnapshotId,
    /// 列统计（REQ-AMB-QRY-COLUMN-PRUNING）：读到的原始 JSON 值，不论形状是否合规，写出新版桶 manifest 时原样带上；
    /// 条目不含该字段（早期版本写出的条目）为 `None`。
    pub column_stats: Option<serde_json::Value>,
}

/// 桶 manifest `deletions` 的一个元素。
#[derive(Debug, Clone, PartialEq)]
pub struct Deletion {
    pub path: String,
    pub content_digest: Digest,
    pub reason: DeletionReason,
    /// 被移除文件条目的 `added_in`（第 2 版）；`orphan` 留痕为 `None`（JSON `null`）。
    pub added_in: Option<SnapshotId>,
    /// 被移除文件条目的 `max_timestamp`（第 2 版）；`orphan` 留痕为 `None`（JSON `null`）。
    pub max_timestamp: Option<i64>,
    /// `ttl` 留痕为移除它的那次 TTL 执行的截止时刻（第 2 版），其余为 `None`（JSON `null`）。
    pub cutoff: Option<i64>,
    pub deleted_at: i64,
    pub removed_in: SnapshotId,
}

/// 删除留痕的 `reason`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeletionReason {
    Orphan,
    Compaction,
    Ttl,
    Rollback,
    /// 入湖交接提交使入湖输入离开存活集（第 2 版，REQ-AMB-STO-LAKE-ENTRY）。
    Lake,
}

/// 留痕归档段与冷层变更段的段号：零填充 20 位十进制，自 1 起单调递增、允许跳号（REQ-AMB-STO-HOT-LAYOUT-V4）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SegmentId(pub u64);

impl fmt::Display for SegmentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// 对一个段的引用：快照的 `retired_segment`、`cold_deletion_segment` 与段的 `previous`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentRef {
    /// 段的路径，相对数据集前缀（`manifest/retired/<segment_id>.json` 或 `manifest/cold-deletions/<segment_id>.json`）。
    pub path: String,
    /// 段文件完整字节的摘要。
    pub content_digest: Digest,
}

/// 桶 manifest `handovers` 的一个元素：一次入湖交接（REQ-AMB-STO-HOT-LAYOUT-V4、REQ-AMB-STO-LAKE-ENTRY）。
#[derive(Debug, Clone, PartialEq)]
pub struct Handover {
    /// 冷层表位置，与冷层表元数据的 `location` 相同。
    pub table_location: String,
    /// 该次冷层提交所写元数据文件 `v<N>.metadata.json` 中的 N。
    pub metadata_version: u64,
    /// 该次冷层提交的 Iceberg 快照号。
    pub iceberg_snapshot_id: i64,
    /// 写入该交接记录的热层快照号。
    pub snapshot_id: SnapshotId,
    pub files: Vec<ColdFileEntry>,
}

/// 一次冷层提交交回的、写交接记录所需的内容：交接记录除 `snapshot_id` 以外的部分
/// （`snapshot_id` 由快照提交在绑定快照号时填，真冲突换号时随之重写，REQ-AMB-STO-MANIFEST-COMMIT-V3）。
#[derive(Debug, Clone, PartialEq)]
pub struct HandoverDraft {
    pub table_location: String,
    pub metadata_version: u64,
    pub iceberg_snapshot_id: i64,
    pub files: Vec<ColdFileEntry>,
}

impl HandoverDraft {
    /// 由热层快照 `snapshot_id` 写入的交接记录。
    pub fn into_handover(self, snapshot_id: SnapshotId) -> Handover {
        Handover {
            table_location: self.table_location,
            metadata_version: self.metadata_version,
            iceberg_snapshot_id: self.iceberg_snapshot_id,
            snapshot_id,
            files: self.files,
        }
    }
}

/// 交接记录 `files` 与冷层变更段 `additions` 的一个元素：一个冷层数据文件。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdFileEntry {
    /// 相对冷层表位置，不以 `/` 开头（`data/_timestamp_day=YYYY-MM-DD/<file_id>.parquet`）。
    pub path: String,
    /// 文件内 `_timestamp` 的最小值与最大值，两端均含。
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    pub content_digest: Digest,
}

/// 一个留痕归档段（`<数据集前缀>/manifest/retired/<segment_id>.json`）。
#[derive(Debug, Clone, PartialEq)]
pub struct RetiredSegment {
    pub format_version: u64,
    pub segment_id: SegmentId,
    /// 写出该段的那次提交尝试所绑定父快照的 `retired_segment`。
    pub previous: Option<SegmentRef>,
    /// 写出该段的那次提交尝试所绑定快照的 `created_at`。
    pub created_at: i64,
    pub buckets: Vec<RetiredBucket>,
}

/// 留痕归档段 `buckets` 的一个元素：该桶被移入本段时其最后一份桶 manifest。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetiredBucket {
    /// `YYYYMMDDHH`
    pub bucket: String,
    /// 桶 manifest 的路径，相对数据集前缀。
    pub manifest: String,
    pub content_digest: Digest,
}

/// 一个冷层变更段（`<数据集前缀>/manifest/cold-deletions/<segment_id>.json`）。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdChangeSegment {
    pub format_version: u64,
    pub segment_id: SegmentId,
    /// 写出该段的那次提交尝试所绑定父快照的 `cold_deletion_segment`。
    pub previous: Option<SegmentRef>,
    /// 写出该段的那次提交尝试所绑定快照的 `created_at`。
    pub created_at: i64,
    pub deletions: Vec<ColdDeletion>,
    /// 合并记录：`reason` 为 `compaction` 的变更段写出该次合并的全部合并产出，其余为空。
    pub additions: Vec<ColdFileEntry>,
}

/// 冷层变更段 `deletions` 的一个元素：一个离开冷层存活集的冷层数据文件（REQ-AMB-STO-AUDIT-INVARIANTS-V3）。
#[derive(Debug, Clone, PartialEq)]
pub struct ColdDeletion {
    /// 相对冷层表位置，不以 `/` 开头。
    pub path: String,
    pub content_digest: Digest,
    /// 该冷层数据文件内 `_timestamp` 的最大值。
    pub max_timestamp: i64,
    pub reason: ColdDeletionReason,
    /// `reason` 为 `ttl` 时为该次 TTL 执行的截止时刻，否则为 `None`（JSON `null`）。
    pub cutoff: Option<i64>,
    pub deleted_at: i64,
    /// 首个引用本段的热层快照号。
    pub removed_in: SnapshotId,
    /// 移除该文件的冷层提交的 Iceberg 快照号。
    pub cold_removed_in: i64,
}

/// 冷层变更段删除留痕的 `reason`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColdDeletionReason {
    Ttl,
    /// 冷层合并。
    Compaction,
}

/// 读取快照链的失败，按 REQ-AMB-STO-HOT-LAYOUT-V2 对读取结果的分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainError {
    /// 对象存储不可访问（该块定义的术语）：`_search` 回 503 `unavailable`。
    Unavailable(String),
    /// 作答所需对象不存在或内容无法解析（含桶 manifest 摘要不符）：`_search` 回 500 `internal`。
    /// 其中使数据集进入 manifest 不可读情形的，实现已把状态记入 [`crate::gate::DatasetGates`]。
    Integrity(String),
    /// 数据集处于两种停用状态之一（此前已读到，或本次读到而进入）：按状态作答，
    /// 见 [`DatasetState::reason`]。
    Disabled(DatasetState),
}

/// 快照链的读取（经共享持久状态，非调用）。
///
/// 实现须遵守的共同约定：
/// - 数据集已处于停用状态时，不发出任何对象存储请求，直接返回 [`ChainError::Disabled`]。
/// - 读到使数据集进入停用状态的结果时，先把状态记入 [`crate::gate::DatasetGates`]，再返回。
/// - 同一路径的快照与桶 manifest 写出后不再改变，实现可以缓存；桶 manifest 按路径核对摘要一次即可。
pub trait ChainReader: Send + Sync {
    /// 取得查询用的快照号 Q（ADR-0021 第 3 条②）。
    ///
    /// 按找链尾（REQ-AMB-STO-MANIFEST-COMMIT-V3）确定当前快照，不以提示所指快照代替。
    ///
    /// 返回 `Ok(None)`：数据集的存在尚未持久化或尚只在本地 WAL 中（Q 按 0 计、全部代参与、不读对象存储），
    /// 或找链尾得到尚无任何快照。找链尾判出 manifest 不可读情形时返回 [`ChainError::Disabled`]。
    fn current<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<SnapshotId>, ChainError>>;

    /// 读取快照 `id`。`format_version` 不是本程序认识的版本（4）时数据集进入格式版本不兼容情形，返回 [`ChainError::Disabled`]。
    fn snapshot<'a>(
        &'a self,
        dataset: &'a Dataset,
        id: SnapshotId,
    ) -> BoxFuture<'a, Result<Arc<Snapshot>, ChainError>>;

    /// 读取 `snapshot` 的 `buckets` 中 `bucket` 所引用的桶 manifest，并按块核对摘要与格式版本
    /// （完整字节的 SHA-256 须等于 `bucket.content_digest`，`format_version` 须等于 `snapshot.format_version`）。
    fn bucket_manifest<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
        bucket: &'a BucketRef,
    ) -> BoxFuture<'a, Result<Arc<BucketManifest>, ChainError>>;

    /// 将要以 [`ChainReader::current`] 定下 `dataset` 的快照：先在在用快照登记里登记（REQ-AMB-STO-SNAPSHOT-GC-IN-USE），
    /// 定下后以 [`SnapshotPin::settle`] 改为确切编号，持有到读完该快照所引用的对象。
    ///
    /// 缺省不登记（测试替身与不经对象存储的实现）；[`crate::reader::StoreChain`] 登记进它的 [`crate::in_use::InUseSnapshots`]。
    fn reading(&self, _dataset: &Dataset) -> SnapshotPin {
        SnapshotPin::detached()
    }
}

/// 两条段链的读取（第 2 版）：自快照的 `retired_segment`、`cold_deletion_segment` 起沿 `previous` 读到链首，
/// 逐段核对 `content_digest`（REQ-AMB-STO-HOT-LAYOUT-V4）。
///
/// 读取**不改变数据集状态**，只返回 [`ChainError::Unavailable`] 或 [`ChainError::Integrity`]：TTL 与热层巡检的读取
/// 不使数据集停用（REQ-AMB-STO-TTL、REQ-AMB-STO-HOT-FSCK），为提交（含归档）所作的读取是否停用由快照提交按
/// REQ-AMB-STO-HOT-LAYOUT-V4 自己处置。段写出后不再改变，实现可以按路径缓存。
pub trait SegmentReader: Send + Sync {
    /// `snapshot` 经 `retired_segment` 可达的全部留痕归档段，自最新起。
    fn retired_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<RetiredSegment>>, ChainError>>;

    /// `snapshot` 经 `cold_deletion_segment` 可达的全部冷层变更段，自最新起（TTL 补写冷层变更段时据各段 `deletions` 的
    /// `cold_removed_in` 判定某个 `ttl` 冷层提交是否已记入）。
    fn cold_change_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<ColdChangeSegment>>, ChainError>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 清单片链的还原（REQ-AMB-STO-HOT-LAYOUT-V8「还原」「对象状态的判定」）。
    mod pieces {
        use std::collections::HashMap;
        use std::convert::Infallible;

        use super::*;
        use crate::format::{FORMAT_VERSION, encode_piece};

        const B: &str = "2026082210";

        fn path(n: u32) -> String {
            format!("manifest/buckets/{B}-01k6{n:022}.json")
        }

        fn file(n: u32) -> FileEntry {
            FileEntry {
                path: format!("data/2026/08/22/10/01k7{n:022}.parquet"),
                min_timestamp: 1,
                max_timestamp: 2,
                record_count: 1,
                size_bytes: 10,
                content_digest: Digest::of(&n.to_le_bytes()),
                added_in: SnapshotId(2),
                column_stats: None,
            }
        }

        fn tomb(n: u32) -> Deletion {
            let f = file(n);
            Deletion {
                path: f.path,
                content_digest: f.content_digest,
                reason: DeletionReason::Compaction,
                added_in: Some(f.added_in),
                max_timestamp: Some(f.max_timestamp),
                cutoff: None,
                deleted_at: 5,
                removed_in: SnapshotId(3),
            }
        }

        #[derive(Default)]
        struct Objects(HashMap<String, Vec<u8>>);

        impl Objects {
            /// 写出清单片 `n`，返回对它的引用。
            fn put(
                &mut self,
                n: u32,
                link: PieceLink,
                files: Vec<FileEntry>,
                deletions: Vec<Deletion>,
            ) -> PieceRef {
                let bytes = encode_piece(&Piece {
                    format_version: FORMAT_VERSION,
                    bucket: B.into(),
                    link,
                    files,
                    deletions,
                    handovers: Vec::new(),
                });
                let r = PieceRef {
                    path: path(n),
                    content_digest: Digest::of(&bytes),
                };
                self.0.insert(path(n), bytes);
                r
            }

            async fn walk(
                &self,
                head: &PieceRef,
                cached: impl Fn(&PieceRef) -> Option<Arc<Restored>>,
            ) -> PieceWalk {
                walk_pieces(
                    head,
                    FORMAT_VERSION,
                    B,
                    |rel| {
                        let got = self.0.get(&rel).cloned().map(bytes::Bytes::from);
                        async move { Ok::<_, Infallible>(got) }
                    },
                    cached,
                )
                .await
                .unwrap_or_else(|e| match e {})
            }
        }

        fn full(ledger_previous: Option<PieceRef>) -> PieceLink {
            PieceLink::Full { ledger_previous }
        }

        fn delta(previous: &PieceRef) -> PieceLink {
            PieceLink::Delta {
                previous: previous.clone(),
            }
        }

        fn paths(files: &[FileEntry]) -> Vec<String> {
            let mut out: Vec<String> = files.iter().map(|f| f.path.clone()).collect();
            out.sort();
            out
        }

        #[tokio::test]
        async fn delta_chain_applies_removals_and_additions() {
            let mut o = Objects::default();
            let p1 = o.put(1, full(None), vec![file(1)], Vec::new());
            let p2 = o.put(2, delta(&p1), vec![file(2)], vec![tomb(1)]);
            let p3 = o.put(3, delta(&p2), vec![file(3)], Vec::new());
            let walk = o.walk(&p3, |_| None).await;
            assert_eq!(walk.problem(), None);
            let r = restore_checked(&walk, FORMAT_VERSION, B).unwrap();
            assert_eq!(paths(&r.manifest.files), paths(&[file(2), file(3)]));
            assert_eq!(r.manifest.deletions, vec![tomb(1)]);
            assert_eq!(r.head, p3);
            assert_eq!(r.base, p1);
            assert_eq!(r.delta_len, 3);
            assert_eq!(r.base_deletions, 0);
            assert_eq!(r.pieces, vec![path(3), path(2), path(1)]);
        }

        #[tokio::test]
        async fn ledger_chain_joins_deletions_in_order() {
            let mut o = Objects::default();
            let a = o.put(1, full(None), Vec::new(), vec![tomb(1)]);
            let b = o.put(2, full(Some(a)), vec![file(9)], vec![tomb(2)]);
            let c = o.put(3, delta(&b), Vec::new(), vec![tomb(3)]);
            let r = restore_checked(&o.walk(&c, |_| None).await, FORMAT_VERSION, B).unwrap();
            assert_eq!(r.manifest.deletions, vec![tomb(1), tomb(2), tomb(3)]);
            assert_eq!(r.base, b);
            assert_eq!(r.base_deletions, 2);
            assert_eq!(r.base_ledger_len, 2);
            assert!(r.base_has_deletions);
            assert_eq!(r.pieces, vec![path(3), path(2), path(1)]);
        }

        #[tokio::test]
        async fn problems_are_reported_per_piece() {
            let mut o = Objects::default();
            let p1 = o.put(1, full(None), vec![file(1)], Vec::new());
            let p2 = o.put(2, delta(&p1), vec![file(2)], Vec::new());
            // 摘要不符：照常解码、往下读，还原可用其内容，据以作答的还原拒用
            o.0.get_mut(&path(1)).unwrap().push(b' ');
            let walk = o.walk(&p2, |_| None).await;
            assert_eq!(
                walk.problem(),
                Some((&PieceProblem::DigestMismatch, path(1).as_str()))
            );
            assert!(restore(&walk, FORMAT_VERSION, B).is_ok());
            assert!(restore_checked(&walk, FORMAT_VERSION, B).is_err());
            // 头片以外的清单片不存在
            o.0.remove(&path(1));
            let walk = o.walk(&p2, |_| None).await;
            assert_eq!(
                walk.problem(),
                Some((&PieceProblem::Missing, path(1).as_str()))
            );
            assert!(restore(&walk, FORMAT_VERSION, B).is_err());
            // `ledger_previous` 指向增量片
            let d = o.put(3, delta(&p2), Vec::new(), Vec::new());
            let f = o.put(4, full(Some(d)), Vec::new(), Vec::new());
            let walk = o.walk(&f, |_| None).await;
            assert!(matches!(
                walk.problem(),
                Some((PieceProblem::Unparseable(_), _))
            ));
        }

        #[tokio::test]
        async fn chains_longer_than_64_pieces_are_unparseable() {
            let mut o = Objects::default();
            let mut head = o.put(0, full(None), vec![file(0)], Vec::new());
            for n in 1..CHAIN_LIMIT as u32 {
                head = o.put(n, delta(&head), vec![file(n)], Vec::new());
            }
            // 64 片（63 份增量片加基准片）可以还原
            let r = restore_checked(&o.walk(&head, |_| None).await, FORMAT_VERSION, B).unwrap();
            assert_eq!(r.delta_len, CHAIN_LIMIT);
            let longer = o.put(99, delta(&head), Vec::new(), Vec::new());
            let walk = o.walk(&longer, |_| None).await;
            assert!(matches!(
                walk.problem(),
                Some((PieceProblem::Unparseable(_), _))
            ));
            // 留痕链同样至多 64 片
            let mut o = Objects::default();
            let mut lp = o.put(0, full(None), Vec::new(), vec![tomb(0)]);
            for n in 1..=CHAIN_LIMIT as u32 {
                lp = o.put(n, full(Some(lp)), Vec::new(), vec![tomb(n)]);
            }
            let walk = o.walk(&lp, |_| None).await;
            assert!(matches!(
                walk.problem(),
                Some((PieceProblem::Unparseable(_), _))
            ));
        }

        #[tokio::test]
        async fn walk_stops_at_a_cached_piece() {
            let mut o = Objects::default();
            let p1 = o.put(1, full(None), vec![file(1)], Vec::new());
            let p2 = o.put(2, delta(&p1), vec![file(2)], Vec::new());
            let seed =
                Arc::new(restore_checked(&o.walk(&p2, |_| None).await, FORMAT_VERSION, B).unwrap());
            let p3 = o.put(3, delta(&p2), vec![file(3)], Vec::new());
            // 已还原过的 p2 之前的清单片不必再读
            o.0.remove(&path(1));
            let walk = o.walk(&p3, |r| (*r == p2).then(|| Arc::clone(&seed))).await;
            assert_eq!(walk.visits.len(), 1);
            let r = restore_checked(&walk, FORMAT_VERSION, B).unwrap();
            assert_eq!(
                paths(&r.manifest.files),
                paths(&[file(1), file(2), file(3)])
            );
            assert_eq!(r.delta_len, 3);
            assert_eq!(r.pieces, vec![path(3), path(2), path(1)]);
            // 摘要与所记不同的不当作已还原过的
            let wrong = PieceRef {
                content_digest: Digest::of(b"other"),
                ..p2.clone()
            };
            let walk = o
                .walk(&p3, |r| {
                    (r.path == wrong.path).then(|| {
                        Arc::new(Restored {
                            head: wrong.clone(),
                            ..(*seed).clone()
                        })
                    })
                })
                .await;
            assert_eq!(
                walk.problem(),
                Some((&PieceProblem::Missing, path(1).as_str()))
            );
        }
    }

    #[test]
    fn digest_display_is_prefixed_lower_hex() {
        let mut bytes = [0u8; 32];
        bytes[0] = 0xab;
        bytes[31] = 0x01;
        let s = Digest(bytes).to_string();
        assert_eq!(s.len(), "sha256:".len() + 64);
        assert!(s.starts_with("sha256:ab00"));
        assert!(s.ends_with("01"));
        assert_eq!(Digest::parse(&s), Some(Digest(bytes)));
    }

    #[test]
    fn digest_of_bytes_and_strict_parse() {
        // SHA-256("abc")
        let d = Digest::of(b"abc");
        assert_eq!(
            d.to_string(),
            "sha256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        for bad in [
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
            "sha256:BA7816BF8F01CFEA414140DE5DAE2223B00361A396177A9CB410FF61F20015AD",
            "sha256:ba7816bf",
            "SHA256:ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
        ] {
            assert_eq!(Digest::parse(bad), None, "{bad}");
        }
    }
}
