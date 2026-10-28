//! 离线查验（REQ-AMB-STO-AUDIT-INVARIANTS「篡改可检出」）：按 `_current` 所指快照经 `buckets` 引用的各桶 manifest
//! （按 REQ-AMB-STO-HOT-LAYOUT-V8 由清单片还原），逐个比对 `files` 条目的 `content_digest` 与数据文件对象完整字节的
//! SHA-256，输出不符的文件。
//!
//! 查询与提交不做数据文件摘要的在线核对，都不调用这里；这是测试用的库函数，不对应命令行子命令。

use std::fmt;

use amberlog_common::dataset::Dataset;

use crate::chain::{Digest, PieceRef, SnapshotId, restore_checked, walk_pieces};
use crate::format::{self, SnapshotContent};
use crate::layout;
use crate::store::{Store, Unavailable};

/// 一个摘要不符的数据文件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mismatch {
    /// 桶 manifest 条目中的 `path`（相对数据集前缀）。
    pub path: String,
    /// 条目记录的 `content_digest`。
    pub recorded: Digest,
    /// 对象实际字节的摘要；对象不存在为 `None`。
    pub actual: Option<Digest>,
}

/// 查验无法进行。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VerifyError {
    /// 对象存储不可访问。
    Unavailable(String),
    /// 快照链本身读不出：`_current`、其所指快照或组成桶 manifest 的清单片不存在、内容无法解析，或清单片摘要与引用者所记不符。
    Chain(String),
}

impl fmt::Display for VerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            VerifyError::Unavailable(m) => write!(f, "object store is unavailable: {m}"),
            VerifyError::Chain(m) => write!(f, "snapshot chain is unreadable: {m}"),
        }
    }
}

impl std::error::Error for VerifyError {}

impl From<Unavailable> for VerifyError {
    fn from(e: Unavailable) -> Self {
        VerifyError::Unavailable(e.0)
    }
}

fn chain(what: impl Into<String>) -> VerifyError {
    VerifyError::Chain(what.into())
}

/// 查验数据集 `dataset` 在 `_current` 所指快照下的全部数据文件，返回摘要不符（含对象不存在）的文件，按桶与条目顺序。
/// 数据集尚无 `_current` 时没有可查验的文件。
pub async fn verify_data_files(
    store: &Store,
    dataset: &Dataset,
) -> Result<Vec<Mismatch>, VerifyError> {
    let read = |rel: String| async move {
        let fetched = store.read(&store.object_path(dataset, &rel)).await?;
        Ok::<_, VerifyError>(fetched.map(|f| f.bytes))
    };
    let Some(current) = read(layout::CURRENT.to_owned()).await? else {
        return Ok(Vec::new());
    };
    let id: SnapshotId = format::decode_current(&current).map_err(|e| chain(e.0))?;
    let bytes = read(layout::snapshot_path(id))
        .await?
        .ok_or_else(|| chain(format!("snapshot {id} does not exist")))?;
    let snapshot = match format::decode_snapshot(&bytes, dataset, id).map_err(|e| chain(e.0))? {
        SnapshotContent::Supported(s) => *s,
        SnapshotContent::Unsupported { format_version } => {
            return Err(chain(format!(
                "snapshot {id} has format_version {format_version}, above what this version reads"
            )));
        }
    };
    let mut mismatches = Vec::new();
    for bucket in &snapshot.buckets {
        let head = PieceRef {
            path: bucket.manifest.clone(),
            content_digest: bucket.content_digest,
        };
        let walk = walk_pieces(&head, snapshot.format_version, &bucket.bucket, read, |_| {
            None
        })
        .await?;
        let restored =
            restore_checked(&walk, snapshot.format_version, &bucket.bucket).map_err(chain)?;
        let manifest = (*restored.manifest).clone();
        for entry in manifest.files {
            let actual = read(entry.path.clone()).await?.map(|b| Digest::of(&b));
            if actual != Some(entry.content_digest) {
                mismatches.push(Mismatch {
                    path: entry.path,
                    recorded: entry.content_digest,
                    actual,
                });
            }
        }
    }
    Ok(mismatches)
}
