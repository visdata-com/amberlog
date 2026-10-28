//! 确定冷层当前版本（REQ-AMB-STO-COLD-TABLE「确定冷层当前版本」）与读取冷层对象的分类。
//!
//! 读提示文件得 h；提示不存在或内容无法解析时取 h = 0；再从 h + 1 起逐个以 HEAD 探测，最后一个存在的编号即冷层当前版本，
//! h 为 0 且 `v1.metadata.json` 不存在即尚无冷层表（REQ-AMB-STO-COLD-TABLE-V2「确定冷层当前版本」）。不另行探测
//! `v<h>.metadata.json`：h + 1 不存在时当前版本即为 h，提示所指版本不存在的，随后读 `v<h>` 得到不存在
//! （[`ColdError::Broken`]），由调用方按来源处置（入湖、查询停用；TTL、冷层孤儿清理与冷层保留只放弃本次）。
//! 读提示或探测遇对象存储不可访问时本次确定失败，不以已探测到的编号代替。

use amberlog_common::dataset::Dataset;
use bytes::Bytes;
use iceberg::spec::{FormatVersion, TableMetadata};
use object_store::ObjectStoreExt;
use object_store::path::Path;

use super::error::ColdError;
use super::layout::{self, ColdLayout};
use crate::store::{Store, Unavailable};

/// 冷层当前版本。
#[derive(Debug, Clone)]
pub struct Current {
    /// 冷层当前版本 N；尚无冷层表时为 0。
    pub version: u64,
    /// 读到的提示（存在且可解析时）：提交前据此判断是否要补写落后的提示（REQ-AMB-STO-COLD-TABLE-HINT-BEFORE）。
    pub hint: Option<u64>,
    /// 当前版本元数据文件的完整字节与解析结果；尚无冷层表时为 `None`。
    pub metadata: Option<(Bytes, TableMetadata)>,
}

/// 对象是否存在（HEAD）。`file://` 所指目录不存在属对象存储不可访问（与 [`Store::read`] 同口径）。
pub(crate) async fn head(store: &Store, path: &Path) -> Result<bool, Unavailable> {
    match store.inner.head(path).await {
        Ok(_) => Ok(true),
        Err(object_store::Error::NotFound { .. }) => {
            check_local_dir(store)?;
            Ok(false)
        }
        Err(e) => Err(Unavailable(format!(
            "object store head of {path} failed: {e}"
        ))),
    }
}

fn check_local_dir(store: &Store) -> Result<(), Unavailable> {
    match &store.local_dir {
        Some(dir) if !dir.is_dir() => Err(Unavailable(format!(
            "object store directory {} does not exist",
            dir.display()
        ))),
        _ => Ok(()),
    }
}

/// 提示文件的内容：不带前导零以外的任何字符的十进制整数；其余一律视为无法解析。
fn parse_hint(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// 解析元数据文件；不是合法 JSON、不是 Iceberg 第 2 版的合法表元数据即内容无法解析（REQ-AMB-STO-COLD-TABLE-UNPARSEABLE）。
pub fn parse_metadata(bytes: &[u8]) -> Result<TableMetadata, String> {
    let metadata: TableMetadata = serde_json::from_slice(bytes).map_err(|e| e.to_string())?;
    if metadata.format_version() != FormatVersion::V2 {
        return Err(format!(
            "format-version is {}, not 2",
            metadata.format_version()
        ));
    }
    Ok(metadata)
}

/// 确定数据集 `dataset` 的冷层当前版本，并读出当前版本的元数据。
///
/// 当前版本的元数据文件读到不存在或无法解析为 [`ColdError::Broken`]（是否停用由调用方按来源定）。
pub async fn current(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
) -> Result<Current, ColdError> {
    let (n, hint) = version(store, layout, dataset).await?;
    if n == 0 {
        return Ok(Current {
            version: 0,
            hint,
            metadata: None,
        });
    }
    let (bytes, metadata) = read_metadata(store, layout, dataset, n).await?;
    Ok(Current {
        version: n,
        hint,
        metadata: Some((bytes, metadata)),
    })
}

/// 读版本 `n`（不小于 1）的元数据文件：完整字节与解析结果。文件不存在或内容无法解析为 [`ColdError::Broken`]，
/// 是否停用由调用方按来源定（只读目录端点读到时不停用，REQ-AMB-STO-COLD-CATALOG-NO-DISABLE）。
pub async fn read_metadata(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    n: u64,
) -> Result<(Bytes, TableMetadata), ColdError> {
    let rel = layout::metadata_path(n);
    let fetched = store
        .read(&layout.object_path(dataset, &rel))
        .await?
        .ok_or_else(|| ColdError::Broken(format!("{rel} does not exist")))?;
    let metadata =
        parse_metadata(&fetched.bytes).map_err(|e| ColdError::Broken(format!("{rel}: {e}")))?;
    Ok((fetched.bytes, metadata))
}

/// 只确定冷层当前版本：读提示后逐个 HEAD 探测，不读元数据。返回（当前版本，0 即尚无冷层表；读到的提示）。
pub async fn version(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
) -> Result<(u64, Option<u64>), ColdError> {
    let hint = store
        .read(&layout.object_path(dataset, layout::HINT))
        .await?
        .and_then(|f| parse_hint(&f.bytes));
    let mut n = hint.unwrap_or(0);
    while let Some(next) = n.checked_add(1)
        && head(
            store,
            &layout.object_path(dataset, &layout::metadata_path(next)),
        )
        .await?
    {
        n = next;
    }
    Ok((n, hint))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hint_parsing() {
        assert_eq!(parse_hint(b"12"), Some(12));
        assert_eq!(parse_hint(b"0"), Some(0));
        assert_eq!(parse_hint(b"12\n"), None);
        assert_eq!(parse_hint(b"abc"), None);
        assert_eq!(parse_hint(b""), None);
        assert_eq!(parse_hint(b"99999999999999999999999"), None);
    }

    #[test]
    fn metadata_must_be_v2() {
        let v1 = br#"{"format-version":1,"table-uuid":"9c12d441-03fe-4693-9a96-a0705ddf69c1","location":"s3://b/t","last-updated-ms":1,"last-column-id":1,"schema":{"type":"struct","fields":[{"id":1,"name":"a","required":false,"type":"long"}]},"partition-spec":[]}"#;
        assert!(parse_metadata(v1).is_err());
        assert!(parse_metadata(b"not json").is_err());
        assert!(parse_metadata(b"{}").is_err());
    }
}
