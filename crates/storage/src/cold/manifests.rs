//! 冷层清单列表与清单的读取（Iceberg 第 2 版 Avro）。
//!
//! 位置一律是表位置之下的 URI；不在本表表位置之下、读到不存在或不是合法的清单列表、清单的，都是
//! [`ColdError::Broken`]（REQ-AMB-STO-COLD-TABLE-UNPARSEABLE）。是否因此停用由调用方按读取的来源定。

use amberlog_common::dataset::Dataset;
use bytes::Bytes;
use iceberg::spec::{
    FormatVersion, Manifest, ManifestContentType, ManifestEntry, ManifestFile, ManifestList,
    Snapshot,
};

use super::error::ColdError;
use super::layout::ColdLayout;
use crate::store::Store;

/// 读表位置之下 URI 为 `uri` 的对象的完整字节。
pub async fn read_uri(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    uri: &str,
) -> Result<Bytes, ColdError> {
    let rel = layout
        .rel_of_uri(dataset, uri)
        .ok_or_else(|| ColdError::Broken(format!("{uri} is outside the table location")))?;
    Ok(store
        .read(&layout.object_path(dataset, rel))
        .await?
        .ok_or_else(|| ColdError::Missing(rel.to_owned()))?
        .bytes)
}

/// 快照 `snapshot` 的清单列表。
pub async fn manifest_list(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    snapshot: &Snapshot,
) -> Result<Vec<ManifestFile>, ColdError> {
    let bytes = read_uri(store, layout, dataset, snapshot.manifest_list()).await?;
    let list = ManifestList::parse_with_version(&bytes, FormatVersion::V2).map_err(|e| {
        ColdError::Broken(format!("manifest list {}: {e}", snapshot.manifest_list()))
    })?;
    Ok(list.entries().to_vec())
}

/// 清单 `file` 的全部条目，已按 Iceberg 第 2 版的继承规则补上快照号与序号。
pub async fn entries(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    file: &ManifestFile,
) -> Result<Vec<ManifestEntry>, ColdError> {
    if file.content != ManifestContentType::Data {
        // 冷层表没有删除清单（REQ-AMB-STO-COLD-TABLE-NO-ROW-DELETE）；出现即不是本系统写出的表
        return Err(ColdError::Broken(format!(
            "manifest {} is a delete manifest",
            file.manifest_path
        )));
    }
    let bytes = read_uri(store, layout, dataset, &file.manifest_path).await?;
    let manifest = Manifest::parse_avro(&bytes)
        .map_err(|e| ColdError::Broken(format!("manifest {}: {e}", file.manifest_path)))?;
    let (entries, _) = manifest.into_parts();
    Ok(entries
        .into_iter()
        .map(|e| {
            let mut e = (*e).clone();
            inherit(&mut e, file);
            e
        })
        .collect())
}

/// 清单条目未写的快照号与序号取自清单列表中该清单的记录（Iceberg 表格式规范「Sequence Number Inheritance」）。
fn inherit(entry: &mut ManifestEntry, file: &ManifestFile) {
    entry.snapshot_id.get_or_insert(file.added_snapshot_id);
    entry.sequence_number.get_or_insert(file.sequence_number);
    entry
        .file_sequence_number
        .get_or_insert(file.sequence_number);
}
