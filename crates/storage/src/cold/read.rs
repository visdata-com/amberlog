//! 冷层表的读取（读写共用）：当前版本的快照与存活文件集、某快照的存活文件、孤儿判定用的引用集、
//! 按字段编号读一个冷层数据文件。
//!
//! 读取不改变数据集状态：所需对象不存在或无法解析返回 [`ColdError::Broken`]，是否停用由调用方按读取的来源定
//! （REQ-AMB-STO-COLD-TABLE-READ-BROKEN）。

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::TIMESTAMP_COLUMN;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};
use bytes::Bytes;
use iceberg::spec::{
    Datum, ManifestFile, ManifestStatus, NestedFieldRef, PrimitiveLiteral, Snapshot, TableMetadata,
};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{PARQUET_FIELD_ID_META_KEY, ProjectionMask};

use super::commit::OPERATION;
use super::current::{self, parse_metadata};
use super::error::{ColdError, invalid};
use super::layout::{self, ColdLayout};
use super::manifests;
use crate::chain::{Digest, SnapshotId};
use crate::layout::HourBucket;
use crate::scan::{self, BATCH_ROWS, Batches};
use crate::store::Store;

/// 冷层快照摘要中 `amberlog.*` 所记的操作。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Operation {
    Lake {
        bucket: HourBucket,
        hot_snapshot_id: SnapshotId,
        file_digests: BTreeMap<String, Digest>,
    },
    Ttl {
        cutoff: i64,
    },
    Compaction {
        file_digests: BTreeMap<String, Digest>,
    },
    /// 没有 `amberlog.operation`、取值不是 `lake`、`ttl` 与 `compaction`，或 `amberlog.*` 写法不合：
    /// 不是本系统入湖、TTL 或冷层合并写出的快照。
    Other,
}

/// 冷层当前版本元数据所列的一个快照。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotView {
    pub snapshot_id: i64,
    pub parent_snapshot_id: Option<i64>,
    pub timestamp_ms: i64,
    pub operation: Operation,
}

/// 冷层的一个存活数据文件（清单条目）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveFile {
    /// 相对表目录。
    pub path: String,
    /// 清单条目的 `snapshot_id`（新增它的冷层快照）。
    pub added_snapshot_id: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// 分区 `_timestamp_day`（自 1970-01-01 起的天数）。
    pub day: i32,
}

/// 冷层当前版本的内容。
#[derive(Debug, Clone)]
pub struct TableView {
    pub location: String,
    pub version: u64,
    pub current_snapshot_id: Option<i64>,
    /// 当前 schema 的各列（按位置）。
    pub fields: Vec<NestedFieldRef>,
    pub snapshots: Vec<SnapshotView>,
    /// 当前快照的存活文件（按 `days` 只读了部分清单时，只含这些清单里的）。
    pub files: Vec<LiveFile>,
}

/// 读冷层当前版本：元数据所列的全部快照，与当前快照中 `days`（`None` 为全部）所含清单里的存活文件。
/// 尚无冷层表为 `Ok(None)`。
pub async fn table(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    days: Option<RangeInclusive<i32>>,
) -> Result<Option<TableView>, ColdError> {
    let (version, _) = current::version(store, layout, dataset).await?;
    if version == 0 {
        return Ok(None);
    }
    table_at(store, layout, dataset, version, days)
        .await
        .map(Some)
}

/// 读已写出的版本 `version` 的元数据与 `days` 所及的清单（`None` 为全部清单）。
pub async fn table_at(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    version: u64,
    days: Option<RangeInclusive<i32>>,
) -> Result<TableView, ColdError> {
    let metadata = metadata(store, layout, dataset, version).await?;
    // 元数据模型内部以哈希表存快照，遍历次序不定：按序号（提交先后）排定
    let mut snapshots: Vec<_> = metadata.snapshots().collect();
    snapshots.sort_by_key(|s| s.sequence_number());
    let files = match metadata.current_snapshot() {
        Some(s) => {
            let mut list = manifests::manifest_list(store, layout, dataset, s).await?;
            if let Some(days) = &days {
                list.retain(|m| may_contain(m, days));
            }
            live_files(store, layout, dataset, &list).await?
        }
        None => Vec::new(),
    };
    Ok(TableView {
        location: metadata.location().to_owned(),
        version,
        current_snapshot_id: metadata.current_snapshot_id(),
        fields: metadata.current_schema().as_struct().fields().to_vec(),
        snapshots: snapshots.into_iter().map(|s| view(s)).collect(),
        files,
    })
}

/// 读已写出的版本 `version` 的元数据（版本文件永不改写，所以可按编号重读）。
pub async fn metadata(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    version: u64,
) -> Result<TableMetadata, ColdError> {
    let rel = layout::metadata_path(version);
    let fetched = store
        .read(&layout.object_path(dataset, &rel))
        .await?
        .ok_or_else(|| ColdError::Broken(format!("{rel} does not exist")))?;
    parse_metadata(&fetched.bytes).map_err(|e| ColdError::Broken(format!("{rel}: {e}")))
}

/// 版本 `version` 所列快照 `snapshot_id` 下的全部存活文件。
pub async fn snapshot_files(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    version: u64,
    snapshot_id: i64,
) -> Result<Vec<LiveFile>, ColdError> {
    let m = metadata(store, layout, dataset, version).await?;
    let s = m
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| invalid(format!("snapshot {snapshot_id} is not in v{version}")))?;
    let list = manifests::manifest_list(store, layout, dataset, s).await?;
    live_files(store, layout, dataset, &list).await
}

/// 版本 `version` 中任一快照（经其清单列表与清单）引用的全部对象，相对表目录（REQ-AMB-STO-COLD-TABLE-ORPHAN）。
pub async fn referenced(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    version: u64,
) -> Result<HashSet<String>, ColdError> {
    let m = metadata(store, layout, dataset, version).await?;
    let mut out = HashSet::new();
    let rel = |uri: &str| {
        layout
            .rel_of_uri(dataset, uri)
            .map(str::to_owned)
            .ok_or_else(|| ColdError::Broken(format!("{uri} is outside the table location")))
    };
    let mut seen = HashSet::new();
    for s in m.snapshots() {
        out.insert(rel(s.manifest_list())?);
        for f in manifests::manifest_list(store, layout, dataset, s).await? {
            if !seen.insert(f.manifest_path.clone()) {
                continue;
            }
            out.insert(rel(&f.manifest_path)?);
            for e in manifests::entries(store, layout, dataset, &f).await? {
                out.insert(rel(e.file_path())?);
            }
        }
    }
    Ok(out)
}

/// 版本 `version` 所列快照 `snapshot_id` 经其清单列表与清单引用的冷层数据文件（相对表目录；清单条目为任何状态都算）。
pub async fn snapshot_references(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    version: u64,
    snapshot_id: i64,
) -> Result<HashSet<String>, ColdError> {
    let m = metadata(store, layout, dataset, version).await?;
    let s = m
        .snapshot_by_id(snapshot_id)
        .ok_or_else(|| ColdError::Broken(format!("snapshot {snapshot_id} is not in v{version}")))?;
    let mut out = HashSet::new();
    for f in manifests::manifest_list(store, layout, dataset, s).await? {
        for e in manifests::entries(store, layout, dataset, &f).await? {
            let rel = layout.rel_of_uri(dataset, e.file_path()).ok_or_else(|| {
                ColdError::Broken(format!("{} is outside the table location", e.file_path()))
            })?;
            out.insert(rel.to_owned());
        }
    }
    Ok(out)
}

/// 冷层数据文件 `path`（相对表目录）的完整字节；对象不存在为 [`ColdError::Missing`]。
pub async fn file_bytes(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    path: &str,
) -> Result<Bytes, ColdError> {
    Ok(store
        .read(&layout.object_path(dataset, path))
        .await?
        .ok_or_else(|| ColdError::Missing(path.to_owned()))?
        .bytes)
}

/// 按字段编号逐批读冷层数据文件 `path`（相对表目录）：每批的 schema 恰为 `target`，其每列按该列的 `PARQUET:field_id`
/// 对到文件中同一字段编号的列，文件中没有的列为空值（REQ-AMB-STO-COLD-TABLE、REQ-AMB-QRY-CROSS-TIER-MISSING-COLUMN）。
///
/// 只解码 `target` 所含的列（另加 `_timestamp`，以便没有别的列时仍按行数交出批次），只读 `_timestamp` 上下界与 `time`
/// 相交的行组（[`scan::row_groups_in`]）；批次逐批解码，不攒成整份（REQ-AMB-QRY-RESOURCE-LIMITS-V2 METERED-SCOPE）。
/// 文件尾部元数据无法解析、或某列的类型与 `target` 不符，在返回之前报出；之后的解码失败作为迭代中的一项报出。
pub async fn read_file(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    path: &str,
    target: SchemaRef,
    time: Range<i64>,
) -> Result<Batches<ColdError>, ColdError> {
    let bytes = file_bytes(store, layout, dataset, path).await?;
    let path = path.to_owned();
    let broken = |e: parquet::errors::ParquetError| ColdError::Broken(format!("{path}: {e}"));
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(broken)?;
    let file = Arc::clone(builder.schema());
    // `target` 各列在文件中的列号（按字段编号对上），类型不符即内容无法解析
    let mut found = Vec::with_capacity(target.fields().len());
    for want in target.fields() {
        let id = want.metadata().get(PARQUET_FIELD_ID_META_KEY);
        let at = file
            .fields()
            .iter()
            .position(|f| id.is_some() && f.metadata().get(PARQUET_FIELD_ID_META_KEY) == id);
        if let Some(i) = at
            && file.field(i).data_type() != want.data_type()
        {
            return Err(ColdError::Broken(format!(
                "{path}: column with field id {} has another type",
                id.map_or("", String::as_str)
            )));
        }
        found.push(at);
    }
    let ts = file.index_of(TIMESTAMP_COLUMN).ok();
    let roots: BTreeSet<usize> = found.iter().flatten().copied().chain(ts).collect();
    let mask = ProjectionMask::roots(builder.parquet_schema(), roots.iter().copied());
    let mut builder = builder.with_projection(mask).with_batch_size(BATCH_ROWS);
    if let Some(ts) = ts {
        let groups = scan::row_groups_in(builder.metadata(), ts, &time);
        builder = builder.with_row_groups(groups);
    }
    let reader = builder.build().map_err(broken)?;
    // 解码出的批次按列号升序含 `roots` 各列
    let at: Vec<Option<usize>> = found
        .iter()
        .map(|f| f.map(|i| roots.iter().position(|r| *r == i).expect("列号在 roots 中")))
        .collect();
    Ok(Box::new(reader.map(move |batch| {
        let batch = batch.map_err(|e| ColdError::Broken(format!("{path}: {e}")))?;
        let columns = target
            .fields()
            .iter()
            .zip(&at)
            .map(|(want, at)| match at {
                Some(i) => Arc::clone(batch.column(*i)),
                None => new_null_array(want.data_type(), batch.num_rows()),
            })
            .collect::<Vec<ArrayRef>>();
        let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
        RecordBatch::try_new_with_options(Arc::clone(&target), columns, &options).map_err(invalid)
    })))
}

/// 清单列表中该清单的分区摘要与 `days` 是否可能有交集（摘要缺上下界时按可能处置）。
fn may_contain(m: &ManifestFile, days: &RangeInclusive<i32>) -> bool {
    let Some(summary) = m.partitions.as_ref().and_then(|p| p.first()) else {
        return true;
    };
    // 日期分区值的单值序列化：4 字节小端整数（Iceberg 表格式规范附录 D）
    let day = |b: Option<&Vec<u8>>| {
        b.and_then(|b| <[u8; 4]>::try_from(b.as_slice()).ok())
            .map(i32::from_le_bytes)
    };
    let lo = day(summary.lower_bound.as_deref()).unwrap_or(i32::MIN);
    let hi = day(summary.upper_bound.as_deref()).unwrap_or(i32::MAX);
    lo <= *days.end() && hi >= *days.start()
}

async fn live_files(
    store: &Store,
    layout: &ColdLayout,
    dataset: &Dataset,
    list: &[ManifestFile],
) -> Result<Vec<LiveFile>, ColdError> {
    let mut out = Vec::new();
    for f in list {
        for e in manifests::entries(store, layout, dataset, f).await? {
            if e.status() == ManifestStatus::Deleted {
                continue;
            }
            let path = layout
                .rel_of_uri(dataset, e.file_path())
                .ok_or_else(|| {
                    ColdError::Broken(format!("{} is outside the table location", e.file_path()))
                })?
                .to_owned();
            let d = e.data_file();
            let ts = |b: Option<&Datum>| match b.map(Datum::literal) {
                Some(PrimitiveLiteral::Long(v)) => Some(*v),
                _ => None,
            };
            let (Some(min_timestamp), Some(max_timestamp)) =
                (ts(d.lower_bounds().get(&1)), ts(d.upper_bounds().get(&1)))
            else {
                // REQ-AMB-STO-COLD-TABLE-ENTRY-TS：本系统写出的条目都带 `_timestamp` 的上下界
                return Err(ColdError::Broken(format!("{path}: no _timestamp bounds")));
            };
            let day = match d.partition().fields().first() {
                Some(Some(iceberg::spec::Literal::Primitive(PrimitiveLiteral::Int(day)))) => *day,
                _ => {
                    return Err(ColdError::Broken(format!(
                        "{path}: no _timestamp_day partition"
                    )));
                }
            };
            out.push(LiveFile {
                path,
                added_snapshot_id: e.snapshot_id().expect("已按继承规则补上"),
                record_count: d.record_count(),
                size_bytes: d.file_size_in_bytes(),
                min_timestamp,
                max_timestamp,
                day,
            });
        }
    }
    out.sort_by(|a, b| a.path.cmp(&b.path));
    Ok(out)
}

fn view(s: &Snapshot) -> SnapshotView {
    SnapshotView {
        snapshot_id: s.snapshot_id(),
        parent_snapshot_id: s.parent_snapshot_id(),
        timestamp_ms: s.timestamp_ms(),
        operation: operation(&s.summary().additional_properties),
    }
}

fn operation(props: &std::collections::HashMap<String, String>) -> Operation {
    let file_digests = || -> Option<BTreeMap<String, Digest>> {
        let digests: BTreeMap<String, String> =
            serde_json::from_str(props.get("amberlog.file_digests")?).ok()?;
        digests
            .into_iter()
            .map(|(k, v)| Some((k, Digest::parse(&v)?)))
            .collect()
    };
    let parse = || -> Option<Operation> {
        match props.get(OPERATION)?.as_str() {
            "lake" => {
                let bucket = HourBucket::parse(props.get("amberlog.bucket")?)?;
                let hot = props.get("amberlog.hot_snapshot_id")?.parse().ok()?;
                Some(Operation::Lake {
                    bucket,
                    hot_snapshot_id: SnapshotId(hot),
                    file_digests: file_digests()?,
                })
            }
            "ttl" => Some(Operation::Ttl {
                cutoff: props.get("amberlog.ttl_cutoff")?.parse().ok()?,
            }),
            "compaction" => Some(Operation::Compaction {
                file_digests: file_digests()?,
            }),
            _ => None,
        }
    };
    parse().unwrap_or(Operation::Other)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> std::collections::HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect()
    }

    #[test]
    fn operation_reads_compaction_with_file_digests() {
        let d = Digest::of(b"o");
        let digests = format!(r#"{{"data/_timestamp_day=2026-10-14/o.parquet":"{d}"}}"#);
        assert_eq!(
            operation(&props(&[
                ("amberlog.operation", "compaction"),
                ("amberlog.file_digests", &digests),
            ])),
            Operation::Compaction {
                file_digests: [("data/_timestamp_day=2026-10-14/o.parquet".to_owned(), d)].into(),
            }
        );
        // 缺 `amberlog.file_digests` 或不是合法 JSON：不认作冷层合并写出的快照
        assert_eq!(
            operation(&props(&[("amberlog.operation", "compaction")])),
            Operation::Other
        );
        assert_eq!(
            operation(&props(&[
                ("amberlog.operation", "compaction"),
                ("amberlog.file_digests", "{"),
            ])),
            Operation::Other
        );
    }
}
