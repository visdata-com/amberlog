//! 跨层查询的冷侧（REQ-AMB-QRY-CROSS-TIER）：确定冷层当前版本、算出冷层部分、按字段编号读冷层数据文件。
//!
//! 冷侧直接读冷层表（`<storage.prefix>/_iceberg/`），经 [`ColdReader`]，不经冷层写入。分三步，次序与
//! `_search` 的报告顺序对应（REQ-AMB-QRY-SEARCH-ENDPOINT-V6）：
//! 1. [`version`]：在确定当前快照之后确定冷层当前版本（读冷层提示与 HEAD 探测；ORDER、PER-QUERY），与确定当前快照同位，
//!    先于 `unknown_column`。尚无冷层表时冷层部分为空集，冷层表整体消失除外（NO-TABLE）。
//! 2. [`plan`]：读该版本的元数据与按分区摘要不能排除的清单，按清单条目的 `_timestamp` 上下界与分区日剪枝（PRUNE-TIME），
//!    剔除处于交接窗口的冷层提交新增的文件（WINDOW），与桶 manifest 同位，在绑定检查之后。
//! 3. [`read`]：按字段编号逐批读冷层数据文件，缺列取 NULL（FIELD-ID、MISSING-COLUMN）。
//!
//! 读取失败的分类（UNAVAILABLE、CORRUPT）：对象存储不可访问回 503；所需冷层对象内容无法解析或不存在回 500，并使数据集
//! 进入 manifest 不可读情形（REQ-AMB-STO-COLD-TABLE-READ-BROKEN；冷层读取本身不改数据集状态，停用由查询这一侧做）。
//! 冷层数据文件不存在时先由调用方重判是否「数据已移动」（MOVED），见 [`crate::search`]；清单列表或清单不存在时由 [`plan`]
//! 重判（REQ-AMB-QRY-CROSS-TIER-V2）。
//! 冷层表的读取不缓存冷层当前版本；已读对象的复用由 [`ColdReader`] 的实现决定。

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Range, RangeInclusive};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_storage::chain::{ChainReader, FileEntry, Snapshot};
use amberlog_storage::cold::{
    ColdBatches, ColdDataFile, ColdOperation, ColdReadError, ColdReader, ColdScope, ColdTable,
};
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::layout::HourBucket;
use arrow::datatypes::SchemaRef;

use crate::error::QueryError;

const DAY_US: i64 = 86_400_000_000;

/// UTC 微秒所在的 UTC 日（自 1970-01-01 起的天数，冷层分区 `_timestamp_day` 的取值）。
fn day_of(us: i64) -> i32 {
    us.div_euclid(DAY_US) as i32
}

/// 查询范围 `[start, end)` 所含的 UTC 日。
fn days(start: i64, end: i64) -> RangeInclusive<i32> {
    day_of(start)..=day_of(end - 1)
}

/// 冷侧读取失败的应答：不可访问 503；内容无法解析或不存在 500，数据集进入 manifest 不可读情形。
pub(crate) fn error(gates: &DatasetGates, dataset: &Dataset, e: ColdReadError) -> QueryError {
    match e {
        ColdReadError::Unavailable(m) => QueryError::new(Reason::Unavailable, m),
        ColdReadError::Integrity(m) | ColdReadError::NotFound(m) => {
            gates.enter(dataset, DatasetState::ManifestUnreadable);
            QueryError::internal(m)
        }
    }
}

/// 确定冷层当前版本（REQ-AMB-QRY-CROSS-TIER-ORDER：调用方已确定当前快照 `snapshot`）。
///
/// 返回 `None` 即冷层部分为空集。尚无冷层表而 `snapshot` 的 `summary.cold.record_count` 大于 0 是冷层表整体消失
/// （REQ-AMB-STO-COLD-TABLE-TABLE-GONE）：数据集进入 manifest 不可读情形，以 500 作答（NO-TABLE）。
pub(crate) async fn version(
    reader: &dyn ColdReader,
    gates: &DatasetGates,
    dataset: &Dataset,
    snapshot: &Snapshot,
) -> Result<Option<u64>, QueryError> {
    match reader.version(dataset).await {
        Ok(Some(v)) => Ok(Some(v)),
        Ok(None) if snapshot.summary.cold.record_count == 0 => Ok(None),
        Ok(None) => {
            gates.enter(dataset, DatasetState::ManifestUnreadable);
            Err(QueryError::internal(format!(
                "the cold table of {dataset} is gone while the current snapshot records {} cold records",
                snapshot.summary.cold.record_count
            )))
        }
        Err(e) => Err(error(gates, dataset, e)),
    }
}

/// 一次查询的冷层部分：冷层当前版本与要扫描的冷层数据文件。
#[derive(Debug, Clone)]
pub(crate) struct ColdPart {
    pub table: Arc<ColdTable>,
    /// 剪枝并剔除交接窗口之后留下的冷层存活数据文件。
    pub files: Vec<ColdDataFile>,
}

/// 冷层数据文件 `f` 是否可能含查询范围 `[start, end)` 内的记录（REQ-AMB-QRY-CROSS-TIER-PRUNE-TIME：清单条目的
/// `_timestamp` 上下界所示闭区间与分区日都要与查询范围相交）。
fn in_range(f: &ColdDataFile, start: i64, end: i64) -> bool {
    f.min_timestamp < end && f.max_timestamp >= start && days(start, end).contains(&f.day)
}

/// 读冷层当前版本 `version`，算出冷层部分（REQ-AMB-QRY-CROSS-TIER-COLD-PART、-WINDOW、-PRUNE-TIME）。
///
/// 只读按分区摘要不能排除的清单（ALL-OR-NOTHING 允许跳过其余）。时间剪枝之后仍有文件留下的 `lake` 冷层提交，才判定
/// 是否处于交接窗口：为此读取当前快照 `snapshot` 中这些提交所记之桶的桶 manifest（经 `chain`，读取失败按热层的规则）。
#[allow(clippy::too_many_arguments)]
pub(crate) async fn plan(
    reader: &dyn ColdReader,
    chain: &dyn ChainReader,
    gates: &DatasetGates,
    dataset: &Dataset,
    snapshot: &Snapshot,
    version: u64,
    start: i64,
    end: i64,
) -> Result<ColdPart, QueryError> {
    let table = match reader
        .table_at(dataset, version, ColdScope::Days(days(start, end)))
        .await
    {
        Ok(t) => t,
        Err(ColdReadError::NotFound(m)) => {
            return Err(moved_metadata(reader, gates, dataset, snapshot, m).await);
        }
        Err(e) => return Err(error(gates, dataset, e)),
    };
    let mut files: Vec<ColdDataFile> = table
        .files
        .iter()
        .filter(|f| in_range(f, start, end))
        .cloned()
        .collect();

    // 留下的文件由哪些 `lake` 冷层提交新增，它们记的是哪些桶
    let added: BTreeSet<i64> = files.iter().map(|f| f.added_snapshot_id).collect();
    let buckets: BTreeSet<HourBucket> = table
        .snapshots
        .iter()
        .filter(|s| added.contains(&s.snapshot_id))
        .filter_map(|s| match &s.operation {
            ColdOperation::Lake { bucket, .. } => Some(*bucket),
            _ => None,
        })
        .collect();
    if !buckets.is_empty() {
        let live = live_files(chain, dataset, snapshot, &buckets).await?;
        let window: BTreeSet<i64> = table
            .handover_window(&live)
            .into_iter()
            .map(|w| w.iceberg_snapshot_id)
            .collect();
        files.retain(|f| !window.contains(&f.added_snapshot_id));
    }
    Ok(ColdPart { table, files })
}

/// 作答所需的清单列表或清单 `missing` 不存在时的重判（REQ-AMB-QRY-CROSS-TIER-V2 关于数据已移动的条款）：重新确定冷层
/// 当前版本，读其当前快照的全部清单列表与清单。都读得到即 `missing` 已不被它引用（冷层保留过期了旧快照、冷层孤儿清理
/// 随后删除），回 503、不停用；读到不存在或无法解析即仍被引用或另有损坏，按冷层对象的读取失败回 500 并停用。
async fn moved_metadata(
    reader: &dyn ColdReader,
    gates: &DatasetGates,
    dataset: &Dataset,
    snapshot: &Snapshot,
    missing: String,
) -> QueryError {
    let moved = || QueryError::moved(format!("data moved while the query ran: {missing}"));
    match version(reader, gates, dataset, snapshot).await {
        Ok(Some(v)) => match reader.table_at(dataset, v, ColdScope::All).await {
            Ok(_) => moved(),
            Err(e) => error(gates, dataset, e),
        },
        Ok(None) => moved(),
        Err(e) => e,
    }
}

/// 当前快照 `snapshot` 中 `buckets` 各桶的存活数据文件（桶 manifest 的 `files`）；不在快照 `buckets` 中的桶不列。
async fn live_files(
    chain: &dyn ChainReader,
    dataset: &Dataset,
    snapshot: &Snapshot,
    buckets: &BTreeSet<HourBucket>,
) -> Result<BTreeMap<HourBucket, Vec<FileEntry>>, QueryError> {
    let mut live = BTreeMap::new();
    for r in &snapshot.buckets {
        let Some(b) = HourBucket::parse(&r.bucket) else {
            continue;
        };
        if !buckets.contains(&b) {
            continue;
        }
        let manifest = chain.bucket_manifest(dataset, snapshot, r).await?;
        live.insert(b, manifest.files.clone());
    }
    Ok(live)
}

/// 按字段编号逐批读冷层数据文件 `file`：每批的列为 `target`（stream 当前 schema 中本查询用到的列，文件里没有的为 NULL），
/// 由调用方再对齐到表 schema（REQ-AMB-QRY-CROSS-TIER-FIELD-ID、-MISSING-COLUMN）；`time` 为查询范围，范围之外的行组可以
/// 不读。
pub(crate) async fn read(
    reader: &dyn ColdReader,
    dataset: &Dataset,
    table: &ColdTable,
    file: &ColdDataFile,
    target: SchemaRef,
    time: Range<i64>,
) -> Result<ColdBatches, ColdReadError> {
    reader.read_file(dataset, table, file, target, time).await
}

/// 冷层数据文件 `path` 是否在冷层版本 `version` 的存活文件中（REQ-AMB-QRY-CROSS-TIER-MOVED 的重判；`day` 为其分区日，
/// 只读该日的清单）。
pub(crate) async fn is_live(
    reader: &dyn ColdReader,
    gates: &DatasetGates,
    dataset: &Dataset,
    version: u64,
    path: &str,
    day: i32,
) -> Result<bool, QueryError> {
    let table = reader
        .table_at(dataset, version, ColdScope::Days(day..=day))
        .await
        .map_err(|e| error(gates, dataset, e))?;
    Ok(table.files.iter().any(|f| f.path == path))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn days_cover_half_open_range() {
        assert_eq!(days(0, DAY_US), 0..=0);
        assert_eq!(days(0, DAY_US + 1), 0..=1);
        assert_eq!(days(DAY_US - 1, DAY_US), 0..=0);
        assert_eq!(day_of(-1), -1);
    }
}
