//! 系统表 `_ledger`（REQ-AMB-GOV-LEDGER-V3）：`{org}` 下每个存在的数据集恰一行，由提示、当前快照与计数对象现算。
//!
//! - 条数、字节与首末时间跨两层：当前快照 `summary` 与 `summary.cold` 合起来算；`tier` 按两层的条数取值。
//!
//! - `status` 取数据集闸门中最先进入的停用状态；台账自己读 `_current` 与快照时读到停用情形，同样经闸门进入停用并以 `status` 呈现。
//! - 取自当前快照的列只对 `active` 的数据集有值；创世快照尚未成为 `_current` 的数据集（[`ChainReader::current`]
//!   返回 `None`，不经对象存储）计数为 0、时刻与所在层为 NULL。
//! - `sources` 与 `reconciliation` 按查询范围读计数对象；读不出（版本过高或内容无法解析）只让该行为 `unknown`。
//! - 判定汇总（`ts_sources`、六个窗口数与 `silent_sources`）按判定时刻用同一个判定函数（[`super::verdict`]）现算；
//!   为判断上报中断，计数对象另读查询范围开始之前 24 小时，不论查询引用了哪些列；`unknown` 的行这八列为 NULL。
//! - 对象存储不可访问时整条查询回 503；台账不读桶 manifest 与数据文件。

use std::collections::BTreeSet;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::ColumnType;
use amberlog_storage::chain::{ChainError, Snapshot};
use amberlog_storage::gate::DatasetState;
use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;

use super::verdict::{Verdict, reports_by_source, silent_in_range};
use super::{Context, CounterError, SystemTable, int64, judge_windows};
use crate::error::QueryError;

pub(super) const COLUMNS: &[(&str, ColumnType)] = &[
    ("dataset", ColumnType::Utf8),
    ("status", ColumnType::Utf8),
    ("sources", ColumnType::Utf8),
    ("time_min", ColumnType::Int64),
    ("time_max", ColumnType::Int64),
    ("record_count", ColumnType::Int64),
    ("size_bytes", ColumnType::Int64),
    ("tier", ColumnType::Utf8),
    ("pending_lake_record_count", ColumnType::Int64),
    ("retention", ColumnType::Utf8),
    ("expires_at", ColumnType::Int64),
    ("reconciliation", ColumnType::Utf8),
    ("ts_sources", ColumnType::Utf8),
    ("windows_pending", ColumnType::Int64),
    ("windows_consistent", ColumnType::Int64),
    ("windows_missing", ColumnType::Int64),
    ("windows_duplicate", ColumnType::Int64),
    ("windows_silent", ColumnType::Int64),
    ("windows_undeterminable", ColumnType::Int64),
    ("silent_sources", ColumnType::Int64),
];

/// `9999-12-31T23:59:59.999999Z`（UTC 微秒）：`expires_at` 的上限。
const MAX_EXPIRES_AT: i64 = 253_402_300_799_999_999;

/// 取自当前快照的各列。
#[derive(Default)]
struct Current {
    time_min: Option<i64>,
    time_max: Option<i64>,
    record_count: Option<i64>,
    size_bytes: Option<i64>,
    tier: Option<&'static str>,
    pending_lake_record_count: Option<i64>,
}

struct Row {
    dataset: String,
    status: &'static str,
    sources: Option<String>,
    current: Current,
    expires_at: Option<i64>,
    reconciliation: &'static str,
    /// 判定汇总；`reconciliation` 为 `unknown` 时为 `None`（八列都为 NULL）。
    summary: Option<Summary>,
}

/// 判定汇总（REQ-AMB-GOV-LEDGER-V3）。
struct Summary {
    ts_sources: Option<String>,
    /// 各判定的窗口数，次序同 [`Verdict::ALL`]。
    windows: [i64; 6],
    silent_sources: i64,
}

fn status(state: DatasetState) -> &'static str {
    match state {
        DatasetState::Normal => "active",
        DatasetState::IncompatibleFormat => "disabled_format_version",
        DatasetState::ManifestUnreadable => "disabled_manifest_unreadable",
    }
}

fn too_large(dataset: &Dataset, what: &str) -> QueryError {
    QueryError::internal(format!("{what} of {dataset} exceeds the Int64 range"))
}

/// 读当前快照：数据集停用（此前已读到或本次读到）时为 `Ok(Err(状态))`。
async fn current_snapshot(
    cx: &Context<'_>,
    dataset: &Dataset,
) -> Result<Result<Option<Arc<Snapshot>>, DatasetState>, QueryError> {
    // 在用快照登记（REQ-AMB-STO-SNAPSHOT-GC-IN-USE）：台账只读快照本身，读完即放下
    let mut pin = cx.chain.reading(dataset);
    let read = match cx.chain.current(dataset).await {
        Ok(None) => Ok(None),
        Ok(Some(id)) => {
            pin.settle(id);
            cx.chain.snapshot(dataset, id).await.map(Some)
        }
        Err(e) => Err(e),
    };
    drop(pin);
    match read {
        Ok(s) => Ok(Ok(s)),
        // 本次读到停用：此前若已并发读到另一种情形，取先读到者
        Err(ChainError::Disabled(state)) => Ok(Err(match cx.gates.first_state(dataset) {
            DatasetState::Normal => state,
            first => first,
        })),
        Err(ChainError::Unavailable(m)) => Err(QueryError::new(
            amberlog_common::error::Reason::Unavailable,
            m,
        )),
        // 读 `_current` 与快照的完整性问题都使数据集进入停用状态；万一没有，按未归类的失败作答。
        Err(ChainError::Integrity(m)) => match cx.gates.first_state(dataset) {
            DatasetState::Normal => Err(QueryError::internal(m)),
            state => Ok(Err(state)),
        },
    }
}

fn from_snapshot(
    cx: &Context<'_>,
    dataset: &Dataset,
    snapshot: Option<&Snapshot>,
) -> Result<Current, QueryError> {
    let Some(s) = snapshot else {
        // 创世快照尚未成为 `_current`
        return Ok(Current {
            record_count: Some(0),
            size_bytes: Some(0),
            pending_lake_record_count: Some(0),
            ..Current::default()
        });
    };
    let pending: u64 = s
        .buckets
        .iter()
        .filter(|b| cx.governance.buckets.pending_lake(b, cx.now_us))
        .map(|b| b.record_count)
        .sum();
    let (hot, cold) = (&s.summary, &s.summary.cold);
    // 条数与字节为热层与冷层之和，首末时间取两层中不为 null 者（REQ-AMB-GOV-LEDGER-V2）
    let record_count = hot
        .record_count
        .checked_add(cold.record_count)
        .and_then(int64)
        .ok_or_else(|| too_large(dataset, "record_count"))?;
    let size_bytes = hot
        .size_bytes
        .checked_add(cold.size_bytes)
        .and_then(int64)
        .ok_or_else(|| too_large(dataset, "size_bytes"))?;
    let pending = int64(pending).ok_or_else(|| too_large(dataset, "pending_lake_record_count"))?;
    let tier = match (hot.record_count > 0, cold.record_count > 0) {
        (false, false) => None,
        (true, false) if pending > 0 => Some("hot_pending_lake"),
        (true, false) => Some("hot"),
        (false, true) => Some("cold"),
        (true, true) => Some("hot_cold"),
    };
    let either = |a: Option<i64>, b: Option<i64>, pick: fn(i64, i64) -> i64| match (a, b) {
        (Some(x), Some(y)) => Some(pick(x, y)),
        (x, y) => x.or(y),
    };
    Ok(Current {
        time_min: either(hot.time_min, cold.time_min, i64::min),
        time_max: either(hot.time_max, cold.time_max, i64::max),
        record_count: Some(record_count),
        size_bytes: Some(size_bytes),
        tier,
        pending_lake_record_count: Some(pending),
    })
}

async fn row(cx: &Context<'_>, dataset: &Dataset) -> Result<Row, QueryError> {
    let mut state = cx.gates.first_state(dataset);
    let mut current = Current::default();
    if state == DatasetState::Normal {
        match current_snapshot(cx, dataset).await? {
            Ok(snapshot) => current = from_snapshot(cx, dataset, snapshot.as_deref())?,
            Err(disabled) => state = disabled,
        }
    }

    // 计数对象每次都读（含范围开始之前 24 小时，为判断上报中断），不论查询引用了哪些列
    let lookback_start = cx.start_time.saturating_sub(super::verdict::LOOKBACK_US);
    let (sources, reconciliation, summary) = match cx
        .governance
        .counters
        .windows(dataset, lookback_start, cx.end_time)
        .await
    {
        Ok(all) => {
            let first = all.partition_point(|w| w.window_start < cx.start_time);
            let (before, windows) = all.split_at(first);
            let names: BTreeSet<&str> = windows
                .iter()
                .flat_map(|w| w.sent.keys().map(String::as_str))
                .collect();
            let (sources, reconciliation) = if names.is_empty() {
                (None, "two_way")
            } else {
                (
                    Some(names.into_iter().collect::<Vec<_>>().join(",")),
                    "three_way",
                )
            };
            let summary = summary(cx, dataset, &all, before.to_vec(), windows).await?;
            (sources, reconciliation, Some(summary))
        }
        Err(CounterError::Unparseable(_)) => (None, "unknown", None),
        Err(e @ CounterError::Unavailable(_)) => return Err(e.into()),
    };

    let expires_at = cx.governance.retention.as_ref().and_then(|r| {
        current
            .time_min
            .and_then(|t| t.checked_add(r.micros))
            .filter(|&t| t <= MAX_EXPIRES_AT)
    });
    Ok(Row {
        dataset: dataset.to_string(),
        status: status(state),
        sources,
        current,
        expires_at,
        reconciliation,
        summary,
    })
}

/// 判定汇总：`windows` 是 `window_start` 落在查询范围内的窗口，`before` 是范围开始之前 24 小时内的窗口，`all` 是两者之并。
async fn summary(
    cx: &Context<'_>,
    dataset: &Dataset,
    all: &[super::CounterWindow],
    before: Vec<super::CounterWindow>,
    windows: &[super::CounterWindow],
) -> Result<Summary, QueryError> {
    let names: BTreeSet<&str> = windows
        .iter()
        .flat_map(|w| w.sent_ts.keys().map(String::as_str))
        .collect();
    let ts_sources = (!names.is_empty()).then(|| names.into_iter().collect::<Vec<_>>().join(","));
    // 按窗口直接算，不经 `_reconciliation` 的列求和
    let mut counts = [0i64; 6];
    for j in judge_windows(cx, dataset, cx.start_time, windows, Some(before)).await? {
        counts[j.verdict.index()] += 1;
    }
    // 在查询范围内某个 W 上报中断、且判定时刻不在补发中的来源（补发中的来源按待定计，不算在 W 上报中断）
    let gov = cx.governance;
    let silent = reports_by_source(all)
        .into_iter()
        .filter(|(source, reported)| {
            !gov.backfill.backfilling(dataset, source, cx.now_us)
                && silent_in_range(
                    reported,
                    cx.start_time,
                    cx.end_time,
                    cx.now_us,
                    gov.grace_us,
                )
        })
        .count();
    debug_assert_eq!(Verdict::ALL.len(), counts.len());
    Ok(Summary {
        ts_sources,
        windows: counts,
        silent_sources: silent as i64,
    })
}

pub(super) async fn table(
    cx: &Context<'_>,
    datasets: &[Dataset],
) -> Result<RecordBatch, QueryError> {
    let mut rows = Vec::with_capacity(datasets.len());
    for d in datasets {
        rows.push(row(cx, d).await?);
    }

    let retention = cx.governance.retention.as_ref().map(|r| r.text.as_str());
    let mut dataset = StringBuilder::new();
    let mut status = StringBuilder::new();
    let mut sources = StringBuilder::new();
    let mut time_min = Int64Builder::new();
    let mut time_max = Int64Builder::new();
    let mut record_count = Int64Builder::new();
    let mut size_bytes = Int64Builder::new();
    let mut tier = StringBuilder::new();
    let mut pending = Int64Builder::new();
    let mut retention_col = StringBuilder::new();
    let mut expires_at = Int64Builder::new();
    let mut reconciliation = StringBuilder::new();
    let mut ts_sources = StringBuilder::new();
    let mut verdicts: [Int64Builder; 6] = std::array::from_fn(|_| Int64Builder::new());
    let mut silent_sources = Int64Builder::new();
    for r in &rows {
        dataset.append_value(&r.dataset);
        status.append_value(r.status);
        sources.append_option(r.sources.as_deref());
        time_min.append_option(r.current.time_min);
        time_max.append_option(r.current.time_max);
        record_count.append_option(r.current.record_count);
        size_bytes.append_option(r.current.size_bytes);
        tier.append_option(r.current.tier);
        pending.append_option(r.current.pending_lake_record_count);
        retention_col.append_option(retention);
        expires_at.append_option(r.expires_at);
        reconciliation.append_value(r.reconciliation);
        ts_sources.append_option(r.summary.as_ref().and_then(|s| s.ts_sources.as_deref()));
        for (i, b) in verdicts.iter_mut().enumerate() {
            b.append_option(r.summary.as_ref().map(|s| s.windows[i]));
        }
        silent_sources.append_option(r.summary.as_ref().map(|s| s.silent_sources));
    }
    let mut columns: Vec<ArrayRef> = vec![
        Arc::new(dataset.finish()),
        Arc::new(status.finish()),
        Arc::new(sources.finish()),
        Arc::new(time_min.finish()),
        Arc::new(time_max.finish()),
        Arc::new(record_count.finish()),
        Arc::new(size_bytes.finish()),
        Arc::new(tier.finish()),
        Arc::new(pending.finish()),
        Arc::new(retention_col.finish()),
        Arc::new(expires_at.finish()),
        Arc::new(reconciliation.finish()),
        Arc::new(ts_sources.finish()),
    ];
    columns.extend(
        verdicts
            .iter_mut()
            .map(|b| Arc::new(b.finish()) as ArrayRef),
    );
    columns.push(Arc::new(silent_sources.finish()));
    RecordBatch::try_new(SystemTable::Ledger.arrow_schema(), columns)
        .map_err(|e| QueryError::internal(e.to_string()))
}
