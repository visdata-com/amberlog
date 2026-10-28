//! 系统表 `_reconciliation`（REQ-AMB-GOV-RECONCILIATION-V3）：每个存在的数据集的计数对象中，`window_start` 落在
//! 查询范围内的每个分钟窗口一行，列出三方计数与相邻环节的差值，以及按记录时间戳的两个数与该窗口在判定时刻的判定
//! （判定函数见 [`super::verdict`]，按宽限期计算）。
//!
//! 行只取自已持久化的计数对象，每次作答都重读；须读的计数对象（含判定要回看的、范围开始之前 24 小时内者）读不出时
//! 整条查询失败——不可访问 503 `unavailable`，版本过高或内容无法解析 500 `internal`；`sent`、`ts_sent`、`ts_durable`
//! 求和超出 Int64 也是 500 `internal`。

use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::ColumnType;
use arrow::array::{ArrayRef, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;

use super::verdict::{Judgment, judged_sources};
use super::{Context, CounterWindow, SystemTable, int64, judge_windows};
use crate::error::QueryError;

pub(super) const COLUMNS: &[(&str, ColumnType)] = &[
    ("dataset", ColumnType::Utf8),
    ("window_start", ColumnType::Int64),
    ("window_end", ColumnType::Int64),
    ("sent", ColumnType::Int64),
    ("sent_sources", ColumnType::Utf8),
    ("received", ColumnType::Int64),
    ("durable", ColumnType::Int64),
    ("gap_sent_received", ColumnType::Int64),
    ("gap_received_durable", ColumnType::Int64),
    ("ts_sent", ColumnType::Int64),
    ("ts_sent_sources", ColumnType::Utf8),
    ("ts_durable", ColumnType::Int64),
    ("verdict", ColumnType::Utf8),
];

struct Row {
    dataset: String,
    window_start: i64,
    window_end: i64,
    sent: Option<i64>,
    sent_sources: Option<String>,
    received: i64,
    durable: i64,
    ts_sent: Option<i64>,
    ts_sent_sources: Option<String>,
    ts_durable: Option<i64>,
    verdict: &'static str,
}

/// 计数之和作 Int64；超出 Int64 时为 `None`。
pub(super) fn checked_sum(values: impl Iterator<Item = u64>) -> Option<i64> {
    values
        .into_iter()
        .try_fold(0i64, |acc, n| int64(n).and_then(|n| acc.checked_add(n)))
}

fn row(dataset: &Dataset, w: &CounterWindow, j: &Judgment) -> Result<Row, QueryError> {
    let too_large = |what: &str| {
        QueryError::internal(format!(
            "{what} of window {} in the counter object of {dataset} exceeds the Int64 range",
            w.window_start
        ))
    };
    let (sent, sent_sources) = if w.sent.is_empty() {
        (None, None)
    } else {
        let sum = w
            .sent
            .values()
            .try_fold(0i64, |acc, &n| int64(n).and_then(|n| acc.checked_add(n)))
            .ok_or_else(|| too_large("sent"))?;
        let sources: Vec<&str> = w.sent.keys().map(String::as_str).collect();
        (Some(sum), Some(sources.join(",")))
    };
    // 受判来源的按时间戳的两个数；没有受判来源时三列都为 NULL
    let judged: Vec<(&str, u64)> = judged_sources(w).collect();
    let (ts_sent, ts_sent_sources, ts_durable) = if judged.is_empty() {
        (None, None, None)
    } else {
        let ts_sent =
            checked_sum(judged.iter().map(|(_, n)| *n)).ok_or_else(|| too_large("ts_sent"))?;
        let ts_durable = checked_sum(
            judged
                .iter()
                .map(|(s, _)| w.durable_ts_by_source.get(*s).copied().unwrap_or(0)),
        )
        .ok_or_else(|| too_large("ts_durable"))?;
        let sources: Vec<&str> = judged.iter().map(|(s, _)| *s).collect();
        (Some(ts_sent), Some(sources.join(",")), Some(ts_durable))
    };
    Ok(Row {
        dataset: dataset.to_string(),
        window_start: w.window_start,
        window_end: w.window_end,
        sent,
        sent_sources,
        received: int64(w.received).ok_or_else(|| too_large("received"))?,
        durable: int64(w.durable).ok_or_else(|| too_large("durable"))?,
        ts_sent,
        ts_sent_sources,
        ts_durable,
        verdict: j.verdict.as_str(),
    })
}

pub(super) async fn table(
    cx: &Context<'_>,
    datasets: &[Dataset],
) -> Result<RecordBatch, QueryError> {
    let mut rows = Vec::new();
    for d in datasets {
        let windows = cx
            .governance
            .counters
            .windows(d, cx.start_time, cx.end_time)
            .await?;
        let judgments = judge_windows(cx, d, cx.start_time, &windows, None).await?;
        for (w, j) in windows.iter().zip(&judgments) {
            rows.push(row(d, w, j)?);
        }
    }

    let mut dataset = StringBuilder::new();
    let mut window_start = Int64Builder::new();
    let mut window_end = Int64Builder::new();
    let mut sent = Int64Builder::new();
    let mut sent_sources = StringBuilder::new();
    let mut received = Int64Builder::new();
    let mut durable = Int64Builder::new();
    let mut gap_sent_received = Int64Builder::new();
    let mut gap_received_durable = Int64Builder::new();
    let mut ts_sent = Int64Builder::new();
    let mut ts_sent_sources = StringBuilder::new();
    let mut ts_durable = Int64Builder::new();
    let mut verdict = StringBuilder::new();
    for r in &rows {
        dataset.append_value(&r.dataset);
        window_start.append_value(r.window_start);
        window_end.append_value(r.window_end);
        sent.append_option(r.sent);
        sent_sources.append_option(r.sent_sources.as_deref());
        received.append_value(r.received);
        durable.append_value(r.durable);
        // 两侧都在 [0, i64::MAX] 之内，差不会溢出
        gap_sent_received.append_option(r.sent.map(|s| s - r.received));
        gap_received_durable.append_value(r.received - r.durable);
        ts_sent.append_option(r.ts_sent);
        ts_sent_sources.append_option(r.ts_sent_sources.as_deref());
        ts_durable.append_option(r.ts_durable);
        verdict.append_value(r.verdict);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(dataset.finish()),
        Arc::new(window_start.finish()),
        Arc::new(window_end.finish()),
        Arc::new(sent.finish()),
        Arc::new(sent_sources.finish()),
        Arc::new(received.finish()),
        Arc::new(durable.finish()),
        Arc::new(gap_sent_received.finish()),
        Arc::new(gap_received_durable.finish()),
        Arc::new(ts_sent.finish()),
        Arc::new(ts_sent_sources.finish()),
        Arc::new(ts_durable.finish()),
        Arc::new(verdict.finish()),
    ];
    RecordBatch::try_new(SystemTable::Reconciliation.arrow_schema(), columns)
        .map_err(|e| QueryError::internal(e.to_string()))
}
