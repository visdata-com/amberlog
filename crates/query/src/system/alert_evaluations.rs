//! 系统表 `_alert_evaluations` 的行集来源（REQ-AMB-ALR-EVALUATIONS「系统表」，ADR-0051 第 8、13 条）。
//!
//! 评估记录对象的格式只由告警模块解析：查询端经窄接口 [`AlertHistory`] 取已写出的评估记录，不解析对象
//! （query 不依赖 server）。实现在 `amberlog serve` 的装配处接上。
//!
//! 本模块按列集合绑定之后（在 [`crate::search`]），以点名规则（REQ-AMB-ALR-EVALUATIONS-NAMED-RULE，与点名数据集
//! 同一段代码）与查询范围向 [`AlertHistory`] 取行，生成整张表交给与其他系统表相同的执行步骤；缺省次序为 `scheduled_at`
//! 降序、`rule` 码点升序（-ORDER、-ORDER-TIES，[`super::SystemTable::default_order`]）。不缓存读到的内容。

use std::collections::BTreeSet;
use std::sync::Arc;

use amberlog_common::error::Reason;
use amberlog_common::schema::ColumnType;
use amberlog_storage::BoxFuture;
use arrow::array::{ArrayRef, BooleanBuilder, Int64Builder, StringBuilder};
use arrow::record_batch::RecordBatch;

use super::{Context, SystemTable};
use crate::error::QueryError;

/// REQ-AMB-ALR-EVALUATIONS-COLUMNS 的十三列。
pub(super) const COLUMNS: &[(&str, ColumnType)] = &[
    ("rule", ColumnType::Utf8),
    ("scheduled_at", ColumnType::Int64),
    ("started_at", ColumnType::Int64),
    ("finished_at", ColumnType::Int64),
    ("window_start", ColumnType::Int64),
    ("window_end", ColumnType::Int64),
    ("outcome", ColumnType::Utf8),
    ("reason", ColumnType::Utf8),
    ("matched_rows", ColumnType::Int64),
    ("truncated", ColumnType::Boolean),
    ("delivery", ColumnType::Utf8),
    ("attempts", ColumnType::Int64),
    ("last_http_status", ColumnType::Int64),
];

/// 生成整张表（未过滤、未排序）：`org` 为请求路径的 `{org}`，范围 `[start_time, end_time)` 作用在 `scheduled_at` 上
/// （REQ-AMB-ALR-EVALUATIONS-ROWS）。读取失败按 [`HistoryError`] 作答，不返回部分结果。
pub(super) async fn table(
    cx: &Context<'_>,
    rules: Option<&BTreeSet<String>>,
) -> Result<RecordBatch, QueryError> {
    let rows = cx
        .governance
        .alerts
        .evaluations(cx.org, rules, cx.start_time, cx.end_time)
        .await?;
    let mut rule = StringBuilder::new();
    let mut scheduled_at = Int64Builder::new();
    let mut started_at = Int64Builder::new();
    let mut finished_at = Int64Builder::new();
    let mut window_start = Int64Builder::new();
    let mut window_end = Int64Builder::new();
    let mut outcome = StringBuilder::new();
    let mut reason = StringBuilder::new();
    let mut matched_rows = Int64Builder::new();
    let mut truncated = BooleanBuilder::new();
    let mut delivery = StringBuilder::new();
    let mut attempts = Int64Builder::new();
    let mut last_http_status = Int64Builder::new();
    for r in &rows {
        rule.append_value(&r.rule);
        scheduled_at.append_value(r.scheduled_at);
        started_at.append_option(r.started_at);
        finished_at.append_value(r.finished_at);
        window_start.append_value(r.window_start);
        window_end.append_value(r.window_end);
        outcome.append_value(&r.outcome);
        reason.append_option(r.reason.as_deref());
        matched_rows.append_option(r.matched_rows);
        truncated.append_option(r.truncated);
        delivery.append_value(&r.delivery);
        attempts.append_value(r.attempts);
        last_http_status.append_option(r.last_http_status);
    }
    let columns: Vec<ArrayRef> = vec![
        Arc::new(rule.finish()),
        Arc::new(scheduled_at.finish()),
        Arc::new(started_at.finish()),
        Arc::new(finished_at.finish()),
        Arc::new(window_start.finish()),
        Arc::new(window_end.finish()),
        Arc::new(outcome.finish()),
        Arc::new(reason.finish()),
        Arc::new(matched_rows.finish()),
        Arc::new(truncated.finish()),
        Arc::new(delivery.finish()),
        Arc::new(attempts.finish()),
        Arc::new(last_http_status.finish()),
    ];
    RecordBatch::try_new(SystemTable::AlertEvaluations.arrow_schema(), columns)
        .map_err(|e| QueryError::internal(e.to_string()))
}

/// 一条已写出的评估记录，即 `_alert_evaluations` 的一行（REQ-AMB-ALR-EVALUATIONS-COLUMNS 的十三列；
/// `rule` 取对象的 `rule`，其余取元素的同名字段，JSON `null` 为 `None`）。时刻均为 UTC 微秒。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationRow {
    pub rule: String,
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: i64,
    pub window_start: i64,
    pub window_end: i64,
    pub outcome: String,
    pub reason: Option<String>,
    pub matched_rows: Option<i64>,
    pub truncated: Option<bool>,
    pub delivery: String,
    pub attempts: i64,
    pub last_http_status: Option<i64>,
}

/// 读取评估记录对象的失败（REQ-AMB-ALR-EVALUATIONS-READ-UNAVAILABLE、-READ-UNPARSEABLE）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistoryError {
    /// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）：503 `unavailable`。
    Unavailable(String),
    /// `format_version` 大于 1，或内容不是合法 JSON、不合评估记录对象的字段与类型：500 `internal`。
    Unparseable(String),
}

impl From<HistoryError> for QueryError {
    fn from(e: HistoryError) -> Self {
        match e {
            HistoryError::Unavailable(m) => QueryError::new(Reason::Unavailable, m),
            HistoryError::Unparseable(m) => QueryError::internal(m),
        }
    }
}

/// 已写出的评估记录的读取（不含尚未写出的进程内记录，REQ-AMB-ALR-EVALUATIONS-SOURCE）。
pub trait AlertHistory: Send + Sync {
    /// `org` 等于 `org` 的评估记录对象中 `scheduled_at` 落在 `[start, end)` 内的全部元素（不排序，排序由执行步骤做）。
    /// `rules` 为点名的规则（REQ-AMB-ALR-EVALUATIONS-NAMED-RULE）：有点名时只列举、读取这些规则的前缀，没有时为
    /// `None`；只读取小时与 `[start, end)` 有交集的对象。每次调用都重读对象存储，不沿用先前读到的内容。
    fn evaluations<'a>(
        &'a self,
        org: &'a str,
        rules: Option<&'a BTreeSet<String>>,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<EvaluationRow>, HistoryError>>;
}
