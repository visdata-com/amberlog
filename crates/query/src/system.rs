//! 治理视图（Governance）：经 `_search` 查询的两张系统表 `_ledger` 与 `_reconciliation`
//! （REQ-AMB-QRY-SEARCH-ENDPOINT-V4「系统表」、REQ-AMB-GOV-LEDGER-V3、REQ-AMB-GOV-RECONCILIATION-V3、ADR-0028），
//! 告警评估记录的系统表 `_alert_evaluations`（REQ-AMB-QRY-SEARCH-ENDPOINT-V8、REQ-AMB-ALR-EVALUATIONS，见
//! [`alert_evaluations`]），以及两表与后台判定共用的漏采判定（[`verdict`]、[`background`]；
//! REQ-AMB-GOV-MISSED-COLLECTION、ADR-0053 第 13 条）。
//!
//! 系统表现算、不落副本：每次作答从各数据集的 `_current`、当前快照（[`ChainReader`]）与已持久化的计数对象
//! （[`CounterObjects`]）生成整张表，再交给与 stream 查询相同的执行步骤（WHERE、排序、`LIMIT`、`from`/`size`）。
//! 不读桶 manifest 与数据文件，成功响应的三项扫描统计恒为 0。
//!
//! 报告顺序（系统表另有一条）：请求体与 SQL 的各项错误在 [`crate::search::Engine::search`] 里先报完；此后没有
//! `stream_not_found` 与数据集停用状态两位，直接按表的列集合绑定（`unknown_column`、`invalid_sql`），最后才读对象存储。
//!
//! 本模块只依赖窄接口：计数对象的读取由 amberlog-ingest 的治理计数提供、桶状态的判定由 amberlog-storage 的桶生命周期
//! 提供，经接入面装配时接上（query 不依赖 ingest）。

mod alert_evaluations;
pub mod backfill;
pub mod background;
mod ledger;
mod reconciliation;
pub mod verdict;

pub use alert_evaluations::{AlertHistory, EvaluationRow, HistoryError};

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_common::schema::ColumnType;
use amberlog_storage::BoxFuture;
use amberlog_storage::chain::{BucketRef, ChainReader};
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::schema::{SchemaColumn, StreamColumns};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use crate::error::QueryError;
use crate::sql::{CmpOp, Cond, Literal, Operand, Query};
use verdict::{Backfill, Judgment, judge, reported_before};

/// 系统表。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemTable {
    Ledger,
    Reconciliation,
    AlertEvaluations,
}

impl SystemTable {
    /// `FROM` 引用的名称（未加引号的已按小写解析）是否为系统表名。
    pub fn from_name(name: &str) -> Option<SystemTable> {
        match name {
            "_ledger" => Some(SystemTable::Ledger),
            "_reconciliation" => Some(SystemTable::Reconciliation),
            "_alert_evaluations" => Some(SystemTable::AlertEvaluations),
            _ => None,
        }
    }

    /// 表的列与类型，按 `SELECT *` 的顺序。
    fn columns(self) -> &'static [(&'static str, ColumnType)] {
        match self {
            SystemTable::Ledger => ledger::COLUMNS,
            SystemTable::Reconciliation => reconciliation::COLUMNS,
            SystemTable::AlertEvaluations => alert_evaluations::COLUMNS,
        }
    }

    /// 绑定用的列集合（`unknown_column` 与类型检查按它判定）。
    pub(crate) fn schema(self) -> StreamColumns {
        StreamColumns::from_columns(
            self.columns()
                .iter()
                .enumerate()
                .map(|(i, (name, t))| SchemaColumn {
                    name: (*name).to_owned(),
                    column_type: *t,
                    field_id: i as u32 + 1,
                })
                .collect(),
        )
    }

    /// 交给执行的表 schema（各列都可为 NULL）。
    pub(crate) fn arrow_schema(self) -> SchemaRef {
        Arc::new(Schema::new(
            self.columns()
                .iter()
                .map(|(name, t)| {
                    let t = match t {
                        ColumnType::Int64 => DataType::Int64,
                        ColumnType::Utf8 => DataType::Utf8,
                        ColumnType::Float64 => DataType::Float64,
                        ColumnType::Boolean => DataType::Boolean,
                    };
                    Field::new(*name, t, true)
                })
                .collect::<Vec<_>>(),
        ))
    }

    /// 缺省排序（列，是否降序）：不含 `ORDER BY` 时按它，含 `ORDER BY` 时接在其后，排序键相同的行按它排列。
    pub(crate) fn default_order(self) -> &'static [(&'static str, bool)] {
        match self {
            SystemTable::Ledger => &[("dataset", false)],
            SystemTable::Reconciliation => &[("window_start", true), ("dataset", false)],
            SystemTable::AlertEvaluations => &[("scheduled_at", true), ("rule", false)],
        }
    }
}

/// 计数对象中的一个分钟窗口（REQ-AMB-OBS-INGEST-COUNTERS 计数对象的 `windows` 元素）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CounterWindow {
    pub window_start: i64,
    pub window_end: i64,
    pub received: u64,
    pub durable: u64,
    /// 各来源按送达的发送数（来源名按 UTF-8 字节序即 Unicode 码点序）。
    pub sent: BTreeMap<String, u64>,
    /// 按记录时间戳落在该窗口的确认数（REQ-AMB-OBS-INGEST-COUNTERS-V3）。
    pub durable_ts: u64,
    /// `durable_ts` 中带来源标识的部分，按来源。
    pub durable_ts_by_source: BTreeMap<String, u64>,
    /// 各来源按时间戳的发送数。
    pub sent_ts: BTreeMap<String, u64>,
}

/// 读取计数对象的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CounterError {
    /// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）。
    Unavailable(String),
    /// `format_version` 大于 1，或内容无法解析（按 REQ-AMB-OBS-INGEST-COUNTERS 的界定）。
    Unparseable(String),
}

/// 一个小时的计数：小时起点，与该小时的窗口或读不出的原因（[`CounterObjects::current_hours`]）。
pub type HourCounts = (i64, Result<Vec<CounterWindow>, CounterError>);

/// 已持久化的计数对象的读取（不含尚未写出的进程内计数）。
pub trait CounterObjects: Send + Sync {
    /// 数据集 `dataset` 的计数对象中 `window_start` 落在 `[start, end)` 内的全部窗口，按 `window_start` 升序。
    /// 每次调用都重读对象存储，不沿用先前读到的内容；某小时的计数对象不存在时该小时没有窗口。
    fn windows<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Result<Vec<CounterWindow>, CounterError>>;

    /// 后台判定所用的计数（REQ-AMB-GOV-MISSED-COLLECTION-BG-COUNTS）：`[start, end)` 所跨的每个 UTC 小时各一项
    /// （小时起点升序），为该小时计数对象中已写出的计数加上本进程已计入、尚未写出的计数；`window_start` 落在
    /// `[start, end)` 之外的窗口不含。每个小时都重读对象存储：对象读不出（不可访问、版本过高或内容无法解析）的小时
    /// 单独给出错误，不影响其他小时。
    fn current_hours<'a>(
        &'a self,
        dataset: &'a Dataset,
        start: i64,
        end: i64,
    ) -> BoxFuture<'a, Vec<HourCounts>>;
}

/// 桶状态的判定（REQ-AMB-STO-BUCKET-LIFECYCLE）。
pub trait BucketStates: Send + Sync {
    /// 快照 `buckets` 中的桶 `bucket` 在时刻 `now_us`（UTC 微秒）是否处于 `pending_lake` 状态。
    fn pending_lake(&self, bucket: &BucketRef, now_us: i64) -> bool;
}

/// 配置项 `retention.period`（REQ-AMB-CFG-STARTUP-V4）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Retention {
    /// 配置值的原文（如 `1095d`）。
    pub text: String,
    /// 换算所得的微秒数。
    pub micros: i64,
}

/// 系统表用到的存储侧句柄与配置。
#[derive(Clone)]
pub struct Governance {
    /// 存在的数据集（REQ-AMB-DM-NAMESPACE 三者之并）。
    pub registry: Arc<DatasetRegistry>,
    pub counters: Arc<dyn CounterObjects>,
    pub buckets: Arc<dyn BucketStates>,
    /// 已写出的评估记录（`_alert_evaluations` 的行集）。
    pub alerts: Arc<dyn AlertHistory>,
    /// 未配置为 `None`。
    pub retention: Option<Retention>,
    /// 本进程内存中的补发中状态（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL）。
    pub backfill: Arc<dyn Backfill>,
    /// 宽限期 `reconciliation.grace`，微秒。
    pub grace_us: i64,
}

/// 生成一张系统表所需的上下文。
pub(crate) struct Context<'a> {
    pub governance: &'a Governance,
    pub chain: &'a dyn ChainReader,
    pub gates: &'a DatasetGates,
    pub org: &'a str,
    pub start_time: i64,
    pub end_time: i64,
    /// 系统收到该查询请求的时刻（UTC 微秒）。
    pub now_us: i64,
}

impl Context<'_> {
    /// 本次作答涉及的数据集：`{org}` 下存在的数据集，有点名数据集时只取被点名的，按标识的码点升序。
    fn datasets(&self, query: &Query) -> Vec<Dataset> {
        let named = named_datasets(query);
        self.governance
            .registry
            .datasets()
            .into_iter()
            .filter(|d| d.org == self.org)
            .filter(|d| named.as_ref().is_none_or(|n| n.contains(&d.to_string())))
            .collect()
    }
}

/// 生成整张表（未过滤、未排序）。
pub(crate) async fn table(
    table: SystemTable,
    cx: &Context<'_>,
    query: &Query,
) -> Result<RecordBatch, QueryError> {
    match table {
        SystemTable::Ledger => ledger::table(cx, &cx.datasets(query)).await,
        SystemTable::Reconciliation => reconciliation::table(cx, &cx.datasets(query)).await,
        SystemTable::AlertEvaluations => {
            alert_evaluations::table(cx, named(query, "rule").as_ref()).await
        }
    }
}

/// 点名数据集（REQ-AMB-GOV-RECONCILIATION「读取范围」）：WHERE 去掉圆括号分组后以顶层 `AND` 连接的各项中，
/// `dataset = '<字符串字面量>'` 或 `'<字符串字面量>' = dataset` 所给出的标识。没有这样的项为 `None`。
pub(crate) fn named_datasets(query: &Query) -> Option<BTreeSet<String>> {
    named(query, "dataset")
}

/// 查询点名的 `column` 取值（REQ-AMB-GOV-RECONCILIATION 点名数据集、REQ-AMB-ALR-EVALUATIONS-NAMED-RULE 点名规则，
/// 两者写法相同）：`WHERE` 去掉圆括号分组后以顶层 `AND` 连接的各项中，形如 `<column> = '<字符串>'` 或
/// `'<字符串>' = <column>` 的项（列名可加双引号）所给出的取值；没有这样的项为 `None`。
/// 语法树里圆括号已不留痕迹，嵌套的 `AND` 即被括起的 `AND`，一并展开。
pub(crate) fn named(query: &Query, column: &str) -> Option<BTreeSet<String>> {
    fn walk(cond: &Cond, column: &str, out: &mut BTreeSet<String>) {
        match cond {
            Cond::And(items) => items.iter().for_each(|c| walk(c, column, out)),
            Cond::Cmp {
                op: CmpOp::Eq,
                left,
                right,
            } => match (left, right) {
                (Operand::Column(c), Operand::Literal(Literal::String(s)))
                | (Operand::Literal(Literal::String(s)), Operand::Column(c))
                    if c == column =>
                {
                    out.insert(s.clone());
                }
                _ => {}
            },
            _ => {}
        }
    }
    let mut out = BTreeSet::new();
    walk(query.filter.as_ref()?, column, &mut out);
    (!out.is_empty()).then_some(out)
}

impl From<CounterError> for QueryError {
    fn from(e: CounterError) -> Self {
        match e {
            CounterError::Unavailable(m) => QueryError::new(Reason::Unavailable, m),
            CounterError::Unparseable(m) => QueryError::internal(m),
        }
    }
}

/// 回看尚未读入的标记（[`judge_windows`] 内部用）。
struct NeedLookback;

/// 对 `windows`（`dataset` 在查询范围 `[start, …)` 内的窗口，按 `window_start` 升序）逐个算出判定时刻 `cx.now_us`
/// 的判定（REQ-AMB-GOV-MISSED-COLLECTION-CURRENT-COUNTS：只用该次查询读到的计数对象）。
///
/// `before` 是已读入的范围之前的窗口（`window_start` 早于 `start`，升序）；为 `None` 时，第一次有窗口要判断上报中断
/// 才读 `[W − 24 小时, start)`——窗口按升序逐个判定，第一个要回看的 W 最早，此后的回看都在它之内
/// （TERM-NEEDED：只读判定所需的计数对象）。读不出按 [`CounterError`] 交回。
async fn judge_windows(
    cx: &Context<'_>,
    dataset: &Dataset,
    start: i64,
    windows: &[CounterWindow],
    before: Option<Vec<CounterWindow>>,
) -> Result<Vec<Judgment>, CounterError> {
    let gov = cx.governance;
    let mut before = before;
    let mut combined: Option<Vec<CounterWindow>> = None;
    let mut out = Vec::with_capacity(windows.len());
    for w in windows {
        let first = judge(
            dataset,
            w,
            cx.now_us,
            gov.grace_us,
            gov.backfill.as_ref(),
            || Err(NeedLookback),
        );
        let j = match first {
            Ok(j) => j,
            Err(NeedLookback) => {
                if combined.is_none() {
                    let earlier = match before.take() {
                        Some(b) => b,
                        None if w.window_start - verdict::LOOKBACK_US < start => {
                            gov.counters
                                .windows(dataset, w.window_start - verdict::LOOKBACK_US, start)
                                .await?
                        }
                        None => Vec::new(),
                    };
                    let mut all = earlier;
                    all.extend(windows.iter().cloned());
                    combined = Some(all);
                }
                let all = combined.as_deref().unwrap_or_default();
                judge(
                    dataset,
                    w,
                    cx.now_us,
                    gov.grace_us,
                    gov.backfill.as_ref(),
                    || Ok::<_, CounterError>(reported_before(all, w.window_start)),
                )?
            }
        };
        out.push(j);
    }
    Ok(out)
}

/// 计数（u64）作 Int64 列的取值；超出 Int64 时为 `None`。
fn int64(n: u64) -> Option<i64> {
    i64::try_from(n).ok()
}

#[cfg(test)]
mod tests;
