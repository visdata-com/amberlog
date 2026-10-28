//! 调度（REQ-AMB-ALR-RULES B 组、C 组、D 组）：计划时刻、告警名额、评估的查询、条件判定、结局与停机时的放弃。
//!
//! 一个调度循环按计划时刻推进：每到一个时刻 T，对 T 到期的全部规则按名字的字节序先了结上一轮（SAME-INSTANT：等告警名额
//! 等到 T 的记 `alert_slots_full`，投递拖到 T 的等它按截止时刻结束），再判定新一轮：上一轮还占着告警名额或在查询中即记
//! `previous_running`，否则一并排进告警名额的队列（同一时刻请求的按名字先后取得，ALERT-SLOT-WAIT）。每次评估是一个任务：
//! 取得告警名额 → 开始执行查询 → 得出结果即还名额 → 判定结局 → 命中则投递 → 结束，交出评估记录、计入指标。
//!
//! 墙钟取 [`amberlog_storage::lifecycle::now_us`]（`fault-injection` 下带测试时钟偏移），计划时刻与评估记录的各时刻都按它。

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::error::Reason;
use amberlog_common::schema::ColumnType;
use amberlog_query::error::QueryError;
use amberlog_query::search::{Engine, EvalQuery, Hit};
use amberlog_query::system::background::Background;
use amberlog_storage::BoxFuture;
use amberlog_storage::fault;
use amberlog_storage::lifecycle::now_us;
use serde_json::Value;
use tokio::sync::{oneshot, watch};
use tokio::task::JoinSet;

use super::metrics::AlertCounters;
use super::outcome::{DeliveryResult, ErrorReason, EvaluationRecord, Outcome, SkipReason};
use super::rule::{Condition, Number, Rule, Source};
use super::{Delivery, DeliveryOutcome, Notice, Recorder, Stop};

/// 投递请求体 `rows` 的行数上限（REQ-AMB-ALR-WEBHOOK-ROWS）。
const NOTICE_ROWS: usize = 10;

/// 评估的查询的结果：输出列（名与类型，与行数无关）、至多 `query.max_size` 行与是否被截断。
pub type Answer = amberlog_query::search::Evaluated;

/// 评估的查询（REQ-AMB-ALR-RULES-QUERY-RESULT）：按 `_search` 的报告顺序，在准入处不等待地取共用的执行名额；
/// 名额已满以 `too_many_queries` 失败。
pub trait Query: Send + Sync {
    fn run<'a>(
        &'a self,
        org: &'a str,
        sql: &'a str,
        start_time: i64,
        end_time: i64,
    ) -> BoxFuture<'a, Result<Answer, QueryError>>;
}

/// 评估的查询经查询规划与执行直接求得（ADR-0051 第 4 条），不经 HTTP。
impl Query for Engine {
    fn run<'a>(
        &'a self,
        org: &'a str,
        sql: &'a str,
        start_time: i64,
        end_time: i64,
    ) -> BoxFuture<'a, Result<Answer, QueryError>> {
        Box::pin(async move {
            self.evaluate(
                org,
                &EvalQuery {
                    sql: sql.to_owned(),
                    start_time,
                    end_time,
                },
            )
            .await
        })
    }
}

/// 内置规则结果的一行（REQ-AMB-ALR-RULES-BUILTIN-RESULT-COLUMNS）：`ts_sent`、`ts_durable` 超出 Int64 时为 `None`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedRow {
    /// `<org>/<stream>`。
    pub dataset: String,
    pub window_start: i64,
    pub window_end: i64,
    pub ts_sent: Option<i64>,
    pub ts_durable: Option<i64>,
    pub missing_sources: String,
}

/// 内置规则取结果之处（治理视图的后台判定，REQ-AMB-GOV-MISSED-COLLECTION）：完成时刻在 `(after, until]` 内的后台判定中
/// 转为缺失、到此刻仍为缺失的窗口，按 BUILTIN-RESULT-ORDER 排好。只读内存，不读对象存储。
pub trait MissedWindows: Send + Sync {
    fn missed_collection(&self, after: i64, until: i64) -> Vec<MissedRow>;
}

impl MissedWindows for Background {
    fn missed_collection(&self, after: i64, until: i64) -> Vec<MissedRow> {
        Background::missed_collection(self, after, until)
            .into_iter()
            .map(|w| MissedRow {
                dataset: w.dataset.to_string(),
                window_start: w.window_start,
                window_end: w.window_end,
                ts_sent: w.ts_sent,
                ts_durable: w.ts_durable,
                missing_sources: w.missing_sources,
            })
            .collect()
    }
}

/// 调度用到的各方。
pub struct Deps {
    /// 启用的规则（含启用的内置规则）。
    pub rules: Vec<Arc<Rule>>,
    /// `alerts.max_concurrency`。
    pub max_concurrency: usize,
    pub query: Arc<dyn Query>,
    pub delivery: Arc<dyn Delivery>,
    pub recorder: Arc<dyn Recorder>,
    pub counters: Arc<AlertCounters>,
    /// 内置规则的结果来源；没有启用内置规则时不用。
    pub missed: Option<Arc<dyn MissedWindows>>,
    /// `query.max_size`：内置规则的结果多于它时被截断（REQ-AMB-ALR-RULES-TRUNCATED）。
    pub max_size: u64,
}

// ---------------------------------------------------------------------------------------------------------------
// 告警名额

/// 告警名额（TERM-ALERT-SLOT）：容量为 `alerts.max_concurrency`，等待者按（计划时刻，规则名的字节序）排队。
struct Slots {
    inner: Mutex<SlotsInner>,
}

struct SlotsInner {
    capacity: usize,
    in_use: usize,
    /// 等待者：键为（计划时刻，规则名），值为截止时刻与交付通道。
    queue: BTreeMap<(i64, String), (i64, oneshot::Sender<Grant>)>,
}

/// 告警名额队列对一个等待者的答复。
enum Grant {
    Slot(Permit),
    /// 至截止时刻仍未取得（ALERT-SLOT-WAIT）。
    Expired,
}

/// 一个已取得的告警名额，丢弃即释放并交给下一个等待者。
struct Permit(Option<Arc<Slots>>);

impl Drop for Permit {
    fn drop(&mut self) {
        if let Some(slots) = self.0.take() {
            let mut inner = slots.inner.lock().unwrap();
            inner.in_use -= 1;
            Slots::dispatch(&slots, &mut inner);
        }
    }
}

impl Slots {
    fn new(capacity: usize) -> Arc<Slots> {
        Arc::new(Slots {
            inner: Mutex::new(SlotsInner {
                capacity,
                in_use: 0,
                queue: BTreeMap::new(),
            }),
        })
    }

    /// 同一时刻的一批请求一并排队后再分配，使名额不论此刻是否已满都按名字先后取得。
    fn request(self: &Arc<Self>, batch: Vec<(i64, String, i64)>) -> Vec<oneshot::Receiver<Grant>> {
        let mut inner = self.inner.lock().unwrap();
        let receivers = batch
            .into_iter()
            .map(|(t, name, deadline)| {
                let (tx, rx) = oneshot::channel();
                inner.queue.insert((t, name), (deadline, tx));
                rx
            })
            .collect();
        Slots::dispatch(self, &mut inner);
        receivers
    }

    /// 把空出的名额按队列先后交出；已到截止时刻的等待者不再交给它名额。
    fn dispatch(this: &Arc<Slots>, inner: &mut SlotsInner) {
        while inner.in_use < inner.capacity {
            let Some((_, (deadline, tx))) = inner.queue.pop_first() else {
                return;
            };
            if now_us() >= deadline {
                let _ = tx.send(Grant::Expired);
                continue;
            }
            inner.in_use += 1;
            // 等待者已不在（停机时放弃了等待）：收回名额；退回的 Permit 先摘掉句柄，丢弃时不再重入本锁
            if let Err(Grant::Slot(mut permit)) =
                tx.send(Grant::Slot(Permit(Some(Arc::clone(this)))))
            {
                permit.0 = None;
                inner.in_use -= 1;
            }
        }
    }

    /// 规则 `name` 在计划时刻 `t` 的评估若还在排队，以 `alert_slots_full` 结束它的等待；返回它是否还在排队。
    fn expire(&self, t: i64, name: &str) -> bool {
        let mut inner = self.inner.lock().unwrap();
        match inner.queue.remove(&(t, name.to_owned())) {
            Some((_, tx)) => {
                let _ = tx.send(Grant::Expired);
                true
            }
            None => false,
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 一次评估

/// 一次评估此刻所处的阶段：调度循环在下一个计划时刻据此了结上一轮（SAME-INSTANT、PREVIOUS-RUNNING）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// 等告警名额。
    Waiting,
    /// 占着告警名额：开始执行查询之前，或查询中。
    Running,
    /// 已得出查询结果（已还告警名额）：判定、投递（截止时刻不晚于下一个计划时刻）与交出评估记录中。
    Finishing,
    /// 已结束（TERM-ENDED），评估记录已交出。
    Ended,
}

/// 评估任务共用的各方。
struct Shared {
    query: Arc<dyn Query>,
    delivery: Arc<dyn Delivery>,
    recorder: Arc<dyn Recorder>,
    counters: Arc<AlertCounters>,
    missed: Option<Arc<dyn MissedWindows>>,
    max_size: u64,
}

/// 一次评估在得出结局时已知的各项（评估记录除 `finished_at` 与投递三项之外的字段）。
struct Draft {
    started_at: Option<i64>,
    outcome: Outcome,
    matched_rows: Option<u64>,
    truncated: Option<bool>,
}

impl Draft {
    fn ended(outcome: Outcome, started_at: Option<i64>) -> Draft {
        Draft {
            started_at,
            outcome,
            matched_rows: None,
            truncated: None,
        }
    }
}

impl Shared {
    /// 评估结束：交出评估记录、计入指标。`delivery` 为 `None` 即没有投递（`not_attempted`）。
    fn finish(&self, rule: &Rule, t: i64, draft: Draft, delivery: Option<DeliveryOutcome>) {
        let delivery = delivery.unwrap_or(DeliveryOutcome {
            result: DeliveryResult::NotAttempted,
            attempts: 0,
            last_status: None,
        });
        self.recorder.record(EvaluationRecord {
            org: rule.org.clone(),
            rule: rule.name.clone(),
            scheduled_at: t,
            started_at: draft.started_at,
            finished_at: now_us(),
            window_start: t - rule.window_us(),
            window_end: t,
            outcome: draft.outcome,
            matched_rows: draft.matched_rows,
            truncated: draft.truncated,
            delivery: delivery.result,
            attempts: delivery.attempts,
            last_http_status: delivery.last_status,
        });
        self.counters.evaluation(&rule.name, draft.outcome);
        self.counters.delivery(&rule.name, delivery.result);
    }
}

/// 规则 `rule` 在计划时刻 `t` 的评估，从排进告警名额的队列起到结束。
async fn evaluate(
    cx: Arc<Shared>,
    rule: Arc<Rule>,
    t: i64,
    grant: oneshot::Receiver<Grant>,
    phase: watch::Sender<Phase>,
    last: Arc<AtomicI64>,
    mut stop: Stop,
) {
    let draft = run(&cx, &rule, t, grant, &phase, &last, &mut stop).await;
    let (draft, notice) = match draft {
        Ok(done) => done,
        Err(draft) => (draft, None),
    };
    let delivery = match notice {
        None => None,
        Some(notice) => Some(deliver(&cx, notice, &mut stop).await),
    };
    cx.finish(&rule, t, draft, delivery);
    phase.send_replace(Phase::Ended);
}

/// 取告警名额、执行查询并判定结局；命中时一并给出交给投递的内容。`Err` 为未得出可判定结果即结束的评估。
/// `last` 为本规则此前最近一次求得结果之时（本进程中此前没有时为开始受理 `/api/` 请求之时），内置规则据此取结果。
async fn run(
    cx: &Shared,
    rule: &Arc<Rule>,
    t: i64,
    grant: oneshot::Receiver<Grant>,
    phase: &watch::Sender<Phase>,
    last: &AtomicI64,
    stop: &mut Stop,
) -> Result<(Draft, Option<Notice>), Draft> {
    // 停机优先：进入停机过程之后不再取得告警名额（SHUTDOWN-NO-NEW）
    let permit = tokio::select! {
        biased;
        () = stop.stopped() => return Err(Draft::ended(Outcome::Skipped(SkipReason::ShuttingDown), None)),
        g = grant => match g {
            Ok(Grant::Slot(p)) => p,
            Ok(Grant::Expired) | Err(_) => {
                return Err(Draft::ended(Outcome::Skipped(SkipReason::AlertSlotsFull), None));
            }
        },
    };
    phase.send_replace(Phase::Running);
    // 测试专用暂停点：取得告警名额之后、开始执行查询之前
    tokio::select! {
        biased;
        () = stop.stopped() => {}
        () = fault::gate("alert.after_slots") => {}
    }
    if stop.is_stopped() {
        return Err(Draft::ended(
            Outcome::Skipped(SkipReason::ShuttingDown),
            None,
        ));
    }
    let started = now_us();
    let started_at = Some(started);
    let answer = match &rule.source {
        Source::Sql(sql) => {
            let query = cx.query.run(&rule.org, sql, t - rule.window_us(), t);
            tokio::select! {
                biased;
                // 中止查询（丢弃其 future，执行名额随之释放）
                () = stop.stopped() => {
                    return Err(Draft::ended(Outcome::Error(ErrorReason::ShuttingDown), started_at));
                }
                answer = query => answer,
            }
        }
        // 内置规则不取查询执行名额、不读对象存储，取得告警名额后即开始求结果（BUILTIN-NO-QUERY-SLOT、BUILTIN-READING）
        Source::MissedCollection => {
            let rows = cx.missed.as_ref().map_or_else(Vec::new, |m| {
                m.missed_collection(last.load(Ordering::SeqCst), started)
            });
            last.store(started, Ordering::SeqCst);
            Ok(builtin_answer(rows, cx.max_size))
        }
    };
    // 得出结果即还告警名额（TERM-ALERT-SLOT）；此后到下一个计划时刻也不使下一轮记 `previous_running`
    drop(permit);
    phase.send_replace(Phase::Finishing);
    let answer = match answer {
        Ok(a) => a,
        Err(e) if e.reason == Reason::TooManyQueries => {
            return Err(Draft::ended(
                Outcome::Skipped(SkipReason::QuerySlotsFull),
                started_at,
            ));
        }
        Err(e) => {
            return Err(Draft::ended(
                Outcome::Error(ErrorReason::Query(e.reason)),
                started_at,
            ));
        }
    };
    let judged = match judge(&rule.condition, &answer) {
        Ok(j) => j,
        Err(reason) => return Err(Draft::ended(Outcome::Error(reason), started_at)),
    };
    let draft = Draft {
        started_at,
        outcome: if judged.matched {
            Outcome::Matched
        } else {
            Outcome::NotMatched
        },
        matched_rows: Some(judged.matched_rows),
        truncated: Some(answer.truncated),
    };
    let notice = judged.matched.then(|| Notice {
        rule: Arc::clone(rule),
        scheduled_at: t,
        matched_rows: judged.matched_rows,
        truncated: answer.truncated,
        rows: judged.rows,
    });
    Ok((draft, notice))
}

/// 交给投递并等它有投递结果；停机时放弃（SHUTDOWN-ABANDON）。
async fn deliver(cx: &Shared, notice: Notice, stop: &mut Stop) -> DeliveryOutcome {
    let abandoned = DeliveryOutcome {
        result: DeliveryResult::Abandoned,
        attempts: 0,
        last_status: None,
    };
    // 测试专用暂停点：得出命中结果之后、发出第一次尝试之前
    tokio::select! {
        biased;
        () = stop.stopped() => return abandoned,
        () = fault::gate("alert.before_delivery") => {}
    }
    if stop.is_stopped() {
        return abandoned;
    }
    cx.delivery.deliver(notice, stop.clone()).await
}

/// 内置规则的结果作为查询结果：六列，多于 `max_size` 行时按次序取前 `max_size` 行并标截断。
fn builtin_answer(rows: Vec<MissedRow>, max_size: u64) -> Answer {
    let truncated = rows.len() as u64 > max_size;
    let rows = rows
        .into_iter()
        .take(usize::try_from(max_size).unwrap_or(usize::MAX))
        .map(|r| {
            let mut hit = vec![
                ("dataset".to_owned(), Value::from(r.dataset)),
                ("window_start".to_owned(), Value::from(r.window_start)),
                ("window_end".to_owned(), Value::from(r.window_end)),
            ];
            // 取值为 NULL 的列不写键（同 `_search` 的 hit）
            if let Some(n) = r.ts_sent {
                hit.push(("ts_sent".to_owned(), Value::from(n)));
            }
            if let Some(n) = r.ts_durable {
                hit.push(("ts_durable".to_owned(), Value::from(n)));
            }
            hit.push(("missing_sources".to_owned(), Value::from(r.missing_sources)));
            Hit(hit)
        })
        .collect();
    Answer {
        columns: [
            ("dataset", ColumnType::Utf8),
            ("window_start", ColumnType::Int64),
            ("window_end", ColumnType::Int64),
            ("ts_sent", ColumnType::Int64),
            ("ts_durable", ColumnType::Int64),
            ("missing_sources", ColumnType::Utf8),
        ]
        .into_iter()
        .map(|(c, t)| (c.to_owned(), t))
        .collect(),
        rows,
        truncated,
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 条件判定

/// 条件判定的结果：是否命中、`matched_rows` 与投递请求体的 `rows`。
#[derive(Debug, PartialEq)]
struct Judged {
    matched: bool,
    matched_rows: u64,
    rows: Vec<Hit>,
}

/// 按条件判定查询结果（MATCH-ROWS、MATCH-COLUMN、COLUMN-MISSING、COLUMN-NOT-NUMERIC）。`answer.rows` 至多
/// `query.max_size` 行，被截断时即前 `query.max_size` 行（TRUNCATED）。
fn judge(condition: &Condition, answer: &Answer) -> Result<Judged, ErrorReason> {
    match condition {
        Condition::Rows { op, n } => {
            let count = answer.rows.len() as u64;
            Ok(Judged {
                matched: op.holds(count.cmp(n)),
                matched_rows: count,
                rows: answer.rows.iter().take(NOTICE_ROWS).cloned().collect(),
            })
        }
        Condition::Column {
            column,
            op,
            threshold,
        } => {
            let Some((_, column_type)) = answer.columns.iter().find(|(c, _)| c == column) else {
                return Err(ErrorReason::ConditionColumnMissing);
            };
            let as_number: fn(&Value) -> Option<Number> = match column_type {
                ColumnType::Int64 => |v| v.as_i64().map(Number::Int64),
                ColumnType::Float64 => |v| v.as_f64().map(Number::Float64),
                ColumnType::Boolean | ColumnType::Utf8 => {
                    return Err(ErrorReason::ConditionColumnNotNumeric);
                }
            };
            // 取值为 NULL 的行在 hit 中没有该键，不算满足
            let satisfying: Vec<&Hit> = answer
                .rows
                .iter()
                .filter(|h| {
                    h.get(column)
                        .and_then(as_number)
                        .is_some_and(|v| threshold.satisfied_by(v, *op))
                })
                .collect();
            Ok(Judged {
                matched: !satisfying.is_empty(),
                matched_rows: satisfying.len() as u64,
                rows: satisfying.into_iter().take(NOTICE_ROWS).cloned().collect(),
            })
        }
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 调度循环

/// 一条启用的规则在调度循环里的状态。
struct Ticker {
    rule: Arc<Rule>,
    /// 下一个计划时刻。
    next: i64,
    /// 最近一次开始的、可能尚未结束的评估：计划时刻与阶段。
    current: Option<(i64, watch::Receiver<Phase>)>,
    /// 本规则此前最近一次求得结果之时（内置规则取结果的起点）。
    last_result: Arc<AtomicI64>,
}

/// 运行中的调度；[`Scheduler::stopped`] 等它在停机时收尾。
pub struct Scheduler {
    task: tokio::task::JoinHandle<()>,
}

/// 停机时等在途评估交出评估记录的上限：它们见到停机通知即结束，这只是兜底，不为告警延长停机（SHUTDOWN-ABANDON）。
const STOP_GRACE: Duration = Duration::from_secs(2);

impl Scheduler {
    /// 在开始受理 `/api/` 请求之时 `opened_at`（UTC 微秒）起调度 `deps.rules`，直到 `stop` 置位（FIRST-TICK）。
    pub fn start(deps: Deps, opened_at: i64, stop: Stop) -> Scheduler {
        Scheduler {
            task: tokio::spawn(schedule(deps, opened_at, stop)),
        }
    }

    /// 停机通知置位之后：等调度循环退出、在途评估交出评估记录（至多 [`STOP_GRACE`]）。
    pub async fn stopped(self) {
        let _ = tokio::time::timeout(STOP_GRACE, self.task).await;
    }
}

/// 不早于 `at` 的第一个 `interval` 整数倍（TERM-SCHEDULE、FIRST-TICK）。
fn first_tick(at: i64, interval: i64) -> i64 {
    at.div_euclid(interval)
        .saturating_add(i64::from(at.rem_euclid(interval) != 0))
        * interval
}

async fn schedule(deps: Deps, opened_at: i64, mut stop: Stop) {
    let cx = Arc::new(Shared {
        query: deps.query,
        delivery: deps.delivery,
        recorder: deps.recorder,
        counters: deps.counters,
        missed: deps.missed,
        max_size: deps.max_size,
    });
    let slots = Slots::new(deps.max_concurrency);
    let mut tickers: Vec<Ticker> = deps
        .rules
        .into_iter()
        .map(|rule| Ticker {
            next: first_tick(opened_at, rule.interval_us()),
            rule,
            current: None,
            last_result: Arc::new(AtomicI64::new(opened_at)),
        })
        .collect();
    // 同一时刻到期的规则按名字的字节序处理（ALERT-SLOT-WAIT）
    tickers.sort_by(|a, b| a.rule.name.cmp(&b.rule.name));
    let mut evaluations = JoinSet::new();

    'ticks: loop {
        let Some(t) = tickers.iter().map(|k| k.next).min() else {
            stop.stopped().await;
            break;
        };
        // 睡到 T：分段睡，墙钟（含测试时钟偏移）跳变时也按墙钟判定
        loop {
            let now = now_us();
            if now >= t {
                break;
            }
            let nap = Duration::from_micros(u64::try_from(t - now).unwrap_or(0))
                .min(Duration::from_secs(1));
            tokio::select! {
                biased;
                () = stop.stopped() => break 'ticks,
                () = tokio::time::sleep(nap) => {}
            }
        }
        // 循环落后时 T 可能在停机之后才轮到：此后到期的计划时刻不评估、不产生评估记录（SHUTDOWN-NO-NEW）
        if stop.is_stopped() {
            break;
        }
        while evaluations.try_join_next().is_some() {}

        let mut batch = Vec::new();
        let mut started = Vec::new();
        for k in tickers.iter_mut().filter(|k| k.next == t) {
            let interval = k.rule.interval_us();
            k.next = t.saturating_add(interval);
            if previous_running(&slots, k).await {
                // PREVIOUS-RUNNING：不排队、不执行查询，即刻结束
                cx.finish(
                    &k.rule,
                    t,
                    Draft::ended(Outcome::Skipped(SkipReason::PreviousRunning), None),
                    None,
                );
                continue;
            }
            let (tx, rx) = watch::channel(Phase::Waiting);
            k.current = Some((t, rx));
            batch.push((t, k.rule.name.clone(), t.saturating_add(interval)));
            started.push((Arc::clone(&k.rule), tx, Arc::clone(&k.last_result)));
        }
        for (grant, (rule, phase, last)) in slots.request(batch).into_iter().zip(started) {
            evaluations.spawn(evaluate(
                Arc::clone(&cx),
                rule,
                t,
                grant,
                phase,
                last,
                stop.clone(),
            ));
        }
    }
    // 进入停机过程：在途评估见到停机通知各自结束并交出评估记录
    while evaluations.join_next().await.is_some() {}
}

/// 了结上一轮时等它离开某一阶段的上限：等告警名额的在 T 被判过期、投递在截止时刻 T 中止，都随即结束；
/// 这只是兜底，免得一条规则挡住同一时刻的其他规则（TICK-LATENCY）。
const SETTLE: Duration = Duration::from_secs(1);

/// 到规则 `k` 的计划时刻 T 时，先了结它的上一轮（SAME-INSTANT），再看上一轮是否仍未结束（PREVIOUS-RUNNING）。
/// 上一轮在等告警名额：截止时刻即 T，按 ALERT-SLOT-WAIT 结束；已得出查询结果：投递的截止时刻不晚于 T，等它结束。
/// 只有占着告警名额（查询之前或查询中）的上一轮使这一轮记 `previous_running`。
async fn previous_running(slots: &Slots, k: &mut Ticker) -> bool {
    let Some((prev_t, phase)) = &mut k.current else {
        return false;
    };
    if *phase.borrow() == Phase::Waiting {
        // 还在队列里即以 `alert_slots_full` 结束；不在了的，是刚取得名额或刚被判过期，等它离开等待阶段
        slots.expire(*prev_t, &k.rule.name);
        let _ = tokio::time::timeout(SETTLE, phase.wait_for(|p| *p != Phase::Waiting)).await;
    }
    if *phase.borrow() == Phase::Finishing {
        let _ = tokio::time::timeout(SETTLE, phase.wait_for(|p| *p == Phase::Ended)).await;
    }
    *phase.borrow() == Phase::Running
}
