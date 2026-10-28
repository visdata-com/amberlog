//! 调度的单元测试：查询、投递与评估记录都用替身；规则的周期取几百毫秒（校验只在配置一步，调度本身不限周期）。

use std::sync::Mutex as StdMutex;

use serde_json::json;
use tokio::sync::Notify;

use std::sync::atomic::AtomicUsize;

use super::*;
use crate::alerting::rule::{CmpOp, Threshold};
use crate::alerting::webhook::Target;

fn hit(pairs: &[(&str, Value)]) -> Hit {
    Hit(pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect())
}

fn answer(columns: &[(&str, ColumnType)], rows: Vec<Hit>, truncated: bool) -> Answer {
    Answer {
        columns: columns.iter().map(|(c, t)| ((*c).to_owned(), *t)).collect(),
        rows,
        truncated,
    }
}

#[test]
fn rows_condition_counts_rows_and_keeps_ten() {
    let rows: Vec<Hit> = (0..12).map(|i| hit(&[("n", json!(i))])).collect();
    let a = answer(&[("n", ColumnType::Int64)], rows, true);
    let j = judge(
        &Condition::Rows {
            op: CmpOp::Ge,
            n: 12,
        },
        &a,
    )
    .unwrap();
    assert!(j.matched);
    assert_eq!(j.matched_rows, 12);
    assert_eq!(j.rows.len(), 10);
    assert_eq!(j.rows[0].get("n"), Some(&json!(0)));
}

#[test]
fn column_condition_any_row_null_not_satisfying() {
    let rows = vec![
        hit(&[("host", json!("a")), ("lat", json!(10))]),
        hit(&[("host", json!("b")), ("lat", json!(250))]),
        hit(&[("host", json!("c"))]),
    ];
    let a = answer(
        &[("host", ColumnType::Utf8), ("lat", ColumnType::Int64)],
        rows,
        false,
    );
    let cond = |t: i64| Condition::Column {
        column: "lat".into(),
        op: CmpOp::Gt,
        threshold: Threshold::Int(t),
    };
    let j = judge(&cond(200), &a).unwrap();
    assert!(j.matched);
    assert_eq!(j.matched_rows, 1);
    assert_eq!(j.rows[0].get("host"), Some(&json!("b")));
    assert!(!judge(&cond(300), &a).unwrap().matched);
    let empty = answer(&[("lat", ColumnType::Float64)], vec![], false);
    assert!(!judge(&cond(0), &empty).unwrap().matched);
}

#[test]
fn column_condition_missing_or_not_numeric_is_error() {
    let a = answer(&[("host", ColumnType::Utf8)], vec![], false);
    let cond = |c: &str| Condition::Column {
        column: c.into(),
        op: CmpOp::Gt,
        threshold: Threshold::Float(0.0),
    };
    assert_eq!(
        judge(&cond("lat"), &a),
        Err(ErrorReason::ConditionColumnMissing)
    );
    assert_eq!(
        judge(&cond("host"), &a),
        Err(ErrorReason::ConditionColumnNotNumeric)
    );
}

#[test]
fn first_tick_is_not_before_opening() {
    assert_eq!(first_tick(120, 60), 120);
    assert_eq!(first_tick(121, 60), 180);
    assert_eq!(first_tick(0, 60), 0);
}

#[tokio::test]
async fn same_instant_requests_are_granted_by_name() {
    let slots = Slots::new(1);
    let far = i64::MAX;
    let mut rx = slots.request(vec![(10, "a2".into(), far), (10, "a1".into(), far)]);
    let a1 = rx.pop().unwrap();
    let a2 = rx.pop().unwrap();
    let Ok(Grant::Slot(p1)) = a1.await else {
        panic!("a1 应先取得名额")
    };
    let mut a2 = a2;
    assert!(a2.try_recv().is_err(), "名额只有一个，a2 还在等");
    drop(p1);
    assert!(matches!(a2.await, Ok(Grant::Slot(_))));
}

#[tokio::test]
async fn expired_waiter_is_not_granted() {
    let slots = Slots::new(1);
    let mut rx = slots.request(vec![(1, "a".into(), i64::MAX)]);
    let Ok(Grant::Slot(p)) = rx.pop().unwrap().await else {
        panic!()
    };
    // 截止时刻已过：名额空出时不交给它
    let late = slots.request(vec![(2, "b".into(), 0)]).pop().unwrap();
    drop(p);
    assert!(matches!(late.await, Ok(Grant::Expired)));
}

// ---------------------------------------------------------------------------------------------------------------
// 调度循环（替身）

/// 每次评估的查询：先等 `gate`（若有），再交出固定的结果。
struct FakeQuery {
    result: Result<Answer, QueryError>,
    calls: AtomicUsize,
    /// 只挡第一次调用。
    gate: Option<Arc<Notify>>,
}


impl Query for FakeQuery {
    fn run<'a>(
        &'a self,
        _org: &'a str,
        _sql: &'a str,
        _start: i64,
        _end: i64,
    ) -> BoxFuture<'a, Result<Answer, QueryError>> {
        Box::pin(async move {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if n == 0
                && let Some(g) = &self.gate
            {
                g.notified().await;
            }
            self.result.clone()
        })
    }
}

struct FakeDelivery;

impl Delivery for FakeDelivery {
    fn deliver(&self, _notice: Notice, _stop: Stop) -> BoxFuture<'_, DeliveryOutcome> {
        Box::pin(async {
            DeliveryOutcome {
                result: DeliveryResult::Delivered,
                attempts: 1,
                last_status: Some(200),
            }
        })
    }
}

#[derive(Default)]
struct Records(StdMutex<Vec<EvaluationRecord>>);

impl Recorder for Records {
    fn record(&self, record: EvaluationRecord) {
        self.0.lock().unwrap().push(record);
    }
}

impl Records {
    fn of(&self, rule: &str) -> Vec<EvaluationRecord> {
        let mut v: Vec<_> = self
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.rule == rule)
            .cloned()
            .collect();
        v.sort_by_key(|r| r.scheduled_at);
        v
    }
}

fn rule(name: &str, interval: Duration) -> Arc<Rule> {
    Arc::new(Rule {
        name: name.into(),
        org: "default".into(),
        interval,
        window: interval,
        source: Source::Sql("SELECT * FROM app".into()),
        condition: Condition::Rows {
            op: CmpOp::Gt,
            n: 0,
        },
        webhook: Target {
            url: crate::alerting::webhook::parse_url("http://127.0.0.1:9/hook").unwrap(),
            headers: vec![],
        },
    })
}

fn one_row() -> Result<Answer, QueryError> {
    Ok(answer(
        &[("k", ColumnType::Int64)],
        vec![hit(&[("k", json!(1))])],
        false,
    ))
}

struct Harness {
    /// 调度开始之时（UTC 微秒）。
    opened: i64,
    records: Arc<Records>,
    counters: Arc<AlertCounters>,
    trigger: crate::alerting::StopTrigger,
    scheduler: Scheduler,
}

fn start(rules: Vec<Arc<Rule>>, capacity: usize, query: Arc<FakeQuery>) -> Harness {
    let records = Arc::new(Records::default());
    let counters = Arc::new(AlertCounters::new(rules.iter().map(|r| r.name.as_str())));
    let (trigger, stop) = Stop::pair();
    let opened = now_us();
    let scheduler = Scheduler::start(
        Deps {
            rules,
            max_concurrency: capacity,
            query,
            delivery: Arc::new(FakeDelivery),
            recorder: Arc::clone(&records) as Arc<dyn Recorder>,
            counters: Arc::clone(&counters),
            missed: None,
            max_size: 10_000,
        },
        opened,
        stop,
    );
    Harness {
        opened,
        records,
        counters,
        trigger,
        scheduler,
    }
}

const TICK: Duration = Duration::from_millis(400);

fn us(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap()
}

/// 睡到墙钟的 `at`（UTC 微秒）。
async fn until(at: i64) {
    let now = now_us();
    if at > now {
        tokio::time::sleep(Duration::from_micros((at - now) as u64)).await;
    }
}

impl Harness {
    /// 周期为 `interval` 的规则的第一个计划时刻。
    fn first(&self, interval: Duration) -> i64 {
        first_tick(self.opened, us(interval))
    }

    async fn stop(self) {
        self.trigger.stop();
        self.scheduler.stopped().await;
    }
}

#[tokio::test]
async fn evaluates_once_per_tick_and_delivers_matches() {
    let q = Arc::new(FakeQuery {
        result: one_row(),
        calls: AtomicUsize::new(0),
        gate: None,
    });
    let h = start(vec![rule("r1", TICK)], 1, q);
    let t1 = h.first(TICK);
    until(t1 + us(TICK) * 2 + us(TICK) / 2).await;
    let (records, counters) = (Arc::clone(&h.records), Arc::clone(&h.counters));
    h.stop().await;
    let recs = records.of("r1");
    assert_eq!(recs.len(), 3, "{recs:?}");
    assert_eq!(recs[0].scheduled_at, t1, "第一个计划时刻不早于开始调度之时");
    let tick = us(TICK);
    for w in recs.windows(2) {
        assert_eq!(
            w[1].scheduled_at - w[0].scheduled_at,
            tick,
            "相邻计划时刻相差一个周期"
        );
    }
    for r in &recs {
        assert_eq!(r.scheduled_at % tick, 0, "按周期的整数倍对齐");
        assert_eq!(r.outcome, Outcome::Matched);
        assert_eq!(r.matched_rows, Some(1));
        assert_eq!(r.delivery, DeliveryResult::Delivered);
        assert_eq!(
            (r.window_start, r.window_end),
            (r.scheduled_at - tick, r.scheduled_at)
        );
        assert!(r.started_at.unwrap() >= r.scheduled_at);
    }
    let mut out = String::new();
    counters.render(&mut out);
    assert!(out.contains(&format!(
        "amberlog_alert_evaluations_total{{rule=\"r1\",outcome=\"matched\"}} {}\n",
        recs.len()
    )));
}

#[tokio::test]
async fn query_running_past_a_tick_skips_the_next() {
    let gate = Arc::new(Notify::new());
    let q = Arc::new(FakeQuery {
        result: one_row(),
        calls: AtomicUsize::new(0),
        gate: Some(Arc::clone(&gate)),
    });
    let h = start(vec![rule("r1", TICK)], 1, q);
    let t1 = h.first(TICK);
    // 第一轮的查询挡过下一个计划时刻
    until(t1 + us(TICK) + us(TICK) / 2).await;
    gate.notify_one();
    until(t1 + us(TICK) * 2 + us(TICK) / 2).await;
    let records = Arc::clone(&h.records);
    h.stop().await;
    let recs = records.of("r1");
    assert_eq!(recs.len(), 3, "{recs:?}");
    assert_eq!(recs[0].outcome, Outcome::Matched, "{recs:?}");
    assert_eq!(
        recs[1].outcome,
        Outcome::Skipped(SkipReason::PreviousRunning)
    );
    assert_eq!(recs[1].started_at, None);
    assert_eq!(recs[1].delivery, DeliveryResult::NotAttempted);
    assert_eq!(recs[2].outcome, Outcome::Matched, "再下一轮照常评估");
}

#[tokio::test]
async fn waiting_for_alert_slot_until_next_tick_is_skipped() {
    // 唯一的名额被第一个评估（a1，周期短）的查询占住；b1 在它的计划时刻 T 排队，等到 T 加 2 个周期仍未取得
    let gate = Arc::new(Notify::new());
    let q = Arc::new(FakeQuery {
        result: one_row(),
        calls: AtomicUsize::new(0),
        gate: Some(Arc::clone(&gate)),
    });
    let h = start(vec![rule("a1", TICK), rule("b1", TICK * 2)], 1, q);
    let t = h.first(TICK * 2);
    until(t + us(TICK) * 2 + us(TICK) / 2).await;
    gate.notify_one();
    until(t + us(TICK) * 3 + us(TICK) / 2).await;
    let records = Arc::clone(&h.records);
    h.stop().await;
    let b1 = records.of("b1");
    assert_eq!(b1[0].scheduled_at, t, "{b1:?}");
    assert_eq!(b1[0].outcome, Outcome::Skipped(SkipReason::AlertSlotsFull));
    assert_eq!(b1[0].started_at, None);
    assert_eq!(b1[1].scheduled_at, t + us(TICK) * 2);
    assert_eq!(
        b1[1].outcome,
        Outcome::Matched,
        "等名额不使下一轮记 previous_running"
    );
}

#[tokio::test]
async fn query_slots_full_is_skipped_with_started_at() {
    let q = Arc::new(FakeQuery {
        result: Err(QueryError::new(Reason::TooManyQueries, "full")),
        calls: AtomicUsize::new(0),
        gate: None,
    });
    let h = start(vec![rule("r1", TICK)], 1, q);
    until(h.first(TICK) + us(TICK) / 2).await;
    let records = Arc::clone(&h.records);
    h.stop().await;
    let r = &records.of("r1")[0];
    assert_eq!(r.outcome, Outcome::Skipped(SkipReason::QuerySlotsFull));
    assert!(r.started_at.is_some());
    assert_eq!((r.matched_rows, r.truncated), (None, None));
}

#[tokio::test]
async fn stop_while_querying_is_error_and_waiting_is_skipped() {
    let gate = Arc::new(Notify::new());
    let q = Arc::new(FakeQuery {
        result: one_row(),
        calls: AtomicUsize::new(0),
        gate: Some(gate),
    });
    let h = start(vec![rule("a1", TICK), rule("a2", TICK)], 1, q);
    until(h.first(TICK) + us(TICK) / 4).await;
    let records = Arc::clone(&h.records);
    h.stop().await;
    let a1 = records.of("a1");
    let a2 = records.of("a2");
    assert_eq!(a1.len(), 1, "{a1:?}");
    assert_eq!(a1[0].outcome, Outcome::Error(ErrorReason::ShuttingDown));
    assert!(a1[0].started_at.is_some());
    assert_eq!(a2.len(), 1, "{a2:?}");
    assert_eq!(a2[0].outcome, Outcome::Skipped(SkipReason::ShuttingDown));
    assert_eq!(a2[0].started_at, None);
}

/// 内置规则的结果来源替身：记下每次调用的 `(after, until)`，只在第一次交出一行。
#[derive(Default)]
struct FakeMissed(StdMutex<Vec<(i64, i64)>>);

impl MissedWindows for FakeMissed {
    fn missed_collection(&self, after: i64, until: i64) -> Vec<MissedRow> {
        let mut calls = self.0.lock().unwrap();
        calls.push((after, until));
        if calls.len() > 1 {
            return Vec::new();
        }
        vec![MissedRow {
            dataset: "default/app".into(),
            window_start: 60_000_000,
            window_end: 120_000_000,
            ts_sent: Some(5),
            ts_durable: None,
            missing_sources: "fw-1".into(),
        }]
    }
}

#[tokio::test]
async fn builtin_rule_reads_missed_windows_without_query_slot() {
    let q = Arc::new(FakeQuery {
        result: Err(QueryError::new(Reason::TooManyQueries, "full")),
        calls: AtomicUsize::new(0),
        gate: None,
    });
    let missed = Arc::new(FakeMissed::default());
    let records = Arc::new(Records::default());
    let builtin = Arc::new(crate::alerting::rule::builtin(
        TICK,
        Target {
            url: crate::alerting::webhook::parse_url("http://127.0.0.1:9/hook").unwrap(),
            headers: vec![],
        },
    ));
    let (trigger, stop) = Stop::pair();
    let opened = now_us();
    let scheduler = Scheduler::start(
        Deps {
            rules: vec![builtin],
            max_concurrency: 1,
            query: Arc::clone(&q) as Arc<dyn Query>,
            delivery: Arc::new(FakeDelivery),
            recorder: Arc::clone(&records) as Arc<dyn Recorder>,
            counters: Arc::new(AlertCounters::new(["missed_collection"])),
            missed: Some(Arc::clone(&missed) as Arc<dyn MissedWindows>),
            max_size: 10_000,
        },
        opened,
        stop,
    );
    until(first_tick(opened, us(TICK)) + us(TICK) + us(TICK) / 2).await;
    trigger.stop();
    scheduler.stopped().await;
    assert_eq!(q.calls.load(Ordering::SeqCst), 0, "不执行查询、不取查询执行名额");
    let recs = records.of("missed_collection");
    assert_eq!(recs[0].outcome, Outcome::Matched, "{recs:?}");
    assert_eq!(recs[0].matched_rows, Some(1));
    assert_eq!(recs[1].outcome, Outcome::NotMatched);
    let calls = missed.0.lock().unwrap().clone();
    assert_eq!(calls[0].0, opened, "第一次自开始受理请求之时起");
    assert_eq!(calls[1].0, calls[0].1, "此后自上一次求得结果之时起");
    assert_eq!(Some(calls[0].1), recs[0].started_at);
}

#[test]
fn builtin_answer_columns_nulls_and_truncation() {
    let row = |w: i64| MissedRow {
        dataset: "default/app".into(),
        window_start: w,
        window_end: w + 60_000_000,
        ts_sent: None,
        ts_durable: Some(3),
        missing_sources: "a,b".into(),
    };
    let a = builtin_answer(vec![row(0), row(60_000_000), row(120_000_000)], 2);
    assert!(a.truncated);
    assert_eq!(a.rows.len(), 2);
    assert_eq!(
        a.columns.iter().map(|(c, _)| c.as_str()).collect::<Vec<_>>(),
        ["dataset", "window_start", "window_end", "ts_sent", "ts_durable", "missing_sources"]
    );
    assert_eq!(a.rows[0].get("ts_sent"), None, "NULL 不写键");
    assert_eq!(a.rows[0].get("ts_durable"), Some(&json!(3)));
}
