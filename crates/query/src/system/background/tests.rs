use std::collections::BTreeMap;
use std::sync::atomic::{AtomicI64, Ordering};

use amberlog_storage::registry::Persistence;

use super::*;
use crate::system::{CounterError, CounterWindow};
use crate::testing::{FakeBackfill, FakeCounters};

const M: i64 = MINUTE_US;
const G: i64 = 60_000_000;
/// 开始受理 `/api/` 请求的时刻（远离整点，免得窗口跨小时）。
const A: i64 = 1_000 * HOUR_US + 10 * M;

fn ds(stream: &str) -> Dataset {
    Dataset::new("default", stream)
}

#[derive(Default)]
struct Events(Mutex<Vec<String>>);

impl VerdictEvents for Events {
    fn missing(&self, dataset: &Dataset, window_start: i64, sources: &str, missing: u64) {
        self.0.lock().unwrap().push(format!(
            "missing {dataset} {window_start} {sources} {missing}"
        ));
    }
    fn silent(&self, dataset: &Dataset, source: &str, last_window_start: i64) {
        self.0
            .lock()
            .unwrap()
            .push(format!("silent {dataset} {source} {last_window_start}"));
    }
}

struct Fx {
    registry: Arc<DatasetRegistry>,
    counters: Arc<FakeCounters>,
    backfill: Arc<FakeBackfill>,
    events: Arc<Events>,
    bg: Background,
}

impl Fx {
    fn new() -> Fx {
        let registry = Arc::new(DatasetRegistry::new());
        let counters = Arc::new(FakeCounters::new());
        let backfill = Arc::new(FakeBackfill::new());
        let events = Arc::new(Events::default());
        let bg = Background::new(
            registry.clone(),
            counters.clone(),
            backfill.clone(),
            events.clone(),
            G,
            Box::new(|| Some(A)),
        );
        Fx {
            registry,
            counters,
            backfill,
            events,
            bg,
        }
    }

    fn put(&self, stream: &str, windows: Vec<CounterWindow>) {
        self.registry.record(&ds(stream), Persistence::Stored);
        self.counters.put(&ds(stream), windows);
    }

    async fn round(&self, t: i64) {
        self.bg.round(t, || t).await;
    }

    fn events(&self) -> Vec<String> {
        std::mem::take(&mut *self.events.0.lock().unwrap())
    }

    /// 只看 `reconciliation_missing`（来源停报的事件另有测试）。
    fn missing_events(&self) -> Vec<String> {
        self.events()
            .into_iter()
            .filter(|e| e.starts_with("missing"))
            .collect()
    }
}

fn win(start: i64, sent_ts: u64, durable: u64) -> CounterWindow {
    let one = |n: u64| BTreeMap::from([("fw-1".to_owned(), n)]);
    CounterWindow {
        window_start: start,
        window_end: start + M,
        received: 0,
        durable: 0,
        sent: BTreeMap::new(),
        durable_ts: durable,
        durable_ts_by_source: if durable > 0 {
            one(durable)
        } else {
            BTreeMap::new()
        },
        sent_ts: one(sent_ts),
    }
}

/// 基线及其之前不打事件；基线之后转为缺失恰打一次，`pending` 再回到 `missing` 不再打；指标按最近一次替换。
#[tokio::test]
async fn baseline_and_turned_missing_once() {
    let fx = Fx::new();
    let w_old = A - 20 * M; // 基线之前已到期
    fx.put("app", vec![win(w_old, 5, 3)]);
    fx.round(A + 30_000_000).await; // 基线之前
    fx.round(A + G).await; // 基线
    assert!(fx.missing_events().is_empty());
    assert_eq!(fx.bg.gauge(&ds("app"))[Verdict::Missing.index()], 1);

    // 基线之后：已有结论为 missing 的窗口不再打；新窗口结束加 G 晚于基线、到期后转为缺失
    let w_new = A + 5 * M;
    fx.put("app", vec![win(w_old, 5, 3), win(w_new, 2, 1)]);
    fx.round(w_new + M + G).await;
    assert_eq!(
        fx.missing_events(),
        [format!("missing default/app {w_new} fw-1 1")]
    );
    // 补发中：待定；补发停下后回到与已有结论相同的 missing，不再打
    fx.backfill.set(&ds("app"), "fw-1");
    fx.round(w_new + 2 * M + G).await;
    assert_eq!(fx.bg.gauge(&ds("app"))[Verdict::Pending.index()], 2);
    fx.backfill.clear();
    fx.round(w_new + 3 * M + G).await;
    assert!(fx.missing_events().is_empty());
    let done = w_new + M + G;
    let row = MissingWindow {
        dataset: ds("app"),
        window_start: w_new,
        window_end: w_new + M,
        ts_sent: Some(2),
        ts_durable: Some(1),
        missing_sources: "fw-1".to_owned(),
    };
    assert_eq!(
        fx.bg.missed_collection(0, i64::MAX),
        std::slice::from_ref(&row)
    );
    // 完成时刻在 `(after, until]` 内才算
    assert_eq!(fx.bg.missed_collection(done - 1, done), [row]);
    assert!(fx.bg.missed_collection(done, i64::MAX).is_empty());
    assert!(fx.bg.missed_collection(0, done - 1).is_empty());

    // 补齐后改为一致；再缺失又是一次转为
    fx.put("app", vec![win(w_old, 5, 3), win(w_new, 2, 2)]);
    fx.round(w_new + 4 * M + G).await;
    assert_eq!(fx.bg.gauge(&ds("app"))[Verdict::Consistent.index()], 1);
    assert!(
        fx.bg.missed_collection(0, i64::MAX).is_empty(),
        "最近的结论已不是 missing"
    );
}

/// 内置规则的结果：同一窗口两次转为缺失只出一行；只取组织 `default`；按窗口起点、再按数据集排序；
/// 计数按最近一次判定所用的，和超出 Int64 时为 `None`。
#[tokio::test]
async fn missed_collection_rows() {
    let fx = Fx::new();
    let t0 = A + G;
    fx.round(t0).await; // 基线
    let (w1, w2) = (A + 5 * M, A + 6 * M);
    let other = Dataset::new("other", "app");
    fx.registry.record(&other, Persistence::Stored);
    fx.counters.put(&other, vec![win(w1, 5, 3)]);
    fx.put("web", vec![win(w1, 5, 3)]);
    fx.put("app", vec![win(w2, 5, 3), win(w1, 5, 3)]);
    let t1 = w2 + M + G;
    fx.round(t1).await;
    // app 的 w1 补齐后再缺：第二次转为缺失
    fx.put("app", vec![win(w2, 5, 3), win(w1, 5, 5)]);
    fx.round(t1 + M).await;
    let mut huge = win(w1, u64::MAX, 4);
    huge.sent_ts.insert("fw-2".to_owned(), 1);
    fx.put("app", vec![win(w2, 5, 3), huge]);
    fx.round(t1 + 2 * M).await;
    let rows: Vec<_> = fx
        .bg
        .missed_collection(0, i64::MAX)
        .into_iter()
        .map(|r| {
            (
                r.window_start,
                r.dataset.to_string(),
                r.ts_sent,
                r.ts_durable,
                r.missing_sources,
            )
        })
        .collect();
    assert_eq!(
        rows,
        [
            (
                w1,
                "default/app".to_owned(),
                None,
                Some(4),
                "fw-1,fw-2".to_owned()
            ),
            (
                w1,
                "default/web".to_owned(),
                Some(5),
                Some(3),
                "fw-1".to_owned()
            ),
            (
                w2,
                "default/app".to_owned(),
                Some(5),
                Some(3),
                "fw-1".to_owned()
            ),
        ]
    );
}

/// 一个小时读不出只略去需要它的窗口与来源；略去的窗口不计入指标、结论照旧，其他数据集照常。
#[tokio::test]
async fn unreadable_hour_skips_only_what_needs_it() {
    let fx = Fx::new();
    let t = A + 3 * HOUR_US;
    let bad_hour = (t - HOUR_US).div_euclid(HOUR_US) * HOUR_US;
    fx.put("app", vec![win(bad_hour + M, 5, 3), win(t - 5 * M, 5, 3)]);
    fx.put("web", vec![win(t - 5 * M, 5, 3)]);
    fx.counters.fail_hour(
        &ds("app"),
        bad_hour,
        CounterError::Unparseable("not json".into()),
    );
    fx.round(A + G).await; // 基线
    fx.round(t).await;
    let app = fx.bg.gauge(&ds("app"));
    assert_eq!(app[Verdict::Missing.index()], 1, "{app:?}");
    assert_eq!(app.iter().sum::<u64>(), 1);
    assert_eq!(fx.bg.gauge(&ds("web"))[Verdict::Missing.index()], 1);
    // 来源的此刻上报中断要 [t − 24 小时, t) 全部读得出：app 的来源略去，web 的照判
    let events = fx.events();
    assert_eq!(events.len(), 3, "{events:?}");
    let web_silent = format!("silent default/web fw-1 {}", t - 5 * M);
    assert!(events.contains(&web_silent), "{events:?}");
    assert!(
        !events.iter().any(|e| e.starts_with("silent default/app")),
        "{events:?}"
    );
}

/// 来源停报：此刻上报中断的转为恰打一次事件。
#[tokio::test]
async fn turned_silent_once() {
    let fx = Fx::new();
    let w2 = A + 4 * M;
    fx.put("app", vec![win(w2 - M, 0, 0), win(w2, 0, 0)]);
    fx.round(A + G).await; // 基线
    fx.round(w2 + 2 * M + G - 1).await;
    assert!(fx.events().is_empty());
    fx.round(w2 + 2 * M + G).await;
    fx.round(w2 + 3 * M + G).await;
    assert_eq!(fx.events(), [format!("silent default/app fw-1 {w2}")]);
}

/// 没有已有结论的窗口：结束加 G 不晚于基线时刻的，第一次得出 missing 不算转为。
#[tokio::test]
async fn no_prior_before_baseline_is_not_turned() {
    let fx = Fx::new();
    let baseline = A + G;
    let w = baseline - G - M; // 结束加 G 恰为基线时刻
    fx.put("app", vec![]);
    fx.round(baseline).await;
    fx.put("app", vec![win(w, 5, 3)]);
    fx.round(baseline + M).await;
    assert!(fx.missing_events().is_empty());
    let next = AtomicI64::new(0);
    next.store(baseline + 2 * M, Ordering::Relaxed);
    let w2 = baseline - G - M + M; // 结束加 G 晚于基线
    fx.put("app", vec![win(w, 5, 3), win(w2, 5, 3)]);
    fx.round(next.load(Ordering::Relaxed)).await;
    assert_eq!(
        fx.missing_events(),
        [format!("missing default/app {w2} fw-1 2")]
    );
}

/// 此前判过该数据集、而来源那时还没有按时间戳发送数：它那时不是此刻上报中断，停报后照样算转为上报中断。
#[tokio::test]
async fn silent_source_first_seen_after_checks() {
    let fx = Fx::new();
    let t0 = A + 30 * M;
    fx.put("app", vec![]);
    fx.round(A + G).await; // 基线
    fx.round(t0).await;
    // 迟来的上报：窗口早已过了「下一个窗口也过宽限期」
    fx.put("app", vec![win(t0 - 10 * M, 0, 0)]);
    fx.round(t0 + M).await;
    assert_eq!(
        fx.events(),
        [format!("silent default/app fw-1 {}", t0 - 10 * M)]
    );
    fx.round(t0 + 2 * M).await;
    assert!(fx.events().is_empty());
}
