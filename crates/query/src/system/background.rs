//! 后台判定（REQ-AMB-GOV-MISSED-COLLECTION C 组）：一次后台判定的计算与内存结论。周期任务本身（每 60 秒以内开始一次、
//! 停机时放弃）由写入实例的启动流程驱动，这里只做「以 t 为时刻算一次」。
//!
//! - **所用计数**：每个数据集读 `[t − 48 小时, t)` 所跨各小时的计数（已写出的加本进程尚未写出的，BG-COUNTS）；
//!   读不出的小时只使需要它的窗口或来源略去（BG-UNREADABLE-SKIP、BG-UNREADABLE-CONTINUE）。
//! - **内存结论**：按 (数据集, 窗口起点) 记已有结论（最近一次未略去、不是 `pending` 的判定，TERM-PRIOR），按 (数据集, 来源)
//!   记最近一次未略去它时是否此刻上报中断；超出 24 小时回看的窗口不再被判定，随之丢弃。
//! - **基线与事件**：开始受理 `/api/` 请求之时加 G 及其之后的第一次后台判定是基线；基线及其之前不产生事件
//!   （BASELINE-NO-EVENT），此后转为缺失、转为上报中断各输出一条事件（EVENT-MISSING、EVENT-SILENT）。
//! - **指标**：每次后台判定结束时按这次未略去的窗口整体替换该数据集的六个取值（GAUGE）。
//!
//! 结论只在内存，不写进对象存储或 `wal.dir`（NOT-PERSISTED）。

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::{Arc, Mutex};

use amberlog_common::dataset::Dataset;
use amberlog_storage::registry::DatasetRegistry;

use super::CounterObjects;
use super::reconciliation::checked_sum;
use super::verdict::{
    Backfill, LOOKBACK_US, MINUTE_US, Verdict, judge, judged_sources, reported_before,
    reports_by_source, silent_now,
};

const HOUR_US: i64 = 3_600_000_000;

fn hour_of(us: i64) -> i64 {
    us.div_euclid(HOUR_US) * HOUR_US
}

/// 后台判定输出的两条稳定事件（由健康与指标的日志出口输出）。
pub trait VerdictEvents: Send + Sync {
    /// `reconciliation_missing`：`sources` 为缺失、且不按待定计的受判来源按码点升序以 `,` 连接，`missing` 为它们的差额之和。
    fn missing(&self, dataset: &Dataset, window_start: i64, sources: &str, missing: u64);
    /// `source_silent`：`last_window_start` 为该来源在 `[t − 24 小时, t)` 内有按时间戳发送数的窗口中最晚者的起点。
    fn silent(&self, dataset: &Dataset, source: &str, last_window_start: i64);
}

/// 某次后台判定中转为缺失的一个窗口。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TurnedMissing {
    /// 这次后台判定的完成时刻，UTC 微秒。
    pub completed_at: i64,
    pub dataset: Dataset,
    pub window_start: i64,
    /// 缺失、且不按待定计的受判来源按码点升序以 `,` 连接。
    pub sources: String,
    /// 这些来源的按时间戳的发送数减确认数之和。
    pub missing: u64,
}

/// 内置告警规则 `missed_collection` 结果的一行（REQ-AMB-ALR-RULES-BUILTIN-RESULT-COLUMNS）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissingWindow {
    pub dataset: Dataset,
    pub window_start: i64,
    pub window_end: i64,
    /// 各受判来源的按时间戳的发送数之和，按最近一次完成的后台判定所用的计数；超出 Int64 时为 `None`。
    pub ts_sent: Option<i64>,
    /// 各受判来源的按时间戳的确认数之和，口径同 `ts_sent`。
    pub ts_durable: Option<i64>,
    /// 缺失的受判来源按码点升序以 `,` 连接（最近一次判为 `missing` 时的）。
    pub missing_sources: String,
}

/// 某窗口在最近几次后台判定中的计数与缺失来源。
struct Latest {
    ts_sent: Option<i64>,
    ts_durable: Option<i64>,
    missing_sources: String,
}

#[derive(Default)]
struct State {
    /// 基线时刻；尚未到基线为 `None`。
    baseline: Option<i64>,
    /// (数据集, 窗口起点) → 已有结论。
    prior: HashMap<(Dataset, i64), Verdict>,
    /// (数据集, 来源) → 最近一次未略去它的后台判定中是否此刻上报中断；只记当时有按时间戳发送数的来源。
    silent: HashMap<(Dataset, String), bool>,
    /// 来源未被略去地判断过此刻上报中断的数据集：此后某来源不在 `silent` 中，即它在最近一次未略去它的后台判定中
    /// 没有按时间戳发送数、不是此刻上报中断（TERM-TURNED-SILENT 的第一分支）。
    checked: std::collections::HashSet<Dataset>,
    /// 最近一次后台判定中各数据集被计算判定的窗口按判定的个数，次序同 [`Verdict::ALL`]。
    gauge: HashMap<Dataset, [u64; 6]>,
    /// 各次后台判定中转为缺失的窗口（保留 24 小时）。
    turned: VecDeque<TurnedMissing>,
    /// (数据集, 窗口起点) → 最近一次判定它时所用的计数，与最近一次判为 `missing` 时缺失的来源。
    latest: HashMap<(Dataset, i64), Latest>,
}

/// 后台判定的计算与内存结论。
pub struct Background {
    registry: Arc<DatasetRegistry>,
    counters: Arc<dyn CounterObjects>,
    backfill: Arc<dyn Backfill>,
    events: Arc<dyn VerdictEvents>,
    grace_us: i64,
    /// 本进程开始受理 `/api/` 请求的时刻（基线由它加 G 定出）。
    accepting_since: Box<dyn Fn() -> Option<i64> + Send + Sync>,
    state: Mutex<State>,
}

/// 一个数据集在这次后台判定中的结果。
struct Outcome {
    counts: [u64; 6],
    events: Vec<Event>,
}

enum Event {
    Missing(TurnedMissing),
    Silent { source: String, last: i64 },
}

/// 判定所需的某个小时读不出。
struct Skip;

impl Background {
    pub fn new(
        registry: Arc<DatasetRegistry>,
        counters: Arc<dyn CounterObjects>,
        backfill: Arc<dyn Backfill>,
        events: Arc<dyn VerdictEvents>,
        grace_us: i64,
        accepting_since: Box<dyn Fn() -> Option<i64> + Send + Sync>,
    ) -> Background {
        Background {
            registry,
            counters,
            backfill,
            events,
            grace_us,
            accepting_since,
            state: Mutex::new(State::default()),
        }
    }

    /// 以 `t` 为时刻做一次后台判定（BG-PERIOD），`completed_at` 给出完成时刻（调用方的时钟）。
    pub async fn round(&self, t: i64, completed_at: impl Fn() -> i64) {
        // 基线：开始受理之时加 G 及其之后的第一次（TERM-BASELINE）；这一次及之前不产生事件
        let emit = {
            let mut st = self.state.lock().expect("后台判定状态未中毒");
            match st.baseline {
                Some(_) => true,
                None => {
                    if let Some(a) = (self.accepting_since)()
                        && t >= a.saturating_add(self.grace_us)
                    {
                        st.baseline = Some(t);
                    }
                    false
                }
            }
        };
        let mut datasets = self.registry.datasets();
        datasets.sort();
        let mut results = Vec::with_capacity(datasets.len());
        for d in datasets {
            let outcome = self.dataset_round(&d, t, emit).await;
            results.push((d, outcome));
        }
        let done = completed_at();
        let mut events = Vec::new();
        {
            let mut st = self.state.lock().expect("后台判定状态未中毒");
            for (d, o) in results {
                st.gauge.insert(d.clone(), o.counts);
                for e in o.events {
                    if let Event::Missing(mut m) = e {
                        m.completed_at = done;
                        st.turned.push_back(m.clone());
                        events.push((d.clone(), Event::Missing(m)));
                    } else {
                        events.push((d.clone(), e));
                    }
                }
            }
            while st
                .turned
                .front()
                .is_some_and(|m| m.completed_at < done - LOOKBACK_US)
            {
                st.turned.pop_front();
            }
            st.prior.retain(|(_, w), _| *w >= t - LOOKBACK_US);
            st.latest.retain(|(_, w), _| *w >= t - LOOKBACK_US);
        }
        for (d, e) in events {
            match e {
                Event::Missing(m) => self
                    .events
                    .missing(&d, m.window_start, &m.sources, m.missing),
                Event::Silent { source, last } => self.events.silent(&d, &source, last),
            }
        }
    }

    async fn dataset_round(&self, d: &Dataset, t: i64, emit: bool) -> Outcome {
        let hours = self.counters.current_hours(d, t - 2 * LOOKBACK_US, t).await;
        let unreadable: BTreeSet<i64> = hours
            .iter()
            .filter(|(_, r)| r.is_err())
            .map(|(h, _)| *h)
            .collect();
        let mut windows: Vec<_> = hours
            .into_iter()
            .filter_map(|(_, r)| r.ok())
            .flatten()
            .collect();
        windows.sort_by_key(|w| w.window_start);
        // `[from, to)` 所跨的小时里有读不出的
        let blocked = |from: i64, to: i64| {
            unreadable
                .range(hour_of(from)..=hour_of(to - 1))
                .next()
                .is_some()
        };

        let mut counts = [0u64; 6];
        let mut events = Vec::new();
        let baseline = self.state.lock().expect("未中毒").baseline;
        let mut judged = Vec::new();
        for w in windows
            .iter()
            .filter(|w| w.window_start >= t - LOOKBACK_US && w.window_start < t)
        {
            let j = judge(d, w, t, self.grace_us, self.backfill.as_ref(), || {
                if blocked(w.window_start - LOOKBACK_US, w.window_start) {
                    Err(Skip)
                } else {
                    Ok(reported_before(&windows, w.window_start))
                }
            });
            if let Ok(j) = j {
                let sums = (
                    checked_sum(judged_sources(w).map(|(_, n)| n)),
                    checked_sum(
                        judged_sources(w)
                            .map(|(s, _)| w.durable_ts_by_source.get(s).copied().unwrap_or(0)),
                    ),
                );
                judged.push((w.window_start, j, sums));
            }
        }

        // 此刻上报中断：要 `[t − 24 小时, t)` 所跨的全部小时都读得出
        let mut silent_now_sources = Vec::new();
        let sources_checked = !blocked(t - LOOKBACK_US, t);
        if sources_checked {
            let recent: Vec<_> = windows
                .iter()
                .filter(|w| w.window_start >= t - LOOKBACK_US && w.window_start < t)
                .cloned()
                .collect();
            for (source, starts) in reports_by_source(&recent) {
                let last = *starts.last().expect("至少一个窗口");
                silent_now_sources.push((source, last, silent_now(last, t, self.grace_us)));
            }
        }

        let mut st = self.state.lock().expect("后台判定状态未中毒");
        for (start, j, (ts_sent, ts_durable)) in judged {
            counts[j.verdict.index()] += 1;
            let key = (d.clone(), start);
            let latest = st.latest.entry(key.clone()).or_insert(Latest {
                ts_sent,
                ts_durable,
                missing_sources: String::new(),
            });
            (latest.ts_sent, latest.ts_durable) = (ts_sent, ts_durable);
            if j.verdict == Verdict::Missing {
                let sources: Vec<&str> = j.missing.iter().map(|(s, _)| s.as_str()).collect();
                latest.missing_sources = sources.join(",");
            }
            let prior = st.prior.get(&key).copied();
            if emit && j.verdict == Verdict::Missing {
                let turned = match prior {
                    Some(p) => p != Verdict::Missing,
                    None => baseline.is_some_and(|b| start + MINUTE_US + self.grace_us > b),
                };
                if turned {
                    let sources: Vec<&str> = j.missing.iter().map(|(s, _)| s.as_str()).collect();
                    events.push(Event::Missing(TurnedMissing {
                        completed_at: 0,
                        dataset: d.clone(),
                        window_start: start,
                        sources: sources.join(","),
                        missing: j
                            .missing
                            .iter()
                            .map(|(_, n)| *n)
                            .fold(0u64, u64::saturating_add),
                    }));
                }
            }
            if j.verdict != Verdict::Pending {
                st.prior.insert(key, j.verdict);
            }
        }
        let checked_before = st.checked.contains(d);
        if sources_checked {
            // 不再有按时间戳发送数的来源这一次不是此刻上报中断
            let current: BTreeSet<&String> = silent_now_sources.iter().map(|(s, _, _)| s).collect();
            st.silent
                .retain(|(dd, s), _| dd != d || current.contains(s));
            st.checked.insert(d.clone());
        }
        for (source, last, now) in silent_now_sources {
            let key = (d.clone(), source);
            let before = st
                .silent
                .get(&key)
                .copied()
                .or_else(|| checked_before.then_some(false));
            if emit && now {
                let turned = match before {
                    Some(b) => !b,
                    None => baseline.is_some_and(|b| last + 2 * MINUTE_US + self.grace_us > b),
                };
                if turned {
                    events.push(Event::Silent {
                        source: key.1.clone(),
                        last,
                    });
                }
            }
            st.silent.insert(key, now);
        }
        Outcome { counts, events }
    }

    /// 最近一次后台判定中 `dataset` 被计算判定的窗口按判定的个数，次序同 [`Verdict::ALL`]；还没有为它做过时全为 0。
    pub fn gauge(&self, dataset: &Dataset) -> [u64; 6] {
        self.state
            .lock()
            .expect("后台判定状态未中毒")
            .gauge
            .get(dataset)
            .copied()
            .unwrap_or_default()
    }

    /// 内置告警规则 `missed_collection` 一次评估的结果（REQ-AMB-ALR-RULES-BUILTIN-RESULT）：组织 `default` 的各数据集中，
    /// 完成时刻在 `(after, until]` 内的某次后台判定中转为缺失、且到此刻已有结论（最近一次未被略去、不为 `pending` 的判定）
    /// 仍为 `missing` 的窗口，每个一行；按 `window_start` 升序、同一起点内按 `dataset` 的码点升序（BUILTIN-RESULT-ORDER）。
    ///
    /// `after` 为本规则此前最近一次求得结果之时（本进程中此前没有时为开始受理 `/api/` 请求之时），`until` 为这次开始
    /// 求结果之时；由调用方在开始求结果之时调用。
    pub fn missed_collection(&self, after: i64, until: i64) -> Vec<MissingWindow> {
        let st = self.state.lock().expect("后台判定状态未中毒");
        let mut keys: Vec<(i64, String, &Dataset)> = st
            .turned
            .iter()
            .filter(|m| m.completed_at > after && m.completed_at <= until)
            .filter(|m| m.dataset.org == "default")
            .filter(|m| {
                st.prior.get(&(m.dataset.clone(), m.window_start)) == Some(&Verdict::Missing)
            })
            .map(|m| (m.window_start, m.dataset.to_string(), &m.dataset))
            .collect();
        keys.sort();
        keys.dedup_by(|a, b| a.0 == b.0 && a.1 == b.1);
        keys.into_iter()
            .map(|(start, _, d)| {
                let latest = st.latest.get(&(d.clone(), start));
                MissingWindow {
                    dataset: d.clone(),
                    window_start: start,
                    window_end: start + MINUTE_US,
                    ts_sent: latest.and_then(|l| l.ts_sent),
                    ts_durable: latest.and_then(|l| l.ts_durable),
                    missing_sources: latest
                        .map(|l| l.missing_sources.clone())
                        .unwrap_or_default(),
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests;
