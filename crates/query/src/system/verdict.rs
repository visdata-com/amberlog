//! 判定函数（REQ-AMB-GOV-MISSED-COLLECTION B 组）：对一个数据集的一个分钟窗口，按宽限期给出六种判定之一。
//! `_reconciliation` 的 `verdict`、`_ledger` 的判定汇总与后台判定都调用这里，判据只有一份。
//!
//! 只比按记录时间戳对齐的两个数：窗口的 `sent_ts` 与 `durable_ts_by_source`；按接收时刻的计数不参与。
//! 回看（W 之前 24 小时内各窗口的 `sent_ts`）只在要判断有没有来源在 W 上报中断时才取（TERM-NEEDED），
//! 由调用方以闭包给出，取不到时把错误原样交回。

use std::collections::{BTreeMap, BTreeSet};

use amberlog_common::dataset::Dataset;

use super::CounterWindow;

/// 分钟窗口的长度，UTC 微秒。
pub const MINUTE_US: i64 = 60_000_000;
/// 上报中断的回看长度：24 小时，UTC 微秒。
pub const LOOKBACK_US: i64 = 24 * 3_600_000_000;
/// 一个窗口 `durable_ts_by_source` 的来源个数上限（REQ-AMB-OBS-INGEST-COUNTERS-V3）。
const MAX_SOURCES: usize = 1000;

/// 判定的六个取值（REQ-AMB-GOV-MISSED-COLLECTION-VERDICT-SET）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Verdict {
    Pending,
    Consistent,
    Missing,
    Duplicate,
    Silent,
    Undeterminable,
}

impl Verdict {
    /// 六个取值，按 `amberlog_reconciliation_windows` 输出的次序。
    pub const ALL: [Verdict; 6] = [
        Verdict::Pending,
        Verdict::Consistent,
        Verdict::Missing,
        Verdict::Duplicate,
        Verdict::Silent,
        Verdict::Undeterminable,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Verdict::Pending => "pending",
            Verdict::Consistent => "consistent",
            Verdict::Missing => "missing",
            Verdict::Duplicate => "duplicate",
            Verdict::Silent => "silent",
            Verdict::Undeterminable => "undeterminable",
        }
    }

    /// 在 [`Verdict::ALL`] 中的位置。
    pub fn index(self) -> usize {
        Verdict::ALL
            .iter()
            .position(|v| *v == self)
            .expect("六个取值之一")
    }
}

/// 补发中的状态（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL），只在本进程内存。
pub trait Backfill: Send + Sync {
    /// 来源 `source` 对 `dataset` 在时刻 `t`（UTC 微秒）是否补发中。
    fn backfilling(&self, dataset: &Dataset, source: &str, t: i64) -> bool;
}

/// 一次判定的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Judgment {
    pub verdict: Verdict,
    /// 缺失、且不按待定计的受判来源及各自的差额（按时间戳的发送数减确认数），按来源名的码点升序；
    /// 判定为 `missing` 时非空，供稳定事件 `reconciliation_missing` 的 `sources` 与 `missing`。
    pub missing: Vec<(String, u64)>,
}

/// 窗口的受判来源（TERM-JUDGED）：`sent_ts` 的键；`durable_ts_by_source` 已满上限时，其中没有的来源不算。
pub fn judged_sources(w: &CounterWindow) -> impl Iterator<Item = (&str, u64)> {
    let full = w.durable_ts_by_source.len() >= MAX_SOURCES;
    w.sent_ts
        .iter()
        .filter(move |(s, _)| !full || w.durable_ts_by_source.contains_key(*s))
        .map(|(s, n)| (s.as_str(), *n))
}

/// 判定窗口 `w`（属于数据集 `dataset`）在时刻 `t` 的判定，`grace` 为宽限期（微秒）。
///
/// `lookback` 只在要判断有没有来源在 W 上报中断时调用，至多一次：它给出 `window_start` 落在 `[W − 24 小时, W)` 内
/// 各窗口的 `sent_ts` 所含来源之并；它的错误原样交回（所需的计数对象读不出，由调用方按各自的条款处置）。
pub fn judge<E>(
    dataset: &Dataset,
    w: &CounterWindow,
    t: i64,
    grace: i64,
    backfill: &dyn Backfill,
    lookback: impl FnOnce() -> Result<BTreeSet<String>, E>,
) -> Result<Judgment, E> {
    let pending = Judgment {
        verdict: Verdict::Pending,
        missing: Vec::new(),
    };
    let window_end = w.window_start + MINUTE_US;
    // PENDING-GRACE
    if t < window_end.saturating_add(grace) {
        return Ok(pending);
    }
    let holding = |s: &str| backfill.backfilling(dataset, s, t);

    // PER-SOURCE 与 BACKFILL-HOLD
    let mut judged = false;
    let mut missing = Vec::new();
    let mut duplicate = false;
    let mut held = false;
    for (source, sent) in judged_sources(w) {
        judged = true;
        let durable = w.durable_ts_by_source.get(source).copied().unwrap_or(0);
        if durable == sent {
            continue;
        }
        if holding(source) {
            held = true;
        } else if durable < sent {
            missing.push((source.to_owned(), sent - durable));
        } else {
            duplicate = true;
        }
    }
    // COMBINE 的前三档不需要回看
    if !missing.is_empty() {
        return Ok(Judgment {
            verdict: Verdict::Missing,
            missing,
        });
    }
    if duplicate {
        return Ok(Judgment {
            verdict: Verdict::Duplicate,
            missing: Vec::new(),
        });
    }
    if held {
        return Ok(pending);
    }

    // 在 W 上报中断（TERM-SILENT-AT）：W 之前 24 小时内报过、W 没报；t 不早于 W 结束加 G 已由上面保证
    let silent: Vec<String> = lookback()?
        .into_iter()
        .filter(|s| !w.sent_ts.contains_key(s))
        .collect();
    let verdict = if silent.iter().any(|s| holding(s)) {
        Verdict::Pending
    } else if !silent.is_empty() {
        Verdict::Silent
    } else if judged {
        Verdict::Consistent
    } else {
        Verdict::Undeterminable
    };
    Ok(Judgment {
        verdict,
        missing: Vec::new(),
    })
}

/// `windows`（按 `window_start` 升序）中 `window_start` 落在 `[W − 24 小时, W)` 内各窗口的 `sent_ts` 所含来源之并。
pub fn reported_before(windows: &[CounterWindow], window_start: i64) -> BTreeSet<String> {
    let from = window_start - LOOKBACK_US;
    let first = windows.partition_point(|w| w.window_start < from);
    windows[first..]
        .iter()
        .take_while(|w| w.window_start < window_start)
        .flat_map(|w| w.sent_ts.keys().cloned())
        .collect()
}

/// 各来源有按时间戳发送数的窗口起点（升序）。
pub fn reports_by_source(windows: &[CounterWindow]) -> BTreeMap<String, Vec<i64>> {
    let mut out: BTreeMap<String, Vec<i64>> = BTreeMap::new();
    for w in windows {
        for s in w.sent_ts.keys() {
            out.entry(s.clone()).or_default().push(w.window_start);
        }
    }
    for v in out.values_mut() {
        v.sort_unstable();
        v.dedup();
    }
    out
}

/// 来源在 `[start, end)` 内的某个整分钟 W 上，于时刻 `t` 是否在 W 上报中断（TERM-SILENT-AT；不看补发中）。
/// `reported` 是该来源有按时间戳发送数的窗口起点（升序），须含 `[start − 24 小时, end)` 内的全部。
/// 不逐分钟遍历：只看每段连续上报之后的空档。
pub fn silent_in_range(reported: &[i64], start: i64, end: i64, t: i64, grace: i64) -> bool {
    // W 结束加 G 不晚于 t，即 W 不晚于 t − G − 60 秒（向下取整到整分钟）
    let due = (t - grace - MINUTE_US).div_euclid(MINUTE_US) * MINUTE_US;
    let last = (end - 1).div_euclid(MINUTE_US) * MINUTE_US;
    let hi_all = due.min(last);
    for (i, &r) in reported.iter().enumerate() {
        let next = reported.get(i + 1).copied();
        if next == Some(r + MINUTE_US) {
            continue; // 不是一段连续上报的末尾
        }
        let lo = start.max(r + MINUTE_US);
        let lo = lo.div_euclid(MINUTE_US) * MINUTE_US
            + if lo.rem_euclid(MINUTE_US) == 0 {
                0
            } else {
                MINUTE_US
            };
        let mut hi = hi_all.min(r + LOOKBACK_US);
        if let Some(n) = next {
            hi = hi.min(n - MINUTE_US);
        }
        if lo <= hi {
            return true;
        }
    }
    false
}

/// 来源此刻是否上报中断（TERM-SILENT-NOW）：`latest` 是它在 `[t − 24 小时, t)` 内有按时间戳发送数的窗口中最晚者的起点。
pub fn silent_now(latest: i64, t: i64, grace: i64) -> bool {
    latest + 2 * MINUTE_US + grace <= t
}

#[cfg(test)]
mod tests;
