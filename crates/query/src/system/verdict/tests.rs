use std::collections::{BTreeMap, BTreeSet};

use super::*;
use crate::testing::FakeBackfill;

const G: i64 = 60_000_000;

fn ds() -> Dataset {
    Dataset::new("default", "app")
}

fn counts(pairs: &[(&str, u64)]) -> BTreeMap<String, u64> {
    pairs.iter().map(|(s, n)| ((*s).to_owned(), *n)).collect()
}

fn win(start: i64, sent_ts: &[(&str, u64)], durable: &[(&str, u64)]) -> CounterWindow {
    CounterWindow {
        window_start: start,
        window_end: start + MINUTE_US,
        received: 0,
        durable: 0,
        sent: BTreeMap::new(),
        durable_ts: durable.iter().map(|(_, n)| n).sum(),
        durable_ts_by_source: counts(durable),
        sent_ts: counts(sent_ts),
    }
}

/// 过了宽限期的时刻。
fn due(w: &CounterWindow) -> i64 {
    w.window_start + MINUTE_US + G
}

fn no_lookback() -> Result<BTreeSet<String>, ()> {
    panic!("前几档已能定出，不应回看")
}

fn sources(s: &[&str]) -> Result<BTreeSet<String>, ()> {
    Ok(s.iter().map(|s| (*s).to_owned()).collect())
}

fn verdict(w: &CounterWindow, t: i64, b: &FakeBackfill, look: &[&str]) -> Verdict {
    judge(&ds(), w, t, G, b, || sources(look)).unwrap().verdict
}

#[test]
fn pending_within_grace() {
    let w = win(0, &[("fw-1", 2)], &[("fw-1", 1)]);
    let b = FakeBackfill::new();
    let j = judge(&ds(), &w, due(&w) - 1, G, &b, no_lookback).unwrap();
    assert_eq!(j.verdict, Verdict::Pending);
    assert_eq!(
        judge(&ds(), &w, due(&w), G, &b, no_lookback)
            .unwrap()
            .verdict,
        Verdict::Missing
    );
}

/// 按来源比，缺失先于重复；有缺失或重复时不回看。
#[test]
fn per_source_and_combine() {
    let b = FakeBackfill::new();
    let missing = win(0, &[("a", 5), ("b", 3)], &[("a", 3), ("b", 4)]);
    let j = judge(&ds(), &missing, due(&missing), G, &b, no_lookback).unwrap();
    assert_eq!(j.verdict, Verdict::Missing);
    assert_eq!(j.missing, vec![("a".to_owned(), 2)]);
    let dup = win(0, &[("a", 3), ("b", 3)], &[("a", 3), ("b", 4)]);
    assert_eq!(
        judge(&ds(), &dup, due(&dup), G, &b, no_lookback)
            .unwrap()
            .verdict,
        Verdict::Duplicate
    );
    // 不在 durable_ts_by_source 中的受判来源按 0 计；未按来源记的确认数不参与
    let mut zero = win(0, &[("a", 1)], &[]);
    zero.durable_ts = 9;
    assert_eq!(verdict(&zero, due(&zero), &b, &[]), Verdict::Missing);
    let ok = win(0, &[("a", 2)], &[("a", 2)]);
    assert_eq!(verdict(&ok, due(&ok), &b, &[]), Verdict::Consistent);
}

/// 补发中的来源本应缺失、重复或在 W 上报中断的都按待定计；别的来源的缺失照常判出。
#[test]
fn backfill_hold() {
    let b = FakeBackfill::new();
    b.set(&ds(), "a");
    let w = win(0, &[("a", 5)], &[("a", 3)]);
    assert_eq!(
        judge(&ds(), &w, due(&w), G, &b, no_lookback)
            .unwrap()
            .verdict,
        Verdict::Pending
    );
    let two = win(0, &[("a", 5), ("b", 2)], &[("a", 3)]);
    let j = judge(&ds(), &two, due(&two), G, &b, no_lookback).unwrap();
    assert_eq!(j.verdict, Verdict::Missing);
    assert_eq!(
        j.missing,
        vec![("b".to_owned(), 2)],
        "按待定计的来源不算缺失"
    );
    // 一致的受判来源补发中也是一致；在 W 上报中断的来源补发中为待定
    let ok = win(0, &[("a", 3)], &[("a", 3)]);
    assert_eq!(verdict(&ok, due(&ok), &b, &[]), Verdict::Consistent);
    let quiet = win(0, &[("c", 1)], &[("c", 1)]);
    assert_eq!(verdict(&quiet, due(&quiet), &b, &["a"]), Verdict::Pending);
}

/// 没有受判来源：有按待定计的来源为待定，否则有在 W 上报中断的来源为上报中断，否则不可判定。
#[test]
fn no_judged_sources() {
    let b = FakeBackfill::new();
    let w = win(0, &[], &[("fw-1", 3)]);
    assert_eq!(verdict(&w, due(&w), &b, &[]), Verdict::Undeterminable);
    assert_eq!(verdict(&w, due(&w), &b, &["fw-1"]), Verdict::Silent);
    b.set(&ds(), "fw-1");
    assert_eq!(verdict(&w, due(&w), &b, &["fw-1"]), Verdict::Pending);
    // 一致，但另一个来源在 W 上报中断
    let c = win(0, &[("a", 1)], &[("a", 1)]);
    assert_eq!(
        verdict(&c, due(&c), &FakeBackfill::new(), &["a", "z"]),
        Verdict::Silent
    );
    // 回看读不出的错误原样交回
    assert_eq!(
        judge(&ds(), &w, due(&w), G, &b, || Err::<BTreeSet<String>, _>(
            "boom"
        )),
        Err("boom")
    );
}

/// `durable_ts_by_source` 已满 1000 个来源时，其中没有的来源不是受判来源。
#[test]
fn source_cap_not_judged() {
    let durable: Vec<(String, u64)> = (1..=1000).map(|i| (format!("s{i:04}"), 1)).collect();
    let refs: Vec<(&str, u64)> = durable.iter().map(|(s, n)| (s.as_str(), *n)).collect();
    let w = win(0, &[("extra", 5)], &refs);
    assert_eq!(judged_sources(&w).count(), 0);
    assert_eq!(
        verdict(&w, due(&w), &FakeBackfill::new(), &[]),
        Verdict::Undeterminable
    );
    let in_cap = win(0, &[("extra", 5), ("s0001", 1)], &refs);
    assert_eq!(
        judged_sources(&in_cap).map(|(s, _)| s).collect::<Vec<_>>(),
        ["s0001"]
    );
}

#[test]
fn lookback_and_reports() {
    let ws = vec![
        win(-LOOKBACK_US - MINUTE_US, &[("old", 1)], &[]),
        win(-LOOKBACK_US, &[("a", 1)], &[]),
        win(-MINUTE_US, &[("b", 1)], &[]),
        win(0, &[("c", 1)], &[]),
    ];
    assert_eq!(
        reported_before(&ws, 0),
        ["a", "b"].iter().map(|s| (*s).to_owned()).collect()
    );
    let by = reports_by_source(&ws);
    assert_eq!(by["a"], vec![-LOOKBACK_US]);
}

/// 在范围内某个 W 上报中断：只看连续上报之后的空档，W 要过了宽限期、在最后一次上报之后 24 小时以内。
#[test]
fn silent_in_range_gaps() {
    let m = MINUTE_US;
    let t = 100 * m;
    // 报了第 10、11 分钟，第 12 分钟起停报
    assert!(silent_in_range(&[10 * m, 11 * m], 0, 100 * m, t, G));
    // 范围只含第 10、11 分钟：W 须落在范围内
    assert!(!silent_in_range(&[10 * m, 11 * m], 10 * m, 12 * m, t, G));
    // 第 12 分钟还没过宽限期
    assert!(!silent_in_range(
        &[10 * m, 11 * m],
        0,
        100 * m,
        13 * m + G - 1,
        G
    ));
    assert!(silent_in_range(
        &[10 * m, 11 * m],
        0,
        100 * m,
        13 * m + G,
        G
    ));
    // 一直报到范围末尾：没有空档
    let all: Vec<i64> = (0..100).map(|i| i * m).collect();
    assert!(!silent_in_range(&all, 0, 100 * m, t, G));
    // 范围开始之前 24 小时内报过，范围内没报
    assert!(silent_in_range(&[-5 * m], 0, 10 * m, t, G));
    // 最后一次上报早于范围内每个 W 的 24 小时回看
    assert!(!silent_in_range(&[-LOOKBACK_US - m], 0, 10 * m, t, G));
    // 范围起点不在整分钟上：W 取起点之后的整分钟
    assert!(!silent_in_range(&[0], m + 1, 2 * m, t, G));
    assert!(silent_in_range(&[0], m - 1, 2 * m, t, G));
}

#[test]
fn silent_now_after_next_window_due() {
    let latest = 0;
    assert!(!silent_now(latest, 2 * MINUTE_US + G - 1, G));
    assert!(silent_now(latest, 2 * MINUTE_US + G, G));
}
