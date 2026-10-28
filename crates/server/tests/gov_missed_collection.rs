//! REQ-AMB-GOV-MISSED-COLLECTION 的系统测试：以子进程运行
//! `amberlog`（`log.format` 为缺省的 json），经 `_json`、`_sent` 写入，经 `_reconciliation`、`_ledger` 与 `/metrics`
//! 观察判定，逐行解析 stderr 里的稳定事件。对象存储用 `file://` 临时目录。
//!
//! 块的「场景共同前提」：`reconciliation.grace` 为 `1m`，场景开始时已开始受理请求超过 2 分钟，记录带
//! `X-Amberlog-Source: fw-1`，迟到的摄取与上报之后等 150 秒再断言判定（[`SETTLE`]）。
//!
//! 「转为缺失」的四个场景是长时场景：没有已有结论的窗口只在它结束加宽限期晚于基线时刻时
//! 才算转为缺失，块场景的 W 是此前第 20 分钟，实例要先运行 22 分钟。设了 `TESTBED_SOAK_HOURS`（跑满时长的长时运行）时
//! 照场景跑满；缺省跑缩短版：实例运行 2 分钟、W 取此前第 1 分钟（同样在基线之后到期），验证同一条逻辑。

mod common;

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use common::api::{
    HOUR_US, MINUTE_US, WalFault, create_stream, ingest, now_us, sent, sent_element,
};
use common::gov::{query, query_ok, write_counter_bytes, write_counters};
use common::missed::{
    GRACE_1M, Instance, SETTLE, events, hour_ago, in_window, ingest_at, ledger_row, minute_ago,
    report, ts_entry, verdict, verdict_of,
};
use common::{Fixture, Server};
use serde_json::{Value, json};

const FW1: Option<&str> = Some("fw-1");
const PREMISE: Duration = Duration::from_secs(120);

fn soak() -> bool {
    std::env::var_os("TESTBED_SOAK_HOURS").is_some()
}

/// 「转为缺失」场景的实例运行时长与窗口（此前第几分钟），见文件头。
fn turned_missing_setup() -> (Duration, i64) {
    if soak() {
        (Duration::from_secs(22 * 60), 20)
    } else {
        (PREMISE, 1)
    }
}

/// `/metrics` 中以 `series`（含标签）开头的那一行的取值。
fn metric(server: &Server, series: &str) -> Option<f64> {
    let text = server.get_auth("/metrics").text();
    text.lines()
        .find(|l| l.starts_with(series) && l[series.len()..].starts_with(' '))
        .and_then(|l| l.rsplit(' ').next())
        .and_then(|v| v.parse().ok())
}

fn windows_metric(server: &Server, stream: &str, verdict: &str) -> Option<f64> {
    metric(
        server,
        &format!(
            "amberlog_reconciliation_windows{{org=\"default\",stream=\"{stream}\",verdict=\"{verdict}\"}}"
        ),
    )
}

/// 场景常用的搭建：上报 W 为 5、摄取 3 条落在 W 的记录，等 150 秒后 W 的判定为 `missing`。
fn missing_window(i: &Instance, w: i64) {
    report(&i.server, "app", "fw-1", w, 5);
    ingest_at(&i.server, "app", FW1, &in_window(w, 3));
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

/// 新建实例并按场景共同前提等足 2 分钟，`app` 已存在。
fn premise() -> Instance {
    let i = Instance::start(&[GRACE_1M]);
    create_stream(&i.server, "app");
    i.wait_accepting(PREMISE);
    i
}

/// 场景「转为缺失时打一次事件」：上报 W 为 5、摄取 3 条落在 W 的记录，再等 5 分钟。返回 W。
fn event_missing_steps(i: &Instance, k: i64) -> i64 {
    let w = minute_ago(k);
    report(&i.server, "app", "fw-1", w, 5);
    ingest_at(&i.server, "app", FW1, &in_window(w, 3));
    std::thread::sleep(Duration::from_secs(300));
    w
}

fn missing_events_for(server: &Server, dataset: &str, w: i64) -> Vec<Value> {
    events(server, "reconciliation_missing")
        .into_iter()
        .filter(|e| e["dataset"] == dataset && e["window_start"] == w)
        .collect()
}

// ---------------------------------------------------------------------------
// 术语
// ---------------------------------------------------------------------------

#[test]
fn missed_collection_backfill_late_report_pending() {
    let i = premise();
    let w = minute_ago(20);
    missing_window(&i, w);
    report(&i.server, "app", "fw-1", w, 5);
    let at = Instant::now();
    let first = verdict_of(&i.server, "app", w);
    assert!(at.elapsed() < Duration::from_secs(20));
    assert_eq!(first, "pending");
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_source_cap_not_judged() {
    let i = premise();
    let h = hour_ago(3);
    let by_source: serde_json::Map<String, Value> =
        (1..=1000).map(|n| (format!("s{n:04}"), json!(1))).collect();
    write_counters(
        &i.fx,
        "app",
        h,
        &[ts_entry(
            h,
            1000,
            1000,
            json!({}),
            Some(json!({"extra": 5})),
            Some((1000, Value::Object(by_source))),
        )],
    );
    assert_eq!(verdict_of(&i.server, "app", h), "undeterminable");
    let hits = query_ok(
        &i.server,
        "SELECT ts_sent, ts_durable FROM _reconciliation WHERE dataset = 'default/app'",
        (h, h + MINUTE_US),
    );
    assert_eq!(hits.len(), 1);
    let row = hits[0].as_object().unwrap();
    assert!(
        !row.contains_key("ts_sent") && !row.contains_key("ts_durable"),
        "{row:?}"
    );
}

// ---------------------------------------------------------------------------
// 判定
// ---------------------------------------------------------------------------

#[test]
fn missed_collection_pending_within_grace() {
    let i = Instance::start(&[("reconciliation.grace", Some("\"2m\""))]);
    create_stream(&i.server, "app");
    i.wait_accepting(PREMISE);
    let now = now_us();
    let w = common::api::minute_of(now);
    ingest_at(&i.server, "app", FW1, &[now]);
    report(&i.server, "app", "fw-1", w, 2);
    // 自 W 结束起每秒查一次，直到返回 1 行（至多到 W 结束后 90 秒）
    let wait_until = |us: i64| {
        let left = us - now_us();
        if left > 0 {
            std::thread::sleep(Duration::from_micros(left as u64));
        }
    };
    wait_until(w + MINUTE_US);
    let first = loop {
        if let Some(v) = verdict(&i.server, "app", w) {
            break v;
        }
        assert!(
            now_us() < w + MINUTE_US + 90_000_000,
            "W 结束后 90 秒内没有对账行"
        );
        std::thread::sleep(Duration::from_secs(1));
    };
    assert_eq!(first, "pending");
    wait_until(w + MINUTE_US + 4 * MINUTE_US);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_missing_duplicate_consistent() {
    let i = premise();
    let (w1, w2, w3) = (minute_ago(20), minute_ago(19), minute_ago(18));
    missing_duplicate_consistent_steps(&i, (w1, w2, w3));
    let hits = query_ok(
        &i.server,
        "SELECT window_start, verdict FROM _reconciliation WHERE dataset = 'default/app'",
        (w1, w3 + MINUTE_US),
    );
    let got: BTreeMap<i64, String> = hits
        .iter()
        .map(|h| {
            (
                h["window_start"].as_i64().unwrap(),
                h["verdict"].as_str().unwrap().to_owned(),
            )
        })
        .collect();
    assert_eq!(
        got,
        BTreeMap::from([
            (w1, "missing".to_owned()),
            (w2, "consistent".to_owned()),
            (w3, "duplicate".to_owned()),
        ])
    );
}

fn missing_duplicate_consistent_steps(i: &Instance, (w1, w2, w3): (i64, i64, i64)) {
    let mut ts = in_window(w1, 3);
    ts.extend(in_window(w2, 2));
    ts.extend(in_window(w3, 2));
    ingest_at(&i.server, "app", FW1, &ts);
    for (w, n) in [(w1, 5), (w2, 2), (w3, 1)] {
        report(&i.server, "app", "fw-1", w, n);
    }
    std::thread::sleep(SETTLE);
}

#[test]
fn missed_collection_untagged_not_compared() {
    let i = premise();
    let w = minute_ago(20);
    ingest_at(&i.server, "app", FW1, &in_window(w, 2));
    ingest_at(&i.server, "app", None, &in_window(w, 5));
    ingest_at(&i.server, "app", Some("fw-2"), &in_window(w, 1));
    report(&i.server, "app", "fw-1", w, 2);
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "consistent");
    let hits = query_ok(
        &i.server,
        "SELECT ts_sent, ts_durable, ts_sent_sources FROM _reconciliation WHERE dataset = 'default/app'",
        (w, w + MINUTE_US),
    );
    assert_eq!(
        hits,
        [json!({"ts_sent": 2, "ts_durable": 2, "ts_sent_sources": "fw-1"})]
    );
}

#[test]
fn missed_collection_worst_source_wins() {
    let i = premise();
    let w = minute_ago(20);
    ingest_at(&i.server, "app", FW1, &in_window(w, 2));
    ingest_at(&i.server, "app", Some("fw-2"), &in_window(w, 1));
    report(&i.server, "app", "fw-1", w, 2);
    report(&i.server, "app", "fw-2", w, 3);
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_undeterminable_and_silent() {
    let i = premise();
    create_stream(&i.server, "web");
    let (w1, w2) = (minute_ago(20), minute_ago(19));
    ingest_at(&i.server, "app", FW1, &in_window(w1, 1));
    let r = sent(
        &i.server,
        "app",
        &format!("[{}]", sent_element("fw-1", w1, 1)),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    ingest_at(
        &i.server,
        "web",
        FW1,
        &[in_window(w1, 1), in_window(w2, 1)].concat(),
    );
    report(&i.server, "web", "fw-1", w1, 1);
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w1), "undeterminable");
    assert_eq!(verdict_of(&i.server, "web", w2), "silent");
}

#[test]
fn missed_collection_backfill_late_records_pending() {
    let i = Instance::start(&[("reconciliation.grace", Some("\"2m\""))]);
    create_stream(&i.server, "app");
    i.wait_accepting(Duration::from_secs(180));
    let w = minute_ago(20);
    report(&i.server, "app", "fw-1", w, 5);
    ingest_at(&i.server, "app", FW1, &in_window(w, 3));
    let last = Instant::now();
    let first = loop {
        if let Some(v) = verdict(&i.server, "app", w) {
            break v;
        }
        assert!(
            last.elapsed() < Duration::from_secs(90),
            "90 秒内没有对账行"
        );
        std::thread::sleep(Duration::from_secs(1));
    };
    assert_eq!(first, "pending");
    if let Some(left) = Duration::from_secs(180).checked_sub(last.elapsed()) {
        std::thread::sleep(left);
    }
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_backfill_per_source() {
    let i = premise();
    let (w1, w2) = (minute_ago(30), minute_ago(20));
    ingest_at(&i.server, "app", Some("fw-2"), &in_window(w1, 3));
    report(&i.server, "app", "fw-2", w1, 5);
    std::thread::sleep(SETTLE);
    // 此后 3 分钟内每 20 秒摄取 1 条落在 W2 的记录并上报；最后一次摄取之前查 W1
    let rounds = 9;
    for n in 1..=rounds {
        if n == rounds {
            assert_eq!(verdict_of(&i.server, "app", w1), "missing");
        }
        ingest_at(&i.server, "app", FW1, &[w2 + n * 1_000_000]);
        report(&i.server, "app", "fw-1", w2, n as u64);
        if n < rounds {
            std::thread::sleep(Duration::from_secs(20));
        }
    }
}

#[test]
fn missed_collection_backfill_restart_pending() {
    let mut i = premise();
    let w = minute_ago(20);
    missing_window(&i, w);
    i.server.sigkill();
    i.restart();
    assert_eq!(
        i.server.wait_readyz(200, Duration::from_secs(30)).status,
        200
    );
    let ready = Instant::now();
    let first = verdict_of(&i.server, "app", w);
    assert!(ready.elapsed() < Duration::from_secs(20));
    assert_eq!(first, "pending");
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_backfill_503_pending() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[GRACE_1M]);
    let i = Instance {
        fx,
        server,
        accepting: Instant::now(),
        overrides: vec![GRACE_1M],
    };
    create_stream(&i.server, "app");
    i.wait_accepting(PREMISE);
    let w = minute_ago(20);
    missing_window(&i, w);
    fault.inject("write");
    let r = ingest(&i.server, "app", r#"[{"a":1}]"#);
    fault.clear();
    r.assert_envelope(503, "unavailable");
    let at = Instant::now();
    let first = verdict_of(&i.server, "app", w);
    assert!(at.elapsed() < Duration::from_secs(20));
    assert_eq!(first, "pending");
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_late_fill_back_to_consistent() {
    let i = premise();
    let w = minute_ago(20);
    missing_window(&i, w);
    ingest_at(&i.server, "app", FW1, &[w + 10_000_000, w + 11_000_000]);
    std::thread::sleep(SETTLE);
    assert_eq!(verdict_of(&i.server, "app", w), "consistent");
}

/// `dir` 之下全部文件（相对路径 → 内容）。
fn snapshot_files(dir: &std::path::Path) -> BTreeMap<String, Vec<u8>> {
    fn walk(base: &std::path::Path, dir: &std::path::Path, out: &mut BTreeMap<String, Vec<u8>>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(base, &p, out);
            } else if let Ok(bytes) = std::fs::read(&p) {
                let rel = p.strip_prefix(base).unwrap().to_string_lossy().into_owned();
                out.insert(rel, bytes);
            }
        }
    }
    let mut out = BTreeMap::new();
    walk(dir, dir, &mut out);
    out
}

#[test]
fn missed_collection_not_persisted() {
    let i = premise();
    let w = minute_ago(20);
    report(&i.server, "app", "fw-1", w, 5);
    ingest_at(&i.server, "app", FW1, &in_window(w, 3));
    std::thread::sleep(SETTLE);
    let counters_dir = i.fx.store_dir().join("amberlog/default/app/counters");
    let system_dir = i.fx.store_dir().join("amberlog/_system");
    let counters = snapshot_files(&counters_dir);
    assert!(!counters.is_empty());
    let system_before: Vec<String> = snapshot_files(&system_dir).into_keys().collect();
    for n in 0..6 {
        assert_eq!(
            verdict_of(&i.server, "app", w),
            "missing",
            "第 {} 次",
            n + 1
        );
        if n < 5 {
            std::thread::sleep(Duration::from_secs(10));
        }
    }
    assert_eq!(snapshot_files(&counters_dir), counters);
    let new: Vec<String> = snapshot_files(&system_dir)
        .into_keys()
        .filter(|k| !system_before.contains(k) && k != "readyz-probe")
        .collect();
    assert!(new.is_empty(), "_system/ 下多出：{new:?}");
}

// ---------------------------------------------------------------------------
// 后台判定
// ---------------------------------------------------------------------------

#[test]
fn missed_collection_event_missing_once() {
    let (warm, k) = turned_missing_setup();
    let i = premise();
    i.wait_accepting(warm);
    let w = event_missing_steps(&i, k);
    let got = missing_events_for(&i.server, "default/app", w);
    assert_eq!(got.len(), 1, "{got:?}");
    let e = &got[0];
    assert_eq!(
        (&e["level"], &e["sources"], &e["missing"]),
        (&json!("warn"), &json!("fw-1"), &json!(2)),
        "{e}"
    );
}

#[test]
fn missed_collection_no_repeat_event_after_hold() {
    let (warm, k) = turned_missing_setup();
    let i = premise();
    i.wait_accepting(warm);
    let w = event_missing_steps(&i, k);
    assert_eq!(missing_events_for(&i.server, "default/app", w).len(), 1);
    let w2 = minute_ago(30);
    ingest_at(&i.server, "app", FW1, &in_window(w2, 1));
    report(&i.server, "app", "fw-1", w2, 1);
    std::thread::sleep(Duration::from_secs(300));
    assert_eq!(missing_events_for(&i.server, "default/app", w).len(), 1);
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_no_event_after_restart() {
    let (warm, k) = turned_missing_setup();
    let mut i = premise();
    i.wait_accepting(warm);
    let w = event_missing_steps(&i, k);
    assert_eq!(missing_events_for(&i.server, "default/app", w).len(), 1);
    common::gov::stop(&mut i.server);
    i.restart();
    std::thread::sleep(Duration::from_secs(300));
    assert!(missing_events_for(&i.server, "default/app", w).is_empty());
    assert_eq!(verdict_of(&i.server, "app", w), "missing");
}

#[test]
fn missed_collection_event_source_silent() {
    let i = premise();
    let (w1, w2) = (minute_ago(5), minute_ago(4));
    report(&i.server, "app", "fw-1", w1, 0);
    report(&i.server, "app", "fw-1", w2, 0);
    std::thread::sleep(Duration::from_secs(360));
    let got: Vec<Value> = events(&i.server, "source_silent")
        .into_iter()
        .filter(|e| e["dataset"] == "default/app" && e["source"] == "fw-1")
        .collect();
    assert_eq!(got.len(), 1, "{got:?}");
    assert_eq!(
        (&got[0]["level"], &got[0]["last_window_start"]),
        (&json!("warn"), &json!(w2))
    );
    let row = ledger_row(&i.server, "app", (w1, now_us()));
    assert_eq!(row["silent_sources"], 1, "{row}");
}

#[test]
fn missed_collection_gauge_values() {
    let i = premise();
    let (w1, w2, w3) = (minute_ago(20), minute_ago(19), minute_ago(18));
    missing_duplicate_consistent_steps(&i, (w1, w2, w3));
    std::thread::sleep(Duration::from_secs(90));
    assert_eq!(windows_metric(&i.server, "app", "missing"), Some(1.0));
    assert_eq!(windows_metric(&i.server, "app", "duplicate"), Some(1.0));
    let now = now_us();
    let hits = query_ok(
        &i.server,
        "SELECT count(*) AS n FROM _reconciliation WHERE dataset = 'default/app' AND verdict = 'consistent'",
        (now - 24 * HOUR_US, now),
    );
    let n = hits[0]["n"].as_f64().unwrap();
    assert_eq!(windows_metric(&i.server, "app", "consistent"), Some(n));
}

#[test]
fn missed_collection_gauge_series_init() {
    let mut i = premise();
    common::gov::stop(&mut i.server);
    i.restart();
    assert_eq!(
        i.server.wait_readyz(200, Duration::from_secs(30)).status,
        200
    );
    let text = i.server.get_auth("/metrics").text();
    let verdicts: Vec<&str> = text
        .lines()
        .filter(|l| {
            l.starts_with("amberlog_reconciliation_windows{") && l.contains("stream=\"app\"")
        })
        .filter_map(|l| l.split("verdict=\"").nth(1)?.split('"').next())
        .collect();
    assert_eq!(
        verdicts,
        [
            "pending",
            "consistent",
            "missing",
            "duplicate",
            "silent",
            "undeterminable"
        ],
        "{text}"
    );
}

#[test]
fn missed_collection_unreadable_object_isolated() {
    let (warm, k) = turned_missing_setup();
    let i = premise();
    create_stream(&i.server, "web");
    i.wait_accepting(warm);
    let w = minute_ago(k);
    report(&i.server, "web", "fw-1", w, 5);
    ingest_at(&i.server, "web", FW1, &in_window(w, 3));
    write_counter_bytes(&i.fx, "app", hour_ago(0), b"not json");
    let started = Instant::now();
    while started.elapsed() < Duration::from_secs(300) {
        assert_eq!(i.server.get("/readyz").status, 200);
        std::thread::sleep(Duration::from_secs(30));
    }
    assert_eq!(missing_events_for(&i.server, "default/web", w).len(), 1);
    let now = now_us();
    let r = query(
        &i.server,
        "SELECT * FROM app",
        (now - HOUR_US, now + 1_000_000),
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn missed_collection_catalog_no_background() {
    let lake: [common::Kv; 3] = [
        GRACE_1M,
        ("lifecycle.lake_watermark", Some("\"1h\"")),
        ("flush.interval", Some("\"1s\"")),
    ];
    let fx = Fixture::new();
    let mut s = fx.start(&lake);
    let h = hour_ago(3);
    ingest_at(&s, "app", FW1, &in_window(h + MINUTE_US, 1));
    // 等入湖：台账所在层出现冷层
    let entered = common::api::poll(
        Duration::from_secs(600),
        || {
            let hits = query_ok(
                &s,
                "SELECT tier FROM _ledger WHERE dataset = 'default/app'",
                (0, now_us() + 1_000_000),
            );
            hits.first()
                .and_then(|h| h["tier"].as_str().map(str::to_owned))
        },
        |t| matches!(t.as_deref(), Some("cold" | "hot_cold")),
    );
    assert!(entered.1, "app 没有入湖：{:?}", entered.0);
    common::gov::stop(&mut s);
    // 此前第 3 小时的某窗口按时间戳的发送数大于确认数（写在入湖之后、写入实例停掉之后）
    write_counters(
        &fx,
        "app",
        h,
        &[ts_entry(
            h,
            1,
            1,
            json!({}),
            Some(json!({"fw-1": 5})),
            Some((1, json!({"fw-1": 1}))),
        )],
    );
    let catalog = Server::start(fx.catalog(&lake));
    std::thread::sleep(Duration::from_secs(180));
    assert!(events(&catalog, "reconciliation_missing").is_empty());
}

#[test]
fn missed_collection_readyz_unaffected() {
    let i = premise();
    event_missing_steps(&i, 20);
    assert_eq!(i.server.get("/readyz").status, 200);
    assert_eq!(metric(&i.server, "amberlog_ready"), Some(1.0));
}

#[test]
fn missed_collection_ingest_unaffected() {
    let i = premise();
    event_missing_steps(&i, 20);
    let r = ingest(&i.server, "app", r#"[{"a":1}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let w = minute_ago(1);
    let body = json!([{"source": "fw-1", "window_start": w, "window_end": w + MINUTE_US,
                       "sent": 1, "basis": "timestamp"}]);
    let r = sent(&i.server, "app", &body.to_string());
    assert_eq!(r.status, 200, "{}", r.text());
}
