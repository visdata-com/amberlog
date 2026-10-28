//! REQ-AMB-GOV-RECONCILIATION 的系统测试：以子进程启动 `amberlog`，测试台架直接写入或改坏计数对象，经 `_search`
//! 查询系统表 `_reconciliation`。对象存储用 `file://` 临时目录；
//! 「对象存储停止」用 MinIO 台架加本进程里的 TCP 转发（见 `common::s3`）。

mod common;

use std::time::Duration;

use common::api::{HOUR_US, MINUTE_US, create_stream, ingest_ok, now_us, poll};
use common::gov::{
    committed, entry, first_bucket_manifest, query, query_ok, write_counter_bytes, write_counters,
};
use common::s3::S3Instance;
use common::{Fixture, Server};
use serde_json::{Value, json};

/// 2026-08-22T10:00:00Z：被测进程不会为这个小时写出计数对象。
const H: i64 = 1_787_392_800_000_000;

/// 启动并以空的 `_sent` 上报建出各 stream。
fn start(streams: &[&str]) -> (Fixture, Server) {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    for st in streams {
        create_stream(&s, st);
    }
    (fx, s)
}

/// 恰覆盖分钟窗口 `w` 的时间范围。
fn minute(w: i64) -> (i64, i64) {
    (w, w + MINUTE_US)
}

const APP: &str = "SELECT * FROM _reconciliation WHERE dataset = 'default/app'";

// ---------------------------------------------------------------------------
// three-way
// ---------------------------------------------------------------------------

#[test]
fn gov_reconciliation_three_way_gaps() {
    let (fx, s) = start(&["app"]);
    write_counters(
        &fx,
        "app",
        H,
        &[entry(H, 1_000_000, 999_997, json!({"vector-1": 1_000_000}))],
    );
    let hits = query_ok(&s, APP, minute(H));
    assert_eq!(hits.len(), 1, "{hits:?}");
    let h = &hits[0];
    assert_eq!(
        (&h["sent"], &h["received"], &h["durable"]),
        (&json!(1_000_000), &json!(1_000_000), &json!(999_997))
    );
    assert_eq!(
        (&h["gap_sent_received"], &h["gap_received_durable"]),
        (&json!(0), &json!(3))
    );
    assert_eq!(h["sent_sources"], "vector-1");
}

#[test]
fn gov_reconciliation_sent_missing_is_null() {
    let (fx, s) = start(&["app"]);
    write_counters(&fx, "app", H, &[entry(H, 10, 10, json!({}))]);
    let hits = query_ok(&s, APP, minute(H));
    assert_eq!(hits.len(), 1, "{hits:?}");
    let h = hits[0].as_object().unwrap();
    for k in ["sent", "sent_sources", "gap_sent_received"] {
        assert!(!h.contains_key(k), "{k}: {h:?}");
    }
    assert_eq!(h["gap_received_durable"], 0);
}

#[test]
fn gov_reconciliation_sent_sources_sum() {
    let (fx, s) = start(&["app"]);
    write_counters(
        &fx,
        "app",
        H,
        &[entry(
            H,
            4,
            4,
            json!({"vector-1": 3, "Fluent": 0, "agent-b": 2}),
        )],
    );
    let hits = query_ok(
        &s,
        "SELECT sent, sent_sources, gap_sent_received FROM _reconciliation WHERE dataset = 'default/app'",
        minute(H),
    );
    assert_eq!(
        hits,
        [json!({"sent": 5, "sent_sources": "Fluent,agent-b,vector-1", "gap_sent_received": 1})]
    );
}

// ---------------------------------------------------------------------------
// rows
// ---------------------------------------------------------------------------

#[test]
fn gov_reconciliation_rows_in_range() {
    let (fx, s) = start(&["app"]);
    let (w1, w2, w3) = (H, H + MINUTE_US, H + 2 * MINUTE_US);
    write_counters(
        &fx,
        "app",
        H,
        &[
            entry(w1, 1, 1, json!({})),
            entry(w2, 1, 1, json!({})),
            entry(w3, 1, 1, json!({})),
        ],
    );
    let hits = query_ok(
        &s,
        "SELECT window_start FROM _reconciliation WHERE dataset = 'default/app'",
        (H - HOUR_US, w2 + MINUTE_US),
    );
    assert_eq!(
        hits,
        [json!({"window_start": w2}), json!({"window_start": w1})]
    );
}

#[test]
fn gov_reconciliation_default_order() {
    let (fx, s) = start(&["app", "web"]);
    let (w1, w2) = (H, H + MINUTE_US);
    for st in ["app", "web"] {
        write_counters(
            &fx,
            st,
            H,
            &[entry(w1, 1, 1, json!({})), entry(w2, 1, 1, json!({}))],
        );
    }
    let pairs = |hits: Vec<Value>| -> Vec<(String, i64)> {
        hits.iter()
            .map(|h| {
                (
                    h["dataset"].as_str().unwrap().to_owned(),
                    h["window_start"].as_i64().unwrap(),
                )
            })
            .collect()
    };
    let p = |d: &str, w: i64| (format!("default/{d}"), w);
    let range = (H, H + HOUR_US);
    assert_eq!(
        pairs(query_ok(
            &s,
            "SELECT dataset, window_start FROM _reconciliation",
            range
        )),
        [p("app", w2), p("web", w2), p("app", w1), p("web", w1)]
    );
    assert_eq!(
        pairs(query_ok(
            &s,
            "SELECT dataset, window_start FROM _reconciliation ORDER BY dataset",
            range
        )),
        [p("app", w2), p("app", w1), p("web", w2), p("web", w1)]
    );
}

#[test]
fn gov_reconciliation_named_dataset_absent() {
    let (fx, s) = start(&["app"]);
    let now = now_us();
    write_counters(
        &fx,
        "app",
        now - now.rem_euclid(HOUR_US),
        &[entry(now - now.rem_euclid(MINUTE_US), 1, 1, json!({}))],
    );
    let hits = query_ok(
        &s,
        "SELECT * FROM _reconciliation WHERE dataset = 'default/ghost'",
        (now - HOUR_US, now + 1),
    );
    assert_eq!(hits, Vec::<Value>::new());
}

#[test]
fn gov_reconciliation_disabled_dataset_rows() {
    let fx = Fixture::new();
    committed(&fx, &[("app", r#"[{"a":1}]"#)]);
    write_counters(&fx, "app", H, &[entry(H, 1, 1, json!({}))]);
    fx.dataset("app").remove(&first_bucket_manifest(&fx, "app"));
    let s = fx.start(&[common::api::FLUSH_1H]);
    // 覆盖该桶的查询使 app 进入 manifest 不可读情形
    common::api::search_resp(&s, "SELECT * FROM app").assert_envelope(500, "internal");
    let hits = query_ok(
        &s,
        "SELECT dataset, received FROM _reconciliation WHERE dataset = 'default/app'",
        minute(H),
    );
    assert_eq!(hits, [json!({"dataset": "default/app", "received": 1})]);
}

// ---------------------------------------------------------------------------
// history
// ---------------------------------------------------------------------------

#[test]
fn gov_reconciliation_history() {
    let (fx, s) = start(&["app"]);
    let then = now_us() - 72 * HOUR_US;
    let w = then - then.rem_euclid(MINUTE_US);
    write_counters(
        &fx,
        "app",
        w - w.rem_euclid(HOUR_US),
        &[entry(w, 100, 97, json!({}))],
    );
    // 此后持续摄取：当前小时的计数随之写出（received 与 durable 相等）
    for i in 0..3 {
        ingest_ok(&s, "app", &format!(r#"[{{"i":{i}}}]"#));
    }
    let hits = query_ok(
        &s,
        "SELECT received, durable, gap_received_durable FROM _reconciliation WHERE dataset = 'default/app'",
        minute(w),
    );
    assert_eq!(
        hits,
        [json!({"received": 100, "durable": 97, "gap_received_durable": 3})]
    );
}

#[test]
fn gov_reconciliation_reads_fresh() {
    let (fx, s) = start(&["app"]);
    let sql = "SELECT sent FROM _reconciliation WHERE dataset = 'default/app'";
    write_counters(&fx, "app", H, &[entry(H, 1, 1, json!({"vector-1": 10}))]);
    assert_eq!(query_ok(&s, sql, minute(H)), [json!({"sent": 10})]);
    write_counters(&fx, "app", H, &[entry(H, 1, 1, json!({"vector-1": 12}))]);
    assert_eq!(query_ok(&s, sql, minute(H)), [json!({"sent": 12})]);
}

// ---------------------------------------------------------------------------
// read-failure
// ---------------------------------------------------------------------------

#[test]
fn gov_reconciliation_counter_object_corrupt() {
    let (fx, s) = start(&["app", "web"]);
    write_counters(&fx, "web", H, &[entry(H, 1, 1, json!({}))]);
    write_counter_bytes(&fx, "app", H, b"{not json");
    let range = (H, H + HOUR_US);
    query(&s, APP, range).assert_envelope(500, "internal");
    let r = query(
        &s,
        "SELECT * FROM _reconciliation WHERE dataset = 'default/web'",
        range,
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn gov_reconciliation_counter_object_version_too_high() {
    let (fx, s) = start(&["app"]);
    let mut obj = common::gov::counter_object("app", &[entry(H, 1, 1, json!({}))]);
    obj["format_version"] = json!(2);
    write_counter_bytes(&fx, "app", H, obj.to_string().as_bytes());
    query(&s, APP, (H, H + HOUR_US)).assert_envelope(500, "internal");
}

#[test]
fn gov_reconciliation_sent_overflow() {
    let (fx, s) = start(&["app"]);
    write_counters(
        &fx,
        "app",
        H,
        &[entry(H, 1, 1, json!({"a": i64::MAX, "b": 1}))],
    );
    query(&s, APP, minute(H)).assert_envelope(500, "internal");
}

#[test]
fn gov_reconciliation_counter_over_int64() {
    let (fx, s) = start(&["app"]);
    let over = i64::MAX as u64 + 1;
    let (w1, w2, w3) = (H, H + MINUTE_US, H + 2 * MINUTE_US);
    write_counters(
        &fx,
        "app",
        H,
        &[
            entry(w1, over, 1, json!({})),
            entry(w2, 1, over, json!({})),
            entry(w3, 1, 1, json!({})),
        ],
    );
    // 落在查询范围内的行 received 或 durable 超出 Int64：整条 500，不截断、不略去该行
    query(&s, APP, minute(w1)).assert_envelope(500, "internal");
    query(&s, APP, minute(w2)).assert_envelope(500, "internal");
    // 按行判定：同一计数对象里范围外的行超出 Int64 不影响范围内的行
    let hits = query_ok(&s, APP, minute(w3));
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        (&hits[0]["received"], &hits[0]["durable"]),
        (&json!(1), &json!(1))
    );
}

#[test]
fn gov_reconciliation_store_unavailable() {
    let inst = S3Instance::new();
    let s = inst.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let now = now_us();
    let hour = now - now.rem_euclid(HOUR_US);
    let chain = common::store::Remote::new(inst.store(), "app");
    let rel = amberlog_ingest::counters::counters_path(hour);
    let (_, written) = poll(
        Duration::from_secs(20),
        || chain.read(&rel),
        Option::is_some,
    );
    assert!(written, "20 秒内未见 app 的计数对象");
    inst.relay.stop();
    query(&s, APP, (now - HOUR_US, now + 1)).assert_envelope(503, "unavailable");
}

// ---------------------------------------------------------------------------
// verdict（REQ-AMB-GOV-RECONCILIATION-V3）
// ---------------------------------------------------------------------------

/// 以 `reconciliation.grace` 为 `1m` 启动、等到开始受理请求超过 2 分钟（越过「刚重启」的补发中），`app` 已存在。
fn judged_instance() -> common::missed::Instance {
    let i = common::missed::Instance::start(&[common::missed::GRACE_1M]);
    create_stream(&i.server, "app");
    i.wait_accepting(Duration::from_secs(120));
    i
}

#[test]
fn gov_reconciliation_verdict_lookback_corrupt() {
    let i = judged_instance();
    let h = common::missed::hour_ago(3);
    write_counters(&i.fx, "app", h, &[entry(h, 10, 10, json!({}))]);
    write_counter_bytes(&i.fx, "app", h - HOUR_US, b"not json");
    query(
        &i.server,
        "SELECT window_start, verdict FROM _reconciliation WHERE dataset = 'default/app'",
        minute(h),
    )
    .assert_envelope(500, "internal");
}

#[test]
fn gov_reconciliation_verdict_missing_consistent() {
    use common::missed::{in_window, ingest_at, minute_ago, report};
    let i = judged_instance();
    let (w1, w2) = (minute_ago(20), minute_ago(19));
    ingest_at(
        &i.server,
        "app",
        Some("fw-1"),
        &[in_window(w1, 3), in_window(w2, 2)].concat(),
    );
    report(&i.server, "app", "fw-1", w1, 5);
    report(&i.server, "app", "fw-1", w2, 2);
    std::thread::sleep(common::missed::SETTLE);
    let hits = query_ok(
        &i.server,
        "SELECT window_start, ts_sent, ts_sent_sources, ts_durable, verdict FROM _reconciliation WHERE dataset = 'default/app'",
        (w1, w2 + MINUTE_US),
    );
    let row = |w: i64| {
        hits.iter()
            .find(|h| h["window_start"] == w)
            .unwrap_or_else(|| panic!("没有 {w} 一行：{hits:?}"))
    };
    assert_eq!(
        row(w1),
        &json!({"window_start": w1, "ts_sent": 5, "ts_sent_sources": "fw-1", "ts_durable": 3, "verdict": "missing"})
    );
    let r2 = row(w2);
    assert_eq!(
        (&r2["ts_sent"], &r2["ts_durable"], &r2["verdict"]),
        (&json!(2), &json!(2), &json!("consistent"))
    );
}

#[test]
fn gov_reconciliation_verdict_pending_within_grace() {
    use common::missed::{SOURCE, report};
    let i = common::missed::Instance::start(&[("reconciliation.grace", Some("\"5m\""))]);
    create_stream(&i.server, "app");
    let now = now_us();
    let w = common::api::minute_of(now);
    let body = json!([{"_timestamp": common::gov::rfc3339(now)}]).to_string();
    let r = common::api::ingest_with(&i.server, "app", &[(SOURCE, "fw-1")], body.as_bytes());
    assert_eq!(r.status, 200, "{}", r.text());
    report(&i.server, "app", "fw-1", w, 1);
    let (hits, ok) = poll(
        Duration::from_secs(60),
        || {
            query_ok(
                &i.server,
                "SELECT verdict FROM _reconciliation WHERE dataset = 'default/app'",
                minute(w),
            )
        },
        |h| h.len() == 1,
    );
    assert!(ok, "60 秒内没有对账行：{hits:?}");
    assert_eq!(hits[0]["verdict"], "pending");
}

#[test]
fn gov_reconciliation_verdict_undeterminable_without_ts() {
    let i = judged_instance();
    let h = common::missed::hour_ago(3);
    write_counters(
        &i.fx,
        "app",
        h,
        &[entry(h, 10, 10, json!({"vector-1": 10}))],
    );
    let hits = query_ok(&i.server, APP, minute(h));
    assert_eq!(hits.len(), 1, "{hits:?}");
    let row = hits[0].as_object().unwrap();
    assert_eq!(
        (&row["verdict"], &row["sent"]),
        (&json!("undeterminable"), &json!(10))
    );
    for k in ["ts_sent", "ts_sent_sources", "ts_durable"] {
        assert!(!row.contains_key(k), "{k}: {row:?}");
    }
}

#[test]
fn gov_reconciliation_ts_columns_multi_source() {
    let i = judged_instance();
    let h = common::missed::hour_ago(3);
    write_counters(
        &i.fx,
        "app",
        h,
        &[common::missed::ts_entry(
            h,
            9,
            9,
            json!({}),
            Some(json!({"fw-b": 2, "Fw-c": 3})),
            Some((9, json!({"fw-b": 2}))),
        )],
    );
    let hits = query_ok(
        &i.server,
        "SELECT ts_sent, ts_sent_sources, ts_durable, verdict FROM _reconciliation WHERE dataset = 'default/app'",
        minute(h),
    );
    assert_eq!(
        hits,
        [
            json!({"ts_sent": 5, "ts_sent_sources": "Fw-c,fw-b", "ts_durable": 2, "verdict": "missing"})
        ]
    );
}

#[test]
fn gov_reconciliation_ts_sent_overflow() {
    let i = judged_instance();
    let h = common::missed::hour_ago(3);
    write_counters(
        &i.fx,
        "app",
        h,
        &[common::missed::ts_entry(
            h,
            1,
            1,
            json!({}),
            Some(json!({"a": 9_223_372_036_854_775_807i64, "b": 1})),
            None,
        )],
    );
    query(&i.server, APP, minute(h)).assert_envelope(500, "internal");
}
