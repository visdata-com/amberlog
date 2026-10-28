//! REQ-AMB-GOV-LEDGER 的系统测试：以子进程启动 `amberlog`，经 `_json`、`_sent` 写入，测试台架直接删改快照、
//! 桶 manifest 与计数对象，经 `_search` 查询系统表 `_ledger`。
//! 对象存储用 `file://` 临时目录；「对象存储停止」用 MinIO 台架加本进程里的 TCP 转发（见 `common::s3`）。

mod common;

use std::time::Duration;

use common::api::{
    FLUSH_1H, HOUR_US, MINUTE_US, counter_file, ingest_ok, now_us, poll, search_resp, sent,
    sent_element,
};
use common::gov::{
    committed, entry, first_bucket_manifest, query, query_ok, rfc3339, write_counter_bytes,
    write_counters,
};
use common::s3::S3Instance;
use common::store::FLUSH_1S;
use common::{Fixture, Server};
use serde_json::{Value, json};

/// 覆盖此刻的时间范围：此前 1 小时到此后 1 秒。
fn around_now() -> (i64, i64) {
    let now = now_us();
    (now - HOUR_US, now + 1_000_000)
}

/// `n` 条 `_timestamp` 为 `ts` 的记录。
fn records(ts: i64, n: usize) -> String {
    let one = json!({"_timestamp": rfc3339(ts)});
    serde_json::to_string(&vec![one; n]).unwrap()
}

/// 在 `hits` 中找 `dataset` 为 `d` 的元素。
fn row<'a>(hits: &'a [Value], d: &str) -> &'a Value {
    hits.iter()
        .find(|h| h["dataset"] == d)
        .unwrap_or_else(|| panic!("没有 {d} 一行：{hits:?}"))
}

/// 以同一配置重启并等 `/readyz` 为 200。
fn restart(fx: &Fixture, overrides: &[common::Kv]) -> Server {
    let s = fx.start(overrides);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    s
}

#[test]
fn rfc3339_known_points() {
    assert_eq!(rfc3339(0), "1970-01-01T00:00:00.000000Z");
    assert_eq!(
        rfc3339(1_787_392_800_000_001),
        "2026-08-22T10:00:00.000001Z"
    );
    assert_eq!(
        rfc3339(251_824_464_000_000_000),
        "9950-01-01T00:00:00.000000Z"
    );
}

// ---------------------------------------------------------------------------
// rows
// ---------------------------------------------------------------------------

#[test]
fn gov_ledger_auto_appears() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    ingest_ok(&s, "fresh", &records(now_us(), 100));
    let sql = "SELECT * FROM _ledger WHERE dataset = 'default/fresh'";
    let (hits, done) = poll(
        Duration::from_secs(10),
        || query_ok(&s, sql, around_now()),
        |hits| hits.first().is_some_and(|h| h["record_count"] == 100),
    );
    assert!(done, "10 秒内 record_count 未到 100：{hits:?}");
    assert_eq!(hits.len(), 1);
    let h = hits[0].as_object().unwrap();
    assert_eq!(
        (&h["status"], &h["tier"], &h["reconciliation"]),
        (&json!("active"), &json!("hot"), &json!("two_way"))
    );
    for k in ["time_min", "time_max", "size_bytes"] {
        assert!(h.contains_key(k), "{k}: {h:?}");
    }
    for k in ["retention", "expires_at", "sources"] {
        assert!(!h.contains_key(k), "{k}: {h:?}");
    }
}

#[test]
fn gov_ledger_default_order() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1H]);
    for st in ["web", "app", "db"] {
        ingest_ok(&s, st, r#"[{"a":1}]"#);
    }
    let names = |sql: &str| -> Vec<Value> {
        query_ok(&s, sql, around_now())
            .iter()
            .map(|h| h["dataset"].clone())
            .collect()
    };
    let want = [
        json!("default/app"),
        json!("default/db"),
        json!("default/web"),
    ];
    assert_eq!(names("SELECT dataset FROM _ledger"), want);
    assert_eq!(names("SELECT dataset FROM _ledger ORDER BY status"), want);
}

// ---------------------------------------------------------------------------
// tier
// ---------------------------------------------------------------------------

#[test]
fn gov_ledger_tier_pending_lake() {
    let fx = Fixture::new();
    // 冷层写入失败（故障注入）：入湖不能完成，桶留在热层（REQ-AMB-GOV-LEDGER-V2）
    let (s, faults) =
        fx.start_with_store_faults(&[FLUSH_1S, ("lifecycle.lake_watermark", Some("\"1h\""))]);
    faults.set("write _iceberg/ 403\n");
    let now = now_us();
    let h3 = now - now.rem_euclid(HOUR_US) - 3 * HOUR_US;
    ingest_ok(&s, "app", &records(h3 + MINUTE_US, 40));
    ingest_ok(&s, "app", &records(now, 60));
    fx.dataset("app").wait_records(100, Duration::from_secs(20));
    let hits = query_ok(
        &s,
        "SELECT tier, record_count, pending_lake_record_count FROM _ledger WHERE dataset = 'default/app'",
        around_now(),
    );
    assert_eq!(
        hits,
        [json!({"tier": "hot_pending_lake", "record_count": 100, "pending_lake_record_count": 40})]
    );
}

/// `n` 条 `_timestamp` 自 `ts` 起每条加 `step` 微秒的记录。
fn spread(ts: i64, n: i64, step: i64) -> String {
    let v: Vec<Value> = (0..n)
        .map(|i| json!({"_timestamp": rfc3339(ts + i * step)}))
        .collect();
    serde_json::to_string(&v).unwrap()
}

#[test]
fn gov_ledger_tier_cross_tier() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1S, ("lifecycle.lake_watermark", Some("\"1h\""))]);
    let now = now_us();
    let h3 = now - now.rem_euclid(HOUR_US) - 3 * HOUR_US;
    let t1 = h3 + MINUTE_US;
    ingest_ok(&s, "app", &spread(t1, 40, 1_000_000));
    let t2 = now_us() - 1_000_000;
    ingest_ok(&s, "app", &spread(t2 - 59_000, 60, 1_000));
    ingest_ok(&s, "old", &spread(h3 + MINUTE_US, 10, 1_000_000));
    let acked = std::time::Instant::now();
    let sql = "SELECT dataset, tier, record_count, pending_lake_record_count, time_min, time_max FROM _ledger";
    let (hits, done) = poll(
        Duration::from_secs(180).saturating_sub(acked.elapsed()),
        || query_ok(&s, sql, around_now()),
        |hits| {
            hits.iter()
                .any(|h| h["dataset"] == "default/old" && h["tier"] == "cold")
                && hits
                    .iter()
                    .any(|h| h["dataset"] == "default/app" && h["pending_lake_record_count"] == 0)
        },
    );
    assert!(done, "180 秒内未入湖完成：{hits:?}");
    assert_eq!(hits.len(), 2, "{hits:?}");
    let app = row(&hits, "default/app");
    assert_eq!(app["tier"], "hot_cold", "{app}");
    assert_eq!(app["record_count"], 100);
    assert_eq!(app["time_min"], t1);
    assert_eq!(app["time_max"], t2);
    assert_eq!(row(&hits, "default/old")["record_count"], 10);
}

// ---------------------------------------------------------------------------
// retention
// ---------------------------------------------------------------------------

#[test]
fn gov_ledger_retention_expires_at() {
    let fx = Fixture::new();
    // 此刻前 2 天：保留期内、未越过默认入湖水位 72 小时（REQ-AMB-GOV-LEDGER-V2）
    let s = fx.start(&[FLUSH_1S, ("retention.period", Some("\"30d\""))]);
    let t = now_us() - 2 * 24 * HOUR_US;
    let body = json!([{"_timestamp": rfc3339(t)}, {"_timestamp": rfc3339(t + HOUR_US)}]);
    ingest_ok(&s, "app", &body.to_string());
    fx.dataset("app").wait_records(2, Duration::from_secs(20));
    let range = (t, now_us() + 1_000_000);
    let hits = query_ok(
        &s,
        "SELECT dataset, retention, expires_at FROM _ledger WHERE dataset = 'default/app'",
        range,
    );
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/app", "retention": "30d", "expires_at": t + 2_592_000_000_000})
        ]
    );
}

#[test]
fn gov_ledger_expires_at_overflow_null() {
    let fx = Fixture::new();
    let s = fx.start(&[
        FLUSH_1S,
        ("retention.period", Some("\"36500d\"")),
        ("ingest.max_future", Some("\"3000000d\"")),
    ]);
    let t = 251_824_464_000_000_000; // 9950-01-01T00:00:00Z
    ingest_ok(&s, "far", &records(t, 1));
    fx.dataset("far").wait_records(1, Duration::from_secs(20));
    let hits = query_ok(
        &s,
        "SELECT retention, time_min, expires_at FROM _ledger WHERE dataset = 'default/far'",
        (t, t + 1),
    );
    assert_eq!(hits, [json!({"retention": "36500d", "time_min": t})]);
}

// ---------------------------------------------------------------------------
// sources
// ---------------------------------------------------------------------------

#[test]
fn gov_ledger_sources_three_way() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let now = now_us();
    let w = now - now.rem_euclid(MINUTE_US);
    let body = format!(
        "[{},{}]",
        sent_element("vector-1", w, 3),
        sent_element("fluentbit-2", w, 4)
    );
    let r = sent(&s, "app", &body);
    assert_eq!(r.status, 200, "{}", r.text());
    // 计数对象按写出节拍写出后才可见
    let sql = "SELECT sources, reconciliation FROM _ledger WHERE dataset = 'default/app'";
    let (hits, done) = poll(
        Duration::from_secs(20),
        || query_ok(&s, sql, around_now()),
        |hits| hits.first().is_some_and(|h| h.get("sources").is_some()),
    );
    assert!(done, "{hits:?}");
    assert_eq!(
        hits,
        [json!({"sources": "fluentbit-2,vector-1", "reconciliation": "three_way"})]
    );
}

#[test]
fn gov_ledger_time_range_scope() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    fx.dataset("app").wait_records(1, Duration::from_secs(20));
    let now = now_us();
    let h = now - now.rem_euclid(HOUR_US) - 3 * HOUR_US;
    write_counters(
        &fx,
        "app",
        h,
        &[entry(h + MINUTE_US, 0, 0, json!({"vector-1": 5}))],
    );
    let sql =
        "SELECT record_count, sources, reconciliation FROM _ledger WHERE dataset = 'default/app'";
    let recent = query_ok(&s, sql, (now - 30 * MINUTE_US, now + 1_000_000));
    assert_eq!(
        recent,
        [json!({"record_count": 1, "reconciliation": "two_way"})]
    );
    let then = query_ok(&s, sql, (h, h + HOUR_US));
    assert_eq!(
        then,
        [json!({"record_count": 1, "sources": "vector-1", "reconciliation": "three_way"})]
    );
}

// ---------------------------------------------------------------------------
// disabled-and-failure
// ---------------------------------------------------------------------------

#[test]
fn gov_ledger_disabled_dataset_status() {
    let fx = Fixture::new();
    committed(&fx, &[("app", r#"[{"a":1}]"#), ("web", r#"[{"a":1}]"#)]);
    fx.dataset("app").remove(&first_bucket_manifest(&fx, "app"));
    let s = restart(&fx, &[FLUSH_1H]);
    search_resp(&s, "SELECT * FROM app").assert_envelope(500, "internal");
    let hits = query_ok(&s, "SELECT * FROM _ledger", around_now());
    let app = row(&hits, "default/app").as_object().unwrap();
    assert_eq!(app["status"], "disabled_manifest_unreadable");
    assert!(
        !app.contains_key("record_count") && !app.contains_key("tier"),
        "{app:?}"
    );
    assert_eq!(row(&hits, "default/web")["status"], "active");
}

/// 进程运行期间写入版本 3 的快照、不对 `app` 发请求，直接查台账：停用由台账自己找链尾、读快照触发，而不是重启时
/// 读入基线所致。热层巡检推迟到 24 小时后，排除其他读取路径；
/// 查询前后各读一次 `/metrics`，确认停用是这次查询读到的。
#[test]
fn gov_ledger_disabled_format_version() {
    let fx = Fixture::new();
    let s = fx.start(&[
        ("flush.max_records", Some("1")),
        ("hot_fsck.interval", Some("\"24h\"")),
    ]);
    for st in ["app", "web"] {
        ingest_ok(&s, st, r#"[{"a":1}]"#);
    }
    for st in ["app", "web"] {
        let ds = fx.dataset(st);
        let (_, done) = poll(
            Duration::from_secs(10),
            || {
                ds.current()
                    .map(|n| ds.snapshot(n)["summary"]["record_count"].clone())
            },
            |c| *c == Some(json!(1)),
        );
        assert!(done, "{st} 10 秒内未提交");
    }
    // 模拟更高版本的写方：写出 format_version 为 3 的下一个快照，使它成为链尾
    fx.dataset("app")
        .append_snapshot(|next| next["format_version"] = json!(3));
    let disabled = |s: &Server| -> Vec<String> {
        let r = s.get_auth("/metrics");
        assert_eq!(r.status, 200, "{}", r.text());
        r.text()
            .lines()
            .filter(|l| l.starts_with("amberlog_dataset_disabled{") && l.contains("stream=\"app\""))
            .map(str::to_owned)
            .collect()
    };
    assert_eq!(
        disabled(&s),
        Vec::<String>::new(),
        "查询台账之前 app 未停用"
    );
    let hits = query_ok(
        &s,
        "SELECT dataset, status, record_count FROM _ledger",
        around_now(),
    );
    assert_eq!(
        hits,
        [
            json!({"dataset": "default/app", "status": "disabled_format_version"}),
            json!({"dataset": "default/web", "status": "active", "record_count": 1}),
        ]
    );
    assert_eq!(
        disabled(&s),
        ["amberlog_dataset_disabled{org=\"default\",stream=\"app\",state=\"format_version\"} 1"]
    );
}

#[test]
fn gov_ledger_no_bucket_manifest_read() {
    let fx = Fixture::new();
    committed(&fx, &[("app", &records(now_us(), 100))]);
    fx.dataset("app").remove(&first_bucket_manifest(&fx, "app"));
    // 热层体检会读可达桶 manifest：测试期间不让它运行
    let s = restart(&fx, &[FLUSH_1H, ("hot_fsck.interval", Some("\"24h\""))]);
    let sql = "SELECT status, record_count FROM _ledger WHERE dataset = 'default/app'";
    for _ in 0..2 {
        assert_eq!(
            query_ok(&s, sql, around_now()),
            [json!({"status": "active", "record_count": 100})]
        );
    }
}

#[test]
fn gov_ledger_counter_object_corrupt_row_only() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1H]);
    let hour = {
        let now = now_us();
        now - now.rem_euclid(HOUR_US)
    };
    for st in ["app", "web"] {
        ingest_ok(&s, st, r#"[{"a":1}]"#);
    }
    for st in ["app", "web"] {
        let (_, written) = poll(
            Duration::from_secs(20),
            || counter_file(&fx, st, hour).exists(),
            |e| *e,
        );
        assert!(written, "20 秒内未见 {st} 的计数对象");
    }
    write_counter_bytes(&fx, "app", hour, b"{not json");
    let hits = query_ok(
        &s,
        "SELECT dataset, reconciliation FROM _ledger",
        around_now(),
    );
    assert_eq!(row(&hits, "default/app")["reconciliation"], "unknown");
    let web = &row(&hits, "default/web")["reconciliation"];
    assert!(*web == "three_way" || *web == "two_way", "{web}");
}

#[test]
fn gov_ledger_store_unavailable() {
    let inst = S3Instance::new();
    let s = inst.start(&[("flush.max_records", Some("1"))]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let chain = common::store::Remote::new(inst.store(), "app");
    let (_, committed) = poll(Duration::from_secs(20), || chain.current(), Option::is_some);
    assert!(committed, "20 秒内未见 app 的 _current");
    inst.relay.stop();
    query(&s, "SELECT * FROM _ledger", around_now()).assert_envelope(503, "unavailable");
}

// ---------------------------------------------------------------------------
// verdict-summary（REQ-AMB-GOV-LEDGER-V3）
// ---------------------------------------------------------------------------

/// 以 `reconciliation.grace` 为 `1m` 启动、等到开始受理请求超过 2 分钟（越过「刚重启」的补发中），`app` 已存在。
fn judged_instance() -> common::missed::Instance {
    let i = common::missed::Instance::start(&[common::missed::GRACE_1M]);
    common::api::create_stream(&i.server, "app");
    i.wait_accepting(Duration::from_secs(120));
    i
}

const SUMMARY_KEYS: [&str; 8] = [
    "ts_sources",
    "windows_pending",
    "windows_consistent",
    "windows_missing",
    "windows_duplicate",
    "windows_silent",
    "windows_undeterminable",
    "silent_sources",
];

#[test]
fn gov_ledger_verdict_summary_and_silent_sources() {
    use common::missed::{in_window, ingest_at, ledger_row, minute_ago, report};
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
    let row = ledger_row(&i.server, "app", (w1, now_us()));
    assert_eq!(
        (
            &row["windows_missing"],
            &row["windows_consistent"],
            &row["windows_pending"]
        ),
        (&json!(1), &json!(1), &json!(0)),
        "{row}"
    );
    assert_eq!(row["silent_sources"], 1, "{row}");
    assert_eq!(
        (&row["ts_sources"], &row["reconciliation"]),
        (&json!("fw-1"), &json!("two_way")),
        "{row}"
    );
    assert!(row.get("sources").is_none(), "{row}");
}

#[test]
fn gov_ledger_verdict_summary_null_when_unknown() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1H]);
    let hour = {
        let now = now_us();
        now - now.rem_euclid(HOUR_US)
    };
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let (_, written) = poll(
        Duration::from_secs(20),
        || counter_file(&fx, "app", hour).exists(),
        |e| *e,
    );
    assert!(written, "20 秒内未见 app 的计数对象");
    write_counter_bytes(&fx, "app", hour, b"{not json");
    let hits = query_ok(
        &s,
        "SELECT * FROM _ledger WHERE dataset = 'default/app'",
        around_now(),
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["reconciliation"], "unknown");
    for k in [
        "ts_sources",
        "windows_missing",
        "windows_consistent",
        "silent_sources",
    ] {
        assert!(hits[0].get(k).is_none(), "{k}: {}", hits[0]);
    }
}

#[test]
fn gov_ledger_verdict_summary_ignores_sum_overflow() {
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
    let range = (h, h + HOUR_US);
    let hits = query_ok(
        &i.server,
        "SELECT windows_missing, windows_consistent, reconciliation FROM _ledger WHERE dataset = 'default/app'",
        range,
    );
    assert_eq!(
        hits,
        [json!({"windows_missing": 1, "windows_consistent": 0, "reconciliation": "two_way"})]
    );
    query(
        &i.server,
        "SELECT * FROM _reconciliation WHERE dataset = 'default/app'",
        range,
    )
    .assert_envelope(500, "internal");
}

#[test]
fn gov_ledger_lookback_corrupt_unknown() {
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
            Some(json!({"fw-1": 1})),
            Some((1, json!({"fw-1": 1}))),
        )],
    );
    write_counter_bytes(&i.fx, "app", h - HOUR_US, b"not json");
    let hits = query_ok(
        &i.server,
        "SELECT * FROM _ledger WHERE dataset = 'default/app'",
        (h, h + HOUR_US),
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(hits[0]["reconciliation"], "unknown");
    for k in ["ts_sources", "windows_consistent", "silent_sources"] {
        assert!(hits[0].get(k).is_none(), "{k}: {}", hits[0]);
    }
}

/// 防回归（不对应块场景）：判定时刻补发中的来源不计入 `silent_sources`——REQ-AMB-GOV-LEDGER-V3 的 `silent_sources` 按
/// REQ-AMB-GOV-MISSED-COLLECTION 在 W 上报中断计，而该块 BACKFILL-HOLD 把补发中的来源按待定计、不算在 W 上报中断。
/// 补发停下（过一个宽限期）之后照常计入。
#[test]
fn gov_ledger_silent_sources_exclude_backfilling() {
    use common::missed::{ledger_row, minute_ago, report};
    let i = judged_instance();
    let w = minute_ago(20);
    // 迟到的按时间戳上报：fw-1 由此补发中一个宽限期
    report(&i.server, "app", "fw-1", w, 0);
    let row = ledger_row(&i.server, "app", (w, now_us()));
    assert_eq!(row["silent_sources"], 0, "{row}");
    std::thread::sleep(common::missed::SETTLE);
    let row = ledger_row(&i.server, "app", (w, now_us()));
    assert_eq!(row["silent_sources"], 1, "{row}");
    for k in SUMMARY_KEYS {
        assert!(row.get(k).is_some(), "{k}: {row}");
    }
}
