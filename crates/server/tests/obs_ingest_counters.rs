//! REQ-AMB-OBS-INGEST-COUNTERS-V3 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 与 `_sent` 上报，直接读对象存储里的计数对象核对。对象存储默认 `file://` 临时目录；
//! 「某前缀持续拒绝访问」用 MinIO 台架的受限用户加按对象标签拒绝读取（[`S3Instance::deny_reads`]）。
//!
//! 「60 秒内」的断言以轮询实现：条件在 60 秒内成立即通过；「60 秒之后逐字节未变」照实等满 60 秒。

mod common;

use std::time::Duration;

use common::api::{
    FLUSH_1H, HOUR_US, MINUTE_US, WalFault, counter_bytes, counter_file, counter_window,
    create_stream, ingest, ingest_ok, ingest_with, now_us, poll, sent, sent_element, settle_minute,
    window_in,
};
use common::s3::S3Instance;
use common::{Fixture, Server};
use serde_json::{Value, json};

const DEADLINE: Duration = Duration::from_secs(60);
/// 断言「60 秒之后」时多等的余量。
const PAST_DEADLINE: Duration = Duration::from_secs(62);

/// 轮询 `stream` 在分钟 `w` 的计数条目，直到 `received`、`durable` 与期望相等（至多 60 秒），断言成立并返回该条目。
fn expect_counts(fx: &Fixture, stream: &str, w: i64, received: u64, durable: u64) -> Value {
    let (entry, ok) = poll(
        DEADLINE,
        || counter_window(fx, stream, w),
        |e| {
            e.as_ref()
                .is_some_and(|e| e["received"] == received && e["durable"] == durable)
        },
    );
    assert!(ok, "{stream} 分钟 {w} 的计数条目：{entry:?}");
    entry.unwrap()
}

// ---------------------------------------------------------------------------
// 计数对象的落点与形状
// ---------------------------------------------------------------------------

#[test]
fn counters_received_durable_window() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let w = settle_minute(Duration::from_secs(5), false);
    ingest_ok(&server, "app", r#"[{"a":1},"bad",{"a":3}]"#);
    expect_counts(&fx, "app", w, 3, 2);
}

#[test]
fn counters_object_format() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "cf");
    // M1 与 M2 须在同一 UTC 小时内：避开小时的最后一分钟
    let m1 = settle_minute(Duration::from_secs(5), true);
    ingest_ok(&server, "cf", r#"[{"a":1}]"#);
    std::thread::sleep(Duration::from_micros(
        (m1 + MINUTE_US - now_us()) as u64 + 500_000,
    ));
    let m2 = settle_minute(Duration::from_secs(5), false);
    assert_eq!(m2, m1 + MINUTE_US);
    ingest_ok(&server, "cf", r#"[{"a":2},{"a":3}]"#);

    let (bytes, ok) = poll(
        DEADLINE,
        || counter_bytes(&fx, "cf", m1),
        |b| {
            b.as_deref()
                .and_then(|b| window_in(b, m2))
                .is_some_and(|e| e["received"] == 2)
        },
    );
    assert!(ok, "{:?}", bytes.map(String::from_utf8));
    // 路径按 UTC 自行拼出字面值（`counters/<YYYY>/<MM>/<DD>/<HH>.json`），不借实现的路径函数
    let hour = m1.div_euclid(HOUR_US) * HOUR_US;
    let ts = common::gov::rfc3339(hour);
    let literal = fx.store_dir().join(format!(
        "amberlog/default/cf/counters/{}/{}/{}/{}.json",
        &ts[0..4],
        &ts[5..7],
        &ts[8..10],
        &ts[11..13]
    ));
    let bytes = std::fs::read(&literal)
        .unwrap_or_else(|e| panic!("计数对象应在 {}：{e}", literal.display()));
    let obj: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(obj["format_version"], json!(1));
    assert_eq!(obj["dataset"], "default/cf");
    let windows = obj["windows"].as_array().unwrap();
    assert_eq!(windows.len(), 2, "{obj}");
    for (entry, (start, received)) in windows.iter().zip([(m1, 1), (m2, 2)]) {
        assert_eq!(entry["window_start"], json!(start), "{entry}");
        assert!(entry["window_start"].is_i64());
        assert_eq!(entry["window_end"], json!(start + MINUTE_US));
        assert!(entry["window_end"].is_i64());
        assert_eq!(entry["received"], json!(received));
        assert!(entry["received"].is_u64() && entry["durable"].is_u64());
        assert_eq!(entry["durable"], json!(received));
        assert_eq!(entry["sent"], json!({}));
        assert_eq!(entry["rejected"], json!(0), "{entry}");
        assert_eq!(entry["received_503"], json!(0), "{entry}");
        assert!(entry.get("by_source").is_none(), "{entry}");
        // 记录不带 `_timestamp`，以接收时刻填充，按时间戳的确认数落在接收分钟
        assert_eq!(entry["durable_ts"], json!(received), "{entry}");
        assert_eq!(entry["sent_ts"], json!({}), "{entry}");
        assert!(entry.get("durable_ts_by_source").is_none(), "{entry}");
    }
}

/// 来源标识请求头。
const SOURCE: &str = "X-Amberlog-Source";

/// 等计数条目的四项总计与期望相等（至多 60 秒），返回该条目。
fn expect_tally(fx: &Fixture, stream: &str, w: i64, want: [u64; 4]) -> Value {
    let (entry, ok) = poll(
        DEADLINE,
        || counter_window(fx, stream, w),
        |e| {
            e.as_ref().is_some_and(|e| {
                ["received", "durable", "rejected", "received_503"].map(|k| e[k].as_u64())
                    == want.map(Some)
            })
        },
    );
    assert!(
        ok,
        "{stream} 分钟 {w} 的计数条目（期望 received、durable、rejected、received_503 = {want:?}）：{entry:?}"
    );
    entry.unwrap()
}

#[test]
fn counters_rejected_and_by_source() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let w = settle_minute(Duration::from_secs(10), false);
    let fw1 = [(SOURCE, "fw-1")];
    let a = ingest_with(&server, "app", &fw1, br#"[{"a":1},"bad",{"a":3}]"#);
    let b = ingest(&server, "app", r#"[{"a":1}]"#);
    let c = ingest_with(&server, "app", &fw1, br#"["bad"]"#);
    assert_eq!(
        (a.status, b.status),
        (200, 200),
        "{} {}",
        a.text(),
        b.text()
    );
    c.assert_envelope(400, "all_records_rejected");
    let entry = expect_tally(&fx, "app", w, [5, 3, 2, 0]);
    assert_eq!(
        entry["by_source"],
        json!({"fw-1": {"received": 4, "durable": 2, "rejected": 2, "received_503": 0}}),
        "{entry}"
    );
}

#[test]
fn counters_received_503() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    create_stream(&server, "app");
    let w = settle_minute(Duration::from_secs(5), false);
    fault.inject("write");
    let r = ingest_with(&server, "app", &[(SOURCE, "fw-1")], br#"[{"a":1},{"a":2}]"#);
    fault.clear();
    r.assert_envelope(503, "unavailable");
    let entry = expect_tally(&fx, "app", w, [2, 0, 0, 2]);
    assert_eq!(
        entry["by_source"],
        json!({"fw-1": {"received": 2, "durable": 0, "rejected": 0, "received_503": 2}}),
        "{entry}"
    );
}

#[test]
fn counters_by_source_cap() {
    const REQUESTS: usize = 1001;
    const WORKERS: usize = 8;
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "app");
    // 1001 个请求须落在同一分钟：从一分钟的开头发起，8 路并发
    let w = settle_minute(Duration::from_secs(50), false);
    std::thread::scope(|scope| {
        for k in 0..WORKERS {
            let server = &server;
            scope.spawn(move || {
                for i in (1..=REQUESTS).filter(|i| i % WORKERS == k) {
                    let source = format!("s-{i}");
                    let r = ingest_with(server, "app", &[(SOURCE, &source)], br#"[{"a":1}]"#);
                    assert_eq!(r.status, 200, "{source}：{}", r.text());
                }
            });
        }
    });
    assert_eq!(
        common::api::minute_of(now_us()),
        w,
        "1001 个请求没能在同一分钟内发完，场景的前提不成立"
    );
    let (entry, ok) = poll(
        DEADLINE,
        || counter_window(&fx, "app", w),
        |e| e.as_ref().is_some_and(|e| e["received"] == REQUESTS as u64),
    );
    assert!(ok, "{entry:?}");
    let by_source = entry.as_ref().unwrap()["by_source"].as_object().unwrap();
    assert_eq!(by_source.len(), 1000);
    let sum: u64 = by_source
        .values()
        .map(|t| t["received"].as_u64().unwrap())
        .sum();
    assert_eq!(sum, 1000);
}

#[test]
fn counters_legacy_object_missing_new_fields() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    // `_sent` 空数组只使 `app` 存在，不上报任何元素
    create_stream(&server, "app");
    // 替换与随后的摄取须在同一分钟内
    let w = settle_minute(Duration::from_secs(20), true);
    let legacy = json!({
        "format_version": 1,
        "dataset": "default/app",
        "windows": [{"window_start": w, "window_end": w + MINUTE_US, "received": 5, "durable": 5, "sent": {}}],
    });
    let file = counter_file(&fx, "app", w);
    std::fs::create_dir_all(file.parent().unwrap()).unwrap();
    std::fs::write(&file, legacy.to_string()).unwrap();
    ingest_ok(&server, "app", r#"[{"a":1},"bad"]"#);
    let entry = expect_tally(&fx, "app", w, [7, 6, 1, 0]);
    assert!(entry.get("by_source").is_none(), "{entry}");
    assert_eq!(entry["durable_ts"], json!(1), "{entry}");
    assert_eq!(entry["sent_ts"], json!({}), "{entry}");
    assert!(entry.get("durable_ts_by_source").is_none(), "{entry}");
}

#[test]
fn counters_durable_ts_by_record_minute() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "app");
    // 两次摄取须在同一分钟内
    let t = settle_minute(Duration::from_secs(10), false);
    let w = t - 10 * MINUTE_US;
    let at = |s: i64| common::gov::rfc3339(w + s * 1_000_000);
    let tagged = format!(
        r#"[{{"_timestamp":"{}"}},{{"_timestamp":"{}"}},"bad"]"#,
        at(1),
        at(2)
    );
    let r = ingest_with(&server, "app", &[(SOURCE, "fw-1")], tagged.as_bytes());
    assert_eq!(r.status, 200, "{}", r.text());
    ingest_ok(
        &server,
        "app",
        &format!(r#"[{{"_timestamp":"{}"}}]"#, at(3)),
    );

    let late = expect_entry(&fx, "app", w, |e| e["durable_ts"] == 3);
    assert_eq!(
        (&late["received"], &late["durable"]),
        (&json!(0), &json!(0)),
        "{late}"
    );
    assert_eq!(late["durable_ts_by_source"], json!({"fw-1": 2}), "{late}");
    expect_counts(&fx, "app", t, 4, 3);
}

// ---------------------------------------------------------------------------
// 计入规则
// ---------------------------------------------------------------------------

#[test]
fn counters_503_received_not_durable() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    create_stream(&server, "c5");
    let w = settle_minute(Duration::from_secs(5), false);
    fault.inject("write");
    let r = ingest(&server, "c5", r#"[{"a":1},{"a":2}]"#);
    fault.clear();
    r.assert_envelope(503, "unavailable");
    expect_counts(&fx, "c5", w, 2, 0);
}

#[test]
fn counters_request_level_rejections_not_received() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "cr");
    let w = settle_minute(Duration::from_secs(5), false);
    let gz = ingest_with(
        &server,
        "cr",
        &[("Content-Encoding", "br")],
        br#"[{"a":1}]"#,
    );
    let bad = ingest(&server, "cr", r#"[{"a":1},{"a":"#);
    let ok = ingest(&server, "cr", r#"[{"a":1}]"#);
    assert_eq!((gz.status, bad.status, ok.status), (415, 400, 200));
    expect_counts(&fx, "cr", w, 1, 1);
}

// ---------------------------------------------------------------------------
// 写出时限、停机与重启
// ---------------------------------------------------------------------------

#[test]
fn counters_persist_restart() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    let w = settle_minute(Duration::from_secs(5), false);
    ingest_ok(&server, "app", r#"[{"a":1},"bad",{"a":3}]"#);
    assert_eq!(ingest(&server, "app", r#"["bad"]"#).status, 400);
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
    let entry = counter_window(&fx, "app", w).expect("停机后计数对象中有该分钟的条目");
    assert_eq!(entry["received"], 4, "{entry}");
    assert_eq!(entry["durable"], 2, "{entry}");
}

#[test]
fn counters_no_double_count_after_restart() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    let w = settle_minute(Duration::from_secs(5), false);
    ingest_ok(&server, "app", r#"[{"a":1},"bad",{"a":3}]"#);
    expect_counts(&fx, "app", w, 3, 2);
    server.sigkill();

    let server = fx.start(&[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    // 重放交回的增量在第一个写出节拍（约 5 秒）内写出；看满三个节拍
    let (entry, changed) = poll(
        Duration::from_secs(16),
        || counter_window(&fx, "app", w),
        |e| {
            e.as_ref()
                .is_none_or(|e| e["received"] != 3 || e["durable"] != 2)
        },
    );
    assert!(!changed, "重启后计数被改写为 {entry:?}");
}

#[test]
fn counters_recovered_after_sigkill_ready_window() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    let w = settle_minute(Duration::from_secs(5), false);
    ingest_ok(&server, "app", r#"[{"a":1},"bad",{"a":3}]"#);
    server.sigkill();
    // 软判断：强杀前计数对象通常尚未写出（写出节拍 5 秒）；偶尔已写出时，本次没有考到「重放补齐」这条路径，
    // 但场景的断言照样成立，不判失败
    if counter_bytes(&fx, "app", w).is_some() {
        eprintln!("注意：强杀前计数对象已写出，本次未考到重放补齐的路径");
    }

    let server = fx.start(&[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    expect_counts(&fx, "app", w, 3, 2);
}

#[test]
fn counters_deadline_isolated_per_object() {
    let inst = S3Instance::with_app_user();
    let server = inst.start(&[]);
    let store = inst.store();
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let dataset = |s: &str| amberlog_common::dataset::Dataset {
        org: "default".into(),
        stream: s.into(),
    };
    let read = |stream: &str, hour: i64| {
        let path = store.object_path(
            &dataset(stream),
            &amberlog_ingest::counters::counters_path(hour),
        );
        rt.block_on(store.read(&path))
            .unwrap()
            .map(|f| f.bytes.to_vec())
    };

    // `bad` 已存在且其计数对象已写出，随后它前缀下的现有对象一律拒绝读取
    let w0 = settle_minute(Duration::from_secs(5), true);
    ingest_ok(&server, "bad", r#"[{"a":1}]"#);
    let (_, written) = poll(DEADLINE, || read("bad", w0), Option::is_some);
    assert!(written, "bad 的计数对象应已写出");
    assert!(inst.deny_reads("default/bad/") > 0);

    let w = settle_minute(Duration::from_secs(5), false);
    // `bad` 在分钟 w 所在小时的计数对象此刻的内容（w0 避开了小时的最后一分钟，w 与 w0 同一小时，对象已存在）
    let bad_before = read("bad", w);
    ingest_ok(&server, "bad", r#"[{"a":1}]"#);
    ingest_ok(&server, "app", r#"[{"a":1},"bad",{"a":3}]"#);
    let (entry, ok) = poll(
        DEADLINE,
        || read("app", w).and_then(|b| window_in(&b, w)),
        |e| {
            e.as_ref()
                .is_some_and(|e| e["received"] == 3 && e["durable"] == 2)
        },
    );
    assert!(ok, "app 的计数条目：{entry:?}");
    // 隔离确实被考到：`app` 写出之后再过一个写出节拍，`bad` 的计数对象仍没有写成（没有新计数）
    std::thread::sleep(Duration::from_secs(6));
    assert!(bad_before.is_some(), "bad 的计数对象应已存在");
    assert_eq!(
        read("bad", w),
        bad_before,
        "bad 的前缀拒绝访问期间，其计数对象不应写出新计数"
    );
}

// ---------------------------------------------------------------------------
// 计数对象的格式版本不兼容情形
// ---------------------------------------------------------------------------

/// `app` 的当前小时计数对象写出后被测试换成 `replacement`，再摄取一条：200，60 秒之后逐字节未变。
fn replaced_object_left_alone(replacement: &[u8]) {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    // 替换与随后的摄取须在同一小时内：避开小时的最后一分钟
    let w = settle_minute(Duration::from_secs(20), true);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    expect_counts(&fx, "app", w, 1, 1);
    std::fs::write(counter_file(&fx, "app", w), replacement).unwrap();

    assert_eq!(ingest(&server, "app", r#"[{"a":2}]"#).status, 200);
    std::thread::sleep(PAST_DEADLINE);
    assert_eq!(counter_bytes(&fx, "app", w).as_deref(), Some(replacement));
}

#[test]
fn counters_object_high_version_not_overwritten() {
    replaced_object_left_alone(
        br#"{"format_version":2,"dataset":"default/app","windows":[],"future":true}"#,
    );
}

#[test]
fn counters_object_unparseable_not_overwritten() {
    replaced_object_left_alone(b"not json");
}

// ---------------------------------------------------------------------------
// _sent 上报
// ---------------------------------------------------------------------------

/// 等计数对象中分钟 `w` 的条目满足 `ok`（至多 60 秒），返回该条目。
fn expect_entry(fx: &Fixture, stream: &str, w: i64, ok: impl Fn(&Value) -> bool) -> Value {
    let (entry, done) = poll(
        DEADLINE,
        || counter_window(fx, stream, w),
        |e| e.as_ref().is_some_and(&ok),
    );
    assert!(done, "{stream} 分钟 {w} 的计数条目：{entry:?}");
    entry.unwrap()
}

#[test]
fn counters_sent_report_absolute() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let w = settle_minute(Duration::from_secs(5), false);
    for n in [100, 120] {
        let r = sent(&server, "app", &format!("[{}]", sent_element("src1", w, n)));
        assert_eq!(r.status, 200, "{}", r.text());
    }
    expect_entry(&fx, "app", w, |e| e["sent"]["src1"] == 120);
}

#[test]
fn counters_sent_basis_timestamp_separate() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "app");
    let w = settle_minute(Duration::from_secs(5), false);
    let end = w + MINUTE_US;
    for body in [
        format!(r#"[{{"source":"a","window_start":{w},"window_end":{end},"sent":90}}]"#),
        format!(
            r#"[{{"source":"a","window_start":{w},"window_end":{end},"sent":100,"basis":"timestamp"}}]"#
        ),
    ] {
        let r = sent(&server, "app", &body);
        assert_eq!(r.status, 200, "{}", r.text());
    }
    let e = expect_entry(&fx, "app", w, |e| {
        e["sent"] == json!({"a": 90}) && e["sent_ts"] == json!({"a": 100})
    });
    assert_eq!(e["durable_ts"], json!(0), "{e}");
}

#[test]
fn counters_sent_zero_entry_kept() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "app");
    let w = settle_minute(Duration::from_secs(5), false);
    for n in [100, 0] {
        let r = sent(&server, "app", &format!("[{}]", sent_element("a", w, n)));
        assert_eq!(r.status, 200, "{}", r.text());
    }
    let e = expect_entry(&fx, "app", w, |e| e["sent"] == json!({"a": 0}));
    assert_eq!(e["received"], 0);
    assert_eq!(e["durable"], 0);
}

#[test]
fn counters_sent_response_and_duplicates() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "sd");
    let w = settle_minute(Duration::from_secs(5), false);
    let end = w + MINUTE_US;
    let body = format!(
        r#"[{{"source":"a","window_start":{w},"window_end":{end},"sent":5,"extra":1}},{{"source":"b","window_start":{w},"window_end":{end},"sent":9}},{{"source":"a","window_start":{w},"window_end":{end},"sent":7}}]"#
    );
    let r = sent(&server, "sd", &body);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json(), json!({"code": 200, "accepted": 3}));
    expect_entry(&fx, "sd", w, |e| e["sent"] == json!({"a": 7, "b": 9}));
}

#[test]
fn counters_sent_basis_invalid() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    for basis in [r#""ts""#, "1"] {
        let body = format!(
            r#"[{{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":1,"basis":{basis}}}]"#
        );
        sent(&server, "app", &body).assert_envelope(400, "invalid_request");
    }
}

#[test]
fn counters_sent_element_validation() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = sent(
        &server,
        "app",
        r#"[{"source":"a","window_start":1700000030000000,"window_end":1700000090000000,"sent":1}]"#,
    );
    r.assert_envelope(400, "invalid_request");
}

#[test]
fn counters_sent_invalid_json_and_negative() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    sent(&server, "app", r#"[{"source":"a","#).assert_envelope(400, "invalid_json");
    sent(
        &server,
        "app",
        r#"[{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":-1}]"#,
    )
    .assert_envelope(400, "invalid_request");
}

#[test]
fn counters_sent_request_validation() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    create_stream(&server, "sv");
    let e =
        r#"{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}"#;
    let long = "a".repeat(129);
    let rows = [
        e.to_owned(),
        format!("[{e}, 5]"),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":"1700000040000000","window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1.5}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"_b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"a b","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"{long}","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}}]"#
        ),
        format!(
            r#"[{e}, {{"source":"b","window_start":1700000040000000,"window_end":1700000160000000,"sent":1}}]"#
        ),
        format!(r#"[{e}, {{"source":"b","window_start":-60000000,"window_end":0,"sent":1}}]"#),
    ];
    for body in &rows {
        sent(&server, "sv", body).assert_envelope(400, "invalid_request");
    }
    std::thread::sleep(DEADLINE);
    // 2023-11-14T22 这一 UTC 小时
    match counter_bytes(&fx, "sv", 1_700_000_040_000_000) {
        None => {}
        Some(bytes) => {
            let obj: Value = serde_json::from_slice(&bytes).unwrap();
            assert!(
                obj["windows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .all(|w| w["sent"].get("a").is_none()),
                "{obj}"
            );
        }
    }
}

#[test]
fn counters_sent_wal_failure_503() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server: Server = fault.start(&fx, &[]);
    create_stream(&server, "sw");
    let w = settle_minute(Duration::from_secs(5), false);
    fault.inject("write");
    let r = sent(&server, "sw", &format!("[{}]", sent_element("a", w, 1)));
    fault.clear();
    r.assert_envelope(503, "unavailable");
}
