//! REQ-AMB-TIME-TIMESTAMP-V2 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查回归一后的
//! `_timestamp`。换算规则的单元测试在 `amberlog-ingest`。
//!
//! 「接收时刻 T」取测试发出请求前后读到的系统时钟区间。

mod common;

use std::time::{SystemTime, UNIX_EPOCH};

use common::api::{ingest, ingest_ok, search};
use common::{BEARER, Fixture, Server};
use serde_json::{Value, json};

const SECOND: i64 = 1_000_000;
const HOUR: i64 = 3_600 * SECOND;

fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

fn start() -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(&[]);
    (f, s)
}

/// 摄取并返回 `(发请求前的时刻, 摄取响应体, 收到应答后的时刻)`，时刻为 UTC 微秒。
fn ingest_at(s: &Server, stream: &str, body: &str) -> (i64, Value, i64) {
    let before = now_us();
    let r = ingest(s, stream, body);
    let after = now_us();
    (before, r.json(), after)
}

/// 查询 `stream` 全部记录的 `m` → `_timestamp`。
fn timestamps_by_m(s: &Server, stream: &str) -> Vec<(String, i64)> {
    let mut out: Vec<(String, i64)> = search(s, &format!("SELECT m, _timestamp FROM {stream}"))
        .iter()
        .map(|h| {
            (
                h["m"].as_str().unwrap().to_owned(),
                h["_timestamp"].as_i64().unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

/// `[before − 5s, after + 5s]` 包含 `t`。
fn assert_near(t: i64, before: i64, after: i64) {
    assert!(
        (before - 5 * SECOND..=after + 5 * SECOND).contains(&t),
        "{t} 不在 [{before} − 5s, {after} + 5s] 内"
    );
}

/// 摄取响应的 `status[0]`。
fn status0(body: &Value) -> &Value {
    &body["status"][0]
}

/// `status[0].errors` 中各条目的 `(index, reason)`。
fn errors(body: &Value) -> Vec<(u64, String)> {
    status0(body)["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["index"].as_u64().unwrap(),
                e["reason"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// numeric-units
// ---------------------------------------------------------------------------

#[test]
fn timestamp_unit_normalization() {
    let (_f, s) = start();
    ingest_ok(&s, "app", r#"{"_timestamp":1700000000.5,"msg":"x"}"#);
    let hits = search(&s, "SELECT _timestamp FROM app");
    assert_eq!(hits, [json!({"_timestamp": 1_700_000_000_500_000i64})]);
}

#[test]
fn timestamp_exact_decimal() {
    let (_f, s) = start();
    ingest_ok(
        &s,
        "app",
        r#"[{"_timestamp":1700002799999999999,"m":"ns"},{"_timestamp":1700002799.9999999,"m":"s"}]"#,
    );
    let body = json!({"query": {
        "sql": "SELECT _timestamp, m FROM app",
        "start_time": 1_700_002_799_999_999i64,
        "end_time": 1_700_002_800_000_000i64,
        "size": 100,
    }});
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let hits = r.json()["hits"].as_array().unwrap().clone();
    assert_eq!(hits.len(), 2, "{hits:?}");
    for h in hits {
        assert_eq!(h["_timestamp"], 1_700_002_799_999_999i64, "{h}");
    }
}

// ---------------------------------------------------------------------------
// rfc3339
// ---------------------------------------------------------------------------

#[test]
fn timestamp_rfc3339() {
    let (_f, s) = start();
    ingest_ok(
        &s,
        "app",
        r#"{"_timestamp":"2026-08-22T10:00:00.123456+08:00","msg":"x"}"#,
    );
    let hits = search(&s, "SELECT _timestamp FROM app");
    assert_eq!(hits, [json!({"_timestamp": 1_787_364_000_123_456i64})]);
}

#[test]
fn timestamp_rfc3339_leap_second() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"[{"_timestamp":"2016-12-31T23:59:60Z","m":"a"},{"_timestamp":"2016-12-31T15:59:60.5-08:00","m":"b"}]"#,
    );
    assert_eq!(status0(&body)["successful"], 2, "{body}");
    assert_eq!(
        timestamps_by_m(&s, "app"),
        [
            ("a".to_owned(), 1_483_228_800_000_000),
            ("b".to_owned(), 1_483_228_800_500_000)
        ]
    );
}

#[test]
fn timestamp_process_tz_ignored() {
    let f = Fixture::new();
    let mut cmd = f.serve(&[]);
    cmd.env("TZ", "Asia/Shanghai");
    let s = Server::start(cmd);
    let (before, body, after) = ingest_at(
        &s,
        "app",
        r#"[{"_timestamp":"2026-08-22T02:00:00Z","m":"a"},{"m":"b"}]"#,
    );
    assert_eq!(status0(&body)["successful"], 2, "{body}");
    let got = timestamps_by_m(&s, "app");
    assert_eq!(got[0], ("a".to_owned(), 1_787_364_000_000_000));
    assert_eq!(got[1].0, "b");
    assert_near(got[1].1, before, after);
}

// ---------------------------------------------------------------------------
// default-fill
// ---------------------------------------------------------------------------

#[test]
fn timestamp_default_fill() {
    let (_f, s) = start();
    let (before, body, after) = ingest_at(&s, "app", r#"{"msg":"x"}"#);
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    let hits = search(&s, "SELECT _timestamp FROM app");
    assert_eq!(hits.len(), 1);
    assert_near(hits[0]["_timestamp"].as_i64().unwrap(), before, after);
}

#[test]
fn timestamp_default_fill_null_zero_same_instant() {
    let (_f, s) = start();
    let (before, body, after) = ingest_at(
        &s,
        "app",
        r#"[{"_timestamp":null,"m":"a"},{"_timestamp":0,"m":"b"},{"_timestamp":0.0,"m":"c"},{"m":"d"}]"#,
    );
    assert_eq!(status0(&body)["successful"], 4, "{body}");
    let got = timestamps_by_m(&s, "app");
    assert_eq!(got.len(), 4);
    let t = got[0].1;
    assert!(got.iter().all(|(_, x)| *x == t), "{got:?}");
    assert_near(t, before, after);
}

// ---------------------------------------------------------------------------
// rejection
// ---------------------------------------------------------------------------

#[test]
fn timestamp_invalid() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"[{"_timestamp":"yesterday","msg":"a"},{"msg":"b"}]"#,
    );
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(1)),
        "{body}"
    );
    assert_eq!(status0(&body)["errors"][0]["reason"], "timestamp_invalid");
}

#[test]
fn timestamp_object_and_array_invalid() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"[{"_timestamp":{"a":1},"m":"a"},{"_timestamp":{},"m":"b"},{"_timestamp":[1],"m":"c"},{"m":"d"}]"#,
    );
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(3)),
        "{body}"
    );
    let errs = errors(&body);
    for i in 0..3 {
        assert!(
            errs.contains(&(i, "timestamp_invalid".to_owned())),
            "{errs:?}"
        );
    }
}

#[test]
fn timestamp_bool_and_negative_rejected() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"[{"_timestamp":true,"m":"a"},{"_timestamp":-1,"m":"b"},{"m":"c"}]"#,
    );
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(2)),
        "{body}"
    );
    let errs = errors(&body);
    assert!(
        errs.contains(&(0, "timestamp_invalid".to_owned())),
        "{errs:?}"
    );
    assert!(
        errs.contains(&(1, "timestamp_out_of_range".to_owned())),
        "{errs:?}"
    );
}

#[test]
fn timestamp_out_of_range() {
    let (_f, s) = start();
    let r = ingest(
        &s,
        "app",
        &json!([{"_timestamp": now_us() + 48 * HOUR, "m": "x"}]).to_string(),
    );
    let v = r.assert_envelope(400, "all_records_rejected");
    assert_eq!(v["details"]["status"][0]["failed"], 1, "{v}");
    assert_eq!(
        v["details"]["status"][0]["errors"][0]["reason"],
        "timestamp_out_of_range"
    );
}

/// `ingest.max_future` 放到 `3000000d`（约 8200 年），使 9999 年末上界与未来窗口分得开。
const MAX_FUTURE_WIDE: common::Kv = ("ingest.max_future", Some("\"3000000d\""));

/// 以覆盖到 9999 年末的时间范围查询 `stream` 的 `m` → `_timestamp`。
fn far_timestamps_by_m(s: &Server, stream: &str) -> Vec<(String, i64)> {
    let body = json!({"query": {
        "sql": format!("SELECT m, _timestamp FROM {stream}"),
        "start_time": 0,
        "end_time": 253_402_300_800_000_000i64,
        "size": 100,
    }});
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let mut out: Vec<(String, i64)> = r.json()["hits"]
        .as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["m"].as_str().unwrap().to_owned(),
                h["_timestamp"].as_i64().unwrap(),
            )
        })
        .collect();
    out.sort();
    out
}

#[test]
fn timestamp_after_year_9999_rejected() {
    let f = Fixture::new();
    let s = f.start(&[MAX_FUTURE_WIDE]);
    let r = ingest(
        &s,
        "app",
        r#"[{"_timestamp":253402300800000000000,"m":"a"},{"_timestamp":253402300799999999000,"m":"b"}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.json();
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(1)),
        "{body}"
    );
    assert_eq!(errors(&body), [(0, "timestamp_out_of_range".to_owned())]);
    assert_eq!(
        far_timestamps_by_m(&s, "app"),
        [("b".to_owned(), 253_402_300_799_999_999)]
    );
}

#[test]
fn timestamp_after_year_9999_rfc3339_offset() {
    let f = Fixture::new();
    let s = f.start(&[MAX_FUTURE_WIDE]);
    let r = ingest(
        &s,
        "app",
        r#"[{"_timestamp":"9999-12-31T23:59:59-01:00","m":"a"},{"_timestamp":"9999-12-31T23:59:59+01:00","m":"b"}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.json();
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(1)),
        "{body}"
    );
    assert_eq!(errors(&body), [(0, "timestamp_out_of_range".to_owned())]);
    assert_eq!(
        far_timestamps_by_m(&s, "app"),
        [("b".to_owned(), 253_402_297_199_000_000)]
    );
}

#[test]
fn timestamp_max_future_window() {
    let (_f, s) = start();
    let t = now_us();
    let (_, body, _) = ingest_at(
        &s,
        "app",
        &json!([
            {"_timestamp": t + 23 * HOUR, "m": "a"},
            {"_timestamp": t + 25 * HOUR, "m": "b"},
        ])
        .to_string(),
    );
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(1)),
        "{body}"
    );
    assert_eq!(errors(&body), [(1, "timestamp_out_of_range".to_owned())]);
}

#[test]
fn timestamp_max_past_window() {
    let f = Fixture::new();
    let s = f.start(&[("ingest.max_past", Some("\"1h\""))]);
    let t = now_us();
    let (_, body, _) = ingest_at(
        &s,
        "app",
        &json!([
            {"_timestamp": t - 2 * HOUR, "m": "a"},
            {"_timestamp": t - HOUR / 2, "m": "b"},
        ])
        .to_string(),
    );
    assert_eq!(
        (&status0(&body)["successful"], &status0(&body)["failed"]),
        (&json!(1), &json!(1)),
        "{body}"
    );
    assert_eq!(errors(&body), [(0, "timestamp_out_of_range".to_owned())]);
}

#[test]
fn timestamp_no_past_limit_by_default() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"{"_timestamp":"1971-01-01T00:00:00Z","m":"old"}"#,
    );
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    let hits = search(&s, "SELECT _timestamp FROM app");
    assert_eq!(hits, [json!({"_timestamp": 31_536_000_000_000i64})]);
}
