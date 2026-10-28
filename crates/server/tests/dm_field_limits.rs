//! REQ-AMB-DM-FIELD-LIMITS 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 摄取、经 `_search` 查询。展平深度的单元测试在 `amberlog-ingest` 的记录模型。

mod common;

use std::time::Duration;

use common::api::{FLUSH_1H, ingest, ingest_ok, search, search_resp};
use common::{Fixture, Kv};
use serde_json::{Value, json};

const CAP_3: Kv = ("ingest.max_columns", Some("3"));

/// `status[0].errors` 的 `(index, reason)`。
fn errors(status: &Value) -> Vec<(u64, String)> {
    status["errors"]
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

fn counts(body: &Value) -> (u64, u64) {
    (
        body["status"][0]["successful"].as_u64().unwrap(),
        body["status"][0]["failed"].as_u64().unwrap(),
    )
}

// ---------------------------------------------------------------------------
// 展平深度
// ---------------------------------------------------------------------------

#[test]
fn field_limits_flatten_depth_8() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let body = ingest_ok(
        &s,
        "app",
        r#"{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":{"l7":{"l8":{"_x":1,"y":[2]},"z":"k"}}}}}}}}"#,
    );
    assert_eq!(counts(&body), (1, 0));
    assert_eq!(
        search(&s, r#"SELECT "l1.l2.l3.l4.l5.l6.l7.l8" FROM app"#),
        [json!({"l1.l2.l3.l4.l5.l6.l7.l8": r#"{"_x":1,"y":[2]}"#})]
    );
    assert_eq!(
        search(&s, r#"SELECT "l1.l2.l3.l4.l5.l6.l7.z" FROM app"#),
        [json!({"l1.l2.l3.l4.l5.l6.l7.z": "k"})]
    );
}

// ---------------------------------------------------------------------------
// 列数上限
// ---------------------------------------------------------------------------

#[test]
fn field_limits_max_columns_reject() {
    let fx = Fixture::new();
    let s = fx.start(&[CAP_3]);
    ingest_ok(&s, "app", r#"{"a":1,"b":2}"#);
    let body = ingest_ok(&s, "app", r#"[{"c":1,"a":3},{"a":4,"b":5}]"#);
    assert_eq!(counts(&body), (1, 1));
    assert_eq!(
        errors(&body["status"][0]),
        [(0, "too_many_columns".to_owned())]
    );
    assert!(search(&s, "SELECT a FROM app WHERE a = 3").is_empty());
    search_resp(&s, "SELECT c FROM app").assert_envelope(400, "unknown_column");
}

#[test]
fn field_limits_shadow_counted() {
    let fx = Fixture::new();
    let s = fx.start(&[CAP_3]);
    ingest_ok(&s, "app", r#"{"a":1}"#);
    ingest_ok(&s, "app", r#"{"a":"x"}"#);
    let v = ingest(&s, "app", r#"{"a":true}"#).assert_envelope(400, "all_records_rejected");
    assert_eq!(
        v["details"]["status"][0]["errors"][0]["reason"],
        "too_many_columns"
    );
}

#[test]
fn field_limits_lowered_cap_keeps_columns() {
    let fx = Fixture::new();
    let mut s = fx.start(&[FLUSH_1H, ("ingest.max_columns", Some("1000"))]);
    ingest_ok(&s, "app", r#"{"a":1,"b":2,"c":3}"#);
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
    drop(s);
    let s = fx.start(&[FLUSH_1H, ("ingest.max_columns", Some("2"))]);
    let body = ingest_ok(&s, "app", r#"[{"b":9},{"d":1}]"#);
    assert_eq!(counts(&body).0, 1);
    assert_eq!(
        errors(&body["status"][0]),
        [(1, "too_many_columns".to_owned())]
    );
    assert_eq!(search(&s, "SELECT c FROM app WHERE c = 3").len(), 1);
}

// ---------------------------------------------------------------------------
// 需求场景之外的补充测试
// ---------------------------------------------------------------------------

#[test]
fn field_limits_depth_8_empty_object() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let body = ingest_ok(
        &s,
        "app",
        r#"{"l1":{"l2":{"l3":{"l4":{"l5":{"l6":{"l7":{"l8":{},"z":"k"}}}}}}}}"#,
    );
    assert_eq!(counts(&body), (1, 0));
    assert_eq!(
        search(&s, r#"SELECT "l1.l2.l3.l4.l5.l6.l7.z" FROM app"#),
        [json!({"l1.l2.l3.l4.l5.l6.l7.z": "k"})]
    );
    search_resp(&s, r#"SELECT "l1.l2.l3.l4.l5.l6.l7.l8" FROM app"#)
        .assert_envelope(400, "unknown_column");
}

#[test]
fn field_limits_default_cap_1000() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let fields: Vec<String> = (1..=999).map(|i| format!(r#""f{i}":1"#)).collect();
    let body = ingest_ok(&s, "wide", &format!("{{{}}}", fields.join(",")));
    assert_eq!(counts(&body), (1, 0));
    let v = ingest(&s, "wide", r#"{"f1000":1}"#).assert_envelope(400, "all_records_rejected");
    assert_eq!(
        v["details"]["status"][0]["errors"][0]["reason"],
        "too_many_columns"
    );
}

#[test]
fn field_limits_batch_order_counted() {
    let fx = Fixture::new();
    let s = fx.start(&[CAP_3]);
    ingest_ok(&s, "app", r#"{"a":1}"#);
    let body = ingest_ok(&s, "app", r#"[{"b":1},{"c":1}]"#);
    assert_eq!(counts(&body), (1, 1));
    assert_eq!(
        errors(&body["status"][0]),
        [(1, "too_many_columns".to_owned())]
    );
    assert_eq!(search(&s, "SELECT b FROM app WHERE b = 1").len(), 1);
}
