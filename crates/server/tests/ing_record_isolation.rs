//! REQ-AMB-ING-RECORD-ISOLATION 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 提交含取值缺陷的元素、经 `_search` 查询。含非法 UTF-8 字节的请求体以原始字节构造，不经 JSON 序列化库。

mod common;

use common::Fixture;
use common::api::{ingest, ingest_ok, ingest_with, search, search_resp};
use serde_json::Value;

/// `status[0].errors` 的 `(index, reason)` 列表。
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

fn malformed(indexes: &[u64]) -> Vec<(u64, String)> {
    indexes
        .iter()
        .map(|&i| (i, "malformed_record".to_owned()))
        .collect()
}

/// 断言 200 且成功、失败条数如期，返回 `status[0]`。
fn ok_status(body: &Value, successful: u64, failed: u64) -> Value {
    let status = body["status"][0].clone();
    assert_eq!(
        (status["successful"].as_u64(), status["failed"].as_u64()),
        (Some(successful), Some(failed)),
        "{body}"
    );
    status
}

/// `depth` 层对象逐层嵌套、最内层为 `{"x":1}`（`depth` 为 1 时即 `{"x":1}`）。
fn nested_object(depth: usize) -> String {
    format!(
        "{}{{\"x\":1}}{}",
        "{\"k\":".repeat(depth - 1),
        "}".repeat(depth - 1)
    )
}

#[test]
fn isolation_number_overflow() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest_ok(&server, "app", r#"[{"a":1},{"b":1e400}]"#);
    let status = ok_status(&v, 1, 1);
    assert_eq!(errors(&status), malformed(&[1]));
    assert_eq!(search(&server, "SELECT a FROM app WHERE a = 1").len(), 1);
}

#[test]
fn isolation_surrogate_and_utf8() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let body = [
        br#"[{"m":"\ud800"},{"m":"ok"},{"m":""#.as_slice(),
        b"\xFF",
        br#""}]"#,
    ]
    .concat();
    let r = ingest_with(&server, "app", &[], &body);
    assert_eq!(r.status, 200, "{}", r.text());
    let status = ok_status(&r.json(), 1, 2);
    assert_eq!(errors(&status), malformed(&[0, 2]));
}

#[test]
fn isolation_nesting_depth() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let body = format!("[{},{}]", nested_object(128), nested_object(129));
    let v = ingest_ok(&server, "app", &body);
    let status = ok_status(&v, 1, 1);
    assert_eq!(errors(&status), malformed(&[1]));
}

#[test]
fn isolation_timestamp_and_big_number() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let big = format!("1{}", "0".repeat(400));
    let body =
        format!(r#"[{{"_timestamp":1e400}},{{"_timestamp":"\ud800"}},{{"n":{big}}},{{"m":"ok"}}]"#);
    let v = ingest_ok(&server, "app", &body);
    let status = ok_status(&v, 2, 2);
    assert_eq!(
        errors(&status),
        [
            (0, "timestamp_out_of_range".to_owned()),
            (1, "malformed_record".to_owned())
        ]
    );
    let hits = search(&server, "SELECT n FROM app WHERE n IS NOT NULL");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["n"], big.as_str());
}

#[test]
fn isolation_single_object_all_rejected() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest(&server, "app", r#"{"b":-1e400}"#).assert_envelope(400, "all_records_rejected");
    assert_eq!(
        v["details"]["status"][0]["errors"][0]["reason"],
        "malformed_record"
    );
}

#[test]
fn isolation_syntax_error_whole_batch() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"{"a":2}"#);
    ingest(&server, "app", r#"[{"a":1},{"b":1e400},{"c":]"#).assert_envelope(400, "invalid_json");
    assert!(search(&server, "SELECT a FROM app WHERE a = 1").is_empty());
}

#[test]
fn isolation_duplicate_key_overwritten() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest_ok(&server, "app", r#"[{"r":1e400,"r":1.5}]"#);
    ok_status(&v, 1, 0);
    assert_eq!(search(&server, "SELECT r FROM app WHERE r = 1.5").len(), 1);
}

// ---------------------------------------------------------------------------
// 需求场景之外的补充测试
// ---------------------------------------------------------------------------

#[test]
fn isolation_defect_registers_no_column() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"{"a":1}"#);
    let v = ingest(&server, "app", r#"[{"fresh":1,"m":"\ud800"}]"#)
        .assert_envelope(400, "all_records_rejected");
    assert_eq!(
        v["details"]["status"][0]["errors"][0]["reason"],
        "malformed_record"
    );
    search_resp(&server, "SELECT fresh FROM app").assert_envelope(400, "unknown_column");
}

#[test]
fn isolation_defect_in_key_and_non_object() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest_ok(
        &server,
        "app",
        r#"[{"\udc00":1},"\ud800",{"t":["\ud800"]},{"ok":1}]"#,
    );
    let status = ok_status(&v, 1, 3);
    assert_eq!(errors(&status), malformed(&[0, 1, 2]));
}
