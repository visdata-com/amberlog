//! REQ-AMB-DM-RECORD-MODEL-V4 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查回。
//! 展平、序列化与命名约束的单元测试在 `amberlog-ingest`。
//! 「对象存储不可达」用 MinIO 台架加进程内 TCP 转发（`common::s3`）。

mod common;

use std::time::Duration;

use common::api::{ingest, ingest_ok, search};
use common::s3::S3Instance;
use common::store::FLUSH_1S;
use common::{BEARER, Fixture, Server};
use serde_json::{Value, json};

fn start() -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(&[]);
    (f, s)
}

/// 摄取响应的 `status[0]`。
fn status0(body: &Value) -> &Value {
    &body["status"][0]
}

/// 断言 `status[0]` 的 `successful` 与 `failed`。
fn assert_counts(body: &Value, successful: u64, failed: u64) {
    assert_eq!(
        (&status0(body)["successful"], &status0(body)["failed"]),
        (&json!(successful), &json!(failed)),
        "{body}"
    );
}

/// `status[0].errors` 中各条目的 `(index, reason)`。
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

/// 以覆盖全部数据的时间范围查询 `sql`，断言错误信封。
fn search_err(s: &Server, sql: &str, status: u16, reason: &str) {
    s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        common::api::search_body(sql).as_bytes(),
    )
    .assert_envelope(status, reason);
}

/// 摄取并断言整批被拒（400 `all_records_rejected`），返回 `details.status[0]`。
fn ingest_all_rejected(s: &Server, stream: &str, body: &str) -> Value {
    let v = ingest(s, stream, body).assert_envelope(400, "all_records_rejected");
    v["details"]["status"][0].clone()
}

// ---------------------------------------------------------------------------
// flatten-and-serialize
// ---------------------------------------------------------------------------

#[test]
fn record_model_flatten() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "app",
        r#"{"host":{"name":"web1","os":{"version":"1.2"}},"port":8080,"ok":true}"#,
    );
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    let hits = search(
        &s,
        r#"SELECT "host.name", "host.os.version", port, ok FROM app"#,
    );
    assert_eq!(
        hits,
        [json!({"host.name": "web1", "host.os.version": "1.2", "port": 8080, "ok": true})]
    );
}

#[test]
fn record_model_array_canonical_json() {
    let (_f, s) = start();
    ingest_ok(
        &s,
        "app",
        r#"{"tags":[ 1.0 , {"b":2,"a":"é"} , 12345678901234567890123 ]}"#,
    );
    let hits = search(&s, "SELECT tags FROM app");
    assert_eq!(
        hits,
        [json!({"tags": r#"[1.0,{"b":2,"a":"é"},12345678901234567890123]"#})]
    );
}

#[test]
fn record_model_deep_subtree_serialized() {
    let (_f, s) = start();
    let mut record = r#"{"leaf":"deep"}"#.to_owned();
    for _ in 0..39 {
        record = format!(r#"{{"a":{record}}}"#);
    }
    let body = ingest_ok(&s, "app", &record);
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    let hits = search(&s, "SELECT * FROM app");
    assert_eq!(hits.len(), 1);
    let found = hits[0]
        .as_object()
        .unwrap()
        .values()
        .any(|v| v.as_str().is_some_and(|t| t.contains(r#""leaf":"deep""#)));
    assert!(found, "{}", hits[0]);
}

// ---------------------------------------------------------------------------
// scalar-and-null
// ---------------------------------------------------------------------------

#[test]
fn record_model_scalar_types() {
    let (_f, s) = start();
    ingest_ok(
        &s,
        "sc",
        r#"{"i":1,"f":1.5,"e":1e2,"big":12345678901234567890,"s":"x","b":false}"#,
    );
    let hits = search(&s, "SELECT i, f, big, s, b FROM sc");
    assert_eq!(
        hits,
        [json!({"i": 1, "f": 1.5, "big": "12345678901234567890", "s": "x", "b": false})]
    );
    for record in [r#"{"i":1.5}"#, r#"{"e":5}"#, r#"{"b":"false"}"#] {
        let body = ingest_ok(&s, "sc", record);
        assert_counts(&body, 1, 0);
    }
    for sql in [
        "SELECT i__float64 FROM sc WHERE i__float64 = 1.5",
        "SELECT e__int64 FROM sc WHERE e__int64 = 5",
        "SELECT b__utf8 FROM sc WHERE b__utf8 = 'false'",
    ] {
        assert_eq!(search(&s, sql).len(), 1, "{sql}");
    }
    let body = ingest_ok(&s, "sc", r#"{"big":"y"}"#);
    assert_counts(&body, 1, 0);
    assert_eq!(search(&s, "SELECT big FROM sc WHERE big = 'y'").len(), 1);
}

#[test]
fn record_model_float_round_half_even() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "fr",
        r#"{"h":1.00000000000000011102230246251565404236316680908203125,"g":1.00000000000000011102230246251565404236316680908203126}"#,
    );
    assert_counts(&body, 1, 0);
    assert_eq!(search(&s, "SELECT h FROM fr WHERE h = 1.0").len(), 1);
    assert_eq!(
        search(&s, "SELECT g FROM fr WHERE g = 1.0000000000000002").len(),
        1
    );
}

#[test]
fn record_model_null_vs_empty() {
    let (_f, s) = start();
    ingest_ok(&s, "app", r#"{"k":"y","e":"z","v":"w"}"#);
    ingest_ok(&s, "app", r#"{"k":null,"e":"","v":"x"}"#);
    let hits = search(&s, "SELECT v FROM app WHERE k IS NULL");
    assert_eq!(hits, [json!({"v": "x"})]);
    let hits = search(&s, "SELECT k, v FROM app WHERE k IS NULL");
    assert_eq!(hits, [json!({"v": "x"})], "该 hit 不含键 k");
    let hits = search(&s, "SELECT v FROM app WHERE e = ''");
    assert_eq!(hits, [json!({"v": "x"})]);
}

#[test]
fn record_model_no_column_for_empty_object_or_null() {
    let (_f, s) = start();
    let body = ingest_ok(&s, "nc", r#"{"o":{},"p":{"q":{}},"n":null,"k":1}"#);
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    for sql in [
        "SELECT o FROM nc",
        r#"SELECT "p.q" FROM nc"#,
        "SELECT n FROM nc",
    ] {
        search_err(&s, sql, 400, "unknown_column");
    }
}

#[test]
fn record_model_duplicate_keys_last_wins() {
    let (_f, s) = start();
    let body = ingest_ok(&s, "dk", r#"{"k":1,"k":"x"}"#);
    assert_eq!(status0(&body)["successful"], 1, "{body}");
    assert_eq!(search(&s, "SELECT k FROM dk"), [json!({"k": "x"})]);
}

// ---------------------------------------------------------------------------
// record-rejection
// ---------------------------------------------------------------------------

#[test]
fn record_model_reserved_field() {
    let (_f, s) = start();
    let body = ingest_ok(&s, "app", r#"[{"_msg":"x"},{"msg":"y"}]"#);
    assert_counts(&body, 1, 1);
    assert_eq!(errors(status0(&body)), [(0, "reserved_field".to_owned())]);
}

#[test]
fn record_model_empty_source_same_name() {
    let (_f, s) = start();
    let body = ingest_ok(&s, "app", r#"[{"a.b":1,"a":{"b":{}}},{"msg":"y"}]"#);
    assert_counts(&body, 1, 1);
    assert_eq!(
        errors(status0(&body)),
        [(0, "invalid_field_name".to_owned())]
    );
}

#[test]
fn record_model_error_precedence() {
    let f = Fixture::new();
    let s = f.start(&[("ingest.max_columns", Some("1"))]);
    let status = ingest_all_rejected(
        &s,
        "ep",
        r#"[{"_x":"\ud800"},5,{"_x":1,"-a":1},{"-a":1,"_timestamp":"bad"},{"_timestamp":"bad","code":"ok"},{"_timestamp":-1,"code":"ok"},{"code":"ok"}]"#,
    );
    let want: Vec<(u64, String)> = [
        "malformed_record",
        "not_an_object",
        "reserved_field",
        "invalid_field_name",
        "timestamp_invalid",
        "timestamp_out_of_range",
        "too_many_columns",
    ]
    .iter()
    .enumerate()
    .map(|(i, r)| (i as u64, (*r).to_owned()))
    .collect();
    assert_eq!(errors(&status), want);
}

#[test]
fn record_model_timestamp_exempt_from_typing() {
    let (_f, s) = start();
    let body = ingest_ok(
        &s,
        "ts",
        r#"[{"_timestamp":"2026-08-22T02:00:00Z","m":"a"},{"_timestamp":1787364000,"m":"b"}]"#,
    );
    assert_counts(&body, 2, 0);
    for sql in [
        "SELECT _timestamp__utf8 FROM ts",
        "SELECT _timestamp__int64 FROM ts",
    ] {
        search_err(&s, sql, 400, "unknown_column");
    }
}

// ---------------------------------------------------------------------------
// schema-evolution
// ---------------------------------------------------------------------------

#[test]
fn record_model_new_field_immediately_queryable() {
    let (_f, s) = start();
    ingest_ok(&s, "app", r#"{"a":1}"#);
    ingest_ok(&s, "app", r#"{"a":2,"b":"new"}"#);
    assert_eq!(
        search(&s, "SELECT b FROM app WHERE a = 2"),
        [json!({"b": "new"})]
    );
    assert_eq!(
        search(&s, "SELECT a FROM app WHERE a = 1"),
        [json!({"a": 1})]
    );
}

#[test]
fn record_model_type_conflict_shadow() {
    let (_f, s) = start();
    ingest_ok(&s, "app", r#"{"code":200}"#);
    let body = ingest_ok(&s, "app", r#"{"code":"ok"}"#);
    assert_counts(&body, 1, 0);
    assert_eq!(
        search(&s, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'").len(),
        1
    );
}

#[test]
fn record_model_memtable_columns_in_schema() {
    let f = Fixture::new();
    let s = f.start(&[("flush.interval", Some("\"1h\""))]);
    ingest_ok(&s, "mt", r#"{"c":1}"#);
    let body = ingest_ok(&s, "mt", r#"{"c":"x"}"#);
    assert_counts(&body, 1, 0);
    assert_eq!(
        search(&s, "SELECT c__utf8 FROM mt WHERE c__utf8 = 'x'").len(),
        1
    );
}

#[test]
fn record_model_batch_first_wins() {
    let (_f, s) = start();
    let body = ingest_ok(&s, "app", r#"[{"n":1},{"n":"x"}]"#);
    assert_counts(&body, 2, 0);
    assert_eq!(
        search(&s, "SELECT n FROM app WHERE n IS NOT NULL"),
        [json!({"n": 1})]
    );
    assert_eq!(
        search(&s, "SELECT n__utf8 FROM app WHERE n__utf8 IS NOT NULL"),
        [json!({"n__utf8": "x"})]
    );
}

#[test]
fn record_model_committed_schema_offline_shadow() {
    let inst = S3Instance::new();
    let mut a = inst.start(&[]);
    ingest_ok(&a, "app", r#"{"code":200}"#);
    a.signal("TERM");
    assert_eq!(a.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    drop(a);
    assert!(committed(&inst, "app"), "A 停机时应已提交快照");

    let wal = inst.fixture.root().join("wal-b");
    let wal = format!("\"{}\"", wal.display());
    let b = inst.start(&[("wal.dir", Some(wal.as_str()))]);
    assert_eq!(b.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();
    let r = ingest(&b, "app", r#"[{"code":"ok"},{"code":201}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_counts(&r.json(), 2, 0);
    inst.relay.resume();
    assert_eq!(
        search(&b, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'").len(),
        1
    );
}

/// 数据集 `stream` 已有快照（测试进程直连台架，按找链尾读取）。
fn committed(inst: &S3Instance, stream: &str) -> bool {
    common::store::Remote::new(inst.store(), stream)
        .current()
        .is_some()
}

#[test]
fn record_model_committed_schema_from_chain_tail() {
    let fx = Fixture::new();
    let mut a = fx.start(&[]);
    ingest_ok(&a, "app", r#"{"code":200}"#);
    a.signal("TERM");
    assert_eq!(a.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    drop(a);
    let ds = fx.dataset("app");
    assert_eq!(
        ds.current(),
        Some(2),
        "A 停机时应已提交含列 `code` 的快照 2"
    );
    assert_eq!(
        schema_entries(&ds.snapshot(2), "code"),
        [json!({"name": "code", "type": "Int64", "field_id": 2})]
    );
    // 提示落后链尾一个编号
    ds.write_json("manifest/_current", &json!({"snapshot_id": 1}));

    let wal = format!("\"{}\"", fx.root().join("wal-b").display());
    let b = fx.start(&[FLUSH_1S, ("wal.dir", Some(wal.as_str()))]);
    let r = ingest(&b, "app", r#"[{"code":"ok"}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_counts(&r.json(), 1, 0);
    assert_eq!(
        search(&b, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'").len(),
        1
    );
    let head = ds.wait_records(2, Duration::from_secs(15));
    let code = schema_entries(&head, "code");
    assert_eq!(code.len(), 1, "{head}");
    assert_eq!(code[0]["type"], "Int64");
}

/// 快照 `snapshot` 的 `schema` 中名为 `name` 的元素。
fn schema_entries(snapshot: &Value, name: &str) -> Vec<Value> {
    snapshot["schema"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["name"] == name)
        .cloned()
        .collect()
}
