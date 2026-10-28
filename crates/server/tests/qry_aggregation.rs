//! REQ-AMB-QRY-AGGREGATION 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查询。
//! 对象存储用 `file://` 临时目录。
//!
//! 场景「聚合调用的形态」是 amberlog-query 的单元测试（`agg_call_form_table`）。

mod common;
mod qry_common;

use common::{Fixture, Server};
use qry_common::{ALL, Chain, early, hits, query};
use serde_json::{Value, json};

fn start() -> (Fixture, Server) {
    start_with(&[])
}

fn start_with(overrides: &[common::Kv]) -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(overrides);
    (f, s)
}

/// 以原文摄取（JSON 宏写不出 `-0.0` 与超出 f64 精度的整数）并断言 200。
fn ingest_text(s: &Server, stream: &str, body: &str) {
    let r = s.ingest_raw(stream, body.as_bytes());
    assert_eq!(r.status, 200, "摄取 {stream}：{}", r.text());
}

/// `hits` 与 `want` 作为集合相等（次序不计）。
fn same_set(got: &[Value], want: &[Value]) {
    let mut g: Vec<String> = got.iter().map(Value::to_string).collect();
    let mut w: Vec<String> = want.iter().map(Value::to_string).collect();
    g.sort();
    w.sort();
    assert_eq!(g, w);
}

// ---------------------------------------------------------------------------
// rows
// ---------------------------------------------------------------------------

#[test]
fn agg_group_by_count() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"level": "error"}, {"level": "info"}, {"level": "error"}, {"host": "a"}]),
    );
    let v = s.sql_ok("SELECT level, count(*) AS n FROM app GROUP BY level ORDER BY n DESC");
    let h = hits(&v);
    assert_eq!(h.len(), 3, "{v}");
    assert_eq!(h[0], json!({"level": "error", "n": 2}), "{v}");
    same_set(
        &h,
        &[
            json!({"level": "error", "n": 2}),
            json!({"level": "info", "n": 1}),
            json!({"n": 1}),
        ],
    );
}

#[test]
fn agg_empty_input_single_row() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"_timestamp": early(1000), "level": "info", "latency": 5}]),
    );
    let range = (2000, ALL.1);
    let r = s.search(&query(
        "SELECT count(*) AS n, max(latency) AS m FROM app",
        range,
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()), [json!({"n": 0})]);
    let r = s.search(&query(
        "SELECT level, count(*) AS n FROM app GROUP BY level",
        range,
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()), Vec::<Value>::new());
}

#[test]
fn agg_signed_zero_same_group() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"f": 0.0}]));
    ingest_text(&s, "app", r#"[{"f":-0.0},{"f":1.5}]"#);
    let v = s.sql_ok("SELECT f, count(*) AS n FROM app GROUP BY f ORDER BY n DESC");
    let h = hits(&v);
    assert_eq!(h.len(), 2, "{v}");
    assert_eq!(h[0]["n"], 2, "{v}");
    let f = h[0]["f"].to_string();
    assert!(f == "0.0" || f == "-0.0", "{v}");
}

#[test]
fn agg_all_null_group_values() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"level": "error", "latency": 10}, {"level": "error"}, {"level": "info"}]),
    );
    let v = s.sql_ok(
        "SELECT level, count(latency) AS c, sum(latency) AS s, min(latency) AS lo FROM app GROUP BY level ORDER BY level",
    );
    assert_eq!(
        hits(&v),
        [
            json!({"level": "error", "c": 1, "s": 10, "lo": 10}),
            json!({"level": "info", "c": 0}),
        ]
    );
}

// ---------------------------------------------------------------------------
// values
// ---------------------------------------------------------------------------

#[test]
fn agg_function_values_and_types() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"latency": 10, "ok": true, "host": "b"},
            {"latency": 30, "ok": false, "host": "a"},
            {"x": 1},
        ]),
    );
    let r = s.sql(
        "SELECT count(*) AS c, count(latency) AS cl, sum(latency) AS s, avg(latency) AS a, min(latency) AS lo, max(host) AS hi, min(ok) AS mo FROM app",
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text()
            .contains(r#"[{"c":3,"cl":2,"s":40,"a":20.0,"lo":10,"hi":"b","mo":false}]"#),
        "{}",
        r.text()
    );
}

#[test]
fn agg_float_sum_exact_values() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"f": 0.5}, {"f": 0.25}, {"f": 1.5}, {"x": 1}]),
    );
    let v = s.sql_ok("SELECT sum(f) AS s, avg(f) AS a, min(f) AS lo, max(f) AS hi FROM app");
    let h = hits(&v);
    assert_eq!(h.len(), 1, "{v}");
    for (k, want) in [("s", 2.25), ("a", 0.75), ("lo", 0.25), ("hi", 1.5)] {
        assert_eq!(h[0][k].as_f64(), Some(want), "{k}：{v}");
    }
}

#[test]
fn agg_int_avg_exact_sum_rounded_once() {
    let (_f, s) = start();
    ingest_text(
        &s,
        "app",
        r#"[{"g":"a","n":9007199254740993},{"g":"a","n":1},{"g":"b","n":9223372036854775807},{"g":"b","n":9223372036854775807}]"#,
    );
    let v = s.sql_ok("SELECT g, avg(n) AS a FROM app GROUP BY g ORDER BY g");
    let h = hits(&v);
    assert_eq!(h.len(), 2, "{v}");
    assert_eq!(h[0]["g"], "a", "{v}");
    assert_eq!(h[0]["a"].as_f64(), Some(4_503_599_627_370_497.0), "{v}");
    assert_eq!(h[1]["g"], "b", "{v}");
    assert_eq!(h[1]["a"].as_f64(), Some(9_223_372_036_854_775_808.0), "{v}");
    s.sql_err(
        "SELECT g, sum(n) AS s FROM app GROUP BY g ORDER BY g",
        400,
        "numeric_overflow",
    );
    // 场景之外（测试约定）：场景两组的精确和都能被个数整除，另加一组平均值带小数的数据，
    // 分出「精确和整除个数」与「只舍入一次」，并按响应原文核对结果是 Float64
    ingest_text(&s, "avgf", r#"[{"g":"c","n":1},{"g":"c","n":2}]"#);
    let r = s.sql("SELECT g, avg(n) AS a FROM avgf GROUP BY g");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text().contains(r#""hits":[{"g":"c","a":1.5}]"#),
        "{}",
        r.text()
    );
}

#[test]
fn agg_shadow_column_not_merged() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"code": 500}]));
    s.ingest_ok("app", &json!([{"code": "E500"}]));
    s.ingest_ok("app", &json!([{"code": 7}]));
    let v = s.sql_ok(
        "SELECT count(*) AS n, count(code) AS c, sum(code) AS s, max(code__utf8) AS m FROM app",
    );
    assert_eq!(hits(&v), [json!({"n": 3, "c": 2, "s": 507, "m": "E500"})]);
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

#[test]
fn agg_invalid_grouping() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"level": "error", "host": "a"}]));
    for sql in [
        "SELECT host, count(*) FROM app GROUP BY level",
        "SELECT * FROM app GROUP BY level",
        "SELECT sum(host) FROM app",
    ] {
        s.sql_err(sql, 400, "invalid_sql");
    }
}

#[test]
fn agg_misplaced_call() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"level": "error", "host": "a"}]));
    for sql in [
        "SELECT level FROM app WHERE count(*) > 1 GROUP BY level",
        "SELECT count(*) AS n FROM app GROUP BY n",
        "SELECT max(count(*)) FROM app",
        "SELECT count(DISTINCT host) FROM app",
    ] {
        s.sql_err(sql, 400, "sql_unsupported");
    }
}

#[test]
fn agg_histogram_not_grouped() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    s.sql_err(
        "SELECT histogram(_timestamp, '1h') AS h, count(*) AS n FROM app GROUP BY histogram(_timestamp, '5m')",
        400,
        "invalid_sql",
    );
    s.sql_err(
        "SELECT histogram(_timestamp, '1h') AS h FROM app GROUP BY _timestamp",
        400,
        "invalid_sql",
    );
    s.sql_ok(
        "SELECT histogram(_timestamp, '60m') AS h, count(*) AS n FROM app GROUP BY histogram(_timestamp, '1h')",
    );
}

// ---------------------------------------------------------------------------
// overflow
// ---------------------------------------------------------------------------

#[test]
fn agg_int_sum_overflow() {
    let (_f, s) = start();
    ingest_text(&s, "app", r#"[{"n":9223372036854775807},{"n":1}]"#);
    s.sql_err("SELECT sum(n) AS s FROM app", 400, "numeric_overflow");
    assert_eq!(
        hits(&s.sql_ok("SELECT count(n) AS c FROM app")),
        [json!({"c": 2})]
    );
}

#[test]
fn agg_float_sum_not_finite() {
    let (_f, s) = start();
    ingest_text(&s, "app", r#"[{"f":1.5e308},{"f":1.5e308}]"#);
    s.sql_err("SELECT sum(f) AS s FROM app", 400, "numeric_overflow");
}

#[test]
fn agg_overflow_checked_for_all_groups() {
    let (_f, s) = start();
    ingest_text(
        &s,
        "app",
        r#"[{"g":"a","n":9223372036854775807},{"g":"a","n":1},{"g":"b","n":1}]"#,
    );
    s.sql_err(
        "SELECT g, sum(n) AS s FROM app GROUP BY g ORDER BY g DESC LIMIT 1",
        400,
        "numeric_overflow",
    );
    s.search(&query(
        "SELECT g, sum(n) AS s FROM app GROUP BY g ORDER BY g DESC",
        ALL,
        &json!({"size": 1}),
    ))
    .assert_envelope(400, "numeric_overflow");
}

// ---------------------------------------------------------------------------
// response
// ---------------------------------------------------------------------------

#[test]
fn agg_total_and_scan_stats() {
    let (f, s) = start_with(&[("flush.interval", Some("\"1s\""))]);
    s.ingest_ok(
        "app",
        &json!([{"level": "a"}, {"level": "b"}, {"level": "c"}, {"level": "a"}, {"level": "b"}]),
    );
    let chain = Chain::file(&f);
    let snapshot = chain.wait_record_count("app", 5);
    let files = chain.files("app", &snapshot);
    assert_eq!(files.len(), 1, "{files:?}");
    let r = s.search(&query(
        "SELECT level, count(*) AS n FROM app GROUP BY level",
        ALL,
        &json!({"size": 2}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(hits(&v).len(), 2, "{v}");
    assert_eq!(v["total"], 2, "{v}");
    assert_eq!(v["scan_files"], 1, "{v}");
    assert_eq!(v["scan_records"], 5, "{v}");
    assert_eq!(v["scan_bytes"], files[0]["size_bytes"], "{v}");
}
