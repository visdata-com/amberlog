//! REQ-AMB-QRY-SEARCH-ENDPOINT-V5 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查询。
//! 对象存储用 `file://` 临时目录；「对象存储停止」用
//! MinIO 台架加本进程里的 TCP 转发（见 `common::s3`）。
//!
//! 场景「子集之外的结构一律 sql_unsupported」是 amberlog-query 的单元测试（`search_sql_unsupported_table`）。

mod common;
mod qry_common;

use std::collections::BTreeSet;

use common::api::{HOUR_US, MINUTE_US, now_us};
use common::gov::{entry, write_counters};
use common::s3::S3Instance;
use common::{Fixture, Server};
use qry_common::{ALL, Chain, column, early, hits, query};
use serde_json::{Value, json};

fn start() -> (Fixture, Server) {
    start_with(&[])
}

fn start_with(overrides: &[common::Kv]) -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(overrides);
    (f, s)
}

/// 各 hit 中 `col` 的取值集合（字符串）。
fn set_of(body: &Value, col: &str) -> BTreeSet<String> {
    column(body, col).iter().map(|v| v.to_string()).collect()
}

// ---------------------------------------------------------------------------
// request
// ---------------------------------------------------------------------------

#[test]
fn search_time_range_half_open() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": early(1000)},
            {"_timestamp": early(2000)},
            {"_timestamp": early(3000)},
        ]),
    );
    let r = s.search(&query(
        "SELECT _timestamp FROM app",
        (1000, 3000),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(column(&v, "_timestamp"), [json!(2000), json!(1000)]);
    assert_eq!(v["total"], 2);
}

#[test]
fn search_timezone_validation() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    let range = (0, 3_600_000_000);
    for tz in [json!("Mars/Olympus"), json!(8), json!("+08:00")] {
        let r = s.search(&query("SELECT * FROM app", range, &json!({"timezone": tz})));
        assert_eq!(r.status, 400, "{tz}：{}", r.text());
        let v = r.json();
        assert_eq!(v["reason"], "invalid_request", "{tz}");
        assert!(
            v["message"]
                .as_str()
                .is_some_and(|m| m.contains("timezone")),
            "{tz}：{v}"
        );
    }
    for tz in ["asia/shanghai", "UTC"] {
        let r = s.search(&query("SELECT * FROM app", range, &json!({"timezone": tz})));
        assert_eq!(r.status, 200, "{tz}：{}", r.text());
    }
}

#[test]
fn search_timezone_no_other_effect() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": early(1000)},
            {"_timestamp": early(2000)},
            {"_timestamp": early(3000)},
        ]),
    );
    for extra in [json!({}), json!({"timezone": "Asia/Shanghai"})] {
        let r = s.search(&query("SELECT _timestamp FROM app", (1000, 3000), &extra));
        assert_eq!(r.status, 200, "{extra}：{}", r.text());
        assert_eq!(
            column(&r.json(), "_timestamp"),
            [json!(2000), json!(1000)],
            "{extra}"
        );
    }
    let r = s.search(&query(
        "SELECT * FROM _ledger",
        ALL,
        &json!({"timezone": "Asia/Shanghai"}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn search_size_type_and_defaults() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    let r = s.search(&query("SELECT * FROM app", ALL, &json!({"size": "10"})));
    r.assert_envelope(400, "invalid_request");
    let r = s.search(&query("SELECT * FROM app", ALL, &json!({})));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(
        (v["size"].clone(), v["from"].clone()),
        (json!(100), json!(0))
    );
}

#[test]
fn search_request_validation_table() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    let q = r#""sql":"SELECT * FROM app","start_time":0,"end_time":4102444800000000"#;
    let rows: Vec<(String, u16, Option<&str>)> = vec![
        ("[]".into(), 400, Some("invalid_request")),
        ("{}".into(), 400, Some("invalid_request")),
        (
            r#"{"query":"SELECT * FROM app"}"#.into(),
            400,
            Some("invalid_request"),
        ),
        (
            r#"{"query":{"start_time":0,"end_time":10}}"#.into(),
            400,
            Some("invalid_request"),
        ),
        (
            r#"{"query":{"sql":1,"start_time":0,"end_time":10}}"#.into(),
            400,
            Some("invalid_request"),
        ),
        (
            r#"{"query":{"sql":"SELECT * FROM app","end_time":10}}"#.into(),
            400,
            Some("invalid_request"),
        ),
        (
            r#"{"query":{"sql":"SELECT * FROM app","start_time":"0","end_time":10}}"#.into(),
            400,
            Some("invalid_request"),
        ),
        (
            format!(r#"{{"query":{{{q},"size":-1}}}}"#),
            400,
            Some("invalid_request"),
        ),
        (
            format!(r#"{{"query":{{{q},"size":10001}}}}"#),
            400,
            Some("invalid_request"),
        ),
        (
            format!(r#"{{"query":{{{q},"from":-1}}}}"#),
            400,
            Some("invalid_request"),
        ),
        (
            format!(r#"{{"query":{{{q},"from":"0"}}}}"#),
            400,
            Some("invalid_request"),
        ),
        (format!(r#"{{"query":{{{q},"extra":1}},"x":2}}"#), 200, None),
    ];
    for (body, status, reason) in rows {
        let r = s.search_raw(body.as_bytes());
        assert_eq!(r.status, status, "{body}：{}", r.text());
        match reason {
            Some(reason) => {
                r.assert_envelope(status, reason);
            }
            None => assert!(r.json()["hits"].is_array(), "{body}"),
        }
    }
}

#[test]
fn search_time_range_invalid_cases() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    for range in [(5, 5), (-1, 10)] {
        s.search(&query("SELECT * FROM app", range, &json!({})))
            .assert_envelope(400, "invalid_time_range");
    }
    let r = s.search(&query("SELECT * FROM app", (0, 1), &json!({})));
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------------------------------------------------------------------------
// sql-subset
// ---------------------------------------------------------------------------

#[test]
fn search_parentheses_and_reference_grammar() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"level": "error", "host": "a"},
            {"level": "info", "host": "b"},
            {"level": "error", "host": "c"},
        ]),
    );
    let v = s.sql_ok("SELECT host FROM app WHERE level = 'error' AND (host = 'a' OR host = 'b')");
    assert_eq!(hits(&v), [json!({"host": "a"})]);
    s.sql_err("SELECT `host` FROM app", 400, "invalid_sql");
    s.sql_err("SELECT host::text FROM app", 400, "sql_unsupported");
}

#[test]
fn search_reference_grammar_postgresql_17() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    for sql in [
        "SELECT a FROM (SELECT a FROM app)",
        "SELECT a FROM app WHERE a IS JSON",
        "SELECT JSON_VALUE(a, '$.x' RETURNING int) FROM app",
    ] {
        s.sql_err(sql, 400, "sql_unsupported");
    }
    for sql in [
        "SELECT 1abc FROM app",
        "INSERT INTO app VALUES (1) RETURNING WITH (OLD AS o) o.a",
    ] {
        s.sql_err(sql, 400, "invalid_sql");
    }
}

#[test]
fn search_quoted_identifiers() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"Host": "A", "host.name": "n"}]));
    let v = s.sql_ok(r#"SELECT "Host", "host.name" FROM app"#);
    let hits = hits(&v);
    assert!(!hits.is_empty());
    for h in hits {
        assert!(
            h.get("Host").is_some() && h.get("host.name").is_some(),
            "{h}"
        );
    }
}

#[test]
fn search_numeric_literal_comparable() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"latency": 150.5, "code": 200}]));
    let v = s.sql_ok("SELECT code FROM app WHERE latency > 100 AND code = 200.0");
    assert_eq!(hits(&v).len(), 1);
}

#[test]
fn search_exact_numeric_comparison() {
    let (_f, s) = start();
    let r = s.ingest_raw("app", br#"[{"id":9007199254740993}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let v = s.sql_ok("SELECT id FROM app WHERE id = 9007199254740992.0");
    assert_eq!(hits(&v), Vec::<Value>::new());
}

#[test]
fn search_float_literal_rounding() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"ratio": 0.1}]));
    for (cond, n) in [
        ("ratio = 0.1", 1),
        ("ratio <= 0.1", 1),
        ("ratio > 0.1", 0),
        ("ratio = 0.10000000000000001", 1),
    ] {
        let v = s.sql_ok(&format!("SELECT ratio FROM app WHERE {cond}"));
        assert_eq!(v["total"], n, "{cond}");
    }
}

#[test]
fn search_literal_literal_comparison() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    for (cond, n) in [
        ("1 = 1.0 AND 'a' < 'b' AND FALSE < TRUE", 1),
        ("2 IN (1, 3)", 0),
    ] {
        let v = s.sql_ok(&format!("SELECT a FROM app WHERE {cond}"));
        assert_eq!(v["total"], n, "{cond}");
    }
    for cond in ["1 = 'a'", "TRUE = 1"] {
        s.sql_err(
            &format!("SELECT a FROM app WHERE {cond}"),
            400,
            "invalid_sql",
        );
    }
}

#[test]
fn search_like_backslash_escape() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"m": "disk 50% full"}, {"m": "disk 500 full"}]),
    );
    let v = s.sql_ok(r"SELECT m FROM app WHERE m LIKE '%50\%%'");
    assert_eq!(hits(&v), [json!({"m": "disk 50% full"})]);
}

#[test]
fn search_order_by_null_position() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"latency": 30}, {"x": 1}, {"latency": 120}]));
    let v = s.sql_ok("SELECT latency FROM app ORDER BY latency ASC");
    assert_eq!(
        hits(&v),
        [json!({"latency": 30}), json!({"latency": 120}), json!({})]
    );
    let v = s.sql_ok("SELECT latency FROM app ORDER BY latency DESC");
    assert_eq!(
        hits(&v),
        [json!({}), json!({"latency": 120}), json!({"latency": 30})]
    );
}

#[test]
fn search_boolean_order() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"ok": true, "k": 1}, {"ok": false, "k": 2}]));
    let v = s.sql_ok("SELECT k FROM app ORDER BY ok");
    assert_eq!(hits(&v), [json!({"k": 2}), json!({"k": 1})]);
}

#[test]
fn search_float_signed_zero_sort_key() {
    let (_f, s) = start();
    let r = s.ingest_raw(
        "app",
        br#"[{"f":-0.0,"k":1},{"f":0.0,"k":2},{"f":-1.5,"k":3}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let v = s.sql_ok("SELECT k FROM app ORDER BY f");
    let h = hits(&v);
    assert_eq!(h.len(), 3, "{v}");
    assert_eq!(h[0], json!({"k": 3}), "{v}");
    let rest: BTreeSet<String> = h[1..].iter().map(Value::to_string).collect();
    assert_eq!(
        rest,
        BTreeSet::from([r#"{"k":1}"#.to_owned(), r#"{"k":2}"#.to_owned()]),
        "{v}"
    );
    // 场景之外（测试约定）：「同一排序键」要在有第二个排序键时才分得出——`-0.0` 与 `0.0` 两行之间由第二个键定先后，
    // 非聚合与聚合两条执行路径各核对一次
    assert_eq!(
        column(&s.sql_ok("SELECT k FROM app ORDER BY f, k DESC"), "k"),
        [json!(3), json!(2), json!(1)]
    );
    assert_eq!(
        column(&s.sql_ok("SELECT k FROM app ORDER BY f DESC, k"), "k"),
        [json!(1), json!(2), json!(3)]
    );
    let r = s.ingest_raw(
        "agz",
        br#"[{"g":"a","f":-0.0},{"g":"b","f":0.0},{"g":"c","f":-1.5}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let v = s.sql_ok("SELECT g, min(f) AS m FROM agz GROUP BY g ORDER BY m, g DESC");
    assert_eq!(column(&v, "g"), [json!("c"), json!("b"), json!("a")], "{v}");
    let v = s.sql_ok("SELECT g, min(f) AS m FROM agz GROUP BY g ORDER BY m DESC, g");
    assert_eq!(column(&v, "g"), [json!("a"), json!("b"), json!("c")], "{v}");
}

#[test]
fn search_string_codepoint_order() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"host": "apple"}, {"host": "Zebra"}, {"host": "éclair"}]),
    );
    let v = s.sql_ok("SELECT host FROM app ORDER BY host");
    assert_eq!(
        column(&v, "host"),
        [json!("Zebra"), json!("apple"), json!("éclair")]
    );
    let v = s.sql_ok("SELECT host FROM app WHERE host > 'B'");
    assert_eq!(
        set_of(&v, "host"),
        BTreeSet::from(["\"Zebra\"".into(), "\"apple\"".into(), "\"éclair\"".into()])
    );
    assert_eq!(hits(&v).len(), 3);
}

#[test]
fn search_sql_lexical_rules() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1, "m": "x"}]));
    for sql in [
        "SELECT a FROM app;",
        "SELECT a /* c */ FROM app -- tail",
        "SELECT A FROM APP",
    ] {
        assert_eq!(hits(&s.sql_ok(sql)), [json!({"a": 1})], "{sql}");
    }
    s.sql_err(r#"SELECT "A" FROM app"#, 400, "unknown_column");
}

#[test]
fn search_invalid_sql_type_checks() {
    let (_f, s) = start();
    s.ingest_ok("tc", &json!([{"a": 1, "f": 1.5, "b": true, "m": "x"}]));
    for cond in [
        "a = 'x'",
        "a = f",
        "b = 1",
        "m = 1",
        "a = NULL",
        "a IN (1, NULL)",
        "a LIKE 'x'",
        r"m LIKE 'x\'",
    ] {
        s.sql_err(
            &format!("SELECT a FROM tc WHERE {cond}"),
            400,
            "invalid_sql",
        );
    }
    for cond in ["a = 1.0", "f > 1", "b = TRUE", "m NOT LIKE 'y%'"] {
        s.sql_ok(&format!("SELECT a FROM tc WHERE {cond}"));
    }
}

#[test]
fn search_string_and_like_semantics() {
    let (_f, s) = start();
    s.ingest_ok(
        "sl",
        &json!([{"m": r"a\b"}, {"m": "it's"}, {"m": "ab"}, {"m": "AB"}, {"m": "é1"}]),
    );
    for (cond, want) in [
        (r"m = 'a\b'", r"a\b"),
        ("m = 'it''s'", "it's"),
        ("m LIKE 'a_'", "ab"),
        ("m LIKE '_1'", "é1"),
        (r"m LIKE 'a\\b'", r"a\b"),
    ] {
        let v = s.sql_ok(&format!("SELECT m FROM sl WHERE {cond}"));
        assert_eq!(hits(&v), [json!({ "m": want })], "{cond}");
    }
}

#[test]
fn search_where_operators() {
    let (_f, s) = start();
    let mut records: Vec<Value> = (1..=5).map(|a| json!({"a": a, "m": "x"})).collect();
    records.push(json!({"a": 6}));
    s.ingest_ok("wo", &Value::Array(records));
    for (cond, n) in [
        ("NOT (a = 1)", 5),
        ("a <> 2 AND a != 3", 4),
        ("a NOT IN (1, 2)", 4),
        ("a IN (+1, -1)", 1),
        ("a <= 2 OR a >= 5", 4),
        ("m IS NULL", 1),
        ("m IS NOT NULL", 5),
    ] {
        let v = s.sql_ok(&format!("SELECT a FROM wo WHERE {cond}"));
        assert_eq!(v["total"], n, "{cond}");
    }
}

// ---------------------------------------------------------------------------
// names
// ---------------------------------------------------------------------------

#[test]
fn search_output_column_names() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"host": "a", "latency": 10}, {"host": "a", "latency": 30}]),
    );
    let r = s.sql("SELECT host AS h, count(*), max(latency) FROM app GROUP BY host");
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()), [json!({"h": "a", "count": 2, "max": 30})]);
    // 键的先后依次为 h、count、max（按响应原文）
    assert!(
        r.text().contains(r#"[{"h":"a","count":2,"max":30}]"#),
        "{}",
        r.text()
    );
    s.sql_err("SELECT host AS _h FROM app", 400, "sql_unsupported");
}

/// 别名为关键字：`AS` 后面那个记号总是别名（PostgreSQL 17 收 `AS from`）。不对应块场景。
#[test]
fn search_alias_keyword_labels() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1, "b": 2}]));
    let r = s.sql("SELECT a AS from, b AS where, a AS select FROM app");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text()
            .contains(r#""hits":[{"from":1,"where":2,"select":1}]"#),
        "{}",
        r.text()
    );
}

#[test]
fn search_duplicate_output_names() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"host": "a"}]));
    s.sql_err("SELECT count(*), count(host) FROM app", 400, "invalid_sql");
    // 按响应原文比对：解析成 JSON 会把重复的键合并，查不出「写了两个同名键」
    for (sql, want) in [
        ("SELECT host, host FROM app", r#""hits":[{"host":"a"}]"#),
        (
            "SELECT count(*), count(*) FROM app",
            r#""hits":[{"count":1}]"#,
        ),
    ] {
        let r = s.sql(sql);
        assert_eq!(r.status, 200, "{sql}：{}", r.text());
        assert!(r.text().contains(want), "{sql}：{}", r.text());
    }
}

#[test]
fn search_name_resolution_group_order() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"a": 1, "b": 2}, {"a": 1, "b": 3}, {"a": 2, "b": 3}]),
    );
    let v = s.sql_ok("SELECT b AS a, count(*) AS n FROM app GROUP BY a ORDER BY a");
    assert_eq!(hits(&v), [json!({"a": 2, "n": 1}), json!({"a": 3, "n": 2})]);
    let r = s.sql("SELECT a AS b, b AS a FROM app WHERE a = 1 ORDER BY a DESC");
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(
        r.text().contains(r#"[{"b":1,"a":3},{"b":1,"a":2}]"#),
        "{}",
        r.text()
    );
}

#[test]
fn search_name_resolution_errors() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1, "level": "error"}]));
    for (sql, reason) in [
        ("SELECT a AS b FROM app WHERE b = 1", "unknown_column"),
        ("SELECT count(b) AS b FROM app", "unknown_column"),
        ("SELECT count(*) AS n FROM app GROUP BY x", "unknown_column"),
        ("SELECT a FROM app ORDER BY x", "unknown_column"),
        (
            "SELECT count(*) AS x, level AS x FROM app GROUP BY x",
            "invalid_sql",
        ),
    ] {
        s.sql_err(sql, 400, reason);
    }
    s.sql_ok("SELECT a AS x FROM app ORDER BY x");
}

// ---------------------------------------------------------------------------
// response
// ---------------------------------------------------------------------------

#[test]
fn search_envelope_fields() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}, {"b": "x"}]));
    let v = s.sql_ok("SELECT a, b FROM app");
    for key in [
        "took",
        "hits",
        "total",
        "from",
        "size",
        "scan_records",
        "scan_files",
        "scan_bytes",
    ] {
        assert!(v.get(key).is_some(), "缺 {key}：{v}");
    }
    let a = hits(&v)
        .into_iter()
        .find(|h| h.get("a") == Some(&json!(1)))
        .expect("有 a 为 1 的 hit");
    assert_eq!(a, json!({"a": 1}));
}

/// 成功响应顶层八个字段按响应原文的写出次序：`took, hits, total, from, size, scan_records, scan_files,
/// scan_bytes`。块未规定先后，本测试不对应块场景，只防这一次序在无意中改变。
#[test]
fn search_envelope_top_level_order() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    let r = s.sql("SELECT a FROM app");
    assert_eq!(r.status, 200, "{}", r.text());
    let text = r.text();
    assert!(text.starts_with(r#"{"took":"#), "{text}");
    let keys = [
        "took",
        "hits",
        "total",
        "from",
        "size",
        "scan_records",
        "scan_files",
        "scan_bytes",
    ];
    let at: Vec<usize> = keys
        .iter()
        .map(|k| {
            let pat = format!("\"{k}\":");
            assert_eq!(text.matches(&pat).count(), 1, "{pat} 应恰出现一次：{text}");
            text.find(&pat).expect("已核对出现一次")
        })
        .collect();
    assert!(at.windows(2).all(|w| w[0] < w[1]), "{text}");
}

#[test]
fn search_hit_value_rendering() {
    let (_f, s) = start();
    let r = s.ingest_raw(
        "hv",
        br#"[{"_timestamp":1700000000000000,"i":7,"f":100.0,"g":1e300,"b":false,"s":"x"}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let r = s.sql("SELECT _timestamp, i, f, g, b, s FROM hv");
    assert_eq!(r.status, 200, "{}", r.text());
    let text = r.text();
    for piece in [
        r#""_timestamp":1700000000000000"#,
        r#""i":7"#,
        r#""f":100.0"#,
        r#""g":1e300"#,
        r#""b":false"#,
        r#""s":"x""#,
    ] {
        assert!(text.contains(piece), "应含 {piece}：{text}");
    }
}

#[test]
fn search_scan_statistics() {
    let (f, s) = start_with(&[
        ("flush.interval", Some("\"1h\"")),
        ("flush.max_records", Some("3")),
    ]);
    s.ingest_ok("st", &json!([{"n": 1}, {"n": 2}, {"n": 3}]));
    let chain = Chain::file(&f);
    let snapshot = chain.wait_record_count("st", 3);
    let files = chain.files("st", &snapshot);
    assert_eq!(files.len(), 1, "{files:?}");
    s.ingest_ok("st", &json!([{"n": 4}, {"n": 5}]));
    let v = s.sql_ok("SELECT * FROM st");
    assert_eq!(v["scan_files"], 1, "{v}");
    assert_eq!(v["scan_bytes"], files[0]["size_bytes"], "{v}");
    assert_eq!(v["scan_records"], 5, "{v}");
}

// ---------------------------------------------------------------------------
// errors
// ---------------------------------------------------------------------------

#[test]
fn search_error_report_order() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    s.search(&query("SELEC * FROM app", (3000, 1000), &json!({})))
        .assert_envelope(400, "invalid_time_range");
    s.search(&query("SELECT typo FROM no_such", (0, 1), &json!({})))
        .assert_envelope(400, "stream_not_found");
}

#[test]
fn search_inverted_range() {
    let (_f, s) = start();
    s.search(&query("SELECT * FROM app", (3000, 1000), &json!({})))
        .assert_envelope(400, "invalid_time_range");
}

/// `SELECT count(*) FROM app` 是合法的，这里用含 `HAVING` 的查询。
#[test]
fn search_sql_unsupported() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"level": "error"}]));
    s.sql_err(
        "SELECT level FROM app GROUP BY level HAVING count(*) > 1",
        400,
        "sql_unsupported",
    );
}

#[test]
fn search_limit_literal_and_empty_sql() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    for sql in [
        "SELECT a FROM app LIMIT -1",
        "SELECT a FROM app LIMIT 1.5",
        "SELECT a FROM app LIMIT ALL",
        "SELECT a FROM app WHERE a = 0x1",
        "SELECT a FROM app;;",
    ] {
        s.sql_err(sql, 400, "sql_unsupported");
    }
    s.sql_err("-- only a comment", 400, "invalid_sql");
}

#[test]
fn search_invalid_sql_and_unknown_stream() {
    let (_f, s) = start();
    s.sql_err("SELEC * FROM app", 400, "invalid_sql");
    s.sql_err("SELECT * FROM no_such_stream", 400, "stream_not_found");
}

#[test]
fn search_unknown_column() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    s.sql_err("SELECT typo FROM app", 400, "unknown_column");
}

#[test]
fn search_error_order_table() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    s.search(&query("SELEC", (3000, 1000), &json!({"size": -1})))
        .assert_envelope(400, "invalid_request");
    // `count(*)` 合法，期望 `stream_not_found`；以 `lower(a)` 核对「子集外先于 stream 不存在」
    for (sql, reason) in [
        ("SELECT lower(a) FROM no_such", "sql_unsupported"),
        ("SELECT count(*) FROM no_such", "stream_not_found"),
        ("SELECT a FROM _sys", "stream_not_found"),
        ("SELECT typo FROM app WHERE a = 'x'", "unknown_column"),
    ] {
        s.sql_err(sql, 400, reason);
    }
}

#[test]
fn search_numeric_overflow_report_order() {
    let (_f, s) = start();
    let r = s.ingest_raw("ov", br#"[{"n":9223372036854775807},{"n":1}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    s.sql_err(
        "SELECT sum(n) AS s, max(typo) AS m FROM ov",
        400,
        "unknown_column",
    );
    s.sql_err(
        "SELECT sum(n) AS s, count(*) AS s FROM ov",
        400,
        "invalid_sql",
    );
    s.sql_err("SELECT sum(n) AS s FROM ov", 400, "numeric_overflow");
}

// ---------------------------------------------------------------------------
// storage（MinIO 台架）
// ---------------------------------------------------------------------------

#[test]
fn search_unknown_stream_object_store_down() {
    let inst = S3Instance::new();
    let s = inst.start(&[]);
    inst.relay.stop();
    s.sql_err("SELECT * FROM ghost", 400, "stream_not_found");
}

#[test]
fn search_timestamp_before_genesis_commit() {
    let inst = S3Instance::new();
    let s = inst.start(&[]);
    inst.relay.stop();
    let r = s.ingest("app5", &json!([{"_x": 1}]));
    assert_eq!(r.status, 400, "{}", r.text());
    let v = s.sql_ok("SELECT _timestamp FROM app5");
    assert_eq!(hits(&v), Vec::<Value>::new());
    assert_eq!(v["total"], 0);
}

#[test]
fn search_object_store_unavailable() {
    let inst = S3Instance::new();
    let s = inst.start(&[("flush.max_records", Some("1"))]);
    s.ingest_ok("app", &json!([{"k": 1}]));
    let chain = Chain::s3(&inst);
    let snapshot = chain.wait_record_count("app", 1);
    assert_eq!(chain.files("app", &snapshot).len(), 1);
    // 此前没有查询过 app：已提交的数据文件都未被本进程读取过
    inst.relay.stop();
    s.sql_err("SELECT * FROM app", 503, "unavailable");
}

#[test]
fn search_object_store_access_denied() {
    let inst = S3Instance::with_app_user();
    let s = inst.start(&[("flush.max_records", Some("1"))]);
    s.ingest_ok("app", &json!([{"k": 1}]));
    let chain = Chain::s3(&inst);
    let snapshot = chain.wait_record_count("app", 1);
    let files = chain.files("app", &snapshot);
    assert_eq!(files.len(), 1);
    // 实例已就绪；此后 amberlog/default/app/ 下对象的读取以 403 拒绝。此前没有查询过 app：数据文件未被本进程读取过
    assert!(inst.deny_reads("default/app/") > 0);
    denied_for_instance(&inst, files[0]["path"].as_str().unwrap());
    s.sql_err("SELECT * FROM app", 503, "unavailable");
}

/// 以被测进程的凭据读数据集 `app` 下的对象 `rel`，确认台架给出的是 403。
fn denied_for_instance(inst: &S3Instance, rel: &str) {
    use object_store::ObjectStoreExt;
    let store = inst.store_as_instance();
    let path = store.object_path(
        &amberlog_common::dataset::Dataset::new("default", "app"),
        rel,
    );
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    match rt.block_on(store.inner.get(&path)) {
        Err(object_store::Error::PermissionDenied { .. }) => {}
        other => panic!("{path} 应以 403 拒绝读取：{other:?}"),
    }
}

// ---------------------------------------------------------------------------
// result-set
// ---------------------------------------------------------------------------

#[test]
fn search_default_order_before_limit() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": early(1000)},
            {"_timestamp": early(2000)},
            {"_timestamp": early(3000)},
            {"_timestamp": early(4000)},
        ]),
    );
    let r = s.search(&query(
        "SELECT _timestamp FROM app LIMIT 2",
        (0, 5000),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(column(&r.json(), "_timestamp"), [json!(4000), json!(3000)]);
}

#[test]
fn search_limit_vs_size() {
    let (_f, s) = start();
    let records: Vec<Value> = (0..10).map(|n| json!({"n": n})).collect();
    s.ingest_ok("app", &Value::Array(records));
    let r = s.search(&query(
        "SELECT * FROM app LIMIT 3",
        ALL,
        &json!({"size": 100}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()).len(), 3);
}

#[test]
fn search_from_size_pagination() {
    let (_f, s) = start();
    let records: Vec<Value> = (1..=5)
        .map(|n| json!({"_timestamp": early(n * 1000)}))
        .collect();
    s.ingest_ok("pg", &Value::Array(records));
    let range = (0, 10_000);

    let r = s.search(&query(
        "SELECT _timestamp FROM pg",
        range,
        &json!({"size": 2, "from": 1}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(column(&v, "_timestamp"), [json!(4000), json!(3000)]);
    assert_eq!(
        (v["from"].clone(), v["size"].clone(), v["total"].clone()),
        (json!(1), json!(2), json!(2))
    );

    let r = s.search(&query(
        "SELECT _timestamp FROM pg LIMIT 3",
        range,
        &json!({"size": 10, "from": 2}),
    ));
    let v = r.json();
    assert_eq!(column(&v, "_timestamp"), [json!(3000)]);
    assert_eq!(v["size"], 10);

    let r = s.search(&query(
        "SELECT _timestamp FROM pg ORDER BY _timestamp ASC",
        range,
        &json!({"size": 2}),
    ));
    assert_eq!(column(&r.json(), "_timestamp"), [json!(1000), json!(2000)]);
}

#[test]
fn search_order_by_multi() {
    let (_f, s) = start();
    s.ingest_ok(
        "ob",
        &json!([{"g": "a", "v": 2}, {"g": "b", "v": 1}, {"g": "a", "v": 1}]),
    );
    let v = s.sql_ok("SELECT g, v FROM ob ORDER BY g, v DESC");
    assert_eq!(
        hits(&v),
        [
            json!({"g": "a", "v": 2}),
            json!({"g": "a", "v": 1}),
            json!({"g": "b", "v": 1}),
        ]
    );
}

// ---------------------------------------------------------------------------
// system-table
// ---------------------------------------------------------------------------

/// 以 `flush.max_records` 为 1 启动，依次向各 stream 摄取一条记录并等它提交。
fn start_committed(streams: &[&str]) -> (Fixture, Server) {
    let (f, s) = start_with(&[("flush.max_records", Some("1"))]);
    let chain = Chain::file(&f);
    for st in streams {
        s.ingest_ok(st, &json!([{"k": 1}]));
        chain.wait_record_count(st, 1);
    }
    (f, s)
}

#[test]
fn search_system_table() {
    let (_f, s) = start_committed(&["app"]);
    let v = s.sql_ok("SELECT dataset, record_count FROM _ledger");
    assert!(
        hits(&v).iter().any(|h| h["dataset"] == "default/app"),
        "{v}"
    );
    for k in ["scan_records", "scan_files", "scan_bytes"] {
        assert_eq!(v[k], 0, "{k}");
    }
}

#[test]
fn search_unknown_system_table() {
    let (_f, s) = start();
    let r = s.search(&query("SELECT * FROM _nosuch", (0, 1), &json!({})));
    r.assert_envelope(400, "stream_not_found");
}

#[test]
fn search_system_table_unknown_column() {
    let (_f, s) = start_committed(&["app"]);
    // `GROUP BY` 合法，`SELECT nosuch … GROUP BY nosuch` 期望 `unknown_column`，放在第四次；
    // 以 `lower(nosuch)` 核对「子集外结构先于 unknown_column」
    s.sql_err("SELECT _timestamp FROM _ledger", 400, "unknown_column");
    s.sql_err("SELECT lower(nosuch) FROM _ledger", 400, "sql_unsupported");
    s.sql_err(
        "SELECT dataset FROM _ledger WHERE dataset = 1",
        400,
        "invalid_sql",
    );
    s.sql_err(
        "SELECT nosuch FROM _ledger GROUP BY nosuch",
        400,
        "unknown_column",
    );
}

#[test]
fn search_system_table_aggregate() {
    let (_f, s) = start_committed(&["app", "web"]);
    let v = s.sql_ok("SELECT status, count(*) AS n FROM _ledger GROUP BY status");
    // 夹具只有 `app` 与 `web` 两个数据集：断言比场景的「不小于 2」更严，恰为一组、`n` 恰为 2（测试约定）
    assert_eq!(hits(&v), [json!({"status": "active", "n": 2})], "{v}");
}

#[test]
fn search_system_table_default_order_before_limit() {
    let (f, s) = start_committed(&["web", "app"]);
    let r = s.search(&query(
        "SELECT dataset FROM _ledger LIMIT 1",
        (0, 1),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()), [json!({"dataset": "default/app"})]);
    // 场景之外（测试约定）：数据集登记表本来就按标识排好序，上面一句分不出排序先于还是后于 LIMIT。
    // `_reconciliation` 生成时窗口按升序排列、缺省排序却是窗口降序，`LIMIT 1` 只在排序先于 LIMIT 时取到较晚的 W2。
    // 两个窗口取在 3 小时前，与进程自己写的计数对象不在同一小时
    let now = now_us();
    let w1 = now - now.rem_euclid(HOUR_US) - 3 * HOUR_US;
    let w2 = w1 + MINUTE_US;
    write_counters(
        &f,
        "app",
        w1,
        &[entry(w1, 1, 1, json!({})), entry(w2, 1, 1, json!({}))],
    );
    let r = s.search(&query(
        "SELECT window_start FROM _reconciliation WHERE dataset = 'default/app' LIMIT 1",
        (w1, w2 + MINUTE_US),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(hits(&r.json()), [json!({"window_start": w2})]);
}

/// 回归（不对应块场景）：超过 63 字节的字段名在 SQL 里按名引用，不因 PostgreSQL 词法分析的标识符截断而回 `unknown_column`。
#[test]
fn search_long_column_name_not_truncated() {
    let (_f, s) = start();
    let name = format!("{}_end", "f".repeat(120));
    s.ingest_ok("app", &json!([{ name.as_str(): 7 }]));
    let v = s.sql_ok(&format!(
        "SELECT \"{name}\" FROM app WHERE {} = 7 ORDER BY {name}",
        name.to_uppercase()
    ));
    assert_eq!(hits(&v), [json!({ name.as_str(): 7 })]);
}
