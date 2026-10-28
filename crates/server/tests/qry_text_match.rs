//! REQ-AMB-QRY-TEXT-MATCH 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查询。
//! 对象存储用 `file://` 临时目录。
//!
//! 场景「文本匹配调用的形态与位置」是 amberlog-query 的单元测试（`text_call_form_table`）。

mod common;
mod qry_common;

use common::{Fixture, Server};
use qry_common::{Chain, hits};
use serde_json::{Value, json};

fn start() -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(&[]);
    (f, s)
}

/// 以 `cond` 为 WHERE 条件查询 `SELECT <cols> FROM app` 的响应体。
fn where_(s: &Server, cols: &str, cond: &str) -> Value {
    s.sql_ok(&format!("SELECT {cols} FROM app WHERE {cond}"))
}

// ---------------------------------------------------------------------------
// match-all
// ---------------------------------------------------------------------------

#[test]
fn text_match_all_columns_ascii_case() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"msg": "Disk ERROR on sda"},
            {"host": "error-node-1"},
            {"msg": "ok", "code": 500},
            {"user": "Émile"},
        ]),
    );
    let totals: Vec<Value> = [
        "match_all('error')",
        "match_all('500')",
        "match_all('ÉMILE')",
        "match_all('émile')",
    ]
    .iter()
    .map(|c| where_(&s, "*", c)["total"].clone())
    .collect();
    assert_eq!(totals, [json!(2), json!(0), json!(1), json!(0)]);
}

#[test]
fn text_match_all_shadow_column() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"code": 500}]));
    s.ingest_ok("app", &json!([{"code": "E500"}]));
    let v = where_(&s, "*", "match_all('e500')");
    let h = hits(&v);
    assert_eq!(h.len(), 1, "{v}");
    assert_eq!(h[0]["code__utf8"], "E500", "{v}");
}

#[test]
fn text_match_all_empty_text() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"m": "x"}, {"m": ""}, {"n": 1}, {"b": true}]),
    );
    assert_eq!(where_(&s, "*", "match_all('')")["total"], 2);
}

#[test]
fn text_match_all_system_table() {
    let f = Fixture::new();
    let s = f.start(&[("flush.max_records", Some("1"))]);
    let chain = Chain::file(&f);
    s.ingest_ok("app", &json!([{"k": 1}, {"k": 2}]));
    chain.wait_record_count("app", 2);
    s.ingest_ok("web", &json!([{"k": 1}]));
    chain.wait_record_count("web", 1);
    let v = s.sql_ok("SELECT record_count FROM _ledger WHERE match_all('DEFAULT/APP')");
    assert_eq!(hits(&v), [json!({"record_count": 2})]);
}

// ---------------------------------------------------------------------------
// str-match
// ---------------------------------------------------------------------------

#[test]
fn text_str_match_case_and_literal() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"m": "50% done"}, {"m": "500 DONE"}, {"m": "5000 done"}]),
    );
    let totals: Vec<Value> = [
        "str_match(m, '50%')",
        "str_match(m, 'done')",
        "str_match(m, 'DONE')",
    ]
    .iter()
    .map(|c| where_(&s, "m", c)["total"].clone())
    .collect();
    assert_eq!(totals, [json!(1), json!(2), json!(1)]);
}

#[test]
fn text_not_str_match_null() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"m": "abc"}, {"x": 1}]));
    assert_eq!(
        hits(&where_(&s, "x", "NOT str_match(m, 'a')")),
        [json!({"x": 1})]
    );
    assert_eq!(
        hits(&where_(&s, "x", "NOT (m LIKE '%a%')")),
        Vec::<Value>::new()
    );
}

#[test]
fn text_underscore_backslash_literal() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"m": "a_b"}, {"m": "axb"}, {"m": "C:\\x"}]));
    assert_eq!(
        hits(&where_(&s, "m", "str_match(m, 'a_b')")),
        [json!({"m": "a_b"})]
    );
    assert_eq!(
        hits(&where_(&s, "m", "str_match(m, 'C:\\x')")),
        [json!({"m": "C:\\x"})]
    );
    assert_eq!(
        hits(&where_(&s, "m", "match_all('A_B')")),
        [json!({"m": "a_b"})]
    );
}

// ---------------------------------------------------------------------------
// validation
// ---------------------------------------------------------------------------

#[test]
fn text_invalid_arguments() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"m": "a", "code": 1}]));
    s.sql_err(
        "SELECT m FROM app WHERE str_match(code, '1')",
        400,
        "invalid_sql",
    );
    for sql in [
        "SELECT m FROM app WHERE str_match(m, m)",
        "SELECT m FROM app WHERE match_all('a') = TRUE",
        "SELECT match_all('a') FROM app",
        "SELECT m FROM app WHERE match_all(m, 'a')",
    ] {
        s.sql_err(sql, 400, "sql_unsupported");
    }
}
