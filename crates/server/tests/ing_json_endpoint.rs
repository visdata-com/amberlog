//! REQ-AMB-ING-JSON-ENDPOINT-V3 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 摄取、经 `_search` 查询；请求头、大小与合法 JSON 的共用规则另对 `_search` 与 `_sent` 核对。合法 JSON 的字节级判定（`ingest_json_validity_rules`）是 amberlog-ingest 的单元测试。

mod common;

use common::api::{
    ingest, ingest_ok, ingest_with, now_us, search, search_body, sent, sent_element,
};
use common::{BEARER, Fixture};

// ---------------------------------------------------------------------------
// 批次形状
// ---------------------------------------------------------------------------

#[test]
fn ingest_json_array_batch() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest_ok(&server, "app", r#"[{"a":1},{"a":2},{"a":3}]"#);
    assert_eq!(v["status"][0]["successful"], 3);
}

#[test]
fn ingest_json_single_object() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let v = ingest_ok(&server, "app", r#"{"a":1}"#);
    assert_eq!(v["status"][0]["successful"], 1);
}

#[test]
fn ingest_json_top_level_scalar() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "ts", r#"[{"a":1}]"#);
    for body in [r#""x""#, "5", "true", "null"] {
        let v = ingest(&server, "ts", body).assert_envelope(400, "all_records_rejected");
        let entry = &v["details"]["status"][0];
        assert_eq!(entry["failed"], 1, "{body}");
        assert_eq!(entry["errors"][0]["index"], 0, "{body}");
        assert_eq!(entry["errors"][0]["reason"], "not_an_object", "{body}");
    }
}

// ---------------------------------------------------------------------------
// 来源标识（V3）
// ---------------------------------------------------------------------------

const SOURCE: &str = "X-Amberlog-Source";

#[test]
fn ingest_source_header_accepted() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = ingest_with(&server, "app", &[(SOURCE, "fw-1")], br#"[{"a":1},{"a":2}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 2);
    let hits = search(&server, "SELECT * FROM app");
    assert_eq!(hits.len(), 2, "{hits:?}");
    for hit in &hits {
        let fields = hit.as_object().unwrap();
        assert!(
            fields.values().all(|v| v != "fw-1"),
            "记录不得含来源标识：{hit}"
        );
    }
}

#[test]
fn ingest_source_header_invalid() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"m":"seed"}]"#);
    let long = "a".repeat(129);
    let cases: [&[(&str, &str)]; 4] = [
        &[(SOURCE, "-fw")],
        &[(SOURCE, "")],
        &[(SOURCE, &long)],
        &[(SOURCE, "fw-1"), (SOURCE, "fw-2")],
    ];
    for headers in cases {
        ingest_with(&server, "app", headers, br#"[{"m":"src-bad"}]"#)
            .assert_envelope(400, "invalid_request");
    }
    let hits = search(&server, "SELECT m FROM app WHERE m = 'src-bad'");
    assert!(hits.is_empty(), "{hits:?}");
}

#[test]
fn ingest_source_header_json_only() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let r = server.request(
        "POST",
        "/api/default/app/_sent",
        &[("Authorization", BEARER), (SOURCE, "-fw")],
        br#"[{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":1}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    // `_search` 同样不看这个请求头
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER), (SOURCE, "-fw")],
        search_body("SELECT a FROM app").as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------------------------------------------------------------------------
// 编码、大小与内容类型
// ---------------------------------------------------------------------------

#[test]
fn ingest_json_unsupported_encoding() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    // 编码只凭请求头判定，请求体的内容无关紧要（测试进程没有 brotli 编码器）
    ingest_with(
        &server,
        "app",
        &[("Content-Encoding", "br")],
        br#"[{"a":1}]"#,
    )
    .assert_envelope(415, "unsupported_encoding");
}

/// 在各端点上以给定 `Content-Encoding` 提交各自合法的请求体（`None` 表示不带该头）。
fn post(server: &common::Server, path: &str, encoding: Option<&str>, body: &[u8]) -> common::Resp {
    let mut headers = vec![("Authorization", BEARER)];
    if let Some(e) = encoding {
        headers.push(("Content-Encoding", e));
    }
    server.request("POST", path, &headers, body)
}

#[test]
fn ingest_content_encoding_rules() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "ce", r#"[{"a":1}]"#);
    let w = now_us().div_euclid(60_000_000) * 60_000_000;
    let search = search_body("SELECT a FROM ce");
    let report = format!("[{}]", sent_element("a", w, 1));
    let rows: [(&str, &str, &[u8], u16); 5] = [
        ("/api/default/ce/_json", "identity", br#"[{"a":2}]"#, 200),
        ("/api/default/ce/_json", "", br#"[{"a":3}]"#, 200),
        ("/api/default/ce/_json", "br", br#"[{"a":4}]"#, 415),
        ("/api/default/_search", "br", search.as_bytes(), 415),
        ("/api/default/ce/_sent", "br", report.as_bytes(), 415),
    ];
    for (path, encoding, body, want) in rows {
        let r = post(&server, path, Some(encoding), body);
        assert_eq!(r.status, want, "{path} {encoding:?}：{}", r.text());
        if want == 415 {
            r.assert_envelope(415, "unsupported_encoding");
        }
    }
}

/// 以空白补足到恰 `n` 字节的请求体。
fn padded(json: &str, n: usize) -> Vec<u8> {
    assert!(json.len() <= n);
    let mut b = json.as_bytes().to_vec();
    b.resize(n, b' ');
    b
}

#[test]
fn ingest_max_body_bytes_boundary() {
    let fx = Fixture::new();
    let server = fx.start(&[("ingest.max_body_bytes", Some("1024"))]);
    ingest_ok(&server, "mb", r#"[{"a":1}]"#);
    let w = now_us().div_euclid(60_000_000) * 60_000_000;
    let search = search_body("SELECT a FROM mb");
    let report = format!("[{}]", sent_element("a", w, 1));
    let rows: [(&str, Vec<u8>, u16); 4] = [
        ("/api/default/mb/_json", padded(r#"[{"a":2}]"#, 1024), 200),
        ("/api/default/mb/_json", padded(r#"[{"a":3}]"#, 1025), 413),
        ("/api/default/_search", padded(&search, 1025), 413),
        ("/api/default/mb/_sent", padded(&report, 1025), 413),
    ];
    for (path, body, want) in rows {
        let r = post(&server, path, None, &body);
        assert_eq!(r.status, want, "{path} {} 字节：{}", body.len(), r.text());
        if want == 413 {
            r.assert_envelope(413, "payload_too_large");
        }
    }
}

#[test]
fn ingest_content_type_ignored() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "ct", r#"[{"a":1}]"#);
    for content_type in [
        Some("text/plain"),
        Some("application/x-www-form-urlencoded"),
        None,
    ] {
        let headers: Vec<(&str, &str)> = content_type
            .map(|t| ("Content-Type", t))
            .into_iter()
            .collect();
        let r = ingest_with(&server, "ct", &headers, br#"[{"a":1}]"#);
        assert_eq!(r.status, 200, "{content_type:?}：{}", r.text());
    }
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER), ("Content-Type", "text/plain")],
        search_body("SELECT a FROM ct").as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------------------------------------------------------------------------
// 合法 JSON 的界定
// ---------------------------------------------------------------------------

#[test]
fn ingest_json_invalid_body() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"b":0}]"#);
    ingest(&server, "app", r#"[{"a":1},{"a":"#).assert_envelope(400, "invalid_json");
    // 整体非法的请求体没有落库：`a` 列从未出现
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        search_body("SELECT a FROM app WHERE a = 1").as_bytes(),
    );
    assert!(
        r.status == 400 && r.json()["reason"] == "unknown_column",
        "{}",
        r.text()
    );
    assert_eq!(search(&server, "SELECT b FROM app").len(), 1);
}

/// 带有效凭据向 `_search` 提交原始请求体。
fn post_search(server: &common::Server, body: &[u8]) -> common::Resp {
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body,
    )
}

#[test]
fn ingest_search_body_number_overflow() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"a":0}]"#);
    post_search(
        &server,
        br#"{"query":{"sql":"SELECT * FROM app","start_time":0,"end_time":1e400}}"#,
    )
    .assert_envelope(400, "invalid_json");
}

#[test]
fn ingest_search_body_nesting_depth() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"a":0}]"#);
    // 顶层对象为第 1 层、`query` 为第 2 层，`deep` 的取值自第 3 层起：127 层数组使整体深度为 129
    let deep = |n: usize| format!("{}1{}", "[".repeat(n), "]".repeat(n));
    let body = |n: usize| {
        format!(
            r#"{{"query":{{"sql":"SELECT * FROM app","start_time":0,"end_time":{},"deep":{}}}}}"#,
            common::api::FAR_END_US,
            deep(n)
        )
    };
    post_search(&server, body(127).as_bytes()).assert_envelope(400, "invalid_json");
    // 对照：整体深度 128 时请求体是合法 JSON（多出的键不影响受理）
    let r = post_search(&server, body(126).as_bytes());
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn ingest_json_validity_applies_to_search_and_sent() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "jv", r#"[{"a":1}]"#);
    let mut bom = vec![0xEF, 0xBB, 0xBF];
    bom.extend_from_slice(search_body("SELECT a FROM jv").as_bytes());
    server
        .request(
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            &bom,
        )
        .assert_envelope(400, "invalid_json");
    sent(
        &server,
        "jv",
        r#"[{"source":"a","window_start":1700000040000000,"window_end":1700000100000000,"sent":1e400}]"#,
    )
    .assert_envelope(400, "invalid_json");
}

// ---------------------------------------------------------------------------
// 逐条处置
// ---------------------------------------------------------------------------

#[test]
fn ingest_append_only() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(
        &server,
        "app",
        r#"[{"_timestamp":1700000000000000,"m":"dup"},{"_timestamp":1700000000000000,"m":"dup"}]"#,
    );
    assert_eq!(
        search(&server, "SELECT m FROM app WHERE m = 'dup'").len(),
        2
    );
}
