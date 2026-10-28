//! REQ-AMB-AUTH-CREDENTIAL 的系统测试。
//!
mod common;

use common::api::{ingest_ok, minute_of, now_us, search_body, sent_element};
use common::{BEARER, Fixture};

/// 一条合法查询的请求体（查询端点落地前，认证失败的场景在路由之前就已作答）。
const QUERY: &[u8] = br#"{"query":{"sql":"SELECT * FROM app","start_time":0,"end_time":1}}"#;

#[test]
fn auth_token_bytewise_mismatch() {
    let f = Fixture::new();
    let s = f.start(&[]);
    for value in [
        "Bearer S3CRET",
        "Bearer s3cre",
        "Bearer s3cretX",
        "Bearer",
        "Bearers3cret",
        "s3cret",
    ] {
        let r = s.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", value)],
            QUERY,
        );
        assert_eq!(r.status, 401, "{value:?}");
        assert_eq!(r.json()["reason"], "unauthorized", "{value:?}");
    }
}

#[test]
fn auth_basic_rejected() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", "Basic czNjcmV0")],
        QUERY,
    );
    r.assert_envelope(401, "unauthorized");
}

#[test]
fn auth_query_param_rejected() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request("POST", "/api/default/_search?token=s3cret", &[], QUERY);
    assert_eq!(r.status, 401);
}

#[test]
fn auth_cookie_rejected() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Cookie", "token=s3cret")],
        QUERY,
    );
    r.assert_envelope(401, "unauthorized");
}

#[test]
fn auth_exempt_endpoints() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_eq!(s.get("/healthz").status, 200);
    assert_eq!(s.get("/metrics").status, 401);
}

#[test]
fn auth_zone_raw_path() {
    let f = Fixture::new();
    let s = f.start(&[]);
    s.request("POST", "/%61pi/default/_search", &[], QUERY)
        .assert_envelope(404, "not_found");
}

#[test]
fn auth_duplicate_header_rejected() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER), ("Authorization", "Bearer wrong")],
        QUERY,
    );
    r.assert_envelope(401, "unauthorized");
}

#[test]
fn auth_duplicate_identical_header_rejected() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER), ("Authorization", BEARER)],
        QUERY,
    );
    r.assert_envelope(401, "unauthorized");
}

const AUTH_OFF: &[common::Kv] = &[("auth.enabled", Some("false")), ("auth.token", None)];

#[test]
fn auth_disabled_no_credentials_required() {
    let f = Fixture::new();
    let s = f.start(AUTH_OFF);
    let json = s.request("POST", "/api/default/app/_json", &[], br#"[{"a":1}]"#);
    assert_ne!(json.status, 401);
    assert_ne!(s.get("/metrics").status, 401);
    let warned = s
        .json_lines()
        .into_iter()
        .any(|v| v["event"] == "auth_disabled" && v["level"] == "warn");
    assert!(warned);
}

#[test]
fn auth_disabled_routing_unchanged() {
    let f = Fixture::new();
    let s = f.start(AUTH_OFF);
    let nope = s.get("/api/default/nope");
    nope.assert_envelope(404, "not_found");
    let post = s.request("POST", "/metrics", &[], b"");
    post.assert_envelope(405, "method_not_allowed");
    assert!(nope.header("www-authenticate").is_none());
    assert!(post.header("www-authenticate").is_none());
}

/// 前提补明文声明（非回环地址上没有证书时须有声明，REQ-AMB-ARCH-ENCRYPTION PLAIN-REJECT），断言不变。
#[test]
fn auth_disabled_listen_addr_unrestricted() {
    let f = Fixture::new();
    let mut overrides = AUTH_OFF.to_vec();
    overrides.push(("server.http_addr", Some("\"0.0.0.0:0\"")));
    overrides.push(("server.behind_tls_proxy", Some("true")));
    let s = f.start(&overrides);
    let ready = s
        .json_lines()
        .into_iter()
        .find(|v| v["event"] == "ready")
        .unwrap();
    let addr = ready["http_addr"].as_str().unwrap();
    let port: u16 = addr.strip_prefix("0.0.0.0:").unwrap().parse().unwrap();
    assert_ne!(port, 0);
}

/// 同样配置去掉明文声明：明文规则不看 `auth.enabled`（REQ-AMB-ARCH-ENCRYPTION PLAIN-REJECT、PLAIN-AUTH-INDEPENDENT）。
#[test]
fn auth_disabled_non_loopback_undeclared_exit2() {
    let f = Fixture::new();
    let mut overrides = AUTH_OFF.to_vec();
    overrides.push(("server.http_addr", Some("\"0.0.0.0:0\"")));
    let out = f.serve(&overrides).output().unwrap();
    let stderr = String::from_utf8_lossy(&out.stderr);
    assert_eq!(out.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("server.http_addr"), "{stderr}");
    assert!(stderr.contains("server.behind_tls_proxy"), "{stderr}");
}

#[test]
fn auth_bearer_accepts() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", "Bearer s3cret")],
        search_body("SELECT * FROM app").as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn auth_bearer_scheme_case_and_whitespace() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    for value in [
        "bearer s3cret",
        "BEARER s3cret",
        "Bearer   s3cret",
        "Bearer s3cret  ",
    ] {
        let r = s.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", value)],
            search_body("SELECT * FROM app").as_bytes(),
        );
        assert_eq!(r.status, 200, "{value:?}：{}", r.text());
    }
}

#[test]
fn auth_before_business_logic() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request("POST", "/api/default/app5/_json", &[], br#"[{"a":1}]"#);
    r.assert_envelope(401, "unauthorized");
    // 未经认证的摄取没有创建 stream
    s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        search_body("SELECT * FROM app5").as_bytes(),
    )
    .assert_envelope(400, "stream_not_found");
}

#[test]
fn auth_valid_token_reaches_all_endpoints() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let auth = [("Authorization", "Bearer s3cret")];
    let element = format!("[{}]", sent_element("src", minute_of(now_us()), 1));
    for (method, path, body) in [
        ("POST", "/api/default/app/_json", br#"[{"a":1}]"#.to_vec()),
        ("POST", "/api/default/app/_sent", element.into_bytes()),
        (
            "POST",
            "/api/default/_search",
            search_body("SELECT * FROM app").into_bytes(),
        ),
        ("GET", "/metrics", Vec::new()),
    ] {
        let r = s.request(method, path, &auth, &body);
        assert_eq!(r.status, 200, "{method} {path}：{}", r.text());
    }
}

// ---------------------------------------------------------------------------
// 目录前缀（REQ-AMB-AUTH-CREDENTIAL-V2）
// ---------------------------------------------------------------------------

/// 目录错误体的 `error.reason`。
fn catalog_reason(r: &common::Resp) -> serde_json::Value {
    r.json()["error"]["reason"].clone()
}

#[test]
fn auth_catalog_prefix_requires_token() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r1 = s.get("/iceberg/v1/config");
    let r2 = s.get_auth("/iceberg/v1/config");
    let r3 = s.get("/iceberg");

    assert_eq!(r1.status, 401, "{r1:?}");
    assert_eq!(r1.header("www-authenticate"), Some("Bearer"));
    assert_eq!(catalog_reason(&r1), "unauthorized");
    assert_eq!(r2.status, 200, "{}", r2.text());
    r3.assert_envelope(404, "not_found");
}

#[test]
fn auth_catalog_auth_before_routing() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let rs = [
        s.request("DELETE", "/iceberg/v1/namespaces/default", &[], b""),
        s.request("POST", "/iceberg/v1/config", &[], b""),
        s.get("/iceberg/nosuch"),
        s.request(
            "GET",
            "/iceberg/v1/amberlog/namespaces",
            &[("Authorization", "Bearer wrong")],
            b"",
        ),
    ];
    for r in &rs {
        assert_eq!(r.status, 401, "{r:?}");
        assert_eq!(catalog_reason(r), "unauthorized");
    }
    let with = s.request(
        "DELETE",
        "/iceberg/v1/namespaces/default",
        &[("Authorization", BEARER)],
        b"",
    );
    assert_eq!(with.status, 405, "{with:?}");
}

#[test]
fn auth_catalog_disabled_exempt() {
    let f = Fixture::new();
    let s = f.start(AUTH_OFF);
    let config = s.get("/iceberg/v1/config");
    let namespaces = s.get("/iceberg/v1/amberlog/namespaces");
    assert_ne!(config.status, 401);
    assert_ne!(namespaces.status, 401);
    assert_eq!(config.status, 200, "{}", config.text());
}
