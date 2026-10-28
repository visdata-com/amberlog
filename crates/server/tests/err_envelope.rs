//! REQ-AMB-ERR-ENVELOPE-V7 的系统测试。
//!
//! 需要预置对象存储状态的场景由测试停机后直接改写 `file://` 临时目录下的对象，再以同一配置重启。对象存储不可访问的
//! 三个场景接 MinIO 台架（[`S3Instance`]），停掉中继：`file://` 没有端点与桶名，测不出 5xx 的脱敏。

mod common;

use std::time::{Duration, Instant};

use common::api::{
    FLUSH_1H, MINUTE_US, SlowRequest, ingest, ingest_ok, minute_of, now_us, search_body, sent,
};
use common::s3::S3Instance;
use common::store::{Ds, snapshot_rel};
use common::{BEARER, Fixture, Kv, Resp, Server};
use serde_json::json;

#[test]
fn error_envelope_request_id() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request("GET", "/healthz", &[("x-request-id", "abc-123")], b"");
    assert_eq!(r.header("x-request-id"), Some("abc-123"));
}

#[test]
fn error_envelope_generated_request_id() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.get("/healthz");
    assert_eq!(r.status, 200);
    assert!(!r.header("x-request-id").unwrap_or("").is_empty());
}

#[test]
fn error_envelope_not_found() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request(
        "GET",
        "/api/default/nope",
        &[("Authorization", BEARER)],
        b"",
    );
    r.assert_envelope(404, "not_found");
}

#[test]
fn error_metrics_auth_before_method() {
    let f = Fixture::new();
    let s = f.start(&[]);
    s.request("POST", "/metrics", &[], b"")
        .assert_envelope(401, "unauthorized");
}

#[test]
fn error_auth_before_route_unregistered() {
    let f = Fixture::new();
    let s = f.start(&[]);
    s.get("/api/default/nope")
        .assert_envelope(401, "unauthorized");
}

#[test]
fn error_envelope_unauthorized_single_shape() {
    let f = Fixture::new();
    let s = f.start(&[]);
    for headers in [&[][..], &[("Authorization", "Bearer wrong")]] {
        let r = s.request("POST", "/api/default/_search", headers, b"{}");
        r.assert_envelope(401, "unauthorized");
        assert!(r.header("www-authenticate").is_some());
    }
}

#[test]
fn error_auth_zone_outside_unauthenticated() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let foo = s.get("/foo");
    foo.assert_envelope(404, "not_found");
    let post = s.request("POST", "/healthz", &[], b"");
    post.assert_envelope(405, "method_not_allowed");
    assert!(foo.header("www-authenticate").is_none());
    assert!(post.header("www-authenticate").is_none());
}

#[test]
fn error_method_not_allowed_allow_header() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let get_only = std::collections::BTreeSet::from(["GET".to_owned()]);
    let head = s.request("HEAD", "/healthz", &[], b"");
    assert_eq!(head.status, 405);
    assert_eq!(head.allow(), get_only);
    let options = s.request("OPTIONS", "/healthz", &[], b"");
    options.assert_envelope(405, "method_not_allowed");
    assert_eq!(options.allow(), get_only);
}

#[test]
fn error_head_response_no_body() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.request("HEAD", "/healthz", &[], b"");
    assert_eq!(r.status, 405);
    assert_eq!(
        r.allow(),
        std::collections::BTreeSet::from(["GET".to_owned()])
    );
    assert!(r.header("x-request-id").is_some());
    assert_eq!(r.header("content-type"), Some("application/json"));
    assert!(r.body.is_empty(), "HEAD 响应不应带体：{:?}", r.text());
}

// ---------------------------------------------------------------------------
// 端到端：各端点、停机与数据集状态
// ---------------------------------------------------------------------------

const MAX_BODY_1K: Kv = ("ingest.max_body_bytes", Some("1024"));

fn post(s: &Server, path: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    let mut all = vec![("Authorization", BEARER)];
    all.extend_from_slice(headers);
    s.request("POST", path, &all, body)
}

/// 以 `flush.interval` 为 `1h` 启动、摄取 `app` 一条记录、SIGTERM 停机（停机时 flush 并提交）。
fn committed_app(f: &Fixture) {
    let mut s = f.start(&[FLUSH_1H]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
    assert!(f.dataset("app").current().is_some(), "停机时应已提交快照");
}

/// 以同一配置重启并等 `/readyz` 为 200。
fn restart_ready(f: &Fixture) -> Server {
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    s
}

/// 把当前快照文件中的 `format_version` 就地改为 `version`。
fn set_current_format_version(ds: &Ds, version: u64) {
    let n = ds.current().expect("当前快照存在");
    let mut snap = ds.snapshot(n);
    snap["format_version"] = json!(version);
    ds.write_json(&snapshot_rel(n), &snap);
}

/// 方法、路径、请求头、请求体、期望状态码、期望 `reason`。
type Row<'a> = (
    &'a str,
    &'a str,
    Vec<(&'a str, &'a str)>,
    &'a [u8],
    u16,
    &'a str,
);

#[test]
fn error_envelope_invariants() {
    let f = Fixture::new();
    let s = f.start(&[MAX_BODY_1K]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    // REQ-AMB-ERR-ENVELOPE-V4：`numeric_overflow` 一行（Int64 求和超出 Int64，REQ-AMB-QRY-AGGREGATION）
    ingest_ok(&s, "ov", r#"[{"n":9223372036854775807},{"n":1}]"#);
    let overflow = br#"{"query":{"sql":"SELECT sum(n) AS s FROM ov","start_time":0,"end_time":4102444800000000}}"#;
    let big = vec![b' '; 2048];
    let rows: Vec<Row> = vec![
        (
            "POST",
            "/api/default/_search",
            vec![],
            b"{}",
            401,
            "unauthorized",
        ),
        (
            "GET",
            "/api/default/nope",
            vec![("Authorization", BEARER)],
            b"",
            404,
            "not_found",
        ),
        (
            "GET",
            "/api/default/app/_json",
            vec![("Authorization", BEARER)],
            b"",
            405,
            "method_not_allowed",
        ),
        (
            "POST",
            "/api/default/app/_json",
            vec![("Authorization", BEARER)],
            b"{",
            400,
            "invalid_json",
        ),
        (
            "POST",
            "/api/default/app/_json",
            vec![("Authorization", BEARER), ("Content-Encoding", "br")],
            b"[{}]",
            415,
            "unsupported_encoding",
        ),
        (
            "POST",
            "/api/default/app/_json",
            vec![("Authorization", BEARER), ("Content-Encoding", "gzip")],
            b"[{}]",
            400,
            "invalid_encoding",
        ),
        (
            "POST",
            "/api/default/app/_json",
            vec![("Authorization", BEARER)],
            &big,
            413,
            "payload_too_large",
        ),
        (
            "POST",
            "/api/default/_search",
            vec![("Authorization", BEARER)],
            b"{}",
            400,
            "invalid_request",
        ),
        (
            "POST",
            "/api/default/_search",
            vec![("Authorization", BEARER)],
            overflow,
            400,
            "numeric_overflow",
        ),
    ];
    for (method, path, headers, body, status, reason) in rows {
        let r = s.request(method, path, &headers, body);
        let v = r.assert_envelope(status, reason);
        assert!(v.is_object() && v["code"].is_u64(), "{method} {path}：{v}");
    }
}

#[test]
fn error_shutdown_gate_before_auth() {
    let f = Fixture::new();
    let mut s = f.start(&[]);
    let slow = SlowRequest::start(&s, "app", r#"[{"seq":1}]"#);
    s.signal("TERM");
    // SIGTERM 送达之前无凭据请求得 401；等到停机闸门生效
    let deadline = Instant::now() + Duration::from_secs(5);
    let r = loop {
        let r = s.request("POST", "/api/default/app/_json", &[], br#"[{"a":1}]"#);
        if r.status != 401 || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    r.assert_envelope(503, "unavailable");
    assert_eq!(r.header("www-authenticate"), None);
    assert_eq!(slow.finish().status, 200);
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
}

#[test]
fn error_validation_order_method_before_path() {
    let f = Fixture::new();
    let s = f.start(&[]);
    s.get_auth("/api/acme/app/_json")
        .assert_envelope(405, "method_not_allowed");
}

#[test]
fn error_validation_order_org_before_stream() {
    let f = Fixture::new();
    let s = f.start(&[]);
    post(&s, "/api/acme/_bad/_json", &[], br#"[{"a":1}]"#).assert_envelope(404, "org_not_found");
}

#[test]
fn error_validation_order_path_before_body() {
    let f = Fixture::new();
    let s = f.start(&[]);
    post(
        &s,
        "/api/acme/app/_json",
        &[("Content-Encoding", "br")],
        br#"[{"a":1}]"#,
    )
    .assert_envelope(404, "org_not_found");
}

#[test]
fn error_state_gate_before_encoding() {
    let f = Fixture::new();
    committed_app(&f);
    set_current_format_version(&f.dataset("app"), 3);
    let s = restart_ready(&f);
    let v = post(
        &s,
        "/api/default/app/_json",
        &[("Content-Encoding", "br")],
        br#"[{"a":1}]"#,
    )
    .assert_envelope(503, "unsupported_format_version");
    assert_eq!(v["code"], 503);
}

#[test]
fn error_validation_order_encoding_before_size() {
    let f = Fixture::new();
    let s = f.start(&[MAX_BODY_1K]);
    post(
        &s,
        "/api/default/app/_json",
        &[("Content-Encoding", "br")],
        &[b'x'; 2048],
    )
    .assert_envelope(415, "unsupported_encoding");
}

#[test]
fn error_validation_order_source_before_encoding() {
    let f = Fixture::new();
    let s = f.start(&[]);
    post(
        &s,
        "/api/default/app/_json",
        &[("X-Amberlog-Source", "-fw"), ("Content-Encoding", "br")],
        br#"[{"a":1}]"#,
    )
    .assert_envelope(400, "invalid_request");
}

#[test]
fn error_validation_order_size_before_parse() {
    let f = Fixture::new();
    let s = f.start(&[MAX_BODY_1K]);
    post(&s, "/api/default/app/_json", &[], &[b'x'; 2048])
        .assert_envelope(413, "payload_too_large");
}

#[test]
fn error_duplicate_keys_last_wins() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1},{"a":2}]"#);
    let q = r#""sql":"SELECT * FROM app","start_time":0,"end_time":4102444800000000"#;
    let r = post(
        &s,
        "/api/default/_search",
        &[],
        format!(r#"{{"query":{{{q},"size":100000,"size":1}}}}"#).as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"].as_array().unwrap().len(), 1);
    post(
        &s,
        "/api/default/_search",
        &[],
        format!(r#"{{"query":{{{q},"size":1,"size":100000}}}}"#).as_bytes(),
    )
    .assert_envelope(400, "invalid_request");
}

#[test]
fn error_all_records_rejected_details() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let v = ingest(&s, "app", "[1, 2]").assert_envelope(400, "all_records_rejected");
    let status = v["details"]["status"]
        .as_array()
        .expect("details.status 是数组");
    assert_eq!(status.len(), 1, "{v}");
    assert_eq!(
        (
            &status[0]["name"],
            &status[0]["successful"],
            &status[0]["failed"]
        ),
        (&json!("app"), &json!(0), &json!(2)),
        "{v}"
    );
    let errors: Vec<(u64, &str)> = status[0]["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| (e["index"].as_u64().unwrap(), e["reason"].as_str().unwrap()))
        .collect();
    assert_eq!(errors, [(0, "not_an_object"), (1, "not_an_object")]);
}

#[test]
fn error_envelope_internal_masked() {
    let f = Fixture::new();
    committed_app(&f);
    let ds = f.dataset("app");
    let n = ds.current().unwrap();
    ds.write(&snapshot_rel(n), b"{not json");
    let s = restart_ready(&f);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        search_body("SELECT * FROM app").as_bytes(),
    );
    let v = r.assert_envelope(500, "internal");
    let message = v["message"].as_str().unwrap();
    let store = f.store_dir();
    for leak in [
        store.to_str().unwrap(),
        "/amberlog/",
        "manifest/",
        ".json",
        "panicked",
        "backtrace",
        "src/",
        ".rs:",
    ] {
        assert!(!message.contains(leak), "message 含 {leak:?}：{message}");
    }
}

#[test]
fn error_protocol_integer_fields() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let q = r#""sql":"SELECT * FROM app","start_time":0,"end_time":4102444800000000"#;
    for (size, status) in [
        ("1.0", 400),
        ("1e2", 400),
        (r#""10""#, 400),
        ("null", 400),
        ("10", 200),
    ] {
        let r = post(
            &s,
            "/api/default/_search",
            &[],
            format!(r#"{{"query":{{{q},"size":{size}}}}}"#).as_bytes(),
        );
        if status == 200 {
            assert_eq!(r.status, 200, "size {size}：{}", r.text());
        } else {
            r.assert_envelope(status, "invalid_request");
        }
    }
    let w = minute_of(now_us());
    let element = format!(
        r#"[{{"source":"src","window_start":{w},"window_end":{},"sent":1.0}}]"#,
        w + MINUTE_US
    );
    sent(&s, "app", &element).assert_envelope(400, "invalid_request");
}

// ---------------------------------------------------------------------------
// 目录前缀（REQ-AMB-ERR-ENVELOPE-V6 ④⑤⑦）
// ---------------------------------------------------------------------------

/// 只含键 `error` 的 JSON 对象，`error.code` 为整数 `status`、`error.reason` 为 `reason`。
fn assert_catalog_body(r: &Resp, status: u16, reason: &str) {
    assert_eq!(r.status, status, "{r:?}");
    assert_eq!(r.header("content-type"), Some("application/json"));
    let v = r.json();
    let keys: Vec<&String> = v.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["error"], "{v}");
    assert!(v["error"]["code"].is_u64(), "{v}");
    assert_eq!(v["error"]["code"], status);
    assert_eq!(v["error"]["reason"], reason);
}

#[test]
fn error_catalog_unauthorized_body() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = s.get("/iceberg/v1/config");
    assert_eq!(r.header("www-authenticate"), Some("Bearer"));
    assert_catalog_body(&r, 401, "unauthorized");
}

#[test]
fn error_catalog_unregistered_path_405() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let auth = [("Authorization", BEARER)];
    for r in [
        s.request("GET", "/iceberg/foo", &auth, b""),
        s.request("POST", "/iceberg/v1/amberlog/no-such-endpoint", &auth, b""),
    ] {
        assert!(r.header("allow").is_some(), "{r:?}");
        assert_catalog_body(&r, 405, "method_not_allowed");
    }
}

#[test]
fn error_catalog_iceberg_error_shape() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let auth = [("Authorization", BEARER)];
    let get = s.request("GET", "/iceberg/v1/amberlog/namespaces/nope", &auth, b"");
    let head = s.request("HEAD", "/iceberg/v1/amberlog/namespaces/nope", &auth, b"");
    for r in [&get, &head] {
        assert_eq!(r.status, 404, "{r:?}");
        assert!(r.header("x-request-id").is_some());
    }
    assert_catalog_body(&get, 404, "namespace_not_found");
    assert!(head.body.is_empty());
}

// ---------------------------------------------------------------------------
// 5xx 的脱敏与 `error` 日志（REQ-AMB-ERR-ENVELOPE-V7，MinIO 台架）
// ---------------------------------------------------------------------------

/// 一个接在台架上、stream `app` 已有一条已提交记录的实例（`extra` 为另加的配置）；返回时中继已停，对象存储不可访问。
fn store_down_after_commit(inst: &S3Instance, extra: &[Kv]) -> Server {
    let mut kv: Vec<Kv> = vec![("flush.max_records", Some("1"))];
    kv.extend_from_slice(extra);
    let s = inst.start(&kv);
    ingest_ok(&s, "app", r#"[{"k":1}]"#);
    inst.wait_committed("app", 1);
    inst.relay.stop();
    s
}

/// 5xx 的说明文字不含对象存储端点（中继地址）、桶名、实例路径与 `storage.prefix`、对象键片段与文件系统路径。
fn assert_masked(message: &str, inst: &S3Instance) {
    let endpoint = inst.relay.addr.to_string();
    let port = inst.relay.addr.port().to_string();
    let bucket = common::s3::testbed("TESTBED_S3_BUCKET");
    let root = inst.fixture.root().to_str().unwrap().to_owned();
    for leak in [
        endpoint.as_str(),
        port.as_str(),
        bucket.as_str(),
        inst.path(),
        common::s3::PREFIX,
        root.as_str(),
        "default/app",
        "manifest",
        ".json",
        ".parquet",
        "://",
        "src/",
        ".rs:",
    ] {
        assert!(!message.contains(leak), "message 含 {leak:?}：{message}");
    }
}

#[test]
fn error_5xx_masked_object_store() {
    let inst = S3Instance::new();
    let s = store_down_after_commit(&inst, &[]);
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        search_body("SELECT * FROM app").as_bytes(),
    );
    let v = r.assert_envelope(503, "unavailable");
    assert_masked(v["message"].as_str().unwrap(), &inst);
}

#[test]
fn error_5xx_error_log_request_id() {
    let inst = S3Instance::new();
    let s = store_down_after_commit(
        &inst,
        &[
            ("log.level", Some("\"error\"")),
            ("log.format", Some("\"json\"")),
        ],
    );
    let query = |id: &str| {
        s.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER), ("x-request-id", id)],
            search_body("SELECT * FROM app").as_bytes(),
        )
    };
    query("r-503").assert_envelope(503, "unavailable");
    // 就绪探测的周期不超过 30 秒
    assert_eq!(s.wait_readyz(503, Duration::from_secs(40)).status, 503);
    let ready = s.request("GET", "/readyz", &[("x-request-id", "r-ready")], b"");
    ready.assert_envelope(503, "not_ready");
    // 再发一个 5xx 作标尺：日志按行序输出，等到它的一行时，`r-ready` 若写了日志也已在其前
    query("r-after").assert_envelope(503, "unavailable");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !error_line_with(&s, "r-after") {
        assert!(
            Instant::now() < deadline,
            "30 秒内没有 r-after 的 error 日志"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    assert!(
        error_line_with(&s, "r-503"),
        "没有 r-503 的 error 日志：{:?}",
        s.stderr_lines()
    );
    assert!(
        !error_line_with(&s, "r-ready"),
        "未就绪的 503 写了 error 日志：{:?}",
        s.stderr_lines()
    );
}

/// 运行日志中有一行的 `level` 等于 `error` 且有一个字段的取值等于 `id`。
fn error_line_with(s: &Server, id: &str) -> bool {
    s.json_lines().iter().any(|line| {
        line["level"] == "error"
            && line
                .as_object()
                .is_some_and(|o| o.values().any(|v| v == id))
    })
}

#[test]
fn error_5xx_masked_catalog() {
    let inst = S3Instance::new();
    let s = inst.start(&[]);
    inst.relay.stop();
    let r = s.request(
        "GET",
        "/iceberg/v1/amberlog/namespaces",
        &[("Authorization", BEARER)],
        b"",
    );
    assert_catalog_body(&r, 503, "unavailable");
    assert_masked(r.json()["error"]["message"].as_str().unwrap(), &inst);
}
