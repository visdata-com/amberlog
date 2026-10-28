//! 字段列表端点 `GET /api/{org}/{stream}/_fields`（REQ-AMB-QRY-FIELDS）。
//!
//! 以子进程启动已构建的 `amberlog`，对象存储用 `file://` 临时目录。「已提交快照」以 `flush.interval` 为 `1h` 的实例
//! 摄取后优雅停机写出（停机时 flush 并提交），再以场景的配置重启。对象存储停止服务用对象存储请求注入（`* * 503`），
//! 执行名额已满用暂停点 `query.after_admission` 的 `hold-all` 停住一个查询；这两个场景要 `fault-injection` 特性。

mod common;

use std::time::Duration;

use common::api::{FLUSH_1H, ingest_ok};
use common::store::snapshot_rel;
use common::{BEARER, Fixture, Resp, Server, gov};
use serde_json::{Value, json};

fn fields_resp(s: &Server, stream: &str) -> Resp {
    s.get_auth(&format!("/api/default/{stream}/_fields"))
}

/// 断言 200 与 `application/json`，返回响应体。
#[track_caller]
fn fields_ok(s: &Server, stream: &str) -> Value {
    let r = fields_resp(s, stream);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("Content-Type"), Some("application/json"));
    r.json()
}

fn names(v: &Value) -> Vec<String> {
    v["fields"]
        .as_array()
        .expect("fields 是数组")
        .iter()
        .map(|f| f["name"].as_str().unwrap().to_owned())
        .collect()
}

#[track_caller]
fn assert_envelope(r: &Resp, status: u16, reason: &str) {
    assert_eq!(r.status, status, "{}", r.text());
    assert_eq!(r.json()["reason"], reason, "{}", r.text());
}

/// 以同一配置重启并等 `/readyz` 为 200。
fn restart_ready(f: &Fixture) -> Server {
    let s = f.start(&[]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    s
}

// ---- 列与类型 ----

#[test]
fn fields_lists_current_schema() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(
        &s,
        "app",
        r#"[{"status":500,"level":"error","http":{"code":404},"lat":0.5,"ok":true}]"#,
    );
    ingest_ok(&s, "app", r#"[{"status":"ok"}]"#);
    assert_eq!(
        fields_ok(&s, "app"),
        json!({"fields": [
            {"name": "_timestamp", "type": "Int64"},
            {"name": "http.code", "type": "Int64"},
            {"name": "lat", "type": "Float64"},
            {"name": "level", "type": "Utf8"},
            {"name": "ok", "type": "Boolean"},
            {"name": "status", "type": "Int64"},
            {"name": "status__utf8", "type": "Utf8"},
        ]})
    );
}

#[test]
fn fields_new_stream_timestamp_only() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "empty", "[{}]");
    assert_eq!(
        fields_ok(&s, "empty"),
        json!({"fields": [{"name": "_timestamp", "type": "Int64"}]})
    );
}

#[test]
fn fields_uncommitted_columns() {
    let f = Fixture::new();
    gov::committed(&f, &[("app", r#"[{"k":"x"}]"#)]);
    let s = f.start(&[FLUSH_1H]);
    ingest_ok(&s, "app", r#"[{"region":"cn-north"}]"#);
    let v = fields_ok(&s, "app");
    let fields = v["fields"].as_array().unwrap();
    assert!(
        fields.contains(&json!({"name": "region", "type": "Utf8"})),
        "{v}"
    );
    let schema = &f.dataset("app").head()["schema"];
    assert!(
        schema
            .as_array()
            .unwrap()
            .iter()
            .all(|c| c["name"] != "region"),
        "当前快照不应含 region：{schema}"
    );
}

#[test]
fn fields_codepoint_order() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "ord", r#"[{"b":1,"B":1,"a":{"z":1},"a-1":1,"Z9":1}]"#);
    assert_eq!(
        names(&fields_ok(&s, "ord")),
        ["B", "Z9", "_timestamp", "a-1", "a.z", "b"]
    );
}

// ---- 作答 ----

#[cfg(feature = "fault-injection")]
#[test]
fn fields_storage_unavailable() {
    let f = Fixture::new();
    gov::committed(&f, &[("app", r#"[{"level":"error"}]"#)]);
    let (s, faults) = f.start_with_store_faults(&[FLUSH_1H]);
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    faults.set("* * 503\n");
    // 前提：对象存储确已不可访问，读存储的查询回 503
    assert_envelope(
        &common::api::search_resp(&s, "SELECT * FROM app"),
        503,
        "unavailable",
    );
    let v = fields_ok(&s, "app");
    assert!(
        v["fields"]
            .as_array()
            .unwrap()
            .contains(&json!({"name": "level", "type": "Utf8"})),
        "{v}"
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn fields_no_execution_slot() {
    let f = Fixture::new();
    gov::committed(&f, &[("app", r#"[{"k":"x1"},{"k":"x2"}]"#)]);
    let gate = f.root().join("gate-query.after_admission");
    std::fs::write(&gate, "hold-all").unwrap();
    let mut cmd = f.serve(&[("query.max_concurrency", Some("1"))]);
    cmd.env(
        "TESTBED_FAULT_GATES",
        format!("query.after_admission={}", gate.display()),
    );
    let s = Server::start(cmd);
    let addr = s.addr;
    let held = std::thread::spawn(move || {
        common::http(
            addr,
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            common::api::search_body("SELECT * FROM app").as_bytes(),
        )
    });
    wait_running(&s, 1.0);

    let v = fields_ok(&s, "app");
    assert!(
        v["fields"]
            .as_array()
            .unwrap()
            .contains(&json!({"name": "_timestamp", "type": "Int64"})),
        "{v}"
    );
    assert_eq!(running(&s), 1.0);

    std::fs::write(&gate, "go").unwrap();
    let r = held.join().unwrap();
    assert_eq!(r.status, 200, "{}", r.text());
}

/// `/metrics` 中 `amberlog_search_running` 的取值（没有该样本按 0 计）。
#[cfg(feature = "fault-injection")]
fn running(s: &Server) -> f64 {
    let r = s.get_auth("/metrics");
    assert_eq!(r.status, 200);
    r.text()
        .lines()
        .find_map(|l| {
            l.strip_prefix("amberlog_search_running ")
                .map(|v| v.trim().parse::<f64>().unwrap())
        })
        .unwrap_or(0.0)
}

#[cfg(feature = "fault-injection")]
#[track_caller]
fn wait_running(s: &Server, n: f64) {
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while running(s) != n {
        assert!(
            std::time::Instant::now() < deadline,
            "amberlog_search_running 未到 {n}：{:#?}",
            s.stderr_lines()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ---- 校验、停用与不存在 ----

#[test]
fn fields_get_only() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    for method in ["POST", "HEAD"] {
        let r = s.request(
            method,
            "/api/default/app/_fields",
            &[("Authorization", BEARER)],
            b"",
        );
        assert_eq!(r.status, 405, "{method}：{}", r.text());
        let allow: Vec<&str> = r
            .header("Allow")
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .collect();
        assert_eq!(allow, ["GET"], "{method}");
    }
}

#[test]
fn fields_body_and_query_ignored() {
    let f = Fixture::new();
    let s = f.start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let plain = fields_resp(&s, "app");
    let odd = s.request(
        "GET",
        "/api/default/app/_fields?start_time=0",
        &[
            ("Authorization", BEARER),
            ("Content-Encoding", "br"),
            ("Content-Type", "text/plain"),
        ],
        b"not json",
    );
    assert_eq!(plain.status, 200, "{}", plain.text());
    assert_eq!(odd.status, 200, "{}", odd.text());
    assert_eq!(plain.body, odd.body);
}

#[test]
fn fields_disabled_dataset_gate() {
    let f = Fixture::new();
    gov::committed(&f, &[("old", r#"[{"a":1}]"#)]);
    let ds = f.dataset("old");
    let n = ds.current().expect("当前快照存在");
    let mut snap = ds.snapshot(n);
    snap["format_version"] = json!(3);
    ds.write_json(&snapshot_rel(n), &snap);
    let s = restart_ready(&f);
    assert_envelope(&fields_resp(&s, "old"), 503, "unsupported_format_version");
}

#[test]
fn fields_manifest_unreadable() {
    let f = Fixture::new();
    gov::committed(&f, &[("app", r#"[{"a":1}]"#)]);
    let ds = f.dataset("app");
    let n = ds.current().expect("当前快照存在");
    ds.write(&snapshot_rel(n), b"{not json");
    let s = restart_ready(&f);
    assert_envelope(&fields_resp(&s, "app"), 500, "internal");
}

#[test]
fn fields_stream_not_found() {
    let f = Fixture::new();
    let s = f.start(&[]);
    assert_envelope(&fields_resp(&s, "nope"), 400, "stream_not_found");
}
