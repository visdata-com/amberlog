//! REQ-AMB-DM-NAMESPACE-V2 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录，经 `_json` 摄取、经 `_search` 查询。
//!
//! 「对象存储不可达」以目录权限注入（`file://` 根目录置为 000）；「WAL 目录不可写」用 `TESTBED_FAULT_WAL` 注入点；
//! 「写出快照后、写出提示 `_current` 前崩溃」用暂停点 `commit.after_snapshot_create`。

mod common;

use std::time::Duration;

use common::api::{
    FLUSH_1H, MINUTE_US, WalFault, ingest, ingest_ok, ingest_with, minute_of, now_us, search,
    search_resp, sent, sent_element,
};
use common::store::{FLUSH_1S, kill_at_marker};
use common::{BEARER, Fixture, Resp, Server, set_mode};

fn post(server: &Server, path: &str, body: &str) -> Resp {
    server.request("POST", path, &[("Authorization", BEARER)], body.as_bytes())
}

fn search_on(server: &Server, path: &str) -> Resp {
    post(server, path, &common::api::search_body("SELECT * FROM app"))
}

// ---------------------------------------------------------------------------
// naming
// ---------------------------------------------------------------------------

#[test]
fn namespace_percent_decoded_segments() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = post(&server, "/api/default/%61pp/_json", r#"[{"k":"pct"}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        search(&server, "SELECT k FROM app WHERE k = 'pct'").len(),
        1
    );
    post(&server, "/api/default/%41pp/_json", r#"[{"k":"pct"}]"#)
        .assert_envelope(400, "invalid_stream_name");
}

#[test]
fn namespace_stream_name_table() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let a64 = "a".repeat(64);
    let a65 = "a".repeat(65);
    let rows: &[(&str, u16)] = &[
        ("a", 200),
        ("0abc", 200),
        (&a64, 200),
        (&a65, 400),
        ("App", 400),
        ("a-b", 400),
        ("a.b", 400),
        ("%C3%A9", 400),
        ("%FF", 400),
        ("a%2Fb", 400),
    ];
    for (segment, want) in rows {
        let r = post(
            &server,
            &format!("/api/default/{segment}/_json"),
            r#"[{"a":1}]"#,
        );
        if *want == 200 {
            assert_eq!(r.status, 200, "{segment}：{}", r.text());
        } else {
            r.assert_envelope(400, "invalid_stream_name");
        }
    }
}

#[test]
fn namespace_org_not_found() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    post(&server, "/api/acme/app/_json", r#"[{"a":1}]"#).assert_envelope(404, "org_not_found");
}

#[test]
fn namespace_org_decoding_and_search() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = post(&server, "/api/%64efault/app/_json", r#"[{"a":1}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    post(&server, "/api/Default/app/_json", r#"[{"a":1}]"#).assert_envelope(404, "org_not_found");
    search_on(&server, "/api/acme/_search").assert_envelope(404, "org_not_found");
}

#[test]
fn namespace_fixed_segments_literal() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    post(&server, "/api/default/app/%5Fjson", r#"[{"a":1}]"#).assert_envelope(404, "not_found");
}

#[test]
fn namespace_invalid_stream_name() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    post(&server, "/api/default/_system/_json", r#"[{"a":1}]"#)
        .assert_envelope(400, "invalid_stream_name");
}

// ---------------------------------------------------------------------------
// existence
// ---------------------------------------------------------------------------

#[test]
fn namespace_data_only_prefix_not_existing() {
    let fx = Fixture::new();
    fx.dataset("ghost").write(
        "data/2026/08/22/10/01k3a0m0000000000000000000.parquet",
        b"not a parquet file",
    );
    let server = fx.start(&[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    search_resp(&server, "SELECT * FROM ghost").assert_envelope(400, "stream_not_found");
}

#[test]
fn namespace_object_store_existence_any_wal_dir() {
    let fx = Fixture::new();
    let mut x = fx.start(&[]);
    ingest_ok(&x, "app", r#"[{"a":1}]"#);
    x.signal("TERM");
    assert_eq!(x.wait_exit(Duration::from_secs(30)), Some(Some(0)));

    let w2 = fx.root().join("wal2");
    let w2 = format!("\"{}\"", w2.display());
    let y = fx.start(&[("wal.dir", Some(w2.as_str()))]);
    assert_eq!(y.wait_readyz(200, Duration::from_secs(10)).status, 200);
    search(&y, "SELECT * FROM app");
}

#[test]
fn namespace_wal_existence_survives_restart() {
    let fx = Fixture::new();
    let mut server = fx.start(&[]);
    set_mode(&fx.store_dir(), 0o000);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    server.sigkill();
    set_mode(&fx.store_dir(), 0o755);

    let server = fx.start(&[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let hits = search(&server, "SELECT a FROM app");
    assert!(hits.iter().any(|h| h["a"] == 1), "{hits:?}");
}

// ---------------------------------------------------------------------------
// autocreate
// ---------------------------------------------------------------------------

#[test]
fn namespace_stream_autocreate() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "nginx_access", r#"[{"path":"/x"}]"#);
    let hits = search(&server, "SELECT path FROM nginx_access");
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["path"], "/x");
}

#[test]
fn namespace_stream_autocreate_via_sent() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let w = minute_of(now_us());
    let r = sent(&server, "app9", &format!("[{}]", sent_element("a", w, 1)));
    assert_eq!(r.status, 200, "{}", r.text());
    search(&server, "SELECT * FROM app9");
}

#[test]
fn namespace_autocreate_on_all_records_rejected() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest(&server, "app4", r#"[{"_x":1}]"#).assert_envelope(400, "all_records_rejected");
    search(&server, "SELECT * FROM app4");
}

#[test]
fn namespace_no_autocreate_on_request_rejection() {
    let fx = Fixture::new();
    // 上限须容得下随后的查询请求体（`_search` 同受 `ingest.max_body_bytes` 约束）
    let server = fx.start(&[("ingest.max_body_bytes", Some("256"))]);
    let body = format!("[{}]", vec![r#"{"a":1}"#; 64].join(","));
    let r = ingest(&server, "app3", &body);
    assert_eq!(r.status, 413, "{}", r.text());
    search_resp(&server, "SELECT * FROM app3").assert_envelope(400, "stream_not_found");
}

#[test]
fn namespace_no_autocreate_request_rejection_table() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let w = minute_of(now_us());
    let no_sent = format!(
        r#"[{{"source":"a","window_start":{w},"window_end":{}}}]"#,
        w + MINUTE_US
    );
    let rows: [(&str, Resp, u16, &str); 4] = [
        (
            "r1",
            ingest_with(
                &server,
                "r1",
                &[("Content-Encoding", "br")],
                br#"[{"a":1}]"#,
            ),
            415,
            "unsupported_encoding",
        ),
        ("r2", ingest(&server, "r2", "{"), 400, "invalid_json"),
        ("r3", sent(&server, "r3", "{}"), 400, "invalid_request"),
        ("r4", sent(&server, "r4", &no_sent), 400, "invalid_request"),
    ];
    for (stream, r, status, reason) in rows {
        assert_eq!(r.status, status, "{stream}：{}", r.text());
        r.assert_envelope(status, reason);
        search_resp(&server, &format!("SELECT * FROM {stream}"))
            .assert_envelope(400, "stream_not_found");
    }
}

// ---------------------------------------------------------------------------
// persistence
// ---------------------------------------------------------------------------

#[test]
fn namespace_autocreate_object_store_down() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    set_mode(&fx.store_dir(), 0o000);
    ingest_ok(&server, "app2", r#"[{"k":"down"}]"#);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let r = search_resp(&server, "SELECT k FROM app2");
        if r.status == 200 && r.json()["hits"][0]["k"] == "down" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "10 秒内未查到：{} {}",
            r.status,
            r.text()
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    set_mode(&fx.store_dir(), 0o755);
}

#[test]
fn namespace_unpersisted_stream_exists_in_process() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    fault.inject("write fsync");
    assert_eq!(ingest(&server, "app7", r#"[{"a":1}]"#).status, 503);
    let r = post(
        &server,
        "/api/default/_search",
        r#"{"query":{"sql":"SELECT * FROM app7","start_time":0,"end_time":4102444800000000}}"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"], serde_json::json!([]));
    assert_eq!(ingest(&server, "app7", "[]").status, 503);
}

#[test]
fn namespace_no_genesis_without_wal() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    fault.inject("write fsync");
    ingest(&server, "app8", r#"[{"a":1}]"#).assert_envelope(503, "unavailable");
    assert_eq!(
        fx.dataset("app8").keys_under("manifest/"),
        Vec::<String>::new()
    );
}

#[test]
fn namespace_unpersisted_stream_genesis_snapshot_before_ack() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[FLUSH_1H]);
    fault.inject("write fsync");
    assert_eq!(ingest(&server, "app7", r#"[{"a":1}]"#).status, 503);
    fault.clear();
    ingest_ok(&server, "app7", r#"[{"a":2}]"#);
    assert!(fx.dataset("app7").exists(&common::store::snapshot_rel(1)));
}

#[test]
fn namespace_genesis_before_first_ack() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "app10", r#"[{"a":1}]"#);
    assert!(fx.dataset("app10").exists(&common::store::snapshot_rel(1)));
}

#[test]
fn namespace_genesis_without_hint_exists_after_node_replacement() {
    let fx = Fixture::new();
    let (mut x, marker) = fx.start_paused(&[FLUSH_1S], "commit.after_snapshot_create");
    let addr = x.addr;
    // 创世快照已创建、提交停在写提示之前，这次摄取不会得到应答
    std::thread::spawn(move || {
        let _ = std::panic::catch_unwind(|| {
            common::http(
                addr,
                "POST",
                "/api/default/app6/_json",
                &[("Authorization", BEARER)],
                br#"[{"a":1}]"#,
            )
        });
    });
    kill_at_marker(&mut x, &marker);
    let ds = fx.dataset("app6");
    assert!(ds.exists(&common::store::snapshot_rel(1)));
    assert!(!ds.exists("manifest/_current"));

    let w2 = format!("\"{}\"", fx.root().join("wal2").display());
    let y = fx.start(&[FLUSH_1S, ("wal.dir", Some(w2.as_str()))]);
    assert_eq!(y.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let r = post(
        &y,
        "/api/default/_search",
        r#"{"query":{"sql":"SELECT _timestamp FROM app6","start_time":0,"end_time":4102444800000000}}"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"], serde_json::json!([]));
    ingest_ok(&y, "app6", r#"{"a":1}"#);
    ds.wait_records(1, Duration::from_secs(10));
    let chain = ds.chain();
    assert_eq!(chain.last().unwrap()["snapshot_id"], 1, "{chain:?}");
}
