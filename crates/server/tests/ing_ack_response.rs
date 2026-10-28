//! REQ-AMB-ING-ACK-RESPONSE 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 摄取、经 `_search` 查询；WAL 写入失败用 `TESTBED_FAULT_WAL` 控制文件注入（`fault-injection` feature）。

mod common;

use std::time::Duration;

use common::Fixture;
use common::api::{FLUSH_1H, WalFault, ingest, search};
use serde_json::{Value, json};

fn assert_status_entry(entry: &Value, name: &str, successful: u64, failed: u64) {
    assert_eq!(entry["name"], name, "{entry}");
    assert!(entry["successful"].is_u64() && entry["failed"].is_u64());
    assert_eq!(entry["successful"], successful, "{entry}");
    assert_eq!(entry["failed"], failed, "{entry}");
}

fn error_indexes_and_reasons(entry: &Value) -> Vec<(u64, String)> {
    entry["errors"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            assert!(e["message"].is_string(), "{e}");
            (
                e["index"].as_u64().unwrap(),
                e["reason"].as_str().unwrap().to_owned(),
            )
        })
        .collect()
}

// ---------------------------------------------------------------------------
// 应答形状
// ---------------------------------------------------------------------------

#[test]
fn ack_partial_failure_details() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = ingest(&server, "app", r#"[{"a":1},"not-an-object",{"a":3}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["status"][0]["successful"], 2);
    assert_eq!(v["status"][0]["failed"], 1);
    assert_eq!(v["status"][0]["errors"][0]["index"], 1);
    assert_eq!(v["status"][0]["errors"][0]["reason"], "not_an_object");
}

#[test]
fn ack_response_body_shape() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    assert_eq!(ingest(&server, "sh", r#"[{"a":1}]"#).status, 200);
    let r = ingest(
        &server,
        "sh",
        r#"[{"_x":1},{"a":2},5,{"a":1e400},{"b":true}]"#,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert!(v["code"].is_u64());
    assert_eq!(v["code"], 200);
    let status = v["status"].as_array().unwrap();
    assert_eq!(status.len(), 1);
    assert_status_entry(&status[0], "sh", 2, 3);
    assert_eq!(
        error_indexes_and_reasons(&status[0]),
        [
            (0, "reserved_field".to_owned()),
            (2, "not_an_object".to_owned()),
            (3, "malformed_record".to_owned()),
        ]
    );
}

#[test]
fn ack_all_rejected_400() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = ingest(&server, "app", r#"["x","y"]"#);
    let v = r.assert_envelope(400, "all_records_rejected");
    let entry = &v["details"]["status"][0];
    assert_eq!(entry["successful"], 0);
    assert_eq!(entry["failed"], 2);
    let indexes: Vec<u64> = error_indexes_and_reasons(entry)
        .into_iter()
        .map(|(i, _)| i)
        .collect();
    assert_eq!(indexes, [0, 1]);
}

#[test]
fn ack_all_rejected_status_constraints() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    assert_eq!(ingest(&server, "ar", r#"[{"a":1}]"#).status, 200);
    let r = ingest(&server, "ar", r#"[{"a":-1e400},{"-bad":1},7]"#);
    let v = r.assert_envelope(400, "all_records_rejected");
    let status = v["details"]["status"].as_array().unwrap();
    assert_eq!(status.len(), 1);
    assert_status_entry(&status[0], "ar", 0, 3);
    assert_eq!(
        error_indexes_and_reasons(&status[0]),
        [
            (0, "malformed_record".to_owned()),
            (1, "invalid_field_name".to_owned()),
            (2, "not_an_object".to_owned()),
        ]
    );
}

#[test]
fn ack_empty_batch_200() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = ingest(&server, "app", "[]");
    assert_eq!(r.status, 200, "{}", r.text());
    let v = r.json();
    assert_eq!(v["status"][0]["successful"], 0);
    assert_eq!(v["status"][0]["failed"], 0);
}

// ---------------------------------------------------------------------------
// 落库确认
// ---------------------------------------------------------------------------

#[test]
fn ack_survives_sigkill() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    assert_eq!(ingest(&server, "app", r#"[{"k":"survive"}]"#).status, 200);
    server.sigkill();

    let server = fx.start(&[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let hits = search(&server, "SELECT k FROM app WHERE k = 'survive'");
    assert_eq!(hits.len(), 1, "{hits:?}");
}

// ---------------------------------------------------------------------------
// WAL 失败与 503 的后果
// ---------------------------------------------------------------------------

#[test]
fn ack_wal_failure_503() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    fault.inject("write fsync");
    ingest(&server, "app", r#"[{"a":1}]"#).assert_envelope(503, "unavailable");
}

#[test]
fn ack_503_batch_never_visible() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let mut server = fault.start(&fx, &[FLUSH_1H]);
    assert_eq!(ingest(&server, "nv", r#"[{"k":"base"}]"#).status, 200);
    // 字节已写进段文件、fsync 失败：回截或作废后不得在重放中复活
    fault.inject("fsync");
    ingest(&server, "nv", r#"[{"k":"lost"}]"#).assert_envelope(503, "unavailable");
    fault.clear();
    assert!(search(&server, "SELECT k FROM nv WHERE k = 'lost'").is_empty());
    server.sigkill();

    let server = fault.start(&fx, &[FLUSH_1H]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert!(search(&server, "SELECT k FROM nv WHERE k = 'lost'").is_empty());
    assert_eq!(
        search(&server, "SELECT k FROM nv WHERE k = 'base'").len(),
        1
    );
}

#[test]
fn ack_503_columns_not_in_schema() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    assert_eq!(ingest(&server, "nc", r#"[{"a":1}]"#).status, 200);
    fault.inject("write fsync");
    ingest(&server, "nc", r#"[{"newcol":"s"}]"#).assert_envelope(503, "unavailable");
    fault.clear();
    let r = ingest(&server, "nc", r#"[{"newcol":1}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 1);
    // `{"a":1}` 那一行的 `newcol` 为空；有值的只有这一条，且是整数
    let hits = search(&server, "SELECT newcol FROM nc");
    let values: Vec<&Value> = hits
        .iter()
        .filter_map(|h| h.get("newcol").filter(|v| !v.is_null()))
        .collect();
    assert_eq!(values, [&json!(1)], "{hits:?}");
}

#[test]
fn ack_retry_after_503_counts_once() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    assert_eq!(ingest(&server, "rt", r#"[{"k":"base"}]"#).status, 200);
    let batch = r#"[{"k":"r1"},{"k":"r2"}]"#;
    fault.inject("fsync");
    ingest(&server, "rt", batch).assert_envelope(503, "unavailable");
    fault.clear();
    assert_eq!(ingest(&server, "rt", batch).status, 200);
    let hits = search(&server, "SELECT k FROM rt WHERE k IN ('r1','r2')");
    assert_eq!(hits.len(), 2, "{hits:?}");
}

#[test]
fn ack_all_rejected_new_stream_wal_failure_5xx() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let server = fault.start(&fx, &[]);
    fault.inject("write fsync");
    ingest(&server, "ax", r#"[{"_x":1}]"#).assert_envelope(503, "unavailable");
}

/// 文件描述符用完时打不开新的 WAL 段：这一批回 503，运行日志有一行 warn，含失败的操作与操作系统给出的原因
/// （REQ-AMB-LIFE-DURABILITY-V5）；描述符释放后照常写入。
#[test]
fn ack_wal_failure_logged_with_reason() {
    use std::io::{Read, Write};
    use std::net::TcpStream;

    const LIMIT: u64 = 128;
    let f = Fixture::new();
    let mut cmd = f.serve(&[FLUSH_1H]);
    common::limit_open_files(&mut cmd, LIMIT, LIMIT);
    let s = common::Server::start(cmd);
    let pid = s.pid();
    // 先建好发请求用的连接，等服务端接受它；还没有写入过，WAL 还没有段。只数套接字：ready 之后服务端还在
    // 逐个读系统根证书（告警 webhook 的客户端），读的时候临时多开着目录与证书文件，总数会先降下来
    let before = common::open_sockets(pid);
    let mut keeper = TcpStream::connect(s.addr).unwrap();
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while common::open_sockets(pid) <= before {
        assert!(std::time::Instant::now() < deadline, "服务端没有接受连接");
        std::thread::sleep(Duration::from_millis(20));
    }
    // 再用空闲连接占满描述符
    let mut idle = Vec::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    while (common::open_fds(pid) as u64) < LIMIT {
        assert!(
            std::time::Instant::now() < deadline,
            "描述符没有占满：{}",
            common::open_fds(pid)
        );
        idle.push(TcpStream::connect(s.addr).unwrap());
        std::thread::sleep(Duration::from_millis(5));
    }
    // 多建几条排在监听队列里：后台任务临时打开的文件关掉后，空出的描述符也被它们占去
    for _ in 0..16 {
        idle.push(TcpStream::connect(s.addr).unwrap());
    }
    std::thread::sleep(Duration::from_millis(1500));
    let body = r#"[{"a":1}]"#;
    let req = format!(
        "POST /api/default/app/_json HTTP/1.1\r\nHost: {}\r\nConnection: close\r\nAuthorization: {}\r\nContent-Length: {}\r\n\r\n{body}",
        s.addr,
        common::BEARER,
        body.len()
    );
    keeper
        .set_read_timeout(Some(Duration::from_secs(30)))
        .unwrap();
    keeper.write_all(req.as_bytes()).unwrap();
    let mut raw = Vec::new();
    keeper.read_to_end(&mut raw).unwrap();
    let r = common::parse_response(&raw);
    r.assert_envelope(503, "unavailable");
    let line = s
        .wait_line(|l| l.contains("\"level\":\"warn\"") && l.contains("WAL write failed"))
        .unwrap_or_else(|| panic!("{:#?}", s.stderr_lines()));
    let v: Value = serde_json::from_str(&line).unwrap();
    assert_eq!(v["op"], "open_segment", "{v}");
    assert!(
        v["reason"]
            .as_str()
            .unwrap()
            .contains("Too many open files"),
        "{v}"
    );
    assert_eq!(v["failures"], 1, "{v}");
    // 释放描述符之后照常写入
    drop(idle);
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        let r = ingest(&s, "app", body);
        if r.status == 200 {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{}", r.text());
        std::thread::sleep(Duration::from_millis(200));
    }
}
