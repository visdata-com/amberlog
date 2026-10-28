//! REQ-AMB-ING-BODY-ENCODING 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 与 `_search` 提交 gzip、zstd 压缩的请求体。压缩请求体由测试进程用服务端同一组压缩库现场生成，
//! 损坏的流由测试改写字节构造。

mod common;

use std::io::Write;

use common::api::{ingest_ok, ingest_with, search, search_body, search_resp};
use common::{BEARER, Fixture, Resp, Server};

fn gzip(data: &[u8]) -> Vec<u8> {
    let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

fn zstd(data: &[u8]) -> Vec<u8> {
    zstd::bulk::compress(data, 3).unwrap()
}

/// 带内容校验值的 zstd 帧。
fn zstd_with_checksum(data: &[u8]) -> Vec<u8> {
    let mut e = zstd::stream::Encoder::new(Vec::new(), 3).unwrap();
    e.include_checksum(true).unwrap();
    e.write_all(data).unwrap();
    e.finish().unwrap()
}

/// zstd 帧头描述符（Frame_Header_Descriptor，紧跟 4 字节魔数）。
fn zstd_fhd(frame: &[u8]) -> u8 {
    assert_eq!(&frame[..4], &[0x28, 0xB5, 0x2F, 0xFD], "zstd 魔数");
    frame[4]
}

/// `_json` 摄取，以给定 `Content-Encoding` 提交原始字节。
fn post_json(server: &Server, stream: &str, encoding: &str, body: &[u8]) -> Resp {
    ingest_with(server, stream, &[("Content-Encoding", encoding)], body)
}

/// 断言 `stream` 不存在（对它的 `_search` 回 400 `stream_not_found`）。
fn assert_stream_absent(server: &Server, stream: &str) {
    search_resp(server, &format!("SELECT * FROM {stream}"))
        .assert_envelope(400, "stream_not_found");
}

/// 由 `{"a":1}` 重复组成、恰为 `n` 字节的合法 JSON 数组。
fn json_array_of_size(n: usize) -> Vec<u8> {
    let item = br#"{"a":1}"#;
    let mut b = b"[".to_vec();
    while b.len() + item.len() + 2 <= n {
        if b.len() > 1 {
            b.push(b',');
        }
        b.extend_from_slice(item);
    }
    b.push(b']');
    b.resize(n, b' ');
    b
}

/// `/proc/<pid>/status` 中某一项的千字节数。
fn proc_status_kb(pid: u32, key: &str) -> u64 {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).unwrap();
    status
        .lines()
        .find_map(|l| l.strip_prefix(key))
        .and_then(|rest| rest.trim_start_matches(':').split_whitespace().next())
        .and_then(|n| n.parse().ok())
        .unwrap_or_else(|| panic!("/proc/{pid}/status 中没有 {key}"))
}

// ---------------------------------------------------------------------------
// 可接受的编码
// ---------------------------------------------------------------------------

#[test]
fn body_gzip_accepted() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = post_json(&server, "app", "gzip", &gzip(br#"[{"a":1},{"a":2}]"#));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 2);
    assert_eq!(search(&server, "SELECT a FROM app WHERE a = 2").len(), 1);
}

#[test]
fn body_zstd_accepted() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = post_json(&server, "app", "zstd", &zstd(br#"[{"a":1},{"a":2}]"#));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 2);
    assert_eq!(search(&server, "SELECT a FROM app WHERE a = 2").len(), 1);
}

#[test]
fn body_gzip_name_variants() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let body = gzip(br#"[{"a":1}]"#);
    for encoding in ["GZIP", "x-gzip"] {
        let r = post_json(&server, "app", encoding, &body);
        assert_eq!(r.status, 200, "{encoding}：{}", r.text());
    }
}

#[test]
fn body_gzip_search_no_response_encoding() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let r = server.request(
        "POST",
        "/api/default/_search",
        &[
            ("Authorization", BEARER),
            ("Content-Encoding", "gzip"),
            ("Accept-Encoding", "gzip"),
        ],
        &gzip(search_body("SELECT a FROM app").as_bytes()),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("content-encoding"), None);
    assert_eq!(r.json()["hits"].as_array().unwrap().len(), 1);
}

// ---------------------------------------------------------------------------
// 拒绝
// ---------------------------------------------------------------------------

#[test]
fn body_gzip_decompressed_over_limit() {
    let fx = Fixture::new();
    let server = fx.start(&[("ingest.max_body_bytes", Some("1048576"))]);
    let body = gzip(&json_array_of_size(2 * 1024 * 1024));
    assert!(body.len() < 64 * 1024, "压缩后 {} 字节", body.len());
    post_json(&server, "zz", "gzip", &body).assert_envelope(413, "payload_too_large");
    assert_stream_absent(&server, "zz");
}

#[test]
fn body_gzip_bomb_bounded() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    // 10240 个 gzip 成员首尾相接，每个解压为 1 MiB 的 0x00：压缩后约 10 MiB，解压后 10 GiB
    let member = gzip(&vec![0u8; 1024 * 1024]);
    let body = member.repeat(10 * 1024);
    let mib = body.len() as f64 / (1024.0 * 1024.0);
    assert!((5.0..=16.0).contains(&mib), "压缩后 {mib:.1} MiB");
    let rss_before = proc_status_kb(server.pid(), "VmRSS");
    let r = server.request_streaming(
        "POST",
        "/api/default/app/_json",
        &[("Authorization", BEARER), ("Content-Encoding", "gzip")],
        body,
    );
    assert_eq!(r.status, 413, "{}", r.text());
    let hwm = proc_status_kb(server.pid(), "VmHWM");
    assert!(
        hwm.saturating_sub(rss_before) <= 256 * 1024,
        "常驻内存峰值 {hwm} kB，发送前 {rss_before} kB"
    );
}

#[test]
fn body_gzip_corrupt_invalid_encoding() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let mut body = gzip(br#"[{"a":1},{"a":2}]"#);
    let n = body.len();
    for b in &mut body[n - 8..] {
        *b ^= 0xFF;
    }
    post_json(&server, "zz", "gzip", &body).assert_envelope(400, "invalid_encoding");
    assert_stream_absent(&server, "zz");
}

#[test]
fn body_zstd_corrupt_invalid_encoding() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let mut body = zstd_with_checksum(br#"[{"a":1},{"a":2}]"#);
    assert_ne!(zstd_fhd(&body) & 0b100, 0, "帧应带内容校验值");
    let n = body.len();
    for b in &mut body[n - 4..] {
        *b ^= 0xFF;
    }
    post_json(&server, "zz", "zstd", &body).assert_envelope(400, "invalid_encoding");
    assert_stream_absent(&server, "zz");
}

#[test]
fn body_other_encodings_415() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    for encoding in ["br", "gzip, gzip", "gzip, zstd"] {
        post_json(&server, "app", encoding, br#"[{"a":1}]"#)
            .assert_envelope(415, "unsupported_encoding");
    }
}

// ---------------------------------------------------------------------------
// 需求场景之外的补充测试
// ---------------------------------------------------------------------------

#[test]
fn body_encoding_list_empty_elements() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let r = post_json(&server, "app", " , gzip , ", &gzip(br#"[{"a":7}]"#));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 1);
}

#[test]
fn body_gzip_multi_member() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    let body = [gzip(br#"[{"a":1},"#), gzip(br#"{"a":2}]"#)].concat();
    let r = post_json(&server, "app", "gzip", &body);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 2);
}

#[test]
fn body_compressed_received_over_limit_413() {
    let fx = Fixture::new();
    let server = fx.start(&[("ingest.max_body_bytes", Some("1048576"))]);
    let r = server.request_streaming(
        "POST",
        "/api/default/zz/_json",
        &[("Authorization", BEARER), ("Content-Encoding", "gzip")],
        vec![0u8; 2 * 1024 * 1024],
    );
    r.assert_envelope(413, "payload_too_large");
}

/// 同上，但分块传输、不带 `Content-Length`：服务端不能按声明的长度不读即拒，要边收边计数；实现若一边收一边解压，
/// 会在读满上限之前先发现「不是 gzip 流」而回 400。
#[test]
fn body_compressed_received_over_limit_413_chunked() {
    let fx = Fixture::new();
    let server = fx.start(&[("ingest.max_body_bytes", Some("1048576"))]);
    let r = server.request_chunked(
        "POST",
        "/api/default/zz/_json",
        &[("Authorization", BEARER), ("Content-Encoding", "gzip")],
        vec![0u8; 2 * 1024 * 1024],
    );
    r.assert_envelope(413, "payload_too_large");
}

#[test]
fn body_compressed_empty_invalid_encoding() {
    let fx = Fixture::new();
    let server = fx.start(&[]);
    for encoding in ["gzip", "zstd"] {
        post_json(&server, "app", encoding, b"").assert_envelope(400, "invalid_encoding");
    }
}

#[test]
fn body_zstd_window_or_dictionary_invalid() {
    let fx = Fixture::new();
    let server = fx.start(&[]);

    // 单段帧，帧内容为 9 MiB 合法 JSON 数组：窗口即帧内容大小，超过 8 MiB
    // 窗口取 16 MiB 不小于帧内容，编码器即把帧写成单段帧
    let mut c = zstd::bulk::Compressor::new(3).unwrap();
    c.set_parameter(zstd::stream::raw::CParameter::WindowLog(24))
        .unwrap();
    let big = c.compress(&json_array_of_size(9 * 1024 * 1024)).unwrap();
    assert_ne!(zstd_fhd(&big) & 0b10_0000, 0, "帧应为单段帧");
    post_json(&server, "app", "zstd", &big).assert_envelope(400, "invalid_encoding");

    // 以字典压缩、帧头带非零 Dictionary_ID 的合法批次
    let samples: Vec<Vec<u8>> = (0..2000)
        .map(|i| {
            format!(
                r#"{{"a":{i},"msg":"request {i} served","host":"web{}"}}"#,
                i % 7
            )
            .into_bytes()
        })
        .collect();
    let dict = zstd::dict::from_samples(&samples, 4096).unwrap();
    let mut c = zstd::bulk::Compressor::with_dictionary(3, &dict).unwrap();
    let framed = c
        .compress(br#"[{"a":1,"msg":"request 1 served","host":"web1"}]"#)
        .unwrap();
    assert_ne!(zstd_fhd(&framed) & 0b11, 0, "帧头应带 Dictionary_ID");
    post_json(&server, "app", "zstd", &framed).assert_envelope(400, "invalid_encoding");
}

/// 在保持连接的 HTTP/1.1 连接上发出请求头与请求体前缀 `body`，读完应答；返回应答头（小写名）与随后连接是否被关闭。
fn keep_alive_request(server: &Server, head: &str, body: &[u8]) -> (String, bool) {
    use std::io::Read;
    let mut s = std::net::TcpStream::connect(server.addr).unwrap();
    s.set_read_timeout(Some(std::time::Duration::from_secs(10)))
        .unwrap();
    s.write_all(head.as_bytes()).unwrap();
    s.write_all(body).unwrap();
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    let split = loop {
        let n = s.read(&mut buf).unwrap();
        assert!(n > 0, "应答头之前连接即关闭");
        raw.extend_from_slice(&buf[..n]);
        if let Some(p) = raw.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
    };
    let headers = String::from_utf8_lossy(&raw[..split]).to_ascii_lowercase();
    let length: usize = headers
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .map(|v| v.trim().parse().unwrap())
        .unwrap();
    let mut rest = raw.len() - split - 4;
    while rest < length {
        let n = s.read(&mut buf).unwrap();
        assert!(n > 0, "应答体之前连接即关闭");
        rest += n;
    }
    // 连接被关闭：读到 EOF，或服务端关闭时有未读字节而收到 RST
    let closed = match s.read(&mut buf) {
        Ok(0) => true,
        Ok(_) => false,
        Err(e) => e.kind() == std::io::ErrorKind::ConnectionReset,
    };
    (headers, closed)
}

/// 回归：不读完请求体就以 413 作答时，应答携带 `Connection: close` 并随后关闭连接（REQ-AMB-ING-BODY-ENCODING）。
/// 两个出口：`Content-Length` 已超出（一字节不读），与分块传输边读边计数越限。
#[test]
fn body_unread_413_connection_close() {
    let fx = Fixture::new();
    let server = fx.start(&[("ingest.max_body_bytes", Some("1024"))]);
    let addr = server.addr;
    let declared = format!(
        "POST /api/default/zz/_json HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {BEARER}\r\n\
         Content-Length: 1048576\r\n\r\n"
    );
    let (headers, closed) = keep_alive_request(&server, &declared, b"");
    assert!(headers.starts_with("http/1.1 413"), "{headers}");
    assert!(headers.contains("\r\nconnection: close"), "{headers}");
    assert!(closed);

    let chunked = format!(
        "POST /api/default/zz/_json HTTP/1.1\r\nHost: {addr}\r\nAuthorization: {BEARER}\r\n\
         Transfer-Encoding: chunked\r\n\r\n"
    );
    let mut chunk = b"800\r\n".to_vec();
    chunk.extend_from_slice(&[b' '; 0x800]);
    chunk.extend_from_slice(b"\r\n");
    let (headers, closed) = keep_alive_request(&server, &chunked, &chunk);
    assert!(headers.starts_with("http/1.1 413"), "{headers}");
    assert!(headers.contains("\r\nconnection: close"), "{headers}");
    assert!(closed);
}
