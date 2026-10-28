//! 经 `/api/` 端点的摄取与查询、WAL 写入故障注入（REQ-AMB-ING-*、REQ-AMB-LIFE-DURABILITY 的系统测试共用）。

use std::path::PathBuf;

use serde_json::{Value, json};

use super::{BEARER, Fixture, Kv, Resp, Server};

/// `flush.interval` 为 `1h`：测试期间不因定时 flush 而提交。
pub const FLUSH_1H: Kv = ("flush.interval", Some("\"1h\""));

/// 覆盖全部测试数据的查询时间范围上界（UTC 微秒，2096 年）。
pub const FAR_END_US: i64 = 4_000_000_000_000_000;

/// `POST /api/default/{stream}/_json`，带有效凭据。
pub fn ingest(server: &Server, stream: &str, body: &str) -> Resp {
    ingest_with(server, stream, &[], body.as_bytes())
}

/// 同 [`ingest`]，另加请求头。
pub fn ingest_with(server: &Server, stream: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    let mut all = vec![("Authorization", BEARER)];
    all.extend_from_slice(headers);
    server.request("POST", &format!("/api/default/{stream}/_json"), &all, body)
}

/// 断言摄取得 200，返回响应体。
pub fn ingest_ok(server: &Server, stream: &str, body: &str) -> Value {
    let r = ingest(server, stream, body);
    assert_eq!(r.status, 200, "{stream} ← {body}：{}", r.text());
    r.json()
}

/// 以 `[0, FAR_END_US)` 为时间范围、`size` 为 10000 执行 `sql` 的 `_search` 请求体。
pub fn search_body(sql: &str) -> String {
    json!({"query": {"sql": sql, "start_time": 0, "end_time": FAR_END_US, "size": 10000}})
        .to_string()
}

/// 以覆盖全部测试数据的时间范围执行 `sql`，返回应答（不论状态码）。
pub fn search_resp(server: &Server, sql: &str) -> Resp {
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        search_body(sql).as_bytes(),
    )
}

/// 以覆盖全部测试数据的时间范围执行 `sql`，断言 200，返回 `hits`。
pub fn search(server: &Server, sql: &str) -> Vec<Value> {
    let r = search_resp(server, sql);
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    r.json()["hits"].as_array().unwrap().clone()
}

/// WAL 写入注入的控制文件（`TESTBED_FAULT_WAL`）：存在且含操作名（`write`、`fsync`、`truncate`、`delete`）时，该操作以 I/O 错误失败。
pub struct WalFault(PathBuf);

impl WalFault {
    pub fn new(fx: &Fixture) -> WalFault {
        WalFault(fx.root().join("wal-fault"))
    }

    pub fn inject(&self, ops: &str) {
        std::fs::write(&self.0, ops).unwrap();
    }

    pub fn clear(&self) {
        let _ = std::fs::remove_file(&self.0);
    }

    /// 控制文件的路径（自行组装启动命令时传给 `TESTBED_FAULT_WAL`）。
    pub fn path(&self) -> &std::path::Path {
        &self.0
    }

    /// 以给定配置启动、并让进程读这个控制文件。
    pub fn start(&self, fx: &Fixture, overrides: &[Kv]) -> Server {
        let mut cmd = fx.serve(overrides);
        cmd.env("TESTBED_FAULT_WAL", &self.0);
        Server::start(cmd)
    }
}

/// 请求体尚未发送完毕的 `_json` 请求（慢请求）：已发出请求头与请求体的前一半，连接保持打开。
pub struct SlowRequest {
    stream: std::net::TcpStream,
    rest: Vec<u8>,
}

impl SlowRequest {
    /// 向 `stream` 的 `_json` 发出 `body` 的前一半，返回时服务端已开始处理该请求（已计入在途）。
    pub fn start(server: &Server, stream: &str, body: &str) -> SlowRequest {
        use std::io::Write;

        let mut s = std::net::TcpStream::connect(server.addr).expect("连接 amberlog");
        s.set_read_timeout(Some(std::time::Duration::from_secs(30)))
            .unwrap();
        let head = format!(
            "POST /api/default/{stream}/_json HTTP/1.1\r\nHost: {}\r\nConnection: close\r\n\
             Authorization: {BEARER}\r\nContent-Length: {}\r\n\r\n",
            server.addr,
            body.len()
        );
        let (first, rest) = body.as_bytes().split_at(body.len() / 2);
        s.write_all(head.as_bytes()).unwrap();
        s.write_all(first).unwrap();
        s.flush().unwrap();
        // 等可观察条件：服务端一侧的这条连接已把收到的字节（请求头与前一半请求体）读走，即已解析请求头、
        // 把请求交给处理流程、正在读请求体（不靠固定时长）
        let client = s.local_addr().unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while rx_queue(server.addr, client) != Some(0) {
            assert!(
                std::time::Instant::now() < deadline,
                "10 秒内服务端没有读走慢请求已发出的字节"
            );
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        SlowRequest {
            stream: s,
            rest: rest.to_vec(),
        }
    }

    /// 发完请求体，读回应答。
    pub fn finish(mut self) -> Resp {
        use std::io::{Read, Write};

        self.stream.write_all(&self.rest).unwrap();
        let mut raw = Vec::new();
        self.stream.read_to_end(&mut raw).unwrap();
        super::parse_response(&raw)
    }
}

/// `/proc/net/tcp` 中本地地址为 `local`、对端为 `remote` 的那条 IPv4 连接的接收队列字节数（尚未被进程读走的字节）；
/// 找不到该连接为 `None`。
fn rx_queue(local: std::net::SocketAddr, remote: std::net::SocketAddr) -> Option<u64> {
    let hex = |a: std::net::SocketAddr| match a {
        std::net::SocketAddr::V4(v4) => Some(format!(
            "{:08X}:{:04X}",
            u32::from_le_bytes(v4.ip().octets()),
            v4.port()
        )),
        std::net::SocketAddr::V6(_) => None,
    };
    let (local, remote) = (hex(local)?, hex(remote)?);
    let table = std::fs::read_to_string("/proc/net/tcp").ok()?;
    table.lines().skip(1).find_map(|line| {
        let f: Vec<&str> = line.split_whitespace().collect();
        if f.len() > 4 && f[1] == local && f[2] == remote {
            u64::from_str_radix(f[4].split_once(':')?.1, 16).ok()
        } else {
            None
        }
    })
}

// ---------------------------------------------------------------------------
// 治理计数（REQ-AMB-OBS-INGEST-COUNTERS）
// ---------------------------------------------------------------------------

/// 分钟窗口的长度，UTC 微秒。
pub const MINUTE_US: i64 = 60_000_000;
/// 小时的长度，UTC 微秒。
pub const HOUR_US: i64 = 3_600_000_000;

/// `POST /api/default/{stream}/_sent`，带有效凭据。
pub fn sent(server: &Server, stream: &str, body: &str) -> Resp {
    sent_with(server, stream, &[], body.as_bytes())
}

/// 同 [`sent`]，另加请求头。
pub fn sent_with(server: &Server, stream: &str, headers: &[(&str, &str)], body: &[u8]) -> Resp {
    let mut all = vec![("Authorization", BEARER)];
    all.extend_from_slice(headers);
    server.request("POST", &format!("/api/default/{stream}/_sent"), &all, body)
}

/// 一个 `_sent` 元素（`window_start` 为 `w`）。
pub fn sent_element(source: &str, w: i64, n: i64) -> String {
    json!({"source": source, "window_start": w, "window_end": w + MINUTE_US, "sent": n}).to_string()
}

/// 以空的 `_sent` 上报建出 stream（不产生任何计数）。
pub fn create_stream(server: &Server, stream: &str) {
    let r = sent(server, stream, "[]");
    assert_eq!(r.status, 200, "{}", r.text());
}

/// 当前时刻，UTC 微秒。
pub fn now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as i64
}

/// `us` 所在整分钟的起点。
pub fn minute_of(us: i64) -> i64 {
    us.div_euclid(MINUTE_US) * MINUTE_US
}

/// 当前分钟剩余不足 `margin` 时等到下一分钟开始，使接下来 `margin` 之内发出的请求落在同一分钟；
/// 返回该分钟的起点。`hour_margin` 为真时同样避开小时末尾的最后一分钟。
pub fn settle_minute(margin: std::time::Duration, hour_margin: bool) -> i64 {
    loop {
        let now = now_us();
        let left = minute_of(now) + MINUTE_US - now;
        let last_minute = (now.rem_euclid(HOUR_US)) >= HOUR_US - MINUTE_US;
        if left > margin.as_micros() as i64 && !(hour_margin && last_minute) {
            return minute_of(now);
        }
        std::thread::sleep(std::time::Duration::from_micros(left as u64 + 1000));
    }
}

/// `file://` 对象存储中 `stream` 在 `hour` 所在小时的计数对象的字节；不存在为 `None`。
pub fn counter_bytes(fx: &Fixture, stream: &str, hour: i64) -> Option<Vec<u8>> {
    std::fs::read(counter_file(fx, stream, hour)).ok()
}

/// 计数对象在 `file://` 对象存储中的文件。
pub fn counter_file(fx: &Fixture, stream: &str, hour: i64) -> PathBuf {
    fx.store_dir()
        .join(format!("amberlog/default/{stream}"))
        .join(amberlog_ingest::counters::counters_path(hour))
}

/// 计数对象中 `window_start` 为 `w` 的条目；对象或条目不存在为 `None`。
pub fn counter_window(fx: &Fixture, stream: &str, w: i64) -> Option<Value> {
    window_in(&counter_bytes(fx, stream, w)?, w)
}

/// 计数对象字节中 `window_start` 为 `w` 的条目。
pub fn window_in(bytes: &[u8], w: i64) -> Option<Value> {
    let obj: Value = serde_json::from_slice(bytes).ok()?;
    obj["windows"]
        .as_array()?
        .iter()
        .find(|e| e["window_start"] == w)
        .cloned()
}

/// 反复读取 `read` 直到 `ok` 成立，至多 `limit`；返回最后一次读到的值与是否成立。
pub fn poll<T>(
    limit: std::time::Duration,
    mut read: impl FnMut() -> T,
    ok: impl Fn(&T) -> bool,
) -> (T, bool) {
    let deadline = std::time::Instant::now() + limit;
    loop {
        let v = read();
        if ok(&v) {
            return (v, true);
        }
        if std::time::Instant::now() >= deadline {
            return (v, false);
        }
        std::thread::sleep(std::time::Duration::from_millis(250));
    }
}
