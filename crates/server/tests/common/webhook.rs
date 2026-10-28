//! 定时告警场景（REQ-AMB-ALR-WEBHOOK、REQ-AMB-ALR-RULES、REQ-AMB-ALR-EVALUATIONS、REQ-AMB-ARCH-ENCRYPTION-V2）的
//! 测试接收端：在 127.0.0.1 的随机端口上手写的 HTTP/1.1 服务，明文或 TLS（rustls，ring 后端）。
//!
//! - 每个收到的请求记下到达时刻（测试进程的系统时钟，读完请求体之时）、方法、请求目标、请求头（名的大小写原样）与
//!   请求体原字节，以及是否经 TLS。
//! - 回应按连接的先后（自 0 起）由 [`Receiver::plan`] 设定：状态码、回应前的延迟、`Location` 头、响应体，或「不接受
//!   连接」（接受连接后不读请求即关闭，不记为收到的请求）；也可按请求本身（如请求目标）决定回应。回应后关闭连接。
//! - TLS 握手失败的连接（被测进程不信任接收端的证书）读不出请求，同样不记。
//! - 「指向一个不接受连接的地址」不用本接收端，用 [`Refusing`]：绑定而不监听的本机端口。

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::{ServerConfig, ServerConnection, StreamOwned};

use super::tls::Pair;

/// 收到的一个请求。
#[derive(Debug, Clone)]
pub struct Request {
    /// 到达时刻（读完请求体之时，测试进程的系统时钟）。
    pub at: SystemTime,
    pub method: String,
    /// 请求行中的请求目标原文（路径与查询串）。
    pub target: String,
    /// 请求头，名与值原样（值去掉首尾空白），按收到的次序。
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
    /// 是否经 TLS 收到。
    pub tls: bool,
}

impl Request {
    /// 按 ASCII 不区分大小写找第一个同名请求头的值。
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// 请求体按 JSON 解析。
    pub fn json(&self) -> serde_json::Value {
        serde_json::from_slice(&self.body).unwrap_or_else(|e| {
            panic!(
                "请求体不是 JSON（{e}）：{}",
                String::from_utf8_lossy(&self.body)
            )
        })
    }

    /// 请求体的 `scheduled_at`（不是 JSON 或没有该字段时为 `None`）。
    pub fn scheduled_at(&self) -> Option<String> {
        let v: serde_json::Value = serde_json::from_slice(&self.body).ok()?;
        v["scheduled_at"].as_str().map(str::to_owned)
    }

    /// 到达时刻的 UNIX 微秒数（测试进程的系统时钟，不含被测进程的 `TESTBED_CLOCK_OFFSET`）。
    pub fn at_us(&self) -> i64 {
        self.at
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64
    }

    /// 本请求的到达时刻比 `earlier` 晚多久（早于它时为 0）。
    pub fn after(&self, earlier: &Request) -> Duration {
        self.at.duration_since(earlier.at).unwrap_or_default()
    }
}

/// 对一个请求的回应。
#[derive(Debug, Clone)]
pub struct Reply {
    pub status: u16,
    /// 读完请求之后、写回应之前等待的时长。
    pub delay: Duration,
    pub location: Option<String>,
    pub body: Vec<u8>,
}

impl Reply {
    pub fn status(status: u16) -> Reply {
        Reply {
            status,
            delay: Duration::ZERO,
            location: None,
            body: Vec::new(),
        }
    }

    pub fn delay(mut self, delay: Duration) -> Reply {
        self.delay = delay;
        self
    }

    pub fn location(mut self, location: &str) -> Reply {
        self.location = Some(location.to_owned());
        self
    }

    pub fn body(mut self, body: &[u8]) -> Reply {
        self.body = body.to_vec();
        self
    }
}

/// 对一个连接的处置。
#[derive(Clone)]
pub enum Step {
    /// 接受连接后不读请求即关闭；不记为收到的请求。
    Refuse,
    /// 读完请求后按给定回应作答。
    Reply(Reply),
    /// 读完请求后按请求本身决定回应。
    By(Arc<dyn Fn(&Request) -> Reply + Send + Sync>),
}

impl Step {
    pub fn status(status: u16) -> Step {
        Step::Reply(Reply::status(status))
    }

    pub fn by(f: impl Fn(&Request) -> Reply + Send + Sync + 'static) -> Step {
        Step::By(Arc::new(f))
    }
}

impl From<Reply> for Step {
    fn from(r: Reply) -> Step {
        Step::Reply(r)
    }
}

type Plan = Arc<dyn Fn(usize) -> Step + Send + Sync>;

struct Shared {
    requests: Mutex<Vec<Request>>,
    plan: Mutex<Plan>,
    connections: AtomicUsize,
    stop: AtomicBool,
}

/// 不接受连接的本机端口：绑定而不监听，连接即被拒绝；随本值析构而释放。
///
/// 不用先绑定再释放的端口（[`super::free_port`]）：释放之后，并行的其他测试（nextest 下是别的进程）可能绑定同一端口
/// 并监听，连接就不再被拒绝。本值持有期间端口一直占着；绑定时不设 `SO_REUSEADDR`，别人带该选项也绑不上，内核也不会
/// 把它分给取随机端口的绑定或向外的连接。
pub struct Refusing(tokio::net::TcpSocket);

impl Refusing {
    pub fn new() -> Refusing {
        let socket = tokio::net::TcpSocket::new_v4().unwrap();
        socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
        Refusing(socket)
    }

    pub fn port(&self) -> u16 {
        self.0.local_addr().unwrap().port()
    }

    /// `http://127.0.0.1:<端口><path>`。
    pub fn url(&self, path: &str) -> String {
        format!("http://127.0.0.1:{}{path}", self.port())
    }
}

/// 测试接收端；随本值析构而停止接受连接。
pub struct Receiver {
    addr: SocketAddr,
    tls: bool,
    shared: Arc<Shared>,
}

impl Receiver {
    /// 明文 HTTP 接收端，缺省对每个请求立即回 HTTP 200。
    pub fn http() -> Receiver {
        Receiver::start(None)
    }

    /// 以 `cert` 的证书（链）与私钥提供 HTTPS 的接收端（证书由 [`super::tls::Pki::leaf`] 签发，主题备用名含
    /// `127.0.0.1`），缺省对每个请求立即回 HTTP 200。
    pub fn https(cert: &Pair) -> Receiver {
        let chain: Vec<CertificateDer<'static>> = CertificateDer::pem_file_iter(&cert.cert)
            .unwrap()
            .map(|c| c.unwrap())
            .collect();
        let key = PrivateKeyDer::from_pem_file(&cert.key).unwrap();
        let config =
            ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_safe_default_protocol_versions()
                .unwrap()
                .with_no_client_auth()
                .with_single_cert(chain, key)
                .unwrap();
        Receiver::start(Some(Arc::new(config)))
    }

    fn start(tls: Option<Arc<ServerConfig>>) -> Receiver {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();
        let ok: Plan = Arc::new(|_| Step::status(200));
        let shared = Arc::new(Shared {
            requests: Mutex::new(Vec::new()),
            plan: Mutex::new(ok),
            connections: AtomicUsize::new(0),
            stop: AtomicBool::new(false),
        });
        let rx = Receiver {
            addr,
            tls: tls.is_some(),
            shared: Arc::clone(&shared),
        };
        std::thread::spawn(move || {
            for conn in listener.incoming() {
                if shared.stop.load(Ordering::SeqCst) {
                    break;
                }
                let Ok(conn) = conn else { continue };
                let n = shared.connections.fetch_add(1, Ordering::SeqCst);
                let step = (shared.plan.lock().unwrap())(n);
                let shared = Arc::clone(&shared);
                let tls = tls.clone();
                std::thread::spawn(move || serve(conn, step, tls, &shared));
            }
        });
        rx
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// 本接收端上 `path`（含查询串）的地址，如 `http://127.0.0.1:<端口>/hook`。
    pub fn url(&self, path: &str) -> String {
        let scheme = if self.tls { "https" } else { "http" };
        format!("{scheme}://{}{path}", self.addr)
    }

    /// 按连接的先后（自 0 起，含不接受的连接）决定处置。此后接受的连接起作用。
    pub fn plan(&self, f: impl Fn(usize) -> Step + Send + Sync + 'static) {
        *self.shared.plan.lock().unwrap() = Arc::new(f);
    }

    /// 自此刻起接受的第 i 个连接取 `steps[i]`，其后的取 `rest`。
    pub fn script(&self, steps: Vec<Step>, rest: Step) {
        let base = self.connections();
        self.plan(move |n| {
            n.checked_sub(base)
                .and_then(|i| steps.get(i))
                .cloned()
                .unwrap_or_else(|| rest.clone())
        });
    }

    /// 自此刻起每个连接都取 `step`。
    pub fn always(&self, step: Step) {
        self.plan(move |_| step.clone());
    }

    /// 已接受的连接数（含不接受的与握手失败的）。
    pub fn connections(&self) -> usize {
        self.shared.connections.load(Ordering::SeqCst)
    }

    /// 已收到的请求。
    pub fn requests(&self) -> Vec<Request> {
        self.shared.requests.lock().unwrap().clone()
    }

    /// 等到（至多 `limit`）收到的请求满足 `pred`，返回此刻收到的全部请求；超时返回 `None`。
    pub fn wait_until(
        &self,
        limit: Duration,
        pred: impl Fn(&[Request]) -> bool,
    ) -> Option<Vec<Request>> {
        let deadline = Instant::now() + limit;
        loop {
            let got = self.requests();
            if pred(&got) {
                return Some(got);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    /// 等到（至多 `limit`）至少收到 `n` 个请求，返回此刻收到的全部请求；超时则测试失败。
    pub fn wait_for(&self, n: usize, limit: Duration) -> Vec<Request> {
        self.wait_until(limit, |r| r.len() >= n).unwrap_or_else(|| {
            panic!(
                "{limit:?} 内只收到 {} 个请求，要 {n} 个：{:#?}",
                self.requests().len(),
                self.requests()
            )
        })
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        self.shared.stop.store(true, Ordering::SeqCst);
        // 让阻塞在 accept 上的线程醒来看到停止标志
        let _ = TcpStream::connect(self.addr);
    }
}

fn serve(conn: TcpStream, step: Step, tls: Option<Arc<ServerConfig>>, shared: &Shared) {
    if matches!(step, Step::Refuse) {
        drop(conn);
        return;
    }
    if shared.stop.load(Ordering::SeqCst) {
        return;
    }
    let _ = conn.set_read_timeout(Some(Duration::from_secs(30)));
    match tls {
        None => exchange(conn, false, step, shared),
        Some(config) => {
            let Ok(server) = ServerConnection::new(config) else {
                return;
            };
            exchange(StreamOwned::new(server, conn), true, step, shared);
        }
    }
}

fn exchange(mut stream: impl Read + Write, tls: bool, step: Step, shared: &Shared) {
    // 握手失败、对端未发完请求即关闭，都读不出请求，不记
    let Ok(Some(req)) = read_request(&mut stream, tls) else {
        return;
    };
    shared.requests.lock().unwrap().push(req.clone());
    let reply = match step {
        Step::Reply(r) => r,
        Step::By(f) => f(&req),
        Step::Refuse => unreachable!(),
    };
    if !reply.delay.is_zero() {
        std::thread::sleep(reply.delay);
    }
    let mut head = format!(
        "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
        reply.status,
        reason(reply.status),
        reply.body.len()
    );
    if let Some(loc) = &reply.location {
        head.push_str(&format!("Location: {loc}\r\n"));
    }
    head.push_str("\r\n");
    // 对端已因超时断开时写失败，忽略
    let _ = stream
        .write_all(head.as_bytes())
        .and_then(|_| stream.write_all(&reply.body))
        .and_then(|_| stream.flush());
}

/// 读一个请求：请求行、请求头，再按 `Content-Length` 读请求体。读不全时返回 `None`。
fn read_request(stream: &mut impl Read, tls: bool) -> io::Result<Option<Request>> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        if let Some(p) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break p;
        }
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        buf.extend_from_slice(&chunk[..n]);
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut first = lines.next().unwrap_or_default().splitn(3, ' ');
    let method = first.next().unwrap_or_default().to_owned();
    let target = first.next().unwrap_or_default().to_owned();
    let headers: Vec<(String, String)> = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.to_owned(), v.trim().to_owned()))
        .collect();
    let len: usize = headers
        .iter()
        .find(|(k, _)| k.eq_ignore_ascii_case("content-length"))
        .and_then(|(_, v)| v.parse().ok())
        .unwrap_or(0);
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < len {
        let n = stream.read(&mut chunk)?;
        if n == 0 {
            return Ok(None);
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(len);
    Ok(Some(Request {
        at: SystemTime::now(),
        method,
        target,
        headers,
        body,
        tls,
    }))
}

fn reason(status: u16) -> &'static str {
    match status {
        200 => "OK",
        202 => "Accepted",
        204 => "No Content",
        302 => "Found",
        401 => "Unauthorized",
        408 => "Request Timeout",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        503 => "Service Unavailable",
        _ => "Status",
    }
}
