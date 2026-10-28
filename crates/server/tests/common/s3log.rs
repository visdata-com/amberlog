//! 加密场景的「台架对象存储请求日志」（REQ-AMB-ARCH-ENCRYPTION 的场景用它核对发往对象存储的请求头）：本测试进程里的 TLS
//! 转发层。被测进程把 `object_store.s3.endpoint` 指向它；它以台架 MinIO 的服务端证书（CA-S 签发、主题备用名含
//! `127.0.0.1`）终止 TLS，再经 TLS 原样转发给 MinIO，逐个记下请求的方法、目标与请求头，以及响应的状态码与响应头。
//! 转发的是原样字节（`Host` 不改），SigV4 签名照常有效。
//!
//! 只解析 HTTP/1.1：请求体按 `Content-Length` 或分块跳过；响应体按请求方法、状态码、`Content-Length` 或分块跳过。

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_rustls::{TlsAcceptor, TlsConnector};

/// 一个经转发层的请求与它的响应。头名一律小写。
#[derive(Debug, Clone)]
pub struct Exchange {
    pub method: String,
    pub target: String,
    pub headers: Vec<(String, String)>,
    pub status: Option<u16>,
    pub response_headers: Vec<(String, String)>,
}

impl Exchange {
    pub fn header(&self, name: &str) -> Option<&str> {
        find(&self.headers, name)
    }

    pub fn response_header(&self, name: &str) -> Option<&str> {
        find(&self.response_headers, name)
    }

    /// 名称以 `prefix` 开头的请求头。
    pub fn headers_with_prefix(&self, prefix: &str) -> Vec<&(String, String)> {
        self.headers
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .collect()
    }

    /// S3 操作的粗分：按方法与查询参数。
    pub fn op(&self) -> &'static str {
        let q = self.target.split_once('?').map_or("", |(_, q)| q);
        let has = |k: &str| {
            q.split('&')
                .any(|p| p == k || p.starts_with(&format!("{k}=")))
        };
        match self.method.as_str() {
            "PUT" if has("partNumber") => "UploadPart",
            "PUT" => "PutObject",
            "POST" if has("uploads") => "CreateMultipartUpload",
            "POST" if has("uploadId") => "CompleteMultipartUpload",
            "POST" if has("delete") => "DeleteObjects",
            "DELETE" if has("uploadId") => "AbortMultipartUpload",
            "DELETE" => "DeleteObject",
            "HEAD" => "HeadObject",
            "GET" if has("list-type") => "ListObjects",
            "GET" => "GetObject",
            _ => "Other",
        }
    }
}

fn find<'a>(headers: &'a [(String, String)], name: &str) -> Option<&'a str> {
    headers
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
}

type Log = Arc<Mutex<Vec<Exchange>>>;

/// 转发层：随本值析构而停止。
pub struct S3Log {
    pub addr: SocketAddr,
    log: Log,
    _rt: tokio::runtime::Runtime,
}

impl S3Log {
    /// 在 127.0.0.1 的随机端口上起转发层，转发给 `upstream`（`https://127.0.0.1:<端口>`）。
    pub fn start(upstream: &str, ca_pem: &str, cert_pem: &str, key_pem: &str) -> S3Log {
        let upstream: SocketAddr = upstream
            .strip_prefix("https://")
            .expect("加密台架端点是 https://")
            .trim_end_matches('/')
            .parse()
            .unwrap();
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let chain: Vec<CertificateDer<'static>> =
            CertificateDer::pem_slice_iter(cert_pem.as_bytes())
                .map(Result::unwrap)
                .collect();
        let key = PrivateKeyDer::from_pem_slice(key_pem.as_bytes()).unwrap();
        let server = ServerConfig::builder_with_provider(Arc::clone(&provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(chain, key)
            .unwrap();
        let mut roots = RootCertStore::empty();
        for c in CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
            roots.add(c.unwrap()).unwrap();
        }
        let client = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
        let acceptor = TlsAcceptor::from(Arc::new(server));
        let connector = TlsConnector::from(Arc::new(client));
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let listener = rt.block_on(TcpListener::bind("127.0.0.1:0")).unwrap();
        let addr = listener.local_addr().unwrap();
        let log: Log = Arc::default();
        let sink = Arc::clone(&log);
        rt.spawn(async move {
            loop {
                let Ok((tcp, _)) = listener.accept().await else {
                    continue;
                };
                let (acceptor, connector, sink) =
                    (acceptor.clone(), connector.clone(), Arc::clone(&sink));
                tokio::spawn(async move {
                    let _ = relay(tcp, upstream, acceptor, connector, sink).await;
                });
            }
        });
        S3Log { addr, log, _rt: rt }
    }

    /// 被测进程配置用的端点。
    pub fn endpoint(&self) -> String {
        format!("https://{}", self.addr)
    }

    /// 到此刻为止记下的全部请求。
    pub fn exchanges(&self) -> Vec<Exchange> {
        self.log.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.log.lock().unwrap().clear();
    }
}

async fn relay(
    tcp: TcpStream,
    upstream: SocketAddr,
    acceptor: TlsAcceptor,
    connector: TlsConnector,
    log: Log,
) -> std::io::Result<()> {
    let down = acceptor.accept(tcp).await?;
    let up_tcp = TcpStream::connect(upstream).await?;
    let up = connector
        .connect(ServerName::IpAddress(upstream.ip().into()), up_tcp)
        .await?;
    let (dr, dw) = tokio::io::split(down);
    let (ur, uw) = tokio::io::split(up);
    // 本连接上尚未收到响应的请求在日志里的下标与方法（HTTP/1.1 按序作答）
    let pending: Arc<Mutex<VecDeque<(usize, String)>>> = Arc::default();
    let requests = pump(
        dr,
        uw,
        Side::Request,
        Arc::clone(&log),
        Arc::clone(&pending),
    );
    let responses = pump(ur, dw, Side::Response, log, pending);
    let _ = tokio::join!(requests, responses);
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Side {
    Request,
    Response,
}

/// 原样搬运 `from` → `to`，同时按 HTTP/1.1 切出报文头记进日志。
async fn pump(
    mut from: impl AsyncRead + Unpin,
    mut to: impl AsyncWrite + Unpin,
    side: Side,
    log: Log,
    pending: Arc<Mutex<VecDeque<(usize, String)>>>,
) -> std::io::Result<()> {
    let mut parser = Parser::default();
    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = from.read(&mut buf).await?;
        if n == 0 {
            let _ = to.shutdown().await;
            return Ok(());
        }
        // 先记日志再转发：请求在上游作答之前已排进 `pending`，响应头在客户端收到之前已记下
        parser.feed(&buf[..n], |head| match side {
            Side::Request => {
                let (method, target, headers) = request_head(head);
                let mut l = log.lock().unwrap();
                l.push(Exchange {
                    method: method.clone(),
                    target,
                    headers: headers.clone(),
                    status: None,
                    response_headers: Vec::new(),
                });
                pending
                    .lock()
                    .unwrap()
                    .push_back((l.len() - 1, method.clone()));
                body_len(&method, None, &headers)
            }
            Side::Response => {
                let (status, headers) = response_head(head);
                let (i, method) = pending
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or((usize::MAX, String::new()));
                if let Some(e) = log.lock().unwrap().get_mut(i) {
                    e.status = Some(status);
                    e.response_headers = headers.clone();
                }
                body_len(&method, Some(status), &headers)
            }
        });
        to.write_all(&buf[..n]).await?;
        // TLS 写端在 TCP 写不进时会把已收下的字节留在 rustls 的发送缓冲里、照样报告写完；不 flush 的话，报文的最后一段
        // 可能一直留在缓冲里（此后这边只等读），对端收不齐报文体，不作答
        to.flush().await?;
    }
}

/// 报文体的长度：固定字节数、分块，或读到连接关闭为止。
#[derive(Debug, Clone, Copy)]
enum Body {
    Fixed(usize),
    Chunked,
    UntilClose,
}

fn body_len(method: &str, status: Option<u16>, headers: &[(String, String)]) -> Body {
    if let Some(s) = status
        && (method == "HEAD" || s == 204 || s == 304 || (100..200).contains(&s))
    {
        return Body::Fixed(0);
    }
    if find(headers, "transfer-encoding").is_some_and(|v| v.eq_ignore_ascii_case("chunked")) {
        return Body::Chunked;
    }
    match find(headers, "content-length").and_then(|v| v.parse().ok()) {
        Some(n) => Body::Fixed(n),
        None if status.is_some() => Body::UntilClose,
        None => Body::Fixed(0),
    }
}

fn split_head(head: &str) -> (Vec<&str>, Vec<(String, String)>) {
    let mut lines = head.split("\r\n");
    let first: Vec<&str> = lines.next().unwrap_or_default().split(' ').collect();
    let headers = lines
        .filter_map(|l| l.split_once(':'))
        .map(|(k, v)| (k.trim().to_ascii_lowercase(), v.trim().to_owned()))
        .collect();
    (first, headers)
}

fn request_head(head: &str) -> (String, String, Vec<(String, String)>) {
    let (first, headers) = split_head(head);
    (
        first.first().copied().unwrap_or_default().to_owned(),
        first.get(1).copied().unwrap_or_default().to_owned(),
        headers,
    )
}

fn response_head(head: &str) -> (u16, Vec<(String, String)>) {
    let (first, headers) = split_head(head);
    (
        first.get(1).and_then(|s| s.parse().ok()).unwrap_or(0),
        headers,
    )
}

/// 增量切分 HTTP/1.1 报文：报文头交给回调，回调给出报文体的长度，随后跳过报文体。
#[derive(Default)]
struct Parser {
    buf: Vec<u8>,
    state: State,
}

#[derive(Default)]
enum State {
    #[default]
    Head,
    Skip(usize),
    /// 分块：`None` 为等待块长度行，`Some(n)` 为跳过 n 字节（含块后的 CRLF）。
    Chunk(Option<usize>),
    /// 末块之后的尾部，读到空行为止。
    Trailer,
    Drain,
}

impl Parser {
    fn feed(&mut self, data: &[u8], mut on_head: impl FnMut(&str) -> Body) {
        self.buf.extend_from_slice(data);
        loop {
            match self.state {
                State::Drain => {
                    self.buf.clear();
                    return;
                }
                State::Head => {
                    let Some(end) = self.buf.windows(4).position(|w| w == b"\r\n\r\n") else {
                        return;
                    };
                    let head = String::from_utf8_lossy(&self.buf[..end]).into_owned();
                    self.buf.drain(..end + 4);
                    self.state = match on_head(&head) {
                        Body::Fixed(n) => State::Skip(n),
                        Body::Chunked => State::Chunk(None),
                        Body::UntilClose => State::Drain,
                    };
                }
                State::Skip(n) => {
                    let k = n.min(self.buf.len());
                    self.buf.drain(..k);
                    if k < n {
                        self.state = State::Skip(n - k);
                        return;
                    }
                    self.state = State::Head;
                }
                State::Chunk(None) => {
                    let Some(eol) = self.buf.windows(2).position(|w| w == b"\r\n") else {
                        return;
                    };
                    let line = String::from_utf8_lossy(&self.buf[..eol]).into_owned();
                    self.buf.drain(..eol + 2);
                    let size =
                        usize::from_str_radix(line.split(';').next().unwrap_or("").trim(), 16)
                            .unwrap_or(0);
                    self.state = if size == 0 {
                        State::Trailer
                    } else {
                        State::Chunk(Some(size + 2))
                    };
                }
                State::Chunk(Some(n)) => {
                    let k = n.min(self.buf.len());
                    self.buf.drain(..k);
                    if k < n {
                        self.state = State::Chunk(Some(n - k));
                        return;
                    }
                    self.state = State::Chunk(None);
                }
                State::Trailer => {
                    let Some(eol) = self.buf.windows(2).position(|w| w == b"\r\n") else {
                        return;
                    };
                    let empty = eol == 0;
                    self.buf.drain(..eol + 2);
                    if empty {
                        self.state = State::Head;
                    }
                }
            }
        }
    }
}
