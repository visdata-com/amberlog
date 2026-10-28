//! webhook 投递（REQ-AMB-ALR-WEBHOOK）与 webhook 出站设置中的信任根（REQ-AMB-ARCH-ENCRYPTION-V2 I 组）。
//!
//! - 地址与请求头的写法（URL、HEADERS）：[`parse_url`]、[`parse_headers`]，由规则校验一步调用；错误消息不含请求头的值、
//!   不含地址的查询串，含 userinfo 时不回显地址。
//! - 投递（[`Webhook`]，[`super::Delivery`] 的实现）：请求体只序列化一次（SAME-BODY）；每次尝试新建连接、发
//!   `POST`、读到响应的状态行与响应头即止，不读响应体（DELIVERED）；按状态机重试、退避与截止（ATTEMPT-TIMEOUT 至
//!   ABANDONED）；停机通知置位时中止进行中的尝试并放弃。只在内存中进行，不写本地文件或对象存储（NO-REPLAY）。
//! - 出站 HTTP/1.1 客户端是在 tokio 与 tokio-rustls（ring 后端）上手写的最小实现：请求头名按配置原样写出
//!   （REQUEST-HEADERS「名与值原样」；hyper 系的客户端会改成小写），不跟随重定向、不读代理环境变量、不解压。
//! - 信任根：系统信任的根证书（`rustls-native-certs`），配置了 `alerts.ca_file` 时追加其中全部证书；与对象存储客户端
//!   互不共用（REQ-AMB-ARCH-ENCRYPTION-V2）。证书按 `webhook_url` 的主机校验，不提供跳过验证的写法；握手失败按连接失败处置。

use std::fmt;
use std::io;
use std::net::{IpAddr, Ipv6Addr};
use std::sync::Arc;
use std::time::Duration;

use amberlog_query::search::Hit;
use amberlog_storage::BoxFuture;
use amberlog_storage::lifecycle::now_us;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use serde_json::Value;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

use super::outcome::DeliveryResult;
use super::{Delivery, DeliveryOutcome, Notice, Stop};
use crate::logging::{self, Level};
use crate::version::SEMVER;

/// 规则的投递目标：`webhook_url` 与 `webhook_headers`（已按 REQ-AMB-ALR-WEBHOOK-URL、-HEADERS 校验）。
#[derive(Debug, Clone)]
pub struct Target {
    pub url: WebhookUrl,
    /// 请求头的名与值，名按配置原样（由 [`parse_headers`] 得出；块不规定各请求头的先后）。
    pub headers: Vec<(String, HeaderValue)>,
}

/// `webhook_headers` 中一个请求头的值：`Debug` 与 `Display` 都不写出取值（REQ-AMB-ALR-WEBHOOK-HEADER-VALUES-HIDDEN）。
#[derive(Clone, PartialEq, Eq)]
pub struct HeaderValue(String);

impl HeaderValue {
    /// 只由 [`parse_headers`] 在校验之后构造（模块私有），所以写出的请求头不会含 CR、LF 等控制字符；
    /// 断言只是保险，校验过的值走不到 panic。
    fn new(value: String) -> HeaderValue {
        assert!(
            valid_header_value(&value),
            "header value violates REQ-AMB-ALR-WEBHOOK-HEADERS"
        );
        HeaderValue(value)
    }

    /// 写进请求的取值。
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

impl fmt::Display for HeaderValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("<redacted>")
    }
}

/// 已校验的 `webhook_url`（REQ-AMB-ALR-WEBHOOK-URL）。`Debug` 只写出去除查询串的地址（LOG-URL）。
#[derive(Clone, PartialEq, Eq)]
pub struct WebhookUrl {
    text: String,
    https: bool,
    /// 连接用的主机：IP 字面量去掉方括号，注册名照原文。
    host: String,
    /// 端口；写出的端口超出 0～65535 时为 `None`（每次尝试按连接失败处置）。
    port: Option<u16>,
    /// `Host` 请求头的取值：URL 的 authority（不含 userinfo，本类型也不接受 userinfo）。
    authority: String,
    /// 请求目标：路径与查询串原样，路径为空时为 `/`。
    target: String,
}

impl WebhookUrl {
    /// 配置中的原文。
    pub fn as_str(&self) -> &str {
        &self.text
    }

    /// 去除查询串的地址：凡写进运行日志与 `webhook_plaintext` 的地址都用它（REQ-AMB-ALR-WEBHOOK-LOG-URL）。
    pub fn shown(&self) -> String {
        shown_url(&self.text)
    }

    pub fn is_https(&self) -> bool {
        self.https
    }

    /// 连接用的主机：IP 字面量去掉方括号，注册名照原文。
    pub fn host(&self) -> &str {
        &self.host
    }

    /// 端口（未写时取方案的缺省端口）；写出的端口超出 0～65535 时为 `None`。
    pub fn port(&self) -> Option<u16> {
        self.port
    }

    /// `Host` 请求头的取值。
    pub fn authority(&self) -> &str {
        &self.authority
    }

    /// 请求目标：路径与查询串原样，路径为空时为 `/`。
    pub fn target(&self) -> &str {
        &self.target
    }
}

impl fmt::Debug for WebhookUrl {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "WebhookUrl({})", self.shown())
    }
}

/// 地址去除查询串（连同片段；`webhook_url` 本不含片段）。
pub fn shown_url(text: &str) -> String {
    text.split(['?', '#']).next().unwrap_or_default().to_owned()
}

/// 校验 `webhook_url`（REQ-AMB-ALR-WEBHOOK-URL）：RFC 3986 的 URI 文法，方案为小写 `https` 或 `http`，带主机，
/// 不含 userinfo 与片段。错误消息不含地址本身（地址可能带令牌）。
pub fn parse_url(text: &str) -> Result<WebhookUrl, String> {
    let Some((scheme, rest)) = text.split_once("://") else {
        return Err("webhook_url must be an absolute URL starting with https:// or http://".into());
    };
    let https = match scheme {
        "https" => true,
        "http" => false,
        s if s.eq_ignore_ascii_case("https") || s.eq_ignore_ascii_case("http") => {
            return Err("webhook_url: the scheme must be lowercase https or http".into());
        }
        _ => {
            return Err(
                "webhook_url must be an absolute URL starting with https:// or http://".into(),
            );
        }
    };
    if rest.contains('#') {
        return Err("webhook_url must not contain a fragment (#...)".into());
    }
    let (authority, path_query) = match rest.find(['/', '?']) {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    if authority.contains('@') {
        return Err(
            "webhook_url must not contain userinfo (credentials before @); send them in a request header via webhook_headers instead"
                .into(),
        );
    }
    let (host, port_text) = split_host_port(authority)?;
    if host.is_empty() {
        return Err("webhook_url must name a host".into());
    }
    let connect_host = if let Some(literal) = host.strip_prefix('[') {
        let inner = literal.strip_suffix(']').unwrap_or_default();
        inner
            .parse::<Ipv6Addr>()
            .map_err(|_| "webhook_url: the IP literal in the host is not a valid IPv6 address")?;
        inner.to_owned()
    } else {
        if !valid_chars(host, b"") {
            return Err("webhook_url: the host contains characters not allowed by RFC 3986".into());
        }
        host.to_owned()
    };
    let port = match port_text {
        None | Some("") => Some(if https { 443 } else { 80 }),
        Some(p) if p.bytes().all(|b| b.is_ascii_digit()) => p.parse::<u16>().ok(),
        Some(_) => return Err("webhook_url: the port must be decimal digits".into()),
    };
    let (path, query) = match path_query.split_once('?') {
        Some((p, q)) => (p, Some(q)),
        None => (path_query, None),
    };
    if !valid_chars(path, b":@/") || !query.is_none_or(|q| valid_chars(q, b":@/?")) {
        return Err(
            "webhook_url: the path or query contains characters not allowed by RFC 3986".into(),
        );
    }
    let target = if path.is_empty() {
        format!("/{}", query.map(|q| format!("?{q}")).unwrap_or_default())
    } else {
        path_query.to_owned()
    };
    Ok(WebhookUrl {
        text: text.to_owned(),
        https,
        host: connect_host,
        port,
        authority: authority.to_owned(),
        target,
    })
}

/// authority 拆成主机与端口（端口之前的 `:` 在 IP 字面量的方括号之外）。
fn split_host_port(authority: &str) -> Result<(&str, Option<&str>), String> {
    if authority.starts_with('[') {
        let Some(end) = authority.find(']') else {
            return Err("webhook_url: the IP literal in the host is not closed by ]".into());
        };
        let (host, rest) = authority.split_at(end + 1);
        return match rest {
            "" => Ok((host, None)),
            r if r.starts_with(':') => Ok((host, Some(&r[1..]))),
            _ => Err("webhook_url: unexpected characters after the IP literal".into()),
        };
    }
    Ok(match authority.rsplit_once(':') {
        Some((h, p)) => (h, Some(p)),
        None => (authority, None),
    })
}

/// RFC 3986 的 unreserved、pct-encoded、sub-delims 与 `extra` 中的字符。
fn valid_chars(s: &str, extra: &[u8]) -> bool {
    let b = s.as_bytes();
    let mut i = 0;
    while i < b.len() {
        let c = b[i];
        if c == b'%' {
            if i + 2 >= b.len() || !(b[i + 1].is_ascii_hexdigit() && b[i + 2].is_ascii_hexdigit()) {
                return false;
            }
            i += 3;
            continue;
        }
        let ok = c.is_ascii_alphanumeric()
            || b"-._~".contains(&c)
            || b"!$&'()*+,;=".contains(&c)
            || extra.contains(&c);
        if !ok {
            return false;
        }
        i += 1;
    }
    true
}

/// 不能由 `webhook_headers` 设置的请求头（REQ-AMB-ALR-WEBHOOK-HEADERS）。
const RESERVED_HEADERS: [&str; 6] = [
    "Host",
    "Content-Type",
    "Content-Length",
    "Transfer-Encoding",
    "Connection",
    "User-Agent",
];

/// 校验 `webhook_headers`（REQ-AMB-ALR-WEBHOOK-HEADERS）：TOML 表；键为 RFC 9110 的 token、不是保留的请求头、按 ASCII
/// 不区分大小写不重名；值为只含可见 ASCII、空格与水平制表符且首尾不是空白的非空字符串（因此不含 CR、LF，写出时
/// 不会拆出另一个请求头）。错误消息只写请求头的名，不写值。
pub fn parse_headers(value: &toml::Value) -> Result<Vec<(String, HeaderValue)>, String> {
    let Some(table) = value.as_table() else {
        return Err("webhook_headers must be a TOML table".into());
    };
    let mut out: Vec<(String, HeaderValue)> = Vec::with_capacity(table.len());
    for (name, v) in table {
        if name.is_empty() || !name.bytes().all(is_tchar) {
            return Err(format!(
                "webhook_headers: {} is not a valid header name (RFC 9110 token)",
                quote_name(name)
            ));
        }
        if let Some(r) = RESERVED_HEADERS
            .iter()
            .find(|r| r.eq_ignore_ascii_case(name))
        {
            return Err(format!(
                "webhook_headers: {name:?} may not be set; {r} is managed by amberlog"
            ));
        }
        if out.iter().any(|(k, _)| k.eq_ignore_ascii_case(name)) {
            return Err(format!(
                "webhook_headers: {name:?} is given more than once (header names are case-insensitive)"
            ));
        }
        let Some(text) = v.as_str() else {
            return Err(format!(
                "webhook_headers: the value of {name:?} must be a string"
            ));
        };
        if !valid_header_value(text) {
            return Err(format!(
                "webhook_headers: the value of {name:?} must be non-empty visible ASCII (spaces and tabs allowed \
                 inside, not at either end)"
            ));
        }
        out.push((name.clone(), HeaderValue::new(text.to_owned())));
    }
    Ok(out)
}

/// 请求头的值：只含可见 ASCII、空格与水平制表符，首尾不是空白，非空。
fn valid_header_value(text: &str) -> bool {
    let bytes = text.as_bytes();
    let visible = |b: u8| (0x21..=0x7e).contains(&b);
    !bytes.is_empty()
        && bytes.iter().all(|&b| visible(b) || b == b' ' || b == b'\t')
        && visible(bytes[0])
        && visible(bytes[bytes.len() - 1])
}

/// RFC 9110 的 tchar。
fn is_tchar(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b)
}

/// 不合法的请求头名写进错误消息时的写法：Rust 字符串字面量的转义，控制字符不原样输出。
fn quote_name(name: &str) -> String {
    format!("{name:?}")
}

/// 一次尝试自开始建立连接起的时限（REQ-AMB-ALR-WEBHOOK-ATTEMPT-TIMEOUT）。
const ATTEMPT_TIMEOUT: Duration = Duration::from_secs(10);

/// 一次投递至多的尝试数（REQ-AMB-ALR-WEBHOOK-RETRYABLE）。
const MAX_ATTEMPTS: u32 = 5;

/// 得出结果时距截止时刻不足此时长即不发尝试（REQ-AMB-ALR-WEBHOOK-FIRST-ATTEMPT 的例外）。
const FIRST_ATTEMPT_WINDOW_US: i64 = 1_000_000;

/// 响应的状态行与响应头的字节上限：超出按连接失败处置（只读到响应头为止，不读响应体）。
const MAX_RESPONSE_HEAD: usize = 64 * 1024;

/// webhook 投递（[`Delivery`] 的实现）。
pub struct Webhook {
    tls: tokio_rustls::TlsConnector,
}

impl Webhook {
    /// `alerts_ca_pem` 为 `alerts.ca_file` 的内容（已在配置一步校验，REQ-AMB-ARCH-ENCRYPTION-V2）。
    pub fn new(alerts_ca_pem: Option<&[u8]>) -> Webhook {
        let mut roots = rustls::RootCertStore::empty();
        // 系统根证书读不全时取能读到的部分（与对象存储客户端的做法相同）
        let native = rustls_native_certs::load_native_certs();
        roots.add_parsable_certificates(native.certs);
        if let Some(pem) = alerts_ca_pem {
            let certs = CertificateDer::pem_slice_iter(pem).filter_map(Result::ok);
            roots.add_parsable_certificates(certs);
        }
        let config = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring 支持缺省的 TLS 版本")
        .with_root_certificates(roots)
        .with_no_client_auth();
        Webhook {
            tls: tokio_rustls::TlsConnector::from(Arc::new(config)),
        }
    }

    async fn run(&self, notice: Notice, mut stop: Stop) -> DeliveryOutcome {
        let rule = &notice.rule;
        let target = &rule.webhook;
        let deadline_us = notice.scheduled_at.saturating_add(rule.interval_us());
        let body = request_body(&notice);
        let head = request_head(target, body.len());
        let mut attempts = 0u32;
        let mut last_status = None;
        let finish = |result: DeliveryResult, attempts: u32, last_status: Option<u16>| {
            if result == DeliveryResult::Abandoned {
                logging::log(
                    Level::Warn,
                    "webhook delivery abandoned",
                    &[
                        ("rule", Value::from(rule.name.as_str())),
                        ("url", Value::from(target.url.shown())),
                        ("attempts", Value::from(attempts)),
                        (
                            "last_http_status",
                            last_status.map_or(Value::Null, Value::from),
                        ),
                    ],
                );
            }
            DeliveryOutcome {
                result,
                attempts,
                last_status,
            }
        };
        let remaining_us = deadline_us.saturating_sub(now_us());
        if remaining_us < FIRST_ATTEMPT_WINDOW_US {
            return finish(DeliveryResult::Abandoned, 0, None);
        }
        let deadline = Instant::now() + Duration::from_micros(remaining_us as u64);
        loop {
            // 退避的计时可能晚醒：截止时刻及其之后不开始新的尝试（REQ-AMB-ALR-WEBHOOK-DEADLINE）
            if stop.is_stopped() || Instant::now() >= deadline {
                return finish(DeliveryResult::Abandoned, attempts, last_status);
            }
            attempts += 1;
            let limit = deadline.min(Instant::now() + ATTEMPT_TIMEOUT);
            let attempt =
                tokio::time::timeout_at(limit, self.attempt(&target.url, &head, &body, limit));
            // 停机、超时与截止时丢弃这次尝试的 future，连接随之关闭
            let got = tokio::select! {
                r = attempt => r,
                _ = stop.stopped() => return finish(DeliveryResult::Abandoned, attempts, last_status),
            };
            let retryable = match got {
                Ok(Ok(status)) => {
                    last_status = Some(status);
                    if (200..300).contains(&status) {
                        let result = if attempts == 1 {
                            DeliveryResult::Delivered
                        } else {
                            DeliveryResult::DeliveredAfterRetry
                        };
                        return finish(result, attempts, last_status);
                    }
                    (500..600).contains(&status) || status == 408 || status == 429
                }
                Ok(Err(e)) => {
                    logging::log(
                        Level::Debug,
                        "webhook attempt failed",
                        &[
                            ("rule", Value::from(rule.name.as_str())),
                            ("url", Value::from(target.url.shown())),
                            ("attempt", Value::from(attempts)),
                            ("error", Value::from(e.to_string())),
                        ],
                    );
                    true
                }
                // 超时
                Err(_) => true,
            };
            if !retryable || attempts >= MAX_ATTEMPTS {
                return finish(DeliveryResult::Abandoned, attempts, last_status);
            }
            let resume = Instant::now() + Duration::from_secs(1 << (attempts - 1));
            if resume >= deadline {
                return finish(DeliveryResult::Abandoned, attempts, last_status);
            }
            tokio::select! {
                _ = tokio::time::sleep_until(resume) => {}
                _ = stop.stopped() => return finish(DeliveryResult::Abandoned, attempts, last_status),
            }
        }
    }

    /// 一次尝试：建立连接（`https` 时完成握手）、发请求、读到最终响应的状态行与响应头，返回状态码。
    /// DNS 解析失败、连接被拒、握手失败（含证书不被信任、与主机不符）、收到完整响应之前连接被关闭都是错误。
    /// `limit` 为本次尝试的时限，用来给各个地址分配连接的时间。
    async fn attempt(
        &self,
        url: &WebhookUrl,
        head: &[u8],
        body: &[u8],
        limit: Instant,
    ) -> io::Result<u16> {
        let port = url
            .port
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "port out of range"))?;
        // 证书按主机校验：主机不是 rustls 认可的 DNS 名（如 `127.1`）时不必先建 TCP 连接，直接按连接失败处置
        let name = if url.https {
            Some(match url.host.parse::<IpAddr>() {
                Ok(ip) => ServerName::IpAddress(ip.into()),
                Err(_) => ServerName::try_from(url.host.clone())
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?,
            })
        } else {
            None
        };
        let tcp = connect(&url.host, port, limit).await?;
        match name {
            Some(name) => exchange(self.tls.connect(name, tcp).await?, head, body).await,
            None => exchange(tcp, head, body).await,
        }
    }
}

impl Delivery for Webhook {
    fn deliver(&self, notice: Notice, stop: Stop) -> BoxFuture<'_, DeliveryOutcome> {
        Box::pin(self.run(notice, stop))
    }
}

/// 依次连接主机解析出的各个地址，取第一个连上的。到 `limit` 为止的剩余时间在未试的地址间平分，一个不应答的地址
/// 不会占满整次尝试。
///
/// 解析经 tokio 的阻塞线程调用系统解析器：尝试被中止时 future 随之丢弃，阻塞线程要到系统解析器自己返回才结束
/// （不持有连接，只占一个阻塞线程）。
async fn connect(host: &str, port: u16, limit: Instant) -> io::Result<TcpStream> {
    let addrs: Vec<_> = tokio::net::lookup_host((host, port)).await?.collect();
    let mut last = io::Error::new(io::ErrorKind::NotFound, "host resolved to no address");
    for (i, addr) in addrs.iter().enumerate() {
        let left = (addrs.len() - i) as u32;
        let slice = limit.saturating_duration_since(Instant::now()) / left;
        match tokio::time::timeout(slice, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => return Ok(s),
            Ok(Err(e)) => last = e,
            Err(_) => last = io::Error::new(io::ErrorKind::TimedOut, "connect timed out"),
        }
    }
    Err(last)
}

/// 写出请求，读到第一个不是 1xx 的响应的状态行与响应头为止。
async fn exchange<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    head: &[u8],
    body: &[u8],
) -> io::Result<u16> {
    let written = async {
        stream.write_all(head).await?;
        stream.write_all(body).await?;
        stream.flush().await
    }
    .await;
    // 写出出错时（接收端不读完请求体就先作答并关闭连接），已经到达的响应照样算数
    match read_status(&mut stream).await {
        Ok(status) => Ok(status),
        Err(e) => Err(written.err().unwrap_or(e)),
    }
}

/// 读到第一个不是 1xx 的响应的状态行与响应头为止，返回其状态码。
async fn read_status<S: AsyncRead + Unpin>(stream: &mut S) -> io::Result<u16> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    // 已找过结尾空行的前缀长度：每读一次只从新到的字节附近找起
    let mut scanned = 0;
    loop {
        while let Some(end) = find_head_end(&buf, scanned) {
            let status = parse_status(&buf[..end])?;
            if (100..200).contains(&status) {
                // 1xx 不是最终响应，丢掉它接着读（REQ-AMB-ALR-WEBHOOK-NOT-RETRYABLE 的括注）
                buf.drain(..end);
                scanned = 0;
                continue;
            }
            return Ok(status);
        }
        scanned = buf.len().saturating_sub(3);
        if buf.len() > MAX_RESPONSE_HEAD {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "response head too large",
            ));
        }
        let n = stream.read(&mut chunk).await?;
        if n == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "connection closed before a complete response head",
            ));
        }
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// 状态行与响应头之后的位置（含结尾的空行），自 `from` 起找。只认 CRLF 的行尾（RFC 9112 的写法）。
fn find_head_end(buf: &[u8], from: usize) -> Option<usize> {
    buf.get(from..)?
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .map(|p| from + p + 4)
}

/// 状态行 `HTTP/1.x SSS ...` 的状态码（100 至 999）；不像状态行、或状态码小于 100（RFC 9110 §15 的状态码是三位、
/// 首位 1～5）时按收到完整响应之前出错处置。6xx 以上照收到的状态码处置（不重试，REQ-AMB-ALR-WEBHOOK-NOT-RETRYABLE）。
fn parse_status(head: &[u8]) -> io::Result<u16> {
    let bad = || io::Error::new(io::ErrorKind::InvalidData, "malformed status line");
    let line = head.split(|&b| b == b'\r').next().ok_or_else(bad)?;
    let rest = line.strip_prefix(b"HTTP/1.").ok_or_else(bad)?;
    match rest {
        [v, b' ', a, b, c, tail @ ..]
            if v.is_ascii_digit()
                && (b'1'..=b'9').contains(a)
                && [b, c].iter().all(|d| d.is_ascii_digit())
                && (tail.is_empty() || tail[0] == b' ') =>
        {
            Ok(u16::from(a - b'0') * 100 + u16::from(b - b'0') * 10 + u16::from(c - b'0'))
        }
        _ => Err(bad()),
    }
}

/// 请求行与请求头（REQ-AMB-ALR-WEBHOOK-METHOD、-REQUEST-HEADERS）：`Host`、`Content-Type`、`User-Agent`、
/// `webhook_headers` 各项（名与值原样），`Content-Length` 为请求体的字节数。
fn request_head(target: &Target, body_len: usize) -> Vec<u8> {
    let url = &target.url;
    let mut head = format!(
        "POST {} HTTP/1.1\r\nHost: {}\r\nContent-Type: application/json\r\nUser-Agent: amberlog/{SEMVER}\r\n",
        url.target, url.authority
    );
    for (name, value) in &target.headers {
        // 名在 parse_headers 已按 token 校验；Target 的字段是公开的，这里再守一次，不让 CR、LF 拆出另一个请求头
        assert!(
            !name.is_empty() && name.bytes().all(is_tchar),
            "header name violates REQ-AMB-ALR-WEBHOOK-HEADERS"
        );
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value.expose());
        head.push_str("\r\n");
    }
    head.push_str(&format!("Content-Length: {body_len}\r\n\r\n"));
    head.into_bytes()
}

/// 请求体（REQ-AMB-ALR-WEBHOOK-BODY 至 -TRUNCATED）：版本 1 的十个字段，按块列出的次序写出；`rows` 每行的写法
/// 同 `_search` 的 hit（键按输出项的先后，NULL 列不写键，数字按 `hits` 的写法）。
fn request_body(notice: &Notice) -> Vec<u8> {
    #[derive(serde::Serialize)]
    struct Body<'a> {
        version: u32,
        rule: &'a str,
        evaluation_id: String,
        scheduled_at: String,
        window_start: String,
        window_end: String,
        matched_rows: u64,
        truncated: bool,
        rows: &'a [Hit],
        sql: &'a str,
    }
    let rule = &notice.rule;
    let t = notice.scheduled_at;
    amberlog_common::json::to_vec(&Body {
        version: 1,
        rule: &rule.name,
        evaluation_id: format!("{}-{t}", rule.name),
        scheduled_at: logging::rfc3339_utc(t),
        window_start: logging::rfc3339_utc(t.saturating_sub(rule.window_us())),
        window_end: logging::rfc3339_utc(t),
        matched_rows: notice.matched_rows,
        truncated: notice.truncated,
        rows: &notice.rows,
        sql: rule.sql_text(),
    })
}

#[cfg(test)]
mod tests;
