//! 身份平台（REQ-AMB-AUTH-OIDC；ADR-0056 第 2 条）：Amberlog 只做 OIDC 的资源服务器，验证身份平台签发的 JWT 访问令牌。
//!
//! - 说明文档与公钥集（A 组）：[`Idp::spawn`] 起一个后台任务，随 `amberlog serve` 与 `amberlog catalog` 的启动开始，
//!   不在启动流程里等待（DISCOVERY-NOT-BLOCKING）；取不到时每 30 秒重试，直到取得（DISCOVERY-RETRY）。取得之前身份平台
//!   令牌一律验证不通过。
//! - 公钥集缓存（JWKS-CACHE）；令牌头部的 `kid` 不在缓存中时重取一次（JWKS-REFETCH），两次因 `kid` 而起的重取至少相隔
//!   60 秒（JWKS-REFETCH-LIMIT）。
//! - 每次取说明文档或公钥集失败使 `amberlog_oidc_fetch_failures_total` 加 1；连续失败只在第一次输出 `oidc_unavailable`
//!   （EVT-UNAVAILABLE）。
//! - 出站 HTTPS：reqwest（object_store 已在用的同一版本）配 rustls 的 ring 后端，信任系统根证书，配置了
//!   `auth.oidc.ca_file` 时另信任其中的 CA（每次访问前读该文件，读不到按访问失败处理）；只认 `https://`、不跟随重定向、
//!   不经代理，每个请求 10 秒内没有完整响应即失败（HTTPS-ONLY、CA-FILE、FETCH-TIMEOUT）。
//! - 令牌的验证（B 组）在 [`jwt`]；验证结果交给授权模块映射成角色（REQ-AMB-AUTH-AUTHZ-IDP-MAPPING）。
//! - 界面登录的服务端（C 组）：登录配置（[`Idp::ui_config`]）与授权码交换（[`Idp::exchange`]）在这里，三个登录
//!   路径在 [`crate::ui::oidc`]。

pub mod jwt;

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

use rustls::pki_types::CertificateDer;
use rustls::pki_types::pem::PemObject;
use serde_json::Value;

use crate::config;
use crate::logging::{self, Level};

pub use jwt::Claims;

/// 取不到说明文档或公钥集时的重试间隔（REQ-AMB-AUTH-OIDC-DISCOVERY-RETRY）。
const RETRY_INTERVAL: Duration = Duration::from_secs(30);
/// 两次因 `kid` 而起的重取之间的最短间隔（REQ-AMB-AUTH-OIDC-JWKS-REFETCH-LIMIT）。
const REFETCH_INTERVAL: Duration = Duration::from_secs(60);
/// 每个请求的时限（REQ-AMB-AUTH-OIDC-FETCH-TIMEOUT）。
const FETCH_TIMEOUT: Duration = Duration::from_secs(10);
/// 说明文档、公钥集与令牌端点响应体的字节上限：超出按失败处理。
const MAX_RESPONSE_BYTES: usize = 1 << 20;

/// 采用了的说明文档里本系统用到的地址。
#[derive(Debug, Clone)]
struct Discovered {
    jwks_uri: String,
    /// 界面登录（REQ-AMB-AUTH-OIDC C 组）用到的两个地址；说明文档没给时为 `None`。
    authorization_endpoint: Option<String>,
    token_endpoint: Option<String>,
}

/// 一个身份平台：配置、取到的说明文档与公钥集、取得失败的计数与状态。全进程一份，由 [`crate::authz::Authz`] 持有。
pub struct Idp {
    cfg: config::Oidc,
    discovery_url: String,
    discovered: RwLock<Option<Arc<Discovered>>>,
    /// 缓存的公钥集；尚未取得时为 `None`。
    keys: RwLock<Option<Arc<[jwt::Jwk]>>>,
    /// 上一次因 `kid` 而起的重取开始的时刻；锁同时使并发的重取只有一个在进行。
    refetch: tokio::sync::Mutex<Option<Instant>>,
    failures: AtomicU64,
    /// 上一次取说明文档或公钥集是否失败（连续失败只在第一次输出 `oidc_unavailable`）。
    failing: AtomicBool,
    /// 最近一次取说明文档失败的原因（登录路径 503 的日志用）。
    last_error: Mutex<String>,
}

impl std::fmt::Debug for Idp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Idp")
            .field("issuer", &self.cfg.issuer)
            .finish()
    }
}

impl Idp {
    pub fn new(cfg: config::Oidc) -> Idp {
        // `issuer` 末尾的 `/` 先去掉再接说明文档的路径（REQ-AMB-AUTH-OIDC-DISCOVERY）
        let discovery_url = format!(
            "{}/.well-known/openid-configuration",
            cfg.issuer.trim_end_matches('/')
        );
        Idp {
            cfg,
            discovery_url,
            discovered: RwLock::new(None),
            keys: RwLock::new(None),
            refetch: tokio::sync::Mutex::new(None),
            failures: AtomicU64::new(0),
            failing: AtomicBool::new(false),
            last_error: Mutex::new("the discovery document has not been fetched yet".to_owned()),
        }
    }

    /// 取得失败的累计次数（`amberlog_oidc_fetch_failures_total`，REQ-AMB-AUTH-OIDC-FETCH-FAILURE-METRIC）。
    pub fn fetch_failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    /// 起后台任务：先取一次，取不到时每 30 秒重试，直到说明文档与公钥集都取得。
    pub fn spawn(self: &Arc<Self>) {
        let idp = Arc::clone(self);
        tokio::spawn(async move {
            while !idp.attempt().await {
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
        });
    }

    /// 一次取得：先取说明文档，取得并采用后接着取公钥集。两者都取得为 true。
    async fn attempt(&self) -> bool {
        let doc = match self.get_json(&self.discovery_url).await {
            Ok(doc) => doc,
            Err(why) => {
                self.failed(&self.discovery_url, &why);
                return false;
            }
        };
        let discovered = match self.adopt(&doc) {
            Ok(d) => Arc::new(d),
            Err(why) => {
                self.failed(&self.discovery_url, &why);
                return false;
            }
        };
        *self.discovered.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::clone(&discovered));
        self.fetch_keys(&discovered.jwks_uri).await
    }

    /// 说明文档的 `issuer` 与配置值逐字节相等、`jwks_uri` 存在，且其中的 `jwks_uri` 与 `token_endpoint` 都以 `https://`
    /// 开头时才采用（REQ-AMB-AUTH-OIDC-DISCOVERY、-HTTPS-ONLY）。
    fn adopt(&self, doc: &Value) -> Result<Discovered, String> {
        let text = |k: &str| doc.get(k).and_then(Value::as_str).map(str::to_owned);
        if doc.get("issuer").and_then(Value::as_str) != Some(self.cfg.issuer.as_str()) {
            return Err(
                "the discovery document's issuer does not equal auth.oidc.issuer".to_owned(),
            );
        }
        let Some(jwks_uri) = text("jwks_uri") else {
            return Err("the discovery document has no jwks_uri".to_owned());
        };
        let token_endpoint = text("token_endpoint");
        if !jwks_uri.starts_with("https://")
            || token_endpoint
                .as_ref()
                .is_some_and(|t| !t.starts_with("https://"))
        {
            return Err("the discovery document lists an endpoint that is not https://".to_owned());
        }
        Ok(Discovered {
            jwks_uri,
            authorization_endpoint: text("authorization_endpoint"),
            token_endpoint,
        })
    }

    /// 取公钥集并换入缓存。取得为 true。
    async fn fetch_keys(&self, jwks_uri: &str) -> bool {
        let keys = self.get_json(jwks_uri).await.and_then(|v| {
            jwt::parse_jwks(&v)
                .ok_or_else(|| "the key set is not a JSON object with a keys array".to_owned())
        });
        match keys {
            Ok(keys) => {
                *self.keys.write().unwrap_or_else(|p| p.into_inner()) = Some(Arc::from(keys));
                self.failing.store(false, Ordering::Relaxed);
                true
            }
            Err(why) => {
                self.failed(jwks_uri, &why);
                false
            }
        }
    }

    /// 记一次取得失败：计数加 1；上一次取得没有失败（或进程启动后第一次失败）时输出 `oidc_unavailable`。
    fn failed(&self, url: &str, why: &str) {
        self.failures.fetch_add(1, Ordering::Relaxed);
        if url == self.discovery_url {
            *self.last_error.lock().unwrap_or_else(|p| p.into_inner()) = why.to_owned();
        }
        if !self.failing.swap(true, Ordering::Relaxed) {
            logging::event(
                Level::Warn,
                "oidc_unavailable",
                &format!(
                    "cannot fetch the identity provider's discovery document or key set: {why}"
                ),
                &[
                    ("issuer", Value::from(self.cfg.issuer.as_str())),
                    ("url", Value::from(url)),
                ],
            );
        }
    }

    /// 以 `GET` 取一个 JSON 文档：状态码须为 200，响应体须是 JSON。
    async fn get_json(&self, url: &str) -> Result<Value, String> {
        let client = self.client()?;
        let resp = client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|e| format!("request to {url} failed: {}", error_chain(&e)))?;
        let status = resp.status().as_u16();
        let body = read_body(resp).await?;
        if status != 200 {
            return Err(format!("{url} answered HTTP {status}"));
        }
        serde_json::from_slice(&body).map_err(|e| format!("{url} did not return JSON: {e}"))
    }

    /// 出站 HTTPS 客户端：每次访问前现建（读一次 `auth.oidc.ca_file`），访问身份平台的次数少，不必复用。
    fn client(&self) -> Result<reqwest::Client, String> {
        let mut roots = rustls::RootCertStore::empty();
        // 系统根证书读不全时取能读到的部分（与告警 webhook、对象存储客户端的做法相同）
        roots.add_parsable_certificates(rustls_native_certs::load_native_certs().certs);
        if let Some(ca) = &self.cfg.ca_file {
            let pem = std::fs::read(&ca.path)
                .map_err(|e| format!("cannot read auth.oidc.ca_file {}: {e}", ca.path.display()))?;
            let certs: Vec<_> = CertificateDer::pem_slice_iter(&pem)
                .filter_map(Result::ok)
                .collect();
            if certs.is_empty() {
                return Err(format!(
                    "auth.oidc.ca_file {} contains no PEM certificate",
                    ca.path.display()
                ));
            }
            roots.add_parsable_certificates(certs);
        }
        let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .expect("ring 支持缺省的 TLS 版本")
        .with_root_certificates(roots)
        .with_no_client_auth();
        reqwest::Client::builder()
            .use_preconfigured_tls(tls)
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .timeout(FETCH_TIMEOUT)
            .build()
            .map_err(|e| format!("cannot build the HTTPS client: {e}"))
    }

    /// 验证身份平台令牌（REQ-AMB-AUTH-OIDC B 组）；验证不通过或无法验证（说明文档或公钥集尚未取得）为 `None`。
    pub async fn verify(&self, token: &[u8]) -> Option<Claims> {
        let parsed = jwt::parse(token)?;
        let mut keys = self.keys()?;
        let key = match parsed.choose(&keys) {
            jwt::Choice::Key(k) => k.clone(),
            jwt::Choice::None => return None,
            jwt::Choice::UnknownKid => {
                self.refetch_for_kid().await;
                keys = self.keys()?;
                match parsed.choose(&keys) {
                    jwt::Choice::Key(k) => k.clone(),
                    _ => return None,
                }
            }
        };
        if !parsed.signature_valid(&key) {
            return None;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0.0, |d| d.as_secs_f64());
        parsed.claims(&jwt::Expect {
            issuer: &self.cfg.issuer,
            audience: &self.cfg.audience,
            subject_claim: &self.cfg.subject_claim,
            groups_claim: &self.cfg.groups_claim,
            now,
        })
    }

    fn keys(&self) -> Option<Arc<[jwt::Jwk]>> {
        self.keys.read().unwrap_or_else(|p| p.into_inner()).clone()
    }

    /// 因 `kid` 不在缓存中而重取公钥集；距上一次这样的重取不足 60 秒时不重取。
    async fn refetch_for_kid(&self) {
        let mut last = self.refetch.lock().await;
        if last.is_some_and(|t| t.elapsed() < REFETCH_INTERVAL) {
            return;
        }
        let Some(discovered) = self
            .discovered
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
        else {
            return;
        };
        *last = Some(Instant::now());
        self.fetch_keys(&discovered.jwks_uri).await;
    }
}

/// 授权码交换成功时转交给界面的两项（REQ-AMB-AUTH-OIDC-UI-TOKEN-RESPONSE）。
#[derive(Debug)]
pub struct Exchanged {
    pub access_token: String,
    /// 身份平台给出的整数；没有（或不是整数）时为 `None`。
    pub expires_in: Option<i64>,
}

/// 授权码交换失败的两类。
#[derive(Debug)]
pub enum ExchangeFailed {
    /// 令牌端点以 4xx 作答（REQ-AMB-AUTH-OIDC-UI-TOKEN-IDP-REJECT）。
    Rejected(u16),
    /// 说明文档尚未取得、令牌端点不可达、超时、以 5xx 作答或返回的内容不含字符串 `access_token`
    /// （REQ-AMB-AUTH-OIDC-UI-TOKEN-IDP-DOWN）。原因只进日志。
    Unavailable(String),
}

impl Idp {
    /// 界面登录的配置；没配 `auth.oidc.client_id` 与 `client_secret` 时为 `None`。
    pub fn login(&self) -> Option<&config::OidcLogin> {
        self.cfg.login.as_ref()
    }

    fn discovered(&self) -> Option<Arc<Discovered>> {
        self.discovered
            .read()
            .unwrap_or_else(|p| p.into_inner())
            .clone()
    }

    /// `GET /ui/oidc/config` 的 `authorization_endpoint`（REQ-AMB-AUTH-OIDC-UI-CONFIG）。说明文档尚未取得（或没给
    /// 这一地址）时为出错原因（REQ-AMB-AUTH-OIDC-UI-CONFIG-UNAVAILABLE 的日志用）。
    pub fn authorization_endpoint(&self) -> Result<String, String> {
        match self.discovered() {
            Some(d) => d
                .authorization_endpoint
                .clone()
                .ok_or_else(|| "the discovery document has no authorization_endpoint".to_owned()),
            None => Err(self
                .last_error
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .clone()),
        }
    }

    /// 以请求体中的三个字段向说明文档的 `token_endpoint` 发出一次授权码交换（RFC 6749 §4.1.3；客户端认证按
    /// §2.3.1 以 HTTP Basic 携带 `auth.oidc.client_id` 与 `client_secret`）。不向令牌端点以外的地址发出请求，`redirect_uri`
    /// 只作为参数转交（REQ-AMB-AUTH-OIDC-UI-TOKEN-EXCHANGE、-UI-TOKEN-ONLY-ENDPOINT）。
    pub async fn exchange(
        &self,
        code: &str,
        code_verifier: &str,
        redirect_uri: &str,
    ) -> Result<Exchanged, ExchangeFailed> {
        let unavailable = ExchangeFailed::Unavailable;
        let Some(login) = self.login() else {
            return Err(unavailable("interface login is not configured".to_owned()));
        };
        let Some(endpoint) = self.discovered().and_then(|d| d.token_endpoint.clone()) else {
            return Err(unavailable(format!(
                "the discovery document has not been fetched or has no token_endpoint: {}",
                self.last_error.lock().unwrap_or_else(|p| p.into_inner())
            )));
        };
        let body = [
            ("grant_type", "authorization_code"),
            ("code", code),
            ("code_verifier", code_verifier),
            ("redirect_uri", redirect_uri),
        ]
        .iter()
        .map(|(k, v)| format!("{k}={}", form_encode(v)))
        .collect::<Vec<_>>()
        .join("&");
        let client = self.client().map_err(unavailable)?;
        let resp = client
            .post(&endpoint)
            .basic_auth(
                form_encode(&login.client_id),
                Some(form_encode(&login.client_secret)),
            )
            .header(
                reqwest::header::CONTENT_TYPE,
                "application/x-www-form-urlencoded",
            )
            .header(reqwest::header::ACCEPT, "application/json")
            .body(body)
            .send()
            .await
            .map_err(|e| {
                unavailable(format!("request to {endpoint} failed: {}", error_chain(&e)))
            })?;
        let status = resp.status().as_u16();
        if (400..500).contains(&status) {
            return Err(ExchangeFailed::Rejected(status));
        }
        let bytes = read_body(resp).await.map_err(unavailable)?;
        if !(200..300).contains(&status) {
            return Err(unavailable(format!("{endpoint} answered HTTP {status}")));
        }
        let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        let Some(access_token) = v.get("access_token").and_then(Value::as_str) else {
            return Err(unavailable(format!(
                "{endpoint} answered without a string access_token"
            )));
        };
        Ok(Exchanged {
            access_token: access_token.to_owned(),
            expires_in: v.get("expires_in").and_then(Value::as_i64),
        })
    }
}

/// `application/x-www-form-urlencoded` 的编码（URL 标准）：字母、数字与 `*-._` 原样，空格为 `+`，其余按 UTF-8 字节写 `%XX`。
fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'*' | b'-' | b'.' | b'_' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// 读响应体，超出 [`MAX_RESPONSE_BYTES`] 按失败处理。
async fn read_body(mut resp: reqwest::Response) -> Result<Vec<u8>, String> {
    let url = resp.url().to_string();
    let mut body = Vec::new();
    while let Some(chunk) = resp.chunk().await.map_err(|e| {
        format!(
            "reading the response from {url} failed: {}",
            error_chain(&e)
        )
    })? {
        body.extend_from_slice(&chunk);
        if body.len() > MAX_RESPONSE_BYTES {
            return Err(format!("the response from {url} is too large"));
        }
    }
    Ok(body)
}

/// 错误连同其来源链：reqwest 的顶层错误只说「请求失败」，原因（连接被拒、证书不受信任、超时）在来源里。
fn error_chain(e: &dyn std::error::Error) -> String {
    let mut text = e.to_string();
    let mut source = e.source();
    while let Some(s) = source {
        text.push_str(": ");
        text.push_str(&s.to_string());
        source = s.source();
    }
    text
}
