//! 测试台架的身份平台（REQ-AMB-AUTH-OIDC）：测试进程内的最小 OIDC 身份平台替身，不依赖 Keycloak 等容器。
//!
//! - 以 [`super::tls::Pki`] 现场生成的测试 CA 签发的证书提供 HTTPS（[`super::webhook::Receiver`]），被测进程经
//!   `auth.oidc.ca_file` 信任该 CA。签发方为 `https://127.0.0.1:<端口>`。
//! - 地址：说明文档 `/.well-known/openid-configuration`、公钥集 `/jwks`、令牌端点 `/token`、授权地址 `/authorize`。
//!   记下各地址收到的请求（[`Idp::requests`]）。
//! - 签名密钥都在测试里现场生成（RSA 由 `openssl` 命令行生成、ECDSA 由 ring 生成），不写进源码；能用任意密钥、任意
//!   声明签发令牌（[`Idp::sign`]），能换密钥（[`Idp::rotate`]）、改说明文档（[`Idp::set_discovery`]）、停止与恢复
//!   （[`Idp::stop`]、[`Idp::resume`]：停止时接受连接后不读请求即关闭，被测进程看到的是取得失败）。
//! - 授权地址不经交互即通过：`GET /authorize` 记下 `redirect_uri`、`code_challenge`，以 302 跳回
//!   `redirect_uri?code=<一次性码>&state=<原值>`；令牌端点按该码核对客户端认证、`code_verifier`（S256）与
//!   `redirect_uri`，每次交换签发一枚新的访问令牌（主体为 alice）。令牌端点的应答可由测试改写（[`Idp::on_token`]）。
//! - 令牌的声明按当前时刻设定，不改系统时钟。

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

use ring::rand::SystemRandom;
use ring::signature::{self as sig, EcdsaKeyPair, KeyPair, RsaKeyPair};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::authz::base64url;
use super::tls::{KeyKind, Pki};
use super::webhook::{Receiver, Reply, Request, Step};
use super::{Fixture, Kv};

/// 场景约定的受众（REQ-AMB-AUTH-OIDC「场景约定」）。
pub const AUDIENCE: &str = "amberlog";
/// 界面登录的客户端编号与密钥（场景约定 C 组）。
pub const CLIENT_ID: &str = "amberlog-ui";
pub const CLIENT_SECRET: &str = "ui-secret";
/// 授权码流程签发的令牌的主体声明（用户 alice）。
pub const ALICE_SUB: &str = "alice";

/// 一把签名密钥。
pub enum Key {
    Rsa(RsaKeyPair),
    Es256(EcdsaKeyPair),
}

impl Key {
    /// 现场生成 2048 位 RSA 密钥（`openssl genpkey`，转成 ring 读的 PKCS#1 DER）。
    pub fn rsa(pki_dir: &std::path::Path) -> Key {
        let n = SEQ.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (pem, der) = (
            pki_dir.join(format!("jws-{n}.pem")),
            pki_dir.join(format!("jws-{n}.der")),
        );
        let (pem_s, der_s) = (pem.to_str().unwrap(), der.to_str().unwrap());
        let openssl = |args: &[&str]| {
            let status = std::process::Command::new("openssl")
                .args(args)
                .stderr(std::process::Stdio::null())
                .status();
            assert!(status.unwrap().success(), "openssl {args:?}");
        };
        openssl(&[
            "genpkey",
            "-algorithm",
            "RSA",
            "-pkeyopt",
            "rsa_keygen_bits:2048",
            "-out",
            pem_s,
        ]);
        openssl(&[
            "pkey",
            "-traditional",
            "-outform",
            "DER",
            "-in",
            pem_s,
            "-out",
            der_s,
        ]);
        let bytes = std::fs::read(&der).unwrap();
        let _ = std::fs::remove_file(&pem);
        let _ = std::fs::remove_file(&der);
        Key::Rsa(RsaKeyPair::from_der(&bytes).unwrap())
    }

    /// 现场生成 P-256 密钥。
    pub fn es256() -> Key {
        let rng = SystemRandom::new();
        let a = &sig::ECDSA_P256_SHA256_FIXED_SIGNING;
        let pkcs8 = EcdsaKeyPair::generate_pkcs8(a, &rng).unwrap();
        Key::Es256(EcdsaKeyPair::from_pkcs8(a, pkcs8.as_ref(), &rng).unwrap())
    }

    pub fn alg(&self) -> &'static str {
        match self {
            Key::Rsa(_) => "RS256",
            Key::Es256(_) => "ES256",
        }
    }

    /// 公钥的 JWK。
    pub fn jwk(&self, kid: &str) -> Value {
        match self {
            Key::Rsa(k) => {
                let c: ring::rsa::PublicKeyComponents<Vec<u8>> = k.public().into();
                json!({"kty": "RSA", "kid": kid, "use": "sig", "n": base64url(&c.n), "e": base64url(&c.e)})
            }
            Key::Es256(k) => {
                let p = k.public_key().as_ref();
                json!({"kty": "EC", "crv": "P-256", "kid": kid, "x": base64url(&p[1..33]), "y": base64url(&p[33..])})
            }
        }
    }

    /// 以本密钥签 `header.payload`（头部原样，不补 `alg`）。
    pub fn sign_raw(&self, header: &Value, payload: &Value) -> String {
        let input = format!(
            "{}.{}",
            base64url(header.to_string().as_bytes()),
            base64url(payload.to_string().as_bytes())
        );
        let rng = SystemRandom::new();
        let s = match self {
            Key::Rsa(k) => {
                let mut out = vec![0; k.public().modulus_len()];
                k.sign(&sig::RSA_PKCS1_SHA256, &rng, input.as_bytes(), &mut out)
                    .unwrap();
                out
            }
            Key::Es256(k) => k.sign(&rng, input.as_bytes()).unwrap().as_ref().to_vec(),
        };
        format!("{input}.{}", base64url(&s))
    }
}

static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// 当前 Unix 秒。
pub fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// 授权地址记下的一次登录：码 → （`redirect_uri`、`code_challenge`）。
struct Grant {
    redirect_uri: String,
    challenge: String,
}

type TokenHook = Arc<dyn Fn(&Request) -> Reply + Send + Sync>;

struct State {
    issuer: String,
    stopped: bool,
    /// 签发用的当前密钥与 `kid`。
    current: (Arc<Key>, String),
    /// 公钥集中公布的密钥（JWK）。
    published: Vec<Value>,
    /// 改写过的说明文档；`None` 时按缺省。
    discovery: Option<Value>,
    codes: HashMap<String, Grant>,
    token_hook: Option<TokenHook>,
    issued: u64,
}

/// 测试台架的身份平台；随本值析构而停止。
pub struct Idp {
    rx: Receiver,
    ca: PathBuf,
    pki_dir: PathBuf,
    state: Arc<Mutex<State>>,
}

impl Idp {
    /// 在夹具目录下生成测试 CA 与服务端证书，起身份平台，缺省以一把 RSA 密钥（`kid` 为 `k1`）签名并公布。
    pub fn start(fx: &Fixture) -> Idp {
        let pki = Pki::new(fx.root());
        let ca = pki.ca("amberlog test idp ca");
        let leaf = pki.leaf(&ca, "127.0.0.1", KeyKind::P256);
        let pki_dir = fx.root().join("pki");
        let key = Arc::new(Key::rsa(&pki_dir));
        let rx = Receiver::https(&leaf);
        let issuer = format!("https://{}", rx.addr());
        let state = Arc::new(Mutex::new(State {
            issuer,
            stopped: false,
            published: vec![key.jwk("k1")],
            current: (key, "k1".to_owned()),
            discovery: None,
            codes: HashMap::new(),
            token_hook: None,
            issued: 0,
        }));
        let s = Arc::clone(&state);
        rx.plan(move |_| {
            if s.lock().unwrap().stopped {
                return Step::Refuse;
            }
            let s = Arc::clone(&s);
            Step::by(move |req| handle(&s, req))
        });
        Idp {
            rx,
            ca: ca.cert,
            pki_dir,
            state,
        }
    }

    /// 签发方（`auth.oidc.issuer` 的取值）。
    pub fn issuer(&self) -> String {
        self.state.lock().unwrap().issuer.clone()
    }

    /// 测试 CA 证书文件（`auth.oidc.ca_file` 的取值）。
    pub fn ca_file(&self) -> PathBuf {
        self.ca.clone()
    }

    /// 本身份平台上的地址。
    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.issuer())
    }

    /// 缺省的说明文档。
    pub fn default_discovery(&self) -> Value {
        discovery(&self.issuer())
    }

    /// 改写说明文档（`None` 恢复缺省）。
    pub fn set_discovery(&self, doc: Option<Value>) {
        self.state.lock().unwrap().discovery = doc;
    }

    /// 公钥集地址此刻的响应体。
    pub fn jwks_body(&self) -> String {
        jwks_body(&self.state.lock().unwrap())
    }

    /// 停止服务：此后接受连接即关闭，不作答。
    pub fn stop(&self) {
        self.state.lock().unwrap().stopped = true;
    }

    /// 恢复服务。
    pub fn resume(&self) {
        self.state.lock().unwrap().stopped = false;
    }

    /// 换用一把 `kid` 为 `kid` 的新密钥签名，公钥集同时公布新旧两把。
    pub fn rotate(&self, kid: &str) {
        let key = Arc::new(Key::rsa(&self.pki_dir));
        let mut s = self.state.lock().unwrap();
        s.published.push(key.jwk(kid));
        s.current = (key, kid.to_owned());
    }

    /// 场景约定的声明：`iss` 为签发方、`aud` 为 `amberlog`、组 [`sec`]、`exp` 为签发后 10 分钟，`sub` 为 `sub`。
    pub fn claims(&self, sub: &str) -> Value {
        let t = now();
        json!({
            "iss": self.issuer(),
            "aud": AUDIENCE,
            "sub": sub,
            "groups": ["sec"],
            "iat": t,
            "exp": t + 600,
        })
    }

    /// 以当前密钥签发载荷为 `payload` 的令牌。
    pub fn sign(&self, payload: &Value) -> String {
        let (key, kid) = self.state.lock().unwrap().current.clone();
        key.sign_raw(
            &json!({"alg": key.alg(), "typ": "at+jwt", "kid": kid}),
            payload,
        )
    }

    /// 为用户 `sub` 签发场景约定的令牌。
    pub fn token(&self, sub: &str) -> String {
        self.sign(&self.claims(sub))
    }

    /// 改写令牌端点的应答（`None` 恢复授权码流程）。
    pub fn on_token(&self, hook: Option<TokenHook>) {
        self.state.lock().unwrap().token_hook = hook;
    }

    /// 收到的全部请求。
    pub fn requests(&self) -> Vec<Request> {
        self.rx.requests()
    }

    /// 请求目标的路径部分（去掉查询串）为 `path` 的请求。
    pub fn requests_to(&self, path: &str) -> Vec<Request> {
        self.requests()
            .into_iter()
            .filter(|r| r.target.split('?').next() == Some(path))
            .collect()
    }

    /// 被测进程配置身份平台用的配置项（`auth.oidc.issuer`、`audience`、`ca_file`，取值为 TOML 写法）。
    pub fn config(&self) -> Vec<(&'static str, String)> {
        vec![
            ("auth.oidc.issuer", format!("\"{}\"", self.issuer())),
            ("auth.oidc.audience", format!("\"{AUDIENCE}\"")),
            ("auth.oidc.ca_file", format!("\"{}\"", self.ca.display())),
        ]
    }

    /// 另加界面登录的三项（场景约定 C 组）。
    pub fn login_config(&self) -> Vec<(&'static str, String)> {
        let mut c = self.config();
        c.push(("auth.oidc.client_id", format!("\"{CLIENT_ID}\"")));
        c.push(("auth.oidc.client_secret", format!("\"{CLIENT_SECRET}\"")));
        c.push(("auth.oidc.scope", "\"openid groups\"".to_owned()));
        c
    }
}

/// 把 [`Idp::config`] 一类的配置项与 `extra` 拼成 [`Fixture`] 的覆盖项。
pub fn overrides<'a>(config: &'a [(&'static str, String)], extra: &[Kv<'a>]) -> Vec<Kv<'a>> {
    let mut all: Vec<Kv> = config.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect();
    all.extend_from_slice(extra);
    all
}

/// 授权文件的一个 `idp_grants` 元素（TOML 文本）：`matcher` 为 `group = "sec"` 一类的一行，`grants` 同
/// [`super::authz::entry`]。
pub fn idp_grant(matcher: &str, grants: &str) -> String {
    format!("[[idp_grants]]\n{matcher}\ngrants = {grants}\n")
}

fn jwks_body(s: &State) -> String {
    json!({ "keys": s.published }).to_string()
}

fn discovery(issuer: &str) -> Value {
    json!({
        "issuer": issuer,
        "jwks_uri": format!("{issuer}/jwks"),
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "response_types_supported": ["code"],
        "subject_types_supported": ["public"],
        "id_token_signing_alg_values_supported": ["RS256"],
    })
}

fn json_reply(status: u16, v: &Value) -> Reply {
    Reply::status(status).body(v.to_string().as_bytes())
}

fn handle(state: &Mutex<State>, req: &Request) -> Reply {
    let (path, query) = req.target.split_once('?').unwrap_or((&req.target, ""));
    match (req.method.as_str(), path) {
        ("GET", "/.well-known/openid-configuration") => {
            let s = state.lock().unwrap();
            let doc = s.discovery.clone().unwrap_or_else(|| discovery(&s.issuer));
            json_reply(200, &doc)
        }
        ("GET", "/jwks") => {
            let body = jwks_body(&state.lock().unwrap());
            Reply::status(200).body(body.as_bytes())
        }
        ("GET", "/authorize") => authorize(state, &form(query)),
        ("POST", "/token") => {
            let hook = state.lock().unwrap().token_hook.clone();
            match hook {
                Some(h) => h(req),
                None => exchange(state, req),
            }
        }
        _ => Reply::status(404),
    }
}

/// 不经交互即通过：记下码与 `code_challenge`，302 跳回 `redirect_uri`。
fn authorize(state: &Mutex<State>, q: &HashMap<String, String>) -> Reply {
    let (Some(redirect), Some(challenge)) = (q.get("redirect_uri"), q.get("code_challenge")) else {
        return Reply::status(400);
    };
    let mut s = state.lock().unwrap();
    s.issued += 1;
    let code = format!("code-{}-{}", s.issued, now());
    s.codes.insert(
        code.clone(),
        Grant {
            redirect_uri: redirect.clone(),
            challenge: challenge.clone(),
        },
    );
    let sep = if redirect.contains('?') { '&' } else { '?' };
    let state_param = q
        .get("state")
        .map(|v| percent_encode(v))
        .unwrap_or_default();
    Reply::status(302).location(&format!("{redirect}{sep}code={code}&state={state_param}"))
}

/// 授权码交换：客户端认证、码、`code_verifier`（S256）与 `redirect_uri` 都对上才签发。
fn exchange(state: &Mutex<State>, req: &Request) -> Reply {
    let f = form(&String::from_utf8_lossy(&req.body));
    let basic = format!(
        "Basic {}",
        base64_std(format!("{CLIENT_ID}:{CLIENT_SECRET}").as_bytes())
    );
    let invalid = |e: &str| json_reply(400, &json!({ "error": e }));
    if req.header("Authorization") != Some(basic.as_str()) {
        return json_reply(401, &json!({"error": "invalid_client"}));
    }
    if f.get("grant_type").map(String::as_str) != Some("authorization_code") {
        return invalid("unsupported_grant_type");
    }
    let mut s = state.lock().unwrap();
    let Some(grant) = f.get("code").and_then(|c| s.codes.remove(c)) else {
        return invalid("invalid_grant");
    };
    let verifier = f.get("code_verifier").cloned().unwrap_or_default();
    let challenge = base64url(&Sha256::digest(verifier.as_bytes()));
    if challenge != grant.challenge || f.get("redirect_uri") != Some(&grant.redirect_uri) {
        return invalid("invalid_grant");
    }
    s.issued += 1;
    let t = now();
    let payload = json!({
        "iss": s.issuer,
        "aud": AUDIENCE,
        "sub": ALICE_SUB,
        "preferred_username": "alice",
        "groups": ["sec"],
        "iat": t,
        "exp": t + 600,
        "jti": format!("at-{}", s.issued),
    });
    let (key, kid) = s.current.clone();
    let token = key.sign_raw(
        &json!({"alg": key.alg(), "typ": "at+jwt", "kid": kid}),
        &payload,
    );
    json_reply(
        200,
        &json!({
            "access_token": token,
            "token_type": "Bearer",
            "expires_in": 300,
            "refresh_token": format!("refresh-{}", s.issued),
            "id_token": format!("id-{}", s.issued),
        }),
    )
}

/// `application/x-www-form-urlencoded`（或查询串）解码。
pub fn form(text: &str) -> HashMap<String, String> {
    text.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < b.len() => {
                let hex = std::str::from_utf8(&b[i + 1..i + 3]).ok();
                match hex.and_then(|h| u8::from_str_radix(h, 16).ok()) {
                    Some(v) => {
                        out.push(v);
                        i += 2;
                    }
                    None => out.push(b'%'),
                }
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn percent_encode(s: &str) -> String {
    s.bytes()
        .map(|c| match c {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                (c as char).to_string()
            }
            _ => format!("%{c:02X}"),
        })
        .collect()
}

/// 带填充的标准 base64（RFC 4648 §4；HTTP Basic 认证）。
pub fn base64_std(bytes: &[u8]) -> String {
    let mut s = base64url(bytes).replace('-', "+").replace('_', "/");
    while !s.len().is_multiple_of(4) {
        s.push('=');
    }
    s
}
