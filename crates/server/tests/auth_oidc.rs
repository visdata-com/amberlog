//! REQ-AMB-AUTH-OIDC 的系统测试：以子进程启动已构建的 `amberlog`，经 HTTP 响应、stderr 的运行日志与台架身份平台的请求记录
//! 断言。
//!
//! 身份平台是测试进程内的替身（[`common::oidc`]），签名密钥与服务令牌都在测试里现场生成，断言里不写令牌原文。
//! 时间相关的场景在签发令牌时按当前时刻设定声明，不改系统时钟。

mod common;

use std::time::{Duration, Instant};

use common::api::ingest_ok;
use common::authz::{self, ServiceToken, events, get_as, search_as, token_entry};
use common::oidc::{self, Idp, Key, idp_grant, now};
use common::{Fixture, Resp, Server, TOKEN};
use serde_json::{Value, json};

/// 场景约定的「合法查询」。
const SQL: &str = "SELECT count(*) AS n FROM app";
const QUERIER_ALL: &str = r#"[{ role = "querier", streams = ["*"] }]"#;

fn bearer(token: &str) -> String {
    format!("Bearer {token}")
}

/// 按场景约定启动写入实例：授权文件为 `entries`（缺省时把组 `sec` 映射为 `querier` 范围 [`*`]），`config` 为身份平台的
/// 配置项；数据集 `app` 写入一条记录。
fn start(fx: &Fixture, idp_cfg: &[(&'static str, String)], entries: &[String]) -> Server {
    let entries = if entries.is_empty() {
        vec![idp_grant(r#"group = "sec""#, QUERIER_ALL)]
    } else {
        entries.to_vec()
    };
    let server = authz::start(fx, &entries, &oidc::overrides(idp_cfg, &[]));
    ingest_ok(&server, "app", r#"[{"msg":"hello"}]"#);
    server
}

/// 以 `token` 提交合法查询。
fn query(server: &Server, token: &str) -> Resp {
    search_as(server, &bearer(token), SQL)
}

/// 等到（至多 20 秒）以 `token` 提交合法查询得 200：说明文档与公钥集在后台取得，取得之前身份平台令牌一律 401。
fn wait_accepted(server: &Server, token: &str) -> Resp {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = query(server, token);
        if r.status == 200 || Instant::now() >= deadline {
            return r;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

/// 等到（至多 `limit`）运行日志里出现 `event` 为 `event` 的记录。
fn wait_event(server: &Server, event: &str, limit: Duration) -> Vec<Value> {
    let deadline = Instant::now() + limit;
    loop {
        let got = events(server, event);
        if !got.is_empty() || Instant::now() >= deadline {
            return got;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

fn assert_401(r: &Resp, what: &str) {
    assert_eq!(r.status, 401, "{what}: {}", r.text());
    assert_eq!(r.json()["reason"], "unauthorized", "{what}");
    let www = r.header("WWW-Authenticate").unwrap_or_default();
    assert!(
        www.split_whitespace()
            .next()
            .is_some_and(|s| s.eq_ignore_ascii_case("bearer")),
        "{what}: WWW-Authenticate {www:?}"
    );
}

// ---------- 说明文档与公钥 ----------

#[test]
fn oidc_token_accepted() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let server = start(&f, &idp.config(), &[]);
    let r = wait_accepted(&server, &idp.token("alice"));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"][0]["n"], 1);
    // 台架只开说明文档、公钥集与令牌端点：没有向其他地址（自省、用户信息）发出请求（NO-INTROSPECTION）
    for req in idp.requests() {
        assert!(
            matches!(
                req.target.as_str(),
                "/.well-known/openid-configuration" | "/jwks"
            ),
            "{} {}",
            req.method,
            req.target
        );
        assert_eq!(req.method, "GET");
    }
}

#[test]
fn oidc_unreachable_at_startup() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    idp.stop();
    let alice = idp.token("alice");
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_401(&query(&server, &alice), "身份平台未运行时以 alice 的令牌");
    assert_eq!(query(&server, TOKEN).status, 200);
    // 后台的首次取得在开始监听之后才发出：等它在身份平台未运行时失败过，再启动身份平台
    wait_event(&server, "oidc_unavailable", Duration::from_secs(10));
    idp.resume();
    std::thread::sleep(Duration::from_secs(65));
    let r = query(&server, &alice);
    assert_eq!(r.status, 200, "{}", r.text());
    let evts = events(&server, "oidc_unavailable");
    assert_eq!(evts.len(), 1, "{evts:#?}");
    assert_eq!(evts[0]["level"], "warn");
}

#[test]
fn oidc_key_rotation() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(wait_accepted(&server, &idp.token("alice")).status, 200);
    idp.rotate("k2");
    let r = query(&server, &idp.token("alice"));
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn oidc_unknown_kid_rate_limited() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(wait_accepted(&server, &idp.token("alice")).status, 200);
    let before = idp.requests_to("/jwks").len();
    let key = Key::es256();
    let t0 = Instant::now();
    for i in 0..20 {
        let forged = key.sign_raw(
            &json!({"alg": "ES256", "kid": format!("forged-{i}")}),
            &idp.claims("alice"),
        );
        assert_401(&query(&server, &forged), &format!("伪造令牌 {i}"));
    }
    assert!(
        t0.elapsed() < Duration::from_secs(10),
        "20 个请求超过 10 秒"
    );
    let refetched = idp.requests_to("/jwks").len() - before;
    assert!(refetched <= 1, "公钥集被请求 {refetched} 次");
}

#[test]
fn oidc_discovery_document_rejected() {
    let bad: [&dyn Fn(&Idp) -> Value; 2] = [
        &|idp| {
            let mut d = idp.default_discovery();
            d["issuer"] = json!(format!("{}/", idp.issuer()));
            d
        },
        &|idp| {
            let mut d = idp.default_discovery();
            d["jwks_uri"] = json!(idp.url("/jwks").replacen("https://", "http://", 1));
            d
        },
    ];
    for (i, doc) in bad.iter().enumerate() {
        let f = Fixture::new();
        let idp = Idp::start(&f);
        idp.set_discovery(Some(doc(&idp)));
        let alice = idp.token("alice");
        let server = start(&f, &idp.config(), &[]);
        assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
        let evts = wait_event(&server, "oidc_unavailable", Duration::from_secs(10));
        assert_401(&query(&server, &alice), &format!("说明文档 {i}"));
        assert!(!evts.is_empty(), "说明文档 {i}：没有 oidc_unavailable");
        assert_eq!(evts[0]["level"], "warn");
        assert_eq!(evts[0]["issuer"], idp.issuer().as_str());
        assert_eq!(
            evts[0]["url"],
            format!("{}/.well-known/openid-configuration", idp.issuer()).as_str()
        );
    }
}

// ---------- 令牌的验证 ----------

#[test]
fn oidc_verification_failures_401() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(wait_accepted(&server, &idp.token("alice")).status, 200);

    let b64 = |v: &Value| authz::base64url(v.to_string().as_bytes());
    let header = json!({"alg": "RS256", "kid": "k1"});
    let with = |f: &dyn Fn(&mut Value)| {
        let mut p = idp.claims("alice");
        f(&mut p);
        idp.sign(&p)
    };
    let mut cases: Vec<(&str, String)> = Vec::new();
    cases.push((
        "alg none",
        format!(
            "{}.{}.",
            b64(&json!({"alg": "none", "kid": "k1"})),
            b64(&idp.claims("alice"))
        ),
    ));
    {
        // 以公钥集的 JSON（台架公钥集地址的响应体）为 HMAC 密钥
        let jwks = idp.jwks_body();
        let input = format!(
            "{}.{}",
            b64(&json!({"alg": "HS256", "kid": "k1"})),
            b64(&idp.claims("alice"))
        );
        let k = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, jwks.as_bytes());
        let tag = ring::hmac::sign(&k, input.as_bytes());
        cases.push((
            "alg HS256",
            format!("{input}.{}", authz::base64url(tag.as_ref())),
        ));
    }
    {
        let good = idp.token("alice");
        let (head, sig) = good.rsplit_once('.').unwrap();
        let mut sig = sig.as_bytes().to_vec();
        sig[0] = if sig[0] == b'A' { b'B' } else { b'A' };
        cases.push((
            "签名改动一个字节",
            format!("{head}.{}", String::from_utf8(sig).unwrap()),
        ));
    }
    cases.push((
        "iss 末尾多一个 /",
        with(&|p| p["iss"] = json!(format!("{}/", p["iss"].as_str().unwrap()))),
    ));
    cases.push(("aud 为 other", with(&|p| p["aud"] = json!("other"))));
    cases.push(("exp 早 120 秒", with(&|p| p["exp"] = json!(now() - 120))));
    cases.push(("nbf 晚 120 秒", with(&|p| p["nbf"] = json!(now() + 120))));
    cases.push((
        "没有 sub",
        with(&|p| {
            p.as_object_mut().unwrap().remove("sub");
        }),
    ));
    cases.push((
        "另一把私钥",
        Key::rsa(&f.root().join("pki")).sign_raw(&header, &idp.claims("alice")),
    ));
    for (what, token) in &cases {
        assert_401(&query(&server, token), what);
    }
}

#[test]
fn oidc_clock_skew() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(wait_accepted(&server, &idp.token("alice")).status, 200);
    let mut p = idp.claims("alice");
    p["exp"] = json!(now() - 30);
    let r = query(&server, &idp.sign(&p));
    assert_eq!(r.status, 200, "{}", r.text());
}

#[test]
fn oidc_client_id_falls_back_to_azp() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let entries = [idp_grant(r#"client_id = "ingest-bot""#, QUERIER_ALL)];
    let server = start(&f, &idp.config(), &entries);
    let token = |set: &[(&str, &str)]| {
        let mut p = idp.claims("svc");
        p.as_object_mut().unwrap().remove("groups");
        for (k, v) in set {
            p[*k] = json!(v);
        }
        idp.sign(&p)
    };
    let r = wait_accepted(&server, &token(&[("client_id", "ingest-bot")]));
    assert_eq!(r.status, 200, "{}", r.text());
    let r = query(&server, &token(&[("azp", "ingest-bot")]));
    assert_eq!(r.status, 200, "{}", r.text());
    let r = query(
        &server,
        &token(&[("client_id", "other"), ("azp", "ingest-bot")]),
    );
    r.assert_envelope(403, "forbidden");
}

#[test]
fn oidc_other_credentials_unaffected() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let q = ServiceToken::generate();
    let entries = [
        idp_grant(r#"group = "sec""#, QUERIER_ALL),
        token_entry("q", &q, QUERIER_ALL),
    ];
    let server = start(&f, &idp.config(), &entries);
    assert_eq!(wait_accepted(&server, &idp.token("alice")).status, 200);
    idp.stop();
    assert_eq!(search_as(&server, &q.bearer(), SQL).status, 200);
    assert_eq!(query(&server, TOKEN).status, 200);
    let unknown =
        Key::es256().sign_raw(&json!({"alg": "ES256", "kid": "k9"}), &idp.claims("alice"));
    assert_401(&query(&server, &unknown), "kid 不在缓存中的身份平台令牌");
}

/// REQ-AMB-CFG-STARTUP-V10「【第 3 刀】到身份平台的连接不读代理环境变量」：三个代理变量都指向没有服务的
/// `127.0.0.1:1`（被测进程以空环境启动，没有 `NO_PROXY`，也没有小写写法），到台架身份平台的连接照常直接建立。
#[test]
fn cfg_idp_ignores_proxy_env() {
    const DEAD_PROXY: &str = "http://127.0.0.1:1";
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let path = authz::write(&f, &[idp_grant(r#"group = "sec""#, QUERIER_ALL)]);
    let value = authz::authz_file_value(&path);
    let config = idp.config();
    let mut cmd = f.serve(&oidc::overrides(
        &config,
        &[("auth.authz_file", Some(value.as_str()))],
    ));
    cmd.env("HTTP_PROXY", DEAD_PROXY)
        .env("HTTPS_PROXY", DEAD_PROXY)
        .env("ALL_PROXY", DEAD_PROXY);
    let server = Server::start(cmd);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    ingest_ok(&server, "app", r#"[{"msg":"hello"}]"#);
    let r = wait_accepted(&server, &idp.token("alice"));
    assert_eq!(r.status, 200, "{}", r.text());
    let stderr = server.stderr_lines();
    assert!(
        !stderr
            .iter()
            .any(|l| l.contains(r#""event":"oidc_unavailable""#)),
        "{stderr:#?}"
    );
}

#[test]
fn oidc_catalog_process() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let path = authz::write(
        &f,
        &[idp_grant(
            r#"group = "eng""#,
            r#"[{ role = "catalog_reader", streams = ["*"] }]"#,
        )],
    );
    let value = authz::authz_file_value(&path);
    let cfg = idp.config();
    let overrides = oidc::overrides(
        &cfg,
        &[("auth.authz_file", Some(&value)), ("wal.dir", None)],
    );
    let server = Server::start(f.catalog(&overrides));
    let mut p = idp.claims("carol");
    p["groups"] = json!(["eng"]);
    let carol = bearer(&idp.sign(&p));
    let deadline = Instant::now() + Duration::from_secs(20);
    let r = loop {
        let r = get_as(&server, &carol, "/iceberg/v1/config");
        if r.status == 200 || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------- 授权块的身份平台场景（REQ-AMB-AUTH-AUTHZ） ----------
//
// 映射与可读名的实现在授权模块（REQ-AMB-AUTH-AUTHZ-IDP-MAPPING），场景要台架身份平台，所以写在本文件。

/// 等到以 `token` 提交合法查询不再是 401（说明文档与公钥集已取得），返回该响应。
fn wait_authenticated(server: &Server, token: &str) -> Resp {
    let deadline = Instant::now() + Duration::from_secs(20);
    loop {
        let r = query(server, token);
        if r.status != 401 || Instant::now() >= deadline {
            return r;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
}

#[test]
fn authz_idp_group_mapping() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let entries = [idp_grant(
        r#"group = "sec""#,
        r#"[{ role = "querier", streams = ["audit_*"] }]"#,
    )];
    let server = start(&f, &idp.config(), &entries);
    ingest_ok(&server, "audit_ssh", r#"[{"msg":"login"}]"#);
    let mut p = idp.claims("alice");
    p["groups"] = json!(["sec", "other"]);
    let alice = bearer(&idp.sign(&p));
    let deadline = Instant::now() + Duration::from_secs(20);
    let r = loop {
        let r = search_as(&server, &alice, "SELECT count(*) AS n FROM audit_ssh");
        if r.status != 401 || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(r.status, 200, "{}", r.text());
    search_as(&server, &alice, SQL).assert_envelope(400, "stream_not_found");
}

#[test]
fn authz_idp_no_grant_403() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let entries = [idp_grant(r#"group = "nobody""#, QUERIER_ALL)];
    let server = start(&f, &idp.config(), &entries);
    let bob = idp.token("bob");
    wait_authenticated(&server, &bob).assert_envelope(403, "forbidden");
    get_as(&server, &bearer(&bob), "/api/default/app/_fields").assert_envelope(403, "forbidden");
    get_as(&server, &bearer(&bob), "/metrics").assert_envelope(403, "forbidden");
}

#[test]
fn authz_access_event_principal_name() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let q = ServiceToken::generate();
    let entries = [
        idp_grant(r#"group = "sec""#, QUERIER_ALL),
        token_entry("q", &q, QUERIER_ALL),
    ];
    let server = start(&f, &idp.config(), &entries);
    let mut p = idp.claims("u-123");
    p["preferred_username"] = json!("alice");
    let alice = idp.sign(&p);
    assert_eq!(wait_accepted(&server, &alice).status, 200);
    assert_eq!(search_as(&server, &q.bearer(), SQL).status, 200);
    let deadline = Instant::now() + Duration::from_secs(10);
    let access = loop {
        let a = events(&server, "access");
        if a.iter().any(|e| e["principal"] == "token:q") || Instant::now() >= deadline {
            break a;
        }
        std::thread::sleep(Duration::from_millis(100));
    };
    let of = |principal: &str| {
        access
            .iter()
            .find(|e| e["principal"] == principal)
            .unwrap_or_else(|| panic!("没有 {principal} 的 access 记录：{access:#?}"))
            .clone()
    };
    assert_eq!(of("oidc:u-123")["principal_name"], "alice");
    let q_rec = of("token:q");
    assert!(
        q_rec.get("principal_name").is_some_and(Value::is_null),
        "{q_rec}"
    );
}

// ---------- 启动配置块的身份平台场景（REQ-AMB-CFG-STARTUP-V10） ----------

/// 运行到退出，返回退出码与 stderr。
fn run(cmd: &mut std::process::Command) -> (Option<i32>, String) {
    let out = cmd.output().expect("启动 amberlog");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

#[test]
fn cfg_oidc_keys_invalid_exit2() {
    let f = Fixture::new();
    let authz = authz::authz_file_value(&authz::write(&f, &[]));
    let a = Some(authz.as_str());
    let cases: [(Vec<common::Kv>, &[&str]); 4] = [
        (
            vec![
                ("auth.authz_file", a),
                ("auth.oidc.issuer", Some("\"http://idp.example\"")),
                ("auth.oidc.audience", Some("\"amberlog\"")),
            ],
            &["auth.oidc.issuer"],
        ),
        (
            vec![
                ("auth.authz_file", a),
                ("auth.oidc.issuer", Some("\"https://idp.example\"")),
            ],
            &["auth.oidc.audience"],
        ),
        (
            vec![
                ("auth.authz_file", a),
                ("auth.oidc.issuer", Some("\"https://idp.example\"")),
                ("auth.oidc.audience", Some("\"amberlog\"")),
                ("auth.oidc.subject_claim", Some("\"\"")),
            ],
            &["auth.oidc.subject_claim"],
        ),
        (
            vec![
                ("auth.oidc.issuer", Some("\"https://idp.example\"")),
                ("auth.oidc.audience", Some("\"amberlog\"")),
            ],
            &["auth.oidc.issuer", "auth.authz_file"],
        ),
    ];
    for (i, (overrides, want)) in cases.iter().enumerate() {
        let (code, stderr) = run(&mut f.serve(overrides));
        assert_eq!(code, Some(2), "设置 {i}: {stderr}");
        for w in *want {
            assert!(stderr.contains(w), "设置 {i} 应含 {w:?}: {stderr}");
        }
        assert!(
            !stderr.contains("\"event\":\"starting\""),
            "设置 {i}: {stderr}"
        );
    }
}

#[test]
fn cfg_oidc_unavailable_not_filtered() {
    let f = Fixture::new();
    let authz = authz::authz_file_value(&authz::write(&f, &[]));
    let mut cmd = f.serve(&[
        ("log.level", Some("\"error\"")),
        ("auth.authz_file", Some(authz.as_str())),
    ]);
    cmd.env("AMBERLOG_AUTH__OIDC__ISSUER", "https://127.0.0.1:1")
        .env("AMBERLOG_AUTH__OIDC__AUDIENCE", "amberlog");
    let s = Server::start(cmd);
    std::thread::sleep(Duration::from_secs(60));
    assert_eq!(s.get("/readyz").status, 200);
    let evts = events(&s, "oidc_unavailable");
    assert!(!evts.is_empty(), "{:#?}", s.stderr_lines());
    assert_eq!(evts[0]["level"], "warn");
    assert_eq!(evts[0]["issuer"], "https://127.0.0.1:1");
    assert!(evts[0]["url"].is_string(), "{}", evts[0]);
}

// ---------- 健康与指标块的身份平台场景（REQ-AMB-OBS-HEALTH-METRICS-V7） ----------

#[test]
fn health_oidc_fetch_failures_counted() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    idp.stop();
    let server = start(&f, &idp.config(), &[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    std::thread::sleep(Duration::from_secs(60));
    let r = get_as(&server, &bearer(TOKEN), "/metrics");
    assert_eq!(r.status, 200, "{}", r.text());
    let n: f64 = r
        .text()
        .lines()
        .find_map(|l| l.strip_prefix("amberlog_oidc_fetch_failures_total "))
        .unwrap_or_else(|| panic!("没有 amberlog_oidc_fetch_failures_total：{}", r.text()))
        .trim()
        .parse()
        .unwrap();
    assert!(n >= 1.0, "{n}");
}

// ---------- 界面登录的服务端 ----------
//
// 场景约定 C 组：另配 `auth.oidc.client_id` 为 `amberlog-ui`、`client_secret` 为 `ui-secret`、`scope` 为 `openid groups`
// （[`Idp::login_config`]）。

const TOKEN_BODY: &str =
    r#"{"code":"c1","code_verifier":"v1","redirect_uri":"https://amberlog.test/ui/callback"}"#;

fn assert_no_store(r: &Resp, what: &str) {
    assert_eq!(
        r.header("Cache-Control"),
        Some("no-store"),
        "{what}: {:?}",
        r.headers
    );
}

/// 按场景约定（含 C 组的三项）启动写入实例，等到说明文档已取得（`GET /ui/oidc/config` 得 200），返回该响应。
fn start_login(fx: &Fixture, idp: &Idp) -> (Server, Resp) {
    let server = start(fx, &idp.login_config(), &[]);
    let deadline = Instant::now() + Duration::from_secs(20);
    let r = loop {
        let r = server.get("/ui/oidc/config");
        if r.status == 200 || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(r.status, 200, "{}", r.text());
    (server, r)
}

fn post_token(server: &Server, body: &[u8]) -> Resp {
    server.request(
        "POST",
        "/ui/oidc/token",
        &[("Content-Type", "application/json")],
        body,
    )
}

#[test]
fn oidc_ui_config_and_callback() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let (server, config) = start_login(&f, &idp);
    assert_eq!(config.header("Content-Type"), Some("application/json"));
    assert_eq!(
        config.json(),
        json!({
            "authorization_endpoint": idp.url("/authorize"),
            "client_id": oidc::CLIENT_ID,
            "scope": "openid groups",
        })
    );
    let with_query = server.get("/ui/callback?code=x&state=y");
    let plain = server.get("/ui/callback");
    for r in [&with_query, &plain] {
        assert_eq!(r.status, 200);
        assert_eq!(r.header("Content-Type"), Some("text/html; charset=utf-8"));
    }
    assert_eq!(with_query.body, plain.body);
    for (what, r) in [
        ("config", &config),
        ("callback?", &with_query),
        ("callback", &plain),
    ] {
        assert_no_store(r, what);
    }
}

#[test]
fn oidc_ui_token_exchange() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let basic = format!(
        "Basic {}",
        oidc::base64_std(format!("{}:{}", oidc::CLIENT_ID, oidc::CLIENT_SECRET).as_bytes())
    );
    idp.on_token(Some(std::sync::Arc::new(move |req: &common::webhook::Request| {
        let form = oidc::form(&String::from_utf8_lossy(&req.body));
        let want = [
            ("grant_type", "authorization_code"),
            ("code", "c1"),
            ("code_verifier", "v1"),
            ("redirect_uri", "https://amberlog.test/ui/callback"),
        ];
        let ok = req.header("Authorization") == Some(basic.as_str())
            && want.iter().all(|(k, v)| form.get(*k).map(String::as_str) == Some(*v));
        let body = if ok {
            json!({"access_token": "T", "token_type": "Bearer", "expires_in": 300, "refresh_token": "R", "id_token": "I"})
        } else {
            json!({"error": "invalid_grant"})
        };
        common::webhook::Reply::status(if ok { 200 } else { 400 }).body(body.to_string().as_bytes())
    })));
    let (server, _) = start_login(&f, &idp);
    let before = idp.requests().len();
    let r = post_token(&server, TOKEN_BODY.as_bytes());
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.header("Content-Type"), Some("application/json"));
    assert_eq!(r.text(), r#"{"access_token":"T","expires_in":300}"#);
    assert_no_store(&r, "token");
    let during: Vec<_> = idp.requests().into_iter().skip(before).collect();
    assert_eq!(during.len(), 1, "{during:#?}");
    assert_eq!(
        (during[0].method.as_str(), during[0].target.as_str()),
        ("POST", "/token")
    );
}

#[test]
fn oidc_ui_token_errors() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let (server, _) = start_login(&f, &idp);
    let tokens = || idp.requests_to("/token").len();
    let before = tokens();
    let big = vec![b' '; 16_385];
    let r1 = post_token(&server, &big);
    let r2 = post_token(&server, b"not json");
    let r3 = post_token(&server, br#"{"code":"c1"}"#);
    assert_eq!(tokens(), before, "前三次不应向令牌端点发出请求");
    // 台架令牌端点不认识授权码 c1，回 400 invalid_grant
    let r4 = post_token(&server, TOKEN_BODY.as_bytes());
    assert_eq!(tokens(), before + 1);
    idp.stop();
    let r5 = post_token(&server, TOKEN_BODY.as_bytes());
    let want = [
        (413, "payload_too_large"),
        (400, "invalid_json"),
        (400, "invalid_request"),
        (400, "invalid_request"),
        (503, "unavailable"),
    ];
    for (i, (r, (status, reason))) in [&r1, &r2, &r3, &r4, &r5].iter().zip(want).enumerate() {
        r.assert_envelope(status, reason);
        assert_no_store(r, &format!("第 {} 个", i + 1));
    }
}

#[test]
fn oidc_ui_discovery_unavailable() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    idp.stop();
    let server = start(&f, &idp.login_config(), &[]);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    let config = server.get("/ui/oidc/config");
    let token = post_token(&server, TOKEN_BODY.as_bytes());
    for (what, r) in [("config", &config), ("token", &token)] {
        r.assert_envelope(503, "unavailable");
        assert_no_store(r, what);
    }
}

#[test]
fn oidc_ui_off_404() {
    let idp_fx = Fixture::new();
    let idp = Idp::start(&idp_fx);
    let grants = [idp_grant(r#"group = "sec""#, QUERIER_ALL)];
    let no_issuer = Fixture::new();
    let no_client = Fixture::new();
    let disabled = Fixture::new();
    let servers = [
        no_issuer.start(&[]),
        authz::start(&no_client, &grants, &oidc::overrides(&idp.config(), &[])),
        authz::start(
            &disabled,
            &grants,
            &oidc::overrides(&idp.login_config(), &[("auth.enabled", Some("false"))]),
        ),
    ];
    for (i, server) in servers.iter().enumerate() {
        for (method, path) in [
            ("GET", "/ui/oidc/config"),
            ("GET", "/ui/callback"),
            ("POST", "/ui/oidc/token"),
        ] {
            let r = server.request(
                method,
                path,
                &[("Content-Type", "application/json")],
                TOKEN_BODY.as_bytes(),
            );
            assert_eq!(r.status, 404, "实例 {i} {method} {path}: {}", r.text());
            assert_eq!(r.json()["reason"], "not_found", "实例 {i} {method} {path}");
        }
    }
}

#[test]
fn oidc_ui_no_auth_no_gate() {
    let f = Fixture::new();
    let idp = Idp::start(&f);
    let path = authz::write(&f, &[idp_grant(r#"group = "sec""#, QUERIER_ALL)]);
    let value = authz::authz_file_value(&path);
    let cfg = idp.login_config();
    let marker = f.root().join("paused");
    let mut cmd = f.serve(&oidc::overrides(&cfg, &[("auth.authz_file", Some(&value))]));
    cmd.env("TESTBED_FAULT_PAUSE", "shutdown.before_exit")
        .env("TESTBED_FAULT_MARKER", &marker);
    let server = Server::start(cmd);
    let deadline = Instant::now() + Duration::from_secs(20);
    let r1 = loop {
        let r = server.request(
            "GET",
            "/ui/oidc/config",
            &[("Authorization", "Bearer wrong")],
            b"",
        );
        if r.status == 200 || Instant::now() >= deadline {
            break r;
        }
        std::thread::sleep(Duration::from_millis(200));
    };
    assert_eq!(r1.status, 200, "{}", r1.text());
    let r2 = server.request("DELETE", "/ui/oidc/token", &[], b"");
    assert_eq!(r2.status, 405, "{}", r2.text());
    assert_eq!(r2.header("Allow"), Some("POST"));
    assert_no_store(&r2, "405");
    server.signal("TERM");
    assert!(
        common::wait_file(&marker, Duration::from_secs(30)),
        "未停在 shutdown.before_exit"
    );
    query(&server, TOKEN).assert_envelope(503, "unavailable");
    let r3 = server.get("/ui/callback");
    assert_eq!(r3.status, 200, "{}", r3.text());
}

#[test]
fn cfg_oidc_client_keys_invalid_exit2() {
    let f = Fixture::new();
    let authz = authz::authz_file_value(&authz::write(&f, &[]));
    let a = Some(authz.as_str());
    let base: [common::Kv; 3] = [
        ("auth.authz_file", a),
        ("auth.oidc.issuer", Some("\"https://idp.example\"")),
        ("auth.oidc.audience", Some("\"amberlog\"")),
    ];
    let with = |extra: &[common::Kv<'static>]| -> Vec<common::Kv> {
        let mut v = base.to_vec();
        v.extend_from_slice(extra);
        v
    };
    let id = ("auth.oidc.client_id", Some("\"amberlog-ui\""));
    let secret = ("auth.oidc.client_secret", Some("\"s\""));
    let settings: [(Vec<common::Kv>, bool); 4] = [
        (with(&[id]), false),
        (base.to_vec(), true),
        (vec![("auth.authz_file", a), id, secret], false),
        (
            with(&[id, secret, ("auth.oidc.scope", Some("\"\""))]),
            false,
        ),
    ];
    let want = [
        "auth.oidc.client_secret",
        "auth.oidc.client_id",
        "auth.oidc.issuer",
        "auth.oidc.scope",
    ];
    for (i, ((overrides, secret_env), w)) in settings.iter().zip(want).enumerate() {
        // `serve` 每次都写同一个配置文件：建好命令就跑
        let mut cmd = f.serve(overrides);
        if *secret_env {
            cmd.env("AMBERLOG_AUTH__OIDC__CLIENT_SECRET", "s");
        }
        let (code, stderr) = run(&mut cmd);
        assert_eq!(code, Some(2), "设置 {i}: {stderr}");
        assert!(stderr.contains(w), "设置 {i} 应含 {w:?}: {stderr}");
        assert!(
            !stderr.contains("\"event\":\"starting\""),
            "设置 {i}: {stderr}"
        );
    }
}
