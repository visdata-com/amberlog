//! REQ-AMB-ARCH-ENCRYPTION 的系统测试：以子进程运行 `amberlog`，
//! 经退出码、stderr、TLS 连接观察。证书与私钥由测试现场生成（`common::tls`），对象存储默认 `file://` 临时目录。
//!
//! - 「台架对象存储停止服务」用不可达的 S3 端点代替：配置一步的判定先于对象存储校验，进程不会走到连接那一步。
//! - 证书重读的场景按块的时限真实等待（150 秒起），不缩短。

mod common;

use std::net::{SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use common::tls::{
    KeyKind, Pair, Pki, TlsClient, Versions, is_http, plaintext_reply, tls11_hello_reply,
};
use common::{BEARER, Fixture, HeldPort, Kv, Server, free_port};
use serde_json::Value;

const READY_WAIT: Duration = Duration::from_secs(60);
/// 「明文请求在 10 秒内没有得到任何 HTTP 响应」。
const NO_REPLY: Duration = Duration::from_secs(10);

struct Run {
    code: i32,
    stderr: String,
}

fn run(cmd: &mut Command) -> Run {
    let Output { status, stderr, .. } = cmd.output().expect("启动 amberlog");
    Run {
        code: status.code().expect("进程应正常退出而不是被信号终止"),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    }
}

fn quoted(p: &Path) -> String {
    format!("\"{}\"", p.display())
}

/// 以证书 `pair` 运行的配置项。
fn with_tls(pair: &Pair) -> Vec<(&'static str, String)> {
    vec![
        ("server.tls.cert_file", quoted(&pair.cert)),
        ("server.tls.key_file", quoted(&pair.key)),
    ]
}

fn kv<'a>(owned: &'a [(&'static str, String)]) -> Vec<Kv<'a>> {
    owned.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect()
}

/// 一个测试的 CA「CA-T」与它签发的 (C1, K1)。
struct Certs {
    pki: Pki,
    ca: Pair,
    c1: Pair,
}

fn certs(f: &Fixture) -> Certs {
    let pki = Pki::new(f.root());
    let ca = pki.ca("CA-T");
    let c1 = pki.leaf(&ca, "c1", KeyKind::P256);
    Certs { pki, ca, c1 }
}

/// 以 TLS 运行并等到 `ready`。
fn start_tls(f: &Fixture, pair: &Pair, extra: &[(&'static str, String)]) -> Server {
    let mut owned = with_tls(pair);
    owned.extend(extra.iter().cloned());
    f.start(&kv(&owned))
}

/// 经 TLS 反复 `GET /readyz` 直到 200（至多 `limit`）。
fn wait_readyz_tls(client: &TlsClient, addr: SocketAddr, limit: Duration) -> u16 {
    let deadline = Instant::now() + limit;
    loop {
        let status = client
            .connect(addr)
            .map(|mut c| c.request("GET", "/readyz", &[], b"").status)
            .unwrap_or(0);
        if status == 200 || Instant::now() >= deadline {
            return status;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn ready(s: &Server) -> Value {
    s.json_lines()
        .into_iter()
        .find(|v| v["event"] == "ready")
        .expect("ready 事件")
}

fn events(s: &Server, name: &str) -> Vec<Value> {
    s.json_lines()
        .into_iter()
        .filter(|v| v["event"] == name)
        .collect()
}

/// 新连接上服务端出示的叶证书。
fn leaf_now(client: &TlsClient, addr: SocketAddr) -> Vec<u8> {
    let mut conn = client.connect(addr).expect("TLS 握手");
    assert_eq!(conn.request("GET", "/healthz", &[], b"").status, 200);
    conn.peer_chain().remove(0)
}

// ---------------------------------------------------------------------------
// 配置一步：证书与 CA 文件
// ---------------------------------------------------------------------------

#[test]
fn enc_cert_error_before_object_store() {
    let f = Fixture::new();
    let c = certs(&f);
    let x = f.root().join("x.pem");
    std::fs::write(&x, "not pem").unwrap();
    let owned = vec![
        ("server.tls.cert_file", quoted(&x)),
        ("server.tls.key_file", quoted(&c.c1.key)),
        ("object_store.url", "\"s3://bucket\"".to_owned()),
        (
            "object_store.s3.conditional_put",
            "\"create_only\"".to_owned(),
        ),
        (
            "object_store.s3.endpoint",
            "\"http://127.0.0.1:1\"".to_owned(),
        ),
        ("object_store.s3.force_path_style", "true".to_owned()),
        ("object_store.s3.access_key_id", "\"k\"".to_owned()),
        ("object_store.s3.secret_access_key", "\"s\"".to_owned()),
    ];
    let r = run(&mut f.serve(&kv(&owned)));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(r.stderr.contains("server.tls.cert_file"), "{}", r.stderr);
    assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
}

#[test]
fn enc_cert_file_invalid_exit2() {
    let f = Fixture::new();
    let pki = Pki::new(f.root());
    let ca = pki.ca("CA-T");
    let (c1, c2) = (
        pki.leaf(&ca, "c1", KeyKind::P256),
        pki.leaf(&ca, "c2", KeyKind::P256),
    );
    let x = f.root().join("x.pem");
    std::fs::write(&x, "not pem").unwrap();
    let e = pki.encrypted(&c1.key);
    let missing = f.root().join("nonexistent.pem");
    let cases = [
        (&missing, &c1.key, &["server.tls.cert_file"][..]),
        (&x, &c1.key, &["server.tls.cert_file"][..]),
        (&c1.cert, &x, &["server.tls.key_file"][..]),
        (&c1.cert, &e, &["server.tls.key_file"][..]),
        (
            &c1.cert,
            &c2.key,
            &["server.tls.cert_file", "server.tls.key_file"][..],
        ),
    ];
    for (cert, key, keys) in cases {
        // 占住的端口：被测进程走到监听这一步必然绑定失败（退出码 4、stderr 含 `cannot listen on`）
        let held = HeldPort::new();
        let owned = vec![
            ("server.http_addr", held.addr()),
            ("server.tls.cert_file", quoted(cert)),
            ("server.tls.key_file", quoted(key)),
        ];
        let mut child = f
            .serve(&kv(&owned))
            .stderr(std::process::Stdio::piped())
            .spawn()
            .unwrap();
        let deadline = Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(s) = child.try_wait().unwrap() {
                break s;
            }
            assert!(Instant::now() < deadline, "{cert:?} {key:?}：进程没有退出");
            std::thread::sleep(Duration::from_millis(20));
        };
        let mut stderr = String::new();
        std::io::Read::read_to_string(child.stderr.as_mut().unwrap(), &mut stderr).unwrap();
        assert_eq!(status.code(), Some(2), "{cert:?} {key:?}：{stderr}");
        // 退出之前没有试过监听
        assert!(
            !stderr.contains("cannot listen on"),
            "{cert:?} {key:?}：{stderr}"
        );
        assert!(!stderr.contains("\"event\":\"starting\""), "{stderr}");
        for k in keys {
            assert!(stderr.contains(k), "{cert:?} {key:?} 应指出 {k}：{stderr}");
        }
    }
}

#[test]
fn enc_key_algorithms_accepted() {
    let f = Fixture::new();
    let pki = Pki::new(f.root());
    let ca = pki.ca("CA-T");
    let client = TlsClient::new(&ca.cert, Versions::Both);
    for kind in [
        KeyKind::Rsa2048Pkcs1,
        KeyKind::Rsa4096Pkcs8,
        KeyKind::P256Sec1,
        KeyKind::P384Pkcs8,
        KeyKind::Ed25519,
    ] {
        let pair = pki.leaf(&ca, "leaf", kind);
        let s = start_tls(&f, &pair, &[]);
        assert_eq!(ready(&s)["tls"], true, "{kind:?}");
        let mut conn = client
            .connect(s.addr)
            .unwrap_or_else(|e| panic!("{kind:?}：握手失败 {e}"));
        assert_eq!(conn.peer_chain()[0], pair.leaf_der(), "{kind:?}");
        assert_eq!(
            conn.request("GET", "/healthz", &[], b"").status,
            200,
            "{kind:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// 入站 TLS
// ---------------------------------------------------------------------------

#[test]
fn enc_tls_only_all_endpoints() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&client, s.addr, READY_WAIT), 200);
    assert_eq!(client.get(s.addr, "/healthz", &[]).status, 200);
    assert_eq!(client.get(s.addr, "/readyz", &[]).status, 200);
    assert_eq!(
        client
            .get(s.addr, "/metrics", &[("Authorization", BEARER)])
            .status,
        200
    );
    let ingest = client.connect(s.addr).unwrap().request(
        "POST",
        "/api/default/app/_json",
        &[
            ("Authorization", BEARER),
            ("Content-Type", "application/json"),
        ],
        br#"[{"k":1}]"#,
    );
    assert_eq!(ingest.status, 200, "{}", ingest.text());
    assert_eq!(
        client
            .get(s.addr, "/iceberg/v1/config", &[("Authorization", BEARER)])
            .status,
        200
    );
    assert!(!is_http(&plaintext_reply(s.addr, "/healthz", NO_REPLY)));
}

#[test]
fn enc_tls_only_ui() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(client.get(s.addr, "/ui", &[]).status, 200);
    assert!(!is_http(&plaintext_reply(s.addr, "/ui", NO_REPLY)));
}

#[test]
fn enc_tls_presents_configured_chain() {
    let f = Fixture::new();
    let pki = Pki::new(f.root());
    let ca = pki.ca("CA-T");
    let int = pki.intermediate(&ca, "CA-I");
    let leaf = pki.leaf(&int, "leaf", KeyKind::P256);
    let chain = f.root().join("chain.pem");
    let mut pem = std::fs::read(&leaf.cert).unwrap();
    pem.extend(std::fs::read(&int.cert).unwrap());
    std::fs::write(&chain, pem).unwrap();
    let s = start_tls(
        &f,
        &Pair {
            cert: chain,
            key: leaf.key.clone(),
        },
        &[],
    );
    let client = TlsClient::new(&ca.cert, Versions::Both);
    let mut conn = client.connect(s.addr).expect("只信任 CA-T 也能握手");
    assert_eq!(conn.request("GET", "/healthz", &[], b"").status, 200);
    assert_eq!(conn.peer_chain(), vec![leaf.leaf_der(), int.leaf_der()]);
}

#[test]
fn enc_tls_versions_12_13_only() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    for v in [Versions::Only13, Versions::Only12] {
        let client = TlsClient::new(&c.ca.cert, v);
        assert_eq!(client.get(s.addr, "/healthz", &[]).status, 200, "{v:?}");
    }
    let reply = tls11_hello_reply(s.addr, Duration::from_secs(5));
    // 没有 ServerHello（握手记录 0x16），更没有 HTTP 响应
    assert!(reply.first() != Some(&0x16), "{reply:?}");
    assert!(!is_http(&reply));
}

#[test]
fn enc_tls_no_client_cert_request() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    for v in [Versions::Only12, Versions::Only13] {
        let client = TlsClient::new(&c.ca.cert, v);
        assert_eq!(client.get(s.addr, "/healthz", &[]).status, 200, "{v:?}");
        assert!(!client.asked_for_client_cert(), "{v:?}");
    }
}

#[test]
fn enc_handshake_fail_no_http_response() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    let trusted = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&trusted, s.addr, READY_WAIT), 200);
    let other_ca = c.pki.ca("CA-other");
    let untrusted = TlsClient::new(&other_ca.cert, Versions::Both);
    assert!(untrusted.connect(s.addr).is_err());
    assert!(!is_http(&plaintext_reply(s.addr, "/readyz", NO_REPLY)));
    assert_eq!(trusted.get(s.addr, "/readyz", &[]).status, 200);
}

#[test]
fn enc_stalled_handshake_isolated() {
    let f = Fixture::new();
    let c = certs(&f);
    let s = start_tls(&f, &c.c1, &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&client, s.addr, READY_WAIT), 200);
    let _idle: Vec<TcpStream> = (0..20)
        .map(|_| TcpStream::connect(s.addr).unwrap())
        .collect();
    let started = Instant::now();
    assert_eq!(client.get(s.addr, "/healthz", &[]).status, 200);
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
}

#[test]
fn enc_ready_tls_field() {
    let f = Fixture::new();
    let c = certs(&f);
    let tls = start_tls(&f, &c.c1, &[]);
    let plain = f.start(&[]);
    let declared = f.start(&[
        ("server.http_addr", Some("\"0.0.0.0:0\"")),
        ("server.behind_tls_proxy", Some("true")),
    ]);
    assert_eq!(ready(&tls)["tls"], true);
    assert_eq!(ready(&plain)["tls"], false);
    assert_eq!(ready(&declared)["tls"], false);
}

// ---------------------------------------------------------------------------
// 明文规则
// ---------------------------------------------------------------------------

#[test]
fn enc_loopback_plaintext_allowed() {
    for overrides in [
        &[("server.http_addr", Some("\"127.0.0.1:0\""))][..],
        &[("server.http_addr", Some("\"127.0.0.2:0\""))][..],
        &[
            ("server.http_addr", Some("\"127.0.0.1:0\"")),
            ("server.behind_tls_proxy", Some("true")),
        ][..],
    ] {
        let f = Fixture::new();
        let s = f.start(overrides);
        assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200, "{overrides:?}");
        assert!(events(&s, "plaintext_listen").is_empty(), "{overrides:?}");
    }
}

#[test]
fn enc_non_loopback_plaintext_rejected() {
    let runs = [
        ("serve", "\"0.0.0.0:0\""),
        ("serve", "\"[::]:0\""),
        ("serve", "\"[::ffff:127.0.0.1]:0\""),
        ("catalog", "\"0.0.0.0:0\""),
    ];
    for (form, addr) in runs {
        let f = Fixture::new();
        let overrides = [("server.http_addr", Some(addr))];
        let mut cmd = match form {
            "serve" => f.serve(&overrides),
            _ => f.catalog(&overrides),
        };
        let r = run(&mut cmd);
        assert_eq!(r.code, 2, "{form} {addr}：{}", r.stderr);
        assert!(r.stderr.contains("server.http_addr"), "{}", r.stderr);
        assert!(r.stderr.contains("server.behind_tls_proxy"), "{}", r.stderr);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
    }
}

#[test]
fn enc_declared_plaintext_listens() {
    let f = Fixture::new();
    let port = free_port();
    let addr = format!("\"0.0.0.0:{port}\"");
    let s = f.start(&[
        ("server.http_addr", Some(&addr)),
        ("server.behind_tls_proxy", Some("true")),
    ]);
    let local: SocketAddr = format!("127.0.0.1:{port}").parse().unwrap();
    let deadline = Instant::now() + READY_WAIT;
    while common::http(local, "GET", "/readyz", &[], b"").status != 200 {
        assert!(Instant::now() < deadline, "60 秒内 /readyz 未返回 200");
        std::thread::sleep(Duration::from_millis(250));
    }
    assert_eq!(ready(&s)["tls"], false);
}

#[test]
fn enc_plaintext_rule_ignores_auth() {
    let f = Fixture::new();
    let base = [
        ("auth.enabled", Some("false")),
        ("auth.token", None),
        ("server.http_addr", Some("\"0.0.0.0:0\"")),
    ];
    let r = run(&mut f.serve(&base));
    assert_eq!(r.code, 2, "{}", r.stderr);
    assert!(r.stderr.contains("server.behind_tls_proxy"), "{}", r.stderr);
    let mut declared = base.to_vec();
    declared.push(("server.behind_tls_proxy", Some("true")));
    let s = f.start(&declared);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    assert_eq!(events(&s, "auth_disabled").len(), 1);
    assert_eq!(events(&s, "plaintext_listen").len(), 1);
}

#[test]
fn enc_plaintext_listen_event() {
    let f = Fixture::new();
    let s = f.start(&[
        ("server.http_addr", Some("\"0.0.0.0:0\"")),
        ("server.behind_tls_proxy", Some("true")),
        ("log.level", Some("\"error\"")),
        ("log.format", Some("\"json\"")),
    ]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    std::thread::sleep(Duration::from_secs(60));
    let order: Vec<String> = s
        .json_lines()
        .into_iter()
        .filter_map(|v| v["event"].as_str().map(str::to_owned))
        .filter(|e| ["starting", "plaintext_listen", "ready"].contains(&e.as_str()))
        .collect();
    assert_eq!(order, ["starting", "plaintext_listen", "ready"]);
    let ev = events(&s, "plaintext_listen");
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0]["level"], "warn");
    assert_eq!(ev[0]["http_addr"], "0.0.0.0:0");
}

// ---------------------------------------------------------------------------
// file 存储下出站与加密设置不起作用
// ---------------------------------------------------------------------------

#[test]
fn enc_file_store_ignores_outbound_and_sse() {
    let f = Fixture::new();
    let s = f.start(&[
        ("object_store.s3.endpoint", Some("\"http://127.0.0.1:9\"")),
        ("object_store.s3.ca_file", Some("\"/nonexistent/ca.pem\"")),
        (
            "object_store.s3.server_side_encryption",
            Some("\"sse_kms\""),
        ),
    ]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    let stderr = s.stderr_lines().join("\n");
    assert!(!stderr.contains("object_store.s3.ca_file"), "{stderr}");
    assert!(events(&s, "object_store_plaintext").is_empty(), "{stderr}");
}

// ---------------------------------------------------------------------------
// 证书重读
// ---------------------------------------------------------------------------

/// 等待 `d`，期间不做别的。
fn wait(d: Duration) {
    std::thread::sleep(d);
}

/// 证书与私钥经同一目录下的符号链接指向实际文件（同 Kubernetes Secret 卷的布局）：`<root>/tls/cert.pem` →
/// `..data/cert.pem`，`..data` → 某个版本目录。返回两个配置路径。
/// 先写临时文件再改名：重读不会读到写了一半的文件（数 `tls_reload_failed` 个数的场景不应多出写到一半的那一种内容）。
fn replace(path: &Path, bytes: &[u8]) {
    let tmp = path.with_extension("tmp");
    std::fs::write(&tmp, bytes).unwrap();
    std::fs::rename(&tmp, path).unwrap();
}

struct Projected {
    root: std::path::PathBuf,
    n: u32,
}

impl Projected {
    fn new(f: &Fixture, pair: &Pair) -> Projected {
        let root = f.root().join("tls");
        std::fs::create_dir_all(&root).unwrap();
        let mut p = Projected { root, n: 0 };
        p.switch(pair, None);
        std::os::unix::fs::symlink("..data/cert.pem", p.root.join("cert.pem")).unwrap();
        std::os::unix::fs::symlink("..data/key.pem", p.root.join("key.pem")).unwrap();
        p
    }

    fn pair(&self) -> Pair {
        Pair {
            cert: self.root.join("cert.pem"),
            key: self.root.join("key.pem"),
        }
    }

    /// 把 `pair` 写进新的版本目录，以一次改名把 `..data` 整体切过去；`older_than` 给出时把新文件的修改时间设为早于它。
    fn switch(&mut self, pair: &Pair, older_than: Option<&Path>) {
        self.n += 1;
        let dir = self.root.join(format!("..v{}", self.n));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(&pair.cert, dir.join("cert.pem")).unwrap();
        std::fs::copy(&pair.key, dir.join("key.pem")).unwrap();
        if let Some(old) = older_than {
            let t = std::fs::metadata(old).unwrap().modified().unwrap() - Duration::from_secs(3600);
            for name in ["cert.pem", "key.pem"] {
                std::fs::File::options()
                    .write(true)
                    .open(dir.join(name))
                    .unwrap()
                    .set_modified(t)
                    .unwrap();
            }
        }
        let tmp = self.root.join("..data_tmp");
        let _ = std::fs::remove_file(&tmp);
        std::os::unix::fs::symlink(format!("..v{}", self.n), &tmp).unwrap();
        std::fs::rename(&tmp, self.root.join("..data")).unwrap();
    }
}

#[test]
fn enc_reload_applies_new_cert() {
    let f = Fixture::new();
    let c = certs(&f);
    let c2 = c.pki.leaf(&c.ca, "c2", KeyKind::P256);
    let mut proj = Projected::new(&f, &c.c1);
    let s = start_tls(&f, &proj.pair(), &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&client, s.addr, READY_WAIT), 200);
    let current = proj.root.join("..v1/cert.pem");
    proj.switch(&c2, Some(&current));
    for _ in 0..12 {
        wait(Duration::from_secs(10));
        assert_eq!(client.get(s.addr, "/readyz", &[]).status, 200);
    }
    assert_eq!(leaf_now(&client, s.addr), c2.leaf_der());
    assert!(events(&s, "tls_reload_failed").is_empty());
}

#[test]
fn enc_reload_keeps_existing_connections() {
    let f = Fixture::new();
    let c = certs(&f);
    let c2 = c.pki.leaf(&c.ca, "c2", KeyKind::P256);
    let mut proj = Projected::new(&f, &c.c1);
    let s = start_tls(&f, &proj.pair(), &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    let mut k = client.connect(s.addr).unwrap();
    assert_eq!(k.request("GET", "/healthz", &[], b"").status, 200);
    proj.switch(&c2, None);
    for _ in 0..15 {
        wait(Duration::from_secs(10));
        assert_eq!(k.request("GET", "/healthz", &[], b"").status, 200);
    }
    assert_eq!(leaf_now(&client, s.addr), c2.leaf_der());
}

#[test]
fn enc_reload_invalid_keeps_old_cert() {
    let f = Fixture::new();
    let c = certs(&f);
    let c2 = c.pki.leaf(&c.ca, "c2", KeyKind::P256);
    let c1_der = c.c1.leaf_der();
    let mut s = start_tls(&f, &c.c1, &[]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&client, s.addr, READY_WAIT), 200);
    std::fs::copy(&c2.cert, &c.c1.cert).unwrap();
    wait(Duration::from_secs(150));
    assert_eq!(leaf_now(&client, s.addr), c1_der);
    assert_eq!(s.wait_exit(Duration::ZERO), None, "进程不应退出");
    assert_eq!(client.get(s.addr, "/readyz", &[]).status, 200);
}

#[test]
fn enc_tls_reload_failed_once_per_content() {
    let f = Fixture::new();
    let c = certs(&f);
    let c2 = c.pki.leaf(&c.ca, "c2", KeyKind::P256);
    let mut proj = Projected::new(&f, &c.c1);
    let s = start_tls(&f, &proj.pair(), &[("log.level", "\"error\"".to_owned())]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    let cert_now = proj.root.join("..v1/cert.pem");
    replace(&cert_now, &std::fs::read(&c2.cert).unwrap());
    wait(Duration::from_secs(150));
    let ev = events(&s, "tls_reload_failed");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["level"], "error");
    assert_eq!(ev[0]["cert_file"], proj.pair().cert.display().to_string());
    assert_eq!(ev[0]["key_file"], proj.pair().key.display().to_string());
    replace(&cert_now, b"not pem");
    wait(Duration::from_secs(150));
    assert_eq!(events(&s, "tls_reload_failed").len(), 2);
    proj.switch(&c2, None);
    wait(Duration::from_secs(150));
    assert_eq!(leaf_now(&client, s.addr), c2.leaf_der());
    assert_eq!(events(&s, "tls_reload_failed").len(), 2);
}

#[test]
fn enc_tls_reload_failed_missing_file_state() {
    let f = Fixture::new();
    let c = certs(&f);
    let c1_der = c.c1.leaf_der();
    let s = start_tls(&f, &c.c1, &[("log.level", "\"error\"".to_owned())]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    std::fs::remove_file(&c.c1.key).unwrap();
    wait(Duration::from_secs(150));
    assert_eq!(events(&s, "tls_reload_failed").len(), 1);
    assert_eq!(leaf_now(&client, s.addr), c1_der);
    wait(Duration::from_secs(150));
    assert_eq!(events(&s, "tls_reload_failed").len(), 1);
}

#[test]
fn enc_tls_reload_failed_rearms_after_valid() {
    let f = Fixture::new();
    let c = certs(&f);
    let c2 = c.pki.leaf(&c.ca, "c2", KeyKind::P256);
    let c1_pem = std::fs::read(&c.c1.cert).unwrap();
    let c1_der = c.c1.leaf_der();
    let s = start_tls(&f, &c.c1, &[("log.level", "\"error\"".to_owned())]);
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    let c2_pem = std::fs::read(&c2.cert).unwrap();
    replace(&c.c1.cert, &c2_pem);
    wait(Duration::from_secs(150));
    replace(&c.c1.cert, &c1_pem);
    wait(Duration::from_secs(150));
    replace(&c.c1.cert, &c2_pem);
    wait(Duration::from_secs(150));
    assert_eq!(events(&s, "tls_reload_failed").len(), 2);
    assert_eq!(leaf_now(&client, s.addr), c1_der);
}

// ---------------------------------------------------------------------------
// 出站：加密台架（tools/testbed-encryption.sh）上的 TLS 对象存储、服务端加密与请求日志
// ---------------------------------------------------------------------------

use common::s3log::{Exchange, S3Log};

fn tb(k: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| {
        panic!(
            "{k} 未设置：这个测试要用加密台架，先 eval \"$(tools/testbed-encryption.sh <标签> up)\""
        )
    })
}

/// 加密台架上的一个被测实例：`s3://<台架桶>/<本实例独有路径>`，CA-S 写进临时文件。
struct TlsS3 {
    f: Fixture,
    path: String,
    ca: std::path::PathBuf,
}

impl TlsS3 {
    fn new() -> TlsS3 {
        static N: std::sync::atomic::AtomicU32 = std::sync::atomic::AtomicU32::new(0);
        let f = Fixture::new();
        let ca = f.root().join("ca-s.pem");
        std::fs::write(&ca, tb("TESTBED_TLS_CA_PEM")).unwrap();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .subsec_nanos();
        let path = format!(
            "enc-{}-{}-{nanos}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        TlsS3 { f, path, ca }
    }

    /// 以端点 `endpoint` 运行的对象存储配置（不含 CA 文件），外加 `extra`。
    fn config(
        &self,
        endpoint: &str,
        extra: &[(&'static str, String)],
    ) -> Vec<(&'static str, String)> {
        let mut owned = vec![
            (
                "object_store.url",
                format!("\"s3://{}/{}\"", tb("TESTBED_TLS_S3_BUCKET"), self.path),
            ),
            (
                "object_store.s3.conditional_put",
                "\"create_only\"".to_owned(),
            ),
            ("object_store.s3.endpoint", format!("\"{endpoint}\"")),
            ("object_store.s3.force_path_style", "true".to_owned()),
            (
                "object_store.s3.access_key_id",
                format!("\"{}\"", tb("TESTBED_TLS_S3_ACCESS_KEY_ID")),
            ),
            (
                "object_store.s3.secret_access_key",
                format!("\"{}\"", tb("TESTBED_TLS_S3_SECRET_ACCESS_KEY")),
            ),
            ("flush.interval", "\"1s\"".to_owned()),
        ];
        owned.extend(extra.iter().cloned());
        owned
    }

    /// 同上，另配 `object_store.s3.ca_file` 为 CA-S。
    fn trusted(
        &self,
        endpoint: &str,
        extra: &[(&'static str, String)],
    ) -> Vec<(&'static str, String)> {
        let mut owned = self.config(endpoint, extra);
        owned.push(("object_store.s3.ca_file", quoted(&self.ca)));
        owned
    }

    fn serve(&self, owned: &[(&'static str, String)]) -> Command {
        self.f.serve(&kv(owned))
    }

    fn start(&self, owned: &[(&'static str, String)]) -> Server {
        Server::start(self.serve(owned))
    }

    /// 请求日志转发层，转发给台架的 TLS+KES MinIO。
    fn log(&self) -> S3Log {
        S3Log::start(
            &tb("TESTBED_TLS_S3_ENDPOINT"),
            &tb("TESTBED_TLS_CA_PEM"),
            &tb("TESTBED_TLS_SERVER_CERT_PEM"),
            &tb("TESTBED_TLS_SERVER_KEY_PEM"),
        )
    }

    /// 测试进程自己的客户端（台架 root 凭据、信任 CA-S），根前缀与被测进程相同。
    fn store(
        &self,
        endpoint: &str,
        sse: Option<amberlog_storage::store::ServerSideEncryption>,
    ) -> amberlog_storage::store::Store {
        use amberlog_storage::store::{Location, S3Options, Store};
        Store::open(
            &Location::S3 {
                bucket: tb("TESTBED_TLS_S3_BUCKET"),
                path: vec![self.path.clone()],
            },
            &S3Options {
                endpoint: Some(endpoint.to_owned()),
                region: "us-east-1".into(),
                access_key_id: Some(tb("TESTBED_TLS_S3_ACCESS_KEY_ID")),
                secret_access_key: Some(tb("TESTBED_TLS_S3_SECRET_ACCESS_KEY")),
                force_path_style: true,
                server_side_encryption: sse,
                ca_pem: Some(tb("TESTBED_TLS_CA_PEM").into_bytes()),
            },
            "amberlog",
        )
        .unwrap()
    }
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

/// 根前缀之下全部对象的键（相对根前缀）。
fn keys(store: &amberlog_storage::store::Store) -> Vec<String> {
    use futures::TryStreamExt;
    rt().block_on(async {
        let all: Vec<_> = store
            .inner
            .list(Some(&store.root))
            .try_collect()
            .await
            .unwrap();
        all.into_iter()
            .map(|m| {
                m.location
                    .as_ref()
                    .strip_prefix(&format!("{}/", store.root))
                    .unwrap()
                    .to_owned()
            })
            .collect()
    })
}

/// 摄取 10 条记录，等到含数据的快照（编号不小于 2）写成。
fn ingest_and_commit(s: &Server, store: &amberlog_storage::store::Store) {
    let body: Vec<String> = (0..10).map(|i| format!(r#"{{"k":{i}}}"#)).collect();
    common::api::ingest_ok(s, "app", &format!("[{}]", body.join(",")));
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let snapshots = keys(store)
            .into_iter()
            .filter_map(|k| {
                k.strip_prefix("default/app/")
                    .and_then(amberlog_storage::layout::parse_snapshot_path)
            })
            .filter(|id| id.0 >= 2)
            .count();
        if snapshots > 0 {
            return;
        }
        assert!(Instant::now() < deadline, "60 秒内没有写成含数据的快照");
        std::thread::sleep(Duration::from_millis(250));
    }
}

fn puts(log: &S3Log) -> Vec<Exchange> {
    log.exchanges()
        .into_iter()
        .filter(|e| e.op() == "PutObject")
        .collect()
}

const SSE: &str = "x-amz-server-side-encryption";
const SSE_KEY: &str = "x-amz-server-side-encryption-aws-kms-key-id";

#[test]
fn enc_https_endpoint_no_plaintext_event() {
    let t = TlsS3::new();
    let s = t.start(&t.trusted(&tb("TESTBED_TLS_S3_ENDPOINT"), &[]));
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    assert!(events(&s, "object_store_plaintext").is_empty());
}

#[test]
fn enc_untrusted_object_store_exit3() {
    let t = TlsS3::new();
    let r = run(&mut t.serve(&t.config(&tb("TESTBED_TLS_S3_ENDPOINT"), &[])));
    assert_eq!(r.code, 3, "{}", r.stderr);
    assert!(r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
}

#[test]
fn enc_ca_file_trusts_object_store() {
    let t = TlsS3::new();
    let endpoint = tb("TESTBED_TLS_S3_ENDPOINT");
    let s = t.start(&t.trusted(&endpoint, &[]));
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    ingest_and_commit(&s, &t.store(&endpoint, None));
    assert_eq!(common::api::search(&s, "SELECT k FROM app").len(), 10);
}

#[test]
fn enc_ca_file_appends_to_system_roots() {
    let t = TlsS3::new();
    let other = Pki::new(t.f.root()).ca("CA-X");
    let mut owned = t.config(&tb("TESTBED_TLS_S3_ENDPOINT"), &[]);
    owned.push(("object_store.s3.ca_file", quoted(&other.cert)));
    let mut cmd = t.serve(&owned);
    cmd.env("SSL_CERT_FILE", &t.ca);
    let s = Server::start(cmd);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
}

/// 第三次运行查验的证据包由同一夹具的 `file://` 目录上的写入实例摄取后导出，与 CA 文件无关。
#[test]
fn enc_ca_file_invalid_exit2() {
    let t = TlsS3::new();
    let x = t.f.root().join("x.pem");
    std::fs::write(&x, "not pem").unwrap();
    let with_ca = |ca: &Path| {
        let mut owned = t.config(&tb("TESTBED_TLS_S3_ENDPOINT"), &[]);
        owned.push(("object_store.s3.ca_file", quoted(ca)));
        owned
    };
    let check = |r: &Run, what: &str| {
        assert_eq!(r.code, 2, "{what}：{}", r.stderr);
        assert!(r.stderr.contains("object_store.s3.ca_file"), "{}", r.stderr);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
    };
    for ca in [Path::new("/nonexistent/ca.pem"), &x] {
        check(&run(&mut t.serve(&with_ca(ca))), &format!("serve {ca:?}"));
    }

    let mut w = t.f.start(&common::evidence::W);
    t.f.hot_data(&w, 10);
    common::store::stop(&mut w);
    let (p, _) = t.f.export_app(&common::evidence::W, "e.tar", &[], 0);
    let owned = with_ca(&x);
    let mut verify =
        t.f.evidence_cmd(&kv(&owned), "verify", &["--evidence", p.to_str().unwrap()]);
    check(&run(&mut verify), "evidence verify");
}

#[test]
fn enc_sse_s3_header() {
    let t = TlsS3::new();
    let log = t.log();
    let s = t.start(&t.trusted(
        &log.endpoint(),
        &[(
            "object_store.s3.server_side_encryption",
            "\"sse_s3\"".to_owned(),
        )],
    ));
    ingest_and_commit(&s, &t.store(&tb("TESTBED_TLS_S3_ENDPOINT"), None));
    let puts = puts(&log);
    assert!(!puts.is_empty());
    for p in &puts {
        assert_eq!(p.header(SSE), Some("AES256"), "{p:?}");
        assert_eq!(p.header(SSE_KEY), None, "{p:?}");
    }
}

#[test]
fn enc_sse_kms_header_and_key_id() {
    let key = tb("TESTBED_TLS_KMS_KEY");
    for key_id in [Some(key.clone()), None] {
        let t = TlsS3::new();
        let log = t.log();
        let mut extra = vec![(
            "object_store.s3.server_side_encryption",
            "\"sse_kms\"".to_owned(),
        )];
        if let Some(k) = &key_id {
            extra.push(("object_store.s3.sse_kms_key_id", format!("\"{k}\"")));
        }
        let mut s = t.start(&t.trusted(&log.endpoint(), &extra));
        ingest_and_commit(&s, &t.store(&tb("TESTBED_TLS_S3_ENDPOINT"), None));
        s.signal("TERM");
        assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
        let puts = puts(&log);
        assert!(!puts.is_empty());
        for p in &puts {
            assert_eq!(p.header(SSE), Some("aws:kms"), "{p:?}");
            assert_eq!(p.header(SSE_KEY), key_id.as_deref(), "{p:?}");
        }
    }
}

/// 系统写出的每类对象都已加密（REQ-AMB-ARCH-ENCRYPTION-V2 SSE-ALL-OBJECTS）：另配一条启用的告警规则，运行到至少一个
/// 计划时刻的评估记录对象写出（`_system/alerts/` 之下）再优雅停机。规则的 `webhook_url` 指向不接受连接的本机端口
/// （场景外的合法取值；投递结果不在本场景的断言之内）。
#[test]
fn enc_sse_all_object_kinds() {
    use common::alerting::{RuleToml, rules};
    let t = TlsS3::new();
    let log = t.log();
    let direct = tb("TESTBED_TLS_S3_ENDPOINT");
    let refusing = common::webhook::Refusing::new();
    let rule = rules(&[RuleToml::new("r1", &refusing.url("/hook")).without("window")]);
    let mut s = t.start(&t.trusted(
        &log.endpoint(),
        &[
            (
                "object_store.s3.server_side_encryption",
                "\"sse_s3\"".to_owned(),
            ),
            ("lifecycle.lake_watermark", "\"1h\"".to_owned()),
            ("alerts.rules", rule),
        ],
    ));
    let now = common::api::now_us();
    let hour = 3_600_000_000;
    let early = now - 3 * hour;
    let body = |ts: i64| -> String {
        let v: Vec<String> = (0..100)
            .map(|i| format!(r#"{{"_timestamp":{},"k":{i}}}"#, ts + i))
            .collect();
        format!("[{}]", v.join(","))
    };
    common::api::ingest_ok(&s, "app", &body(now));
    common::api::ingest_ok(&s, "app", &body(early));
    let store = t.store(&direct, None);
    let deadline = Instant::now() + Duration::from_secs(600);
    let (y, m, d, h) = time_parts((early / hour) * hour / 1_000_000);
    let label = format!("{y:04}{m:02}{d:02}{h:02}");
    loop {
        let ks = keys(&store);
        let lake = ks
            .iter()
            .any(|k| k == "_iceberg/default/app/metadata/v1.metadata.json");
        let alerts = ks.iter().any(|k| k.starts_with("_system/alerts/r1/"));
        if lake && alerts && hot_bucket_empty(&store, &label) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "10 分钟内该小时的桶没有入湖或没有写出评估记录对象：{ks:?}"
        );
        std::thread::sleep(Duration::from_secs(2));
    }
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));
    // 每个对象经转发层 HEAD 一次，看响应头
    let via_log = t.store(&log.endpoint(), None);
    let all = keys(&via_log);
    log.clear();
    rt().block_on(async {
        use object_store::ObjectStoreExt;
        for k in &all {
            let p = object_store::path::Path::from(format!("{}/{k}", via_log.root));
            via_log.inner.head(&p).await.unwrap();
        }
    });
    let heads: Vec<Exchange> = log
        .exchanges()
        .into_iter()
        .filter(|e| e.op() == "HeadObject")
        .collect();
    assert_eq!(heads.len(), all.len());
    for h in &heads {
        assert_eq!(h.response_header(SSE), Some("AES256"), "{h:?}");
    }
    for (what, ok) in [
        (
            "数据文件",
            all.iter()
                .any(|k| k.starts_with("default/app/data/") && k.ends_with(".parquet")),
        ),
        (
            "桶 manifest",
            all.iter()
                .any(|k| k.starts_with("default/app/manifest/buckets/")),
        ),
        (
            "快照",
            all.iter().any(|k| {
                k.strip_prefix("default/app/")
                    .and_then(amberlog_storage::layout::parse_snapshot_path)
                    .is_some()
            }),
        ),
        (
            "提示",
            all.iter().any(|k| k == "default/app/manifest/_current"),
        ),
        (
            "计数对象",
            all.iter().any(|k| k.starts_with("default/app/counters/")),
        ),
        (
            "探测对象",
            all.iter()
                .any(|k| k.starts_with("_system/") && !k.starts_with("_system/alerts/")),
        ),
        (
            "告警评估记录对象",
            all.iter().any(|k| k.starts_with("_system/alerts/r1/")),
        ),
        (
            "冷层数据文件",
            all.iter()
                .any(|k| k.starts_with("_iceberg/default/app/data/")),
        ),
        (
            "冷层元数据文件",
            all.iter()
                .any(|k| k == "_iceberg/default/app/metadata/v1.metadata.json"),
        ),
        (
            "冷层提示",
            all.iter()
                .any(|k| k == "_iceberg/default/app/metadata/version-hint.text"),
        ),
    ] {
        assert!(ok, "缺{what}：{all:?}");
    }
}

/// UTC 秒 → (年, 月, 日, 时)。
fn time_parts(secs: i64) -> (i64, i64, i64, i64) {
    let days = secs.div_euclid(86_400);
    let hour = secs.rem_euclid(86_400) / 3600;
    // Howard Hinnant 的 civil_from_days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d, hour)
}

/// 热层当前快照中该小时的桶已不含存活数据文件：读提示所指快照及其后的链尾，看该桶的桶 manifest 的 `files`。
fn hot_bucket_empty(store: &amberlog_storage::store::Store, label: &str) -> bool {
    use object_store::ObjectStoreExt;
    rt().block_on(async {
        let read = |rel: String| {
            let p = object_store::path::Path::from(format!("{}/default/app/{rel}", store.root));
            let inner = store.inner.clone();
            async move {
                let bytes = inner.get(&p).await.ok()?.bytes().await.ok()?;
                serde_json::from_slice::<Value>(&bytes).ok()
            }
        };
        let mut id: u64 = 1;
        let mut head = None;
        while let Some(s) = read(amberlog_storage::layout::snapshot_path(
            amberlog_storage::chain::SnapshotId(id),
        ))
        .await
        {
            head = Some(s);
            id += 1;
        }
        let Some(head) = head else { return false };
        let Some(b) = head["buckets"]
            .as_array()
            .and_then(|bs| bs.iter().find(|b| b["bucket"] == label))
        else {
            return true;
        };
        let Some(m) = read(b["manifest"].as_str().unwrap().to_owned()).await else {
            return false;
        };
        m["files"].as_array().is_some_and(Vec::is_empty)
    })
}

#[test]
fn enc_sse_multipart_create_carries_header() {
    use amberlog_storage::store::ServerSideEncryption;
    use object_store::{ObjectStoreExt, PutPayload};
    let t = TlsS3::new();
    let log = t.log();
    let store = t.store(&log.endpoint(), Some(ServerSideEncryption::SseS3));
    let path = object_store::path::Path::from(format!("{}/_system/multipart-probe", store.root));
    rt().block_on(async {
        let mut up = store.inner.put_multipart(&path).await.unwrap();
        for i in 0..2u8 {
            up.put_part(PutPayload::from(vec![i; 5 * 1024 * 1024]))
                .await
                .unwrap();
        }
        up.complete().await.unwrap();
        store.inner.head(&path).await.unwrap();
    });
    let ex = log.exchanges();
    let create: Vec<&Exchange> = ex
        .iter()
        .filter(|e| e.op() == "CreateMultipartUpload")
        .collect();
    assert_eq!(create.len(), 1, "{ex:?}");
    assert_eq!(create[0].header(SSE), Some("AES256"));
    let parts: Vec<&Exchange> = ex
        .iter()
        .filter(|e| matches!(e.op(), "UploadPart" | "CompleteMultipartUpload"))
        .collect();
    assert_eq!(parts.len(), 3, "{ex:?}");
    for p in parts {
        assert!(p.headers_with_prefix(SSE).is_empty(), "{p:?}");
    }
    let head = ex.iter().find(|e| e.op() == "HeadObject").unwrap();
    assert_eq!(head.response_header(SSE), Some("AES256"));
}

#[test]
fn enc_no_sse_config_no_header() {
    let t = TlsS3::new();
    let log = t.log();
    let mut cmd = t.serve(&t.trusted(&log.endpoint(), &[]));
    cmd.env("AWS_SERVER_SIDE_ENCRYPTION", "aws:kms");
    let s = Server::start(cmd);
    ingest_and_commit(&s, &t.store(&tb("TESTBED_TLS_S3_ENDPOINT"), None));
    let ex = log.exchanges();
    assert!(!ex.is_empty());
    for e in &ex {
        assert_eq!(e.header(SSE), None, "{e:?}");
        assert_eq!(e.header(SSE_KEY), None, "{e:?}");
    }
}

#[test]
fn enc_never_sends_sse_c() {
    let t = TlsS3::new();
    let log = t.log();
    let mut cmd = t.serve(&t.trusted(
        &log.endpoint(),
        &[(
            "object_store.s3.server_side_encryption",
            "\"sse_kms\"".to_owned(),
        )],
    ));
    // 合法的 256 位密钥的 base64
    cmd.env(
        "AWS_SSE_CUSTOMER_KEY_BASE64",
        "MDEyMzQ1Njc4OWFiY2RlZjAxMjM0NTY3ODlhYmNkZWY=",
    );
    let s = Server::start(cmd);
    ingest_and_commit(&s, &t.store(&tb("TESTBED_TLS_S3_ENDPOINT"), None));
    assert_eq!(common::api::search(&s, "SELECT k FROM app").len(), 10);
    for e in &log.exchanges() {
        assert!(
            e.headers_with_prefix("x-amz-server-side-encryption-customer-")
                .is_empty(),
            "{e:?}"
        );
    }
}

#[test]
fn enc_reads_carry_no_sse_header() {
    let t = TlsS3::new();
    let log = t.log();
    let s = t.start(&t.trusted(
        &log.endpoint(),
        &[(
            "object_store.s3.server_side_encryption",
            "\"sse_kms\"".to_owned(),
        )],
    ));
    ingest_and_commit(&s, &t.store(&tb("TESTBED_TLS_S3_ENDPOINT"), None));
    assert_eq!(common::api::search(&s, "SELECT k FROM app").len(), 10);
    let reads: Vec<Exchange> = log
        .exchanges()
        .into_iter()
        .filter(|e| matches!(e.op(), "GetObject" | "HeadObject" | "ListObjects"))
        .collect();
    assert!(!reads.is_empty());
    for e in &reads {
        assert!(e.headers_with_prefix(SSE).is_empty(), "{e:?}");
    }
}

#[test]
fn enc_kms_unavailable_exit3() {
    let t = TlsS3::new();
    let r = run(&mut t.serve(&t.trusted(
        &tb("TESTBED_TLS_S3_KMS_DOWN_ENDPOINT"),
        &[(
            "object_store.s3.server_side_encryption",
            "\"sse_kms\"".to_owned(),
        )],
    )));
    assert_eq!(r.code, 3, "{}", r.stderr);
    assert!(r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
}

/// 经台架的 http 端点分别运行 `serve` 与 `catalog`：各自告警恰一条、位于 starting 与 ready 之间。
#[test]
fn enc_object_store_plaintext_event() {
    let inst = common::s3::S3Instance::new();
    let endpoint = format!("http://{}", inst.relay.addr);
    let check = |s: &Server, form: &str| {
        assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200, "{form}");
        let order: Vec<String> = s
            .json_lines()
            .into_iter()
            .filter_map(|v| v["event"].as_str().map(str::to_owned))
            .filter(|e| ["starting", "object_store_plaintext", "ready"].contains(&e.as_str()))
            .collect();
        assert_eq!(
            order,
            ["starting", "object_store_plaintext", "ready"],
            "{form}"
        );
        let ev = events(s, "object_store_plaintext");
        assert_eq!(ev[0]["level"], "warn", "{form}");
        assert_eq!(ev[0]["endpoint"], endpoint, "{form}");
    };
    let extra = [("log.level", Some("\"error\""))];
    let serve = inst.start(&extra);
    check(&serve, "serve");
    let owned = inst.config(&extra);
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let catalog = Server::start(inst.fixture.catalog(&kv));
    check(&catalog, "catalog");
}

/// 只读目录模式用 `serve` 同一组入站设置（证书、TLS-ONLY、`ready` 的 `tls`），服务端加密设置被丢弃：台架 MinIO 旁没有
/// 密钥服务，配了 `sse_kms` 仍照常就绪。「stream `app` 已入湖」由同一前缀上的写入实例摄取此刻前第 3 小时的记录、
/// 等到冷层表的第一个元数据文件出现（至多 10 分钟）。
#[test]
fn enc_catalog_mode_uses_inbound_settings() {
    use object_store::ObjectStoreExt;
    let inst = common::s3::S3Instance::new();
    let w = inst.start(&[
        ("flush.interval", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ]);
    let start = common::api::now_us() / 3_600_000_000 * 3_600_000_000 - 3 * 3_600_000_000;
    let body: Vec<String> = (0..10)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"seq":{i}}}"#,
                start + 60_000_000 + i * 1000
            )
        })
        .collect();
    common::api::ingest_ok(&w, "app", &format!("[{}]", body.join(",")));
    let store = inst.store();
    let meta = object_store::path::Path::from(format!(
        "{}/_iceberg/default/app/metadata/v1.metadata.json",
        store.root
    ));
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let (_, laked) = common::api::poll(
        Duration::from_secs(600),
        || rt.block_on(store.inner.head(&meta)).is_ok(),
        |b| *b,
    );
    assert!(laked, "app 没有入湖：{:#?}", w.stderr_lines());

    let f = &inst.fixture;
    let c = certs(f);
    let mut owned = inst.config(&[(
        "object_store.s3.server_side_encryption",
        Some("\"sse_kms\""),
    )]);
    for (k, v) in with_tls(&c.c1) {
        owned.push((k, Some(v)));
    }
    let kv: Vec<Kv> = owned.iter().map(|(k, v)| (*k, v.as_deref())).collect();
    let s = Server::start(f.catalog(&kv));
    let client = TlsClient::new(&c.ca.cert, Versions::Both);
    assert_eq!(wait_readyz_tls(&client, s.addr, READY_WAIT), 200);
    assert_eq!(
        client
            .get(s.addr, "/iceberg/v1/config", &[("Authorization", BEARER)])
            .status,
        200
    );
    assert!(!is_http(&plaintext_reply(s.addr, "/healthz", NO_REPLY)));
    let ready = ready(&s);
    assert_eq!(ready["mode"], "catalog");
    assert_eq!(ready["tls"], true);
}

/// 离线子命令不读证书文件也不受明文规则约束：数据由夹具 `file://` 目录上的写入实例摄取后停止；导出用的配置把监听地址设为
/// 非回环、证书与私钥指向不存在的文件。
#[test]
fn enc_offline_ignores_inbound_settings() {
    let f = Fixture::new();
    let mut w = f.start(&common::evidence::W);
    f.hot_data(&w, 10);
    common::store::stop(&mut w);
    let mut overrides = common::evidence::W.to_vec();
    overrides.extend_from_slice(&[
        ("server.http_addr", Some("\"0.0.0.0:0\"")),
        ("server.tls.cert_file", Some("\"/nonexistent/cert.pem\"")),
        ("server.tls.key_file", Some("\"/nonexistent/key.pem\"")),
    ]);
    let out = f.tmp("e.tar");
    assert!(!out.exists());
    let r = f.export(
        &overrides,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    for s in [
        "server.tls.cert_file",
        "server.behind_tls_proxy",
        "\"event\":\"plaintext_listen\"",
    ] {
        assert!(!r.stderr.contains(s), "{s}：{}", r.stderr);
    }
}

/// 服务端加密对 `retire` 起作用：退役前的实例（不配服务端加密）直连加密台架摄取 10 条并写成含数据的快照后停止；`retire`
/// 配 `sse_s3`、经请求日志转发层连同一前缀，转发层只看到它一个进程的请求。退役记录经转发层 HEAD 一次看响应头。
#[test]
fn enc_retire_writes_with_sse() {
    use object_store::ObjectStoreExt;
    let t = TlsS3::new();
    let direct = tb("TESTBED_TLS_S3_ENDPOINT");
    let mut s = t.start(&t.trusted(&direct, &[]));
    ingest_and_commit(&s, &t.store(&direct, None));
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(30)), Some(Some(0)));

    let log = t.log();
    let owned = t.trusted(
        &log.endpoint(),
        &[(
            "object_store.s3.server_side_encryption",
            "\"sse_s3\"".to_owned(),
        )],
    );
    let r = common::retire::run(t.f.retire(&kv(&owned)));
    assert_eq!(r.code, Some(0), "{:#?}", r.stderr);
    let ex = log.exchanges();
    let puts: Vec<&Exchange> = ex.iter().filter(|e| e.op() == "PutObject").collect();
    assert!(
        puts.iter()
            .any(|p| p.target.contains("/_system/retirement.json")),
        "{ex:?}"
    );
    for p in ex
        .iter()
        .filter(|e| matches!(e.op(), "PutObject" | "CreateMultipartUpload"))
    {
        assert_eq!(p.header(SSE), Some("AES256"), "{p:?}");
    }

    log.clear();
    let via_log = t.store(&log.endpoint(), None);
    let record =
        object_store::path::Path::from(format!("{}/_system/retirement.json", via_log.root));
    rt().block_on(via_log.inner.head(&record)).unwrap();
    let heads: Vec<Exchange> = log
        .exchanges()
        .into_iter()
        .filter(|e| e.op() == "HeadObject")
        .collect();
    assert_eq!(heads.len(), 1, "{heads:?}");
    assert_eq!(heads[0].response_header(SSE), Some("AES256"), "{heads:?}");
}

// ---------------------------------------------------------------------------
// I 组：到 webhook 接收端的连接（REQ-AMB-ARCH-ENCRYPTION-V2）
// ---------------------------------------------------------------------------

/// 一条启用的告警规则（`alerts.rules` 的一个元素，规则的其余字段取缺省）。
fn alert_rule(name: &str, webhook_url: &str, extra: &str) -> String {
    format!(
        r#"{{ name = "{name}", interval = "1m", sql = "SELECT * FROM app", condition = {{ rows = "> 0" }}, webhook_url = "{webhook_url}"{extra} }}"#
    )
}

fn alert_rules(rules: &[String]) -> String {
    format!("[{}]", rules.join(", "))
}

/// 只读目录模式与离线子命令不读 `alerts.ca_file`、不输出 `webhook_plaintext`：数据由写入实例 W 摄取并入湖后停止，
/// 再以带 `alerts.ca_file`（不存在的文件）与一条 `http://` 规则的配置运行 `catalog` 与 `evidence export`。
#[test]
fn enc_webhook_settings_serve_only() {
    let f = Fixture::new();
    let mut w = f.start(&common::evidence::W);
    f.laked(&w);
    common::store::stop(&mut w);
    let rules = alert_rules(&[alert_rule("p1", "http://127.0.0.1:9/hook", "")]);
    let mut overrides = common::evidence::W.to_vec();
    overrides.extend_from_slice(&[
        ("alerts.ca_file", Some("\"/nonexistent/ca.pem\"")),
        ("alerts.rules", Some(rules.as_str())),
    ]);
    let check = |stderr: &str, what: &str| {
        for s in ["alerts.ca_file", "\"event\":\"webhook_plaintext\""] {
            assert!(!stderr.contains(s), "{what} 的 stderr 含 {s}：{stderr}");
        }
    };

    let c = Server::start(f.catalog(&overrides));
    assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);
    check(&c.stderr_lines().join("\n"), "catalog");

    let out = f.tmp("e.tar");
    let r = f.export(
        &overrides,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    check(&r.stderr, "evidence export");
}

/// `alerts.ca_file` 不存在、内容不是 PEM：`serve` 以退出码 2 退出，指出 `alerts.ca_file`，不输出 `starting`。
#[test]
fn enc_alerts_ca_file_invalid_exit2() {
    let f = Fixture::new();
    let x = f.root().join("x.pem");
    std::fs::write(&x, "not pem").unwrap();
    for ca in [Path::new("/nonexistent/ca.pem"), &x] {
        let value = quoted(ca);
        let r = run(&mut f.serve(&[("alerts.ca_file", Some(value.as_str()))]));
        assert_eq!(r.code, 2, "{ca:?}：{}", r.stderr);
        assert!(r.stderr.contains("alerts.ca_file"), "{}", r.stderr);
        assert!(!r.stderr.contains("\"event\":\"starting\""), "{}", r.stderr);
    }
}

/// `webhook_plaintext`：三条规则中只有启用的 `http://` 规则 `p1` 输出一次，`url` 去除查询串，位于 `starting` 与
/// `ready` 之间；`log.level` 为 `error` 时照样输出。
#[test]
fn enc_webhook_plaintext_event() {
    let f = Fixture::new();
    let rules = alert_rules(&[
        alert_rule("p1", "http://127.0.0.1:9/hook?key=abc", ""),
        alert_rule("p2", "http://127.0.0.1:9/other", ", enabled = false"),
        alert_rule("s1", "https://127.0.0.1:9/hook", ""),
    ]);
    let s = f.start(&[
        ("log.level", Some("\"error\"")),
        ("alerts.rules", Some(rules.as_str())),
    ]);
    assert_eq!(s.wait_readyz(200, READY_WAIT).status, 200);
    std::thread::sleep(Duration::from_secs(60));
    let ev = events(&s, "webhook_plaintext");
    assert_eq!(ev.len(), 1, "{ev:?}");
    assert_eq!(ev[0]["level"], "warn");
    assert_eq!(ev[0]["rule"], "p1");
    assert_eq!(ev[0]["url"], "http://127.0.0.1:9/hook");
    let order: Vec<String> = s
        .json_lines()
        .into_iter()
        .filter_map(|v| v["event"].as_str().map(str::to_owned))
        .filter(|e| ["starting", "webhook_plaintext", "ready"].contains(&e.as_str()))
        .collect();
    assert_eq!(order, ["starting", "webhook_plaintext", "ready"]);
}

/// I 组投递类场景的搭建：以 `clock` 启动，`alerts.rules` 只有 `r1`（`webhook_url` 为 `url`），另加 `extra`；
/// 「stream `app` 已有 `_timestamp` 为此刻前 30 秒的已提交数据」。返回被测进程与下一个计划时刻。
fn webhook_case(
    start: impl FnOnce(&[(&'static str, String)]) -> Server,
    clock: common::alerting::Clock,
    url: &str,
    extra: &[(&'static str, String)],
) -> (Server, i64) {
    use common::alerting::{MINUTE_US, RuleToml, SECOND_US, has_data, rules};
    let mut owned = vec![
        ("alerts.rules", rules(&[RuleToml::new("r1", url)])),
        ("flush.interval", "\"1s\"".to_owned()),
    ];
    owned.extend(extra.iter().cloned());
    let s = start(&owned);
    let ts = clock.now() - 30 * SECOND_US;
    has_data(
        &s,
        clock,
        "app",
        &[&format!(r#"{{"_timestamp":{ts},"k":1}}"#)],
    );
    let t = clock.next_tick(MINUTE_US);
    (s, t)
}

/// 以 `clock` 的偏移在夹具 `f` 上启动。
fn start_with(
    f: &Fixture,
    clock: common::alerting::Clock,
    owned: &[(&'static str, String)],
) -> Server {
    let mut cmd = f.serve(&kv(owned));
    clock.apply(&mut cmd);
    Server::start(cmd)
}

/// 接收端证书由 CA-W 签发、进程不信任 CA-W：每次尝试握手失败，按连接失败重试满 5 次后放弃；接收端收不到请求。
#[test]
fn enc_webhook_untrusted_cert_abandoned() {
    use common::alerting::{Clock, wait_evaluation};
    use common::webhook::Receiver;
    let f = Fixture::new();
    let pki = Pki::new(f.root());
    let ca_w = pki.ca("CA-W");
    let rx = Receiver::https(&pki.leaf(&ca_w, "receiver", KeyKind::P256));
    let clock = Clock::before_minute(Duration::from_secs(20));
    let (s, t) = webhook_case(|o| start_with(&f, clock, o), clock, &rx.url("/hook"), &[]);
    let row = wait_evaluation(&s, "r1", t);
    assert!(rx.requests().is_empty(), "{:#?}", rx.requests());
    assert!(rx.connections() >= 5, "应有 5 次握手失败的连接");
    assert_eq!(row["delivery"], "abandoned", "{row}");
    assert_eq!(row["attempts"], 5, "{row}");
    assert!(row.get("last_http_status").is_none(), "{row}");
}

/// 同上，另配 `alerts.ca_file` 为 CA-W：一次送达。
#[test]
fn enc_alerts_ca_file_trusts_receiver() {
    use common::alerting::{Clock, wait_evaluation};
    use common::webhook::Receiver;
    let f = Fixture::new();
    let pki = Pki::new(f.root());
    let ca_w = pki.ca("CA-W");
    let rx = Receiver::https(&pki.leaf(&ca_w, "receiver", KeyKind::P256));
    let clock = Clock::before_minute(Duration::from_secs(20));
    let (s, t) = webhook_case(
        |o| start_with(&f, clock, o),
        clock,
        &rx.url("/hook"),
        &[("alerts.ca_file", quoted(&ca_w.cert))],
    );
    let row = wait_evaluation(&s, "r1", t);
    let id = format!("r1-{t}");
    let got: Vec<_> = rx
        .requests()
        .into_iter()
        .filter(|r| r.json()["evaluation_id"] == id.as_str())
        .collect();
    assert_eq!(got.len(), 1, "{:#?}", rx.requests());
    assert!(got[0].tls);
    assert_eq!(row["delivery"], "delivered", "{row}");
}

/// `object_store.s3.ca_file` 含台架对象存储的 CA-S 与 CA-W、未配 `alerts.ca_file`：对象存储照常可用，webhook 不信任
/// CA-W，接收端收不到请求。
#[test]
fn enc_object_store_ca_not_for_webhook() {
    use common::alerting::{Clock, wait_evaluation};
    use common::webhook::Receiver;
    let t3 = TlsS3::new();
    let pki = Pki::new(t3.f.root());
    let ca_w = pki.ca("CA-W");
    let rx = Receiver::https(&pki.leaf(&ca_w, "receiver", KeyKind::P256));
    let both = t3.f.root().join("ca-s-and-w.pem");
    let mut pem = tb("TESTBED_TLS_CA_PEM");
    pem.push('\n');
    pem.push_str(&std::fs::read_to_string(&ca_w.cert).unwrap());
    std::fs::write(&both, pem).unwrap();
    let clock = Clock::before_minute(Duration::from_secs(20));
    let mut extra = t3.config(&tb("TESTBED_TLS_S3_ENDPOINT"), &[]);
    extra.push(("object_store.s3.ca_file", quoted(&both)));
    let (s, t) = webhook_case(
        |o| start_with(&t3.f, clock, o),
        clock,
        &rx.url("/hook"),
        &extra,
    );
    wait_evaluation(&s, "r1", t);
    assert!(rx.requests().is_empty(), "{:#?}", rx.requests());
}

/// `http://` 的接收端照常以明文收到请求。
#[test]
fn enc_webhook_http_allowed() {
    use common::alerting::{Clock, wait_evaluation};
    use common::webhook::Receiver;
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = Clock::before_minute(Duration::from_secs(20));
    let (s, t) = webhook_case(|o| start_with(&f, clock, o), clock, &rx.url("/hook"), &[]);
    wait_evaluation(&s, "r1", t);
    let id = format!("r1-{t}");
    let got: Vec<_> = rx
        .requests()
        .into_iter()
        .filter(|r| r.json()["evaluation_id"] == id.as_str())
        .collect();
    assert_eq!(got.len(), 1, "{:#?}", rx.requests());
    assert!(!got[0].tls);
}
