//! 加密场景（REQ-AMB-ARCH-ENCRYPTION）的测试设施：现场生成的 CA、证书与私钥，以及 TLS 客户端。
//!
//! - 证书与私钥由 `openssl` 命令行在测试运行时生成，写进临时目录，私钥不进仓库。
//!   PKCS#1 与 SEC1 编码由 `openssl pkey -traditional` 转出，经口令加密的私钥由 `openssl pkey -aes256` 转出。
//! - TLS 客户端基于 rustls（ring 后端，与被测进程同一实现）：可设信任的 CA、限定 TLS 版本、读出服务端出示的证书链、
//!   记下服务端有没有发 CertificateRequest（rustls 只在收到它时调用客户端证书解析器），连接可复用。
//! - TLS 1.1 的握手 rustls 发不出，另以手写的 TLS 1.1 ClientHello 字节验证服务端不接受。

use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use rustls::client::ResolvesClientCert;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, ServerName};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, ClientConnection, RootCertStore, SignatureScheme, StreamOwned};

use super::{Resp, parse_response};

/// 私钥的算法与编码。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyKind {
    /// ECDSA P-256，PKCS#8（缺省）。
    P256,
    Rsa2048Pkcs1,
    Rsa4096Pkcs8,
    P256Sec1,
    P384Pkcs8,
    Ed25519,
}

/// 一对证书与私钥文件。
#[derive(Debug, Clone)]
pub struct Pair {
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl Pair {
    /// 证书文件中的第一张证书（DER）。
    pub fn leaf_der(&self) -> Vec<u8> {
        pem_certs(&self.cert).remove(0)
    }
}

/// 证书文件中的全部证书（DER）。
pub fn pem_certs(path: &Path) -> Vec<Vec<u8>> {
    CertificateDer::pem_file_iter(path)
        .unwrap()
        .map(|c| c.unwrap().as_ref().to_vec())
        .collect()
}

/// 一个临时目录里的签发机构。
pub struct Pki {
    dir: PathBuf,
    n: std::cell::Cell<u32>,
}

impl Pki {
    pub fn new(dir: &Path) -> Pki {
        let dir = dir.join("pki");
        std::fs::create_dir_all(&dir).unwrap();
        Pki {
            dir,
            n: std::cell::Cell::new(0),
        }
    }

    fn path(&self, name: &str) -> PathBuf {
        self.dir.join(name)
    }

    fn fresh(&self, stem: &str) -> String {
        let n = self.n.get();
        self.n.set(n + 1);
        format!("{stem}-{n}")
    }

    /// 自签根 CA。
    pub fn ca(&self, cn: &str) -> Pair {
        let stem = self.fresh("ca");
        let key = self.genkey(&stem, KeyKind::P256);
        let cert = self.path(&format!("{stem}.crt"));
        openssl(&[
            "req",
            "-x509",
            "-new",
            "-key",
            s(&key),
            "-subj",
            &format!("/CN={cn}"),
            "-days",
            "30",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-addext",
            "keyUsage=critical,keyCertSign,cRLSign",
            "-out",
            s(&cert),
        ]);
        Pair { cert, key }
    }

    /// 由 `issuer` 签发的中间 CA。
    pub fn intermediate(&self, issuer: &Pair, cn: &str) -> Pair {
        let stem = self.fresh("int");
        let key = self.genkey(&stem, KeyKind::P256);
        let cert = self.sign(
            issuer,
            &key,
            &stem,
            cn,
            "basicConstraints=critical,CA:TRUE\nkeyUsage=critical,keyCertSign,cRLSign\n",
        );
        Pair { cert, key }
    }

    /// 由 `issuer` 签发、主题备用名含 `127.0.0.1` 的服务端证书与 `kind` 私钥。
    pub fn leaf(&self, issuer: &Pair, cn: &str, kind: KeyKind) -> Pair {
        let stem = self.fresh("leaf");
        let key = self.genkey(&stem, kind);
        let cert = self.sign(
            issuer,
            &key,
            &stem,
            cn,
            "basicConstraints=CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\n\
             extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1\n",
        );
        Pair { cert, key }
    }

    /// 把 `key` 转为经口令加密的 PEM 私钥。
    pub fn encrypted(&self, key: &Path) -> PathBuf {
        let out = self.path(&format!("{}.pem", self.fresh("enc")));
        openssl(&[
            "pkey",
            "-in",
            s(key),
            "-aes256",
            "-passout",
            "pass:secret",
            "-out",
            s(&out),
        ]);
        out
    }

    fn genkey(&self, stem: &str, kind: KeyKind) -> PathBuf {
        let pkcs8 = self.path(&format!("{stem}.pkcs8.key"));
        let alg: &[&str] = match kind {
            KeyKind::P256 | KeyKind::P256Sec1 => {
                &["-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-256"]
            }
            KeyKind::P384Pkcs8 => &["-algorithm", "EC", "-pkeyopt", "ec_paramgen_curve:P-384"],
            KeyKind::Rsa2048Pkcs1 => &["-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:2048"],
            KeyKind::Rsa4096Pkcs8 => &["-algorithm", "RSA", "-pkeyopt", "rsa_keygen_bits:4096"],
            KeyKind::Ed25519 => &["-algorithm", "ED25519"],
        };
        let mut args = vec!["genpkey"];
        args.extend_from_slice(alg);
        args.extend_from_slice(&["-out", s(&pkcs8)]);
        openssl(&args);
        if !matches!(kind, KeyKind::Rsa2048Pkcs1 | KeyKind::P256Sec1) {
            return pkcs8;
        }
        // PKCS#1（RSA）与 SEC1（EC）是 openssl 的「传统」编码
        let traditional = self.path(&format!("{stem}.trad.key"));
        openssl(&[
            "pkey",
            "-in",
            s(&pkcs8),
            "-traditional",
            "-out",
            s(&traditional),
        ]);
        let text = std::fs::read_to_string(&traditional).unwrap();
        let want = match kind {
            KeyKind::Rsa2048Pkcs1 => "-----BEGIN RSA PRIVATE KEY-----",
            _ => "-----BEGIN EC PRIVATE KEY-----",
        };
        assert!(text.contains(want), "{kind:?}：{text}");
        traditional
    }

    fn sign(&self, issuer: &Pair, key: &Path, stem: &str, cn: &str, ext: &str) -> PathBuf {
        let csr = self.path(&format!("{stem}.csr"));
        let extfile = self.path(&format!("{stem}.ext"));
        let cert = self.path(&format!("{stem}.crt"));
        std::fs::write(&extfile, ext).unwrap();
        openssl(&[
            "req",
            "-new",
            "-key",
            s(key),
            "-subj",
            &format!("/CN={cn}"),
            "-out",
            s(&csr),
        ]);
        let serial = format!("{}", self.n.get() + 1000);
        openssl(&[
            "x509",
            "-req",
            "-in",
            s(&csr),
            "-CA",
            s(&issuer.cert),
            "-CAkey",
            s(&issuer.key),
            "-set_serial",
            &serial,
            "-days",
            "30",
            "-extfile",
            s(&extfile),
            "-out",
            s(&cert),
        ]);
        cert
    }
}

fn s(p: &Path) -> &str {
    p.to_str().unwrap()
}

fn openssl(args: &[&str]) {
    let out = Command::new("openssl")
        .args(args)
        .output()
        .expect("运行 openssl（加密场景用它现场生成证书）");
    assert!(
        out.status.success(),
        "openssl {args:?} 失败：{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

/// 记下服务端有没有请求客户端证书；本身不出示证书。
#[derive(Debug, Default)]
struct AskedForCert(AtomicBool);

impl ResolvesClientCert for AskedForCert {
    fn resolve(&self, _: &[&[u8]], _: &[SignatureScheme]) -> Option<Arc<CertifiedKey>> {
        self.0.store(true, Ordering::SeqCst);
        None
    }

    fn has_certs(&self) -> bool {
        false
    }
}

/// TLS 版本的限定。
#[derive(Debug, Clone, Copy)]
pub enum Versions {
    Both,
    Only12,
    Only13,
}

/// 信任给定 CA 的 TLS 客户端。
pub struct TlsClient {
    config: Arc<ClientConfig>,
    asked: Arc<AskedForCert>,
}

impl TlsClient {
    pub fn new(ca: &Path, versions: Versions) -> TlsClient {
        let mut roots = RootCertStore::empty();
        for c in CertificateDer::pem_file_iter(ca).unwrap() {
            roots.add(c.unwrap()).unwrap();
        }
        let asked = Arc::new(AskedForCert::default());
        let v: &[&rustls::SupportedProtocolVersion] = match versions {
            Versions::Both => &[&rustls::version::TLS13, &rustls::version::TLS12],
            Versions::Only12 => &[&rustls::version::TLS12],
            Versions::Only13 => &[&rustls::version::TLS13],
        };
        let config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(v)
                .unwrap()
                .with_root_certificates(roots)
                .with_client_cert_resolver(Arc::clone(&asked) as Arc<dyn ResolvesClientCert>);
        TlsClient {
            config: Arc::new(config),
            asked,
        }
    }

    /// 服务端在此前的某次握手中是否发过 CertificateRequest。
    pub fn asked_for_client_cert(&self) -> bool {
        self.asked.0.load(Ordering::SeqCst)
    }

    /// 建立连接并完成握手；握手失败返回错误。
    pub fn connect(&self, addr: SocketAddr) -> io::Result<TlsConn> {
        let tcp = TcpStream::connect(addr)?;
        tcp.set_read_timeout(Some(Duration::from_secs(30)))?;
        let name = ServerName::IpAddress(addr.ip().into());
        let conn = ClientConnection::new(Arc::clone(&self.config), name)
            .map_err(|e| io::Error::other(e.to_string()))?;
        let mut stream = StreamOwned::new(conn, tcp);
        while stream.conn.is_handshaking() {
            stream.conn.complete_io(&mut stream.sock)?;
        }
        Ok(TlsConn { stream })
    }

    /// 新建连接发一个请求。
    pub fn get(&self, addr: SocketAddr, path: &str, headers: &[(&str, &str)]) -> Resp {
        self.connect(addr)
            .unwrap_or_else(|e| panic!("TLS 握手失败：{e}"))
            .request("GET", path, headers, b"")
    }
}

/// 一条已完成握手的 TLS 连接（可复用）。
pub struct TlsConn {
    stream: StreamOwned<ClientConnection, TcpStream>,
}

impl TlsConn {
    /// 服务端出示的证书链（DER，依次）。
    pub fn peer_chain(&self) -> Vec<Vec<u8>> {
        self.stream
            .conn
            .peer_certificates()
            .unwrap_or_default()
            .iter()
            .map(|c| c.as_ref().to_vec())
            .collect()
    }

    pub fn protocol(&self) -> Option<rustls::ProtocolVersion> {
        self.stream.conn.protocol_version()
    }

    /// 在本连接上发一个保持连接的 HTTP/1.1 请求并读完响应。
    pub fn request(
        &mut self,
        method: &str,
        path: &str,
        headers: &[(&str, &str)],
        body: &[u8],
    ) -> Resp {
        let mut req = format!("{method} {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n");
        for (k, v) in headers {
            req.push_str(&format!("{k}: {v}\r\n"));
        }
        if !body.is_empty() || method == "POST" {
            req.push_str(&format!("Content-Length: {}\r\n", body.len()));
        }
        req.push_str("\r\n");
        self.stream.write_all(req.as_bytes()).unwrap();
        self.stream.write_all(body).unwrap();
        self.stream.flush().unwrap();
        let raw = read_response(&mut self.stream).unwrap_or_else(|e| panic!("读响应失败：{e}"));
        parse_response(&raw)
    }
}

/// 读到一个完整的 HTTP/1.1 响应（按 `Content-Length` 或分块结尾）为止。
fn read_response(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let mut raw = Vec::new();
    let mut buf = [0u8; 4096];
    loop {
        if let Some(end) = complete(&raw) {
            raw.truncate(end);
            return Ok(raw);
        }
        let n = r.read(&mut buf)?;
        if n == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "连接已关闭"));
        }
        raw.extend_from_slice(&buf[..n]);
    }
}

fn complete(raw: &[u8]) -> Option<usize> {
    let head_end = raw.windows(4).position(|w| w == b"\r\n\r\n")? + 4;
    let head = String::from_utf8_lossy(&raw[..head_end]).to_ascii_lowercase();
    if let Some(len) = head
        .lines()
        .find_map(|l| l.strip_prefix("content-length:"))
        .and_then(|v| v.trim().parse::<usize>().ok())
    {
        return (raw.len() >= head_end + len).then_some(head_end + len);
    }
    if head.contains("transfer-encoding: chunked") {
        let body = &raw[head_end..];
        return body
            .windows(5)
            .position(|w| w == b"0\r\n\r\n")
            .map(|p| head_end + p + 5);
    }
    Some(head_end)
}

/// 以明文向 `addr` 发 `GET <path>`，`limit` 内读到的字节（连接被关闭或超时即止）。
pub fn plaintext_reply(addr: SocketAddr, path: &str, limit: Duration) -> Vec<u8> {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\n\r\n");
    let _ = s.write_all(req.as_bytes());
    read_for(&mut s, limit)
}

/// 以手写的 TLS 1.1 ClientHello（不带 supported_versions 扩展）向 `addr` 发起握手，`limit` 内读到的字节。
pub fn tls11_hello_reply(addr: SocketAddr, limit: Duration) -> Vec<u8> {
    let mut body = vec![0x03, 0x02]; // client_version TLS 1.1
    body.extend_from_slice(&[0x5a; 32]); // random
    body.push(0); // session_id
    body.extend_from_slice(&[0x00, 0x04, 0xc0, 0x13, 0x00, 0x2f]); // 两个 TLS 1.1 可用的套件
    body.extend_from_slice(&[0x01, 0x00]); // 只有 null 压缩
    let mut hs = vec![0x01, 0x00];
    hs.extend_from_slice(&(body.len() as u16).to_be_bytes());
    hs.extend_from_slice(&body);
    let mut record = vec![0x16, 0x03, 0x02];
    record.extend_from_slice(&(hs.len() as u16).to_be_bytes());
    record.extend_from_slice(&hs);
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_millis(200)))
        .unwrap();
    s.write_all(&record).unwrap();
    read_for(&mut s, limit)
}

fn read_for(s: &mut TcpStream, limit: Duration) -> Vec<u8> {
    let deadline = Instant::now() + limit;
    let mut got = Vec::new();
    let mut buf = [0u8; 1024];
    while Instant::now() < deadline {
        match s.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => got.extend_from_slice(&buf[..n]),
            Err(e)
                if matches!(
                    e.kind(),
                    io::ErrorKind::WouldBlock | io::ErrorKind::TimedOut
                ) => {}
            Err(_) => break,
        }
    }
    got
}

/// 收到的字节里有没有 HTTP 响应。
pub fn is_http(bytes: &[u8]) -> bool {
    bytes.starts_with(b"HTTP/")
}
