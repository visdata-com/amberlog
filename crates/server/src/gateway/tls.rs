//! 入站 TLS（REQ-AMB-ARCH-ENCRYPTION，入站 TLS 与证书重读归接入面；选型见 ADR-0047 第 9 条）。
//!
//! - 证书与私钥在配置一步读入校验（[`Inbound::load`]），不可用即以退出码 2 退出（CERT-FILE-INVALID）。
//! - 只接受 TLS 1.2 与 1.3，不请求客户端证书，出示 `server.tls.cert_file` 中的全部证书（TLS-VERSION、TLS-NO-CLIENT-CERT、
//!   TLS-CHAIN）。
//! - 握手放在每个连接自己的任务里（[`TlsListener`]）：axum 的接收循环逐个等 `accept` 返回，握手若在 `accept` 里做，一个停滞的
//!   握手会挡住其他连接（TLS-STALL-ISOLATED）。握手未完成的连接不交给 HTTP 层，得不到 HTTP 响应（TLS-HANDSHAKE-FAIL）。
//! - 证书重读（[`Inbound::spawn_reload`]）：开始监听之后每 60 秒按内容读一次两个文件；有效的新内容原子替换当前证书，只影响
//!   此后的新连接（RELOAD-APPLY、RELOAD-EXISTING）；无效时继续用旧证书并输出 `tls_reload_failed`，同一份无效内容只输出一次
//!   （RELOAD-INVALID、EVT-TLS-RELOAD-FAILED）。不做会话恢复，每个新连接都完整握手、出示当时的证书。

use std::io;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use rustls::server::{ClientHello, ResolvesServerCert};
use rustls::sign::CertifiedKey;
use rustls::{ServerConfig, version};
use serde_json::Value;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_rustls::TlsAcceptor;
use tokio_rustls::server::TlsStream;

use crate::config::TlsFiles;
use crate::logging::{self, Level};

/// 证书重读的周期（块要求不超过 60 秒；不做配置项）。
const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// 一个连接完成握手的时限；超时即关闭该连接。只约束这一个连接，不影响其他连接。
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(30);

/// 一个文件的读取结果：内容，或「不存在」「不可读」两种状态之一。证书重读按它判定是否变化与是否同一份无效内容。
#[derive(Debug, Clone, PartialEq, Eq)]
enum FileState {
    Content(Vec<u8>),
    Missing,
    Unreadable,
}

fn read_state(path: &Path) -> FileState {
    match std::fs::read(path) {
        Ok(bytes) => FileState::Content(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => FileState::Missing,
        Err(_) => FileState::Unreadable,
    }
}

/// 证书与私钥不可用的一项原因：所指的配置键与说明。
pub type CertProblem = (&'static str, String);

const CERT_KEY: &str = "server.tls.cert_file";
const KEY_KEY: &str = "server.tls.key_file";

fn unreadable(state: &FileState) -> Option<&'static str> {
    match state {
        FileState::Content(_) => None,
        FileState::Missing => Some("file does not exist"),
        FileState::Unreadable => Some("file is not readable"),
    }
}

/// 由两个文件的读取结果得出可用于握手的证书链与私钥；不可用时给出全部原因（CERT-FILE-INVALID）。
fn certified(cert: &FileState, key: &FileState) -> Result<Arc<CertifiedKey>, Vec<CertProblem>> {
    let mut problems = Vec::new();
    let chain = match cert {
        FileState::Content(bytes) => {
            match CertificateDer::pem_slice_iter(bytes).collect::<Result<Vec<_>, _>>() {
                Ok(chain) if !chain.is_empty() => Some(chain),
                _ => {
                    problems.push((
                        CERT_KEY,
                        "file contains no PEM-encoded certificate".to_owned(),
                    ));
                    None
                }
            }
        }
        other => {
            problems.push((CERT_KEY, unreadable(other).unwrap_or_default().to_owned()));
            None
        }
    };
    let signing = match key {
        FileState::Content(bytes) => match PrivateKeyDer::from_pem_slice(bytes) {
            Ok(der) => match provider().key_provider.load_private_key(der) {
                Ok(signing) => Some(signing),
                Err(e) => {
                    problems.push((
                        KEY_KEY,
                        format!("private key cannot be used for a TLS handshake: {e}"),
                    ));
                    None
                }
            },
            Err(_) => {
                problems.push((
                    KEY_KEY,
                    "file contains no unencrypted PEM-encoded PKCS#8, PKCS#1 or SEC1 private key"
                        .to_owned(),
                ));
                None
            }
        },
        other => {
            problems.push((KEY_KEY, unreadable(other).unwrap_or_default().to_owned()));
            None
        }
    };
    let (Some(chain), Some(signing)) = (chain, signing) else {
        return Err(problems);
    };
    let certified = CertifiedKey::new(chain, signing);
    // ring 后端的三类私钥都能给出公钥，「无法判定」不会出现；判不出也按不匹配处置
    if certified.keys_match().is_err() {
        return Err(vec![(
            KEY_KEY,
            format!("private key does not match the first certificate in {CERT_KEY}"),
        )]);
    }
    Ok(Arc::new(certified))
}

fn provider() -> rustls::crypto::CryptoProvider {
    rustls::crypto::ring::default_provider()
}

/// 当前所用的证书与私钥：新连接握手时取一次，重读成功时整体替换。
#[derive(Debug)]
struct CertSlot(RwLock<Arc<CertifiedKey>>);

impl ResolvesServerCert for CertSlot {
    fn resolve(&self, _: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(Arc::clone(&self.0.read().unwrap()))
    }
}

/// 入站 TLS 的设置：配置一步读入并校验过的证书与私钥。
pub struct Inbound {
    files: TlsFiles,
    slot: Arc<CertSlot>,
    /// 当前所用的证书与私钥对应的两个文件的读取结果。
    current: (FileState, FileState),
    config: Arc<ServerConfig>,
}

impl Inbound {
    /// 读入并校验证书与私钥；不可用时返回全部原因（每项指出所涉的配置键）。
    pub fn load(files: &TlsFiles) -> Result<Inbound, Vec<CertProblem>> {
        let current = (read_state(&files.cert_path), read_state(&files.key_path));
        let key = certified(&current.0, &current.1)?;
        let slot = Arc::new(CertSlot(RwLock::new(key)));
        let mut config = ServerConfig::builder_with_provider(Arc::new(provider()))
            .with_protocol_versions(&[&version::TLS13, &version::TLS12])
            .map_err(|e| vec![(CERT_KEY, format!("cannot set up TLS: {e}"))])?
            .with_no_client_auth()
            .with_cert_resolver(Arc::clone(&slot) as Arc<dyn ResolvesServerCert>);
        // 不做会话恢复：恢复的握手不出示证书，客户端会一直沿用旧会话认下的证书，证书重读后新连接就不是「出示新证书」
        // （RELOAD-APPLY）。每个新连接都完整握手
        config.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        config.send_tls13_tickets = 0;
        Ok(Inbound {
            files: files.clone(),
            slot,
            current,
            config: Arc::new(config),
        })
    }

    /// 在已建立的监听上只以 TLS 提供服务。接收任务随返回的监听一同结束。
    pub fn listener(&self, tcp: TcpListener) -> io::Result<TlsListener> {
        let local = tcp.local_addr()?;
        let (tx, conns) = mpsc::channel(64);
        let acceptor = TlsAcceptor::from(Arc::clone(&self.config));
        let accepting = tokio::spawn(accept_loop(tcp, acceptor, tx));
        Ok(TlsListener {
            conns,
            local,
            accepting,
        })
    }

    /// 启动证书重读：开始监听之后调用。
    pub fn spawn_reload(&self) -> JoinHandle<()> {
        let files = self.files.clone();
        let slot = Arc::clone(&self.slot);
        let mut current = self.current.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(RELOAD_INTERVAL);
            tick.tick().await;
            // 上一次输出 `tls_reload_failed` 时两个文件的读取结果；读到有效内容即清空
            let mut reported: Option<(FileState, FileState)> = None;
            loop {
                tick.tick().await;
                // 连读两遍、两遍相同才判定：两个文件正被替换（例如挂载卷整体切换目录）时，一遍里可能读到新旧混合的一对，
                // 这一轮不判定，下一轮再读
                let now = (read_state(&files.cert_path), read_state(&files.key_path));
                if now != (read_state(&files.cert_path), read_state(&files.key_path)) {
                    continue;
                }
                if now == current {
                    reported = None;
                    continue;
                }
                match certified(&now.0, &now.1) {
                    Ok(key) => {
                        *slot.0.write().unwrap() = key;
                        current = now;
                        reported = None;
                    }
                    Err(_) if reported.as_ref() == Some(&now) => {}
                    Err(problems) => {
                        let why: Vec<String> =
                            problems.iter().map(|(k, m)| format!("{k}: {m}")).collect();
                        logging::event(
                            Level::Error,
                            "tls_reload_failed",
                            &format!(
                                "the TLS certificate or private key changed but is not usable; still serving the previous certificate ({})",
                                why.join("; ")
                            ),
                            &[
                                ("cert_file", Value::from(files.cert_file.clone())),
                                ("key_file", Value::from(files.key_file.clone())),
                            ],
                        );
                        reported = Some(now);
                    }
                }
            }
        })
    }
}

/// 接收 TCP 连接，每个连接在自己的任务里握手，完成握手的交给 [`TlsListener::accept`]。
async fn accept_loop(
    tcp: TcpListener,
    acceptor: TlsAcceptor,
    tx: mpsc::Sender<(TlsStream<TcpStream>, SocketAddr)>,
) {
    loop {
        match tcp.accept().await {
            Ok((stream, addr)) => {
                let acceptor = acceptor.clone();
                let tx = tx.clone();
                tokio::spawn(async move {
                    if let Ok(Ok(tls)) =
                        tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await
                    {
                        let _ = tx.send((tls, addr)).await;
                    }
                });
            }
            // 与 axum 自带的 TCP 监听同一处置：单个连接的错误略过，其他错误（如文件描述符耗尽）稍等再接
            Err(e) if is_connection_error(&e) => {}
            Err(_) => tokio::time::sleep(Duration::from_secs(1)).await,
        }
    }
}

fn is_connection_error(e: &io::Error) -> bool {
    matches!(
        e.kind(),
        io::ErrorKind::ConnectionRefused
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
    )
}

/// 只交出已完成 TLS 握手的连接的监听（供 `axum::serve`）。
pub struct TlsListener {
    conns: mpsc::Receiver<(TlsStream<TcpStream>, SocketAddr)>,
    local: SocketAddr,
    accepting: JoinHandle<()>,
}

impl Drop for TlsListener {
    fn drop(&mut self) {
        self.accepting.abort();
    }
}

impl axum::serve::Listener for TlsListener {
    type Io = TlsStream<TcpStream>;
    type Addr = SocketAddr;

    async fn accept(&mut self) -> (Self::Io, Self::Addr) {
        match self.conns.recv().await {
            Some(conn) => conn,
            // 接收任务只随本监听结束；这里不会走到，走到也只是不再交出连接
            None => std::future::pending().await,
        }
    }

    fn local_addr(&self) -> io::Result<Self::Addr> {
        Ok(self.local)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unusable_files_name_their_keys() {
        let subjects = |cert: FileState, key: FileState| -> Vec<&'static str> {
            certified(&cert, &key)
                .unwrap_err()
                .into_iter()
                .map(|(k, _)| k)
                .collect()
        };
        let not_pem = FileState::Content(b"not pem".to_vec());
        assert_eq!(
            subjects(FileState::Missing, not_pem.clone()),
            vec![CERT_KEY, KEY_KEY]
        );
        assert_eq!(
            subjects(not_pem.clone(), FileState::Unreadable),
            vec![CERT_KEY, KEY_KEY]
        );
        let encrypted = FileState::Content(
            b"-----BEGIN ENCRYPTED PRIVATE KEY-----\nMIIBAA==\n-----END ENCRYPTED PRIVATE KEY-----\n"
                .to_vec(),
        );
        assert_eq!(subjects(not_pem, encrypted), vec![CERT_KEY, KEY_KEY]);
    }
}
