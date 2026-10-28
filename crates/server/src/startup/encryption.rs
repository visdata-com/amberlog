//! 生命周期编排里与加密有关的两段（REQ-AMB-ARCH-ENCRYPTION）：
//!
//! - [`prepare`] 属配置一步（CONFIG-STEP）：在 `starting` 之前、`wal.dir` 与对象存储的校验之前，按形态读入并校验证书与私钥、
//!   判定明文规则、读入并校验 CA 文件；失败与其他配置错误一样以退出码 2 退出。
//! - [`announce`] 在 `starting` 之后、向对象存储发出第一个请求之前输出 `plaintext_listen` 与 `object_store_plaintext`，
//!   以及（只在 `serve` 下）每条 `http://` 的启用告警规则一条 `webhook_plaintext`（REQ-AMB-ARCH-ENCRYPTION-V2 I 组）。
//!
//! 形态的取舍：入站设置与明文规则只对监听的形态（`serve`、`catalog`）起作用；对象存储的 CA 文件与 `http://` 端点的告警对全部形态起作用；
//! 服务端加密只对写对象的形态（`serve`、`retire`）起作用，其余形态照常校验取值后丢弃（SCOPE-*）。`file://` 存储下
//! 出站与加密设置都不起作用（SCOPE-FILE）。webhook 出站设置（`alerts.ca_file` 与 `webhook_plaintext`）只对 `serve`
//! 起作用，其余形态不打开 `alerts.ca_file` 所指的文件（REQ-AMB-ARCH-ENCRYPTION-V2）。

use std::net::{IpAddr, SocketAddr};

use amberlog_storage::store::Location;
use serde_json::Value;

use crate::config::{Config, ConfigError};
use crate::form::Form;
use crate::gateway::tls::Inbound;
use crate::logging::{self, Level};

/// 配置一步的加密校验。返回监听形态在配置了证书时的入站 TLS 设置。
pub(super) fn prepare(form: Form, cfg: &mut Config) -> Result<Option<Inbound>, Vec<ConfigError>> {
    let mut errors = Vec::new();
    let mut inbound = None;
    if form.listens() {
        match &cfg.tls {
            Some(files) => match Inbound::load(files) {
                Ok(i) => inbound = Some(i),
                Err(problems) => {
                    errors.extend(problems.into_iter().map(|(subject, message)| ConfigError {
                        subject: subject.to_owned(),
                        message,
                    }))
                }
            },
            None if !is_loopback(cfg.http_addr) && !cfg.behind_tls_proxy => {
                errors.push(ConfigError {
                    subject: "server.http_addr".to_owned(),
                    message: "is not a loopback address and no certificate is configured: set server.tls.cert_file and \
                              server.tls.key_file, or set server.behind_tls_proxy = true when a reverse proxy in front \
                              terminates TLS"
                        .to_owned(),
                });
            }
            None => {}
        }
    }
    if matches!(cfg.location, Location::S3 { .. }) {
        if let Some(ca) = &cfg.ca_file {
            match read_ca(&ca.path) {
                Ok(pem) => cfg.s3.ca_pem = Some(pem),
                Err(message) => errors.push(ConfigError {
                    subject: "object_store.s3.ca_file".to_owned(),
                    message,
                }),
            }
        }
    } else {
        cfg.s3.server_side_encryption = None;
    }
    if !form.writes_objects() {
        cfg.s3.server_side_encryption = None;
    }
    // webhook 出站设置：只在写入实例下读，不论有没有 https:// 的规则
    if form == Form::Serve
        && let Some(ca) = &cfg.alerts.ca_file
    {
        match read_ca(&ca.path) {
            Ok(pem) => cfg.alerts.ca_pem = Some(pem),
            Err(message) => errors.push(ConfigError {
                subject: "alerts.ca_file".to_owned(),
                message,
            }),
        }
    }
    if errors.is_empty() {
        Ok(inbound)
    } else {
        Err(errors)
    }
}

/// `starting` 之后、向对象存储发出第一个请求之前的明文告警：EVT-PLAINTEXT-LISTEN、EVT-OS-PLAINTEXT，以及只在 `serve`
/// 下的 `webhook_plaintext`（REQ-AMB-ARCH-ENCRYPTION-V2 I 组）。
pub(super) fn announce(form: Form, cfg: &Config) {
    if form.listens() && cfg.tls.is_none() && !is_loopback(cfg.http_addr) && cfg.behind_tls_proxy {
        logging::event(
            Level::Warn,
            "plaintext_listen",
            "listening without TLS on a non-loopback address because server.behind_tls_proxy is true",
            &[("http_addr", Value::from(cfg.http_addr_text.clone()))],
        );
    }
    if matches!(cfg.location, Location::S3 { .. })
        && let Some(endpoint) = cfg.s3.endpoint.as_deref()
        && endpoint.starts_with("http://")
    {
        logging::event(
            Level::Warn,
            "object_store_plaintext",
            "the object store endpoint is http://: requests and data travel unencrypted",
            &[("endpoint", Value::from(shown_endpoint(endpoint)))],
        );
    }
    if form == Form::Serve {
        // 启用的规则（含启用的内置规则）各至多一条；启用与否已在配置一步滤过
        for rule in &cfg.alerts.rules {
            let url = &rule.webhook.url;
            if !url.is_https() {
                logging::event(
                    Level::Warn,
                    "webhook_plaintext",
                    "the alert rule's webhook_url is http://: notifications travel unencrypted",
                    &[
                        ("rule", Value::from(rule.name.as_str())),
                        ("url", Value::from(url.shown())),
                    ],
                );
            }
        }
    }
}

/// 回环地址：IPv4 `127.0.0.0/8` 或 IPv6 `::1`；IPv4 映射地址不算（TERM-LOOPBACK）。
fn is_loopback(addr: SocketAddr) -> bool {
    match addr.ip() {
        IpAddr::V4(a) => a.octets()[0] == 127,
        IpAddr::V6(a) => a == std::net::Ipv6Addr::LOCALHOST,
    }
}

/// 读入 CA 文件：须含至少一张 PEM 证书，且每张都能解析（CA-FILE-INVALID）。
fn read_ca(path: &std::path::Path) -> Result<Vec<u8>, String> {
    use rustls::pki_types::CertificateDer;
    use rustls::pki_types::pem::PemObject;

    let pem = std::fs::read(path).map_err(|e| format!("cannot read {}: {e}", path.display()))?;
    let certs = CertificateDer::pem_slice_iter(&pem)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| format!("cannot parse the PEM content: {e}"))?;
    if certs.is_empty() {
        return Err("file contains no PEM-encoded certificate".to_owned());
    }
    let mut roots = rustls::RootCertStore::empty();
    let (_, ignored) = roots.add_parsable_certificates(certs);
    if ignored > 0 {
        return Err(format!(
            "{ignored} certificate(s) in the file cannot be parsed"
        ));
    }
    Ok(pem)
}

/// `object_store_plaintext` 的 `endpoint`：配置取值去除 userinfo 与查询串（连同片段）。
fn shown_endpoint(endpoint: &str) -> String {
    let (scheme, rest) = endpoint.split_once("://").unwrap_or(("", endpoint));
    let rest = rest.split(['?', '#']).next().unwrap_or_default();
    let (authority, path) = match rest.find('/') {
        Some(i) => rest.split_at(i),
        None => (rest, ""),
    };
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    format!("{scheme}://{host}{path}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_addresses() {
        for (a, want) in [
            ("127.0.0.1:0", true),
            ("127.0.0.2:8800", true),
            ("[::1]:0", true),
            ("0.0.0.0:0", false),
            ("[::]:0", false),
            ("[::ffff:127.0.0.1]:0", false),
            ("10.0.0.1:8800", false),
        ] {
            assert_eq!(is_loopback(a.parse().unwrap()), want, "{a}");
        }
    }

    #[test]
    fn endpoint_without_userinfo_and_query() {
        assert_eq!(shown_endpoint("http://minio:9000"), "http://minio:9000");
        assert_eq!(
            shown_endpoint("http://u:p@minio:9000/x?a=1#f"),
            "http://minio:9000/x"
        );
        assert_eq!(shown_endpoint("http://minio:9000?a=1"), "http://minio:9000");
    }
}
