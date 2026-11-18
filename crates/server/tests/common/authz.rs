//! 授权的测试台架（REQ-AMB-AUTH-AUTHZ）：现场生成服务令牌与它的哈希行，写授权文件，以授权文件启动被测进程，以服务令牌发请求。
//!
//! 服务令牌每次现场取随机量生成，不写进源码；断言里不写令牌原文（需要核对日志不含令牌时，以变量比较）。

use std::io::Read;
use std::path::PathBuf;

use sha2::{Digest, Sha256};

use super::api::search_body;
use super::{Fixture, Kv, Resp, Server};

/// 一枚服务令牌（REQ-AMB-AUTH-AUTHZ-TOKEN-FORMAT）与它的哈希行（REQ-AMB-AUTH-AUTHZ-TOKEN-MATCH）。
#[derive(Clone)]
pub struct ServiceToken {
    pub token: String,
    pub hash: String,
}

impl std::fmt::Debug for ServiceToken {
    /// 令牌原文不进断言消息与调试输出。
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ServiceToken")
            .field("hash", &self.hash)
            .finish()
    }
}

impl ServiceToken {
    /// 现场生成：32 字节随机量的无填充 base64url 编码前加 `amb_`。
    pub fn generate() -> ServiceToken {
        let mut random = [0u8; 32];
        std::fs::File::open("/dev/urandom")
            .and_then(|mut f| f.read_exact(&mut random))
            .expect("读 /dev/urandom");
        let token = format!("amb_{}", base64url(&random));
        let hash = hash_line(&token);
        ServiceToken { token, hash }
    }

    /// `Authorization` 头的取值。
    pub fn bearer(&self) -> String {
        format!("Bearer {}", self.token)
    }
}

/// 无填充的 base64url 编码（RFC 4648 §5）。
pub fn base64url(bytes: &[u8]) -> String {
    const A: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::new();
    for chunk in bytes.chunks(3) {
        let n = chunk.iter().fold(0u32, |n, b| n << 8 | u32::from(*b)) << (8 * (3 - chunk.len()));
        for i in 0..=chunk.len() {
            out.push(A[(n >> (18 - 6 * i) & 0x3f) as usize] as char);
        }
    }
    out
}

/// 任意凭据的哈希行：`sha256:` 加其 UTF-8 字节 SHA-256 摘要的小写十六进制。
pub fn hash_line(credential: &str) -> String {
    let d = Sha256::digest(credential.as_bytes());
    let hex: String = d.iter().map(|b| format!("{b:02x}")).collect();
    format!("sha256:{hex}")
}

/// 一个令牌条目（TOML 文本）：`grants` 为 TOML 写法的授予数组，如 `[{ role = "querier", streams = ["app"] }]`；
/// `extra` 为条目里另加的行（`expires_at`、`sources`），可为空。
pub fn entry(name: &str, hashes: &[&str], grants: &str, extra: &str) -> String {
    let hashes: Vec<String> = hashes.iter().map(|h| format!("\"{h}\"")).collect();
    format!(
        "[[tokens]]\nname = \"{name}\"\nhashes = [{}]\ngrants = {grants}\n{extra}\n",
        hashes.join(", ")
    )
}

/// 一个只含一枚令牌的条目。
pub fn token_entry(name: &str, token: &ServiceToken, grants: &str) -> String {
    entry(name, &[&token.hash], grants, "")
}

/// 授权文件的完整文本：`format_version = 1` 加各条目。
pub fn file_text(entries: &[String]) -> String {
    let mut s = String::from("format_version = 1\n");
    for e in entries {
        s.push_str(e);
    }
    s
}

/// 把授权文件写到夹具目录下的 `authz.toml`，返回路径。
pub fn write(fx: &Fixture, entries: &[String]) -> PathBuf {
    write_text(fx, &file_text(entries))
}

/// 把任意文本写成夹具目录下的 `authz.toml`（整体原子替换：先写临时文件再改名）。
pub fn write_text(fx: &Fixture, text: &str) -> PathBuf {
    let path = fx.root().join("authz.toml");
    let tmp = fx.root().join("authz.toml.tmp");
    std::fs::write(&tmp, text).unwrap();
    std::fs::rename(&tmp, &path).unwrap();
    path
}

/// 配置项 `auth.authz_file` 的 TOML 取值（带引号）。
pub fn authz_file_value(path: &std::path::Path) -> String {
    format!("\"{}\"", path.display())
}

/// 以授权文件 `entries` 启动写入实例（`auth.token` 仍为台架口令，`overrides` 另加），等到 `ready`。
pub fn start(fx: &Fixture, entries: &[String], overrides: &[Kv]) -> Server {
    let path = write(fx, entries);
    let value = authz_file_value(&path);
    let mut all: Vec<Kv> = vec![("auth.authz_file", Some(value.as_str()))];
    all.extend_from_slice(overrides);
    fx.start(&all)
}

/// 以凭据 `bearer`（`Authorization` 头的取值）提交 SQL。
pub fn search_as(server: &Server, bearer: &str, sql: &str) -> Resp {
    server.request(
        "POST",
        "/api/default/_search",
        &[
            ("Authorization", bearer),
            ("Content-Type", "application/json"),
        ],
        search_body(sql).as_bytes(),
    )
}

/// 以凭据 `bearer` 向 `stream` 的 `_json` 提交请求体 `body`，另带请求头 `headers`。
pub fn ingest_as(
    server: &Server,
    bearer: &str,
    stream: &str,
    headers: &[(&str, &str)],
    body: &str,
) -> Resp {
    let path = format!("/api/default/{stream}/_json");
    let mut all = vec![
        ("Authorization", bearer),
        ("Content-Type", "application/json"),
    ];
    all.extend_from_slice(headers);
    server.request("POST", &path, &all, body.as_bytes())
}

/// 以凭据 `bearer` 发 `GET`。
pub fn get_as(server: &Server, bearer: &str, path: &str) -> Resp {
    server.request("GET", path, &[("Authorization", bearer)], b"")
}

/// 运行日志里 `event` 为 `event` 的 JSON 行。
pub fn events(server: &Server, event: &str) -> Vec<serde_json::Value> {
    server
        .json_lines()
        .into_iter()
        .filter(|v| v["event"] == event)
        .collect()
}

/// 等待（至多 30 秒）运行日志里出现满足 `pred` 的 `event` 事件，返回它。访问事件在作答之前写出，stderr 由收集线程读入，
/// 读到响应之后可能还要片刻才看得到。
pub fn wait_event(
    server: &Server,
    event: &str,
    pred: impl Fn(&serde_json::Value) -> bool,
) -> serde_json::Value {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    loop {
        if let Some(v) = events(server, event).into_iter().find(|v| pred(v)) {
            return v;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "30 秒内没有满足条件的 {event}：{:#?}",
            server.stderr_lines()
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
}

/// 两个应答完全相同（REQ-AMB-AUTH-AUTHZ-TERM-SAME-AS-ABSENT）：状态码相同、响应体逐字节相同、除 `x-request-id` 与 `Date`
/// 之外的响应头（名字不区分大小写，与取值一起）相同。
pub fn assert_same_as_absent(out_of_scope: &Resp, absent: &Resp) {
    let headers = |r: &Resp| {
        let mut h: Vec<(String, String)> = r
            .headers
            .iter()
            .map(|(k, v)| (k.to_ascii_lowercase(), v.clone()))
            .filter(|(k, _)| k != "x-request-id" && k != "date")
            .collect();
        h.sort();
        h
    };
    assert_eq!(
        out_of_scope.status, absent.status,
        "{out_of_scope:?}\n{absent:?}"
    );
    assert_eq!(
        out_of_scope.body,
        absent.body,
        "响应体不同：{:?} / {:?}",
        out_of_scope.text(),
        absent.text()
    );
    assert_eq!(headers(out_of_scope), headers(absent));
}
