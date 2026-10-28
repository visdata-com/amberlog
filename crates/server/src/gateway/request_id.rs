//! 请求关联头 `x-request-id`：请求带了就原样回传，否则由系统生成（REQ-AMB-ERR-ENVELOPE）。
//!
//! 生成格式不作承诺。生成的值同时写回请求头，后续处理与日志可从请求头取到同一个 id。
//!
//! 这一层在最外，响应在这里已是最终响应：5xx 在这里写一行 `error` 日志，带上同一个 id 与出错原因
//! （REQ-AMB-ERR-ENVELOPE-V7）。

use std::collections::hash_map::RandomState;
use std::hash::BuildHasher;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

use amberlog_common::error::Reason;
use axum::extract::Request;
use axum::http::{HeaderName, HeaderValue};
use axum::middleware::Next;
use axum::response::Response;
use serde_json::Value;

use super::Enveloped;
use crate::logging::{self, Level};

pub const X_REQUEST_ID: HeaderName = HeaderName::from_static("x-request-id");

fn generate() -> HeaderValue {
    static SEED: OnceLock<RandomState> = OnceLock::new();
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let h = SEED.get_or_init(RandomState::new).hash_one(n);
    HeaderValue::from_str(&format!("{h:016x}{n:08x}")).expect("十六进制字符是合法的头取值")
}

pub(crate) async fn propagate(mut req: Request, next: Next) -> Response {
    let id = match req.headers().get(&X_REQUEST_ID) {
        Some(v) => v.clone(),
        None => {
            let v = generate();
            req.headers_mut().insert(X_REQUEST_ID, v.clone());
            v
        }
    };
    let (method, path) = (req.method().clone(), req.uri().path().to_owned());
    let mut res = next.run(req).await;
    // 测试专用暂停点：响应已得出（在途计数已结束）、尚未交给连接写出（验证停机等应答写完再退出）
    amberlog_storage::fault::gate("gateway.after_response").await;
    if res.status().is_server_error() {
        log_server_error(&res, &id, method.as_str(), &path);
    }
    res.headers_mut().insert(X_REQUEST_ID, id);
    res
}

/// 一个 5xx 写一行 `error` 日志：请求编号、状态码、`reason`、响应的写定文字与底层错误的原文（URL 去除用户信息与
/// 查询串）。`log.level` 的最低档就是 `error`，所以任何取值下都输出。字段名不作承诺。
fn log_server_error(res: &Response, id: &HeaderValue, method: &str, path: &str) {
    let mark = res.extensions().get::<Enveloped>();
    // `/readyz` 未就绪（`not_ready` 只用于它）与停机闸门的 503 不写
    if mark.is_some_and(|m| !m.logged || m.reason == Reason::NotReady) {
        return;
    }
    let mut fields = vec![
        (
            "request_id",
            Value::from(String::from_utf8_lossy(id.as_bytes())),
        ),
        ("status", Value::from(res.status().as_u16())),
        ("method", Value::from(method)),
        ("path", Value::from(path)),
    ];
    if let Some(m) = mark {
        fields.push(("reason", Value::from(m.reason.as_str())));
        fields.push(("message", Value::from(m.message.as_str())));
        if let Some(cause) = &m.cause {
            fields.push(("error", Value::from(logging::redact_urls(cause))));
        }
    }
    logging::log(Level::Error, "request failed with a server error", &fields);
}
