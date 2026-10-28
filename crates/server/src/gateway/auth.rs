//! 停机闸门与认证（REQ-AMB-ERR-ENVELOPE 校验链第 1、2 步；REQ-AMB-AUTH-CREDENTIAL）。
//!
//! 作用于路由与方法匹配之前：`/api/` 前缀与目录前缀 `/iceberg/` 在停机过程中一律 503 `unavailable`
//! （REQ-AMB-STO-COLD-CATALOG-SHUTDOWN）；认证区（`/api/` 前缀 ∪ 路径 `/metrics` ∪ 目录前缀，按请求目标中未经百分号解码的
//! 路径判定，REQ-AMB-AUTH-CREDENTIAL-V2）在 `auth.enabled` 为 true 时要求 `Authorization: Bearer <auth.token>`。
//! 目录前缀下的 503 与 401 由出口改写为目录错误体（[`crate::catalog::shape`]）。

use amberlog_common::error::Reason;
use axum::extract::{Request, State};
use axum::http::{HeaderMap, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use sha2::{Digest, Sha256};

use super::{ApiError, AppState};
use crate::config::Auth;

pub(crate) async fn gate(State(state): State<AppState>, req: Request, next: Next) -> Response {
    let path = req.uri().path();
    let api = path.starts_with("/api/") || crate::catalog::in_prefix(path);
    // 在途计数随本函数的 future 存活到响应得出（REQ-AMB-LIFE-DURABILITY 优雅停机的排空对象）
    let _admitted = match api.then(|| state.health.admit()) {
        Some(None) => {
            return ApiError::new(Reason::Unavailable, "the server is shutting down")
                .unlogged()
                .into_response();
        }
        admitted => admitted.flatten(),
    };
    if (api || path == "/metrics")
        && let Auth::Enabled { token } = state.auth.as_ref()
        && !authorized(req.headers(), token.as_bytes())
    {
        return ApiError::new(Reason::Unauthorized, "missing or invalid credentials")
            .into_response();
    }
    next.run(req).await
}

/// 恰有一行 `Authorization`，方案名不区分大小写地等于 `Bearer`，其后一个或多个空格，余下部分与口令逐字节相同。
fn authorized(headers: &HeaderMap, token: &[u8]) -> bool {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    // 字段值首尾的空白不属于字段值（RFC 9110 §5.5）
    let v = value.as_bytes().trim_ascii();
    let Some(sp) = v.iter().position(|&b| b == b' ') else {
        return false;
    };
    let (scheme, rest) = v.split_at(sp);
    if !scheme.eq_ignore_ascii_case(b"bearer") {
        return false;
    }
    let presented = rest.trim_ascii_start();
    constant_time_eq(presented, token)
}

/// 两边各取 SHA-256 摘要再逐字节比较：摘要定长，耗时既不随首个不同字节的位置变化，也不随口令的长度变化。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let (a, b) = (Sha256::digest(a), Sha256::digest(b));
    let diff = a.iter().zip(b.iter()).fold(0u8, |d, (x, y)| d | (x ^ y));
    std::hint::black_box(diff) == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderValue;

    fn check(values: &[&str]) -> bool {
        let mut h = HeaderMap::new();
        for v in values {
            h.append(header::AUTHORIZATION, HeaderValue::from_str(v).unwrap());
        }
        authorized(&h, b"s3cret")
    }

    #[test]
    fn bearer_forms() {
        for ok in [
            "Bearer s3cret",
            "bearer s3cret",
            "BEARER s3cret",
            "Bearer   s3cret",
            "Bearer s3cret  ",
        ] {
            assert!(check(&[ok]), "{ok:?}");
        }
        for bad in [
            "Bearer S3CRET",
            "Bearer s3cre",
            "Bearer s3cretX",
            "Bearer",
            "Bearer ",
            "Bearers3cret",
            "s3cret",
            "Basic czNjcmV0",
            "Bearer\ts3cret",
        ] {
            assert!(!check(&[bad]), "{bad:?}");
        }
        assert!(!check(&[]));
        assert!(!check(&["Bearer s3cret", "Bearer s3cret"]));
        assert!(!check(&["Bearer s3cret", "Bearer wrong"]));
    }

    #[test]
    fn digest_comparison() {
        assert!(constant_time_eq(b"", b""));
        assert!(constant_time_eq(b"s3cret", b"s3cret"));
        assert!(!constant_time_eq(b"s3cret", b""));
        assert!(!constant_time_eq(b"s3cret", b"s3cret\0"));
        assert!(!constant_time_eq(b"a", b"b"));
    }
}
