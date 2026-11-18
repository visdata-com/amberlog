//! 停机闸门与认证（REQ-AMB-ERR-ENVELOPE 校验链第 1、2 步；REQ-AMB-AUTH-CREDENTIAL-V3）。
//!
//! 作用于路由与方法匹配之前：`/api/` 前缀与目录前缀 `/iceberg/` 在停机过程中一律 503 `unavailable`
//! （REQ-AMB-STO-COLD-CATALOG-SHUTDOWN）；认证区（`/api/` 前缀 ∪ 路径 `/metrics` ∪ 目录前缀，按请求目标中未经百分号解码的
//! 路径判定）内按凭据得出主体（[`crate::authz::Authz::authenticate`]），凭据缺失或无效回 401；得出的主体连同访问记录作为
//! 请求扩展 [`Access`] 交给路由与各端点（授权的判定在 [`crate::authz`]）。`auth.enabled` 为 false 时不要求凭据。
//! 目录前缀下的 503 与 401 由出口改写为目录错误体（[`crate::catalog::shape`]）。
//!
//! 认证区内的请求得出响应之后，在这里按最终状态码计 401、403（REQ-AMB-OBS-HEALTH-METRICS-V7），并输出访问事件
//! （[`Access::emit`]，REQ-AMB-AUTH-AUTHZ C 组）。其外的几层不改认证区内响应的状态码。

use amberlog_common::error::Reason;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::{ApiError, AppState, X_REQUEST_ID};
use crate::authz::Access;
use crate::authz::matrix::in_auth_zone;

pub(crate) async fn gate(State(state): State<AppState>, mut req: Request, next: Next) -> Response {
    let path = req.uri().path();
    let api = path.starts_with("/api/") || crate::catalog::in_prefix(path);
    let zone = in_auth_zone(path);
    // 在途计数随本函数的 future 存活到响应得出（REQ-AMB-LIFE-DURABILITY 优雅停机的排空对象）
    let _admitted = match api.then(|| state.health.admit()) {
        Some(None) => {
            return ApiError::new(Reason::Unavailable, "the server is shutting down")
                .unlogged()
                .into_response();
        }
        admitted => admitted.flatten(),
    };
    if !zone {
        return next.run(req).await;
    }
    let counters = state.authz.counters();
    // `auth.token`、服务令牌都不是且形如 JWS 时按身份平台令牌验证（REQ-AMB-AUTH-CREDENTIAL-V3），验证不通过同走这条 401
    let Some(principal) = state.authz.authenticate_idp(req.headers()).await else {
        counters.record_final(StatusCode::UNAUTHORIZED);
        return ApiError::new(Reason::Unauthorized, "missing or invalid credentials")
            .into_response();
    };
    let access = Access::new(principal);
    req.extensions_mut().insert(access.clone());
    let method = req.method().clone();
    // 最外层已把请求带的或生成的 `x-request-id` 写进请求头
    let request_id = req
        .headers()
        .get(&X_REQUEST_ID)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
        .unwrap_or_default();
    let res = next.run(req).await;
    counters.record_final(res.status());
    access.emit(
        state.authz.log_access(),
        method.as_str(),
        res.status(),
        &request_id,
    );
    res
}
