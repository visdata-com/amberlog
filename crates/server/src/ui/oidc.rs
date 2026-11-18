//! 界面登录的三个登录路径（REQ-AMB-AUTH-OIDC C 组；ADR-0056 第 3 条）：`GET /ui/oidc/config`、`GET /ui/callback`
//! 与 `POST /ui/oidc/token`。
//!
//! - 只挂在写入实例的界面路由旁（只读目录模式的路由表不挂，按未注册路径作答，UI-ROUTES）；都在认证区与停机闸门之外
//!   （`gateway/auth.rs` 的闸门只作用于 `/api/`、目录前缀与 `/metrics`；UI-NOT-AUTH-ZONE、UI-NO-SHUTDOWN-GATE）。
//! - 未配置 `auth.oidc.issuer`、`auth.oidc.client_id`，或 `auth.enabled` 为 false 时不注册，三个路径按未注册路径回 404
//!   （UI-OFF-404）。
//! - 注册时每个最终响应（含 405 与错误响应）带 `Cache-Control: no-store`（UI-NO-STORE）。请求体自己按收到的字节读、
//!   至多 16384 字节，不经框架的请求体上限，413 也由本模块以错误信封作答，所以这个头不会被出口的改写丢掉。
//! - 回调页是 `ui/` 的第二个构建产物（`ui/callback.html`），与 `GET /ui` 共用安全响应头（[`super::html_page`]）。

use std::sync::Arc;

use amberlog_common::error::Reason;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::MethodRouter;
use serde_json::{Value, json};

use crate::authz::Authz;
use crate::gateway::{ApiError, Endpoint};
use crate::oidc::{ExchangeFailed, Idp};

/// 回调页全文（`ui/` 的构建产物，REQ-AMB-UI-CONSOLE-V3-CALLBACK-PAGE）。
pub const CALLBACK: &str = include_str!("callback.html");

/// `POST /ui/oidc/token` 的请求体上限（REQ-AMB-AUTH-OIDC-UI-TOKEN-BODY-LIMIT）。
const TOKEN_BODY_LIMIT: usize = 16_384;

/// 三个登录路径；界面登录没配（或认证关闭）时为空表。
pub fn router(authz: &Authz) -> Router {
    let Some(idp) = authz.idp().filter(|i| i.login().is_some()).cloned() else {
        return Router::new();
    };
    Router::new()
        .route("/ui/oidc/config", no_store(Endpoint::new().get(config)))
        .route("/ui/callback", no_store(Endpoint::new().get(callback)))
        .route("/ui/oidc/token", no_store(Endpoint::new().post(token)))
        .with_state(idp)
}

/// 方法表的每个响应（含 405）都带 `Cache-Control: no-store`。
fn no_store(endpoint: Endpoint<Arc<Idp>>) -> MethodRouter<Arc<Idp>> {
    endpoint
        .into_method_router()
        .layer(middleware::map_response(|mut res: Response| async move {
            res.headers_mut()
                .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
            res
        }))
}

/// `GET /ui/oidc/config`（UI-CONFIG、UI-CONFIG-UNAVAILABLE）。
async fn config(State(idp): State<Arc<Idp>>) -> Response {
    let login = idp.login().expect("只在配了界面登录时注册");
    match idp.authorization_endpoint() {
        Ok(endpoint) => axum::Json(json!({
            "authorization_endpoint": endpoint,
            "client_id": login.client_id,
            "scope": login.scope,
        }))
        .into_response(),
        Err(why) => unavailable(why),
    }
}

/// `GET /ui/callback`（UI-CALLBACK）：固定页面，查询串、请求头与配置都不影响响应。
async fn callback() -> Response {
    super::html_page(CALLBACK)
}

/// `POST /ui/oidc/token`：先按收到的字节校验请求体（上限、JSON 对象、三个字段），通过后才向令牌端点发请求
/// （UI-TOKEN-REQUEST 至 UI-TOKEN-IDP-DOWN）。
async fn token(State(idp): State<Arc<Idp>>, body: Body) -> Response {
    let Ok(bytes) = axum::body::to_bytes(body, TOKEN_BODY_LIMIT).await else {
        return ApiError::new(Reason::PayloadTooLarge, "request body too large").into_response();
    };
    let Ok(Value::Object(fields)) = amberlog_common::json::parse_body(&bytes) else {
        return ApiError::new(Reason::InvalidJson, "the request body is not a JSON object")
            .into_response();
    };
    let text = |k: &str| {
        fields
            .get(k)
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
    };
    let (Some(code), Some(verifier), Some(redirect)) =
        (text("code"), text("code_verifier"), text("redirect_uri"))
    else {
        return ApiError::new(
            Reason::InvalidRequest,
            "the request body must contain non-empty string fields code, code_verifier and redirect_uri",
        )
        .into_response();
    };
    match idp.exchange(code, verifier, redirect).await {
        Ok(t) => axum::Json(json!({
            "access_token": t.access_token,
            "expires_in": t.expires_in,
        }))
        .into_response(),
        Err(ExchangeFailed::Rejected(status)) => ApiError::new(
            Reason::InvalidRequest,
            format!("the identity provider rejected the authorization code (HTTP {status})"),
        )
        .into_response(),
        Err(ExchangeFailed::Unavailable(why)) => unavailable(why),
    }
}

/// 503 `unavailable`；出错原因只进 5xx 运行日志（REQ-AMB-ERR-ENVELOPE）。
fn unavailable(why: String) -> Response {
    ApiError::new(Reason::Unavailable, "the identity provider is unavailable")
        .with_cause(why)
        .into_response()
}
