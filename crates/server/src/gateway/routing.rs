//! 路由与方法匹配的错误处置（REQ-AMB-ERR-ENVELOPE「路由与方法匹配」三条）。
//!
//! - 未注册路径：404 `not_found`。
//! - 已注册路径上不支持的方法（含 `HEAD` 与 `OPTIONS`）：405 `method_not_allowed`，带 `Allow`。
//!   axum 的 `get` 路由默认兼接 `HEAD`，所以端点一律经 [`Endpoint`] 注册，由它把 `HEAD` 显式拒掉。
//! - axum 与提取器自带的错误响应（空体或纯文本体）在出口改写为错误信封。

use amberlog_common::error::Reason;
use axum::Router;
use axum::handler::Handler;
use axum::http::{HeaderValue, StatusCode, header};
use axum::middleware;
use axum::response::{IntoResponse, Response};
use axum::routing::{MethodFilter, MethodRouter};

use super::envelope::{ApiError, Enveloped};
use super::{recover, request_id};

/// 一个路径上的方法表。只接受显式注册的方法，其余方法一律 405。
pub struct Endpoint<S = ()> {
    router: MethodRouter<S>,
    allowed: Vec<&'static str>,
}

impl<S> Default for Endpoint<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Endpoint {
            router: MethodRouter::new(),
            allowed: Vec::new(),
        }
    }
}

impl<S> Endpoint<S>
where
    S: Clone + Send + Sync + 'static,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get<H, T>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.on(MethodFilter::GET, "GET", handler)
    }

    pub fn post<H, T>(self, handler: H) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.on(MethodFilter::POST, "POST", handler)
    }

    fn on<H, T>(mut self, filter: MethodFilter, name: &'static str, handler: H) -> Self
    where
        H: Handler<T, S>,
        T: 'static,
    {
        self.router = self.router.on(filter, handler);
        self.allowed.push(name);
        self
    }

    pub fn into_method_router(self) -> MethodRouter<S> {
        let allow = HeaderValue::from_str(&self.allowed.join(", ")).expect("方法名是合法的头取值");
        let deny = move || async move { ApiError::method_not_allowed(allow) };
        // 只注册了 GET 时 axum 会让 HEAD 落到 GET 上；显式挂一个拒绝处理，使 HEAD 也得 405。
        // 405 响应自带 `Allow`，axum 见到已有 `Allow` 就不再按自己的方法表补写。
        self.router.head(deny.clone()).fallback(deny)
    }
}

impl<S> From<Endpoint<S>> for MethodRouter<S>
where
    S: Clone + Send + Sync + 'static,
{
    fn from(endpoint: Endpoint<S>) -> Self {
        endpoint.into_method_router()
    }
}

/// 给装配好的路由表补上 404 兜底、接住 panic、错误响应改写与 `x-request-id`。须在全部路由注册之后调用。
pub fn finish(router: Router) -> Router {
    outer_layers(recovering(router))
}

/// 404 兜底，再在全部路由之外接住处理请求时的 panic（[`recover`]）。
pub(crate) fn recovering(router: Router) -> Router {
    router
        .fallback(not_found)
        .layer(middleware::from_fn(recover::catch_panic))
}

/// 最外两层：错误响应改写与 `x-request-id`（后者在最外，使每个最终响应都带上它）。
pub(crate) fn outer_layers(router: Router) -> Router {
    router
        .layer(middleware::map_response(normalize))
        .layer(middleware::from_fn(request_id::propagate))
}

pub(crate) async fn not_found() -> ApiError {
    ApiError::new(Reason::NotFound, "no route for this path")
}

/// 把非信封的 4xx、5xx 响应改写为错误信封。
///
/// 端点自己的错误都经 [`ApiError`] 返回；走到这里的只有框架层（路由、方法、提取器、请求体上限）
/// 生成的响应。404、405、413 按原义改写；其余状态码说明有端点让框架的拒绝透了出来，
/// 按未归类的服务端错误处理。
async fn normalize(res: Response) -> Response {
    let status = res.status();
    if !(status.is_client_error() || status.is_server_error())
        || res.extensions().get::<Enveloped>().is_some()
    {
        return res;
    }
    match status {
        StatusCode::NOT_FOUND => not_found().await.into_response(),
        StatusCode::METHOD_NOT_ALLOWED => match res.headers().get(header::ALLOW) {
            Some(allow) => ApiError::method_not_allowed(allow.clone()).into_response(),
            None => ApiError::new(Reason::MethodNotAllowed, "method not allowed for this path")
                .into_response(),
        },
        StatusCode::PAYLOAD_TOO_LARGE => {
            ApiError::new(Reason::PayloadTooLarge, "request body too large").into_response()
        }
        other => ApiError::internal()
            .with_cause(format!("unenveloped framework response {other}"))
            .into_response(),
    }
}
