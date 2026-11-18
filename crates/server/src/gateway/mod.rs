//! 接入面（Gateway）：路由装配与全站一致的错误处置。
//!
//! 错误信封、`reason` 与校验链见 REQ-AMB-ERR-ENVELOPE。
//! 校验链自外向内：`x-request-id` → 错误响应改写 → 目录前缀下的目录错误体与计数 → 停机闸门与认证 → 接住 panic
//! → 路由与方法匹配 → 权限矩阵的登记（[`crate::authz::matrix`]）→ 各端点（授权的路径一步与请求体一步由端点调用 [`crate::authz`]）。目录前缀 `/iceberg/` 下的路由与方法匹配由目录投影自己完成（[`crate::catalog`]，REQ-AMB-STO-COLD-CATALOG）。

mod auth;
pub mod conn;
mod envelope;
mod recover;
mod request_id;
mod routing;
pub mod tls;

use std::sync::Arc;

use axum::Router;
use axum::middleware;

use crate::authz::Authz;
use crate::telemetry::{self, Health, Metrics};

pub use envelope::ApiError;
pub(crate) use envelope::Enveloped;
pub use request_id::X_REQUEST_ID;
pub use routing::{Endpoint, finish};

/// 各端点共享的进程状态。
#[derive(Clone)]
pub struct AppState {
    pub health: Arc<Health>,
    pub metrics: Arc<Metrics>,
    /// 认证与授权的设置（`auth.enabled`、`auth.token`、授权文件的当前内容、`log.access`）。
    pub authz: Arc<Authz>,
}

/// 装配全部对外端点：接入面自己的 `/healthz`、`/readyz`、`/metrics`、检索界面的 `/ui`，加上各模块的路由表 `modules`。
///
/// 模块挂载的约定（各模块因此不必改本文件，`/api/` 下的模块表在 [`crate::api::routes`]）：
/// - 一个模块交出一个 `Router`，已用 `with_state` 绑好它自己的状态；
/// - 路径一律经 [`Endpoint`] 注册，使 `HEAD` 与未注册的方法得 405 并带 `Allow`；
/// - 模块不设 `fallback`、不加 `layer`：停机闸门、认证区、404 兜底、错误信封与 `x-request-id`
///   在这里对合并后的路由表统一施加，校验链的先后因此对每个模块都相同。
pub fn app(state: AppState, modules: Vec<Router>) -> Router {
    assemble(state, true, modules)
}

/// 只读目录模式（REQ-AMB-STO-COLD-CATALOG-CATALOG-ONLY）的对外端点：`/healthz`、`/readyz`、`/metrics` 与目录端点
/// （`catalog` 为 [`crate::catalog::router`]）；不挂 `/api/` 模块与检索界面，这些路径按未注册路径作答。
pub fn catalog_app(state: AppState, catalog: Router) -> Router {
    assemble(state, false, vec![catalog])
}

fn assemble(state: AppState, ui: bool, modules: Vec<Router>) -> Router {
    let mut routes = Router::new()
        .route("/healthz", Endpoint::new().get(telemetry::healthz).into())
        .route("/readyz", Endpoint::new().get(telemetry::readyz).into())
        .route("/metrics", Endpoint::new().get(telemetry::metrics).into())
        .with_state(state.clone());
    if ui {
        routes = routes.merge(crate::ui::router());
        // 界面登录的三个登录路径（REQ-AMB-AUTH-OIDC C 组）；没配界面登录时为空表
        routes = routes.merge(crate::ui::oidc::router(&state.authz));
    }
    for module in modules {
        routes = routes.merge(module);
    }
    // 认证区内匹配到的路由须在权限矩阵登记（REQ-AMB-AUTH-AUTHZ-MATRIX-CLOSED），端点种类随之回填
    let routes = routes.route_layer(middleware::from_fn(crate::authz::matrix::registered));
    let routes = routing::recovering(routes)
        .layer(middleware::from_fn_with_state(state.clone(), auth::gate))
        .layer(middleware::from_fn_with_state(
            Arc::clone(&state.metrics),
            crate::catalog::shape,
        ));
    routing::outer_layers(routes)
}

#[cfg(test)]
mod tests;
