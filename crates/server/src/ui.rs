//! 检索界面（REQ-AMB-UI-CONSOLE-V4）：四个页面路径返回同一个自包含的单页（脚本、样式与图标内嵌，编译时嵌入二进制）。
//!
//! 页面是仓根 `ui/` 前端工程的构建产物（ADR-0060）：改 `ui/` 的源码后在 `ui/` 下执行 `pnpm build` 重新生成
//! 本文件旁的 `ui/index.html` 并一起提交，Rust 构建不调 Node。页面是浏览器里的 `_search` 客户端：检索、台账、对账三屏的数据只经
//! `POST /api/default/_search` 与 `GET /api/default/<stream>/_fields` 取得（台账与对账是系统表 `_ledger`、`_reconciliation`，
//! 后者给检索屏的字段栏与取值提示），访问口令由用户在页面上填写、
//! 只存当前标签页的会话存储。`/ui` 不在认证区内（REQ-AMB-ERR-ENVELOPE），服务端不往页面里嵌入任何口令或数据。

use axum::Router;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};

use crate::gateway::Endpoint;

pub mod oidc;

/// 页面全文（`ui/` 的构建产物）。
pub const PAGE: &str = include_str!("ui/index.html");

/// 页面路径：`/ui` 与三个屏地址（REQ-AMB-UI-CONSOLE-V4-PAGE、SCREENS）。逐个注册、不用通配路由（ADR-0060 第 5 条）；
/// 查询串不影响结果，`/ui/` 与其他 `/ui` 之下的路径由接入面按未注册路径作答（登录路径见 [`oidc`]）。
pub const PAGE_PATHS: [&str; 4] = ["/ui", "/ui/search", "/ui/ledger", "/ui/reconciliation"];

pub fn router() -> Router {
    PAGE_PATHS.iter().fold(Router::new(), |r, path| {
        r.route(path, Endpoint::new().get(page).into())
    })
}

/// 页面响应另带 `Cache-Control: no-cache`：浏览器每次向服务端核对，升级可执行文件后不会拿到旧页面
/// （REQ-AMB-UI-CONSOLE-V4-PAGE；回调页不带，CALLBACK-PAGE）。
async fn page() -> Response {
    let mut res = html_page(PAGE);
    res.headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-cache"));
    res
}

/// 界面页面的响应：`Content-Type: text/html; charset=utf-8`，另带防嵌入与不外发来源的三个响应头
/// （REQ-AMB-UI-CONSOLE-V4-PAGE；回调页同样用它，REQ-AMB-UI-CONSOLE-V4-CALLBACK-PAGE）。`Content-Security-Policy`
/// 只写 `frame-ancestors 'none'`，不限制脚本来源：页面脚本是内联的。
pub fn html_page(body: &'static str) -> Response {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::X_FRAME_OPTIONS, "DENY"),
            (header::CONTENT_SECURITY_POLICY, "frame-ancestors 'none'"),
            (header::REFERRER_POLICY, "no-referrer"),
        ],
        body,
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn serves_page_paths_only() {
        let app = crate::gateway::finish(router());
        for path in [
            "/ui",
            "/ui?x=1",
            "/ui/search",
            "/ui/search?x=1",
            "/ui/ledger",
            "/ui/reconciliation",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK, "{path}");
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
            assert_eq!(response.headers()[header::X_FRAME_OPTIONS], "DENY");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                PAGE.as_bytes(),
                "{path}"
            );
        }
        for path in [
            "/ui/",
            "/ui/search/",
            "/ui/settings",
            "/ui/search/x",
            "/ui/assets/index.js",
            "/ui/index.html",
            "/ui/oidc/config",
            "/ui/callback",
            "/uix",
        ] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::NOT_FOUND, "{path}");
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let value: serde_json::Value = serde_json::from_slice(&body).unwrap();
            assert_eq!(value["reason"], "not_found");
        }
        for path in PAGE_PATHS {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .method("POST")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED, "{path}");
            assert_eq!(response.headers()[header::ALLOW], "GET");
        }
    }
}
