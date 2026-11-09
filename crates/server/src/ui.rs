//! 检索界面（REQ-AMB-UI-CONSOLE）：`GET /ui` 返回 Preact 工作台，HTML、JS、CSS 均编译时嵌入二进制。
//!
//! 页面是仓根 `ui/` 前端工程的构建产物（ADR-0055）：改 `ui/src/` 后在 `ui/` 下执行 `pnpm build` 重新生成
//! 本文件旁的 `ui/` 资源目录并一起提交，Rust 构建不调 Node，部署无需静态文件目录。页面是浏览器里的 `_search` 客户端：检索、台账、对账三屏的数据只经
//! `POST /api/default/_search` 与 `GET /api/default/<stream>/_fields` 取得（台账与对账是系统表 `_ledger`、`_reconciliation`，
//! 后者给检索屏的字段栏与取值提示），访问口令由用户在页面上填写、
//! 只存当前标签页的会话存储。`/ui` 不在认证区内（REQ-AMB-ERR-ENVELOPE），服务端不往页面里嵌入任何口令或数据。

use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;

use crate::gateway::Endpoint;

/// 页面全文（`ui/` 的构建产物）。
pub const PAGE: &str = include_str!("ui/index.html");

struct Asset {
    path: &'static str,
    content_type: &'static str,
    bytes: &'static [u8],
}

include!(concat!(env!("OUT_DIR"), "/ui_assets.rs"));

/// 页面入口和构建产物都由 Rust 从二进制中提供，不依赖运行时的静态文件目录。
pub fn router() -> Router {
    let mut routes = Router::new().route("/ui", Endpoint::new().get(page).into());
    for asset in ASSETS {
        routes = routes.route(
            asset.path,
            Endpoint::new()
                .get(move || async move {
                    (
                        [
                            (header::CONTENT_TYPE, asset.content_type),
                            (header::CACHE_CONTROL, "public, max-age=31536000, immutable"),
                            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
                        ],
                        asset.bytes,
                    )
                })
                .into(),
        );
    }
    routes
}

async fn page() -> impl IntoResponse {
    (
        [
            (header::CONTENT_TYPE, "text/html; charset=utf-8"),
            (header::CACHE_CONTROL, "no-cache"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        PAGE,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    #[tokio::test]
    async fn serves_ui_routes_and_embedded_assets() {
        let app = crate::gateway::finish(router());
        for path in ["/ui", "/ui?x=1"] {
            let response = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.headers()[header::CONTENT_TYPE],
                "text/html; charset=utf-8"
            );
            assert_eq!(response.headers()[header::CACHE_CONTROL], "no-cache");
            assert_eq!(
                response.into_body().collect().await.unwrap().to_bytes(),
                PAGE.as_bytes(),
                "{path}"
            );
        }
        assert!(ASSETS.iter().any(|asset| asset.path.ends_with(".js")));
        assert!(ASSETS.iter().any(|asset| asset.path.ends_with(".css")));
        for asset in ASSETS {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(asset.path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(response.headers()[header::CONTENT_TYPE], asset.content_type);
            assert_eq!(
                response.headers()[header::CACHE_CONTROL],
                "public, max-age=31536000, immutable"
            );
            assert_eq!(
                response
                    .into_body()
                    .collect()
                    .await
                    .unwrap()
                    .to_bytes()
                    .as_ref(),
                asset.bytes
            );
        }
    }

    #[tokio::test]
    async fn ui_keeps_gateway_errors_and_method_rules() {
        let app = crate::gateway::finish(router());
        for path in [
            "/ui/assets",
            "/ui/assets/",
            "/ui/assets/missing.js",
            "/ui/assets/missing",
            "/ui/assets%2Fmissing.js",
            "/ui/.vite/manifest.json",
            "/ui/%2Evite/manifest.json",
            "/ui/src",
            "/ui/src/",
            "/ui/src/main.tsx",
            "/ui/src/component",
            "/ui/",
            "/ui/index.html",
            "/ui/missing.css",
            "/ui/missing.js.map",
            "/ui/%FF",
            "/uix/settings",
            "/api/default/does-not-exist",
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
        for path in [
            "/ui",
            ASSETS[0].path,
        ] {
            for method in ["HEAD", "OPTIONS", "POST"] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(path)
                            .method(method)
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
                assert_eq!(response.headers()[header::ALLOW], "GET");
            }
        }
    }
}
