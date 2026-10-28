//! 路径段 `{org}`、`{stream}` 的取用（REQ-AMB-DM-NAMESPACE）。
//!
//! 路由按原始（未解码）路径匹配，固定路径段（`api`、`_json`、`_search`、`_sent`）因此按原样比对，
//! `%2F` 也不会把一个路径段拆成两个。`{org}` 与 `{stream}` 不经 axum 的 `Path`/`RawPathParams` 提取——
//! 两者都先自行解码，对非法 UTF-8 回框架自己的 400 纯文本——而是由端点把请求的原始路径交给这里，
//! 取出原始路径段后交 [`amberlog_common::dataset`] 解码与校验，失败即错误信封。

use amberlog_common::dataset::{Dataset, org_from_path_segment};
use amberlog_common::error::Reason;
use axum::response::{IntoResponse, Response};

use crate::gateway::ApiError;

/// `/api/{org}/_search` 这类路径中的 org（第二个路径段）。
pub fn org_from_path(path: &str) -> Result<String, Rejection> {
    let raw = segment(path, 2).ok_or(Rejection(Reason::Internal))?;
    org_from_path_segment(raw).map_err(Rejection)
}

/// `/api/{org}/{stream}/…` 这类路径中的数据集（第二、三个路径段），按校验链先 `{org}` 后 `{stream}`。
pub fn dataset_from_path(path: &str) -> Result<Dataset, Rejection> {
    match (segment(path, 2), segment(path, 3)) {
        (Some(org), Some(stream)) => Dataset::from_path_segments(org, stream).map_err(Rejection),
        // 路由模板保证有这两段；走到这里是端点挂错了路径
        _ => Err(Rejection(Reason::Internal)),
    }
}

/// 原始路径按未解码的 `/` 分段后的第 `index` 段（`/api/…` 中 `api` 为第 1 段）。
fn segment(path: &str, index: usize) -> Option<&str> {
    path.split('/').nth(index)
}

/// 路径段校验失败：`org_not_found` 或 `invalid_stream_name`；转成错误信封（[`ApiError`]）作答。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rejection(pub Reason);

impl From<Rejection> for ApiError {
    fn from(Rejection(reason): Rejection) -> ApiError {
        match reason {
            Reason::OrgNotFound => ApiError::new(reason, "organization not found"),
            Reason::InvalidStreamName => {
                ApiError::new(reason, "stream name must match ^[a-z0-9][a-z0-9_]{0,63}$")
            }
            _ => ApiError::internal()
                .with_cause("the endpoint is mounted on a path without these segments"),
        }
    }
}

impl IntoResponse for Rejection {
    fn into_response(self) -> Response {
        ApiError::from(self).into_response()
    }
}

#[cfg(test)]
mod tests {
    use axum::Router;
    use axum::body::Body;
    use axum::http::{Request, Uri};
    use http_body_util::BodyExt;
    use serde_json::Value;
    use tower::ServiceExt;

    use super::*;
    use crate::gateway::{Endpoint, finish};

    /// 测试桩：与摄取、查询端点同形的两条路由，应答取到的数据集或 org。
    fn app() -> Router {
        let router =
            Router::new()
                .route(
                    "/api/{org}/{stream}/_json",
                    Endpoint::new()
                        .post(|uri: Uri| async move {
                            dataset_from_path(uri.path()).map(|d| d.to_string())
                        })
                        .into(),
                )
                .route(
                    "/api/{org}/_search",
                    Endpoint::new()
                        .post(|uri: Uri| async move { org_from_path(uri.path()) })
                        .into(),
                );
        finish(router)
    }

    async fn post(uri: &str) -> (u16, Value) {
        let res = app()
            .oneshot(Request::post(uri).body(Body::empty()).expect("合法请求"))
            .await
            .expect("路由不失败");
        let status = res.status().as_u16();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
        (status, body)
    }

    #[tokio::test]
    async fn raw_segments_are_decoded_then_checked() {
        assert_eq!(
            post("/api/default/%61pp/_json").await,
            (200, Value::from("default/app"))
        );
        assert_eq!(
            post("/api/%64efault/app/_json").await,
            (200, Value::from("default/app"))
        );
        for (uri, status, reason) in [
            ("/api/default/%41pp/_json", 400, "invalid_stream_name"),
            ("/api/default/a%2Fb/_json", 400, "invalid_stream_name"),
            ("/api/default/%FF/_json", 400, "invalid_stream_name"),
            ("/api/default/%C3%A9/_json", 400, "invalid_stream_name"),
            ("/api/default/_system/_json", 400, "invalid_stream_name"),
            ("/api/acme/app/_json", 404, "org_not_found"),
            ("/api/Default/App/_json", 404, "org_not_found"),
            ("/api/acme/_search", 404, "org_not_found"),
            // 固定路径段按原样匹配，不经解码
            ("/api/default/app/%5Fjson", 404, "not_found"),
            ("/%61pi/default/app/_json", 404, "not_found"),
        ] {
            let (got, body) = post(uri).await;
            assert_eq!(got, status, "{uri}: {body}");
            assert_eq!(body["reason"], reason, "{uri}");
        }
        assert_eq!(
            post("/api/%64efault/_search").await,
            (200, Value::from("default"))
        );
    }
}
