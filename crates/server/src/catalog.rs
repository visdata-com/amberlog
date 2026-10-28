//! 目录投影（Catalog）：按 Apache Iceberg REST 目录协议提供的只读子集，挂在目录前缀 `/iceberg/` 之下
//! （REQ-AMB-STO-COLD-CATALOG；ADR-0039 第 6 条 B、第 7 条，ADR-0046）。写入实例与只读目录模式都提供，作答相同。
//!
//! 目录前缀下的校验链（CHAIN）：停机闸门与认证在接入面（[`crate::gateway`]，路由之前）；其后由 [`answer`] 自己做路由与
//! 方法匹配（[`route`]，未注册路径回 405、`Allow` 为空）→ `{prefix}` → 命名空间与表名 → 读取对象存储（[`projection`]）。
//! 各步的失败都以错误信封类型 [`ApiError`] 返回，由出口 [`shape`] 统一改写为目录错误体并计数。目录端点不读请求体。

mod projection;
mod route;

use std::sync::Arc;

use amberlog_common::dataset::{
    Dataset, org_from_path_segment, percent_decode_segment, stream_from_path_segment,
};
use amberlog_common::error::Reason;
use amberlog_storage::store::Store;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, Method, StatusCode, Uri, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::any;
use serde_json::{Value, json};

use crate::gateway::{ApiError, Enveloped};
use crate::telemetry::Metrics;

use projection::{Projection, ReadError};
use route::{ENDPOINTS, Route};

/// 目录投影：对象存储客户端与配置项 `storage.prefix` 的取值。不持有任何跨请求的缓存（FRESH）。
pub struct Catalog {
    projection: Projection,
    prefix: String,
}

impl Catalog {
    /// `prefix` 为配置项 `storage.prefix` 的取值（`/v1/config` 的 `overrides.prefix`，也是 `{prefix}` 段应等于的值）。
    pub fn new(store: Store, prefix: String) -> Arc<Catalog> {
        Arc::new(Catalog {
            projection: Projection::new(store),
            prefix,
        })
    }
}

/// 请求是否属目录前缀（按请求目标中未经百分号解码的路径判定）。路径恰为 `/iceberg` 的不算。
pub fn in_prefix(path: &str) -> bool {
    path.starts_with(route::PREFIX)
}

/// 目录前缀下全部路径（任何方法）交给 [`answer`]。与其他模块不同，这里不经 [`crate::gateway::Endpoint`] 注册：
/// 目录前缀下的路由与方法匹配（含未注册路径回 405）由目录投影自己完成。
pub fn router(catalog: Arc<Catalog>) -> Router {
    Router::new()
        .route(route::PREFIX, any(answer))
        .route("/iceberg/{*rest}", any(answer))
        .with_state(catalog)
}

/// 校验链中某一步的失败：以错误信封类型给出，由 [`shape`] 改写为目录错误体。
type Refusal = Box<ApiError>;

/// 已作答请求所匹配的目录端点名（指标标签 `endpoint`）；没有它的按 `other` 计。
#[derive(Clone, Copy)]
struct Matched(&'static str);

async fn answer(State(catalog): State<Arc<Catalog>>, method: Method, uri: Uri) -> Response {
    let Some(route) = Route::of(uri.path()) else {
        return ApiError::method_not_allowed(HeaderValue::from_static("")).into_response();
    };
    let Some(endpoint) = route.endpoint(&method) else {
        return ApiError::method_not_allowed(HeaderValue::from_static(route.allow()))
            .into_response();
    };
    let head = method == Method::HEAD;
    let mut res = match catalog.dispatch(route, head, uri.query()).await {
        Ok(res) => res,
        Err(e) => (*e).into_response(),
    };
    res.extensions_mut().insert(Matched(endpoint));
    res
}

fn namespace_not_found() -> Refusal {
    Box::new(ApiError::new(
        Reason::NamespaceNotFound,
        "the namespace does not exist",
    ))
}

fn table_not_found() -> Refusal {
    Box::new(ApiError::new(
        Reason::TableNotFound,
        "the table does not exist",
    ))
}

fn no_content() -> Response {
    StatusCode::NO_CONTENT.into_response()
}

impl Catalog {
    async fn dispatch(
        &self,
        route: Route<'_>,
        head: bool,
        query: Option<&str>,
    ) -> Result<Response, Refusal> {
        match route {
            Route::Config => Ok(Json(json!({
                "defaults": {},
                "overrides": {"prefix": self.prefix},
                "endpoints": ENDPOINTS,
            }))
            .into_response()),
            Route::Namespaces { prefix } => {
                self.check_prefix(prefix, namespace_not_found)?;
                if let Some(parent) = route::query_param(query, "parent") {
                    let org = org_from_path_segment(parent).map_err(|_| namespace_not_found())?;
                    if !self.read(self.projection.namespace_exists(&org)).await? {
                        return Err(namespace_not_found());
                    }
                    return Ok(Json(json!({"namespaces": []})).into_response());
                }
                let names = self.read(self.projection.namespaces()).await?;
                let namespaces: Vec<Value> = names.into_iter().map(|n| json!([n])).collect();
                Ok(Json(json!({"namespaces": namespaces})).into_response())
            }
            Route::Namespace { prefix, namespace } => {
                self.check_prefix(prefix, namespace_not_found)?;
                let org = org_from_path_segment(namespace).map_err(|_| namespace_not_found())?;
                if !self.read(self.projection.namespace_exists(&org)).await? {
                    return Err(namespace_not_found());
                }
                Ok(if head {
                    no_content()
                } else {
                    Json(json!({"namespace": [org], "properties": {}})).into_response()
                })
            }
            Route::Tables { prefix, namespace } => {
                self.check_prefix(prefix, namespace_not_found)?;
                let org = org_from_path_segment(namespace).map_err(|_| namespace_not_found())?;
                let tables = self.read(self.projection.tables(&org, false)).await?;
                if tables.is_empty() {
                    return Err(namespace_not_found());
                }
                let identifiers: Vec<Value> = tables
                    .into_iter()
                    .map(|name| json!({"namespace": [org], "name": name}))
                    .collect();
                Ok(Json(json!({"identifiers": identifiers})).into_response())
            }
            Route::Table {
                prefix,
                namespace,
                table,
            } => {
                self.check_prefix(prefix, table_not_found)?;
                let org = org_from_path_segment(namespace).map_err(|_| table_not_found())?;
                let stream = stream_from_path_segment(table).map_err(|_| table_not_found())?;
                let dataset = Dataset::new(org, stream);
                let n = self.read(self.projection.version(&dataset)).await?;
                if n == 0 {
                    return Err(table_not_found());
                }
                if head {
                    return Ok(no_content());
                }
                // 测试专用暂停点：已确定冷层当前版本、尚未读取元数据文件（REQ-AMB-STO-COLD-CATALOG 场景「停机过程中新请求回 503
                // 而在途请求照常完成」）
                amberlog_storage::fault::gate("catalog.after_current_version").await;
                let loaded = self.read(self.projection.load(&dataset, n)).await?;
                Ok(Json(json!({
                    "metadata-location": loaded.metadata_location,
                    "metadata": loaded.metadata,
                    "config": {},
                }))
                .into_response())
            }
        }
    }

    /// `{prefix}` 段解码后须等于 `storage.prefix`，否则按所请求的端点回 404（REQ-AMB-STO-COLD-CATALOG-PREFIX-VALUE）。
    fn check_prefix(&self, raw: &str, not_found: fn() -> Refusal) -> Result<(), Refusal> {
        match percent_decode_segment(raw) {
            Some(p) if p == self.prefix => Ok(()),
            _ => Err(not_found()),
        }
    }

    /// 读取对象存储：不可访问回 503 `unavailable`（READ-INACCESSIBLE），元数据不存在或无法解析回 500 `internal`
    /// （READ-BROKEN）。`error.message` 只用写定的文字，原始错误由出口写进这个 5xx 的 `error` 日志
    /// （REQ-AMB-ERR-ENVELOPE-V7）。
    async fn read<T>(&self, fut: impl Future<Output = Result<T, ReadError>>) -> Result<T, Refusal> {
        fut.await.map_err(|e| {
            Box::new(match e {
                ReadError::Unavailable(m) => {
                    ApiError::new(Reason::Unavailable, "the object store is not accessible")
                        .with_cause(m)
                }
                ReadError::Broken(m) => ApiError::internal().with_cause(format!(
                    "catalog: table metadata is missing or unparseable: {m}"
                )),
            })
        })
    }
}

/// 目录错误体的 `type`（REQ-AMB-STO-COLD-CATALOG-ERR-TYPE-MAP）：与 `reason` 一一对应。目录前缀下只出现这六个 `reason`；
/// 其余按状态码归到 4xx 或 5xx 的通用类型（不应出现）。
fn iceberg_type(reason: Reason, code: u16) -> &'static str {
    match reason {
        Reason::Unauthorized => "NotAuthorizedException",
        Reason::NamespaceNotFound => "NoSuchNamespaceException",
        Reason::TableNotFound => "NoSuchTableException",
        Reason::MethodNotAllowed => "UnsupportedOperationException",
        Reason::Unavailable => "ServiceUnavailableException",
        Reason::Internal => "ServiceFailureException",
        _ if code >= 500 => "ServiceFailureException",
        _ => "BadRequestException",
    }
}

/// 目录前缀下的出口（接入面在停机闸门与认证之外施加）：把错误信封改写为目录错误体（REQ-AMB-STO-COLD-CATALOG-ERR-SHAPE，
/// REQ-AMB-ERR-ENVELOPE-V6 的唯一例外；`HEAD` 不含响应体；`Allow`、`WWW-Authenticate` 等响应头照留），并按端点与状态码
/// 计入 `amberlog_catalog_requests_total`（METRIC）。目录前缀之外的请求原样放过。
pub(crate) async fn shape(
    State(metrics): State<Arc<Metrics>>,
    req: Request,
    next: Next,
) -> Response {
    if !in_prefix(req.uri().path()) {
        return next.run(req).await;
    }
    let head = req.method() == Method::HEAD;
    let res = next.run(req).await;
    let endpoint = res.extensions().get::<Matched>().map_or("other", |m| m.0);
    metrics.record_catalog(endpoint, res.status().as_u16());
    let Some(e) = res.extensions().get::<Enveloped>().cloned() else {
        return res;
    };
    let (mut parts, _) = res.into_parts();
    parts.headers.remove(header::CONTENT_LENGTH);
    if head {
        parts.headers.remove(header::CONTENT_TYPE);
        return Response::from_parts(parts, Body::empty());
    }
    let body = json!({"error": {
        "message": e.message,
        "type": iceberg_type(e.reason, e.code),
        "code": e.code,
        "reason": e.reason.as_str(),
    }});
    parts.headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/json"),
    );
    Response::from_parts(parts, Body::from(body.to_string()))
}
