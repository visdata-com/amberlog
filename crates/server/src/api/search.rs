//! 查询端点：`POST /api/{org}/_search`（REQ-AMB-QRY-SEARCH-ENDPOINT-V4）。
//!
//! 业务在 [`amberlog_query::search::Engine`]；这里只做 HTTP 一侧的适配：`{org}` 路径段、请求体字节、应答组装。
//!
//! 校验链：`{org}` 路径段 → `Content-Encoding` 与请求体大小（[`super::body::read_body`]，REQ-AMB-ING-JSON-ENDPOINT 对
//! `/api/` 下全部 POST 端点的规定，`Content-Type` 不看）→ 交 `Engine`（请求体是否合法 JSON 及其后各步）。
//!
//! [`routes`] 以 [`super::Storage`] 中的可见集、快照链、闸门与数据文件句柄，和 [`super::Settings`] 中的
//! `query.default_size`/`query.max_size` 与 `ingest.max_body_bytes` 经 [`router`] 注册（接线见启动流程）。
//! 系统表所需的计数对象读回与桶状态判定在 [`governance`] 里接到 amberlog-query 的窄接口上。

use std::sync::Arc;

use amberlog_common::error::Reason;
use amberlog_query::error::QueryError;
use amberlog_query::search::Engine;
use axum::Router;
use axum::body::Body;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};

use super::body::read_body;
use super::namespace::org_from_path;
use super::{Settings, Storage};
use crate::gateway::{ApiError, AppState, Endpoint};
use crate::telemetry::Metrics;

pub fn routes(state: &AppState, storage: &Storage, settings: &Settings) -> Router {
    router(
        Arc::new(engine(storage, settings)),
        settings.max_body_bytes,
        Arc::clone(&state.metrics),
    )
}

/// 查询规划与执行的句柄：`_search` 与告警评估的查询（REQ-AMB-ALR-RULES-QUERY-RESULT）各持一份，共用同一组存储句柄与
/// 同一份执行名额 `settings.admission`。
pub fn engine(storage: &Storage, settings: &Settings) -> Engine {
    Engine {
        memtable: Arc::clone(&storage.memtable),
        chain: Arc::clone(&storage.chain),
        gates: Arc::clone(&storage.gates),
        files: Arc::clone(&storage.files),
        cold: Arc::clone(&storage.cold),
        limits: settings.query,
        governance: governance::governance(storage, settings),
        admission: Arc::clone(&settings.admission),
    }
}

/// `_search` 的依赖。
#[derive(Clone)]
struct Search {
    engine: Arc<Engine>,
    /// `ingest.max_body_bytes`。
    max_body_bytes: u64,
    /// 越限拒绝计数（`amberlog_search_rejected_total`）。
    metrics: Arc<Metrics>,
}

/// 以 `engine` 与请求体上限 `max_body_bytes`（`ingest.max_body_bytes`）注册 `POST /api/{org}/_search`（其余方法 405）；
/// 越限拒绝计入 `metrics`。
pub fn router(engine: Arc<Engine>, max_body_bytes: u64, metrics: Arc<Metrics>) -> Router {
    Router::new()
        .route("/api/{org}/_search", Endpoint::new().post(search).into())
        .with_state(Search {
            engine,
            max_body_bytes,
            metrics,
        })
}

async fn search(
    State(search): State<Search>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let org = match org_from_path(uri.path()) {
        Ok(org) => org,
        Err(rejection) => return rejection.into_response(),
    };
    let bytes = match read_body(&headers, body, search.max_body_bytes).await {
        Ok(b) => b,
        Err(e) => return e.into_response(),
    };
    match search.engine.search(&org, &bytes).await {
        Ok(r) => (
            StatusCode::OK,
            [(header::CONTENT_TYPE, "application/json")],
            r.to_body(),
        )
            .into_response(),
        Err(e) => {
            search.metrics.record_search_error(e.reason);
            to_api_error(e).into_response()
        }
    }
}

/// 查询侧的错误转错误信封（字段列表端点同用）。5xx 的 `message` 按情形写定，查询侧的说明（含对象存储的原始错误）只作
/// 出错原因交给出口写进日志（REQ-AMB-ERR-ENVELOPE-V7）；4xx 的说明照旧进 `message`。
pub(crate) fn to_api_error(e: QueryError) -> ApiError {
    let message = match e.reason {
        Reason::Internal => return ApiError::internal().with_cause(e.message),
        Reason::Unavailable if e.moved => {
            "data moved while the query ran; the query may be retried"
        }
        Reason::Unavailable => "the object store is not accessible; the query may be retried",
        Reason::UnsupportedFormatVersion => {
            "the dataset was written in a format version this program does not support; it is disabled until restart"
        }
        r if r.status() >= 500 => "the query could not be answered",
        _ => return ApiError::new(e.reason, e.message),
    };
    ApiError::new(e.reason, message).with_cause(e.message)
}

pub(crate) mod governance;
#[cfg(test)]
mod tests;
