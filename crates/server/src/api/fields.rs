//! 字段列表端点：`GET /api/{org}/{stream}/_fields`（REQ-AMB-QRY-FIELDS）。
//!
//! 业务在 [`amberlog_query::fields`]；这里只做 HTTP 一侧的适配：`{org}`、`{stream}` 路径段与应答组装。
//!
//! 校验链：方法（只有 `GET`，其余 405）→ `{org}`、`{stream}` 路径段 → 授权的路径一步（[`Access::path_step`]：查询者，
//! 范围外按不存在作答，REQ-AMB-AUTH-AUTHZ-STEP-PATH、-HIDE-READ-SCOPE）→ 状态闸门 → 数据集是否存在。处理器不取请求体，
//! 也不看查询串与 `Content-Encoding`、`Content-Type`；作答只读可见集的当前 schema，不读对象存储，不取查询的执行名额。

use std::sync::Arc;

use amberlog_query::error::QueryError;
use amberlog_query::fields::fields;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::memtable::MemtableView;
use axum::Router;
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri, header};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::Storage;
use super::namespace::dataset_from_path;
use super::search::to_api_error;
use crate::authz::{Access, Denied};
use crate::gateway::{ApiError, Endpoint};

pub fn routes(storage: &Storage) -> Router {
    router(Arc::clone(&storage.memtable), Arc::clone(&storage.gates))
}

/// 以可见集 `memtable` 与闸门 `gates` 注册 `GET /api/{org}/{stream}/_fields`。
pub fn router(memtable: Arc<dyn MemtableView>, gates: Arc<DatasetGates>) -> Router {
    Router::new()
        .route(
            "/api/{org}/{stream}/_fields",
            Endpoint::new().get(handle).into(),
        )
        .with_state(Fields { memtable, gates })
}

#[derive(Clone)]
struct Fields {
    memtable: Arc<dyn MemtableView>,
    gates: Arc<DatasetGates>,
}

async fn handle(
    State(state): State<Fields>,
    access: Access,
    uri: Uri,
    headers: HeaderMap,
) -> Response {
    let dataset = match dataset_from_path(uri.path()) {
        Ok(d) => d,
        Err(rejection) => return rejection.into_response(),
    };
    match access.path_step(Some(&dataset), &headers) {
        Ok(()) => {}
        Err(Denied::Forbidden) => return ApiError::forbidden().into_response(),
        // 与数据集不存在时同一应答（REQ-AMB-AUTH-AUTHZ-HIDE-READ-SCOPE）
        Err(Denied::Absent) => {
            return to_api_error(QueryError::stream_not_found(&dataset.stream)).into_response();
        }
    }
    match fields(state.memtable.as_ref(), &state.gates, &dataset) {
        Ok(columns) => {
            let fields: Vec<_> = columns
                .iter()
                .map(|(name, t)| json!({"name": name, "type": t.as_str()}))
                .collect();
            (
                StatusCode::OK,
                [(header::CONTENT_TYPE, "application/json")],
                json!({ "fields": fields }).to_string(),
            )
                .into_response()
        }
        Err(e) => to_api_error(e).into_response(),
    }
}
