//! 摄取端点：`POST /api/{org}/{stream}/_json`（REQ-AMB-ING-JSON-ENDPOINT、REQ-AMB-ING-ACK-RESPONSE）与 `POST /api/{org}/{stream}/_sent`
//! （REQ-AMB-OBS-INGEST-COUNTERS）。
//!
//! 两者只做 HTTP 一侧的适配，按 REQ-AMB-ERR-ENVELOPE-V5 校验链在停机闸门与认证（接入面）之后依次：
//! `{org}` 与 `{stream}`（REQ-AMB-DM-NAMESPACE，[`super::namespace::dataset_from_path`] 按原始路径段判定）→ 目标数据集状态闸门 →
//! 来源标识（仅 `_json`，REQ-AMB-ING-JSON-ENDPOINT-V3 的 `X-Amberlog-Source`）→
//! `Content-Encoding` 与请求体大小（[`super::body::read_body`]）→ 交摄取管道（`_json`：请求体解析、逐条判定、WAL、应答体；
//! `_sent`：请求体与元素校验、WAL、计入治理计数）。`_sent` 不计摄取指标（REQ-AMB-OBS-HEALTH-METRICS），也不看来源标识请求头。

use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::{ErrorBody, Reason};
use amberlog_ingest::counters::{SentError, valid_source};
use amberlog_ingest::pipeline::{Pipeline, Rejected, SentRejected};
use amberlog_query::system::backfill::BackfillTracker;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::lifecycle;
use amberlog_storage::wal::Basis;
use axum::Json;
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use serde_json::json;

use super::body::read_body;
use super::namespace::dataset_from_path;
use super::{Settings, Storage};
use crate::gateway::{ApiError, AppState, Endpoint};
use crate::telemetry::Metrics;

/// `_json` 与 `_sent` 的路由。
pub fn routes(state: &AppState, storage: &Storage, settings: &Settings) -> Router {
    let pipeline = Pipeline::new(
        Arc::clone(&storage.wal),
        Arc::clone(&storage.memtable),
        Arc::clone(&storage.writer),
        Arc::clone(&storage.committer),
        Arc::clone(&storage.registry),
        settings.window,
        settings.baselines.clone(),
    )
    .with_counters(Arc::clone(&storage.counters))
    .with_max_columns(settings.max_columns)
    .with_lifecycle(settings.lifecycle);
    json_routes(JsonIngest {
        pipeline: Arc::new(pipeline),
        gates: Arc::clone(&storage.gates),
        metrics: Arc::clone(&state.metrics),
        max_body_bytes: settings.max_body_bytes,
        backfill: Arc::clone(&settings.backfill),
    })
}

/// `_json` 的依赖。
#[derive(Clone)]
pub struct JsonIngest {
    pub pipeline: Arc<Pipeline>,
    pub gates: Arc<DatasetGates>,
    pub metrics: Arc<Metrics>,
    /// `ingest.max_body_bytes`。
    pub max_body_bytes: u64,
    /// 漏采判定的补发中状态：确认迟到记录、受理迟到的按时间戳上报与回 503 时记下（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL）。
    pub backfill: Arc<BackfillTracker>,
}

/// `POST /api/{org}/{stream}/_json` 与 `POST /api/{org}/{stream}/_sent` 的路由（其余方法 405）。
/// `_sent` 要求管道已接上治理计数（[`Pipeline::with_counters`]）。
pub fn json_routes(ingest: JsonIngest) -> Router {
    Router::new()
        .route(
            "/api/{org}/{stream}/_json",
            Endpoint::new().post(ingest_json).into(),
        )
        .route(
            "/api/{org}/{stream}/_sent",
            Endpoint::new().post(report_sent).into(),
        )
        .with_state(ingest)
}

/// 来源标识请求头（REQ-AMB-ING-JSON-ENDPOINT-V3）。
const SOURCE_HEADER: &str = "x-amberlog-source";

/// 两个端点共用的请求级检查：路径段 → 数据集状态闸门 → 来源标识（`with_source` 时，即 `_json`）→
/// `Content-Encoding` 与请求体大小。返回数据集、合法的来源标识（未带或不检查时为 `None`）与请求体。
async fn admit(
    ingest: &JsonIngest,
    uri: &Uri,
    headers: &HeaderMap,
    body: Body,
    with_source: bool,
) -> Result<(Dataset, Option<String>, Bytes), Box<Response>> {
    let dataset = dataset_from_path(uri.path()).map_err(|r| Box::new(r.into_response()))?;
    if let Some(reason) = ingest.gates.state(&dataset).reason() {
        return Err(Box::new(match reason {
            Reason::Internal => ApiError::internal()
                .with_cause(format!("dataset {dataset} is disabled: its manifest is unreadable")),
            _ => ApiError::new(
                reason,
                "the dataset was written in a newer format version; ingestion is stopped until restart",
            )
            .with_cause(format!(
                "dataset {dataset} is disabled: its format version is not supported"
            )),
        }
        .into_response()));
    }
    let source = if with_source {
        source_header(headers)?
    } else {
        None
    };
    let bytes = read_body(headers, body, ingest.max_body_bytes)
        .await
        .map_err(|e| Box::new(e.into_response()))?;
    Ok((dataset, source, bytes))
}

/// `X-Amberlog-Source`：至多一个，取值匹配来源标识的取值规则（与 `_sent` 元素的 `source` 共用 [`valid_source`]）；
/// 不合时 400 `invalid_request`。
fn source_header(headers: &HeaderMap) -> Result<Option<String>, Box<Response>> {
    let mut values = headers.get_all(SOURCE_HEADER).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    match value.to_str() {
        Ok(s) if values.next().is_none() && valid_source(s) => Ok(Some(s.to_owned())),
        _ => Err(Box::new(
            ApiError::new(
                Reason::InvalidRequest,
                "X-Amberlog-Source must appear at most once and match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$",
            )
            .into_response(),
        )),
    }
}

async fn report_sent(
    State(ingest): State<JsonIngest>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let received_at_us = now_us();
    let (dataset, _, bytes) = match admit(&ingest, &uri, &headers, body, false).await {
        Ok(v) => v,
        Err(r) => return *r,
    };
    let response = match ingest.pipeline.report_sent(&dataset, &bytes).await {
        Ok(accepted) => {
            let timestamped = accepted
                .iter()
                .filter(|e| e.basis == Basis::Timestamp)
                .map(|e| (e.source.as_str(), e.window_start_us));
            ingest
                .backfill
                .note_reports(&dataset, timestamped, received_at_us, now_us());
            Json(json!({ "code": 200, "accepted": accepted.len() })).into_response()
        }
        Err(SentRejected::Invalid(SentError::InvalidJson)) => {
            ApiError::new(Reason::InvalidJson, "the request body is not valid JSON").into_response()
        }
        Err(SentRejected::Invalid(SentError::InvalidRequest(why))) => {
            ApiError::new(Reason::InvalidRequest, why).into_response()
        }
        Err(SentRejected::Unavailable(cause)) => ApiError::new(
            Reason::Unavailable,
            "the report could not be made durable; it was not accepted and may be retried",
        )
        .with_cause(cause)
        .into_response(),
    };
    note_unavailable(&ingest, &dataset, &response);
    response
}

/// 对发往该数据集的请求回了 503：记进补发中状态（不论来源）。
fn note_unavailable(ingest: &JsonIngest, dataset: &Dataset, response: &Response) {
    if response.status() == StatusCode::SERVICE_UNAVAILABLE {
        ingest.backfill.note_unavailable(dataset, now_us());
    }
}

async fn ingest_json(
    State(ingest): State<JsonIngest>,
    uri: Uri,
    headers: HeaderMap,
    body: Body,
) -> Response {
    let received_at_us = now_us();
    let (dataset, source, bytes) = match admit(&ingest, &uri, &headers, body, true).await {
        Ok(v) => v,
        Err(r) => return *r,
    };
    let response = match ingest
        .pipeline
        .ingest_from(&dataset, &bytes, received_at_us, source.clone())
        .await
    {
        Ok(status) => {
            if let (Some(source), Some(earliest)) = (&source, status.earliest_timestamp_us) {
                ingest
                    .backfill
                    .note_records(&dataset, source, earliest, received_at_us, now_us());
            }
            ingest.metrics.record_ingest(
                &dataset.org,
                &dataset.stream,
                status.successful,
                status.failed,
            );
            ingest.metrics.record_late(
                &dataset.org,
                &dataset.stream,
                status.late.sealed,
                status.late.pending_lake,
            );
            Json(status.ok_body()).into_response()
        }
        Err(rejected) => {
            if let Rejected::AllRecordsRejected(status) = &rejected {
                ingest
                    .metrics
                    .record_ingest(&dataset.org, &dataset.stream, 0, status.failed);
            }
            let e = envelope(rejected.error_body());
            match rejected {
                Rejected::Unavailable(cause) => e.with_cause(cause),
                _ => e,
            }
            .into_response()
        }
    };
    note_unavailable(&ingest, &dataset, &response);
    response
}

fn envelope(body: ErrorBody) -> ApiError {
    let e = ApiError::new(body.reason, body.message);
    match body.details {
        Some(details) => e.with_details(details),
        None => e,
    }
}

/// 接收时刻：与桶状态判定共用同一个时钟（[`lifecycle::now_us`]）。
fn now_us() -> i64 {
    lifecycle::now_us()
}
