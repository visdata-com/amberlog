//! 查询端点 HTTP 适配的进程内测试：`Engine` 用 `amberlog_storage::testing` 的替身，路由经 `gateway::app` 挂载。
//! 启动 `amberlog` 二进制的系统测试在 `crates/server/tests/qry_search.rs`。

use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_query::request::Limits;
use amberlog_query::search::Engine;
use amberlog_query::testing::FakeFiles;
use amberlog_storage::chain::ChainError;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::schema::{SchemaColumn, StreamColumns};
use amberlog_storage::testing::{FakeChain, FakeMemtable};
use arrow::array::{ArrayRef, Float64Array, TimestampMicrosecondArray};
use arrow::record_batch::RecordBatch;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

use super::router;
use crate::config::Auth;
use crate::gateway::{self, AppState};
use crate::telemetry::{Health, Metrics};

const TOKEN: &str = "s3cret";

struct Fx {
    memtable: Arc<FakeMemtable>,
    chain: Arc<FakeChain>,
    app: Router,
}

/// 请求体上限取 1024 字节（与 REQ-AMB-ING-JSON-ENDPOINT 系统测试的边界场景同值）。
fn fx() -> Fx {
    fx_with_limit(1024)
}

fn fx_with_limit(max_body_bytes: u64) -> Fx {
    let memtable = Arc::new(FakeMemtable::new());
    let chain = Arc::new(FakeChain::new());
    let engine = Engine {
        memtable: memtable.clone(),
        chain: chain.clone(),
        gates: Arc::new(DatasetGates::new()),
        files: Arc::new(FakeFiles::new()),
        cold: Arc::new(amberlog_storage::testing::FakeColdReader::new()),
        limits: Limits::default(),
        admission: amberlog_query::testing::admission(),
        governance: amberlog_query::testing::governance().0,
    };
    let state = AppState {
        health: Arc::new(Health::new(std::env::temp_dir(), "file:///unused".into())),
        metrics: Arc::new(Metrics::default()),
        auth: Arc::new(Auth::Enabled {
            token: TOKEN.into(),
        }),
    };
    let metrics = Arc::clone(&state.metrics);
    let app = gateway::app(
        state,
        vec![router(Arc::new(engine), max_body_bytes, metrics)],
    );
    Fx {
        memtable,
        chain,
        app,
    }
}

impl Fx {
    /// stream `app`：一列 Float64 `g`，一条 `_timestamp` 为 1000、`g` 为 1e300 的记录；尚无快照。
    fn with_app(self) -> Self {
        let ds = Dataset::new("default", "app");
        let columns = StreamColumns::from_columns(vec![
            SchemaColumn {
                name: TIMESTAMP_COLUMN.into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
            SchemaColumn {
                name: "g".into(),
                column_type: ColumnType::Float64,
                field_id: 2,
            },
        ]);
        let batch = RecordBatch::try_new(
            Arc::new(columns.arrow_schema()),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1000]).with_timezone("UTC"))
                    as ArrayRef,
                Arc::new(Float64Array::from(vec![1e300])),
            ],
        )
        .unwrap();
        self.memtable.add_dataset(&ds, columns);
        self.memtable.push_generation(&ds, vec![batch]);
        self.chain.set_current(&ds, Ok(None));
        self
    }

    async fn send(
        &self,
        method: Method,
        uri: &str,
        body: &str,
    ) -> (StatusCode, Vec<u8>, Option<String>) {
        self.send_with(method, uri, &[], body.as_bytes().to_vec())
            .await
    }

    /// 带额外请求头 `headers` 发送原始字节 `body`。
    async fn send_with(
        &self,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> (StatusCode, Vec<u8>, Option<String>) {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let req = req.body(Body::from(body)).unwrap();
        let res = self.app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let ct = res
            .headers()
            .get(header::CONTENT_TYPE)
            .map(|v| v.to_str().unwrap().to_owned());
        let bytes = res.into_body().collect().await.unwrap().to_bytes().to_vec();
        (status, bytes, ct)
    }

    async fn post(&self, uri: &str, body: Value) -> (StatusCode, Value) {
        let (status, bytes, _) = self.send(Method::POST, uri, &body.to_string()).await;
        (status, serde_json::from_slice(&bytes).unwrap())
    }
}

fn q(sql: &str) -> Value {
    json!({"query":{"sql":sql,"start_time":0,"end_time":4_102_444_800_000_000_i64}})
}

#[tokio::test]
async fn route_answers_json_with_float_formatting() {
    let fx = fx().with_app();
    let (status, body, ct) = fx
        .send(
            Method::POST,
            "/api/default/_search",
            &q("SELECT g FROM app").to_string(),
        )
        .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(ct.as_deref(), Some("application/json"));
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains("\"g\":1e300"), "{text}");
    let v: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(v["total"], 1);
}

#[tokio::test]
async fn route_org_is_default_after_percent_decoding() {
    let fx = fx().with_app();
    let (status, body) = fx.post("/api/acme/_search", q("SELECT g FROM app")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["reason"], "org_not_found");
    let (status, _) = fx
        .post("/api/%64efault/_search", q("SELECT g FROM app"))
        .await;
    assert_eq!(status, StatusCode::OK);
    // 只解码一次；解码结果不是合法 UTF-8 也按不匹配处理（不能落到框架的 400）
    for uri in ["/api/%2564efault/_search", "/api/%ff/_search"] {
        let (status, body) = fx.post(uri, q("SELECT g FROM app")).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["reason"], "org_not_found", "{uri}");
    }
}

#[tokio::test]
async fn route_maps_query_errors_to_envelopes() {
    let fx = fx().with_app();
    let (status, bytes, _) = fx.send(Method::POST, "/api/default/_search", "{").await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["reason"],
        "invalid_json"
    );

    for (body, status, reason) in [
        (
            json!({"query":{"sql":"SELECT g FROM app","start_time":3000,"end_time":1000}}),
            400,
            "invalid_time_range",
        ),
        (q("SELEC * FROM app"), 400, "invalid_sql"),
        (q("SELECT lower(g) FROM app"), 400, "sql_unsupported"),
        (q("SELECT * FROM no_such_stream"), 400, "stream_not_found"),
        (q("SELECT typo FROM app"), 400, "unknown_column"),
    ] {
        let (s, v) = fx.post("/api/default/_search", body).await;
        assert_eq!(
            (s.as_u16(), v["reason"].as_str().unwrap()),
            (status, reason)
        );
        assert_eq!(v["code"], status);
    }

    let ds = Dataset::new("default", "app");
    fx.chain.set_current(
        &ds,
        Err(ChainError::Unavailable("connection refused".into())),
    );
    let (s, v) = fx
        .post("/api/default/_search", q("SELECT g FROM app"))
        .await;
    assert_eq!(
        (s, v["reason"].as_str()),
        (StatusCode::SERVICE_UNAVAILABLE, Some("unavailable"))
    );

    fx.chain.set_current(
        &ds,
        Err(ChainError::Integrity("/secret/path corrupt".into())),
    );
    let (s, v) = fx
        .post("/api/default/_search", q("SELECT g FROM app"))
        .await;
    assert_eq!(
        (s, v["reason"].as_str()),
        (StatusCode::INTERNAL_SERVER_ERROR, Some("internal"))
    );
    assert!(
        !v["message"].as_str().unwrap().contains("secret"),
        "500 不带内部原文"
    );
}

/// `/api/` 下 POST 端点共用的请求级检查（REQ-AMB-ING-JSON-ENDPOINT）：415 先于 413，二者都在 `{org}` 之后、
/// 请求体解析之前；`Content-Type` 不看；带 BOM 的请求体按非法 JSON 处理。
#[tokio::test]
async fn route_reads_body_like_other_post_endpoints() {
    let fx = fx().with_app();
    let reason = |bytes: &[u8]| {
        serde_json::from_slice::<Value>(bytes).unwrap()["reason"]
            .as_str()
            .unwrap()
            .to_owned()
    };
    let valid = q("SELECT g FROM app").to_string();
    let padded = |n: usize| {
        let mut b = valid.clone().into_bytes();
        b.resize(n, b' ');
        b
    };

    for (headers, body, status, want) in [
        (
            vec![("content-encoding", "br")],
            valid.clone().into_bytes(),
            415,
            "unsupported_encoding",
        ),
        (
            vec![("content-encoding", "br")],
            padded(1025),
            415,
            "unsupported_encoding",
        ),
        (vec![], padded(1025), 413, "payload_too_large"),
        (vec![], b"{".to_vec(), 400, "invalid_json"),
        (
            vec![],
            [b"\xEF\xBB\xBF".as_slice(), valid.as_bytes()].concat(),
            400,
            "invalid_json",
        ),
    ] {
        let (s, bytes, _) = fx
            .send_with(Method::POST, "/api/default/_search", &headers, body)
            .await;
        assert_eq!(
            (s.as_u16(), reason(&bytes).as_str()),
            (status, want),
            "{headers:?}"
        );
    }
    // {org} 不匹配先于请求体检查
    let (s, bytes, _) = fx
        .send_with(
            Method::POST,
            "/api/acme/_search",
            &[("content-encoding", "br")],
            padded(1025),
        )
        .await;
    assert_eq!(
        (s.as_u16(), reason(&bytes).as_str()),
        (404, "org_not_found")
    );

    for headers in [
        vec![("content-encoding", "identity")],
        vec![("content-encoding", "")],
        vec![("content-type", "text/plain")],
        vec![("content-type", "application/x-www-form-urlencoded")],
    ] {
        let (s, _, _) = fx
            .send_with(Method::POST, "/api/default/_search", &headers, padded(1024))
            .await;
        assert_eq!(s, StatusCode::OK, "{headers:?}");
    }
}

/// 请求体上限只看 `ingest.max_body_bytes`，不受框架默认上限（2 MiB）约束。
#[tokio::test]
async fn route_body_limit_is_the_configured_one() {
    let fx = fx_with_limit(4 << 20).with_app();
    let mut body = q("SELECT g FROM app").to_string().into_bytes();
    body.resize(3 << 20, b' ');
    let (s, _, _) = fx
        .send_with(Method::POST, "/api/default/_search", &[], body)
        .await;
    assert_eq!(s, StatusCode::OK);
}

#[tokio::test]
async fn route_goes_through_gateway_chain() {
    let fx = fx().with_app();
    let (status, _, _) = fx.send(Method::GET, "/api/default/_search", "").await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED);
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/default/_search")
        .body(Body::from(q("SELECT g FROM app").to_string()))
        .unwrap();
    let res = fx.app.clone().oneshot(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

/// 不提供触发 flush 的端点（REQ-AMB-QRY-PLAN-VISIBILITY）：挂上查询端点后，三种 `_flush` 路径仍是 404。
#[tokio::test]
async fn route_offers_no_flush_endpoint() {
    let fx = fx().with_app();
    for uri in [
        "/api/default/app/_flush",
        "/api/default/_flush",
        "/api/_flush",
    ] {
        let (status, body) = fx.post(uri, json!({})).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{uri}");
        assert_eq!(body["reason"], "not_found");
    }
}
