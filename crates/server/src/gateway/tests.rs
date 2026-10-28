//! 接入面错误处置的进程内测试。路由是测试桩，不是对外端点；
//! 启动 `amberlog` 二进制的系统测试在 `crates/server/tests/err_envelope.rs`。

use std::collections::BTreeSet;
use std::io::{Read, Write};

use amberlog_common::error::Reason;
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use axum::response::Response;
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

use super::{ApiError, Endpoint, X_REQUEST_ID, finish};

fn app() -> Router {
    let router = Router::new()
        .route("/probe", Endpoint::new().get(|| async { "ok" }).into())
        .route("/submit", Endpoint::new().post(|| async { "ok" }).into())
        .route(
            "/both",
            Endpoint::new()
                .get(|| async { "ok" })
                .post(|| async { "ok" })
                .into(),
        )
        .route(
            "/plain-413",
            Endpoint::new()
                .get(|| async { (StatusCode::PAYLOAD_TOO_LARGE, "too big") })
                .into(),
        )
        .route(
            "/plain-400",
            Endpoint::new()
                .get(|| async { (StatusCode::BAD_REQUEST, "rejected by extractor") })
                .into(),
        )
        .route(
            "/unauthorized",
            Endpoint::new()
                .get(|| async { ApiError::new(Reason::Unauthorized, "missing credential") })
                .into(),
        )
        .route(
            "/internal",
            Endpoint::new()
                .get(|| async { ApiError::internal() })
                .into(),
        )
        .route("/panic", Endpoint::new().get(explode).into());
    finish(router)
}

async fn explode() -> &'static str {
    panic!("handler exploded at /secret/path")
}

async fn send(method: Method, uri: &str, request_id: Option<&str>) -> Response {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(id) = request_id {
        req = req.header(X_REQUEST_ID, id);
    }
    app()
        .oneshot(req.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn envelope(res: Response) -> Value {
    let bytes = res.into_body().collect().await.unwrap().to_bytes();
    serde_json::from_slice(&bytes).expect("错误响应体应为 JSON")
}

fn allow_set(res: &Response) -> BTreeSet<String> {
    res.headers()
        .get(header::ALLOW)
        .expect("405 应带 Allow")
        .to_str()
        .unwrap()
        .split(',')
        .map(|m| m.trim().to_owned())
        .collect()
}

/// 信封不变量：Content-Type、`code` 等于状态码、`reason` 与 `message` 为字符串、带 `x-request-id`。
async fn assert_envelope(res: Response, status: StatusCode, reason: &str) -> Value {
    assert_eq!(res.status(), status);
    assert_eq!(res.headers()[header::CONTENT_TYPE], "application/json");
    assert!(res.headers().contains_key(X_REQUEST_ID));
    let v = envelope(res).await;
    assert_eq!(v["code"], status.as_u16());
    assert_eq!(v["reason"], reason);
    assert!(v["message"].is_string());
    v
}

#[tokio::test]
async fn request_id_echoed() {
    let res = send(Method::GET, "/probe", Some("abc-123")).await;
    assert_eq!(res.headers()[X_REQUEST_ID], "abc-123");
    let res = send(Method::GET, "/nope", Some("abc-123")).await;
    assert_eq!(res.headers()[X_REQUEST_ID], "abc-123");
}

#[tokio::test]
async fn request_id_generated_when_absent() {
    let a = send(Method::GET, "/probe", None).await;
    let b = send(Method::GET, "/probe", None).await;
    assert_eq!(a.status(), StatusCode::OK);
    let (a, b) = (&a.headers()[X_REQUEST_ID], &b.headers()[X_REQUEST_ID]);
    assert!(!a.is_empty());
    assert_ne!(a, b);
}

#[tokio::test]
async fn unregistered_path_is_not_found_envelope() {
    assert_envelope(
        send(Method::GET, "/nope", None).await,
        StatusCode::NOT_FOUND,
        "not_found",
    )
    .await;
    assert_envelope(
        send(Method::POST, "/api/default/nope", None).await,
        StatusCode::NOT_FOUND,
        "not_found",
    )
    .await;
}

#[tokio::test]
async fn unsupported_methods_are_405_with_allow() {
    for method in [
        Method::POST,
        Method::HEAD,
        Method::OPTIONS,
        Method::PUT,
        Method::DELETE,
    ] {
        let res = send(method.clone(), "/probe", None).await;
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED, "{method}");
        assert_eq!(
            allow_set(&res),
            BTreeSet::from(["GET".to_owned()]),
            "{method}"
        );
        assert_eq!(res.headers()[header::CONTENT_TYPE], "application/json");
        if method != Method::HEAD {
            assert_envelope(res, StatusCode::METHOD_NOT_ALLOWED, "method_not_allowed").await;
        }
    }
    let res = send(Method::GET, "/submit", None).await;
    assert_eq!(allow_set(&res), BTreeSet::from(["POST".to_owned()]));
    let res = send(Method::DELETE, "/both", None).await;
    assert_eq!(
        allow_set(&res),
        BTreeSet::from(["GET".to_owned(), "POST".to_owned()])
    );
}

#[tokio::test]
async fn framework_errors_rewritten_to_envelope() {
    assert_envelope(
        send(Method::GET, "/plain-413", None).await,
        StatusCode::PAYLOAD_TOO_LARGE,
        "payload_too_large",
    )
    .await;
    // 框架的其他拒绝透出来属于端点缺陷，按未归类的服务端错误处理
    assert_envelope(
        send(Method::GET, "/plain-400", None).await,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
    )
    .await;
}

#[tokio::test]
async fn unauthorized_carries_www_authenticate() {
    let res = send(Method::GET, "/unauthorized", None).await;
    assert_eq!(res.headers()[header::WWW_AUTHENTICATE], "Bearer");
    assert_envelope(res, StatusCode::UNAUTHORIZED, "unauthorized").await;
    let res = send(Method::GET, "/nope", None).await;
    assert!(!res.headers().contains_key(header::WWW_AUTHENTICATE));
}

#[tokio::test]
async fn internal_error_message_is_fixed() {
    let v = assert_envelope(
        send(Method::GET, "/internal", None).await,
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal",
    )
    .await;
    assert!(v.get("details").is_none());
}

/// 经真实 HTTP/1.1 连接发 HEAD：状态码、`Allow`、`Content-Type` 与 `x-request-id` 照常，
/// 但响应不带体——HTTP 规定 HEAD 的响应没有内容，axum 与 hyper 都会丢弃响应体。
#[tokio::test(flavor = "multi_thread")]
async fn head_405_has_headers_but_no_body_on_the_wire() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app()).await.unwrap() });
    let raw = tokio::task::spawn_blocking(move || {
        let mut s = std::net::TcpStream::connect(addr).unwrap();
        s.write_all(b"HEAD /probe HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n")
            .unwrap();
        let mut out = String::new();
        s.read_to_string(&mut out).unwrap();
        out
    })
    .await
    .unwrap();
    let (head, body) = raw.split_once("\r\n\r\n").unwrap();
    let head = head.to_ascii_lowercase();
    assert!(head.starts_with("http/1.1 405"), "{head}");
    assert!(head.contains("\r\nallow: get"), "{head}");
    assert!(
        head.contains("\r\ncontent-type: application/json"),
        "{head}"
    );
    assert!(head.contains("\r\nx-request-id: "), "{head}");
    assert_eq!(body, "", "HEAD 响应不应带体");
}

/// 处理函数 panic 按未归类的服务端错误作答：500 `internal`、固定文字，panic 文字不进响应。
#[tokio::test]
async fn handler_panic_is_internal_envelope() {
    let res = send(Method::GET, "/panic", Some("rid-1")).await;
    assert_eq!(res.headers()[X_REQUEST_ID], "rid-1");
    let v = assert_envelope(res, StatusCode::INTERNAL_SERVER_ERROR, "internal").await;
    assert_eq!(v["message"], "internal server error");
    assert!(!v.to_string().contains("exploded"), "{v}");
}

/// 经真实 HTTP/1.1 连接：panic 的请求收到 500 响应而不是连接被关，之后的请求照常得到服务。
#[tokio::test(flavor = "multi_thread")]
async fn handler_panic_answers_on_the_wire_and_keeps_serving() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app()).await.unwrap() });
    let get = move |path: &'static str| {
        tokio::task::spawn_blocking(move || {
            let mut s = std::net::TcpStream::connect(addr).unwrap();
            write!(
                s,
                "GET {path} HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n"
            )
            .unwrap();
            let mut out = String::new();
            s.read_to_string(&mut out).unwrap();
            out
        })
    };
    for _ in 0..2 {
        let raw = get("/panic").await.unwrap();
        assert!(raw.starts_with("HTTP/1.1 500"), "{raw:?}");
        let (_, body) = raw.split_once("\r\n\r\n").unwrap();
        let v: Value = serde_json::from_str(body).unwrap();
        assert_eq!(v["reason"], "internal");
        let raw = get("/probe").await.unwrap();
        assert!(raw.starts_with("HTTP/1.1 200"), "{raw:?}");
    }
}

// ---------------------------------------------------------------------------
// 模块挂载（`app` 的 `modules` 参数）
// ---------------------------------------------------------------------------

fn app_state(auth: crate::config::Auth) -> super::AppState {
    super::AppState {
        health: std::sync::Arc::new(crate::telemetry::Health::new(
            std::env::temp_dir(),
            "file:///unused".into(),
        )),
        metrics: std::sync::Arc::new(crate::telemetry::Metrics::default()),
        auth: std::sync::Arc::new(auth),
    }
}

/// 一个自带状态的测试模块：`POST /api/{org}/_probe` 回模块状态里的字符串。
fn probe_module() -> Router {
    use axum::extract::State;
    Router::new()
        .route(
            "/api/{org}/_probe",
            Endpoint::new()
                .post(|State(s): State<std::sync::Arc<str>>| async move { s.to_string() })
                .into(),
        )
        .with_state(std::sync::Arc::<str>::from("module-state"))
}

async fn send_to(app: Router, method: Method, uri: &str, bearer: Option<&str>) -> Response {
    let mut req = Request::builder().method(method).uri(uri);
    if let Some(token) = bearer {
        req = req.header(header::AUTHORIZATION, format!("Bearer {token}"));
    }
    app.oneshot(req.body(Body::empty()).unwrap()).await.unwrap()
}

/// 模块只注册路径与自己的状态；认证区、405/`Allow`、404 兜底与信封由 `app` 统一施加。
#[tokio::test]
async fn mounted_module_gets_uniform_gateway_handling() {
    let token = "s3cret";
    let state = app_state(crate::config::Auth::Enabled {
        token: token.into(),
    });
    let app = super::app(state, vec![probe_module()]);

    let res = send_to(
        app.clone(),
        Method::POST,
        "/api/default/_probe",
        Some(token),
    )
    .await;
    assert_eq!(res.status(), StatusCode::OK);
    assert_eq!(
        res.into_body().collect().await.unwrap().to_bytes(),
        "module-state"
    );

    let res = send_to(app.clone(), Method::POST, "/api/default/_probe", None).await;
    assert_envelope(res, StatusCode::UNAUTHORIZED, "unauthorized").await;

    for method in [Method::GET, Method::HEAD] {
        let res = send_to(app.clone(), method, "/api/default/_probe", Some(token)).await;
        assert_eq!(res.status(), StatusCode::METHOD_NOT_ALLOWED);
        assert_eq!(allow_set(&res), BTreeSet::from(["POST".to_owned()]));
    }

    let res = send_to(app.clone(), Method::POST, "/api/default/_nope", Some(token)).await;
    assert_envelope(res, StatusCode::NOT_FOUND, "not_found").await;

    let res = send_to(app.clone(), Method::GET, "/healthz", None).await;
    assert_eq!(res.status(), StatusCode::OK, "接入面自己的端点照常");
}

/// 挂载模块里的 panic 同样接住：认证区内回 500 信封，目录前缀下由出口改写为目录错误体；在途计数照常归还。
#[tokio::test]
async fn mounted_module_panic_is_internal() {
    let state = app_state(crate::config::Auth::Disabled);
    let module = Router::new()
        .route("/api/{org}/_boom", Endpoint::new().post(explode).into())
        .route("/iceberg/v1/config", Endpoint::new().get(explode).into());
    let app = super::app(state.clone(), vec![module]);

    let res = send_to(app.clone(), Method::POST, "/api/default/_boom", None).await;
    assert_envelope(res, StatusCode::INTERNAL_SERVER_ERROR, "internal").await;

    let res = send_to(app.clone(), Method::GET, "/iceberg/v1/config", None).await;
    assert_eq!(res.status(), StatusCode::INTERNAL_SERVER_ERROR);
    let v = envelope(res).await;
    assert_eq!(v["error"]["code"], 500);
    assert_eq!(v["error"]["reason"], "internal");
    assert!(!v.to_string().contains("exploded"), "{v}");

    state.health.begin_shutdown();
    assert!(
        state
            .health
            .drained(std::time::Duration::from_millis(500))
            .await,
        "panic 的请求应归还在途计数"
    );
}

/// 停机闸门对挂载的模块同样排在认证之前。
#[tokio::test]
async fn mounted_module_is_behind_shutdown_gate() {
    let state = app_state(crate::config::Auth::Enabled { token: "t".into() });
    state.health.begin_shutdown();
    let app = super::app(state, vec![probe_module()]);
    let res = send_to(app, Method::POST, "/api/default/_probe", None).await;
    assert!(!res.headers().contains_key(header::WWW_AUTHENTICATE));
    assert_envelope(res, StatusCode::SERVICE_UNAVAILABLE, "unavailable").await;
}

/// `/api/` 模块表能以各接口的替身构造并挂进 `app`。
#[tokio::test]
async fn api_routes_mount_with_storage_fakes() {
    use amberlog_common::dataset::Dataset;
    use amberlog_query::files::{DataFiles, FileError};
    use amberlog_storage::BoxFuture;
    use amberlog_storage::chain::FileEntry;
    use amberlog_storage::gate::DatasetGates;
    use amberlog_storage::registry::DatasetRegistry;
    use amberlog_storage::testing::{FakeChain, FakeCommitter, FakeMemtable, FakeWal};
    use std::sync::Arc;

    struct NoFiles;

    impl DataFiles for NoFiles {
        fn read<'a>(
            &'a self,
            _dataset: &'a Dataset,
            file: &'a FileEntry,
            _columns: &'a [String],
            _time: std::ops::Range<i64>,
        ) -> BoxFuture<'a, Result<amberlog_query::files::FileBatches, FileError>> {
            let e = FileError::NotFound(format!("{} not set", file.path));
            Box::pin(std::future::ready(Err(e)))
        }
    }

    let state = app_state(crate::config::Auth::Disabled);
    let memtable = Arc::new(FakeMemtable::new());
    let storage = crate::api::Storage {
        wal: Arc::new(FakeWal::new()),
        memtable: memtable.clone(),
        committer: Arc::new(FakeCommitter::new()),
        chain: Arc::new(FakeChain::new()),
        gates: Arc::new(DatasetGates::new()),
        writer: memtable,
        registry: Arc::new(DatasetRegistry::new()),
        files: Arc::new(NoFiles),
        cold: Arc::new(amberlog_storage::testing::FakeColdReader::new()),
        counters: Arc::new(amberlog_ingest::counters::Counters::new(
            amberlog_storage::store::Store::open(
                &amberlog_storage::store::Location::File {
                    dir: std::env::temp_dir(),
                },
                &amberlog_storage::store::S3Options::default(),
                "amberlog-unused",
            )
            .unwrap(),
            Arc::new(FakeWal::new()),
        )),
        alerts: Arc::new(amberlog_query::testing::FakeAlertHistory::new()),
    };
    let settings = crate::api::Settings::default();
    let app = super::app(
        state.clone(),
        crate::api::routes(&state, &storage, &settings),
    );
    let res = send_to(app, Method::GET, "/healthz", None).await;
    assert_eq!(res.status(), StatusCode::OK);
}
