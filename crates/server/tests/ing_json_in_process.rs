//! `_json` 端点的进程内测试：经接入面 `gateway::app` 挂载 `api::ingest::json_routes`，WAL 与快照提交用替身、
//! 可见集用真实实现，以 tower 直接发请求。
//!
//! 对应 REQ-AMB-ING-JSON-ENDPOINT 与 REQ-AMB-ING-ACK-RESPONSE 中只涉及 `_json` 的场景；
//! 黑盒测试（启动二进制、经 `_search` 查询）在 `ing_json_endpoint.rs` 与 `ing_ack_response.rs`，本文件的测试名与它们不重名。

use std::collections::HashMap;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_ingest::pipeline::Pipeline;
use amberlog_ingest::timestamp::TimestampWindow;
use amberlog_server::api::ingest::{JsonIngest, json_routes};
use amberlog_server::config::Auth;
use amberlog_server::gateway::{self, AppState};
use amberlog_server::telemetry::{Health, Metrics};
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::memtable::{Memtable, MemtableView};
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::testing::{FakeCommitter, FakeWal};
use arrow::array::{Array, Int64Array, StringArray};
use axum::Router;
use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::Value;
use tower::ServiceExt;

const TOKEN: &str = "s3cret";

struct World {
    app: Router,
    wal: Arc<FakeWal>,
    memtable: Arc<Memtable>,
    gates: Arc<DatasetGates>,
    metrics: Arc<Metrics>,
}

fn world(max_body_bytes: u64) -> World {
    let wal = Arc::new(FakeWal::new());
    let memtable = Arc::new(Memtable::new());
    let gates = Arc::new(DatasetGates::new());
    let metrics = Arc::new(Metrics::default());
    let pipeline = Arc::new(Pipeline::new(
        wal.clone(),
        memtable.clone(),
        memtable.clone(),
        Arc::new(FakeCommitter::new()),
        Arc::new(DatasetRegistry::new()),
        TimestampWindow::default(),
        HashMap::new(),
    ));
    let state = AppState {
        health: Arc::new(Health::new(std::env::temp_dir(), "file:///unused".into())),
        metrics: metrics.clone(),
        auth: Arc::new(Auth::Enabled {
            token: TOKEN.into(),
        }),
    };
    let module = json_routes(JsonIngest {
        pipeline,
        gates: gates.clone(),
        metrics: metrics.clone(),
        max_body_bytes,
        backfill: Default::default(),
    });
    World {
        app: gateway::app(state, vec![module]),
        wal,
        memtable,
        gates,
        metrics,
    }
}

struct Reply {
    status: StatusCode,
    body: Value,
}

impl World {
    async fn send(
        &self,
        method: Method,
        uri: &str,
        headers: &[(&str, &str)],
        body: Vec<u8>,
    ) -> Reply {
        let mut req = Request::builder()
            .method(method)
            .uri(uri)
            .header(header::AUTHORIZATION, format!("Bearer {TOKEN}"));
        for (k, v) in headers {
            req = req.header(*k, *v);
        }
        let res = self
            .app
            .clone()
            .oneshot(req.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        let status = res.status();
        let bytes = res.into_body().collect().await.unwrap().to_bytes();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        Reply { status, body }
    }

    async fn post(&self, stream: &str, body: &str) -> Reply {
        self.post_with(stream, &[], body.as_bytes().to_vec()).await
    }

    async fn post_with(&self, stream: &str, headers: &[(&str, &str)], body: Vec<u8>) -> Reply {
        self.send(
            Method::POST,
            &format!("/api/default/{stream}/_json"),
            headers,
            body,
        )
        .await
    }

    /// 可见集中该 stream 的全部行（无快照时即 `SELECT` 的结果），以 (列名 → 取值文本) 表示。
    fn rows(&self, stream: &str) -> Vec<HashMap<String, String>> {
        let mut out = Vec::new();
        for batch in self
            .memtable
            .candidates(&Dataset::new("default", stream))
            .participating(None)
        {
            for i in 0..batch.num_rows() {
                let mut row = HashMap::new();
                for (f, col) in batch.schema().fields().iter().zip(batch.columns()) {
                    if col.is_null(i) {
                        continue;
                    }
                    let v = if let Some(s) = col.as_any().downcast_ref::<StringArray>() {
                        s.value(i).to_owned()
                    } else if let Some(n) = col.as_any().downcast_ref::<Int64Array>() {
                        n.value(i).to_string()
                    } else {
                        continue;
                    };
                    row.insert(f.name().clone(), v);
                }
                out.push(row);
            }
        }
        out
    }
}

fn assert_reason(r: &Reply, status: u16, reason: &str) {
    assert_eq!(r.status.as_u16(), status, "{}", r.body);
    assert_eq!(r.body["code"], status);
    assert_eq!(r.body["reason"], reason);
    assert!(r.body["message"].is_string());
}

#[tokio::test]
async fn json_array_single_object_and_scalars() {
    let w = world(33_554_432);
    let r = w.post("app", r#"[{"a":1},{"a":2},{"a":3}]"#).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"][0]["successful"], 3);

    let r = w.post("app", r#"{"a":1}"#).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"][0]["successful"], 1);

    w.post("ts", r#"{"ok":1}"#).await;
    for scalar in [r#""x""#, "5", "true", "null"] {
        let r = w.post("ts", scalar).await;
        assert_reason(&r, 400, "all_records_rejected");
        let s = &r.body["details"]["status"][0];
        assert_eq!(s["failed"], 1, "{scalar}");
        assert_eq!(s["errors"][0]["index"], 0);
        assert_eq!(s["errors"][0]["reason"], "not_an_object");
    }
}

#[tokio::test]
async fn content_encoding_other_than_identity_is_415() {
    let w = world(33_554_432);
    w.post("ce", r#"{"a":1}"#).await;
    for (value, status) in [("identity", 200), ("", 200), ("br", 415), ("deflate", 415)] {
        let r = w
            .post_with(
                "ce",
                &[("content-encoding", value)],
                br#"[{"a":1}]"#.to_vec(),
            )
            .await;
        assert_eq!(r.status.as_u16(), status, "Content-Encoding: {value:?}");
        if status == 415 {
            assert_reason(&r, 415, "unsupported_encoding");
        }
    }
}

#[tokio::test]
async fn body_size_limit_is_inclusive() {
    let w = world(1024);
    w.post("mb", r#"{"a":1}"#).await;
    let padded = |n: usize| {
        let mut b = br#"[{"a":1}]"#.to_vec();
        b.resize(n, b' ');
        b
    };
    let r = w.post_with("mb", &[], padded(1024)).await;
    assert_eq!(r.status, StatusCode::OK);
    let r = w.post_with("mb", &[], padded(1025)).await;
    assert_reason(&r, 413, "payload_too_large");
    // 请求没有 Content-Length：按读到的字节计，读到超出即停
}

#[tokio::test]
async fn content_type_is_ignored() {
    let w = world(33_554_432);
    w.post("ct", r#"{"a":1}"#).await;
    for headers in [
        &[("content-type", "text/plain")][..],
        &[("content-type", "application/x-www-form-urlencoded")],
        &[],
    ] {
        let r = w.post_with("ct", headers, br#"[{"a":1}]"#.to_vec()).await;
        assert_eq!(r.status, StatusCode::OK, "{headers:?}");
    }
}

#[tokio::test]
async fn invalid_bodies_are_400_and_store_nothing() {
    let w = world(33_554_432);
    w.post("iv", r#"{"b":0}"#).await;
    for body in [r#"[{"a":1},{"a":"#, r#"[{"a":1},{"b":1e400},{"c":]"#] {
        let r = w.post("iv", body).await;
        assert_reason(&r, 400, "invalid_json");
    }
    let bom = [b"\xEF\xBB\xBF".as_slice(), br#"[{"a":1}]"#].concat();
    assert_reason(&w.post_with("iv", &[], bom).await, 400, "invalid_json");
    assert!(
        w.rows("iv").iter().all(|r| !r.contains_key("a")),
        "整体非法的请求体不落任何记录"
    );
}

#[tokio::test]
async fn identical_records_are_all_kept() {
    let w = world(33_554_432);
    let r = w
        .post(
            "app",
            r#"[{"_timestamp":1700000000000000,"m":"dup"},{"_timestamp":1700000000000000,"m":"dup"}]"#,
        )
        .await;
    assert_eq!(r.status, StatusCode::OK);
    let dups = w
        .rows("app")
        .into_iter()
        .filter(|r| r.get("m").map(String::as_str) == Some("dup"))
        .count();
    assert_eq!(dups, 2);
}

#[tokio::test]
async fn ack_shapes_through_http() {
    let w = world(33_554_432);
    let r = w.post("app", r#"[{"a":1},"not-an-object",{"a":3}]"#).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["code"], 200);
    assert_eq!(
        (
            &r.body["status"][0]["successful"],
            &r.body["status"][0]["failed"]
        ),
        (&Value::from(2), &Value::from(1))
    );
    assert_eq!(r.body["status"][0]["errors"][0]["reason"], "not_an_object");

    let r = w.post("app", "[]").await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(r.body["status"][0]["successful"], 0);

    let r = w.post("app", r#"["x","y"]"#).await;
    assert_reason(&r, 400, "all_records_rejected");
    assert_eq!(r.body["details"]["status"][0]["failed"], 2);

    w.wal.set_failing(true);
    let r = w.post("app", r#"[{"a":9}]"#).await;
    assert_reason(&r, 503, "unavailable");
    w.wal.set_failing(false);
    assert!(
        w.rows("app")
            .iter()
            .all(|r| r.get("a").map(String::as_str) != Some("9"))
    );

    // 摄取指标按逐条结果递增：200 的 3 + 0，400 全拒的 2；503 与 invalid_json 不计
    let text = w.metrics.render(true);
    let line = |outcome: &str| {
        text.lines()
            .find(|l| {
                l.starts_with("amberlog_ingest_records_total")
                    && l.contains(&format!("outcome=\"{outcome}\""))
                    && l.contains("stream=\"app\"")
            })
            .map(|l| l.rsplit(' ').next().unwrap().to_owned())
    };
    assert_eq!(line("accepted").as_deref(), Some("2"));
    assert_eq!(line("rejected").as_deref(), Some("3"));
}

#[tokio::test]
async fn validation_chain_order_on_json() {
    let w = world(8);
    // {org} 先于一切请求体检查
    let r = w
        .send(
            Method::POST,
            "/api/other/app/_json",
            &[("content-encoding", "br")],
            vec![b' '; 100],
        )
        .await;
    assert_reason(&r, 404, "org_not_found");
    // 路径段按原始字节交给路径段校验（`api::namespace`）：解码一次，非法 UTF-8 与双重编码都得错误信封
    let r = w
        .send(
            Method::POST,
            "/api/default/%61pp/_json",
            &[],
            br#"{"z":1}"#.to_vec(),
        )
        .await;
    assert_eq!(r.status, StatusCode::OK, "%61pp 即 app");
    assert!(
        w.rows("app")
            .iter()
            .any(|r| r.get("z").map(String::as_str) == Some("1"))
    );
    for raw in ["%FF", "%2561pp", "App"] {
        let r = w
            .send(
                Method::POST,
                &format!("/api/default/{raw}/_json"),
                &[],
                b"[]".to_vec(),
            )
            .await;
        assert_reason(&r, 400, "invalid_stream_name");
    }
    // 数据集状态闸门先于 415 与 413
    w.gates.enter(
        &Dataset::new("default", "bad"),
        DatasetState::ManifestUnreadable,
    );
    let r = w
        .post_with("bad", &[("content-encoding", "br")], vec![b' '; 100])
        .await;
    assert_reason(&r, 500, "internal");
    w.gates.enter(
        &Dataset::new("default", "old"),
        DatasetState::IncompatibleFormat,
    );
    let r = w.post("old", "[]").await;
    assert_reason(&r, 503, "unsupported_format_version");
    // 415 先于 413
    let r = w
        .post_with("app", &[("content-encoding", "br")], vec![b' '; 100])
        .await;
    assert_reason(&r, 415, "unsupported_encoding");
    // 其余方法 405，带 Allow: POST
    let r = w
        .send(Method::GET, "/api/default/app/_json", &[], Vec::new())
        .await;
    assert_reason(&r, 405, "method_not_allowed");
}
