//! REQ-AMB-STO-COLD-CATALOG 的系统测试：以子进程运行写入实例 W
//! （`amberlog serve`）与只读目录模式进程 C（`amberlog catalog`，与 W 相同的 `object_store.url`、`storage.prefix` 与
//! `auth.token`），对象存储默认用 `file://` 临时目录；测试直接读写对象存储目录下的冷层元数据与热层快照。
//!
//! - 「stream `x` 已入湖」由 [`Env::laked`] 搭建：W 以 `lifecycle.lake_watermark` 为 `1h`、`flush.interval` 为 `1s` 运行，
//!   向 `x` 摄取 100 条 `_timestamp` 落在此刻前第 3 小时的记录，等到 `v1.metadata.json` 出现（至多 10 分钟）。
//! - 「尚未入湖」：摄取之后立即请求。记录要先经 flush（`flush.interval` 为 `1s`）提交进热层才可能入湖，摄取应答之后
//!   紧接着的请求一定先于入湖。
//! - 故障注入（只在 `fault-injection` 特性下生效）：「冷层元数据读取失败」为 `TESTBED_FAULT_STORE` 规则
//!   `get,head _iceberg/default/app/metadata/ 403`，「冷层列举失败」为 `list _iceberg/ 403`；暂停点经 `TESTBED_FAULT_GATES`
//!   （每个步骤一个控制文件，运行中写 `hold` 即启用）。W 与 C 各用自己的注入控制文件。
//! - 「请求日志」：`file://` 后端用 `TESTBED_FAULT_STORE_LOG`（被测进程的每个对象存储请求记一行 `<操作> <相对根前缀的键>`）。

// 部分场景要用测试专用的注入，只在 `fault-injection` 特性下编译
#![cfg_attr(not(feature = "fault-injection"), allow(dead_code, unused_imports))]

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::process::Command;
use std::time::{Duration, Instant};

use common::api::{ingest, now_us, poll};
use common::lake::ColdDir;
use common::s3::{self, S3Instance};
use common::store::{HOUR, hour_of, metric, snapshot_rel, stop};
use common::{BEARER, Fixture, HeldPort, Kv, Resp, Server};
use serde_json::{Value, json};

/// 等新的冷层版本出现的上限（场景约定：10 分钟）。
const LAKE_WAIT: Duration = Duration::from_secs(600);
/// 等 `/readyz` 变化、等进程退出的上限（60 秒）。
const READY_WAIT: Duration = Duration::from_secs(60);
const MINUTE: i64 = 60_000_000;
const APP: &str = "app";
const T: &str = "/iceberg/v1/amberlog/namespaces/default/tables";
const TABLE_APP: &str = "/iceberg/v1/amberlog/namespaces/default/tables/app";

/// 用到的暂停点（每个一个控制文件，起初为空即不停）。`cold_compaction.before_run` 与冷层合并测试所用的暂停点相同。
const GATES: &[&str] = &[
    "catalog.after_current_version",
    "cold_compaction.before_run",
    "gateway.after_response",
];

/// 一个测试的环境：夹具与 W、C 各自的注入控制文件。
struct Env {
    f: Fixture,
    ctl: PathBuf,
}

impl Env {
    fn new() -> Env {
        let f = Fixture::new();
        let ctl = f.root().join("catalog-ctl");
        std::fs::create_dir_all(&ctl).unwrap();
        for name in ["store-w", "store-c"] {
            std::fs::write(ctl.join(name), "").unwrap();
        }
        for g in GATES {
            std::fs::write(ctl.join(g), "").unwrap();
        }
        Env { f, ctl }
    }

    fn cold(&self, stream: &str) -> ColdDir {
        self.f.cold(stream)
    }

    /// 本测试对象存储根前缀 `<store>/amberlog/` 下的路径。
    fn root_path(&self, rel: &str) -> PathBuf {
        self.f.store_dir().join("amberlog").join(rel)
    }

    /// C 的对象存储注入规则（运行中改写即生效）。
    fn faults_c(&self, rules: &str) {
        std::fs::write(self.ctl.join("store-c"), rules).unwrap();
    }

    fn hold(&self, step: &str) {
        std::fs::write(self.ctl.join(step), "hold").unwrap();
    }

    fn held(&self, step: &str) -> bool {
        std::fs::read_to_string(self.ctl.join(step)).is_ok_and(|s| s.trim() == "held")
    }

    fn release(&self, step: &str) {
        std::fs::write(self.ctl.join(step), "go").unwrap();
    }

    fn inject(&self, cmd: &mut Command, store: &str) {
        cmd.env("TESTBED_FAULT_STORE", self.ctl.join(store));
        let gates: Vec<String> = GATES
            .iter()
            .map(|g| format!("{g}={}", self.ctl.join(g).display()))
            .collect();
        cmd.env("TESTBED_FAULT_GATES", gates.join(";"));
    }

    /// W：`flush.interval` 为 `1s`、`lifecycle.lake_watermark` 为 `1h`，外加 `kv`。
    fn w(&self, kv: &[Kv]) -> Server {
        let mut all: Vec<Kv> = vec![
            ("flush.interval", Some("\"1s\"")),
            ("lifecycle.lake_watermark", Some("\"1h\"")),
        ];
        all.extend_from_slice(kv);
        let mut cmd = self.f.serve(&all);
        self.inject(&mut cmd, "store-w");
        Server::start(cmd)
    }

    fn c_cmd(&self, kv: &[Kv]) -> Command {
        let mut cmd = self.f.catalog(kv);
        self.inject(&mut cmd, "store-c");
        cmd
    }

    /// C：与 W 相同的 `object_store.url`、`storage.prefix` 与 `auth.token`，等到 `ready`。
    fn c(&self, kv: &[Kv]) -> Server {
        Server::start(self.c_cmd(kv))
    }

    /// W 向 `stream` 摄取 100 条 `_timestamp` 落在此刻前第 `hours` 小时的记录，等到冷层表出现比摄取前更新的版本
    /// （至多 10 分钟），返回此刻的冷层当前版本。
    fn laked_at(&self, w: &Server, stream: &str, hours: i64) -> u64 {
        let cold = self.cold(stream);
        let before = cold.version().unwrap_or(0);
        ingest_hours_ago(w, stream, hours);
        let (n, ok) = poll(LAKE_WAIT, || cold.version().unwrap_or(0), |n| *n > before);
        assert!(ok, "{stream} 没有入湖：{:#?}", w.stderr_lines());
        n
    }

    /// 「stream `stream` 已入湖」。
    fn laked(&self, w: &Server, stream: &str) -> u64 {
        self.laked_at(w, stream, 3)
    }
}

/// 摄取 100 条 `_timestamp` 落在此刻前第 `hours` 小时的记录，断言全部受理。
fn ingest_hours_ago(w: &Server, stream: &str, hours: i64) {
    let start = hour_of(now_us()) - hours * HOUR;
    let body: Vec<String> = (0..100)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"seq":{i}}}"#,
                start + MINUTE + i * 1000
            )
        })
        .collect();
    let r = ingest(w, stream, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["status"][0]["successful"], 100, "{}", r.text());
}

/// 携带有效凭据的请求。
fn call(s: &Server, method: &str, path: &str) -> Resp {
    s.request(method, path, &[("Authorization", BEARER)], b"")
}

fn get(s: &Server, path: &str) -> Resp {
    call(s, "GET", path)
}

fn head(s: &Server, path: &str) -> Resp {
    call(s, "HEAD", path)
}

/// 200 与 JSON 响应体。
fn ok_json(r: &Resp) -> Value {
    assert_eq!(r.status, 200, "{r:?} {}", r.text());
    assert_eq!(r.header("content-type"), Some("application/json"));
    r.json()
}

/// 对象的成员名集合。
fn members(v: &Value) -> BTreeSet<String> {
    v.as_object()
        .unwrap_or_else(|| panic!("不是对象：{v}"))
        .keys()
        .cloned()
        .collect()
}

fn set(names: &[&str]) -> BTreeSet<String> {
    names.iter().map(|s| (*s).to_owned()).collect()
}

/// 目录错误体：状态码、`Content-Type`、顶层只有 `error`、`error` 恰含四个成员且 `code` 等于状态码；返回 `error`。
fn iceberg_error(r: &Resp, status: u16, ty: &str, reason: &str) -> Value {
    assert_eq!(r.status, status, "{r:?} {}", r.text());
    assert_eq!(r.header("content-type"), Some("application/json"), "{r:?}");
    assert!(r.header("x-request-id").is_some());
    let v = r.json();
    assert_eq!(members(&v), set(&["error"]), "{v}");
    let e = v["error"].clone();
    assert_eq!(
        members(&e),
        set(&["message", "type", "code", "reason"]),
        "{e}"
    );
    assert!(e["message"].is_string() && e["type"].is_string() && e["reason"].is_string());
    assert_eq!(e["code"], status);
    assert_eq!(e["type"], ty, "{e}");
    assert_eq!(e["reason"], reason, "{e}");
    e
}

/// 对 `HEAD` 的错误：状态码，没有响应体。
fn head_error(r: &Resp, status: u16) {
    assert_eq!(r.status, status, "{r:?}");
    assert!(r.body.is_empty(), "{}", r.text());
}

fn no_such_table(r: &Resp) -> Value {
    iceberg_error(r, 404, "NoSuchTableException", "table_not_found")
}

fn no_such_namespace(r: &Resp) -> Value {
    iceberg_error(r, 404, "NoSuchNamespaceException", "namespace_not_found")
}

/// `metadata-location` 的结尾 `/amberlog/_iceberg/default/<stream>/metadata/v<n>.metadata.json`。
fn location_tail(stream: &str, n: u64) -> String {
    format!("/amberlog/_iceberg/default/{stream}/metadata/v{n}.metadata.json")
}

fn metadata_location(body: &Value) -> &str {
    body["metadata-location"].as_str().unwrap()
}

// ---------------------------------------------------------------------------
// 两种运行方式
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_serve_provides_endpoints() {
    let env = Env::new();
    let w = env.w(&[]);
    let n = env.laked(&w, APP);

    let config = ok_json(&get(&w, "/iceberg/v1/config"));
    assert_eq!(config["overrides"], json!({"prefix": "amberlog"}));
    let table = ok_json(&get(&w, TABLE_APP));
    assert!(
        metadata_location(&table).ends_with(&location_tail(APP, n)),
        "{table}"
    );
}

#[test]
fn cold_catalog_mode_rejects_api_and_ui() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);
    let before = ok_json(&get(&c, TABLE_APP));

    let auth = [("Authorization", BEARER)];
    c.request("POST", "/api/default/app/_json", &auth, br#"[{"k":1}]"#)
        .assert_envelope(404, "not_found");
    let search = json!({"query": {"sql": "SELECT * FROM app", "start_time": 0, "end_time": common::api::FAR_END_US}});
    c.request(
        "POST",
        "/api/default/_search",
        &auth,
        search.to_string().as_bytes(),
    )
    .assert_envelope(404, "not_found");
    c.request("GET", "/ui", &auth, b"")
        .assert_envelope(404, "not_found");
    c.request("POST", "/api/default/app/_json", &[], br#"[{"k":1}]"#)
        .assert_envelope(401, "unauthorized");

    assert_eq!(ok_json(&get(&c, TABLE_APP)), before);
}

#[test]
fn cold_catalog_disabled_dataset_still_served() {
    let env = Env::new();
    let mut w = env.w(&[]);
    env.laked(&w, APP);
    stop(&mut w);

    let ds = env.f.dataset(APP);
    ds.write(&snapshot_rel(ds.current().unwrap()), b"not json");
    let w = env.w(&[]);
    let series =
        r#"amberlog_dataset_disabled{org="default",stream="app",state="manifest_unreadable"}"#;
    let (v, ok) = poll(READY_WAIT, || metric(&w, series), |v| *v == Some(1.0));
    assert!(ok, "app 没有停用：{v:?} {:#?}", w.stderr_lines());
    let c = env.c(&[]);

    let from_w = ok_json(&get(&w, TABLE_APP));
    let from_c = ok_json(&get(&c, TABLE_APP));
    assert_eq!(from_w, from_c);
}

// ---------------------------------------------------------------------------
// 命名空间、表与 prefix
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_table_exists_needs_metadata() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let r = ingest(&w, "web", &format!(r#"[{{"_timestamp":{}}}]"#, now_us()));
    assert_eq!(r.status, 200, "{}", r.text());
    let stray = env.root_path("_iceberg/default/stray/data/x.parquet");
    std::fs::create_dir_all(stray.parent().unwrap()).unwrap();
    std::fs::write(&stray, b"PAR1").unwrap();
    let c = env.c(&[]);

    let list = ok_json(&get(&c, T));
    assert_eq!(
        list["identifiers"],
        json!([{"namespace": ["default"], "name": "app"}])
    );
    no_such_table(&get(&c, &format!("{T}/web")));
    no_such_table(&get(&c, &format!("{T}/stray")));
}

#[test]
fn cold_catalog_invalid_names_404() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let rs = [
        get(&c, "/iceberg/v1/amberlog/namespaces/Default"),
        get(&c, "/iceberg/v1/amberlog/namespaces/default/tables/App"),
        get(
            &c,
            "/iceberg/v1/amberlog/namespaces/default%1Fapp/tables/snapshots",
        ),
        get(&c, "/iceberg/v1/amberlog/namespaces/default/tables/app%FF"),
    ];
    no_such_namespace(&rs[0]);
    for r in &rs[1..] {
        no_such_table(r);
    }
    assert!(rs.iter().all(|r| r.status != 400));
}

#[test]
fn cold_catalog_wrong_prefix_404() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    no_such_namespace(&get(&c, "/iceberg/v1/other/namespaces"));
    no_such_namespace(&get(&c, "/iceberg/v1/other/namespaces/default"));
    head_error(&head(&c, "/iceberg/v1/other/namespaces/default"), 404);
    no_such_table(&get(&c, "/iceberg/v1/other/namespaces/default/tables/app"));
}

// ---------------------------------------------------------------------------
// /v1/config
// ---------------------------------------------------------------------------

const LOGS1: Kv = ("storage.prefix", Some("\"logs1\""));

/// 任何层级上有没有名为 `key` 的成员。
fn has_key(v: &Value, key: &str) -> bool {
    match v {
        Value::Object(m) => m.iter().any(|(k, v)| k == key || has_key(v, key)),
        Value::Array(a) => a.iter().any(|v| has_key(v, key)),
        _ => false,
    }
}

#[test]
fn cold_catalog_config_prefix() {
    let env = Env::new();
    let c = env.c(&[LOGS1]);
    let body = ok_json(&get(&c, "/iceberg/v1/config"));
    assert_eq!(members(&body), set(&["defaults", "overrides", "endpoints"]));
    assert_eq!(body["defaults"], json!({}));
    assert_eq!(body["overrides"], json!({"prefix": "logs1"}));
    assert!(!has_key(&body, "namespace-separator"), "{body}");
}

#[test]
fn cold_catalog_config_endpoints() {
    let env = Env::new();
    let c = env.c(&[LOGS1]);
    let body = ok_json(&get(&c, "/iceberg/v1/config"));
    let endpoints: Vec<&str> = body["endpoints"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e.as_str().unwrap())
        .collect();
    assert_eq!(endpoints.len(), 7);
    let got: BTreeSet<&str> = endpoints.iter().copied().collect();
    let want: BTreeSet<&str> = [
        "GET /v1/config",
        "GET /v1/{prefix}/namespaces",
        "GET /v1/{prefix}/namespaces/{namespace}",
        "HEAD /v1/{prefix}/namespaces/{namespace}",
        "GET /v1/{prefix}/namespaces/{namespace}/tables",
        "GET /v1/{prefix}/namespaces/{namespace}/tables/{table}",
        "HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}",
    ]
    .into_iter()
    .collect();
    assert_eq!(got, want);
    assert!(
        endpoints
            .iter()
            .all(|e| !e.contains("/iceberg") && !e.contains("logs1"))
    );
}

#[test]
fn cold_catalog_config_ignores_warehouse() {
    let env = Env::new();
    let c = env.c(&[]);
    let bodies: Vec<Value> = [
        "/iceberg/v1/config",
        "/iceberg/v1/config?warehouse=",
        "/iceberg/v1/config?warehouse=nosuch",
    ]
    .iter()
    .map(|p| ok_json(&get(&c, p)))
    .collect();
    assert_eq!(bodies[0], bodies[1]);
    assert_eq!(bodies[0], bodies[2]);
}

// ---------------------------------------------------------------------------
// 命名空间与表的列举
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_list_namespaces() {
    let env = Env::new();
    let w = env.w(&[]);
    let c = env.c(&[]);
    ingest_hours_ago(&w, APP, 3);
    let first = ok_json(&get(&c, "/iceberg/v1/amberlog/namespaces"));
    let cold = env.cold(APP);
    let (_, ok) = poll(LAKE_WAIT, || cold.version(), Option::is_some);
    assert!(ok, "app 没有入湖：{:#?}", w.stderr_lines());
    let second = ok_json(&get(&c, "/iceberg/v1/amberlog/namespaces"));

    assert_eq!(first, json!({"namespaces": []}));
    assert_eq!(second, json!({"namespaces": [["default"]]}));
}

#[test]
fn cold_catalog_list_namespaces_parent() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r = ok_json(&get(&c, "/iceberg/v1/amberlog/namespaces?parent=default"));
    assert_eq!(r, json!({"namespaces": []}));
    no_such_namespace(&get(&c, "/iceberg/v1/amberlog/namespaces?parent=nosuch"));
}

#[test]
fn cold_catalog_get_namespace() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r = ok_json(&get(&c, "/iceberg/v1/amberlog/namespaces/default"));
    assert_eq!(r, json!({"namespace": ["default"], "properties": {}}));
}

#[test]
fn cold_catalog_list_tables_sorted() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, "web");
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r = ok_json(&get(&c, T));
    assert_eq!(
        r,
        json!({"identifiers": [
            {"namespace": ["default"], "name": "app"},
            {"namespace": ["default"], "name": "web"},
        ]})
    );
}

#[test]
fn cold_catalog_head_204() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    for path in ["/iceberg/v1/amberlog/namespaces/default", TABLE_APP] {
        let r = head(&c, path);
        assert_eq!(r.status, 204, "{path}: {r:?}");
        assert!(r.body.is_empty());
    }
}

#[test]
fn cold_catalog_pagination_ignored() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    env.laked(&w, "web");
    let c = env.c(&[]);

    let tables = ok_json(&get(&c, &format!("{T}?pageToken=&pageSize=1")));
    let namespaces = ok_json(&get(
        &c,
        "/iceberg/v1/amberlog/namespaces?pageToken=abc&pageSize=1",
    ));
    assert_eq!(tables["identifiers"].as_array().unwrap().len(), 2);
    assert_eq!(namespaces["namespaces"], json!([["default"]]));
    assert!(!has_key(&tables, "next-page-token"));
    assert!(!has_key(&namespaces, "next-page-token"));
}

#[test]
fn cold_catalog_namespace_404() {
    let env = Env::new();
    let w = env.w(&[]);
    let c = env.c(&[]);
    ingest_hours_ago(&w, "web", 3);

    let e = no_such_namespace(&get(&c, "/iceberg/v1/amberlog/namespaces/default"));
    assert_eq!(e["code"], 404);
    head_error(&head(&c, "/iceberg/v1/amberlog/namespaces/default"), 404);
    let e = no_such_namespace(&get(&c, T));
    assert_eq!(e["code"], 404);
}

// ---------------------------------------------------------------------------
// 读表
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_load_table_body() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let plain = get(&c, TABLE_APP);
    let delegated = c.request(
        "GET",
        TABLE_APP,
        &[
            ("Authorization", BEARER),
            ("X-Iceberg-Access-Delegation", "vended-credentials"),
        ],
        b"",
    );
    for r in [&plain, &delegated] {
        let body = ok_json(r);
        assert_eq!(
            members(&body),
            set(&["metadata-location", "metadata", "config"])
        );
        assert_eq!(body["config"], json!({}));
        assert!(!has_key(&body, "storage-credentials"));
    }
}

#[test]
fn cold_catalog_metadata_matches_file() {
    let env = Env::new();
    let w = env.w(&[]);
    let n = env.laked(&w, APP);
    let c = env.c(&[]);

    let body = ok_json(&get(&c, TABLE_APP));
    let file = env.cold(APP).metadata(n);
    assert_eq!(body["metadata"], file);
    assert_eq!(
        body["metadata"]["current-snapshot-id"],
        file["current-snapshot-id"]
    );
}

#[test]
fn cold_catalog_fresh_per_request() {
    let env = Env::new();
    let w = env.w(&[]);
    let c = env.c(&[]);
    no_such_table(&get(&c, TABLE_APP));

    let n = env.laked(&w, APP);
    let body = ok_json(&get(&c, TABLE_APP));
    assert!(
        metadata_location(&body).ends_with(&format!("/v{n}.metadata.json")),
        "{body}"
    );

    // 等到 v<N+1> 出现后立即请求
    ingest_hours_ago(&w, APP, 4);
    let cold = env.cold(APP);
    let (_, ok) = poll(LAKE_WAIT, || cold.version().unwrap_or(0), |v| *v > n);
    assert!(ok, "第二批没有入湖：{:#?}", w.stderr_lines());
    let body = ok_json(&get(&c, TABLE_APP));
    assert!(
        metadata_location(&body).ends_with(&format!("/v{}.metadata.json", n + 1)),
        "{body}"
    );
}

#[test]
fn cold_catalog_load_ignores_snapshots_etag() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let rs = [
        get(&c, TABLE_APP),
        get(&c, &format!("{TABLE_APP}?snapshots=refs")),
        c.request(
            "GET",
            &format!("{TABLE_APP}?snapshots=all"),
            &[("Authorization", BEARER), ("If-None-Match", "\"abc\"")],
            b"",
        ),
    ];
    let bodies: Vec<Value> = rs.iter().map(ok_json).collect();
    assert_eq!(bodies[0], bodies[1]);
    assert_eq!(bodies[0], bodies[2]);
    assert!(rs.iter().all(|r| r.header("etag").is_none()));
}

#[test]
fn cold_catalog_table_404() {
    let env = Env::new();
    let c1 = env.c(&[]);
    let r1 = get(&c1, TABLE_APP);
    drop(c1);
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c2 = env.c(&[]);
    let r2 = get(&c2, &format!("{T}/nosuch"));
    let r3 = head(&c2, &format!("{T}/nosuch"));

    for r in [&r1, &r2] {
        let e = no_such_table(r);
        assert_eq!(e["code"], 404);
    }
    head_error(&r3, 404);
}

// ---------------------------------------------------------------------------
// 错误体
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_error_shape() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r = c.get("/iceberg/v1/config");
    iceberg_error(&r, 401, "NotAuthorizedException", "unauthorized");
    no_such_table(&get(&c, &format!("{T}/nosuch")));
    head_error(&head(&c, &format!("{T}/nosuch")), 404);
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_error_type_map() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r1 = c.get("/iceberg/v1/config");
    let r2 = call(&c, "POST", "/iceberg/v1/config");
    env.faults_c("get,head _iceberg/default/app/metadata/ 403");
    let r3 = get(&c, TABLE_APP);

    iceberg_error(&r1, 401, "NotAuthorizedException", "unauthorized");
    assert_eq!(r1.header("www-authenticate"), Some("Bearer"));
    iceberg_error(
        &r2,
        405,
        "UnsupportedOperationException",
        "method_not_allowed",
    );
    iceberg_error(&r3, 503, "ServiceUnavailableException", "unavailable");
}

// ---------------------------------------------------------------------------
// 校验链与路由
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_chain_order() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r1 = c.request("DELETE", "/iceberg/v1/namespaces/default", &[], b"");
    let r2 = call(&c, "DELETE", "/iceberg/v1/namespaces/default");
    let r3 = call(&c, "DELETE", "/iceberg/v1/other/namespaces/default");
    env.faults_c("get,head _iceberg/default/app/metadata/ 403\nlist _iceberg/ 403");
    let r4 = get(&c, "/iceberg/v1/other/namespaces/default/tables/app");
    let r5 = get(&c, "/iceberg/v1/amberlog/namespaces/default/tables/App");

    assert_eq!(r1.status, 401, "{r1:?}");
    assert_eq!(r2.status, 405, "{r2:?}");
    assert_eq!(r3.status, 405, "{r3:?}");
    no_such_table(&r4);
    no_such_table(&r5);
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_shutdown_503() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let mut c = env.c(&[("server.shutdown_timeout", Some("\"30s\""))]);
    env.hold("catalog.after_current_version");
    let addr = c.addr;
    let r1 = std::thread::spawn(move || {
        common::http(addr, "GET", TABLE_APP, &[("Authorization", BEARER)], b"")
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while !env.held("catalog.after_current_version") {
        assert!(Instant::now() < deadline, "R1 没有停在暂停点");
        std::thread::sleep(Duration::from_millis(20));
    }

    c.signal("TERM");
    let r = c.wait_readyz(503, READY_WAIT);
    assert_eq!(r.json()["details"]["checks"]["shutting_down"], true);
    let after = get(&c, "/iceberg/v1/config");
    env.release("catalog.after_current_version");
    let r1 = r1.join().unwrap();

    iceberg_error(&after, 503, "ServiceUnavailableException", "unavailable");
    ok_json(&r1);
    assert_eq!(c.wait_exit(READY_WAIT), Some(Some(0)));
}

/// 在途请求已得出应答、还没写出时进入停机：排空不等这条应答，进程等它写完再以 0 退出，客户端收到完整的 200
/// （REQ-AMB-STO-COLD-CATALOG-V2-SHUTDOWN、REQ-AMB-STO-COLD-CATALOG-V2-RO-SHUTDOWN-EXIT）。不对应块场景。
#[test]
fn cold_catalog_shutdown_waits_for_response_write() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let mut c = env.c(&[("server.shutdown_timeout", Some("\"30s\""))]);
    env.hold("gateway.after_response");
    let addr = c.addr;
    let r1 = std::thread::spawn(move || {
        common::http(addr, "GET", TABLE_APP, &[("Authorization", BEARER)], b"")
    });
    let deadline = Instant::now() + Duration::from_secs(30);
    while !env.held("gateway.after_response") {
        assert!(Instant::now() < deadline, "R1 没有停在暂停点");
        std::thread::sleep(Duration::from_millis(20));
    }

    c.signal("TERM");
    assert_eq!(
        c.wait_exit(Duration::from_secs(3)),
        None,
        "{:#?}",
        c.stderr_lines()
    );
    env.release("gateway.after_response");
    ok_json(&r1.join().unwrap());
    assert_eq!(c.wait_exit(READY_WAIT), Some(Some(0)));
}

/// 405、`Allow` 存在且取值为空、目录错误体。
fn unrouted(r: &Resp) {
    iceberg_error(
        r,
        405,
        "UnsupportedOperationException",
        "method_not_allowed",
    );
    assert_eq!(r.header("allow"), Some(""), "{r:?}");
}

#[test]
fn cold_catalog_unrouted_405() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    for (method, path) in [
        ("DELETE", "/iceberg/v1/namespaces/default/tables/app"),
        (
            "GET",
            "/iceberg/v1/amberlog/namespaces/default/tables/app/credentials",
        ),
        ("POST", "/iceberg/v1/amberlog/tables/rename"),
        ("POST", "/iceberg/v1/oauth/tokens"),
    ] {
        unrouted(&call(&c, method, path));
    }
}

#[test]
fn cold_catalog_path_variants_unrouted_405() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    for path in [
        "/iceberg/v1/config/",
        "/iceberg/v1//namespaces",
        "/iceberg/v1/amberlog/namespaces/default/tables/app/",
        "/iceberg/v1/%63onfig",
    ] {
        unrouted(&get(&c, path));
    }
}

#[test]
fn cold_catalog_method_405() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    let r1 = call(&c, "POST", "/iceberg/v1/amberlog/namespaces");
    let r2 = call(&c, "DELETE", TABLE_APP);
    let r3 = call(&c, "OPTIONS", "/iceberg/v1/amberlog/namespaces/default");
    let r4 = call(&c, "HEAD", "/iceberg/v1/config");

    for r in [&r1, &r2, &r3] {
        iceberg_error(
            r,
            405,
            "UnsupportedOperationException",
            "method_not_allowed",
        );
    }
    head_error(&r4, 405);
    assert_eq!(r1.allow(), set(&["GET"]));
    assert_eq!(r4.allow(), set(&["GET"]));
    assert_eq!(r2.allow(), set(&["GET", "HEAD"]));
    assert_eq!(r3.allow(), set(&["GET", "HEAD"]));
    ok_json(&get(&c, TABLE_APP));
}

// ---------------------------------------------------------------------------
// 读取对象存储
// ---------------------------------------------------------------------------

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_read_inaccessible_503() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);

    env.faults_c("get,head _iceberg/default/app/metadata/ 403");
    let r1 = get(&c, TABLE_APP);
    let r2 = head(&c, TABLE_APP);
    env.faults_c("list _iceberg/ 403");
    let r3 = get(&c, "/iceberg/v1/amberlog/namespaces");

    iceberg_error(&r1, 503, "ServiceUnavailableException", "unavailable");
    head_error(&r2, 503);
    iceberg_error(&r3, 503, "ServiceUnavailableException", "unavailable");
}

#[test]
fn cold_catalog_read_broken_500() {
    let env = Env::new();
    let w = env.w(&[]);
    let n = env.laked(&w, APP);
    let cold = env.cold(APP);
    std::fs::write(
        cold.path(&format!("metadata/v{}.metadata.json", n + 1)),
        b"not json",
    )
    .unwrap();
    let c = env.c(&[]);

    let r = get(&c, TABLE_APP);
    let e = iceberg_error(&r, 500, "ServiceFailureException", "internal");
    let message = e["message"].as_str().unwrap();
    for leak in ["_iceberg", "metadata.json", "amberlog/"] {
        assert!(!message.contains(leak), "{message}");
    }
    let list = ok_json(&get(&c, T));
    assert!(
        list["identifiers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|t| t["name"] == "app"),
        "{list}"
    );
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_read_no_disable() {
    let env = Env::new();
    // 冷层合并停在暂停点上直到本场景结束，使它不读到坏元数据
    env.hold("cold_compaction.before_run");
    let w = env.w(&[]);
    let n = env.laked(&w, APP);
    std::fs::write(
        env.cold(APP)
            .path(&format!("metadata/v{}.metadata.json", n + 1)),
        b"not json",
    )
    .unwrap();

    let r1 = get(&w, TABLE_APP);
    let r2 = ingest(&w, APP, r#"[{"k":"after"}]"#);

    assert_eq!(r1.status, 500, "{r1:?}");
    assert_eq!(r2.status, 200, "{}", r2.text());
    assert_eq!(r2.json()["status"][0]["successful"], 1, "{}", r2.text());
    let metrics = w.get_auth("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains("stream=\"app\"")),
        "{metrics}"
    );
}

// ---------------------------------------------------------------------------
// 只读目录模式
// ---------------------------------------------------------------------------

/// 目录 `base` 之下全部普通文件：相对路径 → (长度, 内容摘要)。`file://` 后端没有独立于内容的 ETag，「键、大小与 ETag
/// 不变」按「键、长度与内容摘要不变」断言。
fn snapshot_files(base: &std::path::Path) -> std::collections::BTreeMap<String, (u64, String)> {
    fn walk(
        base: &std::path::Path,
        p: &std::path::Path,
        out: &mut std::collections::BTreeMap<String, (u64, String)>,
    ) {
        let Ok(rd) = std::fs::read_dir(p) else {
            return;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(base, &path, out);
            } else {
                let bytes = std::fs::read(&path).unwrap();
                let rel = path.strip_prefix(base).unwrap().display().to_string();
                out.insert(rel, (bytes.len() as u64, common::store::sha256(&bytes)));
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    walk(base, base, &mut out);
    out
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_mode_no_write() {
    let env = Env::new();
    let mut w = env.w(&[]);
    env.laked(&w, APP);
    stop(&mut w);
    let root = env.root_path("");
    let before = snapshot_files(&root);

    let log = env.f.root().join("catalog-requests.log");
    let mut cmd = env.c_cmd(&[
        ("hot_fsck.interval", Some("\"1m\"")),
        ("retention.period", Some("\"2h\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ]);
    cmd.env("TESTBED_FAULT_STORE_LOG", &log);
    let mut c = Server::start(cmd);
    let deadline = Instant::now() + Duration::from_secs(180);
    while Instant::now() < deadline {
        ok_json(&get(&c, "/iceberg/v1/config"));
        ok_json(&get(&c, TABLE_APP));
        std::thread::sleep(Duration::from_secs(10));
    }
    stop(&mut c);

    assert_eq!(snapshot_files(&root), before);
    let lines = std::fs::read_to_string(&log).unwrap();
    assert!(!lines.is_empty());
    for line in lines.lines() {
        let op = line.split_whitespace().next().unwrap();
        assert!(
            matches!(op, "get" | "head" | "list"),
            "C 发出了写请求：{line}"
        );
    }
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_mode_ready_probe_list_only() {
    let env = Env::new();
    let mut w = env.w(&[]);
    env.laked(&w, APP);
    stop(&mut w);
    // 只留已入湖的 app 的冷层表
    for entry in std::fs::read_dir(env.root_path("")).unwrap().flatten() {
        if entry.file_name() != "_iceberg" {
            std::fs::remove_dir_all(entry.path()).unwrap();
        }
    }
    let c = env.c(&[]);
    let system = env.root_path("_system");

    std::thread::sleep(Duration::from_secs(60));
    assert_eq!(c.get("/readyz").status, 200);
    assert!(snapshot_files(&system).is_empty());

    env.faults_c("list _iceberg/ 403");
    let r = c.wait_readyz(503, READY_WAIT);
    assert_eq!(r.status, 503, "{r:?}");
    assert_eq!(r.json()["details"]["checks"]["object_store"], false);

    env.faults_c("");
    assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);
    assert!(snapshot_files(&system).is_empty());
}

#[cfg(feature = "fault-injection")]
#[test]
fn cold_catalog_not_ready_still_serves() {
    let env = Env::new();
    let w = env.w(&[]);
    let n = env.laked(&w, APP);
    let c = env.c(&[]);
    assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);

    env.faults_c("list _iceberg/ 403");
    assert_eq!(c.wait_readyz(503, READY_WAIT).status, 503);
    ok_json(&get(&c, "/iceberg/v1/config"));
    let body = ok_json(&get(&c, TABLE_APP));
    assert!(
        metadata_location(&body).ends_with(&location_tail(APP, n)),
        "{body}"
    );
}

#[test]
fn cold_catalog_mode_graceful_exit() {
    let env = Env::new();
    let mut c = env.c(&[("server.shutdown_timeout", Some("\"10s\""))]);
    assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);
    c.signal("TERM");
    assert_eq!(c.wait_exit(Duration::from_secs(15)), Some(Some(0)));
}

/// 启动 `cmd`（`server.http_addr` 为 `held` 占住的端口）并等它退出；返回退出码。断言它没有试过监听。
fn exit_without_listening(cmd: Command, held: &HeldPort) -> (Server, Option<i32>) {
    let mut c = Server::spawn(cmd);
    let code = c
        .wait_exit(READY_WAIT)
        .unwrap_or_else(|| panic!("没有退出：{:#?}", c.stderr_lines()));
    held.assert_never_listened(code, &c);
    (c, code)
}

fn has_event(c: &Server, event: &str) -> bool {
    c.json_lines().iter().any(|l| l["event"] == event)
}

#[test]
fn cold_catalog_mode_missing_root_exit3() {
    let env = Env::new();
    let p = env.f.root().join("no-such-dir");
    let held = HeldPort::new();
    let addr = held.addr();
    let url = format!("\"file://{}\"", p.display());
    let cmd = env.c_cmd(&[
        ("object_store.url", Some(&url)),
        ("server.http_addr", Some(&addr)),
    ]);

    let (c, code) = exit_without_listening(cmd, &held);
    assert_eq!(code, Some(3), "{:#?}", c.stderr_lines());
    assert!(has_event(&c, "starting"));
    assert!(!has_event(&c, "ready"));
    assert!(!p.exists());
}

#[test]
fn cold_catalog_mode_no_wal() {
    let env = Env::new();
    let mut w = env.w(&[]);
    env.laked(&w, APP);
    stop(&mut w);
    // 留下未提交的 WAL：flush 间隔拉长，摄取 10 条后 SIGKILL
    let mut w = env.w(&[("flush.interval", Some("\"1h\""))]);
    let body: Vec<String> = (0..10).map(|i| format!(r#"{{"seq":{i}}}"#)).collect();
    let r = ingest(&w, APP, &format!("[{}]", body.join(",")));
    assert_eq!(r.status, 200, "{}", r.text());
    w.sigkill();
    let d = env.f.wal_dir();
    let before = snapshot_files(&d);
    assert!(!before.is_empty());
    let f = env.f.root().join("plain-file");
    std::fs::write(&f, b"x").unwrap();
    let f_wal = format!("\"{}\"", f.join("wal").display());

    for kv in [vec![], vec![("wal.dir", Some(f_wal.as_str()))]] {
        let mut c = env.c(&kv);
        assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);
        c.signal("TERM");
        assert_eq!(
            c.wait_exit(READY_WAIT),
            Some(Some(0)),
            "{:#?}",
            c.stderr_lines()
        );
    }
    assert_eq!(snapshot_files(&d), before);
    assert!(f.is_file());
}

// ---------------------------------------------------------------------------
// 接在 MinIO 台架上的场景（`s3://` 写法、只读凭据）
// ---------------------------------------------------------------------------

/// MinIO 台架上的对象 `<路径>/amberlog/<rel>` 是否存在（测试进程以 root 凭据直连台架）。
fn s3_exists(inst: &S3Instance, rel: &str) -> bool {
    use object_store::ObjectStoreExt;
    let store = inst.store();
    let path = object_store::path::Path::from(format!("{}/{rel}", store.root));
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async { store.inner.head(&path).await.is_ok() })
}

/// W 向 `app` 摄取此刻前第 3 小时的记录，等到 MinIO 上出现 `v1.metadata.json`；返回此刻的冷层当前版本。
fn s3_laked(inst: &S3Instance, w: &Server) -> u64 {
    ingest_hours_ago(w, APP, 3);
    let meta = |n: u64| format!("_iceberg/default/app/metadata/v{n}.metadata.json");
    let (_, ok) = poll(LAKE_WAIT, || s3_exists(inst, &meta(1)), |b| *b);
    assert!(ok, "app 没有入湖：{:#?}", w.stderr_lines());
    let mut n = 1;
    while s3_exists(inst, &meta(n + 1)) {
        n += 1;
    }
    n
}

/// 台架上的写入实例：`lifecycle.lake_watermark` 为 `1h`、`flush.interval` 为 `1s`。
fn s3_w(inst: &S3Instance) -> Server {
    inst.start(&[
        ("flush.interval", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ])
}

#[test]
fn cold_catalog_metadata_location() {
    let inst = S3Instance::new();
    let w = s3_w(&inst);
    let n = s3_laked(&inst, &w);
    let c = Server::start(
        inst.fixture.catalog(
            &inst
                .config(&[])
                .iter()
                .map(|(k, v)| (*k, v.as_deref()))
                .collect::<Vec<Kv>>(),
        ),
    );

    let body = ok_json(&get(&c, TABLE_APP));
    let want = format!(
        "s3://{}/{}/amberlog/_iceberg/default/app/metadata/v{n}.metadata.json",
        s3::testbed("TESTBED_S3_BUCKET"),
        inst.path()
    );
    assert_eq!(metadata_location(&body), want);
    assert_eq!(
        metadata_location(&body),
        format!(
            "{}/metadata/v{n}.metadata.json",
            body["metadata"]["location"].as_str().unwrap()
        )
    );
}

#[test]
fn cold_catalog_readonly_credentials() {
    let inst = S3Instance::new();
    let mut w = s3_w(&inst);
    s3_laked(&inst, &w);
    stop(&mut w);
    let creds = s3::session_credentials(&inst.read_only_policy("_iceberg/"));
    let seen = inst.relay.statuses().len();

    let c = Server::start(inst.catalog(&[], &creds));
    assert_eq!(c.wait_readyz(200, READY_WAIT).status, 200);
    for path in [
        "/iceberg/v1/config",
        "/iceberg/v1/amberlog/namespaces",
        T,
        TABLE_APP,
    ] {
        ok_json(&get(&c, path));
    }
    assert!(
        c.json_lines()
            .iter()
            .any(|l| l["event"] == "ready" && l["mode"] == "catalog")
    );
    let statuses = inst.relay.statuses();
    assert!(statuses.len() > seen);
    assert!(
        !statuses[seen..].contains(&403),
        "以只读凭据发出的请求得到了 403：{:?}",
        &statuses[seen..]
    );
}

#[test]
fn cold_catalog_mode_probe_fail_exit3() {
    // C1：端点上没有任何服务
    let env = Env::new();
    let dead = format!("\"http://127.0.0.1:{}\"", common::free_port());
    let held1 = HeldPort::new();
    let addr1 = held1.addr();
    let c1 = env.c_cmd(&[
        ("object_store.url", Some("\"s3://bkt\"")),
        ("object_store.s3.endpoint", Some(&dead)),
        ("object_store.s3.force_path_style", Some("true")),
        ("object_store.s3.access_key_id", Some("\"id\"")),
        ("object_store.s3.secret_access_key", Some("\"secret\"")),
        ("server.http_addr", Some(&addr1)),
    ]);
    // C2：凭据只授予 `_system/` 的读取，不授予 `_iceberg/` 的列举
    let inst = S3Instance::new();
    let creds = s3::session_credentials(&json!({
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": ["s3:GetObject"], "Resource": [inst.arn("_system/*")]}],
    }));
    let held2 = HeldPort::new();
    let addr2 = held2.addr();
    let c2 = inst.catalog(&[("server.http_addr", Some(&addr2))], &creds);

    for (i, (cmd, held)) in [(c1, &held1), (c2, &held2)].into_iter().enumerate() {
        let (c, code) = exit_without_listening(cmd, held);
        assert_eq!(code, Some(3), "{:#?}", c.stderr_lines());
        assert!(has_event(&c, "starting"));
        if i == 1 {
            // 失败来自台架按会话策略拒绝列举，而不是凭据没生效
            let stderr = c.stderr_lines().join("\n");
            assert!(
                stderr.contains("403") || stderr.contains("AccessDenied"),
                "{stderr}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// 指标
// ---------------------------------------------------------------------------

#[test]
fn cold_catalog_requests_metric() {
    let env = Env::new();
    let w = env.w(&[]);
    env.laked(&w, APP);
    let c = env.c(&[]);
    let series = [
        r#"amberlog_catalog_requests_total{endpoint="config",code="200"}"#,
        r#"amberlog_catalog_requests_total{endpoint="get_table",code="404"}"#,
        r#"amberlog_catalog_requests_total{endpoint="other",code="405"}"#,
    ];
    let read = || -> Vec<f64> {
        series
            .iter()
            .map(|s| metric(&c, s).unwrap_or(0.0))
            .collect()
    };
    let before = read();

    get(&c, "/iceberg/v1/config");
    get(&c, &format!("{T}/nosuch"));
    call(&c, "POST", "/iceberg/v1/config");

    let after = read();
    for (b, a) in before.iter().zip(&after) {
        assert_eq!(*a, b + 1.0, "{before:?} → {after:?}");
    }
}
