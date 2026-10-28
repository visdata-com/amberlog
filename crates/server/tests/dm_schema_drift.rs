//! REQ-AMB-DM-SCHEMA-DRIFT-V2 的系统测试：以子进程运行 `amberlog`，
//! 经 `_json` 摄取、经 `_search` 查询、读对象存储中的快照。「对象存储被停止」用 MinIO 台架加进程内 TCP 转发
//! （`common::s3`），「WAL 写入失败」与「批次停在登记之后」用 `fault-injection` 的控制文件
//! （`TESTBED_FAULT_WAL`、`TESTBED_FAULT_HOLD`）。

mod common;

use std::time::{Duration, Instant};

use common::api::{FLUSH_1H, WalFault, ingest, ingest_ok, search, search_resp};
use common::s3::S3Instance;
use common::store::FLUSH_1S;
use common::{BEARER, Fixture, Server};
use serde_json::{Value, json};

/// 断言 200 且 `status[0]` 的 `successful` 与 `failed` 如期。
fn assert_ok(body: &Value, successful: u64, failed: u64) {
    assert_eq!(
        (
            &body["status"][0]["successful"],
            &body["status"][0]["failed"]
        ),
        (&json!(successful), &json!(failed)),
        "{body}"
    );
}

fn hits(server: &Server, sql: &str) -> usize {
    search(server, sql).len()
}

/// 快照 `schema` 中名为 `name` 的元素（测试取当前快照：按找链尾确定，不以提示所指快照代替）。
fn schema_entries(schema: &Value, name: &str) -> Vec<Value> {
    schema
        .as_array()
        .unwrap()
        .iter()
        .filter(|c| c["name"] == name)
        .cloned()
        .collect()
}

// ---------------------------------------------------------------------------
// 类型冲突落入影子列
// ---------------------------------------------------------------------------

#[test]
fn schema_drift_int_then_string() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    ingest_ok(&s, "app", r#"{"code":200}"#);
    let body = ingest_ok(&s, "app", r#"{"code":"ok"}"#);
    assert_ok(&body, 1, 0);
    assert_eq!(hits(&s, "SELECT code FROM app WHERE code = 200"), 1);
    assert_eq!(
        hits(&s, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'"),
        1
    );
}

#[test]
fn schema_drift_int_float_not_widened() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    ingest_ok(&s, "app", r#"{"n":9007199254740993}"#);
    ingest_ok(&s, "app", r#"{"n":1.5}"#);
    assert_eq!(
        search(&s, "SELECT n FROM app WHERE n = 9007199254740993"),
        [json!({"n": 9007199254740993_i64})]
    );
    assert_eq!(
        hits(&s, "SELECT n__float64 FROM app WHERE n__float64 = 1.5"),
        1
    );
}

#[test]
fn schema_drift_one_column_per_type() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    ingest_ok(&s, "app", r#"{"v":1}"#);
    let body = ingest_ok(
        &s,
        "app",
        r#"[{"v":"a"},{"v":true},{"v":2.5},{"v":"b"},{"v":3}]"#,
    );
    assert_ok(&body, 5, 0);
    let all = search(&s, "SELECT * FROM app");
    assert_eq!(all.len(), 6);
    for hit in &all {
        let present = ["v", "v__utf8", "v__boolean", "v__float64"]
            .iter()
            .filter(|k| hit.get(**k).is_some())
            .count();
        assert_eq!(present, 1, "{hit}");
    }
    assert_eq!(
        hits(&s, "SELECT v__utf8 FROM app WHERE v__utf8 IS NOT NULL"),
        2
    );
}

#[test]
fn schema_drift_batch_order() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let body = ingest_ok(&s, "app", r#"[{"n":1},{"n":"x"}]"#);
    assert_ok(&body, 2, 0);
    assert_eq!(hits(&s, "SELECT n FROM app WHERE n = 1"), 1);
    assert_eq!(hits(&s, "SELECT n__utf8 FROM app WHERE n__utf8 = 'x'"), 1);
}

#[test]
fn schema_drift_snapshot_schema() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", r#"{"code":200}"#);
    ingest_ok(&s, "app", r#"{"code":"ok"}"#);
    let snapshot = fx.dataset("app").wait_records(2, Duration::from_secs(20));
    let schema = &snapshot["schema"];
    let code = schema_entries(schema, "code");
    let shadow = schema_entries(schema, "code__utf8");
    assert_eq!(code.len(), 1, "{schema}");
    assert_eq!(shadow.len(), 1, "{schema}");
    assert_eq!(code[0]["type"], "Int64");
    assert_eq!(shadow[0]["type"], "Utf8");
    assert_ne!(code[0]["field_id"], shadow[0]["field_id"]);
}

// ---------------------------------------------------------------------------
// 边界
// ---------------------------------------------------------------------------

#[test]
fn schema_drift_big_int_shadow() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    ingest_ok(&s, "app", r#"{"id":1}"#);
    ingest_ok(&s, "app", r#"{"id":12345678901234567890123}"#);
    assert_eq!(
        hits(
            &s,
            "SELECT id__utf8 FROM app WHERE id__utf8 = '12345678901234567890123'"
        ),
        1
    );
}

#[test]
fn schema_drift_committed_schema_offline() {
    let inst = S3Instance::new();
    let mut a = inst.start(&[]);
    ingest_ok(&a, "app", r#"{"code":200}"#);
    a.signal("TERM");
    assert_eq!(a.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    drop(a);

    let wal = format!("\"{}\"", inst.fixture.root().join("wal-b").display());
    let b = inst.start(&[("wal.dir", Some(wal.as_str()))]);
    assert_eq!(b.wait_readyz(200, Duration::from_secs(30)).status, 200);
    inst.relay.stop();
    let r = ingest(&b, "app", r#"[{"code":"ok"},{"code":201}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_ok(&r.json(), 2, 0);
    inst.relay.resume();
    assert_eq!(
        hits(&b, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'"),
        1
    );
    assert_eq!(hits(&b, "SELECT code FROM app WHERE code = 201"), 1);
}

/// 等控制文件的内容变为 `want`（至多 10 秒）。
fn wait_content(path: &std::path::Path, want: &str) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !std::fs::read_to_string(path).is_ok_and(|s| s.trim() == want) {
        assert!(Instant::now() < deadline, "未等到 {want}");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn schema_drift_replay_keeps_target_column() {
    let fx = Fixture::new();
    let fault = WalFault::new(&fx);
    let hold = fx.root().join("ingest-hold");
    let start = || {
        let mut cmd = fx.serve(&[FLUSH_1H]);
        cmd.env("TESTBED_FAULT_HOLD", &hold)
            .env("TESTBED_FAULT_WAL", fault.path());
        Server::start(cmd)
    };
    let mut s = start();
    ingest_ok(&s, "app", r#"{"x":1}"#);

    // 批次 A 登记 `code`（Int64）后停在追加 WAL 之前
    std::fs::write(&hold, "hold").unwrap();
    let addr = s.addr;
    let first = std::thread::spawn(move || {
        common::http(
            addr,
            "POST",
            "/api/default/app/_json",
            &[("Authorization", BEARER)],
            br#"[{"code":1}]"#,
        )
    });
    wait_content(&hold, "held");

    // 批次 B 判定时 `code` 已属当前 schema：字符串落入影子列
    let body = ingest_ok(&s, "app", r#"[{"code":"ok"}]"#);
    assert_ok(&body, 1, 0);

    // 放行 A，其 WAL 写入失败：503，`code` 撤回
    fault.inject("write");
    std::fs::write(&hold, "go").unwrap();
    let r = first.join().unwrap();
    r.assert_envelope(503, "unavailable");
    fault.clear();
    search_resp(&s, "SELECT code FROM app").assert_envelope(400, "unknown_column");

    s.sigkill();
    let _ = std::fs::remove_file(&hold);
    let s = start();
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    assert_eq!(
        hits(&s, "SELECT code__utf8 FROM app WHERE code__utf8 = 'ok'"),
        1
    );
    search_resp(&s, "SELECT code FROM app").assert_envelope(400, "unknown_column");
}

// ---------------------------------------------------------------------------
// 需求场景之外的补充测试
// ---------------------------------------------------------------------------

#[test]
fn schema_drift_shadow_name_length_exempt() {
    let fx = Fixture::new();
    let s = fx.start(&[]);
    let name = "k".repeat(128);
    ingest_ok(&s, "app", &format!(r#"{{"{name}":1}}"#));
    let body = ingest_ok(&s, "app", &format!(r#"{{"{name}":"x"}}"#));
    assert_ok(&body, 1, 0);
    assert_eq!(
        hits(
            &s,
            &format!(r#"SELECT "{name}__utf8" FROM app WHERE "{name}__utf8" = 'x'"#)
        ),
        1
    );
}

#[test]
fn schema_drift_concurrent_single_registration() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", r#"{"k":1}"#);
    let addr = s.addr;
    let batches: Vec<_> = (0..16)
        .map(|_| {
            std::thread::spawn(move || {
                common::http(
                    addr,
                    "POST",
                    "/api/default/app/_json",
                    &[("Authorization", BEARER)],
                    br#"[{"k":"x"}]"#,
                )
            })
        })
        .collect();
    for b in batches {
        let r = b.join().unwrap();
        assert_eq!(r.status, 200, "{}", r.text());
    }
    let snapshot = fx.dataset("app").wait_records(17, Duration::from_secs(20));
    assert_eq!(
        schema_entries(&snapshot["schema"], "k__utf8").len(),
        1,
        "{}",
        snapshot["schema"]
    );
    assert_eq!(hits(&s, "SELECT k__utf8 FROM app WHERE k__utf8 = 'x'"), 16);
}

/// 并发批次为新基名的两种类型只登记一个基名列：判定与登记之间若没有同一把锁，两种类型的批次都可能
/// 认为自己在登记 `k2`。
#[test]
fn schema_drift_concurrent_new_base_name() {
    let fx = Fixture::new();
    let s = fx.start(&[FLUSH_1S]);
    ingest_ok(&s, "app", r#"{"k":1}"#);
    let addr = s.addr;
    let batches: Vec<_> = (0..16)
        .map(|i| {
            std::thread::spawn(move || {
                let body: &[u8] = if i % 2 == 0 {
                    br#"[{"k2":1}]"#
                } else {
                    br#"[{"k2":"x"}]"#
                };
                common::http(
                    addr,
                    "POST",
                    "/api/default/app/_json",
                    &[("Authorization", BEARER)],
                    body,
                )
            })
        })
        .collect();
    for b in batches {
        let r = b.join().unwrap();
        assert_eq!(r.status, 200, "{}", r.text());
    }
    let snapshot = fx.dataset("app").wait_records(17, Duration::from_secs(20));
    let schema = &snapshot["schema"];
    let base = schema_entries(schema, "k2");
    assert_eq!(base.len(), 1, "{schema}");
    let (shadow, base_sql, shadow_sql) = match base[0]["type"].as_str().unwrap() {
        "Int64" => (
            "k2__utf8",
            "SELECT k2 FROM app WHERE k2 = 1",
            "SELECT k2__utf8 FROM app WHERE k2__utf8 = 'x'",
        ),
        "Utf8" => (
            "k2__int64",
            "SELECT k2 FROM app WHERE k2 = 'x'",
            "SELECT k2__int64 FROM app WHERE k2__int64 = 1",
        ),
        t => panic!("k2 的类型 {t}：{schema}"),
    };
    assert_eq!(schema_entries(schema, shadow).len(), 1, "{schema}");
    assert_eq!(hits(&s, base_sql), 8);
    assert_eq!(hits(&s, shadow_sql), 8);
}
