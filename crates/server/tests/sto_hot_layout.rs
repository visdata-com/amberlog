//! REQ-AMB-STO-HOT-LAYOUT-V8 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录，测试直接读写目录下的对象来检查或构造状态，Parquet 以 `parquet` crate 的读取器检查。
//!
//! 「对象存储已停止」用 tests/common/s3.rs 的 TCP 转发（被测进程经它连 MinIO 台架）。

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use common::api::{
    FLUSH_1H, ingest, ingest_ok, ingest_with, minute_of, now_us, poll, search, search_resp, sent,
    sent_element,
};
use common::lake::{COLD_TTL, COLD_TTL_OFFSET_S, RETIRE_OFFSET_S};
use common::s3::S3Instance;
use common::store::{
    Ds, FLUSH_1S, H10, HOUR, Remote, bucket_label, commits_total, hour_of, ingest_times,
    parquet_column, parquet_columns, parquet_timestamps, sha256, snapshot_rel, wait_snapshots_gone,
};
use common::{Fixture, Kv, Server};
use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(15);
/// 使 2026-08-22 的小时桶在测试期间不越过入湖水位。
const LAKE_FAR: Kv = ("lifecycle.lake_watermark", Some("\"87600h\""));

/// 以 `flush.interval` 为 `1h` 启动，依次摄取各批（须得 200），SIGTERM 停机（停机 flush 并提交），断言退出码 0。
/// 入湖水位取 [`LAKE_FAR`]：调用方多写固定日期的记录，入湖每次启动 2 秒后即第一次检查。
fn committed(fx: &Fixture, batches: &[(&str, &str)]) {
    let mut server = fx.start(&[FLUSH_1H, LAKE_FAR]);
    for (stream, body) in batches {
        ingest_ok(&server, stream, body);
    }
    stop(&mut server);
}

fn stop(server: &mut Server) {
    server.signal("TERM");
    assert_eq!(
        server.wait_exit(Duration::from_secs(30)),
        Some(Some(0)),
        "{:#?}",
        server.stderr_lines()
    );
}

/// 以同一配置启动并等 `/readyz` 为 200。
fn restart(fx: &Fixture, overrides: &[Kv]) -> Server {
    let server = fx.start(overrides);
    assert_eq!(server.wait_readyz(200, Duration::from_secs(10)).status, 200);
    server
}

fn record(ts: &str, fields: &str) -> String {
    format!(r#"[{{"_timestamp":"{ts}",{fields}}}]"#)
}

/// `name` 由 `n` 个十进制数字组成。
fn digits(name: &str, n: usize) -> bool {
    name.len() == n && name.bytes().all(|b| b.is_ascii_digit())
}

/// 小写 ULID：26 个 Crockford base32 字符（`[0-9a-hjkmnp-tv-z]`）。
fn lower_ulid(s: &str) -> bool {
    s.len() == 26
        && s.bytes().all(|b| {
            b.is_ascii_digit()
                || (b.is_ascii_lowercase() && !matches!(b, b'i' | b'l' | b'o' | b'u'))
        })
}

fn bucket<'a>(s: &'a Value, label: &str) -> &'a Value {
    s["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .find(|b| b["bucket"] == label)
        .unwrap_or_else(|| panic!("快照没有桶 {label}：{s}"))
}

// ---------------------------------------------------------------------------
// layout
// ---------------------------------------------------------------------------

#[test]
fn layout_paths_after_flush() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, LAKE_FAR]);
    ingest_ok(&server, "app", &record("2026-08-22T10:15:00Z", r#""k":1"#));
    let ds = fx.dataset("app");
    let s = ds.wait_records(1, WAIT);
    let files: Vec<String> = ds
        .keys_under("data/2026/08/22/10/")
        .into_iter()
        .filter(|k| k.ends_with(".parquet"))
        .collect();
    assert!(!files.is_empty());
    let m = ds.restore(bucket(&s, "2026082210")["manifest"].as_str().unwrap());
    let listed: Vec<&str> = m["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap())
        .collect();
    assert!(
        files.iter().any(|f| listed.contains(&f.as_str())),
        "{listed:?}"
    );
}

#[test]
fn layout_root_prefix_under_url_path() {
    let fx = Fixture::new();
    let base = fx.root().join("D").join("base");
    std::fs::create_dir_all(&base).unwrap();
    let url = format!("\"file://{}\"", base.display());
    let server = fx.start(&[FLUSH_1S, ("object_store.url", Some(url.as_str()))]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let current = base.join("amberlog/default/app/manifest/_current");
    let deadline = Instant::now() + Duration::from_secs(5);
    while !current.exists() && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(current.exists());
}

#[test]
fn layout_dataset_prefix_subdirs() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1S]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let w = minute_of(now_us());
    let r = sent(&server, "app", &format!("[{}]", sent_element("a", w, 1)));
    assert_eq!(r.status, 200, "{}", r.text());
    std::thread::sleep(Duration::from_secs(5));
    stop(&mut server);
    let ds = fx.dataset("app");
    let keys = ds.keys();
    assert!(keys.iter().any(|k| k.starts_with("counters/")), "{keys:?}");
    for k in &keys {
        let top = k.split('/').next().unwrap();
        assert!(["data", "manifest", "counters"].contains(&top), "{k}");
    }
    assert_eq!(ds.keys_under("_system/"), Vec::<String>::new());
}

#[test]
fn layout_object_naming() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let ds = fx.dataset("app");
    ds.wait_records(1, WAIT);
    ingest_ok(&server, "app", r#"[{"a":2}]"#);
    ds.wait_records(2, WAIT);

    for key in ds.keys() {
        let parts: Vec<&str> = key.split('/').collect();
        match parts.as_slice() {
            ["data", y, m, d, h, file] => {
                assert!(
                    digits(y, 4) && digits(m, 2) && digits(d, 2) && digits(h, 2),
                    "{key}"
                );
                let id = file
                    .strip_suffix(".parquet")
                    .unwrap_or_else(|| panic!("{key}"));
                assert!(lower_ulid(id), "{key}");
            }
            ["manifest", "buckets", file] => {
                let stem = file
                    .strip_suffix(".json")
                    .unwrap_or_else(|| panic!("{key}"));
                let (hour, id) = stem.split_once('-').unwrap_or_else(|| panic!("{key}"));
                assert!(digits(hour, 10) && lower_ulid(id), "{key}");
            }
            ["manifest", "_current"] => {
                let v = ds.json(&key);
                let obj = v.as_object().unwrap();
                assert_eq!(obj.len(), 1, "{v}");
                let id = v["snapshot_id"].as_u64().expect("整数 snapshot_id");
                assert!(ds.exists(&snapshot_rel(id)));
            }
            ["manifest", file] => {
                let stem = file
                    .strip_suffix(".json")
                    .unwrap_or_else(|| panic!("{key}"));
                assert!(digits(stem, 20), "{key}");
                assert_eq!(
                    ds.json(&key)["snapshot_id"].as_u64(),
                    Some(stem.parse().unwrap())
                );
            }
            // 计数对象由后台每 5 秒写出，列对象时可能已经存在（REQ-AMB-OBS-INGEST-COUNTERS-V4 的路径规则）
            ["counters", y, m, d, file] => {
                let hour = file
                    .strip_suffix(".json")
                    .unwrap_or_else(|| panic!("{key}"));
                assert!(
                    digits(y, 4) && digits(m, 2) && digits(d, 2) && digits(hour, 2),
                    "{key}"
                );
            }
            _ => panic!("意外的对象 {key}"),
        }
    }
}

#[test]
fn layout_flush_splits_by_hour() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H, LAKE_FAR]);
    ingest_ok(
        &server,
        "hb",
        r#"[{"_timestamp":"2026-08-22T10:59:59Z"},{"_timestamp":"2026-08-22T11:00:00Z"}]"#,
    );
    stop(&mut server);
    let ds = fx.dataset("hb");
    for (dir, start) in [
        ("data/2026/08/22/10/", H10),
        ("data/2026/08/22/11/", H10 + HOUR),
    ] {
        let files = ds.keys_under(dir);
        assert_eq!(files.len(), 1, "{dir}：{files:?}");
        for ts in parquet_timestamps(&ds.path(&files[0])) {
            assert!((start..start + HOUR).contains(&ts), "{dir}：{ts}");
        }
    }
}

// ---------------------------------------------------------------------------
// parquet
// ---------------------------------------------------------------------------

#[test]
fn layout_parquet_field_id() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"host":"a"}]"#);
    ds.wait_records(1, WAIT);
    ingest_ok(&server, "app", r#"[{"host":"b"}]"#);
    ds.wait_records(2, WAIT);
    let files = ds.keys_under("data/");
    assert_eq!(files.len(), 2, "{files:?}");
    let ids: Vec<i32> = files
        .iter()
        .map(|f| parquet_column(&ds.path(f), "host").field_id)
        .collect();
    assert_eq!(ids[0], ids[1]);
}

#[test]
fn layout_field_id_order_within_record() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    ingest_ok(
        &server,
        "app6",
        r#"[{"host":"a","level":"info","msg":"x"}]"#,
    );
    let s = fx.dataset("app6").wait_records(1, WAIT);
    let ids: BTreeMap<&str, u64> = s["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| (c["name"].as_str().unwrap(), c["field_id"].as_u64().unwrap()))
        .collect();
    assert_eq!((ids["host"], ids["level"], ids["msg"]), (2, 3, 4));
}

#[test]
fn hot_parquet_timestamp_annotation() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let ds = fx.dataset("app");
    let s = ds.wait_records(1, WAIT);
    let file = &ds.keys_under("data/")[0];
    let ts = parquet_column(&ds.path(file), "_timestamp");
    assert_eq!(ts.physical, PhysicalType::INT64);
    assert_eq!(
        ts.logical,
        Some(LogicalType::timestamp(true, TimeUnit::MICROS))
    );
    assert_eq!(ts.repetition, Repetition::REQUIRED);
    let col = s["schema"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["name"] == "_timestamp")
        .unwrap();
    assert_eq!(col["type"], "Int64");
}

#[test]
fn layout_field_id_allocation() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("fid");
    for (i, body) in [r#"{"a":1}"#, r#"{"b":"x"}"#, r#"{"c":true,"a":2}"#]
        .iter()
        .enumerate()
    {
        ingest_ok(&server, "fid", body);
        ds.wait_records(i as u64 + 1, WAIT);
    }
    let head = ds.head();
    let schema: BTreeMap<String, u64> = head["schema"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| {
            (
                c["name"].as_str().unwrap().to_owned(),
                c["field_id"].as_u64().unwrap(),
            )
        })
        .collect();
    assert_eq!(
        schema,
        BTreeMap::from([
            ("_timestamp".into(), 1),
            ("a".into(), 2),
            ("b".into(), 3),
            ("c".into(), 4)
        ])
    );
    for f in ds.keys_under("data/") {
        for c in parquet_columns(&ds.path(&f)) {
            assert_eq!(c.field_id as u64, schema[&c.name], "{f}：{}", c.name);
            let want = if c.name == "_timestamp" {
                Repetition::REQUIRED
            } else {
                Repetition::OPTIONAL
            };
            assert_eq!(c.repetition, want, "{f}：{}", c.name);
        }
    }
    // 列 b 首次出现在 schema 中的快照
    let chain = ds.chain();
    let first_b = chain
        .iter()
        .rev()
        .find(|s| {
            s["schema"]
                .as_array()
                .unwrap()
                .iter()
                .any(|c| c["name"] == "b")
        })
        .unwrap();
    let id = first_b["snapshot_id"].as_u64().unwrap();
    let with_b = ds.file_entries(first_b).into_iter().any(|e| {
        e["added_in"] == id
            && parquet_columns(&ds.path(e["path"].as_str().unwrap()))
                .iter()
                .any(|c| c.name == "b")
    });
    assert!(with_b, "快照 {id} 没有同号加入、含列 b 的数据文件");
}

// ---------------------------------------------------------------------------
// snapshot-fields
// ---------------------------------------------------------------------------

#[test]
fn layout_snapshot_fields() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    let ds = fx.dataset("app");
    let s = ds.wait_records(1, WAIT);
    for k in [
        "format_version",
        "snapshot_id",
        "parent_snapshot_id",
        "parent_digest",
        "created_at",
        "schema",
        "buckets",
        "summary",
        "retired_segment",
        "cold_deletion_segment",
    ] {
        assert!(s.get(k).is_some(), "快照缺 {k}：{s}");
    }
    assert_eq!(s["format_version"], 4);
    for k in ["record_count", "size_bytes", "time_min", "time_max"] {
        assert!(
            s["summary"]["cold"].get(k).is_some(),
            "summary.cold 缺 {k}：{s}"
        );
    }
    let id = s["snapshot_id"].as_u64().unwrap();
    assert!(id > 1);
    assert_eq!(
        s["parent_digest"],
        json!(sha256(&ds.read(&snapshot_rel(id - 1))))
    );
    let b = &s["buckets"][0];
    for k in [
        "bucket",
        "manifest",
        "content_digest",
        "record_count",
        "size_bytes",
        "time_min",
        "time_max",
    ] {
        assert!(b.get(k).is_some(), "buckets[0] 缺 {k}：{b}");
    }
    let head = ds.json(b["manifest"].as_str().unwrap());
    for k in ["format_version", "bucket", "kind"] {
        assert!(head.get(k).is_some(), "头片缺 {k}：{head}");
    }
    assert_eq!(head["format_version"], 4);
    let (fields, arrays): (&[&str], &[&str]) = match head["kind"].as_str().unwrap() {
        "full" => (
            &["files", "deletions", "handovers", "ledger_previous"],
            &["files", "deletions", "handovers"],
        ),
        "delta" => (
            &["previous", "files_added", "deletions", "handovers_added"],
            &["files_added", "deletions", "handovers_added"],
        ),
        other => panic!("头片的 kind 为 {other}"),
    };
    for k in fields {
        assert!(head.get(*k).is_some(), "头片缺 {k}：{head}");
    }
    for k in arrays {
        assert!(head[*k].is_array(), "头片的 {k} 不是数组：{head}");
    }
    let m = ds.restore(b["manifest"].as_str().unwrap());
    let f = &m["files"][0];
    for k in [
        "path",
        "min_timestamp",
        "max_timestamp",
        "record_count",
        "size_bytes",
        "content_digest",
        "added_in",
    ] {
        assert!(f.get(k).is_some(), "files[0] 缺 {k}：{f}");
    }
    for v in [
        &s["snapshot_id"],
        &s["created_at"],
        &b["time_min"],
        &f["min_timestamp"],
        &f["added_in"],
    ] {
        assert!(v.is_i64() || v.is_u64(), "不是 JSON 整数：{v}");
    }
}

#[test]
fn layout_genesis_schema_timestamp() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1H]);
    ingest(&server, "app4", r#"[{"_x":1}]"#).assert_envelope(400, "all_records_rejected");
    let s = fx.dataset("app4").head();
    assert_eq!(s["snapshot_id"], 1);
    assert_eq!(s["parent_snapshot_id"], Value::Null);
    assert_eq!(s["buckets"], json!([]));
    assert_eq!(
        s["schema"],
        json!([{"name":"_timestamp","type":"Int64","field_id":1}])
    );
}

#[test]
fn layout_summary_consistency() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, LAKE_FAR]);
    let ds = fx.dataset("sm");
    let batches = [
        r#"[{"_timestamp":"2026-08-22T10:01:00Z"},{"_timestamp":"2026-08-22T11:02:00Z"}]"#,
        r#"[{"_timestamp":"2026-08-22T10:30:00Z"}]"#,
        r#"[{"_timestamp":"2026-08-22T10:00:00Z"},{"_timestamp":"2026-08-22T11:59:59Z"}]"#,
    ];
    for (b, total) in batches.iter().zip([2, 3, 5]) {
        ingest_ok(&server, "sm", b);
        ds.wait_records(total, WAIT);
    }
    let s = ds.head();
    let sum = &s["summary"];
    let buckets = s["buckets"].as_array().unwrap();
    assert_eq!(sum["dataset"], "default/sm");
    assert_eq!(sum["record_count"], 5);
    let total = |k: &str| -> u64 { buckets.iter().map(|b| b[k].as_u64().unwrap()).sum() };
    assert_eq!(sum["record_count"].as_u64().unwrap(), total("record_count"));
    assert_eq!(sum["size_bytes"].as_u64().unwrap(), total("size_bytes"));
    let min = |xs: &[Value], k: &str| xs.iter().map(|b| b[k].as_i64().unwrap()).min().unwrap();
    let max = |xs: &[Value], k: &str| xs.iter().map(|b| b[k].as_i64().unwrap()).max().unwrap();
    assert_eq!(sum["time_min"].as_i64().unwrap(), min(buckets, "time_min"));
    assert_eq!(sum["time_max"].as_i64().unwrap(), max(buckets, "time_max"));
    for b in buckets {
        let m = ds.restore(b["manifest"].as_str().unwrap());
        assert_eq!(m["bucket"], b["bucket"]);
        let files = m["files"].as_array().unwrap();
        let fsum = |k: &str| -> u64 { files.iter().map(|f| f[k].as_u64().unwrap()).sum() };
        assert_eq!(b["record_count"].as_u64().unwrap(), fsum("record_count"));
        assert_eq!(b["size_bytes"].as_u64().unwrap(), fsum("size_bytes"));
        assert_eq!(b["time_min"].as_i64().unwrap(), min(files, "min_timestamp"));
        assert_eq!(b["time_max"].as_i64().unwrap(), max(files, "max_timestamp"));
        for f in files {
            let path = ds.path(f["path"].as_str().unwrap());
            assert_eq!(
                f["size_bytes"].as_u64().unwrap(),
                std::fs::metadata(&path).unwrap().len()
            );
            let ts = parquet_timestamps(&path);
            assert_eq!(f["min_timestamp"].as_i64(), ts.iter().min().copied());
            assert_eq!(f["max_timestamp"].as_i64(), ts.iter().max().copied());
        }
    }
}

// ---------------------------------------------------------------------------
// commit-rewrite
// ---------------------------------------------------------------------------

#[test]
fn layout_commit_rewrites_affected_buckets_only() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, LAKE_FAR]);
    let ds = fx.dataset("app");
    ingest_ok(
        &server,
        "app",
        r#"[{"_timestamp":"2026-08-22T10:10:00Z"},{"_timestamp":"2026-08-22T11:10:00Z"}]"#,
    );
    let before = ds.wait_records(2, WAIT);
    let n = before["snapshot_id"].as_u64().unwrap();
    ingest_ok(&server, "app", &record("2026-08-22T12:10:00Z", r#""k":1"#));
    let after = ds.wait_records(3, WAIT);
    assert_eq!(after["snapshot_id"], n + 1);
    for label in ["2026082210", "2026082211"] {
        let (x, y) = (bucket(&before, label), bucket(&after, label));
        assert_eq!(x["manifest"], y["manifest"], "{label}");
        assert_eq!(x["content_digest"], y["content_digest"], "{label}");
    }
    bucket(&after, "2026082212");
}

#[test]
fn layout_no_overwrite_no_delete() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let ds = fx.dataset("nw");
    // 只写当前小时：不触发入湖与到期删除
    let body = format!(r#"[{{"_timestamp":{},"k":1}}]"#, common::api::now_us());
    ingest_ok(&server, "nw", &body);
    ds.wait_records(1, WAIT);
    let snapshot_of = |ds: &Ds| -> BTreeMap<String, String> {
        ds.keys()
            .into_iter()
            .filter(|k| {
                (k.starts_with("data/") || k.starts_with("manifest/")) && k != "manifest/_current"
            })
            .map(|k| {
                let d = sha256(&ds.read(&k));
                (k, d)
            })
            .collect()
    };
    let recorded = snapshot_of(&ds);
    for i in 0..3 {
        ingest_ok(&server, "nw", &body);
        ds.wait_records(i + 2, WAIT);
    }
    let now = snapshot_of(&ds);
    for (k, d) in &recorded {
        assert_eq!(now.get(k), Some(d), "{k}");
    }
}

// ---------------------------------------------------------------------------
// object-states
// ---------------------------------------------------------------------------

#[test]
fn layout_snapshot_format_version_classification() {
    let fx = Fixture::new();
    committed(
        &fx,
        &[
            ("v0", r#"[{"a":1}]"#),
            ("vs", r#"[{"a":1}]"#),
            ("v5", r#"[{"a":1}]"#),
        ],
    );
    for (stream, v) in [("v0", json!(0)), ("vs", json!("4")), ("v5", json!(5))] {
        fx.dataset(stream)
            .append_snapshot(|s| s["format_version"] = v);
    }
    let server = restart(&fx, &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM v0").assert_envelope(500, "internal");
    search_resp(&server, "SELECT * FROM vs").assert_envelope(500, "internal");
    search_resp(&server, "SELECT * FROM v5").assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_bucket_manifest_version_above_snapshot() {
    let fx = Fixture::new();
    let rec = record("2026-08-22T10:10:00Z", r#""k":1"#);
    committed(&fx, &[("bm", &rec), ("bl", &rec)]);
    // `bm` 的桶 manifest 的头片版本高于快照（5），`bl` 的低于快照（2）
    for (stream, version) in [("bm", 5), ("bl", 2)] {
        let ds = fx.dataset(stream);
        let n = ds.current().unwrap();
        let mut s = ds.snapshot(n);
        let b = s["buckets"][0].clone();
        let mut m = ds.json(b["manifest"].as_str().unwrap());
        m["format_version"] = json!(version);
        let bytes = m.to_string();
        let rel = format!(
            "manifest/buckets/{}-{}.json",
            b["bucket"].as_str().unwrap(),
            amberlog_storage::layout::new_object_id()
        );
        ds.write(&rel, bytes.as_bytes());
        s["buckets"][0]["manifest"] = json!(rel);
        s["buckets"][0]["content_digest"] = json!(sha256(bytes.as_bytes()));
        ds.write_json(&snapshot_rel(n), &s);
    }

    let server = restart(&fx, &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM bm").assert_envelope(500, "internal");
    search_resp(&server, "SELECT * FROM bl").assert_envelope(500, "internal");
}

#[test]
fn layout_readers_ignore_unknown_fields() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "uf", r#"[{"k":"a"},{"k":"b"}]"#);
    stop(&mut server);
    let ds = fx.dataset("uf");
    let n = ds.append_snapshot(|s| s["x_future"] = json!(1)) - 1;
    ds.write_json(
        "manifest/_current",
        &json!({"snapshot_id": n + 1, "x_future": 1}),
    );

    let server = restart(&fx, &[FLUSH_1H]);
    let hits: BTreeSet<String> = search(&server, "SELECT k FROM uf")
        .iter()
        .map(|h| h["k"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(hits, BTreeSet::from(["a".into(), "b".into()]));
    ingest_ok(&server, "uf", r#"[{"k":"c"}]"#);
}

// ---------------------------------------------------------------------------
// format-incompatible
// ---------------------------------------------------------------------------

/// `app`（与 `others` 各一条记录）已提交，再为 `app` 写入 `format_version` 为 5 的快照 N+1 作为链尾，以同一配置启动。
fn app_on_v5(fx: &Fixture, others: &[&str], overrides: &[Kv]) -> Server {
    let mut batches = vec![("app", r#"[{"a":1}]"#)];
    batches.extend(others.iter().map(|s| (*s, r#"[{"a":1}]"#)));
    committed(fx, &batches);
    fx.dataset("app")
        .append_snapshot(|s| s["format_version"] = json!(5));
    restart(fx, overrides)
}

#[test]
fn layout_format_version_query_503() {
    let fx = Fixture::new();
    let server = app_on_v5(&fx, &[], &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_format_version_ingest_503() {
    let fx = Fixture::new();
    let server = app_on_v5(&fx, &[], &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_format_version_sent_503() {
    let fx = Fixture::new();
    let server = app_on_v5(&fx, &[], &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    let w = minute_of(now_us());
    sent(&server, "app", &format!("[{}]", sent_element("a", w, 1)))
        .assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_format_version_gate_before_body() {
    let fx = Fixture::new();
    let server = app_on_v5(&fx, &[], &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    ingest(&server, "app", "[]").assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_v2_snapshot_gates_one_dataset() {
    let fx = Fixture::new();
    let server = app_on_v5(&fx, &["web"], &[FLUSH_1H]);
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(503, "unsupported_format_version");
    ingest_ok(&server, "web", r#"[{"a":2}]"#);
}

#[test]
fn layout_format_version_query_sticky_when_store_down() {
    let inst = S3Instance::new();
    let mut server = inst.start(&[FLUSH_1H]);
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    stop(&mut server);
    let remote = Remote::new(inst.store(), "app");
    let n = remote.current().unwrap();
    let parent = remote.read(&snapshot_rel(n)).unwrap();
    let mut s: Value = serde_json::from_slice(&parent).unwrap();
    s["snapshot_id"] = json!(n + 1);
    s["parent_snapshot_id"] = json!(n);
    s["parent_digest"] = json!(sha256(&parent));
    s["format_version"] = json!(5);
    remote.write_json(&snapshot_rel(n + 1), &s);

    let server = inst.start(&[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    inst.relay.stop();
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
}

#[test]
fn layout_format_version_writer_abort() {
    let fx = Fixture::new();
    let flush = ("flush.interval", Some("\"2s\""));
    let server = fx.start(&[flush]);
    let ds = fx.dataset("app");
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    // 刚提交完一次：下一次 flush 约在 2 秒后，其间留下一条已确认、未提交的记录，再写入版本 5 的链尾
    ds.wait_records(1, WAIT);
    ingest_ok(&server, "app", r#"[{"a":2}]"#);
    let n = ds.append_snapshot(|s| s["format_version"] = json!(5)) - 1;
    // 核对前提「内存表中有已确认、尚未提交的记录」：写入之前的当前快照 N 只计入第一条，第二条尚未提交
    assert_eq!(
        ds.snapshot(n)["summary"]["record_count"],
        json!(1),
        "写入版本 5 的快照之前第二条记录已提交，前提不成立"
    );
    std::thread::sleep(Duration::from_secs(5));
    assert!(!ds.exists(&snapshot_rel(n + 2)), "写出了快照 N+2");
    assert_ne!(ds.hint(), Some(n + 1), "提示被写为 N+1");
}

#[test]
fn layout_suspended_no_upload_failed_per_interval() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1H]);
    ingest_ok(&server, "app", r#"{"a":1}"#);
    server.sigkill();
    let ds = fx.dataset("app");
    ds.append_snapshot(|s| s["format_version"] = json!(5));

    let server = restart(&fx, &[("flush.interval", Some("\"2s\""))]);
    let (d0, f0) = (
        ds.keys_under("data/").len(),
        commits_total(&server, "app", "failed"),
    );
    std::thread::sleep(Duration::from_secs(20));
    let (d1, f1) = (
        ds.keys_under("data/").len(),
        commits_total(&server, "app", "failed"),
    );
    assert_eq!(d1, d0);
    assert!((9..=11).contains(&(f1 - f0)), "F1 − F0 = {}", f1 - f0);
}

// ---------------------------------------------------------------------------
// manifest-unreadable
// ---------------------------------------------------------------------------

#[test]
fn layout_manifest_unreadable_ingest_500() {
    let fx = Fixture::new();
    committed(&fx, &[("app", r#"[{"a":1}]"#)]);
    let ds = fx.dataset("app");
    ds.remove(&snapshot_rel(ds.current().unwrap()));
    let server = restart(&fx, &[FLUSH_1H]);
    ingest_with(&server, "app", &[("Content-Encoding", "br")], b"[]")
        .assert_envelope(500, "internal");
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
}

#[test]
fn layout_manifest_unreadable_variants() {
    let fx = Fixture::new();
    committed(&fx, &[("cp", r#"[{"a":1}]"#), ("mb", r#"[{"a":1}]"#)]);
    fx.dataset("cp").write("manifest/_current", b"not json");
    let mb = fx.dataset("mb");
    let m = mb.head()["buckets"][0]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    mb.remove(&m);

    let server = restart(&fx, &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM cp").assert_envelope(500, "internal");
    ingest(&server, "cp", r#"[{"a":2}]"#).assert_envelope(500, "internal");
    search_resp(&server, "SELECT * FROM mb").assert_envelope(500, "internal");
    ingest(&server, "mb", r#"[{"a":2}]"#).assert_envelope(500, "internal");
}

#[test]
fn layout_current_deleted_while_running() {
    let fx = Fixture::new();
    let server = fx.start(&[("flush.interval", Some("\"2s\""))]);
    let ds = fx.dataset("cd");
    ingest_ok(&server, "cd", r#"[{"a":1}]"#);
    ds.wait_records(1, WAIT);
    ds.remove("manifest/_current");
    let _ = ingest(&server, "cd", r#"[{"a":2}]"#);
    std::thread::sleep(Duration::from_secs(10));
    let d0 = ds.keys_under("data/").len();
    ingest(&server, "cd", r#"[{"a":3}]"#).assert_envelope(500, "internal");
    std::thread::sleep(Duration::from_secs(10));
    assert_eq!(ds.keys_under("data/").len(), d0);
    assert!(!ds.exists("manifest/_current"));
}

#[test]
fn layout_manifest_unreadable_sticky_until_restart() {
    let fx = Fixture::new();
    committed(&fx, &[("app", r#"[{"a":1}]"#)]);
    let ds = fx.dataset("app");
    let rel = snapshot_rel(ds.current().unwrap());
    let bytes = ds.read(&rel);
    ds.remove(&rel);

    let mut server = restart(&fx, &[FLUSH_1H]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
    ds.write(&rel, &bytes);
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(500, "internal");
    server.sigkill();

    let server = restart(&fx, &[FLUSH_1H]);
    search(&server, "SELECT * FROM app");
}

#[test]
fn layout_bucket_manifest_digest_mismatch_halts_commit() {
    let fx = Fixture::new();
    committed(&fx, &[("app", &record("2026-08-22T10:10:00Z", r#""k":1"#))]);
    let ds = fx.dataset("app");
    let n = ds.current().unwrap();
    let rel = ds.snapshot(n)["buckets"][0]["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    // 从头片删去 F 的文件条目：全量片在 `files` 中，增量片在 `files_added` 中
    let mut m = ds.json(&rel);
    let key = if m["kind"] == "full" {
        "files"
    } else {
        "files_added"
    };
    m[key].as_array_mut().unwrap().remove(0);
    ds.write_json(&rel, &m);

    // 不入湖：只验写入提交读到被改动的桶 manifest 即停用
    let server = restart(&fx, &[FLUSH_1S, LAKE_FAR]);
    let _ = ingest(&server, "app", &record("2026-08-22T10:20:00Z", r#""k":2"#));
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(ds.current(), Some(n));
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
}

// ---------------------------------------------------------------------------
// external-tamper
// ---------------------------------------------------------------------------

#[test]
fn layout_next_snapshot_slot_occupied_suspends_dataset() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    ingest_ok(&server, "web", r#"[{"a":1}]"#);
    app.wait_records(1, WAIT);
    web.wait_records(1, WAIT);
    // 前提：提示与当前快照均为 N
    let n = app.wait_hint_at_tail(WAIT);
    let web0 = web.current().unwrap();
    app.write(&snapshot_rel(n + 1), b"{}");

    let deadline = Instant::now() + Duration::from_secs(30);
    let rejected = loop {
        let r = ingest(&server, "app", r#"[{"a":2}]"#);
        ingest_ok(&server, "web", r#"[{"a":2}]"#);
        if r.status != 200 {
            break r;
        }
        assert!(Instant::now() < deadline, "30 秒内 app 的摄取一直得 200");
        std::thread::sleep(Duration::from_secs(1));
    };
    rejected.assert_envelope(500, "internal");
    let deadline = Instant::now() + WAIT;
    while web.current().unwrap() <= web0 {
        assert!(Instant::now() < deadline, "web 的当前快照没有前进");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert_eq!(app.read(&snapshot_rel(n + 1)), b"{}");
    assert!(!app.exists(&snapshot_rel(n + 2)), "写出了快照 N+2");
}

#[test]
fn layout_hint_deleted_suspends_dataset() {
    let fx = Fixture::new();
    committed(&fx, &[("app", r#"[{"a":1}]"#)]);
    let ds = fx.dataset("app");
    assert!(ds.snapshot_ids().len() >= 2);
    ds.remove("manifest/_current");
    let server = restart(&fx, &[FLUSH_1S]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(500, "internal");
    std::thread::sleep(Duration::from_secs(3));
    assert!(!ds.exists("manifest/_current"));
}

// ---------------------------------------------------------------------------
// 第 2 版格式：第 1 版数据、链尾倒退与父快照核对（REQ-AMB-STO-HOT-LAYOUT-V4）
// ---------------------------------------------------------------------------

/// 把 `ds` 改写成早期版本写出的样子（第 1 版）：各快照与桶 manifest 的 `format_version` 改为 1、去掉第 2 版新增的字段，
/// 快照对桶 manifest 的 `content_digest` 随之重算，提示指向最新快照。
fn downgrade_to_v1(ds: &Ds) {
    let ids = ds.snapshot_ids();
    for id in &ids {
        let mut s = ds.snapshot(*id);
        let obj = s.as_object_mut().unwrap();
        obj.insert("format_version".into(), json!(1));
        for k in ["parent_digest", "retired_segment", "cold_deletion_segment"] {
            obj.remove(k);
        }
        obj["summary"].as_object_mut().unwrap().remove("cold");
        for b in obj["buckets"].as_array_mut().unwrap() {
            let rel = b["manifest"].as_str().unwrap().to_owned();
            let mut m = ds.json(&rel);
            let mo = m.as_object_mut().unwrap();
            mo.insert("format_version".into(), json!(1));
            mo.remove("handovers");
            for d in mo["deletions"].as_array_mut().unwrap() {
                let d = d.as_object_mut().unwrap();
                for k in ["added_in", "max_timestamp", "cutoff"] {
                    d.remove(k);
                }
            }
            let bytes = m.to_string();
            ds.write(&rel, bytes.as_bytes());
            b["content_digest"] = json!(sha256(bytes.as_bytes()));
        }
        ds.write_json(&snapshot_rel(*id), &s);
    }
    ds.write_json(
        "manifest/_current",
        &json!({"snapshot_id": ids.last().unwrap()}),
    );
}

#[test]
fn layout_v1_dataset_unsupported_format_version() {
    let fx = Fixture::new();
    committed(
        &fx,
        &[
            ("app", &record("2026-08-22T10:10:00Z", r#""k":1"#)),
            ("web", r#"[{"a":1}]"#),
        ],
    );
    let app = fx.dataset("app");
    downgrade_to_v1(&app);
    let before = (app.keys_under("manifest/"), app.keys_under("data/"));

    let server = restart(&fx, &[FLUSH_1S]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(503, "unsupported_format_version");
    ingest_ok(&server, "web", r#"[{"a":2}]"#);
    std::thread::sleep(Duration::from_millis(2500));
    assert_eq!(
        (app.keys_under("manifest/"), app.keys_under("data/")),
        before
    );
}

#[test]
fn layout_dataset_prefix_cleared_suspends() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    ingest_ok(&server, "web", r#"[{"a":1}]"#);
    app.wait_records(1, WAIT);
    web.wait_records(1, WAIT);
    assert_eq!(search(&server, "SELECT a FROM app").len(), 1);

    std::fs::remove_dir_all(&app.dir).unwrap();
    search_resp(&server, "SELECT a FROM app").assert_envelope(500, "internal");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(500, "internal");
    assert_eq!(search(&server, "SELECT a FROM web").len(), 1);
    std::thread::sleep(Duration::from_millis(2500));
    assert!(app.keys_under("manifest/").is_empty());
    assert!(app.keys_under("data/").is_empty());
}

#[test]
fn layout_tail_regression_suspends() {
    let fx = Fixture::new();
    let (server, faults) = fx.start_with_store_faults(&[FLUSH_1S]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    ingest_ok(&server, "web", r#"[{"a":1}]"#);
    app.wait_records(1, WAIT);
    web.wait_records(1, WAIT);
    let n = app.wait_hint_at_tail(WAIT);
    faults.set("put default/app/manifest/_current 503\n");
    ingest_ok(&server, "app", r#"[{"a":2}]"#);
    app.wait_snapshot(n + 1, WAIT);
    assert_eq!(search(&server, "SELECT a FROM app WHERE a = 2").len(), 1);

    app.remove(&snapshot_rel(n + 1));
    search_resp(&server, "SELECT a FROM app").assert_envelope(500, "internal");
    ingest(&server, "app", r#"[{"a":3}]"#).assert_envelope(500, "internal");
    assert_eq!(search(&server, "SELECT a FROM web").len(), 1);
    std::thread::sleep(Duration::from_millis(2500));
    assert!(!app.exists(&snapshot_rel(n + 1)));
    assert!(!app.exists(&snapshot_rel(n + 2)));
}

#[test]
fn layout_parent_digest_checked_before_commit() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "app", r#"[{"a":1}]"#);
    ingest_ok(&server, "web", r#"[{"a":1}]"#);
    app.wait_records(1, WAIT);
    web.wait_records(1, WAIT);
    let n = app.wait_hint_at_tail(WAIT);
    // 快照 N 由本实例的写入提交创建；改写为另一份合法内容
    let mut s = app.snapshot(n);
    s["created_at"] = json!(s["created_at"].as_i64().unwrap() + 1);
    app.write_json(&snapshot_rel(n), &s);

    let _ = ingest(&server, "app", r#"[{"a":2}]"#);
    std::thread::sleep(Duration::from_millis(2500));
    assert!(
        !app.exists(&snapshot_rel(n + 1)),
        "以被改动的快照为父提交了"
    );
    assert_eq!(search_resp(&server, "SELECT a FROM app").status, 500);
    assert_eq!(ingest(&server, "app", r#"[{"a":3}]"#).status, 500);
    assert_eq!(search(&server, "SELECT a FROM web").len(), 1);
}

/// `v` 是 JSON 整数。
fn int(v: &Value) -> bool {
    v.is_i64() || v.is_u64()
}

/// 入湖以 `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark` 为 `1h` 运行，B 取此刻前第 3 小时的桶。
#[test]
fn layout_handover_record_and_summary_cold() {
    let fx = Fixture::new();
    let server = fx.start(&[
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ]);
    let ds = fx.dataset("app");
    let b = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(b);
    let ts: Vec<i64> = (0..100).map(|i| b + 60_000_000 + i * 1000).collect();
    let (t1, t2) = (ts[0], ts[99]);
    let recs: Vec<String> = ts
        .iter()
        .enumerate()
        .map(|(i, t)| format!(r#"{{"_timestamp":{t},"seq":{i}}}"#))
        .collect();
    ingest_ok(&server, "app", &format!("[{}]", recs.join(",")));
    let (head, ok) = poll(
        Duration::from_secs(150),
        || ds.head(),
        |h| {
            h["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["bucket"] == label.as_str() && e["record_count"] == 0)
        },
    );
    assert!(ok, "B 未入湖完成：{head}\n{:#?}", server.stderr_lines());

    let m = ds.bucket_manifest(&head, &label).unwrap();
    let handovers = m["handovers"].as_array().unwrap();
    assert_eq!(handovers.len(), 1, "{m}");
    let h = &handovers[0];
    for k in ["metadata_version", "iceberg_snapshot_id", "snapshot_id"] {
        assert!(int(&h[k]), "{k}：{h}");
    }
    let location = h["table_location"].as_str().unwrap();
    assert!(location.ends_with("/amberlog/_iceberg/default/app"), "{h}");
    let cold = fx.cold("app");
    let files = h["files"].as_array().unwrap();
    assert!(!files.is_empty(), "{h}");
    let mut sizes = 0;
    for x in files {
        for k in [
            "path",
            "min_timestamp",
            "max_timestamp",
            "record_count",
            "size_bytes",
            "content_digest",
        ] {
            assert!(!x[k].is_null(), "{k}：{x}");
        }
        let path = x["path"].as_str().unwrap();
        assert!(!path.starts_with('/'), "{path}");
        assert!(path.starts_with("data/_timestamp_day="), "{path}");
        let size = x["size_bytes"].as_u64().unwrap();
        assert_eq!(cold.read(path).len() as u64, size, "{path}");
        sizes += size;
    }
    let min = files
        .iter()
        .map(|x| x["min_timestamp"].as_i64().unwrap())
        .min();
    let max = files
        .iter()
        .map(|x| x["max_timestamp"].as_i64().unwrap())
        .max();
    assert_eq!((min, max), (Some(t1), Some(t2)));
    let records: u64 = files
        .iter()
        .map(|x| x["record_count"].as_u64().unwrap())
        .sum();
    assert_eq!(records, 100);

    let summary = &head["summary"];
    assert_eq!(summary["record_count"], 0, "{summary}");
    assert_eq!(summary["cold"]["record_count"], 100, "{summary}");
    assert_eq!(summary["cold"]["size_bytes"], sizes, "{summary}");
    assert_eq!(summary["cold"]["time_min"], t1, "{summary}");
    assert_eq!(summary["cold"]["time_max"], t2, "{summary}");

    for key in common::lake::keys(&fx.store_dir().join("amberlog")) {
        assert!(
            ["default/app/", "_system/", "_iceberg/"]
                .iter()
                .any(|p| key.starts_with(p)),
            "amberlog/{key}"
        );
    }
}

/// 冷层文件到期以 `retention.period` 为 `6h` 运行，入湖之后以时钟偏移重启使其到期（[`Fixture::lake_expiring`]）。
#[test]
fn layout_cold_deletion_segment_fields() {
    let fx = Fixture::new();
    let handed = fx.lake_expiring();
    let server = fx.start_offset(&COLD_TTL, COLD_TTL_OFFSET_S, &[]);
    let ds = fx.dataset("app");
    let (head, ok) = poll(
        Duration::from_secs(7 * 60),
        || ds.head(),
        |h| !h["cold_deletion_segment"].is_null(),
    );
    assert!(ok, "{head}\n{:#?}", server.stderr_lines());

    let r = &head["cold_deletion_segment"];
    let rel = "manifest/cold-deletions/00000000000000000001.json";
    assert_eq!(r["path"], rel);
    assert_eq!(r["content_digest"], sha256(&ds.read(rel)));
    let seg = ds.json(rel);
    for k in [
        "format_version",
        "segment_id",
        "previous",
        "created_at",
        "deletions",
        "additions",
    ] {
        assert!(seg.get(k).is_some(), "{k}：{seg}");
    }
    assert_eq!(seg["format_version"], 2);
    assert_eq!(seg["segment_id"], 1);
    assert!(seg["previous"].is_null());
    assert_eq!(seg["additions"], json!([]));
    let deletions = seg["deletions"].as_array().unwrap();
    for (path, rec) in &handed {
        let d = deletions
            .iter()
            .find(|d| d["path"] == path.as_str())
            .unwrap_or_else(|| panic!("冷层变更段中没有 {path}：{seg}"));
        assert_eq!(d["content_digest"], rec["content_digest"]);
        assert_eq!(d["reason"], "ttl");
        for k in [
            "deleted_at",
            "removed_in",
            "cold_removed_in",
            "max_timestamp",
            "cutoff",
        ] {
            assert!(int(&d[k]), "{k}：{d}");
        }
        assert_eq!(d["max_timestamp"], rec["max_timestamp"]);
        assert!(d["max_timestamp"].as_i64().unwrap() < d["cutoff"].as_i64().unwrap());
    }
    // 链自当前快照起向前；最早一个引用该段的快照即首次指向它的那个
    let first = ds
        .chain()
        .into_iter()
        .rfind(|s| s["cold_deletion_segment"]["path"] == rel)
        .unwrap();
    assert_eq!(first["created_at"], seg["created_at"]);
}

/// 「可控的系统时钟」用 `fault-injection` 下的时钟偏移 `TESTBED_CLOCK_OFFSET` 重启实现。B1 为
/// 此刻前第 3 小时的桶：入湖之后以偏移 2 小时重启，B1 变空满 1 小时即被归档；B2 为被测进程此刻前第 3 小时的桶，
/// 入湖之后以偏移 50 小时（自 B1 变空起前进 48 小时以上）重启，B2 归档进另一 UTC 日的段。
#[test]
fn layout_retired_segment_within_48h_one_per_day() {
    let fx = Fixture::new();
    let config = [
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ];
    let ds = fx.dataset("app");
    let reference = |label: &str| -> Value {
        ds.head()["buckets"]
            .as_array()
            .unwrap()
            .iter()
            .find(|e| e["bucket"] == label)
            .cloned()
            .unwrap()
    };
    let lake_one = |server: &Server, start: i64| -> (String, Value) {
        let label = bucket_label(start);
        let recs: Vec<String> = (0..10)
            .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, start + 1 + i * 1000))
            .collect();
        ingest_ok(server, "app", &format!("[{}]", recs.join(",")));
        ds.wait_handovers(&label, 1, Duration::from_secs(150));
        let r = reference(&label);
        assert_eq!(r["record_count"], 0, "{r}");
        (label, r)
    };
    let wait_gone = |server: &Server, label: &str| {
        let (_, ok) = poll(
            Duration::from_secs(150),
            || ds.bucket_now(label).is_none(),
            |gone| *gone,
        );
        assert!(
            ok,
            "150 秒内 {label} 未移入留痕归档段：{:#?}",
            server.stderr_lines()
        );
    };

    let mut s = fx.start(&config);
    let (b1, r1) = lake_one(&s, hour_of(now_us()) - 3 * HOUR);
    stop(&mut s);
    let mut s = fx.start_offset(&config, RETIRE_OFFSET_S, &[]);
    wait_gone(&s, &b1);
    let (b2, r2) = lake_one(
        &s,
        hour_of(now_us() + RETIRE_OFFSET_S * 1_000_000) - 3 * HOUR,
    );
    stop(&mut s);
    let s = fx.start_offset(&config, RETIRE_OFFSET_S + 48 * 3600, &[]);
    wait_gone(&s, &b2);

    let head = ds.head();
    let labels: BTreeSet<&str> = head["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["bucket"].as_str().unwrap())
        .collect();
    assert!(
        !labels.contains(b1.as_str()) && !labels.contains(b2.as_str()),
        "{head}"
    );
    assert!(head["retired_segment"].is_object(), "{head}");
    let segments = ds.segments(&head["retired_segment"]);
    for (label, r) in [(&b1, &r1), (&b2, &r2)] {
        let found: Vec<&Value> = segments
            .iter()
            .flat_map(|seg| seg["buckets"].as_array().unwrap())
            .filter(|e| e["bucket"] == label.as_str())
            .collect();
        assert_eq!(found.len(), 1, "{label}：{segments:?}");
        assert_eq!(found[0]["manifest"], r["manifest"]);
        assert_eq!(found[0]["content_digest"], r["content_digest"]);
    }
    let ids: Vec<u64> = segments
        .iter()
        .map(|seg| seg["segment_id"].as_u64().unwrap())
        .collect();
    assert!(ids.windows(2).all(|w| w[0] > w[1]), "{ids:?}");
    assert!(segments.last().unwrap()["previous"].is_null());
    let days: BTreeSet<i64> = segments
        .iter()
        .map(|seg| seg["created_at"].as_i64().unwrap().div_euclid(24 * HOUR))
        .collect();
    assert_eq!(days.len(), segments.len(), "{segments:?}");
}

/// 已入湖、变空的桶 B 以时钟偏移 2 小时重启后即可归档（归档每 60 秒一轮，第一轮在开始受理 60 秒后）；重启后随即
/// SIGTERM，停机过程停在暂停点 `shutdown.before_exit`（`fault-injection`），等到第一轮归档本应开始之后检查。
#[test]
fn layout_retire_not_started_during_shutdown() {
    let fx = Fixture::new();
    let config = [
        FLUSH_1S,
        ("lifecycle.seal_grace", Some("\"1s\"")),
        ("lifecycle.lake_watermark", Some("\"1h\"")),
    ];
    let ds = fx.dataset("app");
    let mut s = fx.start(&config);
    let b = hour_of(now_us()) - 3 * HOUR;
    let label = bucket_label(b);
    let recs: Vec<String> = (0..10)
        .map(|i| format!(r#"{{"_timestamp":{},"seq":{i}}}"#, b + 1 + i * 1000))
        .collect();
    ingest_ok(&s, "app", &format!("[{}]", recs.join(",")));
    ds.wait_handovers(&label, 1, Duration::from_secs(150));
    stop(&mut s);

    let marker = fx.root().join("shutdown-marker");
    let mut s = fx.start_offset(
        &config,
        RETIRE_OFFSET_S,
        &[
            ("TESTBED_FAULT_PAUSE", "shutdown.before_exit".to_owned()),
            ("TESTBED_FAULT_MARKER", marker.display().to_string()),
        ],
    );
    let started = Instant::now();
    assert_eq!(s.wait_readyz(200, Duration::from_secs(10)).status, 200);
    s.signal("TERM");
    assert!(
        common::wait_file(&marker, Duration::from_secs(30)),
        "{:#?}",
        s.stderr_lines()
    );
    std::thread::sleep(Duration::from_secs(90).saturating_sub(started.elapsed()));
    let head = ds.head();
    assert!(head["retired_segment"].is_null(), "{head}");
    assert!(ds.bucket_now(&label).is_some(), "{head}");
    assert!(ds.keys_under("manifest/retired/").is_empty());
    s.sigkill();
}

/// 场景「提示被外部删除、1 号快照已被回收时该数据集停用」（REQ-AMB-STO-HOT-LAYOUT-V5，manifest 不可读情形 (b)
/// 的核对列举一支）。
#[test]
fn layout_hint_deleted_after_gc_suspends_dataset() {
    let f = Fixture::new();
    let app = f.dataset("app");
    let config = [
        ("hot_retention.snapshots", Some("2")),
        ("hot_retention.age", Some("\"1s\"")),
        ("hot_fsck.interval", Some("\"5s\"")),
        FLUSH_1S,
    ];
    let mut s = f.start(&config);
    ingest_ok(&s, "web", r#"[{"seq":0}]"#);
    f.dataset("web").wait_records(1, Duration::from_secs(15));
    ingest_times(&s, &app, 1, 6);
    wait_snapshots_gone(&app, [1], Duration::from_secs(60));
    stop(&mut s);
    app.remove("manifest/_current");

    let s = f.start(&config);
    s.wait_readyz(200, Duration::from_secs(30));
    search_resp(&s, "SELECT * FROM app").assert_envelope(500, "internal");
    ingest(&s, "app", r#"[{"seq":7}]"#).assert_envelope(500, "internal");
    let r = search_resp(&s, "SELECT * FROM web");
    assert_eq!(r.status, 200, "{}", r.text());
}

// ---------------------------------------------------------------------------
// pieces：清单片的种类、还原与写出量（REQ-AMB-STO-HOT-LAYOUT-V8）
// ---------------------------------------------------------------------------

/// 等 `f` 返回 `Some`（至多 `limit`，每 50 毫秒一次）。
fn wait_some<T>(limit: Duration, what: &str, mut f: impl FnMut() -> Option<T>) -> T {
    let deadline = Instant::now() + limit;
    loop {
        if let Some(v) = f() {
            return v;
        }
        assert!(
            Instant::now() < deadline,
            "{} 秒内未等到：{what}",
            limit.as_secs()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// `lifecycle.seal_grace` 为 `6h`：此刻前第 2 小时所在的小时桶在测试期间处于 `open`，不合并、不入湖。
const GRACE_6H: Kv = ("lifecycle.seal_grace", Some("\"6h\""));

/// 向新 stream `stream` 依次摄取 `n` 批记录（每批一条，`_timestamp` 落在 `bucket_start` 起的小时桶内，形状固定），每批收到
/// HTTP 200 之后等到当前快照中该桶的 `record_count` 等于已发出的批数再发下一批；返回这 `n` 个快照依次引用的该桶头片路径。
fn ingest_one_by_one(
    server: &Server,
    ds: &Ds,
    stream: &str,
    bucket_start: i64,
    n: u64,
) -> Vec<String> {
    let label = bucket_label(bucket_start);
    let mut heads = Vec::new();
    for i in 1..=n {
        let ts = bucket_start + 1_000_000 + i as i64;
        ingest_ok(
            server,
            stream,
            &format!(r#"[{{"_timestamp":{ts},"k":"v"}}]"#),
        );
        let s = wait_some(
            Duration::from_secs(30),
            &format!("{stream} 的桶 {label} 的 record_count 为 {i}"),
            || {
                let s = ds.head();
                let b = s["buckets"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|b| b["bucket"] == label)?
                    .clone();
                (b["record_count"] == i).then_some(b)
            },
        );
        heads.push(s["manifest"].as_str().unwrap().to_owned());
    }
    heads
}

#[test]
fn layout_piece_chain_delta_then_full() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, GRACE_6H]);
    let ds = fx.dataset("app7");
    let b = hour_of(now_us() - 2 * HOUR);
    let heads = ingest_one_by_one(&server, &ds, "app7", b, 65);
    let pieces: Vec<Value> = heads.iter().map(|h| ds.json(h)).collect();
    assert_eq!(pieces[0]["kind"], "full");
    assert_eq!(pieces[0]["ledger_previous"], Value::Null);
    for k in 1..64 {
        let p = &pieces[k];
        assert_eq!(p["kind"], "delta", "第 {} 片", k + 1);
        assert_eq!(
            p["previous"]["path"],
            heads[k - 1].as_str(),
            "第 {} 片",
            k + 1
        );
        assert_eq!(
            p["previous"]["content_digest"],
            json!(sha256(&ds.read(&heads[k - 1]))),
            "第 {} 片",
            k + 1
        );
        assert_eq!(
            p["files_added"].as_array().unwrap().len(),
            1,
            "第 {} 片",
            k + 1
        );
        assert_eq!(p["deletions"], json!([]), "第 {} 片", k + 1);
        assert_eq!(p["handovers_added"], json!([]), "第 {} 片", k + 1);
    }
    let last = &pieces[64];
    assert_eq!(last["kind"], "full");
    assert_eq!(last["deletions"], json!([]));
    let written: BTreeSet<String> = pieces[..64]
        .iter()
        .flat_map(|p| {
            p[if p["kind"] == "full" {
                "files"
            } else {
                "files_added"
            }]
            .as_array()
            .unwrap()
            .clone()
        })
        .chain(ds.added_entries(ds.current().unwrap()))
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    let in_last: BTreeSet<String> = last["files"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["path"].as_str().unwrap().to_owned())
        .collect();
    assert_eq!(in_last.len(), 65);
    assert_eq!(in_last, written);
}

#[test]
fn layout_piece_bytes_bounded() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, GRACE_6H]);
    let ds = fx.dataset("app8");
    let b = hour_of(now_us() - 2 * HOUR);
    let n = 200u64;
    let heads = ingest_one_by_one(&server, &ds, "app8", b, n);
    let mut total = 0u64;
    let mut e = 0u64;
    for (k, h) in heads.iter().enumerate() {
        let bytes = ds.read(h);
        total += bytes.len() as u64;
        let p: Value = serde_json::from_slice(&bytes).unwrap();
        let full = [0, 64, 128, 192].contains(&k);
        assert_eq!(
            p["kind"],
            if full { "full" } else { "delta" },
            "第 {} 片",
            k + 1
        );
        for f in p[if full { "files" } else { "files_added" }]
            .as_array()
            .unwrap()
        {
            e = e.max(serde_json::to_vec(f).unwrap().len() as u64);
        }
    }
    // (2 + N ÷ 64) × N × E + 1024 × N
    let bound = (2.0 + n as f64 / 64.0) * n as f64 * e as f64 + 1024.0 * n as f64;
    assert!(
        (total as f64) <= bound,
        "清单片字节之和 {total} 超过判据 {bound}（E = {e}）"
    );
}

#[test]
fn layout_emptied_bucket_consolidated_full() {
    let fx = Fixture::new();
    let grace = ("lifecycle.seal_grace", Some("\"1s\""));
    let mut server = fx.start(&[grace, LAKE_FAR, FLUSH_1S]);
    let ds = fx.dataset("app");
    let b = hour_of(now_us() - 3 * HOUR);
    let label = bucket_label(b);
    common::store::three_ingests(&server, &ds, b);
    ds.wait_compaction(&label, Duration::from_secs(90));
    stop(&mut server);
    let h0 = bucket(&ds.head(), &label)["manifest"]
        .as_str()
        .unwrap()
        .to_owned();
    let before = ds.restore(&h0);

    let _server = fx.start(&[
        grace,
        ("lifecycle.lake_watermark", Some("\"2s\"")),
        FLUSH_1S,
    ]);
    wait_some(
        Duration::from_secs(120),
        &format!("桶 {label} 入湖完成"),
        || (bucket(&ds.head(), &label)["record_count"] == 0).then_some(()),
    );
    let head = ds.json(bucket(&ds.head(), &label)["manifest"].as_str().unwrap());
    assert_eq!(head["kind"], "full");
    assert_eq!(head["ledger_previous"], Value::Null);
    assert_eq!(head["files"], json!([]));
    let key = |d: &Value| {
        (
            d["path"].as_str().unwrap().to_owned(),
            d["reason"].as_str().unwrap().to_owned(),
        )
    };
    let got: Vec<(String, String)> = head["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .map(key)
        .collect();
    let mut want: Vec<(String, String)> = before["deletions"]
        .as_array()
        .unwrap()
        .iter()
        .map(key)
        .collect();
    assert!(want.iter().any(|(_, r)| r == "compaction"), "{before}");
    want.extend(
        before["files"]
            .as_array()
            .unwrap()
            .iter()
            .map(|f| (f["path"].as_str().unwrap().to_owned(), "lake".to_owned())),
    );
    let (mut got_sorted, mut want_sorted) = (got.clone(), want.clone());
    got_sorted.sort();
    want_sorted.sort();
    assert_eq!(got_sorted, want_sorted, "合拢全量片的 deletions：{head}");
    // 前一份桶 manifest 的留痕条目原样带上
    for d in before["deletions"].as_array().unwrap() {
        assert!(head["deletions"].as_array().unwrap().contains(d), "{d}");
    }
}

#[test]
fn layout_ledger_previous_links_fulls() {
    let fx = Fixture::new();
    let server = fx.start(&[FLUSH_1S, ("lifecycle.seal_grace", Some("\"1s\""))]);
    let ds = fx.dataset("app9");
    let b = hour_of(now_us() - 2 * HOUR);
    let label = bucket_label(b);
    // 台架在每个新快照出现时记下其中 B 的头片
    let mut heads: Vec<String> = Vec::new();
    let mut next = 1u64;
    let mut record = |ds: &Ds, heads: &mut Vec<String>| {
        while ds.exists(&snapshot_rel(next)) {
            let s = ds.snapshot(next);
            if let Some(r) = s["buckets"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["bucket"] == label)
            {
                let h = r["manifest"].as_str().unwrap().to_owned();
                if heads.last() != Some(&h) {
                    heads.push(h);
                }
            }
            next += 1;
        }
    };
    let deadline = Instant::now() + Duration::from_secs(600);
    let mut i = 0i64;
    while heads.len() < 193 {
        assert!(
            Instant::now() < deadline,
            "600 秒内只记下 {} 份头片",
            heads.len()
        );
        i += 1;
        ingest_ok(
            &server,
            "app9",
            &format!(r#"[{{"_timestamp":{},"k":"v"}}]"#, b + 1_000_000 + i),
        );
        std::thread::sleep(Duration::from_secs(1));
        record(&ds, &mut heads);
    }
    // 场景所称「台架所记各份头片」是前 193 份：最后一轮可能一次读到不止一个新快照（同一秒内既有 flush 提交又有
    // 合并提交），多记的头片在第 193 份之后，其留痕不在第 193 份所属的桶 manifest 中，不计入之并与下面的比较
    heads.truncate(193);
    let p129 = ds.json(&heads[128]);
    let p193 = ds.json(&heads[192]);
    assert_eq!(p129["kind"], "full");
    assert_eq!(p193["kind"], "full");
    assert_eq!(p193["ledger_previous"]["path"], heads[128].as_str());
    assert_eq!(
        p193["ledger_previous"]["content_digest"],
        json!(sha256(&ds.read(&heads[128])))
    );
    // 各份头片自身的 `deletions` 之并（相同条目只计一次）
    let mut union: Vec<Value> = Vec::new();
    for h in &heads {
        for d in ds.json(h)["deletions"].as_array().unwrap() {
            if !union.contains(d) {
                union.push(d.clone());
            }
        }
    }
    assert!(
        union.iter().any(|d| d["reason"] == "compaction"),
        "没有合并留痕"
    );
    let restored = ds.restore(&heads[192]);
    let restored: Vec<Value> = restored["deletions"].as_array().unwrap().clone();
    assert_eq!(restored.len(), union.len());
    for d in &union {
        assert!(restored.contains(d), "还原所得缺 {d}");
    }
    // 第 193 份与经其 `ledger_previous` 可达的各全量片的 `deletions` 合起来，每个条目恰出现一次
    let mut all: Vec<Value> = Vec::new();
    let mut p = p193;
    loop {
        all.extend(p["deletions"].as_array().unwrap().iter().cloned());
        if p["ledger_previous"].is_null() {
            break;
        }
        p = ds.json(p["ledger_previous"]["path"].as_str().unwrap());
    }
    for d in &all {
        assert_eq!(all.iter().filter(|x| *x == d).count(), 1, "{d}");
    }
    assert_eq!(all.len(), union.len());
}

#[test]
fn layout_piece_mid_chain_tampered_suspends() {
    let fx = Fixture::new();
    let mut server = fx.start(&[FLUSH_1S, LAKE_FAR]);
    let (app, web) = (fx.dataset("app"), fx.dataset("web"));
    ingest_ok(&server, "web", r#"[{"a":1}]"#);
    for (i, ts) in ["2026-08-22T10:10:00Z", "2026-08-22T10:20:00Z"]
        .iter()
        .enumerate()
    {
        ingest_ok(&server, "app", &record(ts, r#""k":1"#));
        app.wait_records(i as u64 + 1, WAIT);
    }
    web.wait_records(1, WAIT);
    stop(&mut server);
    let n = app.current().unwrap();
    assert_eq!(app.hint(), Some(n));
    let h = app.json(
        bucket(&app.snapshot(n), "2026082210")["manifest"]
            .as_str()
            .unwrap(),
    );
    assert_eq!(h["kind"], "delta", "{h}");
    let previous = h["previous"]["path"].as_str().unwrap().to_owned();
    let mut bytes = app.read(&previous);
    bytes.push(b' ');
    app.write(&previous, &bytes);

    let server = restart(&fx, &[FLUSH_1S, LAKE_FAR]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(500, "internal");
    ingest(&server, "app", &record("2026-08-22T10:30:00Z", r#""k":2"#))
        .assert_envelope(500, "internal");
    assert_eq!(search(&server, "SELECT a FROM web").len(), 1);
    std::thread::sleep(Duration::from_secs(3));
    assert!(!app.exists(&snapshot_rel(n + 1)), "写出了快照 N+1");
}

/// 把 `ds` 改写成较早版本写出的样子（第 2 版）：各快照的 `format_version` 改为 2，所引用的桶 manifest 整份写出
/// （去掉 `kind` 与 `ledger_previous`、版本改为 2；调用方只提交一次，头片都是全量片），快照对它的 `content_digest`
/// 随之重算，提示指向最新快照。
fn downgrade_to_v2(ds: &Ds) {
    let ids = ds.snapshot_ids();
    for id in &ids {
        let mut s = ds.snapshot(*id);
        s["format_version"] = json!(2);
        for b in s["buckets"].as_array_mut().unwrap() {
            let rel = b["manifest"].as_str().unwrap().to_owned();
            let mut m = ds.json(&rel);
            assert_eq!(m["kind"], "full", "{m}");
            let mo = m.as_object_mut().unwrap();
            mo.insert("format_version".into(), json!(2));
            mo.remove("kind");
            mo.remove("ledger_previous");
            let bytes = m.to_string();
            ds.write(&rel, bytes.as_bytes());
            b["content_digest"] = json!(sha256(bytes.as_bytes()));
        }
        ds.write_json(&snapshot_rel(*id), &s);
    }
    ds.write_json(
        "manifest/_current",
        &json!({"snapshot_id": ids.last().unwrap()}),
    );
}

#[test]
fn layout_v2_dataset_unsupported_format_version() {
    let fx = Fixture::new();
    committed(
        &fx,
        &[
            ("app", &record("2026-08-22T10:10:00Z", r#""k":1"#)),
            ("web", r#"[{"a":1}]"#),
        ],
    );
    let app = fx.dataset("app");
    downgrade_to_v2(&app);
    let before = (app.keys_under("manifest/"), app.keys_under("data/"));

    let server = restart(&fx, &[FLUSH_1S]);
    search_resp(&server, "SELECT * FROM app").assert_envelope(503, "unsupported_format_version");
    ingest(&server, "app", r#"[{"a":2}]"#).assert_envelope(503, "unsupported_format_version");
    ingest_ok(&server, "web", r#"[{"a":2}]"#);
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(
        (app.keys_under("manifest/"), app.keys_under("data/")),
        before
    );
}
