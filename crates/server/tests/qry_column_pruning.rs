//! REQ-AMB-QRY-COLUMN-PRUNING 的系统测试：以子进程运行
//! `amberlog`，对象存储用 `file://` 临时目录，测试直接读写目录下的对象来构造或检查桶 manifest。
//!
//! 统计函数与判定函数的单元测试在 amberlog-storage（`stats`）与 amberlog-query（`prune`）的单元测试里；
//! 「与时间范围无交集的桶 manifest 不被读取」在 amberlog-query 的单元测试里以读即报错的快照链替身验证。

mod common;

use std::collections::BTreeSet;
use std::time::Duration;

use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Int64Type};
use common::api::{ingest_ok, now_us, poll};
use common::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of, sha256, snapshot_rel, stop};
use common::{BEARER, Fixture, Server};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use serde_json::{Value, json};

/// 此刻所在小时（`open` 状态）内的时间戳；离整点太近时取该小时的前段，保证两次摄取落在同一个桶。
fn this_hour() -> i64 {
    hour_of(now_us()) + 60_000_000
}

/// 以覆盖 `start` 所在小时的时间范围执行 `sql`，返回应答体（断言 200）。
fn query(s: &Server, sql: &str, start: i64) -> Value {
    let h = hour_of(start);
    let body = json!({"query": {"sql": sql, "start_time": h, "end_time": h + HOUR, "size": 10000}})
        .to_string();
    let r = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()
}

/// 以 `[{"_timestamp":ts+i, <fields>}]` 摄取 `rows` 条并等提交。
fn ingest_rows(s: &Server, ds: &Ds, ts: i64, rows: &[String], total: u64) {
    let body: Vec<String> = rows
        .iter()
        .enumerate()
        .map(|(i, f)| format!(r#"{{"_timestamp":{},{f}}}"#, ts + i as i64))
        .collect();
    ingest_ok(s, "app", &format!("[{}]", body.join(",")));
    ds.wait_records(total, Duration::from_secs(15));
}

fn status_rows(from: i64, to: i64) -> Vec<String> {
    (from..to).map(|v| format!(r#""status":{v}"#)).collect()
}

/// 两个文件：F1 的 `status` 为 500，F2 的 `status` 为 200；实例已停。返回桶标签。
fn two_files(f: &Fixture) -> (String, i64) {
    let mut s = f.start(&[FLUSH_1S]);
    let ds = f.dataset("app");
    let ts = this_hour();
    ingest_rows(&s, &ds, ts, &vec![r#""status":500"#.to_owned(); 3], 3);
    ingest_rows(&s, &ds, ts + 100, &vec![r#""status":200"#.to_owned(); 3], 6);
    stop(&mut s);
    (bucket_label(ts), ts)
}

/// 改写 `_current` 所指快照中桶 `label` 的桶 manifest（还原后改写，写成新路径的一份合拢全量片），并以引用它的新快照
/// 推进 `_current`。
fn rewrite_bucket(ds: &Ds, label: &str, edit: impl FnOnce(&mut Value)) {
    let n = ds.current().unwrap();
    let mut snap = ds.snapshot(n);
    let i = snap["buckets"]
        .as_array()
        .unwrap()
        .iter()
        .position(|b| b["bucket"] == label)
        .unwrap();
    let mut m = ds.restore(snap["buckets"][i]["manifest"].as_str().unwrap());
    m["kind"] = json!("full");
    m["ledger_previous"] = Value::Null;
    edit(&mut m);
    let bytes = m.to_string().into_bytes();
    let path = format!(
        "manifest/buckets/{label}-{}.json",
        amberlog_storage::layout::new_object_id()
    );
    ds.write(&path, &bytes);
    snap["buckets"][i]["manifest"] = json!(path);
    snap["buckets"][i]["content_digest"] = json!(sha256(&bytes));
    snap["snapshot_id"] = json!(n + 1);
    snap["parent_snapshot_id"] = json!(n);
    ds.write_json(&snapshot_rel(n + 1), &snap);
    ds.write_json("manifest/_current", &json!({"snapshot_id": n + 1}));
}

/// 桶 manifest `m` 中 `status` 取值为 `v` 的那个文件条目。
fn entry_with_status(m: &mut Value, v: i64) -> &mut Value {
    m["files"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .find(|e| e["column_stats"]["status"]["min"] == v)
        .unwrap()
}

#[test]
fn pruning_stats_carried_unchanged() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S]);
    let ds = f.dataset("app");
    let ts = this_hour();
    let label = bucket_label(ts);
    ingest_rows(&s, &ds, ts, &status_rows(1, 4), 3);
    let m = ds.bucket_manifest(&ds.head(), &label).unwrap();
    let f1 = m["files"][0].clone();
    assert!(f1["column_stats"].is_object(), "{f1}");

    ingest_rows(&s, &ds, ts + 100, &status_rows(10, 20), 13);
    std::thread::sleep(Duration::from_secs(5));
    let m = ds.bucket_manifest(&ds.head(), &label).unwrap();
    let files = m["files"].as_array().unwrap();
    assert!(files.len() > 1, "{m}");
    let now = files.iter().find(|e| e["path"] == f1["path"]).unwrap();
    assert_eq!(now["column_stats"], f1["column_stats"]);
}

#[test]
fn pruning_malformed_stats_ignored() {
    let f = Fixture::new();
    let (label, ts) = two_files(&f);
    let ds = f.dataset("app");
    rewrite_bucket(&ds, &label, |m| {
        entry_with_status(m, 500)["column_stats"] = json!("bogus");
    });
    let s = f.start(&[FLUSH_1S]);
    let r = query(&s, "SELECT * FROM app WHERE status = 500", ts);
    assert_eq!(r["scan_files"], 1, "{r}");
    let hits = r["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 3);
    assert!(hits.iter().all(|h| h["status"] == 500));
    assert!(
        !s.json_lines()
            .iter()
            .any(|v| v["event"] == "dataset_disabled" && v["dataset"] == "default/app"),
        "{:#?}",
        s.stderr_lines()
    );
}

#[test]
fn pruning_column_minmax_scan_files() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S]);
    let ds = f.dataset("app");
    let ts = this_hour();
    ingest_rows(&s, &ds, ts, &status_rows(200, 300), 100);
    ingest_rows(&s, &ds, ts + 1000, &status_rows(500, 600), 200);
    let r = query(&s, "SELECT * FROM app WHERE status >= 500", ts);
    assert_eq!(r["scan_files"], 1, "{r}");
    let hits = r["hits"].as_array().unwrap();
    assert_eq!(hits.len(), 100);
    assert!(hits.iter().all(|h| h["status"].as_i64().unwrap() >= 500));
}

#[test]
fn pruning_absent_column_skipped() {
    let f = Fixture::new();
    let s = f.start(&[FLUSH_1S]);
    let ds = f.dataset("app");
    let ts = this_hour();
    ingest_rows(
        &s,
        &ds,
        ts,
        &[r#""err":"x""#.to_owned(), r#""err":"y""#.to_owned()],
        2,
    );
    ingest_rows(
        &s,
        &ds,
        ts + 100,
        &[r#""ok":1"#.to_owned(), r#""ok":2"#.to_owned()],
        4,
    );
    let r = query(&s, "SELECT * FROM app WHERE err IS NOT NULL", ts);
    assert_eq!(r["scan_files"], 1, "{r}");
}

#[test]
fn pruning_no_stats_time_only() {
    let f = Fixture::new();
    let (label, ts) = two_files(&f);
    let ds = f.dataset("app");
    rewrite_bucket(&ds, &label, |m| {
        entry_with_status(m, 500)
            .as_object_mut()
            .unwrap()
            .remove("column_stats");
    });
    let s = f.start(&[FLUSH_1S]);
    let r = query(&s, "SELECT * FROM app WHERE status = 500", ts);
    assert_eq!(r["scan_files"], 1, "{r}");
}

#[test]
fn pruning_compaction_output_has_stats() {
    let f = Fixture::new();
    let start = hour_of(now_us()) - 2 * HOUR;
    let label = bucket_label(start);
    let ds = f.dataset("app");
    {
        let mut s = f.start(&[
            ("lifecycle.seal_grace", Some("\"24h\"")),
            ("flush.max_records", Some("3")),
            FLUSH_1S,
        ]);
        ingest_rows(
            &s,
            &ds,
            start,
            &[
                r#""n":1,"s":"b""#.into(),
                r#""n":7"#.into(),
                r#""s":"a""#.into(),
            ],
            3,
        );
        ingest_rows(
            &s,
            &ds,
            start + 10,
            &[
                r#""n":-2"#.into(),
                r#""n":3,"f":1.5"#.into(),
                r#""f":0.5"#.into(),
            ],
            6,
        );
        stop(&mut s);
    }
    let _s = f.start(&[("lifecycle.seal_grace", Some("\"1s\"")), FLUSH_1S]);
    let (m, ok) = poll(
        Duration::from_secs(60),
        || ds.bucket_manifest(&ds.head(), &label).unwrap(),
        |m| m["files"].as_array().unwrap().len() == 1,
    );
    assert!(ok, "60 秒内未合并：{m}");
    let entry = &m["files"][0];
    let stats = entry["column_stats"]
        .as_object()
        .expect("合并输出带 column_stats");

    // 读出 F 的 Parquet 内容独立求值
    let file = std::fs::File::open(ds.path(entry["path"].as_str().unwrap())).unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
    let schema = reader.schema().clone();
    let batches: Vec<_> = reader.build().unwrap().map(|b| b.unwrap()).collect();
    let names: BTreeSet<String> = schema
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .filter(|n| n != "_timestamp")
        .collect();
    assert_eq!(stats.keys().cloned().collect::<BTreeSet<_>>(), names);
    for name in &names {
        let mut nulls = 0u64;
        let mut values: Vec<Value> = Vec::new();
        for b in &batches {
            let col = b.column_by_name(name).unwrap();
            nulls += col.null_count() as u64;
            for i in 0..col.len() {
                if col.is_null(i) {
                    continue;
                }
                values.push(match col.data_type() {
                    DataType::Int64 => json!(col.as_primitive::<Int64Type>().value(i)),
                    DataType::Float64 => {
                        json!(col.as_primitive::<arrow::datatypes::Float64Type>().value(i))
                    }
                    DataType::Utf8 => json!(col.as_string::<i32>().value(i)),
                    DataType::Boolean => json!(col.as_boolean().value(i)),
                    other => panic!("{other}"),
                });
            }
        }
        let cmp = |a: &Value, b: &Value| match (a, b) {
            (Value::Number(x), Value::Number(y)) => x
                .as_f64()
                .unwrap()
                .partial_cmp(&y.as_f64().unwrap())
                .unwrap(),
            (Value::String(x), Value::String(y)) => x.cmp(y),
            (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
            _ => panic!("{a} {b}"),
        };
        let min = values
            .iter()
            .min_by(|a, b| cmp(a, b))
            .cloned()
            .unwrap_or(Value::Null);
        let max = values
            .iter()
            .max_by(|a, b| cmp(a, b))
            .cloned()
            .unwrap_or(Value::Null);
        let st = &stats[name];
        assert_eq!(st["null_count"], nulls, "{name}");
        assert_eq!(st["min"], min, "{name}");
        assert_eq!(st["max"], max, "{name}");
    }
}
