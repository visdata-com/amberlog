//! `_search` 在 query 这一层的测试：存储侧一律用 `amberlog_storage::testing` 的替身与 [`crate::testing::FakeFiles`]，
//! 不经 HTTP、不读对象存储。块场景的系统测试（启动 `amberlog` 二进制）在 `amberlog-server` 的 `qry_*.rs`。

use std::collections::HashMap;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_storage::chain::{
    BucketManifest, BucketRef, ChainError, ColdSummary, Digest, FileEntry, Snapshot, SnapshotId,
    Summary,
};
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::schema::{SchemaColumn, StreamColumns};
use amberlog_storage::testing::{FakeChain, FakeColdReader, FakeMemtable};
use arrow::array::{
    ArrayRef, BooleanBuilder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondArray,
};
use arrow::record_batch::RecordBatch;
use serde_json::{Value, json};

use super::*;
use crate::files::FileError;
use crate::request::Limits;
use crate::testing::FakeFiles;

// ---------------------------------------------------------------------------
// 替身与夹具
// ---------------------------------------------------------------------------

struct Fx {
    memtable: Arc<FakeMemtable>,
    chain: Arc<FakeChain>,
    gates: Arc<DatasetGates>,
    files: Arc<FakeFiles>,
    engine: Engine,
    /// 各 stream 的列（`_timestamp` 在前）。
    columns: HashMap<String, StreamColumns>,
}

const END: i64 = 4_102_444_800_000_000;

fn ds(stream: &str) -> Dataset {
    Dataset::new("default", stream)
}

impl Fx {
    fn new() -> Fx {
        let memtable = Arc::new(FakeMemtable::new());
        let chain = Arc::new(FakeChain::new());
        let gates = Arc::new(DatasetGates::new());
        let files = Arc::new(FakeFiles::new());
        let engine = Engine {
            memtable: memtable.clone(),
            chain: chain.clone(),
            gates: gates.clone(),
            files: files.clone(),
            cold: Arc::new(FakeColdReader::new()),
            limits: Limits::default(),
            admission: crate::testing::admission(),
            governance: crate::testing::governance().0,
        };
        Fx {
            memtable,
            chain,
            gates,
            files,
            engine,
            columns: HashMap::new(),
        }
    }

    /// 登记 stream（尚无快照：`_current` 不存在、只以可见集作答）。
    fn stream(&mut self, name: &str, cols: &[(&str, ColumnType)]) {
        let mut all = vec![SchemaColumn {
            name: TIMESTAMP_COLUMN.into(),
            column_type: ColumnType::Int64,
            field_id: 1,
        }];
        all.extend(cols.iter().enumerate().map(|(i, (n, t))| SchemaColumn {
            name: (*n).into(),
            column_type: *t,
            field_id: i as u32 + 2,
        }));
        let columns = StreamColumns::from_columns(all);
        self.memtable.add_dataset(&ds(name), columns.clone());
        self.chain.set_current(&ds(name), Ok(None));
        self.columns.insert(name.into(), columns);
    }

    /// 可见集中推入一个未绑定的代，行为 `(_timestamp, 字段对象)`。
    fn memtable_rows(&self, stream: &str, rows: &[(i64, Value)]) {
        let b = batch(&self.columns[stream], rows);
        self.memtable.push_generation(&ds(stream), vec![b]);
    }

    /// 让 stream 的 `_current` 指向快照 `id`，快照只含一个桶、桶里是 `files`（路径 → 行）。
    fn commit(&self, stream: &str, id: u64, files: &[(&str, Vec<(i64, Value)>)]) {
        let entries: Vec<FileEntry> = files
            .iter()
            .map(|(path, rows)| {
                let b = batch(&self.columns[stream], rows);
                self.files.put(path, vec![b]);
                let ts: Vec<i64> = rows.iter().map(|r| r.0).collect();
                file_entry(
                    path,
                    *ts.iter().min().unwrap(),
                    *ts.iter().max().unwrap(),
                    rows.len(),
                )
            })
            .collect();
        self.commit_entries(stream, id, entries);
    }

    fn commit_entries(&self, stream: &str, id: u64, entries: Vec<FileEntry>) {
        // 每个文件一个桶，以便桶级剪枝与文件级剪枝都能被测到；同一桶内多文件另用 commit_bucket。
        let buckets = entries
            .into_iter()
            .enumerate()
            .map(|(i, e)| self.bucket(stream, &format!("b{id}-{i}"), vec![e]))
            .collect();
        self.put_snapshot(stream, id, buckets);
    }

    fn bucket(&self, stream: &str, name: &str, files: Vec<FileEntry>) -> BucketRef {
        let path = format!("manifest/buckets/{name}.json");
        let bucket = BucketRef {
            bucket: name.into(),
            manifest: path.clone(),
            content_digest: Digest([0; 32]),
            record_count: files.iter().map(|f| f.record_count).sum(),
            size_bytes: files.iter().map(|f| f.size_bytes).sum(),
            time_min: files.iter().map(|f| f.min_timestamp).min(),
            time_max: files.iter().map(|f| f.max_timestamp).max(),
        };
        self.chain.put_bucket_manifest(
            &ds(stream),
            &path,
            BucketManifest {
                format_version: 1,
                bucket: name.into(),
                files,
                deletions: Vec::new(),
                handovers: Vec::new(),
            },
        );
        bucket
    }

    fn put_snapshot(&self, stream: &str, id: u64, buckets: Vec<BucketRef>) {
        self.put_snapshot_with(stream, id, buckets, ColdSummary::EMPTY);
    }

    /// 同 [`Fx::put_snapshot`]，另给 `summary.cold`。
    fn put_snapshot_with(&self, stream: &str, id: u64, buckets: Vec<BucketRef>, cold: ColdSummary) {
        self.chain.put_snapshot(
            &ds(stream),
            Snapshot {
                format_version: 1,
                snapshot_id: SnapshotId(id),
                parent_snapshot_id: None,
                parent_digest: None,
                created_at: 0,
                schema: self.columns[stream].columns().to_vec(),
                buckets,
                summary: Summary {
                    dataset: format!("default/{stream}"),
                    record_count: 0,
                    size_bytes: 0,
                    time_min: None,
                    time_max: None,
                    cold,
                },
                retired_segment: None,
                cold_deletion_segment: None,
            },
        );
        self.chain
            .set_current(&ds(stream), Ok(Some(SnapshotId(id))));
    }

    async fn run(&self, body: Value) -> Result<Value, (Reason, String)> {
        self.engine
            .search("default", body.to_string().as_bytes())
            .await
            .map(|r| r.to_json())
            .map_err(|e| (e.reason, e.message))
    }

    /// 以 `[0, END)` 查询 `sql`。
    async fn sql(&self, sql: &str) -> Result<Value, Reason> {
        self.run(json!({"query": {"sql": sql, "start_time": 0, "end_time": END}}))
            .await
            .map_err(|e| e.0)
    }

    async fn ok(&self, sql: &str) -> Value {
        match self.sql(sql).await {
            Ok(v) => v,
            Err(r) => panic!("{sql}: {r:?}"),
        }
    }
}

fn file_entry(path: &str, min: i64, max: i64, records: usize) -> FileEntry {
    FileEntry {
        path: path.into(),
        min_timestamp: min,
        max_timestamp: max,
        record_count: records as u64,
        size_bytes: 1000 + records as u64,
        content_digest: Digest([0; 32]),
        added_in: SnapshotId(1),
        column_stats: None,
    }
}

/// 按 stream 的列造一个批次（全部列；`_timestamp` 为可见集与数据文件中的时间戳类型）。
fn batch(columns: &StreamColumns, rows: &[(i64, Value)]) -> RecordBatch {
    let schema = Arc::new(columns.arrow_schema());
    let arrays: Vec<ArrayRef> = columns
        .columns()
        .iter()
        .map(|c| -> ArrayRef {
            let get = |r: &(i64, Value)| r.1.get(&c.name).cloned().filter(|v| !v.is_null());
            if c.name == TIMESTAMP_COLUMN {
                return Arc::new(
                    TimestampMicrosecondArray::from(rows.iter().map(|r| r.0).collect::<Vec<_>>())
                        .with_timezone("UTC"),
                );
            }
            match c.column_type {
                ColumnType::Int64 => {
                    let mut b = Int64Builder::new();
                    rows.iter()
                        .for_each(|r| b.append_option(get(r).map(|v| v.as_i64().unwrap())));
                    Arc::new(b.finish())
                }
                ColumnType::Float64 => {
                    let mut b = Float64Builder::new();
                    rows.iter()
                        .for_each(|r| b.append_option(get(r).map(|v| v.as_f64().unwrap())));
                    Arc::new(b.finish())
                }
                ColumnType::Boolean => {
                    let mut b = BooleanBuilder::new();
                    rows.iter()
                        .for_each(|r| b.append_option(get(r).map(|v| v.as_bool().unwrap())));
                    Arc::new(b.finish())
                }
                ColumnType::Utf8 => {
                    let mut b = StringBuilder::new();
                    rows.iter().for_each(|r| {
                        b.append_option(get(r).map(|v| v.as_str().unwrap().to_owned()))
                    });
                    Arc::new(b.finish())
                }
            }
        })
        .collect();
    RecordBatch::try_new(schema, arrays).unwrap()
}

fn hits(v: &Value) -> &Vec<Value> {
    v["hits"].as_array().unwrap()
}

fn col(v: &Value, name: &str) -> Vec<Value> {
    hits(v).iter().map(|h| h[name].clone()).collect()
}

use ColumnType::{Boolean as B, Float64 as F, Int64 as I, Utf8 as S};

// ---------------------------------------------------------------------------
// 请求与时间范围
// ---------------------------------------------------------------------------

#[tokio::test]
async fn time_range_half_open_and_default_descending() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    fx.memtable_rows(
        "app",
        &[(1000, json!({})), (2000, json!({})), (3000, json!({}))],
    );
    let r = fx
        .run(
            json!({"query":{"sql":"SELECT _timestamp FROM app","start_time":1000,"end_time":3000}}),
        )
        .await
        .unwrap();
    assert_eq!(col(&r, "_timestamp"), [json!(2000), json!(1000)]);
    assert_eq!(r["total"], 2);
    assert_eq!(r["scan_records"], 2, "只计落在时间范围内的内存表行");
}

#[tokio::test]
async fn size_and_from_defaults_are_echoed() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    let r = fx.ok("SELECT * FROM app").await;
    assert_eq!(
        (r["size"].clone(), r["from"].clone()),
        (json!(100), json!(0))
    );
    let e = fx
        .run(json!({"query":{"sql":"SELECT * FROM app","start_time":0,"end_time":1,"size":"10"}}))
        .await
        .unwrap_err();
    assert_eq!(e.0, Reason::InvalidRequest);
}

// ---------------------------------------------------------------------------
// 结果集：排序、LIMIT、from/size
// ---------------------------------------------------------------------------

#[tokio::test]
async fn limit_takes_newest_without_order_by() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    fx.memtable_rows(
        "app",
        &[
            (1000, json!({})),
            (3000, json!({})),
            (2000, json!({})),
            (4000, json!({})),
        ],
    );
    let r = fx
        .run(json!({"query":{"sql":"SELECT _timestamp FROM app LIMIT 2","start_time":0,"end_time":5000}}))
        .await
        .unwrap();
    assert_eq!(col(&r, "_timestamp"), [json!(4000), json!(3000)]);
}

#[tokio::test]
async fn limit_versus_size_and_pagination() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    let rows: Vec<_> = (1..=10).map(|i| (i * 100, json!({}))).collect();
    fx.memtable_rows("app", &rows);
    let r = fx
        .run(json!({"query":{"sql":"SELECT * FROM app LIMIT 3","start_time":0,"end_time":END,"size":100}}))
        .await
        .unwrap();
    assert_eq!(hits(&r).len(), 3);

    fx.stream("pg", &[]);
    fx.memtable_rows(
        "pg",
        &[1000, 2000, 3000, 4000, 5000].map(|t| (t, json!({}))),
    );
    let q = |sql: &str, extra: Value| {
        let mut body = json!({"query":{"sql":sql,"start_time":0,"end_time":10000}});
        body["query"]
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        body
    };
    let r = fx
        .run(q("SELECT _timestamp FROM pg", json!({"size":2,"from":1})))
        .await
        .unwrap();
    assert_eq!(col(&r, "_timestamp"), [json!(4000), json!(3000)]);
    assert_eq!(
        (r["from"].clone(), r["size"].clone(), r["total"].clone()),
        (json!(1), json!(2), json!(2))
    );
    let r = fx
        .run(q(
            "SELECT _timestamp FROM pg LIMIT 3",
            json!({"size":10,"from":2}),
        ))
        .await
        .unwrap();
    assert_eq!(col(&r, "_timestamp"), [json!(3000)]);
    assert_eq!(r["size"], 10, "size 取生效值，不因 LIMIT 改变");
    let r = fx
        .run(q(
            "SELECT _timestamp FROM pg ORDER BY _timestamp ASC",
            json!({"size":2}),
        ))
        .await
        .unwrap();
    assert_eq!(col(&r, "_timestamp"), [json!(1000), json!(2000)]);
    let r = fx
        .run(q("SELECT _timestamp FROM pg", json!({"size":0})))
        .await
        .unwrap();
    assert!(hits(&r).is_empty());
}

#[tokio::test]
async fn order_by_multiple_columns() {
    let mut fx = Fx::new();
    fx.stream("ob", &[("g", S), ("v", I)]);
    fx.memtable_rows(
        "ob",
        &[
            (1, json!({"g":"a","v":2})),
            (2, json!({"g":"b","v":1})),
            (3, json!({"g":"a","v":1})),
        ],
    );
    let r = fx.ok("SELECT g, v FROM ob ORDER BY g, v DESC").await;
    assert_eq!(
        hits(&r),
        &vec![
            json!({"g":"a","v":2}),
            json!({"g":"a","v":1}),
            json!({"g":"b","v":1})
        ]
    );
}

#[tokio::test]
async fn nulls_sort_as_largest() {
    let mut fx = Fx::new();
    fx.stream("app", &[("latency", I), ("x", I)]);
    fx.memtable_rows(
        "app",
        &[
            (1, json!({"latency":30})),
            (2, json!({"x":1})),
            (3, json!({"latency":120})),
        ],
    );
    let r = fx.ok("SELECT latency FROM app ORDER BY latency ASC").await;
    assert_eq!(
        hits(&r),
        &vec![json!({"latency":30}), json!({"latency":120}), json!({})]
    );
    let r = fx.ok("SELECT latency FROM app ORDER BY latency DESC").await;
    assert_eq!(
        hits(&r),
        &vec![json!({}), json!({"latency":120}), json!({"latency":30})]
    );
}

#[tokio::test]
async fn strings_compare_and_sort_by_code_point() {
    let mut fx = Fx::new();
    fx.stream("app", &[("host", S)]);
    fx.memtable_rows(
        "app",
        &[
            (1, json!({"host":"apple"})),
            (2, json!({"host":"Zebra"})),
            (3, json!({"host":"éclair"})),
        ],
    );
    let r = fx.ok("SELECT host FROM app ORDER BY host").await;
    assert_eq!(
        col(&r, "host"),
        [json!("Zebra"), json!("apple"), json!("éclair")]
    );
    let r = fx.ok("SELECT host FROM app WHERE host > 'B'").await;
    assert_eq!(hits(&r).len(), 3);
    let r = fx.ok("SELECT host FROM app WHERE host < 'a'").await;
    assert_eq!(col(&r, "host"), [json!("Zebra")]);
}

// ---------------------------------------------------------------------------
// WHERE 语义
// ---------------------------------------------------------------------------

#[tokio::test]
async fn parentheses_quoted_identifiers_and_lexical_rules() {
    let mut fx = Fx::new();
    fx.stream(
        "app",
        &[
            ("level", S),
            ("host", S),
            ("Host", S),
            ("host.name", S),
            ("a", I),
        ],
    );
    fx.memtable_rows(
        "app",
        &[
            (
                1,
                json!({"level":"error","host":"a","Host":"H","host.name":"n","a":1}),
            ),
            (
                2,
                json!({"level":"info","host":"b","Host":"H","host.name":"n"}),
            ),
            (
                3,
                json!({"level":"error","host":"c","Host":"H","host.name":"n"}),
            ),
        ],
    );
    let r = fx
        .ok("SELECT host FROM app WHERE level = 'error' AND (host = 'a' OR host = 'b')")
        .await;
    assert_eq!(col(&r, "host"), [json!("a")]);
    let r = fx.ok("SELECT \"Host\", \"host.name\" FROM app").await;
    assert!(
        hits(&r)
            .iter()
            .all(|h| h.get("Host").is_some() && h.get("host.name").is_some())
    );
    for sql in [
        "SELECT a FROM app WHERE a = 1;",
        "SELECT a /* c */ FROM app WHERE a = 1 -- tail",
        "SELECT A FROM APP WHERE A = 1",
    ] {
        assert_eq!(hits(&fx.ok(sql).await), &vec![json!({"a":1})], "{sql}");
    }
    assert_eq!(
        fx.sql("SELECT \"A\" FROM app").await.unwrap_err(),
        Reason::UnknownColumn
    );
}

#[tokio::test]
async fn where_operator_table() {
    let mut fx = Fx::new();
    fx.stream("wo", &[("a", I), ("m", S)]);
    let mut rows: Vec<_> = (1..=5).map(|a| (a, json!({"a":a,"m":"x"}))).collect();
    rows.push((6, json!({"a":6})));
    fx.memtable_rows("wo", &rows);
    for (cond, n) in [
        ("NOT (a = 1)", 5),
        ("a <> 2 AND a != 3", 4),
        ("a NOT IN (1, 2)", 4),
        ("a IN (+1, -1)", 1),
        ("a <= 2 OR a >= 5", 4),
        ("m IS NULL", 1),
        ("m IS NOT NULL", 5),
    ] {
        let r = fx.ok(&format!("SELECT a FROM wo WHERE {cond}")).await;
        assert_eq!(r["total"], n, "{cond}");
    }
}

#[tokio::test]
async fn numeric_comparisons_are_exact() {
    let mut fx = Fx::new();
    fx.stream("app", &[("latency", F), ("code", I), ("id", I)]);
    fx.memtable_rows(
        "app",
        &[
            (1, json!({"latency":150.5,"code":200})),
            (2, json!({"id":9007199254740993_i64})),
        ],
    );
    let r = fx
        .ok("SELECT code FROM app WHERE latency > 100 AND code = 200.0")
        .await;
    assert_eq!(hits(&r).len(), 1);
    let r = fx
        .ok("SELECT id FROM app WHERE id = 9007199254740992.0")
        .await;
    assert!(hits(&r).is_empty(), "不得把 Int64 近似为 Float64 再比");
    let r = fx
        .ok("SELECT id FROM app WHERE id = 9007199254740993.0")
        .await;
    assert_eq!(hits(&r).len(), 1);

    // Int64 列与非整数、越界字面量
    for (cond, n) in [
        ("code < 200.5", 1),
        ("code <= 199.5", 0),
        ("code > 199.9", 1),
        ("code >= 200.1", 0),
        ("code = 200.1", 0),
        ("code <> 200.1", 1),
        ("code < 1e30", 1),
        ("code > -1e30", 1),
        ("code > 1e30", 0),
        ("code = 1e30", 0),
        ("NOT (code = 1e30)", 1),
        ("code IN (1.5, 200)", 1),
        ("code NOT IN (1.5, 2)", 1),
        ("200.5 > code", 1),
    ] {
        let r = fx.ok(&format!("SELECT code FROM app WHERE {cond}")).await;
        assert_eq!(r["total"], n, "{cond}");
    }
    // Float64 列：字面量先就近舍入为双精度再比（REQ-AMB-QRY-SEARCH-ENDPOINT-V3）
    for (cond, n) in [
        ("latency = 150.5", 1),
        ("latency < 150.50000000000000001", 0),
        ("latency > 150.49999999999999999", 0),
        ("latency = 150.50000000000000001", 1),
        ("latency >= 150.50000000000000001", 1),
        ("latency < 150.50000000000003", 1),
        ("latency = 150.50000000000003", 0),
        ("latency < 1e400", 1),
        ("latency > -1e400", 1),
        ("latency > 1e400", 0),
        ("150 < latency", 1),
    ] {
        let r = fx.ok(&format!("SELECT code FROM app WHERE {cond}")).await;
        assert_eq!(r["total"], n, "{cond}");
    }
    // NULL 行在 NOT 之下仍不出现（三值逻辑）
    let r = fx.ok("SELECT id FROM app WHERE NOT (code = 1.5)").await;
    assert_eq!(r["total"], 1);
}

#[tokio::test]
async fn float_zero_matches_both_signs() {
    let mut fx = Fx::new();
    fx.stream("z", &[("f", F)]);
    fx.memtable_rows(
        "z",
        &[
            (1, json!({"f":-0.0})),
            (2, json!({"f":0.0})),
            (3, json!({"f":1.0})),
        ],
    );
    for (cond, n) in [
        ("f = 0", 2),
        ("f = -0.0", 2),
        ("f <> 0", 1),
        ("f < 0", 0),
        ("f <= 0", 2),
        ("f > 0", 1),
        ("f >= 0", 3),
    ] {
        let r = fx.ok(&format!("SELECT f FROM z WHERE {cond}")).await;
        assert_eq!(r["total"], n, "{cond}");
    }
}

#[tokio::test]
async fn like_and_string_literal_semantics() {
    let mut fx = Fx::new();
    fx.stream("app", &[("m", S)]);
    fx.memtable_rows(
        "app",
        &[
            (1, json!({"m":"disk 50% full"})),
            (2, json!({"m":"disk 500 full"})),
        ],
    );
    let r = fx.ok("SELECT m FROM app WHERE m LIKE '%50\\%%'").await;
    assert_eq!(col(&r, "m"), [json!("disk 50% full")]);

    fx.stream("sl", &[("m", S)]);
    fx.memtable_rows(
        "sl",
        &[
            (1, json!({"m":"a\\b"})),
            (2, json!({"m":"it's"})),
            (3, json!({"m":"ab"})),
            (4, json!({"m":"AB"})),
            (5, json!({"m":"é1"})),
        ],
    );
    for (cond, want) in [
        ("m = 'a\\b'", "a\\b"),
        ("m = 'it''s'", "it's"),
        ("m LIKE 'a_'", "ab"),
        ("m LIKE '_1'", "é1"),
        ("m LIKE 'a\\\\b'", "a\\b"),
        ("m LIKE 'A%'", "AB"),
    ] {
        let r = fx.ok(&format!("SELECT m FROM sl WHERE {cond}")).await;
        assert_eq!(col(&r, "m"), [json!(want)], "{cond}");
    }
    let r = fx.ok("SELECT m FROM sl WHERE m NOT LIKE '%b'").await;
    assert_eq!(r["total"], 3);
}

#[tokio::test]
async fn type_errors_are_invalid_sql() {
    let mut fx = Fx::new();
    fx.stream("tc", &[("a", I), ("f", F), ("b", B), ("m", S)]);
    fx.memtable_rows("tc", &[(1, json!({"a":1,"f":1.5,"b":true,"m":"x"}))]);
    for cond in [
        "a = 'x'",
        "a = f",
        "b = 1",
        "m = 1",
        "a = NULL",
        "a IN (1, NULL)",
        "a LIKE 'x'",
        "m LIKE 'x\\'",
    ] {
        assert_eq!(
            fx.sql(&format!("SELECT a FROM tc WHERE {cond}"))
                .await
                .unwrap_err(),
            Reason::InvalidSql,
            "{cond}"
        );
    }
    for cond in ["a = 1.0", "f > 1", "b = TRUE", "m NOT LIKE 'y%'"] {
        let r = fx.ok(&format!("SELECT a FROM tc WHERE {cond}")).await;
        assert_eq!(r["total"], 1, "{cond}");
    }
}

// ---------------------------------------------------------------------------
// 响应
// ---------------------------------------------------------------------------

#[tokio::test]
async fn envelope_fields_and_null_columns_omitted() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I), ("b", S)]);
    fx.memtable_rows("app", &[(2, json!({"a":1})), (1, json!({"b":"x"}))]);
    let r = fx.ok("SELECT a, b FROM app").await;
    let keys: Vec<&str> = r.as_object().unwrap().keys().map(String::as_str).collect();
    for k in [
        "took",
        "hits",
        "total",
        "from",
        "size",
        "scan_records",
        "scan_files",
        "scan_bytes",
    ] {
        assert!(keys.contains(&k), "缺 {k}");
    }
    assert_eq!(hits(&r)[0], json!({"a":1}));
}

#[tokio::test]
async fn hit_values_render_by_type() {
    let mut fx = Fx::new();
    fx.stream("hv", &[("i", I), ("f", F), ("g", F), ("b", B), ("s", S)]);
    fx.memtable_rows(
        "hv",
        &[
            (
                1_700_000_000_000_000,
                json!({"i":7,"f":100.0,"g":1e300,"b":false,"s":"x"}),
            ),
            (1, json!({"f":1e-7,"g":-2.5e20})),
        ],
    );
    let body = fx
        .engine
        .search(
            "default",
            json!({"query":{"sql":"SELECT _timestamp, i, f, g, b, s FROM hv","start_time":0,"end_time":END}})
                .to_string()
                .as_bytes(),
        )
        .await
        .unwrap()
        .to_body();
    let text = String::from_utf8(body).unwrap();
    for part in [
        "\"_timestamp\":1700000000000000",
        "\"i\":7",
        "\"f\":100.0",
        "\"g\":1e300",
        "\"b\":false",
        "\"s\":\"x\"",
        "\"f\":1e-7",
        "\"g\":-2.5e20",
    ] {
        assert!(text.contains(part), "{part} not in {text}");
    }
}

// ---------------------------------------------------------------------------
// 可见性与剪枝（REQ-AMB-QRY-PLAN-VISIBILITY）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn memtable_and_committed_files_with_scan_statistics() {
    let mut fx = Fx::new();
    fx.stream("st", &[("s", I)]);
    fx.commit(
        "st",
        1,
        &[(
            "data/f.parquet",
            (1..=3).map(|s| (s * 10, json!({"s":s}))).collect(),
        )],
    );
    fx.memtable_rows("st", &[(40, json!({"s":4})), (50, json!({"s":5}))]);
    let r = fx.ok("SELECT * FROM st").await;
    let mut s: Vec<i64> = col(&r, "s").iter().map(|v| v.as_i64().unwrap()).collect();
    s.sort();
    assert_eq!(s, [1, 2, 3, 4, 5]);
    assert_eq!(r["scan_files"], 1);
    assert_eq!(r["scan_bytes"], 1003, "F 在桶 manifest 中的 size_bytes");
    assert_eq!(r["scan_records"], 5);
}

/// ADR-0021「逐情形核对」：同一批记录在冻结代与已提交文件中各有一份时，按绑定号与 Q 只取一份。
#[tokio::test]
async fn flush_boundary_is_exactly_once() {
    let mut fx = Fx::new();
    fx.stream("fb", &[("seq", I)]);
    let rows: Vec<_> = (1..=3).map(|s| (s, json!({"seq":s}))).collect();
    let b = batch(&fx.columns["fb"], &rows);
    let gen1 = fx.memtable.push_generation(&ds("fb"), vec![b]);
    fx.memtable_rows("fb", &[(4, json!({"seq":4}))]);

    let seqs = |r: Value| {
        let mut s: Vec<i64> = col(&r, "seq").iter().map(|v| v.as_i64().unwrap()).collect();
        s.sort();
        s
    };
    // 冻结代已绑定到 P = 1，条件写尚未成功：`_current` 仍不存在（Q 按 0 计），从内存出
    fx.memtable.bind(&ds("fb"), gen1, SnapshotId(1));
    assert_eq!(seqs(fx.ok("SELECT seq FROM fb").await), [1, 2, 3, 4]);

    // 条件写成功（Q = 1 ≥ P），代尚未移除：只从文件出
    fx.commit("fb", 1, &[("data/f1.parquet", rows.clone())]);
    let r = fx.ok("SELECT seq FROM fb").await;
    assert_eq!(r["scan_files"], 1);
    assert_eq!(seqs(r), [1, 2, 3, 4]);

    // 回执之后代被移除：同样只出一份
    fx.memtable.remove(&ds("fb"), gen1);
    assert_eq!(seqs(fx.ok("SELECT seq FROM fb").await), [1, 2, 3, 4]);
}

#[tokio::test]
async fn pruning_by_file_time_range() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    const H: i64 = 3_600_000_000;
    fx.commit(
        "app",
        1,
        &[
            ("data/h1.parquet", vec![(10, json!({})), (H - 1, json!({}))]),
            (
                "data/h2.parquet",
                vec![(H, json!({})), (2 * H - 1, json!({}))],
            ),
        ],
    );
    let r = fx
        .run(json!({"query":{"sql":"SELECT * FROM app","start_time":H,"end_time":2 * H}}))
        .await
        .unwrap();
    assert_eq!(r["scan_files"], 1);
    assert_eq!(fx.files.reads(), ["data/h2.parquet"], "不读无交集的文件");
    assert_eq!(r["total"], 2);
}

#[tokio::test]
async fn pruning_boundary_closed_file_half_open_query() {
    let (a, b) = (1_000_000, 2_000_000);
    for (start, end, want) in [
        (b, b + 1, 1),
        (b + 1, b + 100, 0),
        (a - 100, a, 0),
        (a - 100, a + 1, 1),
    ] {
        let mut fx = Fx::new();
        fx.stream("pb", &[]);
        fx.commit(
            "pb",
            1,
            &[("data/f.parquet", vec![(a, json!({})), (b, json!({}))])],
        );
        let r = fx
            .run(json!({"query":{"sql":"SELECT * FROM pb","start_time":start,"end_time":end}}))
            .await
            .unwrap();
        assert_eq!(r["scan_files"], want, "[{start}, {end})");
    }
}

#[tokio::test]
async fn pruning_files_within_one_bucket() {
    let mut fx = Fx::new();
    fx.stream("pf", &[]);
    const MIN: i64 = 60_000_000;
    let t0 = 1_787_392_800_000_000; // 2026-08-22T10:00:00Z
    let rows = |a: i64, b: i64| vec![(t0 + a * MIN, json!({})), (t0 + b * MIN, json!({}))];
    let mut entries = Vec::new();
    for (path, r) in [
        ("data/f1.parquet", rows(1, 2)),
        ("data/f2.parquet", rows(31, 32)),
    ] {
        let bt = batch(&fx.columns["pf"], &r);
        fx.files.put(path, vec![bt]);
        entries.push(file_entry(path, r[0].0, r[1].0, 2));
    }
    let bucket = fx.bucket("pf", "2026082210", entries);
    fx.put_snapshot("pf", 1, vec![bucket]);
    let r = fx
        .run(json!({"query":{"sql":"SELECT * FROM pf","start_time":t0 + 20 * MIN,"end_time":t0 + 40 * MIN}}))
        .await
        .unwrap();
    assert_eq!(r["scan_files"], 1);
    assert_eq!(r["total"], 2);
}

#[tokio::test]
async fn bucket_outside_range_is_not_read() {
    let mut fx = Fx::new();
    fx.stream("app", &[]);
    fx.commit("app", 1, &[("data/old.parquet", vec![(10, json!({}))])]);
    fx.chain.fail_bucket_manifest(
        &ds("app"),
        "manifest/buckets/b1-0.json",
        ChainError::Unavailable("down".into()),
    );
    let r = fx
        .run(json!({"query":{"sql":"SELECT * FROM app","start_time":100,"end_time":200}}))
        .await
        .unwrap();
    assert_eq!(r["scan_files"], 0);
    let e = fx.sql("SELECT * FROM app").await.unwrap_err();
    assert_eq!(e, Reason::Unavailable, "覆盖该桶时才读它的 manifest");
}

// ---------------------------------------------------------------------------
// 错误与报告顺序
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stream_resolution() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    for sql in [
        "SELECT * FROM no_such_stream",
        "SELECT a FROM _sys",
        "SELECT * FROM \"App\"",
        "SELECT typo FROM no_such",
    ] {
        assert_eq!(
            fx.sql(sql).await.unwrap_err(),
            Reason::StreamNotFound,
            "{sql}"
        );
    }
    // `count(*)` 合法，不存在的 stream 回 `stream_not_found`；子集外的调用仍先于它报告
    assert_eq!(
        fx.sql("SELECT count(*) FROM no_such").await.unwrap_err(),
        Reason::StreamNotFound
    );
    assert_eq!(
        fx.sql("SELECT lower(a) FROM no_such").await.unwrap_err(),
        Reason::SqlUnsupported
    );
    assert_eq!(
        fx.sql("SELEC * FROM app").await.unwrap_err(),
        Reason::InvalidSql
    );
    assert_eq!(
        fx.sql("SELECT typo FROM app WHERE a = 'x'")
            .await
            .unwrap_err(),
        Reason::UnknownColumn
    );
    let e = fx
        .run(json!({"query":{"sql":"SELEC * FROM app","start_time":3000,"end_time":1000}}))
        .await
        .unwrap_err();
    assert_eq!(e.0, Reason::InvalidTimeRange);
    assert_eq!(fx.chain.reads(), 1, "stream 不存在的各次都不读快照链");
}

#[tokio::test]
async fn disabled_dataset_answers_by_state_without_reading() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.gates.enter(&ds("app"), DatasetState::ManifestUnreadable);
    let e = fx.sql("SELECT typo FROM app").await.unwrap_err();
    assert_eq!(e, Reason::Internal);
    fx.gates.enter(&ds("app"), DatasetState::IncompatibleFormat);
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::UnsupportedFormatVersion
    );
    assert_eq!(fx.chain.reads(), 0);
}

#[tokio::test]
async fn chain_failures_map_and_precede_column_checks() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.chain
        .set_current(&ds("app"), Err(ChainError::Unavailable("down".into())));
    assert_eq!(
        fx.sql("SELECT typo FROM app").await.unwrap_err(),
        Reason::Unavailable
    );

    fx.chain.set_current(&ds("app"), Ok(Some(SnapshotId(3))));
    fx.chain.fail_snapshot(
        &ds("app"),
        SnapshotId(3),
        ChainError::Integrity("bad json".into()),
    );
    assert_eq!(
        fx.sql("SELECT typo FROM app").await.unwrap_err(),
        Reason::Internal
    );

    fx.chain.fail_snapshot(
        &ds("app"),
        SnapshotId(3),
        ChainError::Disabled(DatasetState::IncompatibleFormat),
    );
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::UnsupportedFormatVersion
    );
}

#[tokio::test]
async fn manifest_and_file_failures_come_after_type_checks() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.commit("app", 1, &[("data/f.parquet", vec![(10, json!({"a":1}))])]);
    fx.chain.fail_bucket_manifest(
        &ds("app"),
        "manifest/buckets/b1-0.json",
        ChainError::Integrity("digest".into()),
    );
    assert_eq!(
        fx.sql("SELECT a FROM app WHERE a = 'x'").await.unwrap_err(),
        Reason::InvalidSql
    );
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Internal
    );

    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.commit("app", 1, &[("data/f.parquet", vec![(10, json!({"a":1}))])]);
    fx.files
        .fail("data/f.parquet", FileError::Unavailable("403".into()));
    assert_eq!(
        fx.sql("SELECT a FROM app").await.unwrap_err(),
        Reason::Unavailable
    );
}

/// 创世快照尚未成为 `_current` 的 stream 只以可见集作答：`_timestamp` 可查，结果为空。
#[tokio::test]
async fn stream_without_snapshot_answers_from_memtable() {
    let mut fx = Fx::new();
    fx.stream("app5", &[]);
    let r = fx.ok("SELECT _timestamp FROM app5").await;
    assert!(hits(&r).is_empty());
    assert_eq!(r["total"], 0);
    assert_eq!(r["scan_files"], 0);
}

/// Float64 列存的是二进制浮点数：写入的 `0.1` 精确值略大于十进制的 0.1。字面量先就近舍入为双精度再比
/// （REQ-AMB-QRY-SEARCH-ENDPOINT-V3），所以 `= 0.1`、`<= 0.1` 成立、`> 0.1` 不成立；舍入到同一个双精度数的其他写法也相等。
#[tokio::test]
async fn float_column_against_decimal_literal_rounds() {
    let mut fx = Fx::new();
    fx.stream("fl", &[("ratio", F)]);
    fx.memtable_rows("fl", &[(1, json!({"ratio":0.1}))]);
    for (cond, n) in [
        ("ratio = 0.1", 1),
        ("ratio <= 0.1", 1),
        ("ratio > 0.1", 0),
        ("ratio = 0.10000000000000001", 1),
        (
            "ratio = 0.1000000000000000055511151231257827021181583404541015625",
            1,
        ),
        ("ratio IN (0.1, 0.2)", 1),
        ("ratio = 1e-400", 0),
    ] {
        let r = fx.ok(&format!("SELECT ratio FROM fl WHERE {cond}")).await;
        assert_eq!(r["total"], n, "{cond}");
    }
}

// ---------------------------------------------------------------------------
// 聚合、histogram、文本匹配与 hit 的键序（不对应块场景，块场景见 `amberlog-server` 的系统测试）
// ---------------------------------------------------------------------------

#[tokio::test]
async fn aggregate_groups_values_and_overflow() {
    let mut fx = Fx::new();
    fx.stream("ag", &[("g", S), ("n", I), ("f", F), ("ok", B)]);
    fx.memtable_rows(
        "ag",
        &[
            (1, json!({"g": "a", "n": 1, "f": 0.0, "ok": true})),
            (2, json!({"g": "a", "n": 2, "f": -0.0, "ok": false})),
            (3, json!({"g": "b", "n": 5, "f": 1.5})),
            (4, json!({})),
        ],
    );
    let r = fx
        .ok("SELECT g, count(*) AS c, sum(n) AS s, avg(n) AS a, min(ok) AS mo FROM ag GROUP BY g ORDER BY g")
        .await;
    assert_eq!(
        r["hits"],
        json!([
            {"g": "a", "c": 2, "s": 3, "a": 1.5, "mo": false},
            {"g": "b", "c": 1, "s": 5, "a": 5.0},
            {"c": 1},
        ])
    );
    let r = fx
        .ok("SELECT f, count(*) AS c FROM ag WHERE f IS NOT NULL GROUP BY f ORDER BY c DESC")
        .await;
    assert_eq!(hits(&r).len(), 2, "{r}");
    assert_eq!(hits(&r)[0]["c"], 2, "{r}");
    let r = fx
        .ok("SELECT count(*) AS n, sum(n) AS s FROM ag WHERE n > 100")
        .await;
    assert_eq!(r["hits"], json!([{"n": 0}]));
    // 只在 ORDER BY 中的聚合调用也算出并参与排序，不出现在 hit 中
    let r = fx
        .ok("SELECT g FROM ag GROUP BY g ORDER BY max(n) DESC")
        .await;
    assert_eq!(
        r["hits"],
        json!([{}, {"g": "b"}, {"g": "a"}]),
        "max(n) 为 NULL 的组在降序中排在最前"
    );

    fx.stream("ov", &[("n", I)]);
    fx.memtable_rows("ov", &[(1, json!({"n": i64::MAX})), (2, json!({"n": 1}))]);
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
        Reason::NumericOverflow
    );
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov LIMIT 0").await.unwrap_err(),
        Reason::NumericOverflow
    );
    fx.stream("fo", &[("f", F)]);
    fx.memtable_rows(
        "fo",
        &[(1, json!({"f": 1.5e308})), (2, json!({"f": 1.5e308}))],
    );
    for sql in ["SELECT sum(f) FROM fo", "SELECT avg(f) FROM fo"] {
        assert_eq!(
            fx.sql(sql).await.unwrap_err(),
            Reason::NumericOverflow,
            "{sql}"
        );
    }
}

#[tokio::test]
async fn histogram_and_text_match() {
    let mut fx = Fx::new();
    fx.stream("hm", &[("m", S), ("k", S), ("n", I)]);
    fx.memtable_rows(
        "hm",
        &[
            (0, json!({"m": "Disk ERROR", "n": 1})),
            (299_999_999, json!({"k": "error-1", "n": 2})),
            (300_000_000, json!({"m": "50% done"})),
            (900_000_001, json!({"n": 3})),
        ],
    );
    let r = fx
        .ok("SELECT histogram(_timestamp, '5m') AS t, count(*) AS n FROM hm GROUP BY t ORDER BY t")
        .await;
    assert_eq!(
        r["hits"],
        json!([{"t": 0, "n": 2}, {"t": 300_000_000, "n": 1}, {"t": 900_000_000, "n": 1}])
    );
    let r = fx
        .ok("SELECT n FROM hm WHERE match_all('ERROR') ORDER BY n")
        .await;
    assert_eq!(r["hits"], json!([{"n": 1}, {"n": 2}]));
    let r = fx
        .ok("SELECT n FROM hm WHERE NOT str_match(m, 'Disk') ORDER BY n")
        .await;
    assert_eq!(r["hits"], json!([{"n": 2}, {"n": 3}, {}]));
    let r = fx.ok("SELECT m FROM hm WHERE str_match(m, '50%')").await;
    assert_eq!(r["hits"], json!([{"m": "50% done"}]));
}

#[tokio::test]
async fn hit_keys_follow_select_order() {
    let mut fx = Fx::new();
    fx.stream("ko", &[("a", I), ("b", I), ("z", S)]);
    fx.memtable_rows("ko", &[(1, json!({"a": 1, "b": 2, "z": "x"}))]);
    let body = fx
        .engine
        .search(
            "default",
            json!({"query": {"sql": "SELECT z, b AS a, a AS b FROM ko", "start_time": 0, "end_time": END}})
                .to_string()
                .as_bytes(),
        )
        .await
        .unwrap()
        .to_body();
    let text = String::from_utf8(body).unwrap();
    assert!(text.contains(r#""hits":[{"z":"x","a":2,"b":1}]"#), "{text}");
}

// ---------------------------------------------------------------------------
// 告警评估的查询（REQ-AMB-QRY-RESOURCE-LIMITS-V2「占用执行名额的查询」，ADR-0051 第 4 条）
// ---------------------------------------------------------------------------

fn eval_query(sql: &str) -> EvalQuery {
    EvalQuery {
        sql: sql.into(),
        start_time: 0,
        end_time: END,
    }
}

#[tokio::test]
async fn evaluate_answers_like_search_with_column_types() {
    let mut fx = Fx::new();
    fx.stream("ev", &[("a", I), ("f", F), ("s", S), ("ok", B)]);
    fx.memtable_rows(
        "ev",
        &[
            (1, json!({"a": 1, "f": 0.5, "s": "x", "ok": true})),
            (2, json!({"a": 2})),
        ],
    );
    let sql = "SELECT a, f, s, ok FROM ev ORDER BY a";
    let e = fx
        .engine
        .evaluate("default", &eval_query(sql))
        .await
        .unwrap();
    assert_eq!(
        e.columns,
        [
            ("a".to_owned(), I),
            ("f".to_owned(), F),
            ("s".to_owned(), S),
            ("ok".to_owned(), B)
        ]
    );
    let r = fx.ok(sql).await;
    let rows: Vec<Value> = e.rows.iter().map(|h| Value::Object(h.to_map())).collect();
    assert_eq!(
        Value::Array(rows),
        r["hits"],
        "与同一 SQL 的 _search 同样作答"
    );
    assert!(!e.truncated);

    // 聚合查询与零行结果同样给出列类型
    let e = fx
        .engine
        .evaluate(
            "default",
            &eval_query("SELECT s, count(*) AS n FROM ev WHERE a > 5 GROUP BY s"),
        )
        .await
        .unwrap();
    assert_eq!(e.columns, [("s".to_owned(), S), ("n".to_owned(), I)]);
    assert!(e.rows.is_empty());

    // 查询范围 [start_time, end_time) 照 _search
    let q = EvalQuery {
        sql: "SELECT a FROM ev".into(),
        start_time: 2,
        end_time: 3,
    };
    let e = fx.engine.evaluate("default", &q).await.unwrap();
    assert_eq!(e.rows.len(), 1);
    assert_eq!(e.rows[0].get("a"), Some(&json!(2)));
}

#[tokio::test]
async fn evaluate_truncates_at_max_size() {
    let mut fx = Fx::new();
    fx.engine.limits = Limits {
        default_size: 1,
        max_size: 2,
    };
    fx.stream("tr", &[("a", I)]);
    fx.memtable_rows(
        "tr",
        &[
            (1, json!({"a": 1})),
            (2, json!({"a": 2})),
            (3, json!({"a": 3})),
        ],
    );
    let e = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM tr ORDER BY a"))
        .await
        .unwrap();
    assert!(e.truncated, "3 行多于 query.max_size 2");
    let a: Vec<_> = e.rows.iter().map(|h| h.get("a").cloned()).collect();
    assert_eq!(a, [Some(json!(1)), Some(json!(2))], "按最终排序取前 2 行");

    let e = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM tr WHERE a > 1"))
        .await
        .unwrap();
    assert!(!e.truncated, "恰为 query.max_size 行不算被截断");
    assert_eq!(e.rows.len(), 2);

    // SQL 的 LIMIT 之后的行数才是结果行数
    let e = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM tr LIMIT 2"))
        .await
        .unwrap();
    assert!(!e.truncated);
    let e = fx
        .engine
        .evaluate(
            "default",
            &eval_query("SELECT a, count(*) AS n FROM tr GROUP BY a"),
        )
        .await
        .unwrap();
    assert!(e.truncated, "聚合结果 3 组");
    assert_eq!(e.rows.len(), 2);
}

#[tokio::test]
async fn evaluate_shares_admission_with_search() {
    let mut fx = Fx::new();
    fx.engine.admission = crate::limits::Admission::new(1, 536_870_912);
    fx.stream("sh", &[("a", I)]);
    fx.memtable_rows("sh", &[(1, json!({"a": 1}))]);

    let held = fx.engine.admission.admit().unwrap();
    let err = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM sh"))
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::TooManyQueries, "名额已满即返回，不等待");
    // 准入之前的检查照报告顺序先报，不取名额
    let err = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM no_such"))
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::StreamNotFound);
    let err = fx
        .engine
        .evaluate("default", &eval_query("SELEC a FROM sh"))
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::InvalidSql);
    let err = fx
        .engine
        .evaluate("default", &eval_query("SELECT dataset FROM _ledger"))
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::TooManyQueries, "系统表同样取名额");
    assert_eq!(fx.engine.admission.running(), 1);
    drop(held);

    let e = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM sh"))
        .await
        .unwrap();
    assert_eq!(e.rows.len(), 1);
    assert_eq!(fx.engine.admission.running(), 0, "得出结果即释放名额");

    // 名额被占着时 _search 同样得 too_many_queries（两者共用同一组名额）
    let held = fx.engine.admission.admit().unwrap();
    assert_eq!(
        fx.sql("SELECT a FROM sh").await.unwrap_err(),
        Reason::TooManyQueries
    );
    drop(held);
}

#[tokio::test]
async fn evaluate_memory_limit_applies() {
    let mut fx = Fx::new();
    fx.engine.admission = crate::limits::Admission::new(4, 1);
    fx.stream("mm", &[("a", I)]);
    let rows: Vec<(i64, Value)> = (0..1000).map(|i| (i, json!({"a": i}))).collect();
    fx.memtable_rows("mm", &rows);
    let err = fx
        .engine
        .evaluate("default", &eval_query("SELECT a FROM mm ORDER BY a"))
        .await
        .unwrap_err();
    assert_eq!(err.reason, Reason::QueryMemoryExceeded);
    assert_eq!(fx.engine.admission.running(), 0);
}

// ---------------------------------------------------------------------------
// 逐批读取（REQ-AMB-QRY-RESOURCE-LIMITS-V2 METERED-SCOPE）：读取与执行交织之后，全有或全无与报告顺序不变
// ---------------------------------------------------------------------------

/// 执行不取输入（`LIMIT 0`）也照样读完作答所需的每个文件：读不到即以错误作答（ADR-0037）。
#[tokio::test]
async fn query_that_takes_no_input_still_reads_every_file() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.memtable_rows("app", &[(5, json!({"a": 0}))]);
    fx.commit(
        "app",
        1,
        &[
            ("data/f1.parquet", vec![(10, json!({"a": 1}))]),
            ("data/f2.parquet", vec![(20, json!({"a": 2}))]),
        ],
    );
    let r = fx.ok("SELECT a FROM app LIMIT 0").await;
    assert_eq!(r["total"], 0);
    assert_eq!(fx.files.reads(), ["data/f1.parquet", "data/f2.parquet"]);

    fx.files.fail(
        "data/f2.parquet",
        FileError::Integrity("not parquet".into()),
    );
    assert_eq!(
        fx.sql("SELECT a FROM app LIMIT 0").await.unwrap_err(),
        Reason::Internal
    );
    fx.files
        .fail("data/f2.parquet", FileError::Unavailable("403".into()));
    assert_eq!(
        fx.sql("SELECT a FROM app LIMIT 0").await.unwrap_err(),
        Reason::Unavailable
    );
}

/// 读取的 503、500 排在 `numeric_overflow` 之前（REQ-AMB-QRY-SEARCH-ENDPOINT 报告顺序）：溢出的行已交给执行，后面的
/// 文件读不到，仍以读取失败作答。
#[tokio::test]
async fn read_failures_precede_numeric_overflow() {
    let mut fx = Fx::new();
    fx.stream("ov", &[("n", I)]);
    fx.memtable_rows("ov", &[(1, json!({"n": i64::MAX})), (2, json!({"n": 1}))]);
    fx.commit("ov", 1, &[("data/f.parquet", vec![(10, json!({"n": 1}))])]);
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
        Reason::NumericOverflow
    );
    fx.files
        .fail("data/f.parquet", FileError::Unavailable("403".into()));
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
        Reason::Unavailable
    );
    let good = batch(&fx.columns["ov"], &[(10, json!({"n": 1}))]);
    fx.files.fail_after(
        "data/f.parquet",
        vec![good],
        FileError::Integrity("torn".into()),
    );
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
        Reason::Internal
    );
}

/// 多个分区同时从输入取批时，报告顺序仍是确定的：溢出的行在前面的文件里已交给执行，最后一个文件读不到，仍以读取
/// 失败作答，每次都一样（REQ-AMB-QRY-SEARCH-ENDPOINT 报告顺序）。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_failure_after_many_files_still_precedes_numeric_overflow() {
    const FILES: usize = 24;
    let mut fx = Fx::new();
    fx.stream("ov", &[("n", I)]);
    let paths: Vec<String> = (0..FILES)
        .map(|i| format!("data/f{i:02}.parquet"))
        .collect();
    let files: Vec<(&str, Vec<(i64, Value)>)> = paths
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let n = if i == 0 { i64::MAX } else { 1 };
            (p.as_str(), vec![(10 + i as i64, json!({"n": n}))])
        })
        .collect();
    fx.commit("ov", 1, &files);
    assert_eq!(
        fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
        Reason::NumericOverflow
    );
    fx.files
        .fail(&paths[FILES - 1], FileError::Unavailable("403".into()));
    for _ in 0..20 {
        assert_eq!(
            fx.sql("SELECT sum(n) FROM ov").await.unwrap_err(),
            Reason::Unavailable
        );
    }
}

/// 文件解码到中途失败：已交出的批次不形成部分结果，以 500 作答。
#[tokio::test]
async fn failure_in_the_middle_of_a_file_answers_without_partial_results() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I)]);
    fx.commit("app", 1, &[("data/f.parquet", vec![(10, json!({"a": 1}))])]);
    let first = batch(&fx.columns["app"], &[(10, json!({"a": 1}))]);
    fx.files.fail_after(
        "data/f.parquet",
        vec![first],
        FileError::Integrity("torn".into()),
    );
    for sql in [
        "SELECT a FROM app",
        "SELECT count(*) AS n FROM app",
        "SELECT a FROM app LIMIT 1",
    ] {
        assert_eq!(fx.sql(sql).await.unwrap_err(), Reason::Internal, "{sql}");
    }
}

/// 只向数据文件要本查询用到的列（`_timestamp` 总要：查询范围的过滤用它）。
#[tokio::test]
async fn only_columns_the_query_uses_are_read() {
    let mut fx = Fx::new();
    fx.stream("app", &[("a", I), ("b", I), ("s", S), ("t", S)]);
    fx.commit(
        "app",
        1,
        &[(
            "data/f.parquet",
            vec![(10, json!({"a": 1, "b": 2, "s": "xy", "t": "zw"}))],
        )],
    );
    let last = |fx: &Fx| {
        let mut c = fx.files.columns().pop().unwrap();
        c.sort();
        c
    };
    let r = fx.ok("SELECT count(*) AS n FROM app").await;
    assert_eq!(col(&r, "n"), [json!(1)]);
    assert_eq!(last(&fx), ["_timestamp"]);
    let r = fx.ok("SELECT a FROM app WHERE b = 2").await;
    assert_eq!(col(&r, "a"), [json!(1)]);
    assert_eq!(last(&fx), ["_timestamp", "a", "b"]);
    let r = fx
        .ok("SELECT count(*) AS n FROM app WHERE match_all('ZW')")
        .await;
    assert_eq!(col(&r, "n"), [json!(1)]);
    assert_eq!(last(&fx), ["_timestamp", "s", "t"]);
    let r = fx.ok("SELECT * FROM app").await;
    assert_eq!(hits(&r)[0]["t"], json!("zw"));
    assert_eq!(last(&fx), ["_timestamp", "a", "b", "s", "t"]);
}

mod cold;
