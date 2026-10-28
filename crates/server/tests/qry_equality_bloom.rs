//! REQ-AMB-QRY-EQUALITY-BLOOM 的系统测试：以子进程运行 `amberlog`，
//! 对象存储用 `file://` 临时目录。「某列有 bloom」「某列没有 bloom」由测试以 `parquet` 读取器读数据文件的尾部元数据、
//! 逐行组看该列列块的 `bloom_filter_offset`；「该文件的 bloom 对该取值判肯定不在」由测试以 `parquet` 的 `Sbbf` 读出核对。
//!
//! 可判等值条件的筛选、多行组与 bloom 偏移越界三项另在 amberlog-query（`bloom`）的单元测试里。

mod common;

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, RecordBatch, StringArray, TimestampMicrosecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use common::api::{ingest_ok, now_us, poll};
use common::lake::ColdDir;
use common::store::{Ds, FLUSH_1S, HOUR, bucket_label, hour_of, sha256, snapshot_rel, stop};
use common::{BEARER, Fixture, Kv, Resp, Server};
use parquet::arrow::ArrowWriter;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use serde_json::{Value, json};

const WAIT: Duration = Duration::from_secs(180);
const DAY: i64 = 24 * HOUR;

/// 块的场景约定「声明配置」。
const DECLARED: Kv = (
    "index_hints",
    Some(r#"{ "default/app" = ["trace_id", "user_id", "ratio", "ok", "_timestamp"] }"#),
);
const GRACE_1S: Kv = ("lifecycle.seal_grace", Some("\"1s\""));
const GRACE_24H: Kv = ("lifecycle.seal_grace", Some("\"24h\""));
const LAKE_1H: Kv = ("lifecycle.lake_watermark", Some("\"1h\""));
const LAKE_2S: Kv = ("lifecycle.lake_watermark", Some("\"2s\""));
const LAKE_24H: Kv = ("lifecycle.lake_watermark", Some("\"24h\""));
const CC_10S: Kv = ("cold_compaction.check_interval", Some("\"10s\""));
const CC_24H: Kv = ("cold_compaction.check_interval", Some("\"24h\""));

/// 未被声明的列与声明了却不可写 bloom 的列：这些列在三种输出里都没有 bloom。
const NO_BLOOM: [&str; 5] = ["ratio", "ok", "msg", "n", "_timestamp"];

// ---------------------------------------------------------------------------
// 数据
// ---------------------------------------------------------------------------

/// 此刻前第 `k` 小时那个小时桶内的一个时刻（距整点 10 分钟）。
fn hours_ago(k: i64) -> i64 {
    hour_of(now_us()) - k * HOUR + 600_000_000
}

/// 一条记录：块的场景约定所列的六列。
fn rec(ts: i64, trace: &str, user: i64) -> String {
    format!(
        r#"{{"_timestamp":{ts},"trace_id":"{trace}","user_id":{user},"ratio":0.5,"ok":true,"msg":"m","n":1}}"#
    )
}

fn ingest_recs(s: &Server, recs: &[String]) {
    ingest_ok(s, "app", &format!("[{}]", recs.join(",")));
}

/// `ts` 起的 `n` 条记录（逐条加 1 毫秒），`trace_id`、`user_id` 依次取 `values` 循环。
fn recs(ts: i64, n: usize, values: &[(&str, i64)]) -> Vec<String> {
    (0..n)
        .map(|i| {
            let (t, u) = values[i % values.len()];
            rec(ts + i as i64 * 1000, t, u)
        })
        .collect()
}

/// H1、H2 的记录取值。
const H12: [(&str, i64); 2] = [("t-100", 100), ("t-300", 300)];
/// H3 的记录取值（`t-200` 与 200 各恰一条且是同一条，见 [`h3`]）。
const H3_OTHERS: [(&str, i64); 2] = [("t-100", 100), ("t-300", 300)];

/// H3 一次摄取的记录：`t-200`/200 只在 `first` 为真的那次出现一次。
fn h3(ts: i64, n: usize, first: bool) -> Vec<String> {
    let mut v = recs(ts, n, &H3_OTHERS);
    if first {
        v[0] = rec(ts, "t-200", 200);
    }
    v
}

/// 每次摄取的记录条数（块未固定、测试可调的部分）。
const PER_INGEST: usize = 3;

/// 三个小时的时刻 H1、H2、H3（此刻前第 5、4、3 小时）。
fn three_hours() -> [i64; 3] {
    [hours_ago(5), hours_ago(4), hours_ago(3)]
}

/// 块的场景约定「三个小时的热层数据」：`overrides` 之外以 `lifecycle.seal_grace` 为 `1s`、`lifecycle.lake_watermark`
/// 为 `24h`、`flush.interval` 为 `1s` 运行；每个小时分两次（间隔 2 秒）摄取，等三个桶都只有一个数据文件。
/// 返回实例与 H1～H3。
fn hot_three_hours(f: &Fixture, overrides: &[Kv]) -> (Server, [i64; 3]) {
    let mut kv = vec![GRACE_1S, LAKE_24H, FLUSH_1S];
    kv.extend_from_slice(overrides);
    let s = f.start(&kv);
    let hs = three_hours();
    write_hot_hours(&s, hs);
    wait_single_files(&f.dataset("app"), &hs, 2 * PER_INGEST as u64);
    (s, hs)
}

fn write_hot_hours(s: &Server, hs: [i64; 3]) {
    for second in [false, true] {
        let off = if second { 100_000 } else { 0 };
        ingest_recs(s, &recs(hs[0] + off, PER_INGEST, &H12));
        ingest_recs(s, &recs(hs[1] + off, PER_INGEST, &H12));
        ingest_recs(s, &h3(hs[2] + off, PER_INGEST, !second));
        if !second {
            std::thread::sleep(Duration::from_secs(2));
        }
    }
}

/// 等 `hours` 各桶的桶 manifest `files` 都恰有一个含 `records` 条记录的数据文件（两次摄取都已提交且合并完成），
/// 返回各桶那个文件条目。
fn wait_single_files(ds: &Ds, hours: &[i64], records: u64) -> Vec<Value> {
    hours
        .iter()
        .map(|h| {
            let label = bucket_label(*h);
            let (m, ok) = poll(
                WAIT,
                || ds.bucket_now(&label),
                |m| {
                    m.as_ref().is_some_and(|m| {
                        m["files"].as_array().is_some_and(|f| {
                            f.len() == 1 && f[0]["record_count"].as_u64() == Some(records)
                        })
                    })
                },
            );
            assert!(
                ok,
                "桶 {label} 未合并为一个含 {records} 条记录的数据文件：{m:?}"
            );
            m.unwrap()["files"][0].clone()
        })
        .collect()
}

/// 以覆盖 `[start, end)` 的时间范围、`size` 为 10000 执行 `sql`。
fn search_range(s: &Server, sql: &str, start: i64, end: i64) -> Resp {
    let body = json!({"query": {"sql": sql, "start_time": start, "end_time": end, "size": 10000}})
        .to_string();
    s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    )
}

/// 覆盖 H1 至 H3 的时间范围执行 `sql`，断言 200 并返回应答体。
fn search_hours(s: &Server, sql: &str, hs: [i64; 3]) -> Value {
    let r = search_range(s, sql, hour_of(hs[0]), hour_of(hs[2]) + HOUR);
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    r.json()
}

// ---------------------------------------------------------------------------
// 读数据文件里的 bloom
// ---------------------------------------------------------------------------

/// 数据文件 `bytes` 的尾部元数据。
fn metadata(bytes: &[u8]) -> ParquetMetaData {
    let n = bytes.len();
    assert_eq!(&bytes[n - 4..], b"PAR1", "不是 Parquet 文件");
    let len = u32::from_le_bytes(bytes[n - 8..n - 4].try_into().unwrap()) as usize;
    ParquetMetaDataReader::decode_metadata(&bytes[n - 8 - len..n - 8]).expect("尾部元数据可解析")
}

/// 数据文件 `bytes` 中列 `column`（顶层列名）逐行组的 bloom：列块带 `bloom_filter_offset` 时读出（并以 `Sbbf` 解析）。
/// 文件中没有该列时为空。
fn blooms(bytes: &[u8], column: &str) -> Vec<Option<Sbbf>> {
    let meta = metadata(bytes);
    let schema = meta.file_metadata().schema_descr();
    let Some(i) = (0..schema.num_columns()).find(|&i| schema.column(i).name() == column) else {
        return Vec::new();
    };
    meta.row_groups()
        .iter()
        .map(|g| {
            let c = g.column(i);
            c.bloom_filter_offset().map(|off| {
                let len = c.bloom_filter_length().expect("写库写出 bloom 长度") as usize;
                let off = off as usize;
                Sbbf::from_bytes(&bytes[off..off + len]).expect("bloom 可解析")
            })
        })
        .collect()
}

/// 「某列有 bloom」：每个行组中该列的列块都带 bloom。
fn has_bloom(bytes: &[u8], column: &str) -> bool {
    let b = blooms(bytes, column);
    !b.is_empty() && b.iter().all(Option::is_some)
}

/// 「某列没有 bloom」：每个行组中该列的列块都不带。
fn no_bloom(bytes: &[u8], column: &str) -> bool {
    blooms(bytes, column).iter().all(Option::is_none)
}

/// 文件全部叶子列名。
fn columns(bytes: &[u8]) -> Vec<String> {
    let meta = metadata(bytes);
    let schema = meta.file_metadata().schema_descr();
    (0..schema.num_columns())
        .map(|i| schema.column(i).name().to_owned())
        .collect()
}

/// 该文件每个行组 `column` 的 bloom 对字符串 `v` 都判肯定不在（场景前提的核对）。
fn absent_str(bytes: &[u8], column: &str, v: &str) -> bool {
    let b = blooms(bytes, column);
    !b.is_empty() && b.iter().all(|x| x.as_ref().is_some_and(|x| !x.check(v)))
}

/// 同 [`absent_str`]，整数取值。
fn absent_int(bytes: &[u8], column: &str, v: i64) -> bool {
    let b = blooms(bytes, column);
    !b.is_empty() && b.iter().all(|x| x.as_ref().is_some_and(|x| !x.check(&v)))
}

fn assert_declared_blooms(bytes: &[u8], what: &str) {
    for c in ["trace_id", "user_id"] {
        assert!(has_bloom(bytes, c), "{what} 中 {c} 应有 bloom");
    }
    for c in NO_BLOOM {
        assert!(no_bloom(bytes, c), "{what} 中 {c} 不应有 bloom");
    }
}

/// 三个小时的热层数据中 H1 与 H2 的合并输出（字节）。
fn h12_hot_files(ds: &Ds, hs: [i64; 3]) -> Vec<Vec<u8>> {
    wait_single_files(ds, &hs[..2], 2 * PER_INGEST as u64)
        .iter()
        .map(|e| ds.read(e["path"].as_str().unwrap()))
        .collect()
}

fn hits(body: &Value) -> Vec<Value> {
    body["hits"].as_array().cloned().unwrap_or_default()
}

// ---------------------------------------------------------------------------
// 写方：哪些产出带 bloom
// ---------------------------------------------------------------------------

#[test]
fn bloom_seal_output_declared_columns() {
    let f = Fixture::new();
    let s = f.start(&[DECLARED, GRACE_1S, LAKE_24H, FLUSH_1S]);
    let ts = hours_ago(2);
    ingest_recs(&s, &recs(ts, 10, &H12));
    std::thread::sleep(Duration::from_secs(2));
    ingest_recs(&s, &recs(ts + 100_000, 10, &H12));
    let ds = f.dataset("app");
    let e = &wait_single_files(&ds, &[ts], 20)[0];
    assert_eq!(e["record_count"], 20, "{e}");
    let bytes = ds.read(e["path"].as_str().unwrap());
    assert_declared_blooms(&bytes, "合并输出");
}

#[test]
fn bloom_lake_output_declared_columns() {
    let f = Fixture::new();
    let s = f.start(&[DECLARED, LAKE_1H]);
    let ts = hours_ago(3);
    ingest_recs(&s, &recs(ts, 20, &H12));
    let m = f.dataset("app").wait_handovers(&bucket_label(ts), 1, WAIT);
    let cold = f.cold("app");
    let files = m["handovers"][0]["files"].as_array().unwrap();
    assert!(!files.is_empty(), "{m}");
    for x in files {
        let bytes = cold.read(x["path"].as_str().unwrap());
        assert_declared_blooms(&bytes, "冷层数据文件");
    }
}

/// D 日（此刻所在 UTC 日的前两日）`h` 时 `m` 分。
fn d_at(h: i64, m: i64) -> i64 {
    now_us().div_euclid(DAY) * DAY - 2 * DAY + h * HOUR + m * 60_000_000
}

/// 等冷层当前版本中出现 `amberlog.operation` 为 `compaction` 的快照，返回其新增的合并产出（相对表位置）。
fn wait_cold_compaction(cold: &ColdDir) -> Vec<String> {
    let (got, ok) = poll(
        WAIT,
        || {
            let meta = cold.version().map(|_| cold.current())?;
            let snap = ColdDir::snapshots(&meta)
                .into_iter()
                .find(|s| s["summary"]["amberlog.operation"] == "compaction")?;
            Some(
                cold.added(&meta, &snap)
                    .into_iter()
                    .map(|e| e.path)
                    .collect::<Vec<_>>(),
            )
        },
        Option::is_some,
    );
    assert!(ok, "{} 秒内未见冷层合并", WAIT.as_secs());
    got.unwrap()
}

#[test]
fn bloom_cold_compaction_output_declared_columns() {
    let f = Fixture::new();
    let s = f.start(&[DECLARED, GRACE_1S, LAKE_2S, CC_10S, FLUSH_1S]);
    let mut v = recs(d_at(1, 0), 10, &H12);
    v.extend(recs(d_at(2, 0), 20, &H12));
    ingest_recs(&s, &v);
    let outputs = wait_cold_compaction(&f.cold("app"));
    assert!(!outputs.is_empty());
    let cold = f.cold("app");
    for o in &outputs {
        assert_declared_blooms(&cold.read(o), "合并产出");
    }
}

#[test]
fn bloom_cold_renamed_column_by_field_id() {
    let f = Fixture::new();
    let s = f.start(&[
        ("index_hints", Some(r#"{ "default/app" = ["Trace_Id"] }"#)),
        LAKE_1H,
    ]);
    let ts = hours_ago(3);
    let v: Vec<String> = (0..20)
        .map(|i| {
            format!(
                r#"{{"_timestamp":{},"trace_id":"a-{i}","Trace_Id":"b-{i}"}}"#,
                ts + i * 1000
            )
        })
        .collect();
    ingest_recs(&s, &v);
    let m = f.dataset("app").wait_handovers(&bucket_label(ts), 1, WAIT);
    let cold = f.cold("app");
    for x in m["handovers"][0]["files"].as_array().unwrap() {
        let bytes = cold.read(x["path"].as_str().unwrap());
        let cols = columns(&bytes);
        assert!(
            cols.contains(&"trace_id".to_owned()) && cols.contains(&"Trace_Id__2".to_owned()),
            "{cols:?}"
        );
        assert!(has_bloom(&bytes, "Trace_Id__2"));
        assert!(no_bloom(&bytes, "trace_id"));
    }
}

// ---------------------------------------------------------------------------
// 写方：只写在这些产出里、写在数据文件内、不补写
// ---------------------------------------------------------------------------

#[test]
fn bloom_flush_files_none() {
    let f = Fixture::new();
    let s = f.start(&[DECLARED, GRACE_24H, FLUSH_1S]);
    // 此刻所在小时内、离整点不太近，保证两次摄取落在同一个桶
    let ts = hour_of(now_us()) + 60_000_000;
    let ts = ts.min(now_us());
    ingest_recs(&s, &recs(ts, 3, &H12));
    std::thread::sleep(Duration::from_secs(2));
    ingest_recs(&s, &recs(ts + 1000, 3, &H12));
    let ds = f.dataset("app");
    let label = bucket_label(ts);
    let (m, ok) = poll(
        Duration::from_secs(60),
        || ds.bucket_now(&label),
        |m| {
            m.as_ref()
                .is_some_and(|m| m["files"].as_array().is_some_and(|f| f.len() == 2))
        },
    );
    assert!(ok, "{m:?}");
    for e in m.unwrap()["files"].as_array().unwrap() {
        let bytes = ds.read(e["path"].as_str().unwrap());
        for c in columns(&bytes) {
            assert!(no_bloom(&bytes, &c), "flush 写出的文件中 {c} 不应有 bloom");
        }
    }
}

/// 一个数组各元素（对象）的键集合，按元素先后。
fn key_sets(v: &Value) -> Vec<BTreeSet<String>> {
    v.as_array()
        .map(|a| {
            a.iter()
                .map(|e| e.as_object().unwrap().keys().cloned().collect())
                .collect()
        })
        .unwrap_or_default()
}

fn object_keys(v: &Value) -> BTreeSet<String> {
    v.as_object().unwrap().keys().cloned().collect()
}

#[test]
fn bloom_no_extra_objects_or_fields() {
    let (fx, fy) = (Fixture::new(), Fixture::new());
    let common = [GRACE_1S, LAKE_1H, FLUSH_1S];
    let x = fx.start(&[&common[..], &[DECLARED]].concat());
    let y = fy.start(&common);
    let ts = hours_ago(3);
    let label = bucket_label(ts);
    let (dx, dy) = (fx.dataset("app"), fy.dataset("app"));
    // 两次摄取之间各等到第一次的记录已入湖：两个实例都恰好做两次入湖，冷层表目录的对象个数才可比
    for (k, batch) in [recs(ts, 5, &H12), recs(ts + 100_000, 5, &H12)]
        .into_iter()
        .enumerate()
    {
        let started = std::time::Instant::now();
        ingest_recs(&x, &batch);
        ingest_recs(&y, &batch);
        dx.wait_handovers(&label, k + 1, WAIT);
        dy.wait_handovers(&label, k + 1, WAIT);
        if let Some(rest) = Duration::from_secs(2).checked_sub(started.elapsed()) {
            std::thread::sleep(rest);
        }
    }

    assert!(
        dx.keys_under("_system/").is_empty(),
        "{:?}",
        dx.keys_under("_system/")
    );
    assert_eq!(fx.cold("app").keys().len(), fy.cold("app").keys().len());

    let (mx, my) = (
        dx.bucket_now(&label).unwrap(),
        dy.bucket_now(&label).unwrap(),
    );
    for field in ["files", "deletions", "handovers"] {
        assert_eq!(key_sets(&mx[field]), key_sets(&my[field]), "{field}");
    }
    let hx = mx["handovers"].as_array().unwrap();
    let hy = my["handovers"].as_array().unwrap();
    assert_eq!(hx.len(), hy.len());
    for (a, b) in hx.iter().zip(hy) {
        assert_eq!(
            key_sets(&a["files"]),
            key_sets(&b["files"]),
            "handovers[].files"
        );
    }

    assert_eq!(object_keys(&dx.head()), object_keys(&dy.head()));
    let lake_summary_keys = |cold: ColdDir| -> Vec<BTreeSet<String>> {
        let meta = cold.current();
        ColdDir::snapshots(&meta)
            .iter()
            .filter(|s| s["summary"]["amberlog.operation"] == "lake")
            .map(|s| object_keys(&s["summary"]))
            .collect()
    };
    assert_eq!(
        lake_summary_keys(fx.cold("app")),
        lake_summary_keys(fy.cold("app"))
    );
}

#[test]
fn bloom_hint_change_not_backfilled() {
    let f = Fixture::new();
    let base = [GRACE_1S, LAKE_24H, FLUSH_1S];
    let trace_only: Kv = ("index_hints", Some(r#"{ "default/app" = ["trace_id"] }"#));
    let user_only: Kv = ("index_hints", Some(r#"{ "default/app" = ["user_id"] }"#));
    let ds = f.dataset("app");

    let mut s = f.start(&[&base[..], &[trace_only]].concat());
    let t3 = hours_ago(3);
    ingest_recs(&s, &recs(t3, 3, &H12));
    std::thread::sleep(Duration::from_secs(2));
    ingest_recs(&s, &recs(t3 + 100_000, 3, &H12));
    let f1 = wait_single_files(&ds, &[t3], 6).remove(0);
    stop(&mut s);

    let s = f.start(&[&base[..], &[user_only]].concat());
    let t2 = hours_ago(2);
    ingest_recs(&s, &recs(t2, 3, &H12));
    std::thread::sleep(Duration::from_secs(2));
    ingest_recs(&s, &recs(t2 + 100_000, 3, &H12));
    let f2 = wait_single_files(&ds, &[t2], 6).remove(0);

    let m3 = ds.bucket_now(&bucket_label(t3)).unwrap();
    let files = m3["files"].as_array().unwrap();
    assert_eq!(files.len(), 1, "{m3}");
    assert_eq!(files[0]["path"], f1["path"]);
    assert_eq!(files[0]["content_digest"], f1["content_digest"]);

    let b1 = ds.read(f1["path"].as_str().unwrap());
    assert!(has_bloom(&b1, "trace_id") && no_bloom(&b1, "user_id"));
    let b2 = ds.read(f2["path"].as_str().unwrap());
    assert!(has_bloom(&b2, "user_id") && no_bloom(&b2, "trace_id"));
    drop(s);
}

// ---------------------------------------------------------------------------
// 读方：按 bloom 跳过文件、逐文件判定
// ---------------------------------------------------------------------------

fn assert_t200_only(body: &Value, with_user: bool) {
    let h = hits(body);
    assert_eq!(h.len(), 1, "{body}");
    assert_eq!(h[0]["trace_id"], "t-200", "{body}");
    if with_user {
        assert_eq!(h[0]["user_id"], 200, "{body}");
    }
}

#[test]
fn bloom_hot_skip_scan_files() {
    let f = Fixture::new();
    let (s, hs) = hot_three_hours(&f, &[DECLARED]);
    for b in h12_hot_files(&f.dataset("app"), hs) {
        assert!(
            absent_str(&b, "trace_id", "t-200"),
            "前提：bloom 对 t-200 判肯定不在"
        );
        assert!(
            absent_int(&b, "user_id", 200),
            "前提：bloom 对 200 判肯定不在"
        );
        assert!(
            absent_int(&b, "user_id", 250),
            "前提：bloom 对 250 判肯定不在"
        );
    }
    for sql in [
        "SELECT trace_id, user_id FROM app WHERE trace_id = 't-200'",
        "SELECT trace_id, user_id FROM app WHERE user_id IN (200, 250)",
    ] {
        let r = search_hours(&s, sql, hs);
        assert_eq!(r["scan_files"], 1, "{sql}：{r}");
        assert_t200_only(&r, true);
    }
}

#[test]
fn bloom_cold_skip_scan_files() {
    let f = Fixture::new();
    let s = f.start(&[DECLARED, LAKE_1H, CC_24H]);
    let hs = three_hours();
    ingest_recs(&s, &recs(hs[0], PER_INGEST, &H12));
    ingest_recs(&s, &recs(hs[1], PER_INGEST, &H12));
    ingest_recs(&s, &h3(hs[2], PER_INGEST, true));
    let ds = f.dataset("app");
    let cold = f.cold("app");
    for (i, h) in hs.iter().enumerate() {
        let m = ds.wait_handovers(&bucket_label(*h), 1, WAIT);
        let files = m["handovers"][0]["files"].as_array().unwrap();
        assert_eq!(files.len(), 1, "{m}");
        if i < 2 {
            let b = cold.read(files[0]["path"].as_str().unwrap());
            assert!(
                absent_str(&b, "trace_id", "t-200"),
                "前提：bloom 对 t-200 判肯定不在"
            );
        }
    }
    let r = search_hours(
        &s,
        "SELECT trace_id, user_id FROM app WHERE trace_id = 't-200'",
        hs,
    );
    assert_eq!(r["scan_files"], 1, "{r}");
    assert_t200_only(&r, true);
}

#[test]
fn bloom_skip_by_file_without_hint() {
    let f = Fixture::new();
    let (mut s, hs) = hot_three_hours(&f, &[DECLARED]);
    for b in h12_hot_files(&f.dataset("app"), hs) {
        assert!(
            absent_str(&b, "trace_id", "t-200"),
            "前提：bloom 对 t-200 判肯定不在"
        );
    }
    stop(&mut s);
    let s = f.start(&[GRACE_1S, LAKE_24H, FLUSH_1S]);
    let r = search_hours(&s, "SELECT trace_id FROM app WHERE trace_id = 't-200'", hs);
    assert_eq!(r["scan_files"], 1, "{r}");
    assert_t200_only(&r, false);
}

// ---------------------------------------------------------------------------
// 读方：坏了的 bloom 当作没有
// ---------------------------------------------------------------------------

/// 含 `trace_id` 为 `t-100`、`t-200`、`t-300` 三条记录（`_timestamp` 自 `ts` 起逐条加 1 毫秒）的 Parquet，
/// `trace_id` 带 bloom；随后把该 bloom 的头部改成不是合法 bloom 过滤器的内容。
fn broken_bloom_file(ts: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
            false,
        ),
        Field::new("trace_id", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(
                TimestampMicrosecondArray::from(vec![ts, ts + 1000, ts + 2000])
                    .with_timezone("UTC"),
            ) as ArrayRef,
            Arc::new(StringArray::from(vec!["t-100", "t-200", "t-300"])),
        ],
    )
    .unwrap();
    let props = WriterProperties::builder()
        .set_column_bloom_filter_enabled(ColumnPath::from("trace_id"), true)
        .build();
    let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
    w.write(&batch).unwrap();
    let mut bytes = w.into_inner().unwrap();
    let meta = metadata(&bytes);
    let off = meta.row_group(0).column(1).bloom_filter_offset().unwrap() as usize;
    assert!(has_bloom(&bytes, "trace_id"));
    bytes[off..off + 4].copy_from_slice(&[0xff; 4]);
    bytes
}

/// 把当前快照中桶 `label` 的唯一文件换成 `bytes`（写成新路径，条目按改后的完整字节写 `size_bytes` 与
/// `content_digest`，去掉 `column_stats`；桶 manifest 还原后写成一份合拢全量片），以引用新桶 manifest 的新快照推进
/// `_current`。
fn replace_only_file(ds: &Ds, label: &str, bytes: &[u8], ts: i64) {
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
    let files = m["files"].as_array_mut().unwrap();
    assert_eq!(files.len(), 1, "{files:?}");
    let path = amberlog_storage::layout::data_file_path(
        amberlog_storage::layout::HourBucket::of(ts),
        &amberlog_storage::layout::new_object_id(),
    );
    ds.write(&path, bytes);
    let e = files[0].as_object_mut().unwrap();
    e.insert("path".into(), json!(path));
    e.insert("size_bytes".into(), json!(bytes.len()));
    e.insert("content_digest".into(), json!(sha256(bytes)));
    e.insert("record_count".into(), json!(3));
    e.insert("min_timestamp".into(), json!(ts));
    e.insert("max_timestamp".into(), json!(ts + 2000));
    e.remove("column_stats");
    let mbytes = m.to_string().into_bytes();
    let mpath = format!(
        "manifest/buckets/{label}-{}.json",
        amberlog_storage::layout::new_object_id()
    );
    ds.write(&mpath, &mbytes);
    snap["buckets"][i]["manifest"] = json!(mpath);
    snap["buckets"][i]["content_digest"] = json!(sha256(&mbytes));
    snap["snapshot_id"] = json!(n + 1);
    snap["parent_snapshot_id"] = json!(n);
    ds.write_json(&snapshot_rel(n + 1), &snap);
    ds.write_json("manifest/_current", &json!({"snapshot_id": n + 1}));
}

#[test]
fn bloom_broken_ignored() {
    let f = Fixture::new();
    let config = [DECLARED, LAKE_24H, FLUSH_1S];
    let ts = hours_ago(2);
    let label = bucket_label(ts);
    let ds = f.dataset("app");
    let mut s = f.start(&config);
    ingest_recs(
        &s,
        &recs(ts, 3, &[("t-100", 100), ("t-200", 200), ("t-300", 300)]),
    );
    ds.wait_records(3, Duration::from_secs(15));
    stop(&mut s);
    replace_only_file(&ds, &label, &broken_bloom_file(ts), ts);

    let s = f.start(&config);
    let r = search_range(
        &s,
        "SELECT trace_id FROM app WHERE trace_id = 't-200'",
        hour_of(ts),
        hour_of(ts) + HOUR,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_t200_only(&r.json(), false);
    let metrics = s.get("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled") && l.contains(r#"stream="app""#)),
        "{metrics}"
    );
}

// ---------------------------------------------------------------------------
// 读方：结果不变
// ---------------------------------------------------------------------------

#[test]
fn bloom_result_unchanged() {
    let (fx, fy) = (Fixture::new(), Fixture::new());
    let (x, hs) = hot_three_hours(&fx, &[DECLARED]);
    let y = fy.start(&[GRACE_1S, LAKE_24H, FLUSH_1S]);
    write_hot_hours(&y, hs);
    wait_single_files(&fy.dataset("app"), &hs, 2 * PER_INGEST as u64);
    for sql in [
        "SELECT trace_id, user_id FROM app WHERE trace_id = 't-200' ORDER BY user_id",
        "SELECT trace_id, user_id FROM app WHERE user_id IN (100, 200) ORDER BY trace_id, user_id",
        "SELECT trace_id FROM app WHERE trace_id = 't-999'",
        "SELECT trace_id, user_id FROM app WHERE trace_id = 't-200' OR user_id = 300 ORDER BY trace_id, user_id",
    ] {
        let (rx, ry) = (search_hours(&x, sql, hs), search_hours(&y, sql, hs));
        assert_eq!(rx["hits"], ry["hits"], "{sql}");
        assert_eq!(rx["total"], ry["total"], "{sql}");
        assert!(
            rx["scan_files"].as_u64() <= ry["scan_files"].as_u64(),
            "{sql}：X {} > Y {}",
            rx["scan_files"],
            ry["scan_files"]
        );
    }
}
