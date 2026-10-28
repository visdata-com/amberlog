//! REQ-AMB-QRY-HISTOGRAM-V2 的系统测试：以子进程启动已构建的 `amberlog`，经 `_json` 摄取、经 `_search` 查询。
//! 对象存储用 `file://` 临时目录。
//!
//! 场景「间隔的写法与换算」「histogram 调用的形态与位置」是 amberlog-query 的单元测试
//! （`histogram_interval_parse_table`、`histogram_call_form_table`）。按查询时区对齐的各场景用 2026 年的时刻，
//! 晚于当前的记录要放宽 `ingest.max_future` 才收得下。

mod common;
mod qry_common;

use common::{Fixture, Server};
use qry_common::{Chain, early, hits, query};
use serde_json::{Value, json};

fn start() -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(&[]);
    (f, s)
}

/// 收得下 2026 年各时刻的实例（`ingest.max_future` 放宽到约 10 年）。
fn start_2026() -> (Fixture, Server) {
    let f = Fixture::new();
    let s = f.start(&[("ingest.max_future", Some("\"3650d\""))]);
    (f, s)
}

/// 覆盖 2026 年 3 月至 11 月各记录的时间范围。
const Y2026: (i64, i64) = (1_772_000_000_000_000, 1_794_000_000_000_000);

/// 以查询时区 `tz` 与时间范围 `range` 提交 `sql`，断言 200 并返回 `hits`。
fn hits_in(s: &Server, sql: &str, range: (i64, i64), tz: &str) -> Vec<Value> {
    let r = s.search(&query(sql, range, &json!({"timezone": tz})));
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    hits(&r.json())
}

#[test]
fn histogram_five_minute_buckets() {
    let (_f, s) = start();
    // 0 与 299999999 微秒在 1970-01-01 第一分钟之外，以 RFC 3339 写入
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": early(0)},
            {"_timestamp": "1970-01-01T00:04:59.999999Z"},
            {"_timestamp": "1970-01-01T00:05:00Z"},
            {"_timestamp": "1970-01-01T00:15:00.000001Z"},
        ]),
    );
    let r = s.search(&query(
        "SELECT histogram(_timestamp, '5m') AS t, count(*) AS n FROM app GROUP BY t ORDER BY t",
        (0, 3_600_000_000),
        &json!({}),
    ));
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(
        hits(&r.json()),
        [
            json!({"t": 0, "n": 2}),
            json!({"t": 300_000_000, "n": 1}),
            json!({"t": 900_000_000, "n": 1}),
        ]
    );
}

#[test]
fn histogram_day_aligned_utc() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": 1_759_449_600_000_000i64},
            {"_timestamp": 1_759_535_999_999_999i64},
        ]),
    );
    let v = s.sql_ok(
        "SELECT histogram(_timestamp, '24h') AS d, count(*) AS n FROM app GROUP BY histogram(_timestamp, '1d')",
    );
    assert_eq!(hits(&v), [json!({"d": 1_759_449_600_000_000i64, "n": 2})]);
}

#[test]
fn histogram_day_aligned_local_midnight() {
    let (_f, s) = start_2026();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": 1_792_166_399_999_999i64},
            {"_timestamp": 1_792_166_400_000_000i64},
        ]),
    );
    let h = hits_in(
        &s,
        "SELECT histogram(_timestamp, '1d') AS d, count(*) AS n FROM app GROUP BY d ORDER BY d",
        (1_792_080_000_000_000, 1_792_252_800_000_000),
        "Asia/Shanghai",
    );
    assert_eq!(
        h,
        [
            json!({"d": 1_792_080_000_000_000i64, "n": 1}),
            json!({"d": 1_792_166_400_000_000i64, "n": 1}),
        ]
    );
}

#[test]
fn histogram_dst_fall_back_25h_day() {
    let (_f, s) = start_2026();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": 1_793_505_600_000_000i64},
            {"_timestamp": 1_793_595_599_999_999i64},
            {"_timestamp": 1_793_595_600_000_000i64},
        ]),
    );
    let h = hits_in(
        &s,
        "SELECT histogram(_timestamp, '1d') AS d, count(*) AS n FROM app GROUP BY d ORDER BY d",
        Y2026,
        "America/New_York",
    );
    assert_eq!(
        h,
        [
            json!({"d": 1_793_505_600_000_000i64, "n": 2}),
            json!({"d": 1_793_595_600_000_000i64, "n": 1}),
        ]
    );
}

#[test]
fn histogram_repeated_hour_two_buckets() {
    let (_f, s) = start_2026();
    s.ingest_ok(
        "app",
        &json!([
            {"_timestamp": 1_793_511_000_000_000i64},
            {"_timestamp": 1_793_514_600_000_000i64},
        ]),
    );
    let h = hits_in(
        &s,
        "SELECT histogram(_timestamp, '1h') AS h, count(*) AS n FROM app GROUP BY h ORDER BY h",
        Y2026,
        "America/New_York",
    );
    assert_eq!(
        h,
        [
            json!({"h": 1_793_509_200_000_000i64, "n": 1}),
            json!({"h": 1_793_512_800_000_000i64, "n": 1}),
        ]
    );
}

#[test]
fn histogram_repeated_hour_minute_buckets() {
    let (_f, s) = start_2026();
    s.ingest_ok("app", &json!([{"_timestamp": 1_793_514_600_000_000i64}]));
    let h = hits_in(
        &s,
        "SELECT histogram(_timestamp, '1m') AS h, count(*) AS n FROM app GROUP BY h",
        Y2026,
        "America/New_York",
    );
    assert_eq!(h, [json!({"h": 1_793_514_600_000_000i64, "n": 1})]);
}

#[test]
fn histogram_skipped_local_start() {
    let (_f, s) = start_2026();
    s.ingest_ok("app", &json!([{"_timestamp": 1_772_955_000_000_000i64}]));
    let h = hits_in(
        &s,
        "SELECT histogram(_timestamp, '2h') AS h, count(*) AS n FROM app GROUP BY h",
        Y2026,
        "America/New_York",
    );
    assert_eq!(h, [json!({"h": 1_772_953_200_000_000i64, "n": 1})]);
}

#[test]
fn histogram_row_values_without_grouping() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"_timestamp": early(1_500_000)}, {"_timestamp": early(2_999_999)}]),
    );
    let v = s
        .sql_ok("SELECT _timestamp, histogram(_timestamp, '1s') AS t FROM app ORDER BY _timestamp");
    assert_eq!(
        hits(&v),
        [
            json!({"_timestamp": 1_500_000, "t": 1_000_000}),
            json!({"_timestamp": 2_999_999, "t": 2_000_000}),
        ]
    );
}

#[test]
fn histogram_equal_interval_same_call() {
    let (_f, s) = start();
    s.ingest_ok(
        "app",
        &json!([{"_timestamp": early(0)}, {"_timestamp": early(59_999_999)}]),
    );
    let v = s.sql_ok(
        "SELECT histogram(_timestamp, '60s'), count(*) AS n FROM app GROUP BY histogram(_timestamp, '1m')",
    );
    assert_eq!(hits(&v), [json!({"histogram": 0, "n": 2})]);
    s.sql_ok("SELECT histogram(_timestamp, '60s'), histogram(_timestamp, '1m') FROM app");
    s.sql_err(
        "SELECT histogram(_timestamp, '60s'), histogram(_timestamp, '2m') FROM app",
        400,
        "invalid_sql",
    );
}

#[test]
fn histogram_system_table_unknown_column() {
    let f = Fixture::new();
    let s = f.start(&[("flush.max_records", Some("1"))]);
    s.ingest_ok("app", &json!([{"k": 1}]));
    Chain::file(&f).wait_record_count("app", 1);
    s.sql_err(
        "SELECT histogram(_timestamp, '1h') AS t, count(*) AS n FROM _ledger GROUP BY t",
        400,
        "unknown_column",
    );
}

#[test]
fn histogram_invalid_interval_and_form() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    for sql in [
        "SELECT histogram(_timestamp, '5 minutes') FROM app",
        "SELECT histogram(_timestamp, '0s') FROM app",
        "SELECT histogram(_timestamp, '1.5h') FROM app",
    ] {
        s.sql_err(sql, 400, "invalid_sql");
    }
    for sql in [
        "SELECT histogram(a, '5m') FROM app",
        "SELECT histogram(_timestamp) FROM app",
        "SELECT a FROM app WHERE histogram(_timestamp, '5m') = 0",
    ] {
        s.sql_err(sql, 400, "sql_unsupported");
    }
}

#[test]
fn histogram_interval_error_report_order() {
    let (_f, s) = start();
    s.ingest_ok("app", &json!([{"a": 1}]));
    s.sql_err(
        "SELECT histogram(_timestamp, '0s') FROM nosuch",
        400,
        "stream_not_found",
    );
    s.sql_err(
        "SELECT histogram(_timestamp, '0s'), b FROM app",
        400,
        "unknown_column",
    );
}
