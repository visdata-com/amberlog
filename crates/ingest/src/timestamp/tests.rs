//! `_timestamp` 归一的单元测试。
//!
//! `timestamp_unit_boundaries` 与 `timestamp_rfc3339_forms` 验证 REQ-AMB-TIME-TIMESTAMP 的两个场景，
//! 表格逐行照抄场景；其余以 `normalize_` 为前缀，覆盖块的其他条款。

use super::*;

const HOUR_US: i64 = 3_600_000_000;

/// 以 JSON 原文构造取值，经 serde_json 解析（与摄取路径一致：数字保留原文）。
fn json(text: &str) -> Value {
    serde_json::from_str(text).unwrap_or_else(|e| panic!("测试输入 {text} 不是合法 JSON：{e}"))
}

fn run(value: Option<&Value>, received_at_us: i64, window: &TimestampWindow) -> String {
    match normalize_timestamp(value, received_at_us, window) {
        Ok(us) => us.to_string(),
        Err(reason) => reason.as_str().to_owned(),
    }
}

/// 场景「四种单位与三个幅值分界点」。
#[test]
fn timestamp_unit_boundaries() {
    // 接收时刻 T = 2026-08-22T02:00:00Z，`ingest.max_future` 为 24 小时。
    let received = 1_787_364_000_000_000;
    let window = TimestampWindow {
        max_future: Duration::from_secs(24 * 3600),
        max_past: None,
    };
    let table: &[(&str, &str)] = &[
        ("1787364000", "1787364000000000"),
        ("1787364000123", "1787364000123000"),
        ("1787364000123456", "1787364000123456"),
        ("1787364000123456789", "1787364000123456"),
        ("100000000000", "100000000000000"),
        ("99999999999", "timestamp_out_of_range"),
        ("100000000000000", "100000000000000"),
        ("99999999999999", "timestamp_out_of_range"),
        ("100000000000000000", "100000000000000"),
        ("99999999999999999", "timestamp_out_of_range"),
        ("1787364000.0000009", "1787364000000000"),
        ("-1", "timestamp_out_of_range"),
    ];
    for (literal, want) in table {
        let v = json(literal);
        assert_eq!(run(Some(&v), received, &window), *want, "字面量 {literal}");
    }
}

/// 场景「RFC 3339 的写法边界」。
#[test]
fn timestamp_rfc3339_forms() {
    // 接收时刻 T = 2026-08-22T03:00:00Z，时间窗取默认值。
    let received = 1_787_367_600_000_000;
    let window = TimestampWindow::default();
    let table: &[(&str, &str)] = &[
        ("2026-08-22T02:00:00Z", "1787364000000000"),
        ("2026-08-22t02:00:00z", "1787364000000000"),
        ("2026-08-22T10:00:00+08:00", "1787364000000000"),
        ("2026-08-21T21:00:00-05:00", "1787364000000000"),
        ("2026-08-22T02:00:00.1234569Z", "1787364000123456"),
        ("2026-08-22T02:00:00", "timestamp_invalid"),
        ("2026-08-22 02:00:00Z", "timestamp_invalid"),
        ("2026-08-22", "timestamp_invalid"),
        ("1787364000", "timestamp_invalid"),
        ("1969-12-31T23:59:59Z", "timestamp_out_of_range"),
    ];
    for (text, want) in table {
        let v = Value::String((*text).to_owned());
        assert_eq!(run(Some(&v), received, &window), *want, "字符串 {text}");
    }
}

/// 缺失、`null` 与精确值为零的数字（含 `-0`、`0e5`、`0.000`）以接收时刻填充。
#[test]
fn normalize_default_fill() {
    let received = 1_787_364_000_000_000;
    let window = TimestampWindow::default();
    assert_eq!(normalize_timestamp(None, received, &window), Ok(received));
    for literal in [
        "null", "0", "-0", "0.0", "-0.0", "0e5", "0E-3", "0.000", "0.0e+10",
    ] {
        let v = json(literal);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Ok(received),
            "{literal}"
        );
    }
}

/// 数字按原文的精确十进制值判档与向零截断，不经 f64（块场景「高精度数值时间戳按字面量精确值截断」
/// 的换算部分，以及秒级小数、指数形式）。
#[test]
fn normalize_exact_decimal() {
    let received = 1_787_364_000_000_000;
    let window = TimestampWindow::default();
    let table: &[(&str, i64)] = &[
        ("1700002799999999999", 1_700_002_799_999_999),
        ("1700002799.9999999", 1_700_002_799_999_999),
        ("1700000000.5", 1_700_000_000_500_000),
        ("1700000000123.9", 1_700_000_000_123_900),
        ("1700000000123456.9", 1_700_000_000_123_456),
        ("1700000000123456789.9", 1_700_000_000_123_456),
        ("1.7e9", 1_700_000_000_000_000),
        ("1.7E12", 1_700_000_000_000_000),
        ("17000000000e-1", 1_700_000_000_000_000),
        ("1.700000000123456789E18", 1_700_000_000_123_456),
        ("0.5", 500_000),
        ("0.0000001", 0),
    ];
    for (literal, want) in table {
        let v = json(literal);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Ok(*want),
            "{literal}"
        );
    }
}

/// 布尔、对象（含空对象）、数组（含空数组）与无法解析的字符串报 `timestamp_invalid`。
#[test]
fn normalize_invalid_kinds() {
    let received = 1_787_364_000_000_000;
    let window = TimestampWindow::default();
    for text in [
        "true",
        "false",
        "{}",
        r#"{"a":1}"#,
        "[]",
        "[1]",
        r#""yesterday""#,
        r#""""#,
        r#""1787364000""#,
        r#""-1""#,
    ] {
        let v = json(text);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Err(RecordReason::TimestampInvalid),
            "{text}"
        );
    }
}

/// RFC 3339 文法与日历校验：只收 `date-time`，日期须真实存在，偏移须为 `Z` 或 `±HH:MM`。
#[test]
fn normalize_rfc3339_grammar() {
    let received = 1_787_367_600_000_000;
    let window = TimestampWindow::default();
    let ok: &[(&str, i64)] = &[
        ("2024-02-29T00:00:00Z", 1_709_164_800_000_000),
        ("2026-08-22T02:00:00-00:00", 1_787_364_000_000_000),
        ("2026-08-22T02:00:00.5Z", 1_787_364_000_500_000),
        (
            "2026-08-22T02:00:00.000000000000001Z",
            1_787_364_000_000_000,
        ),
        ("2026-08-22T01:59:59.999999999Z", 1_787_363_999_999_999),
        ("2026-08-21T23:30:00-02:30", 1_787_364_000_000_000),
        ("1970-01-01T00:00:00Z", 0),
    ];
    for (text, want) in ok {
        let v = Value::String((*text).to_owned());
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Ok(*want),
            "{text}"
        );
    }
    let invalid = [
        "2026-02-29T00:00:00Z",
        "2026-04-31T00:00:00Z",
        "2026-13-01T00:00:00Z",
        "2026-00-10T00:00:00Z",
        "2026-08-00T00:00:00Z",
        "2026-08-22T24:00:00Z",
        "2026-08-22T02:60:00Z",
        "2026-08-22T02:00:61Z",
        "2026-08-22T02:00:00.Z",
        "2026-08-22T02:00Z",
        "2026-8-22T02:00:00Z",
        " 2026-08-22T02:00:00Z",
        "2026-08-22T02:00:00Z ",
        "2026-08-22T02:00:00ZZ",
        "2026-08-22T02:00:00+08",
        "2026-08-22T02:00:00+0800",
        "2026-08-22T02:00:00+24:00",
        "2026-08-22T02:00:00+08:60",
        "2026-08-22T02:00:00+08:00:00",
        "2026-08-22T02:00:00UTC",
        "2026-08-22T02:00:00.123",
        "+2026-08-22T02:00:00Z",
        "２026-08-22T02:00:00Z",
    ];
    for text in invalid {
        let v = Value::String(text.to_owned());
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Err(RecordReason::TimestampInvalid),
            "{text}"
        );
    }
}

/// 秒为 `60` 折算为下一分钟第 0 秒并保留小数，不校验是否确有闰秒。
#[test]
fn normalize_rfc3339_leap_second() {
    let received = 1_787_367_600_000_000;
    let window = TimestampWindow::default();
    let table: &[(&str, i64)] = &[
        ("2016-12-31T23:59:60Z", 1_483_228_800_000_000),
        ("2016-12-31T15:59:60.5-08:00", 1_483_228_800_500_000),
        ("2026-08-22T01:59:60Z", 1_787_364_000_000_000),
        ("2026-08-22T01:15:60.25Z", 1_787_361_360_250_000),
    ];
    for (text, want) in table {
        let v = Value::String((*text).to_owned());
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Ok(*want),
            "{text}"
        );
    }
}

/// 负值、未来窗口两侧、过去窗口两侧（配置与不配置）、超出 Int64 的数字。
#[test]
fn normalize_window_edges() {
    let received = 1_787_364_000_000_000;
    let default = TimestampWindow::default();
    let out = Err(RecordReason::TimestampOutOfRange);

    // 未来：恰为接收时刻 + 24 小时仍接受，再晚 1 微秒拒绝。
    let edge = received + 24 * HOUR_US;
    let v = json(&edge.to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &default), Ok(edge));
    let v = json(&(edge + 1).to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &default), out);
    let v = json(&(received + 23 * HOUR_US).to_string());
    assert!(normalize_timestamp(Some(&v), received, &default).is_ok());
    let v = json(&(received + 25 * HOUR_US).to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &default), out);

    // 过去：默认不限制；配置 1 小时后恰为接收时刻 − 1 小时仍接受，再早 1 微秒拒绝。
    let v = Value::String("1971-01-01T00:00:00Z".to_owned());
    assert_eq!(
        normalize_timestamp(Some(&v), received, &default),
        Ok(31_536_000_000_000)
    );
    let past = TimestampWindow {
        max_past: Some(Duration::from_secs(3600)),
        ..TimestampWindow::default()
    };
    let edge = received - HOUR_US;
    let v = json(&edge.to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &past), Ok(edge));
    let v = json(&(edge - 1).to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &past), out);
    let v = json(&(received - 2 * HOUR_US).to_string());
    assert_eq!(normalize_timestamp(Some(&v), received, &past), out);
    let v = json(&(received - HOUR_US / 2).to_string());
    assert!(normalize_timestamp(Some(&v), received, &past).is_ok());

    // 负值与早于 epoch 的字符串。
    for text in ["-1", "-1.5", "-1700000000000", "-1e30"] {
        let v = json(text);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &default),
            out,
            "{text}"
        );
    }
    let v = Value::String("1969-12-31T23:59:59.999999Z".to_owned());
    assert_eq!(normalize_timestamp(Some(&v), received, &default), out);

    // 超出 Int64 或远在未来的数字（含 serde_json 在 arbitrary_precision 下放行的 1e400）。
    for text in [
        "9223372036854775807",
        "99999999999999999999999999",
        "1e30",
        "1e400",
        "1.5e99999999999999999999",
    ] {
        let v = json(text);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &default),
            out,
            "{text}"
        );
    }
}

/// 9999 年末上界与 `ingest.max_future` 各自独立判定：未来窗口放得再宽，归一后晚于上界的也拒绝；
/// 上界对归一后的微秒值比较，数字与带偏移的 RFC 3339 字符串同一规则。
#[test]
fn normalize_year_9999_upper_bound() {
    let received = 1_787_364_000_000_000;
    let wide = TimestampWindow {
        max_future: Duration::from_secs(3_000_000 * 86_400),
        ..TimestampWindow::default()
    };
    // 数字形态只有纳秒一档到得了 9999 年（≥ 1e17 判为纳秒）
    let cases = [
        ("253402300799999999000", "253402300799999999"),
        ("253402300799999999999", "253402300799999999"),
        ("253402300800000000000", "timestamp_out_of_range"),
    ];
    for (text, want) in cases {
        assert_eq!(run(Some(&json(text)), received, &wide), want, "{text}");
    }
    let strings = [
        ("9999-12-31T23:59:59.999999Z", "253402300799999999"),
        ("9999-12-31T23:59:59.9999999Z", "253402300799999999"),
        ("9999-12-31T23:59:60Z", "timestamp_out_of_range"),
        ("9999-12-31T23:59:59-01:00", "timestamp_out_of_range"),
        ("9999-12-31T23:59:59+01:00", "253402297199000000"),
    ];
    for (text, want) in strings {
        let v = Value::String(text.to_owned());
        assert_eq!(run(Some(&v), received, &wide), want, "{text}");
    }
}

/// 同一请求共用一个接收时刻：缺省填充的记录彼此相等，且窗口按该时刻判定。
#[test]
fn normalize_uses_given_received_instant() {
    let window = TimestampWindow::default();
    for received in [0, 1_483_228_800_000_000, 1_787_364_000_000_000] {
        let a = normalize_timestamp(None, received, &window);
        let b = normalize_timestamp(Some(&Value::Null), received, &window);
        let c = normalize_timestamp(Some(&json("0.0")), received, &window);
        assert_eq!(a, Ok(received));
        assert_eq!(a, b);
        assert_eq!(b, c);
    }
}

/// 早于 epoch 不足一微秒的值：「不足一微秒向零截断」是归一的一步，归一后为 0 而不是负数，
/// 故不触发「归一后为负数则拒绝」一条，按 1970-01-01T00:00:00Z 接受。
#[test]
fn normalize_sub_microsecond_before_epoch() {
    let received = 1_787_364_000_000_000;
    let window = TimestampWindow::default();
    for text in [
        "-0.0000001",
        r#""1969-12-31T23:59:59.9999999Z""#,
        r#""1970-01-01T07:59:59.9999999+08:00""#,
    ] {
        let v = json(text);
        assert_eq!(
            normalize_timestamp(Some(&v), received, &window),
            Ok(0),
            "{text}"
        );
    }
    let v = json(r#""1969-12-31T23:59:59.999999Z""#);
    assert_eq!(
        normalize_timestamp(Some(&v), received, &window),
        Err(RecordReason::TimestampOutOfRange)
    );
}
