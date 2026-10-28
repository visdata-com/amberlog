//! 记录模型的单元测试。
//!
//! `record_model_canonical_json_rules` 与 `record_model_field_name_rules` 验证 REQ-AMB-DM-RECORD-MODEL 的两个场景，
//! 表格逐行照抄场景；其余以 `record_` 为前缀、不以 `record_model_` 开头，覆盖块的其他条款。

use super::*;
use amberlog_common::json::{parse_body_literal, parse_ingest_body};
use amberlog_common::schema::StreamSchema;

/// 接收时刻 T = 2026-08-22T02:00:00Z（UTC 微秒）。
const RECEIVED: i64 = 1_787_364_000_000_000;

fn json(text: &str) -> Json {
    parse_body_literal(text.as_bytes())
        .unwrap_or_else(|e| panic!("测试输入 {text} 不是合法 JSON：{e}"))
}

fn run_with(text: &str, schema: &StreamSchema) -> Result<Record, RecordReason> {
    process_record(
        &json(text),
        RECEIVED,
        &TimestampWindow::default(),
        schema,
        1000,
    )
}

fn run(text: &str) -> Result<Record, RecordReason> {
    run_with(text, &StreamSchema::new())
}

/// 判定结果写成表格里的形式：接受记为「接受」，拒绝记为 `reason`。
fn verdict(result: &Result<Record, RecordReason>) -> String {
    match result {
        Ok(_) => "接受".to_owned(),
        Err(reason) => reason.as_str().to_owned(),
    }
}

fn field<'a>(record: &'a Record, name: &str) -> Option<&'a FieldValue> {
    record
        .fields
        .iter()
        .find(|(n, _)| n == name)
        .map(|(_, v)| v)
}

fn names(record: &Record) -> Vec<&str> {
    record.fields.iter().map(|(n, _)| n.as_str()).collect()
}

fn schema_of(columns: &[(&str, ColumnType)]) -> StreamSchema {
    let mut s = StreamSchema::new();
    for (name, t) in columns {
        s.insert((*name).to_owned(), *t);
    }
    s
}

fn batch(text: &str, schema: &StreamSchema) -> BatchOutcome {
    let elements = parse_ingest_body(text.as_bytes())
        .unwrap_or_else(|e| panic!("测试输入 {text} 不是合法 JSON：{e}"));
    process_batch(
        &elements,
        RECEIVED,
        &TimestampWindow::default(),
        schema,
        1000,
    )
}

// ---------------------------------------------------------------------------
// 块场景的单元测试
// ---------------------------------------------------------------------------

/// 场景「规范形序列化的键序、重复键、数字与转义」。
#[test]
fn record_model_canonical_json_rules() {
    let table: &[(&str, &str)] = &[
        (r#"{"t":[{"a":1,"b":2,"a":3}]}"#, r#"[{"a":3,"b":2}]"#),
        (
            r#"{"t":[1.0, 1e2, -0, 0.10, 1E+2]}"#,
            r#"[1.0,1e2,-0,0.10,1E+2]"#,
        ),
        (r#"{"t":["q\"b\\s\/"]}"#, r#"["q\"b\\s/"]"#),
        (
            r#"{"t":["\u0001\u0008\u000C\n\r\t\u001F"]}"#,
            r#"["\u0001\b\f\n\r\t\u001f"]"#,
        ),
        // 以 UTF-8 原样输出 é、U+2028 与 中 的字符串
        ("{\"t\":[\"é\u{2028}中\"]}", "[\"é\u{2028}中\"]"),
    ];
    for (input, want) in table {
        let record = run(input).unwrap_or_else(|r| panic!("{input} 被拒绝：{r:?}"));
        assert_eq!(
            field(&record, "t"),
            Some(&FieldValue::Utf8((*want).to_owned())),
            "输入 {input}"
        );
    }
}

/// 场景「字段命名约束」。
#[test]
fn record_model_field_name_rules() {
    let key128 = format!("{{\"{}\":1}}", "a".repeat(128));
    let key129 = format!("{{\"{}\":1}}", "a".repeat(129));
    let table: &[(&str, &str)] = &[
        (r#"{"a-b.c_d":1}"#, "接受"),
        (r#"{"A1":1}"#, "接受"),
        (r#"{"a":{"b_c":1}}"#, "接受"),
        (&key128, "接受"),
        (&key129, "invalid_field_name"),
        (r#"{"-a":1}"#, "invalid_field_name"),
        (r#"{"a-":1}"#, "invalid_field_name"),
        (r#"{"a__b":1}"#, "invalid_field_name"),
        (r#"{"a..b":1}"#, "invalid_field_name"),
        (r#"{"a":{"_b":1}}"#, "invalid_field_name"),
        (r#"{"a_":{"b":1}}"#, "invalid_field_name"),
        (r#"{"a b":1}"#, "invalid_field_name"),
        (r#"{"é":1}"#, "invalid_field_name"),
        (r#"{"a.b":1,"a":{"b":2}}"#, "invalid_field_name"),
        (r#"{"-a":null}"#, "invalid_field_name"),
        (r#"{"_x":null}"#, "reserved_field"),
        (r#"{"_x":{}}"#, "reserved_field"),
    ];
    for (input, want) in table {
        assert_eq!(verdict(&run(input)), *want, "记录 {input}");
    }
}

// ---------------------------------------------------------------------------
// 块的其他条款
// ---------------------------------------------------------------------------

#[test]
fn record_flatten_nested_objects() {
    let r =
        run(r#"{"host":{"name":"web1","os":{"version":"1.2"}},"port":8080,"ok":true}"#).unwrap();
    assert_eq!(names(&r), ["host.name", "host.os.version", "port", "ok"]);
    assert_eq!(
        field(&r, "host.name"),
        Some(&FieldValue::Utf8("web1".into()))
    );
    assert_eq!(
        field(&r, "host.os.version"),
        Some(&FieldValue::Utf8("1.2".into()))
    );
    assert_eq!(field(&r, "port"), Some(&FieldValue::Int64(8080)));
    assert_eq!(field(&r, "ok"), Some(&FieldValue::Boolean(true)));
    // 嵌套键以 `_` 开头不是保留字段，按命名约束处理
    assert_eq!(
        verdict(&run(r#"{"a":{"_timestamp":1}}"#)),
        "invalid_field_name"
    );
}

#[test]
fn record_scalar_type_mapping() {
    let r = run(
        r#"{"i":1,"f":1.5,"e":1e2,"big":12345678901234567890,"nbig":-9223372036854775809,
            "max":9223372036854775807,"min":-9223372036854775808,"z":-0,"fz":-0.0,"s":"x","b":false,"es":""}"#,
    )
    .unwrap();
    let want: &[(&str, FieldValue)] = &[
        ("i", FieldValue::Int64(1)),
        ("f", FieldValue::Float64(1.5)),
        ("e", FieldValue::Float64(100.0)),
        ("big", FieldValue::Utf8("12345678901234567890".into())),
        ("nbig", FieldValue::Utf8("-9223372036854775809".into())),
        ("max", FieldValue::Int64(i64::MAX)),
        ("min", FieldValue::Int64(i64::MIN)),
        ("z", FieldValue::Int64(0)),
        ("fz", FieldValue::Float64(-0.0)),
        ("s", FieldValue::Utf8("x".into())),
        ("b", FieldValue::Boolean(false)),
        ("es", FieldValue::Utf8(String::new())),
    ];
    for (name, value) in want {
        assert_eq!(field(&r, name), Some(value), "字段 {name}");
    }
    // 超出 int64 的整数不舍入：大整数照原样成为十进制字符串
    let r = run(r#"{"n":123456789012345678901234567890}"#).unwrap();
    assert_eq!(
        field(&r, "n"),
        Some(&FieldValue::Utf8("123456789012345678901234567890".into()))
    );
}

#[test]
fn record_null_and_empty_object_create_no_column() {
    let r = run(r#"{"o":{},"p":{"q":{}},"n":null,"k":1,"e":""}"#).unwrap();
    assert_eq!(names(&r), ["k", "e"]);
    assert_eq!(field(&r, "e"), Some(&FieldValue::Utf8(String::new())));
    let out = batch(
        r#"[{"o":{},"p":{"q":{}},"n":null,"k":1}]"#,
        &StreamSchema::new(),
    );
    assert_eq!(out.new_columns, [("k".to_owned(), ColumnType::Int64)]);
    // 已在 schema 中的列取值为 null：记录照常接受，该列为 NULL
    let s = schema_of(&[("k", ColumnType::Utf8)]);
    let r = run_with(r#"{"k":null,"v":"x"}"#, &s).unwrap();
    assert_eq!(names(&r), ["v"]);
}

#[test]
fn record_duplicate_keys_last_wins() {
    let r = run(r#"{"k":1,"k":"x"}"#).unwrap();
    assert_eq!(r.fields, [("k".to_owned(), FieldValue::Utf8("x".into()))]);
    let r = run(r#"{"a":{"x":1},"b":2,"a":{"y":3}}"#).unwrap();
    assert_eq!(names(&r), ["a.y", "b"]);
}

#[test]
fn record_array_serialized_canonically() {
    let r = run(r#"{"tags":[ 1.0 , {"b":2,"a":"é"} , 12345678901234567890123 ]}"#).unwrap();
    assert_eq!(
        field(&r, "tags"),
        Some(&FieldValue::Utf8(
            r#"[1.0,{"b":2,"a":"é"},12345678901234567890123]"#.into()
        ))
    );
    // 数组内的空对象、null、嵌套数组与超大指数字面量都照原样保留
    let r = run(r#"{"t":[{},[],null,true,[1E-7,{"":{}}]], "u":[]}"#).unwrap();
    assert_eq!(
        field(&r, "t"),
        Some(&FieldValue::Utf8(
            r#"[{},[],null,true,[1E-7,{"":{}}]]"#.into()
        ))
    );
    assert_eq!(field(&r, "u"), Some(&FieldValue::Utf8("[]".into())));
    // 其余控制字符写作小写十六进制，U+007F 与 `/` 原样输出
    let r = run(r#"{"t":["\u001b\u007f/\u00e9"]}"#).unwrap();
    assert_eq!(
        field(&r, "t"),
        Some(&FieldValue::Utf8("[\"\\u001b\u{7f}/\u{e9}\"]".into()))
    );
}

/// 嵌套 `levels` 层对象、最内层为 `{"leaf":"deep"}` 的记录（`levels` 个 `k` 键包裹）。
fn nested(levels: usize) -> String {
    let mut s = r#"{"leaf":"deep"}"#.to_owned();
    for _ in 0..levels {
        s = format!(r#"{{"k":{s}}}"#);
    }
    s
}

#[test]
fn record_deep_subtree_serialized_whole() {
    // `leaf` 位于第 8 层：完全展平（REQ-AMB-DM-FIELD-LIMITS）
    let r = run(&nested(7)).unwrap();
    let name = format!("{}leaf", "k.".repeat(7));
    assert_eq!(r.fields, [(name, FieldValue::Utf8("deep".into()))]);
    // `leaf` 位于第 9 层：第 8 层键的取值整体序列化
    let r = run(&nested(8)).unwrap();
    let name = ["k"; 8].join(".");
    assert_eq!(
        r.fields,
        [(name, FieldValue::Utf8(r#"{"leaf":"deep"}"#.into()))]
    );
    // 40 层：不丢弃，含 `"leaf":"deep"` 的字符串落在第 32 层的字段名上
    let r = run(&nested(40)).unwrap();
    assert_eq!(r.fields.len(), 1);
    let (name, value) = &r.fields[0];
    assert_eq!(name.split('.').count(), MAX_FLATTEN_DEPTH);
    let FieldValue::Utf8(text) = value else {
        panic!("应为字符串")
    };
    assert!(text.contains(r#""leaf":"deep""#));
    assert!(text.starts_with(r#"{"k":{"k":"#));
}

#[test]
fn record_not_an_object() {
    for text in ["5", "\"x\"", "null", "true", "[1]", "[]"] {
        assert_eq!(verdict(&run(text)), "not_an_object", "元素 {text}");
    }
    assert_eq!(verdict(&run("{}")), "接受");
}

#[test]
fn record_reserved_prefix_top_level_only() {
    for text in [
        r#"{"_msg":"x"}"#,
        r#"{"_":1}"#,
        r#"{"_x":[1]}"#,
        r#"{"ok":1,"_x":{"a":1}}"#,
    ] {
        assert_eq!(verdict(&run(text)), "reserved_field", "记录 {text}");
    }
}

#[test]
fn record_same_name_from_two_sources() {
    for text in [
        r#"{"a":{"b":2},"a.b":1}"#,
        r#"{"a":{"b.c":1},"a.b":{"c":2}}"#,
        // 取值为 null 或空对象的来源同样计入（与命名约束同一口径）
        r#"{"a.b":null,"a":{"b":1}}"#,
        r#"{"a.b":1,"a":{"b":{}}}"#,
    ] {
        assert_eq!(verdict(&run(text)), "invalid_field_name", "记录 {text}");
    }
}

#[test]
fn record_timestamp_only_by_timestamp_block() {
    // 对象、空对象与数组一律 timestamp_invalid，不展平、不建列
    for text in [
        r#"{"_timestamp":{}}"#,
        r#"{"_timestamp":{"a":1}}"#,
        r#"{"_timestamp":[1]}"#,
        r#"{"_timestamp":[]}"#,
        r#"{"_timestamp":true}"#,
        r#"{"_timestamp":"bad"}"#,
    ] {
        assert_eq!(verdict(&run(text)), "timestamp_invalid", "记录 {text}");
    }
    let r = run(r#"{"_timestamp":"2026-08-22T02:00:00Z","m":"a"}"#).unwrap();
    assert_eq!(r.timestamp_us, 1_787_364_000_000_000);
    assert_eq!(names(&r), ["m"]);
    let r = run(r#"{"_timestamp":1787364000,"m":"b"}"#).unwrap();
    assert_eq!(r.timestamp_us, 1_787_364_000_000_000);
    // 指数写法与 serde_json 的规范化写法同值
    let r = run(r#"{"_timestamp":1.787364E9}"#).unwrap();
    assert_eq!(r.timestamp_us, 1_787_364_000_000_000);
    // 缺失、null 与 0 以接收时刻填充
    for text in ["{}", r#"{"_timestamp":null}"#, r#"{"_timestamp":-0}"#] {
        assert_eq!(run(text).unwrap().timestamp_us, RECEIVED, "记录 {text}");
    }
    // 同批两种写法都接受，`_timestamp` 不参与类型冲突
    let out = batch(
        r#"[{"_timestamp":"2026-08-22T02:00:00Z","m":"a"},{"_timestamp":1787364000,"m":"b"}]"#,
        &StreamSchema::new(),
    );
    assert!(out.results.iter().all(Result::is_ok));
    assert_eq!(out.new_columns, [("m".to_owned(), ColumnType::Utf8)]);
}

#[test]
fn record_type_mismatch_goes_to_shadow_columns() {
    let s = schema_of(&[
        ("code", ColumnType::Int64),
        ("i", ColumnType::Int64),
        ("i__float64", ColumnType::Float64),
        ("e", ColumnType::Float64),
        ("b", ColumnType::Boolean),
        ("big", ColumnType::Utf8),
    ]);
    // 类型与同名列不同：不拒绝，落入影子列（已有的影子列直接取用）
    for (text, column) in [
        (r#"{"code":"ok"}"#, "code__utf8"),
        (r#"{"i":1.5}"#, "i__float64"),
        (r#"{"e":5}"#, "e__int64"),
        (r#"{"b":"false"}"#, "b__utf8"),
        (r#"{"code":[200]}"#, "code__utf8"),
        (r#"{"big":12345678901234567890}"#, "big"),
        (r#"{"code":201}"#, "code"),
        (r#"{"code":{"x":"new"}}"#, "code.x"),
    ] {
        let r = run_with(text, &s).unwrap_or_else(|e| panic!("记录 {text}：{e:?}"));
        let names: Vec<&str> = r.fields.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, [column], "记录 {text}");
    }
    assert_eq!(
        verdict(&run_with(r#"{"code":{"x":1},"code.x":1}"#, &s)),
        "invalid_field_name"
    );
    for text in [r#"{"code":null}"#, r#"{"code":{}}"#] {
        assert!(run_with(text, &s).unwrap().fields.is_empty(), "记录 {text}");
    }
}

#[test]
fn record_error_precedence_order() {
    let s = schema_of(&[("code", ColumnType::Int64), ("a", ColumnType::Int64)]);
    let out = process_batch(
        &parse_ingest_body(
            br#"[{"x":1e400,"_y":1},5,{"_x":1,"-a":1},{"-a":1,"_timestamp":"bad"},{"_timestamp":"bad","new":1},{"_timestamp":-1,"new":1},{"new":1},{"code":"ok"}]"#,
        )
        .unwrap(),
        RECEIVED,
        &TimestampWindow::default(),
        &s,
        3,
    );
    let got: Vec<String> = out.results.iter().map(verdict).collect();
    assert_eq!(
        got,
        [
            "malformed_record",
            "not_an_object",
            "reserved_field",
            "invalid_field_name",
            "timestamp_invalid",
            "timestamp_out_of_range",
            "too_many_columns",
            "too_many_columns",
        ]
    );
    assert!(out.new_columns.is_empty());
}

#[test]
fn record_batch_first_accepted_defines_type() {
    let out = batch(r#"[{"n":1},{"n":"x"}]"#, &StreamSchema::new());
    let got: Vec<String> = out.results.iter().map(verdict).collect();
    assert_eq!(got, ["接受", "接受"]);
    assert_eq!(
        out.new_columns,
        [
            ("n".to_owned(), ColumnType::Int64),
            ("n__utf8".to_owned(), ColumnType::Utf8)
        ]
    );

    // 被拒绝的记录不登记列：序号 0 因保留字段被拒，序号 1 以基名定型为 Int64，序号 2 落入影子列
    let out = batch(
        r#"[{"n":"x","_bad":1},{"n":1,"m":true},{"n":"y"},{"m":false,"z":1.5}]"#,
        &StreamSchema::new(),
    );
    let got: Vec<String> = out.results.iter().map(verdict).collect();
    assert_eq!(got, ["reserved_field", "接受", "接受", "接受"]);
    assert_eq!(
        out.new_columns,
        [
            ("n".to_owned(), ColumnType::Int64),
            ("m".to_owned(), ColumnType::Boolean),
            ("n__utf8".to_owned(), ColumnType::Utf8),
            ("z".to_owned(), ColumnType::Float64),
        ]
    );

    // 已在 schema 中的列不算新列
    let s = schema_of(&[
        ("code", ColumnType::Int64),
        ("code__utf8", ColumnType::Utf8),
    ]);
    let out = batch(r#"[{"code":"ok"},{"code":201}]"#, &s);
    assert!(out.results.iter().all(Result::is_ok));
    assert!(out.new_columns.is_empty());
}

#[test]
fn record_column_cap_counts_batch_registrations() {
    // 列数 2（`_timestamp`、`a`），上限 3：序号 0 登记 `b` 后已满，序号 1 的新列 `c` 超限，序号 2 只用已有列
    let s = schema_of(&[("a", ColumnType::Int64)]);
    let out = process_batch(
        &parse_ingest_body(br#"[{"b":1},{"c":1,"a":2},{"a":3,"b":4},{"a":"x"}]"#).unwrap(),
        RECEIVED,
        &TimestampWindow::default(),
        &s,
        3,
    );
    let got: Vec<String> = out.results.iter().map(verdict).collect();
    assert_eq!(
        got,
        ["接受", "too_many_columns", "接受", "too_many_columns"]
    );
    assert_eq!(out.new_columns, [("b".to_owned(), ColumnType::Int64)]);
}

#[test]
fn record_field_name_charset_and_separators() {
    let table: &[(&str, &str)] = &[
        (r#"{"a-.b":1}"#, "接受"),
        (r#"{"a--b":1}"#, "接受"),
        (r#"{"a-_b":1}"#, "接受"),
        (r#"{"9":1}"#, "接受"),
        (r#"{"a._b":1}"#, "invalid_field_name"),
        (r#"{"a_.b":1}"#, "invalid_field_name"),
        (r#"{"":1}"#, "invalid_field_name"),
        (r#"{"a":{"":1}}"#, "invalid_field_name"),
        (r#"{"a.":1}"#, "invalid_field_name"),
        (r#"{".a":1}"#, "invalid_field_name"),
        (r#"{"a:b":1}"#, "invalid_field_name"),
        (r#"{"a b":{}}"#, "invalid_field_name"),
        (r#"{"a":{"b c":null}}"#, "invalid_field_name"),
    ];
    for (input, want) in table {
        assert_eq!(verdict(&run(input)), *want, "记录 {input}");
    }
    // 展平后才超长：64 + 1 + 64 = 129 字节
    let long = format!(r#"{{"{a}":{{"{a}":1}}}}"#, a = "a".repeat(64));
    assert_eq!(verdict(&run(&long)), "invalid_field_name");
}
