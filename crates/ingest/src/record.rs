//! 记录模型：一条 JSON 记录到 typed 列的映射与逐条判定（REQ-AMB-DM-RECORD-MODEL-V3、REQ-AMB-DM-SCHEMA-DRIFT、
//! REQ-AMB-DM-FIELD-LIMITS）。
//!
//! 输入是批次中的一个元素（保留数字原文的 [`Json`]，由 `amberlog_common::json::parse_ingest_body` 得到）、
//! 本请求的服务端接收时刻、时间窗配置、「stream 当前 schema」的只读视图与列数上限；输出是规范化后的记录
//! （`_timestamp` 的 UTC 微秒 + 各字段的目标列名与 typed 取值），或逐条记录错误的 `reason`。
//! 记录里的字段名即目标列名（基名或影子列名）：WAL 照此保存，重放按受理时确定的目标列恢复。
//! 本模块是纯函数，不碰 WAL、内存表与对象存储；新列何时并入 stream schema 由调用方在批次确认后决定。
//!
//! 一条记录的判定次序（块「多种逐条错误只报首个」）：
//! 0. 含取值缺陷（REQ-AMB-ING-RECORD-ISOLATION）→ `malformed_record`：缺陷①②④在解析时已标出（[`Element::Defective`]），
//!    缺陷③（字段生效取值是按就近舍入超出 binary64 有限范围的小数）在这里判定；
//! 1. 不是 JSON 对象 → `not_an_object`；
//! 2. 顶层键除 `_timestamp` 外以 `_` 开头（不论取值）→ `reserved_field`；
//! 3. 展平后的字段名不满足命名约束（不论取值，含 `null` 与 `{}`），或两个来源展平到同一字段名 → `invalid_field_name`；
//! 4. 顶层 `_timestamp` 按 REQ-AMB-TIME-TIMESTAMP 归一 → `timestamp_invalid` / `timestamp_out_of_range`；
//! 5. 按落列规则①～④确定各字段的目标列（类型不同不再拒绝，落入影子列）；须登记的新列会使列数超过上限 → `too_many_columns`。

use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

use amberlog_common::error::RecordReason;
use amberlog_common::json::{Element, Json, number_out_of_range};
use amberlog_common::schema::{ColumnType, SchemaLookup, TIMESTAMP_COLUMN, place_record};

use crate::timestamp::{TimestampWindow, normalize_timestamp};

/// 记录类型定义在 common：WAL 与内存表（amberlog-storage）也要承载它，而 storage 不依赖本 crate。
pub use amberlog_common::record::{FieldValue, Record};

/// 展平深度（REQ-AMB-DM-FIELD-LIMITS）：顶层键为第 1 层，至多展平到第 8 层；第 8 层键的取值若仍是非空对象，
/// 该对象整体序列化为 JSON 字符串落在第 8 层的字段名上。
pub const MAX_FLATTEN_DEPTH: usize = 8;

/// 展平后字段名的字节数上限（块「总长不超过 128 字节」）。
pub const MAX_FIELD_NAME_BYTES: usize = 128;

/// 判定一条记录并把它映射为 typed 列。
///
/// `schema` 须是判定时刻的「stream 当前 schema」（含已提交快照的列、内存表已确认记录与在途批次登记的列、
/// 同批次序号更小且被接受的记录引入的列，恒含 `_timestamp`）；批次内的累积见 [`process_batch`]。
/// `max_columns` 为 `ingest.max_columns`。
pub fn process_record<S: SchemaLookup + ?Sized>(
    element: &Json,
    received_at_us: i64,
    window: &TimestampWindow,
    schema: &S,
    max_columns: usize,
) -> Result<Record, RecordReason> {
    // 1. 不是对象
    let Json::Object(members) = element else {
        return Err(RecordReason::NotAnObject);
    };

    // 0. 缺陷③：只看对象字段的生效取值，故在确认是对象之后、其余判定之前（两者不会同时命中）
    if has_out_of_range_float(members) {
        return Err(RecordReason::MalformedRecord);
    }

    // 2. 保留前缀：只看顶层键，与取值无关
    if members
        .iter()
        .any(|(key, _)| key != TIMESTAMP_COLUMN && key.starts_with('_'))
    {
        return Err(RecordReason::ReservedField);
    }

    // 3. 展平（顶层 `_timestamp` 不参与），再查字段名与来源冲突——`null` 与 `{}` 的字段同样检查
    let mut leaves = Vec::new();
    let mut timestamp = None;
    for (key, value) in members {
        if key == TIMESTAMP_COLUMN {
            timestamp = Some(value);
        } else {
            flatten_member(key.clone(), value, 1, &mut leaves);
        }
    }
    let mut seen = HashSet::with_capacity(leaves.len());
    for (name, _) in &leaves {
        if !valid_field_name(name) || !seen.insert(name.as_str()) {
            return Err(RecordReason::InvalidFieldName);
        }
    }

    // 4. `_timestamp`：只按时间戳块处置
    let timestamp_us = normalize_timestamp(
        timestamp.map(Json::to_value).as_ref(),
        received_at_us,
        window,
    )?;

    // 5. 落列与列数上限：只看有 typed 取值的字段（`null` 与 `{}` 不建列）
    let typed: Vec<(String, FieldValue)> = leaves
        .into_iter()
        .filter_map(|(name, leaf)| leaf.map(|v| (name, v)))
        .collect();
    let placement = place_record(
        typed
            .iter()
            .map(|(name, v)| (name.as_str(), v.column_type())),
        schema,
        max_columns,
    )
    .ok_or(RecordReason::TooManyColumns)?;
    let fields = placement
        .targets
        .into_iter()
        .zip(typed)
        .map(|(target, (_, value))| (target, value))
        .collect();
    Ok(Record {
        timestamp_us,
        fields,
    })
}

/// 一个批次的判定结果。
#[derive(Debug, Clone, PartialEq)]
pub struct BatchOutcome {
    /// 与批次元素一一对应（下标即 `index`）。
    pub results: Vec<Result<Record, RecordReason>>,
    /// 被接受的记录登记、而批次开始时的 stream 当前 schema 中没有的列，按登记的顺序。
    /// 调用方在判定之后即把它们登记进 stream 当前 schema（在途），批次以 503 结束时撤回
    /// （REQ-AMB-DM-SCHEMA-DRIFT「在且仅在落入该列的记录所在的全部批次都收到 503 时退出」）。
    pub new_columns: Vec<(String, ColumnType)>,
}

/// 按 `index` 升序逐条判定一个批次；序号较小且被接受的记录登记的列，对同批序号较大的记录属于当前 schema
/// （同批两条记录对同一新字段推断出不同类型时，先被接受者以基名定型，后者落入影子列）。
/// 含取值缺陷①②④的元素只报 `malformed_record`，不另作判定、不引入列。
pub fn process_batch<S: SchemaLookup + ?Sized>(
    elements: &[Element],
    received_at_us: i64,
    window: &TimestampWindow,
    schema: &S,
    max_columns: usize,
) -> BatchOutcome {
    let mut overlay = BatchSchema {
        base: schema,
        added: Vec::new(),
        index: HashMap::new(),
    };
    let mut results = Vec::with_capacity(elements.len());
    for element in elements {
        let result = match element {
            Element::Value(json) => {
                process_record(json, received_at_us, window, &overlay, max_columns)
            }
            Element::Defective => Err(RecordReason::MalformedRecord),
        };
        if let Ok(record) = &result {
            for (name, value) in &record.fields {
                overlay.add(name, value.column_type());
            }
        }
        results.push(result);
    }
    BatchOutcome {
        results,
        new_columns: overlay.added,
    }
}

/// 批次开始时的 stream 当前 schema 加上本批已接受记录引入的列。
struct BatchSchema<'a, S: ?Sized> {
    base: &'a S,
    added: Vec<(String, ColumnType)>,
    index: HashMap<String, usize>,
}

impl<S: SchemaLookup + ?Sized> BatchSchema<'_, S> {
    fn add(&mut self, name: &str, column_type: ColumnType) {
        if self.column_type(name).is_none() {
            self.index.insert(name.to_owned(), self.added.len());
            self.added.push((name.to_owned(), column_type));
        }
    }
}

impl<S: SchemaLookup + ?Sized> SchemaLookup for BatchSchema<'_, S> {
    fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.base
            .column_type(name)
            .or_else(|| self.index.get(name).map(|&i| self.added[i].1))
    }

    fn column_count(&self) -> usize {
        self.base.column_count() + self.added.len()
    }
}

// ---------------------------------------------------------------------------
// 展平与标量类型映射
// ---------------------------------------------------------------------------

/// 记录的字段生效取值（重复键已取最后出现者）中是否有含小数部分或指数、按就近舍入超出 binary64 有限范围的数字
/// （REQ-AMB-ING-RECORD-ISOLATION 缺陷③）。顶层 `_timestamp`、数组之内与整体序列化的子树之内不算，与 [`flatten_member`] 的遍历一致。
fn has_out_of_range_float(members: &[(String, Json)]) -> bool {
    let mut stack: Vec<(&Json, usize)> = members
        .iter()
        .filter(|(key, _)| key != TIMESTAMP_COLUMN)
        .map(|(_, value)| (value, 1))
        .collect();
    while let Some((value, depth)) = stack.pop() {
        match value {
            Json::Number(literal) if is_float_literal(literal) && number_out_of_range(literal) => {
                return true;
            }
            Json::Object(inner) if depth < MAX_FLATTEN_DEPTH => {
                stack.extend(inner.iter().map(|(_, child)| (child, depth + 1)));
            }
            _ => {}
        }
    }
    false
}

fn is_float_literal(literal: &str) -> bool {
    literal.bytes().any(|b| matches!(b, b'.' | b'e' | b'E'))
}

/// 展平一个键值。`name` 是该键展平后的字段名，`depth` 是该键所在层（顶层为 1）。
/// 叶子取值为 `None` 表示 JSON `null` 或空对象：字段名照样参与命名检查，但不建列、不落值。
fn flatten_member(
    name: String,
    value: &Json,
    depth: usize,
    out: &mut Vec<(String, Option<FieldValue>)>,
) {
    let leaf = match value {
        Json::Null => None,
        Json::Object(inner) if inner.is_empty() => None,
        Json::Object(inner) if depth < MAX_FLATTEN_DEPTH => {
            for (key, child) in inner {
                flatten_member(format!("{name}.{key}"), child, depth + 1, out);
            }
            return;
        }
        // 超深子树与数组：整体序列化为规范紧凑形 JSON 文本，落为字符串列
        Json::Object(_) | Json::Array(_) => Some(FieldValue::Utf8(canonical_json(value))),
        Json::Bool(b) => Some(FieldValue::Boolean(*b)),
        Json::String(s) => Some(FieldValue::Utf8(s.clone())),
        Json::Number(literal) => Some(number_value(literal)),
    };
    out.push((name, leaf));
}

/// 按数字字面量的形态定类型：含小数部分或指数 → Float64；否则落在 int64 内 → Int64，
/// 超出 int64 → 字面量本身（JSON 整数字面量即其十进制写法），不舍入、不转浮点。
fn number_value(literal: &str) -> FieldValue {
    if is_float_literal(literal) {
        // 字面量符合 JSON 数字文法，且按就近舍入不超出 binary64 有限范围（超出者已报 `malformed_record`）；
        // 标准库按精确十进制值就近舍入、恰在正中取偶（REQ-AMB-DM-RECORD-MODEL-V3）。
        FieldValue::Float64(literal.parse().unwrap_or(f64::NAN))
    } else {
        match literal.parse::<i64>() {
            Ok(i) => FieldValue::Int64(i),
            Err(_) => FieldValue::Utf8(literal.to_owned()),
        }
    }
}

/// 字段命名约束：仅 `A-Z a-z 0-9 - _ .`，以字母或数字开头与结尾，不超过 128 字节，
/// 且 `_` 与 `.` 两种分隔符不相邻（无 `__`、`..`、`._`、`_.`）。
fn valid_field_name(name: &str) -> bool {
    let b = name.as_bytes();
    let (Some(first), Some(last)) = (b.first(), b.last()) else {
        return false;
    };
    let separator = |c: u8| matches!(c, b'_' | b'.');
    b.len() <= MAX_FIELD_NAME_BYTES
        && first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && b.iter()
            .all(|&c| c.is_ascii_alphanumeric() || c == b'-' || separator(c))
        && !b.windows(2).any(|w| separator(w[0]) && separator(w[1]))
}

// ---------------------------------------------------------------------------
// 规范紧凑形 JSON 文本（数组与超深子树）
// ---------------------------------------------------------------------------

/// 序列化为规范紧凑形：无结构外空白；对象键按输入出现顺序（重复键已在解析时取最后值、留首次位置）；
/// 数字照抄输入字面量；字符串只转义 `"`、`\` 与 U+0000–U+001F。
fn canonical_json(value: &Json) -> String {
    let mut out = String::new();
    write_canonical(value, &mut out);
    out
}

fn write_canonical(value: &Json, out: &mut String) {
    match value {
        Json::Null => out.push_str("null"),
        Json::Bool(true) => out.push_str("true"),
        Json::Bool(false) => out.push_str("false"),
        Json::Number(literal) => out.push_str(literal),
        Json::String(s) => write_string(s, out),
        Json::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_canonical(item, out);
            }
            out.push(']');
        }
        Json::Object(members) => {
            out.push('{');
            for (i, (key, item)) in members.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write_string(key, out);
                out.push(':');
                write_canonical(item, out);
            }
            out.push('}');
        }
    }
}

/// U+0008、U+000C、U+000A、U+000D、U+0009 写作 `\b`、`\f`、`\n`、`\r`、`\t`，
/// U+0000–U+001F 中其余字符写作小写十六进制 `\u00XX`，其余字符按 UTF-8 原样输出。
fn write_string(s: &str, out: &mut String) {
    out.push('"');
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", u32::from(c));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

#[cfg(test)]
mod tests;
