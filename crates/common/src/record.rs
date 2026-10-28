//! 被接受的记录：规范化后的 `_timestamp` 与展平后的 typed 取值（REQ-AMB-DM-RECORD-MODEL-V2）。
//!
//! 由摄取管道产出（`amberlog_ingest::record`），经耐久缓冲（WAL）持久化、写入可见集（Memtable）后由物化写成 Parquet；
//! 三者分属不同 crate，所以类型放在 common。

use crate::schema::ColumnType;

/// 一个字段的 typed 取值；类型即其列类型。
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Int64(i64),
    Float64(f64),
    Boolean(bool),
    Utf8(String),
}

impl FieldValue {
    pub fn column_type(&self) -> ColumnType {
        match self {
            FieldValue::Int64(_) => ColumnType::Int64,
            FieldValue::Float64(_) => ColumnType::Float64,
            FieldValue::Boolean(_) => ColumnType::Boolean,
            FieldValue::Utf8(_) => ColumnType::Utf8,
        }
    }
}

/// 一条被接受的记录。
///
/// `fields` 按展平后在输入中的出现顺序排列，字段名互不相同，不含 `_timestamp`。
/// 取值为 JSON `null` 与空对象 `{}` 的字段不在其中：它们在所有列上都是 SQL NULL（与缺失相同），
/// 且不因它们建列。
#[derive(Debug, Clone, PartialEq)]
pub struct Record {
    pub timestamp_us: i64,
    pub fields: Vec<(String, FieldValue)>,
}
