//! 带字段 id 的 stream schema：快照 `schema` 的元素（REQ-AMB-STO-HOT-LAYOUT-V2）与可见集持有的当前 schema（ADR-0022）。
//!
//! 字段 id 的分配（接受新列时取当前最大值 + 1）属可见集的写入一侧，不在这里；本模块只承载结果，
//! 并给出列到 Arrow 字段的对应，使可见集的批次、Parquet 文件与查询的表 schema 用同一套写法。

use std::collections::HashMap;
use std::sync::Arc;

use amberlog_common::schema::{ColumnType, SchemaLookup, TIMESTAMP_COLUMN};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;

/// Iceberg 兼容的字段 id：自 `_timestamp` 为 1 起递增，永不复用。
pub type FieldId = u32;

/// `_timestamp` 的字段 id。
pub const TIMESTAMP_FIELD_ID: FieldId = 1;

/// 快照 `schema` 的一个元素：`name`、`type`、`field_id`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaColumn {
    pub name: String,
    pub column_type: ColumnType,
    pub field_id: FieldId,
}

impl SchemaColumn {
    /// 该列在 Arrow 中的字段：带 `PARQUET:field_id` 元数据。
    /// `_timestamp` 为不可空的 `Timestamp(Microsecond, "UTC")`（写进 Parquet 即 INT64 + TIMESTAMP(UTC, MICROS) + REQUIRED），
    /// 其余列可空，类型按 [`ColumnType`] 一一对应。
    pub fn arrow_field(&self) -> Field {
        let (data_type, nullable) = if self.name == TIMESTAMP_COLUMN {
            (
                DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC"))),
                false,
            )
        } else {
            let data_type = match self.column_type {
                ColumnType::Int64 => DataType::Int64,
                ColumnType::Float64 => DataType::Float64,
                ColumnType::Boolean => DataType::Boolean,
                ColumnType::Utf8 => DataType::Utf8,
            };
            (data_type, true)
        };
        Field::new(&self.name, data_type, nullable).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            self.field_id.to_string(),
        )]))
    }
}

/// 一个 stream 的全部列（带字段 id），按字段 id 升序。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamColumns {
    columns: Vec<SchemaColumn>,
    index: HashMap<String, usize>,
}

impl StreamColumns {
    /// 创世快照的 schema：只有 `_timestamp`（Int64，字段 id 1）。
    pub fn genesis() -> Self {
        Self::from_columns(vec![SchemaColumn {
            name: TIMESTAMP_COLUMN.to_owned(),
            column_type: ColumnType::Int64,
            field_id: TIMESTAMP_FIELD_ID,
        }])
    }

    /// 由已有的列构造（例如从快照 `schema` 读入）。不做校验：内容是否合规由读取方按 REQ-AMB-STO-HOT-LAYOUT-V2 判定。
    pub fn from_columns(mut columns: Vec<SchemaColumn>) -> Self {
        columns.sort_by_key(|c| c.field_id);
        let index = columns
            .iter()
            .enumerate()
            .map(|(i, c)| (c.name.clone(), i))
            .collect();
        StreamColumns { columns, index }
    }

    pub fn columns(&self) -> &[SchemaColumn] {
        &self.columns
    }

    pub fn get(&self, name: &str) -> Option<&SchemaColumn> {
        self.index.get(name).map(|&i| &self.columns[i])
    }

    /// 全部列对应的 Arrow schema（查询的表 schema；可见集的批次与数据文件各取其中一部分列）。
    pub fn arrow_schema(&self) -> Schema {
        Schema::new(
            self.columns
                .iter()
                .map(SchemaColumn::arrow_field)
                .collect::<Vec<_>>(),
        )
    }
}

impl SchemaLookup for StreamColumns {
    fn column_type(&self, name: &str) -> Option<ColumnType> {
        self.get(name).map(|c| c.column_type)
    }

    fn column_count(&self) -> usize {
        self.columns.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn genesis_has_only_timestamp() {
        let s = StreamColumns::genesis();
        assert_eq!(s.columns().len(), 1);
        assert_eq!(s.column_type(TIMESTAMP_COLUMN), Some(ColumnType::Int64));
        assert_eq!(s.get(TIMESTAMP_COLUMN).unwrap().field_id, 1);
    }

    #[test]
    fn arrow_fields_carry_field_id_and_types() {
        let s = StreamColumns::from_columns(vec![
            SchemaColumn {
                name: "msg".into(),
                column_type: ColumnType::Utf8,
                field_id: 2,
            },
            SchemaColumn {
                name: TIMESTAMP_COLUMN.into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
        ]);
        let schema = s.arrow_schema();
        let ts = schema.field(0);
        assert_eq!(ts.name(), TIMESTAMP_COLUMN);
        assert!(!ts.is_nullable());
        assert_eq!(
            ts.data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, Some(Arc::from("UTC")))
        );
        let msg = schema.field(1);
        assert_eq!(msg.data_type(), &DataType::Utf8);
        assert!(msg.is_nullable());
        assert_eq!(
            msg.metadata()
                .get(PARQUET_FIELD_ID_META_KEY)
                .map(String::as_str),
            Some("2")
        );
    }
}
