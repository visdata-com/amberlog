//! 冷层 schema 与列名（REQ-AMB-STO-COLD-TABLE「schema 与列名」；ADR-0041 第 1、2 条）。
//!
//! 冷层列名取热层原名；两列的热层列名按 ASCII 转小写后相同时，字段编号较大的列改名为 `<原名>__<k>`、原名写进注释。
//! 用户字段名不允许含 `__`，影子列后缀是类型名而不是数字，所以改出的名字不会与现有或将来的列撞名（ADR-0041 第 2 条）。
//! 冷层 schema 只增不减：已有列的编号、列名、类型与先后位置不变，新列按字段编号从小到大追加在后面。

use std::collections::HashSet;
use std::sync::Arc;

use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use iceberg::spec::{NestedField, NestedFieldRef, PrimitiveType, Schema, Type};

use crate::schema::{SchemaColumn, TIMESTAMP_FIELD_ID};

/// 热层类型在冷层的对应（REQ-AMB-STO-COLD-TABLE-TYPES）。
pub fn cold_type(column: &SchemaColumn) -> PrimitiveType {
    if column.field_id == TIMESTAMP_FIELD_ID {
        return PrimitiveType::Timestamptz;
    }
    match column.column_type {
        ColumnType::Int64 => PrimitiveType::Long,
        ColumnType::Float64 => PrimitiveType::Double,
        ColumnType::Boolean => PrimitiveType::Boolean,
        ColumnType::Utf8 => PrimitiveType::String,
    }
}

/// 冷层列对应的热层列名：改过名的列取注释里的原名，其余取列名本身。
pub fn hot_name(field: &NestedField) -> &str {
    match &field.doc {
        Some(doc) if field.name != *doc => doc,
        _ => &field.name,
    }
}

/// 在冷层 schema `prev`（尚无冷层表时为 `None`）之后并入热层列 `hot`，得到新 schema 的各列（按位置）。
///
/// `prev` 中已有的列原样保留（REQ-AMB-STO-COLD-TABLE-GROW）；`hot` 中字段编号不在 `prev` 里的列按字段编号从小到大追加
/// （REQ-AMB-STO-COLD-TABLE-APPEND），列名按 REQ-AMB-STO-COLD-TABLE-NAME、-RENAME、-RENAME-DOC 定。
pub fn evolve(prev: Option<&Schema>, hot: &[SchemaColumn]) -> Vec<NestedFieldRef> {
    let mut fields: Vec<NestedFieldRef> = prev
        .map(|s| s.as_struct().fields().to_vec())
        .unwrap_or_default();
    let known: HashSet<i32> = fields.iter().map(|f| f.id).collect();
    // 字段编号较小的列的热层列名（小写），与已有的冷层列名（小写）
    let mut hot_lower: HashSet<String> = fields
        .iter()
        .map(|f| hot_name(f).to_ascii_lowercase())
        .collect();
    let mut cold_lower: HashSet<String> =
        fields.iter().map(|f| f.name.to_ascii_lowercase()).collect();

    let mut new: Vec<&SchemaColumn> = hot
        .iter()
        .filter(|c| !known.contains(&field_id(c)))
        .collect();
    new.sort_by_key(|c| c.field_id);
    for column in new {
        let lower = column.name.to_ascii_lowercase();
        let name = if hot_lower.contains(&lower) {
            (2u32..)
                .map(|k| format!("{}__{k}", column.name))
                .find(|n| !cold_lower.contains(&n.to_ascii_lowercase()))
                .expect("整数无穷，总能找到")
        } else {
            column.name.clone()
        };
        let field_type = Type::Primitive(cold_type(column));
        let field = if column.name == TIMESTAMP_COLUMN {
            NestedField::required(field_id(column), &name, field_type)
        } else {
            NestedField::optional(field_id(column), &name, field_type)
        };
        let field = if name != column.name {
            field.with_doc(&column.name)
        } else {
            field
        };
        hot_lower.insert(lower);
        cold_lower.insert(name.to_ascii_lowercase());
        fields.push(Arc::new(field));
    }
    fields
}

fn field_id(column: &SchemaColumn) -> i32 {
    i32::try_from(column.field_id).expect("字段编号不超过 i32（Iceberg 字段编号的取值范围）")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, column_type: ColumnType, field_id: u32) -> SchemaColumn {
        SchemaColumn {
            name: name.into(),
            column_type,
            field_id,
        }
    }

    fn ts() -> SchemaColumn {
        col(TIMESTAMP_COLUMN, ColumnType::Int64, 1)
    }

    fn schema(fields: Vec<NestedFieldRef>) -> Schema {
        Schema::builder().with_fields(fields).build().unwrap()
    }

    #[test]
    fn types_and_required() {
        let fields = evolve(
            None,
            &[
                ts(),
                col("n", ColumnType::Int64, 2),
                col("x", ColumnType::Float64, 3),
                col("ok", ColumnType::Boolean, 4),
                col("msg", ColumnType::Utf8, 5),
            ],
        );
        let got: Vec<_> = fields
            .iter()
            .map(|f| (f.id, f.name.as_str(), f.field_type.to_string(), f.required))
            .collect();
        assert_eq!(
            got,
            [
                (1, "_timestamp", "timestamptz".into(), true),
                (2, "n", "long".into(), false),
                (3, "x", "double".into(), false),
                (4, "ok", "boolean".into(), false),
                (5, "msg", "string".into(), false),
            ]
        );
    }

    #[test]
    fn case_only_duplicates_are_renamed_by_field_id() {
        // 场景「只差大小写的列在冷层改名」「改名的列以原名为注释」
        let fields = evolve(
            None,
            &[
                ts(),
                col("FOO", ColumnType::Utf8, 4),
                col("Foo", ColumnType::Utf8, 2),
                col("foo", ColumnType::Utf8, 3),
            ],
        );
        let got: Vec<_> = fields
            .iter()
            .map(|f| (f.id, f.name.as_str(), f.doc.as_deref()))
            .collect();
        assert_eq!(
            got,
            [
                (1, "_timestamp", None),
                (2, "Foo", None),
                (3, "foo__2", Some("foo")),
                (4, "FOO__3", Some("FOO")),
            ]
        );
        assert_eq!(hot_name(&fields[3]), "FOO");
        assert_eq!(hot_name(&fields[1]), "Foo");
    }

    #[test]
    fn original_names_kept() {
        let fields = evolve(
            None,
            &[
                ts(),
                col("kubernetes.pod.name", ColumnType::Utf8, 2),
                col("X-Request-Id", ColumnType::Utf8, 3),
                col("status", ColumnType::Int64, 4),
                col("status__utf8", ColumnType::Utf8, 5),
            ],
        );
        let names: Vec<_> = fields.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            [
                "_timestamp",
                "kubernetes.pod.name",
                "X-Request-Id",
                "status",
                "status__utf8"
            ]
        );
    }

    #[test]
    fn schema_only_grows_and_appends_by_field_id() {
        // 场景「新列按字段编号追加在已有列之后」：已有 `_timestamp`、`c`、`a`，再并入 `b`
        let v1 = schema(evolve(
            None,
            &[
                ts(),
                col("c", ColumnType::Utf8, 2),
                col("a", ColumnType::Utf8, 3),
            ],
        ));
        let v2 = evolve(
            Some(&v1),
            &[
                ts(),
                col("b", ColumnType::Utf8, 4),
                col("a", ColumnType::Utf8, 3),
            ],
        );
        let names: Vec<_> = v2.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, ["_timestamp", "c", "a", "b"]);
        // 已有列原样保留
        assert_eq!(&v2[..3], v1.as_struct().fields());
    }

    #[test]
    fn later_case_duplicate_renamed_against_frozen_names() {
        // 改名一经写入即冻结：后来的 `FOO` 与已有的 `Foo`、`foo__2` 比较
        let v1 = schema(evolve(
            None,
            &[
                ts(),
                col("Foo", ColumnType::Utf8, 2),
                col("foo", ColumnType::Utf8, 3),
            ],
        ));
        let v2 = evolve(Some(&v1), &[col("FOO", ColumnType::Utf8, 4)]);
        assert_eq!(v2[3].name, "FOO__3");
        assert_eq!(v2[3].doc.as_deref(), Some("FOO"));
    }
}
