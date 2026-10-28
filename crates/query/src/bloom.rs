//! 按等值 bloom 判定数据文件能否不扫描（REQ-AMB-QRY-EQUALITY-BLOOM-READ-SKIP、-READ-BY-FILE、-READ-BROKEN）。
//!
//! 可判等值条件（TERM-EQ-COND）从列统计剪枝已提取的可判条件（[`prune::checks`]）中筛出：`c = v`（含字面量在左）
//! 或 `c IN (…)`，`Utf8` 列配字符串字面量，`Int64` 列配 Int64 范围内的整数字面量（原文只有数字，可带负号）。
//! 其他写法一概不用 bloom。判定是纯函数：输入可判等值条件与文件里实际有的 bloom（[`amberlog_storage::bloom::probe`]
//! 读出，不看本进程此刻的索引提示），某个条件在每个行组都有该列的 bloom、且每个字面量都判「肯定不在」即可跳过。
//! 某行组该列没有 bloom 或 bloom 无法解析（`None`）时该条件不作依据。

use amberlog_common::schema::ColumnType;
use amberlog_storage::bloom::{FileBlooms, Pick};
use amberlog_storage::schema::StreamColumns;
use bigdecimal::ToPrimitive;

use crate::prune::Check;
use crate::sql::{CmpOp, Literal, Numeral};

/// 一个可判等值条件的字面量，按列类型取值（与写方的物理类型一致：`Utf8` 按 UTF-8 字节、`Int64` 按 64 位整数）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Values {
    Utf8(Vec<String>),
    Int64(Vec<i64>),
}

/// 一条查询的全部可判等值条件；列去重，每列在 [`FileBlooms`] 中占一位。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EqConds {
    columns: Vec<String>,
    conds: Vec<(usize, Values)>,
}

impl EqConds {
    /// 从可判条件 `checks` 中筛出可判等值条件；列类型取 `schema`。
    pub fn of(checks: &[Check], schema: &StreamColumns) -> EqConds {
        let mut out = EqConds::default();
        for check in checks {
            let (column, literals) = match check {
                Check::Cmp {
                    column,
                    op: CmpOp::Eq,
                    value,
                } => (column, std::slice::from_ref(value)),
                Check::In { column, values } => (column, values.as_slice()),
                _ => continue,
            };
            let Some(c) = schema.get(column) else {
                continue;
            };
            let values = match c.column_type {
                ColumnType::Utf8 => literals
                    .iter()
                    .map(|l| match l {
                        Literal::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(Values::Utf8),
                ColumnType::Int64 => literals
                    .iter()
                    .map(|l| match l {
                        Literal::Number(x, Numeral::Integer) => x.to_i64(),
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(Values::Int64),
                ColumnType::Float64 | ColumnType::Boolean => None,
            };
            let Some(values) = values else {
                continue;
            };
            let i = match out.columns.iter().position(|x| x == column) {
                Some(i) => i,
                None => {
                    out.columns.push(column.clone());
                    out.columns.len() - 1
                }
            };
            out.conds.push((i, values));
        }
        out
    }

    pub fn is_empty(&self) -> bool {
        self.conds.is_empty()
    }

    /// 各条件（列名, 字面量），按在 `WHERE` 中出现的先后。
    pub fn listed(&self) -> Vec<(&str, &Values)> {
        self.conds
            .iter()
            .map(|(i, v)| (self.columns[*i].as_str(), v))
            .collect()
    }

    /// 热层数据文件要取 bloom 的列：按列名。
    pub fn hot_picks(&self) -> Vec<Pick> {
        self.columns.iter().cloned().map(Pick::Name).collect()
    }

    /// 冷层数据文件要取 bloom 的列：按热层列的字段编号对到冷层物理列（REQ-AMB-QRY-EQUALITY-BLOOM-TERM-DECLARED）。
    /// 每列都在 `schema` 中（[`EqConds::of`] 已据此筛过）。
    pub fn cold_picks(&self, schema: &StreamColumns) -> Vec<Pick> {
        self.columns
            .iter()
            .map(|c| Pick::FieldId(schema.get(c).map_or(0, |c| c.field_id)))
            .collect()
    }

    /// 按 `blooms`（列序同 [`EqConds::hot_picks`]）判定该数据文件能否不扫描。
    pub fn skip(&self, blooms: &FileBlooms) -> bool {
        self.conds.iter().any(|(i, values)| {
            blooms.iter().all(|group| {
                group
                    .get(*i)
                    .and_then(Option::as_ref)
                    .is_some_and(|b| match values {
                        Values::Utf8(v) => v.iter().all(|s| !b.check(s.as_str())),
                        Values::Int64(v) => v.iter().all(|x| !b.check(x)),
                    })
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use amberlog_storage::bloom::{self, InMemory};
    use amberlog_storage::schema::SchemaColumn;
    use arrow::array::{ArrayRef, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};
    use parquet::file::properties::WriterProperties;

    use crate::prune;
    use crate::sql;

    fn schema() -> StreamColumns {
        let col = |name: &str, column_type, field_id| SchemaColumn {
            name: name.into(),
            column_type,
            field_id,
        };
        StreamColumns::from_columns(vec![
            col("_timestamp", ColumnType::Int64, 1),
            col("trace_id", ColumnType::Utf8, 2),
            col("user_id", ColumnType::Int64, 3),
            col("ratio", ColumnType::Float64, 4),
            col("ok", ColumnType::Boolean, 5),
            col("msg", ColumnType::Utf8, 6),
        ])
    }

    fn conds(filter: &str) -> EqConds {
        let q = sql::parse(&format!("SELECT * FROM app WHERE {filter}")).unwrap();
        EqConds::of(&prune::checks(q.filter.as_ref()), &schema())
    }

    fn utf8(v: &[&str]) -> Values {
        Values::Utf8(v.iter().map(|s| s.to_string()).collect())
    }

    /// 只有可判等值条件按 bloom 判定。
    #[test]
    fn bloom_eq_cond_table() {
        let rows: &[(&str, &[(&str, Values)])] = &[
            ("trace_id = 't-200'", &[("trace_id", utf8(&["t-200"]))]),
            ("'t-200' = trace_id", &[("trace_id", utf8(&["t-200"]))]),
            (
                "trace_id IN ('t-200', 't-250')",
                &[("trace_id", utf8(&["t-200", "t-250"]))],
            ),
            ("user_id = 200", &[("user_id", Values::Int64(vec![200]))]),
            ("user_id = -200", &[("user_id", Values::Int64(vec![-200]))]),
            (
                "user_id IN (200, 250)",
                &[("user_id", Values::Int64(vec![200, 250]))],
            ),
            (
                "user_id > 0 AND (trace_id = 't-200')",
                &[("trace_id", utf8(&["t-200"]))],
            ),
            ("user_id = 200.0", &[]),
            ("user_id = 2e2", &[]),
            ("user_id = 9223372036854775808", &[]),
            ("ratio = 0.5", &[]),
            ("ok = true", &[]),
            ("trace_id LIKE 't-2%'", &[]),
            ("trace_id != 't-200'", &[]),
            ("trace_id NOT IN ('t-200')", &[]),
            ("NOT (trace_id = 't-200')", &[]),
            ("trace_id = 't-200' OR user_id = 300", &[]),
        ];
        for (filter, want) in rows {
            let got = conds(filter);
            let want: Vec<(&str, &Values)> = want.iter().map(|(c, v)| (*c, v)).collect();
            assert_eq!(got.listed(), want, "{filter}");
        }
    }

    /// 20 条记录、行组行数上限 10 的文件：前 10 条 `trace_id` 为 `t-100`、后 10 条为 `t-200`，`trace_id` 写 bloom。
    fn two_groups() -> Bytes {
        let props = bloom::with_blooms(
            WriterProperties::builder().set_max_row_group_row_count(Some(10)),
            ["trace_id"],
            10,
        )
        .build();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let values: Vec<&str> = (0..20)
            .map(|i| if i < 10 { "t-100" } else { "t-200" })
            .collect();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(values)) as ArrayRef],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        Bytes::from(w.into_inner().unwrap())
    }

    /// 以 `edit` 改写 `bytes` 的尾部元数据（数据页与 bloom 的字节不动），返回改写后的完整文件。
    fn rewrite_footer(
        bytes: &Bytes,
        edit: impl FnOnce(ParquetMetaData) -> ParquetMetaData,
    ) -> Bytes {
        let meta = ParquetMetaDataReader::new()
            .parse_and_finish(bytes)
            .unwrap();
        let meta_len =
            u32::from_le_bytes(bytes[bytes.len() - 8..bytes.len() - 4].try_into().unwrap());
        let body = &bytes[..bytes.len() - 8 - meta_len as usize];
        let mut out = body.to_vec();
        ParquetMetaDataWriter::new(&mut out, &edit(meta))
            .finish()
            .unwrap();
        Bytes::from(out)
    }

    async fn blooms_of(bytes: &Bytes) -> FileBlooms {
        bloom::probe(
            &InMemory(bytes.clone()),
            bytes.len() as u64,
            &[Pick::Name("trace_id".into())],
        )
        .await
        .unwrap()
    }

    /// 多行组文件只在每个行组都判肯定不在时才跳过。
    #[tokio::test]
    async fn bloom_skip_requires_all_row_groups() {
        let p1 = two_groups();
        // P2：同样的记录，只有第一个行组的 `trace_id` 有 bloom
        let p2 = rewrite_footer(&p1, |meta| {
            let mut b = meta.into_builder();
            let mut groups = b.take_row_groups();
            let second = groups.remove(1);
            let column = second.column(0).clone().into_builder();
            let column = column
                .set_bloom_filter_offset(None)
                .set_bloom_filter_length(None)
                .build()
                .unwrap();
            let mut columns = second.columns().to_vec();
            columns[0] = column;
            let second = second
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap();
            groups.insert(1, second);
            for g in groups {
                b = b.add_row_group(g);
            }
            b.build()
        });

        let (b1, b2) = (blooms_of(&p1).await, blooms_of(&p2).await);
        assert_eq!(b1.len(), 2, "P1 有两个行组");
        assert_eq!(b2.len(), 2, "P2 有两个行组");
        // 前提核对
        for group in [&b1[0], &b1[1], &b2[0]] {
            assert!(!group[0].as_ref().unwrap().check("t-999"));
        }
        assert!(b2[1][0].is_none());
        assert!(!b1[0][0].as_ref().unwrap().check("t-200"));

        let cases = [
            (&b1, "trace_id = 't-200'", false),
            (&b1, "trace_id = 't-999'", true),
            (&b1, "trace_id IN ('t-999', 't-200')", false),
            (&b2, "trace_id = 't-999'", false),
        ];
        for (blooms, filter, want) in cases {
            assert_eq!(conds(filter).skip(blooms), want, "{filter}");
        }
    }

    /// bloom 偏移越过文件末尾时视为没有 bloom。
    #[tokio::test]
    async fn bloom_broken_offset_ignored() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trace_id",
            DataType::Utf8,
            true,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(StringArray::from(vec!["t-100"])) as ArrayRef],
        )
        .unwrap();
        let props = bloom::with_blooms(WriterProperties::builder(), ["trace_id"], 1).build();
        let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        let good = Bytes::from(w.into_inner().unwrap());
        let beyond = good.len() as i64 + 4096;
        let broken = rewrite_footer(&good, |meta| {
            let mut b = meta.into_builder();
            let mut groups = b.take_row_groups();
            let g = groups.remove(0);
            let column = g
                .column(0)
                .clone()
                .into_builder()
                .set_bloom_filter_offset(Some(beyond))
                .build()
                .unwrap();
            let mut columns = g.columns().to_vec();
            columns[0] = column;
            let g = g
                .into_builder()
                .set_column_metadata(columns)
                .build()
                .unwrap();
            b = b.add_row_group(g);
            b.build()
        });

        let blooms = blooms_of(&broken).await;
        assert!(blooms[0][0].is_none(), "越过文件末尾的 bloom 视为没有");
        assert!(!conds("trace_id = 't-999'").skip(&blooms));
    }
}
