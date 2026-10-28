//! 数据文件的列统计 `column_stats`（REQ-AMB-QRY-COLUMN-PRUNING「`column_stats`」）：在写出 Parquet 的同一遍中
//! 由 Arrow 数组算出，flush 与合并共用。
//!
//! 每列（`_timestamp` 除外）一个 `{"null_count", "min", "max"}`；`min`、`max` 按该列类型在 `_search` 的 `hits` 中的
//! JSON 呈现写出（Float64 的零值写作 `0.0`；正指数不带 `+` 由桶 manifest 的序列化保证）。全为 NULL、最小或最大值是
//! 长于 256 字节的字符串、或含非有限 Float64 时，两者都为 null。

use amberlog_common::schema::TIMESTAMP_COLUMN;
use arrow::array::{Array, AsArray};
use arrow::datatypes::{DataType, Float64Type, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::{Map, Number, Value};

/// Utf8 的 `min`、`max` 的 UTF-8 长度上限（字节）。
const MAX_STRING_BYTES: usize = 256;

/// `batch` 的列统计：键为除 `_timestamp` 外的各列名。
pub fn column_stats(batch: &RecordBatch) -> Value {
    let mut stats = StatsAccumulator::new(&batch.schema());
    stats.update(batch);
    stats.finish()
}

/// 逐批累计的列统计：对同一 schema 的若干批次依次 [`StatsAccumulator::update`]，[`StatsAccumulator::finish`] 得到的
/// 与把这些批次拼成一个批次再 [`column_stats`] 相同（合并边读边写时用，不必拼出整个文件的批次）。
pub struct StatsAccumulator {
    /// 除 `_timestamp` 外的各列：列名、在批次中的位置、空值个数与取值范围。
    columns: Vec<(String, usize, u64, Range)>,
}

/// 一列至此的取值范围。
enum Range {
    Int64(Option<(i64, i64)>),
    /// 遇到非有限值后为 `None`（两者都写 null）。
    Float64(Option<Option<(f64, f64)>>),
    Boolean(Option<(bool, bool)>),
    Utf8(Option<(String, String)>),
    Other,
}

impl StatsAccumulator {
    pub fn new(schema: &Schema) -> StatsAccumulator {
        let columns = schema
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() != TIMESTAMP_COLUMN)
            .map(|(i, f)| {
                let range = match f.data_type() {
                    DataType::Int64 => Range::Int64(None),
                    DataType::Float64 => Range::Float64(Some(None)),
                    DataType::Boolean => Range::Boolean(None),
                    DataType::Utf8 => Range::Utf8(None),
                    _ => Range::Other,
                };
                (f.name().clone(), i, 0, range)
            })
            .collect();
        StatsAccumulator { columns }
    }

    /// 计入一个批次（schema 与 [`StatsAccumulator::new`] 的相同）。
    pub fn update(&mut self, batch: &RecordBatch) {
        for (_, i, nulls, range) in &mut self.columns {
            let col = batch.column(*i);
            *nulls += col.null_count() as u64;
            match range {
                Range::Int64(r) => {
                    let a = col.as_primitive::<Int64Type>();
                    for v in a.iter().flatten() {
                        *r = Some(r.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
                    }
                }
                Range::Float64(r) => {
                    let a = col.as_primitive::<Float64Type>();
                    for v in a.iter().flatten() {
                        let Some(seen) = r else { break };
                        if !v.is_finite() {
                            *r = None;
                            break;
                        }
                        *seen = Some(seen.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
                    }
                }
                Range::Boolean(r) => {
                    let a = col.as_boolean();
                    for v in a.iter().flatten() {
                        *r = Some(r.map_or((v, v), |(lo, hi)| (lo.min(v), hi.max(v))));
                    }
                }
                Range::Utf8(r) => {
                    // `str` 的字节序即 Unicode 码点序
                    let a = col.as_string::<i32>();
                    let values = a.iter().flatten();
                    if let (Some(lo), Some(hi)) = (values.clone().min(), values.max()) {
                        match r {
                            None => *r = Some((lo.to_owned(), hi.to_owned())),
                            Some((l, h)) => {
                                if lo < l.as_str() {
                                    *l = lo.to_owned();
                                }
                                if hi > h.as_str() {
                                    *h = hi.to_owned();
                                }
                            }
                        }
                    }
                }
                Range::Other => {}
            }
        }
    }

    /// 各列的 `{"null_count", "min", "max"}`，键序与 schema 中的列序相同。
    pub fn finish(self) -> Value {
        let mut out = Map::new();
        for (name, _, nulls, range) in self.columns {
            let (min, max) = range.min_max();
            let mut stat = Map::new();
            stat.insert("null_count".into(), Value::from(nulls));
            stat.insert("min".into(), min);
            stat.insert("max".into(), max);
            out.insert(name, Value::Object(stat));
        }
        Value::Object(out)
    }
}

impl Range {
    fn min_max(self) -> (Value, Value) {
        let none = (Value::Null, Value::Null);
        match self {
            Range::Int64(Some((lo, hi))) => (Value::from(lo), Value::from(hi)),
            Range::Float64(Some(Some((lo, hi)))) => {
                // 零值写作 `0.0`：-0.0 换成 0.0
                let number = |v: f64| Number::from_f64(if v == 0.0 { 0.0 } else { v });
                match (number(lo), number(hi)) {
                    (Some(lo), Some(hi)) => (Value::Number(lo), Value::Number(hi)),
                    _ => none,
                }
            }
            Range::Boolean(Some((lo, hi))) => (Value::Bool(lo), Value::Bool(hi)),
            Range::Utf8(Some((lo, hi)))
                if lo.len() <= MAX_STRING_BYTES && hi.len() <= MAX_STRING_BYTES =>
            {
                (Value::from(lo), Value::from(hi))
            }
            _ => none,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{
        ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, TimestampMicrosecondArray,
    };
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use serde_json::json;

    /// 列统计的键、取值与 JSON 写法（REQ-AMB-QRY-COLUMN-PRUNING `column_stats`）。
    #[test]
    fn pruning_column_stats_shape() {
        let x256 = "x".repeat(256);
        let y257 = "y".repeat(257);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("a", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("s256", DataType::Utf8, true),
            Field::new("s257", DataType::Utf8, true),
            Field::new("n", DataType::Utf8, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3]).with_timezone("UTC")),
            Arc::new(Int64Array::from(vec![Some(3), None, Some(-7)])),
            Arc::new(Float64Array::from(vec![Some(2.5), Some(0.0), None])),
            Arc::new(Float64Array::from(vec![Some(1.0), Some(f64::NAN), None])),
            Arc::new(BooleanArray::from(vec![true, false, true])),
            Arc::new(StringArray::from(vec!["b", "é", "a"])),
            Arc::new(StringArray::from(vec![Some(x256.as_str()), None, None])),
            Arc::new(StringArray::from(vec![
                Some(y257.as_str()),
                Some("a"),
                None,
            ])),
            Arc::new(StringArray::from(vec![None::<&str>, None, None])),
        ];
        let batch = RecordBatch::try_new(schema, columns).unwrap();
        let stats = column_stats(&batch);
        let keys: std::collections::BTreeSet<&str> = stats
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect();
        let want_keys = ["a", "f", "g", "b", "s", "s256", "s257", "n"];
        assert_eq!(keys, want_keys.into_iter().collect());
        let want = json!({
            "a": {"null_count": 1, "min": -7, "max": 3},
            "f": {"null_count": 1, "min": 0.0, "max": 2.5},
            "g": {"null_count": 1, "min": null, "max": null},
            "b": {"null_count": 0, "min": false, "max": true},
            "s": {"null_count": 0, "min": "a", "max": "é"},
            "s256": {"null_count": 2, "min": x256, "max": x256},
            "s257": {"null_count": 1, "min": null, "max": null},
            "n": {"null_count": 3, "min": null, "max": null},
        });
        assert_eq!(stats, want);
        // JSON 写法：Float64 的零值写作 `0.0`
        let text = serde_json::to_string(&stats["f"]).unwrap();
        assert!(text.contains("\"min\":0.0"), "{text}");
        assert!(text.contains("\"max\":2.5"), "{text}");
    }

    /// 逐批累计与整批一次算出相同：同一批次按各种切法分成若干段依次累计。
    #[test]
    fn accumulated_stats_equal_whole_batch() {
        let long = "z".repeat(300);
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("a", DataType::Int64, true),
            Field::new("f", DataType::Float64, true),
            Field::new("g", DataType::Float64, true),
            Field::new("b", DataType::Boolean, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("l", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let columns: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3, 4, 5, 6]).with_timezone("UTC")),
            Arc::new(Int64Array::from(vec![
                None,
                Some(4),
                Some(-2),
                None,
                Some(9),
                Some(0),
            ])),
            Arc::new(Float64Array::from(vec![
                Some(-0.0),
                None,
                Some(3.5),
                Some(0.0),
                Some(-1.25),
                None,
            ])),
            Arc::new(Float64Array::from(vec![
                Some(1.0),
                Some(2.0),
                None,
                Some(f64::INFINITY),
                Some(0.5),
                Some(7.0),
            ])),
            Arc::new(BooleanArray::from(vec![
                Some(true),
                None,
                Some(true),
                Some(false),
                None,
                Some(true),
            ])),
            Arc::new(StringArray::from(vec![
                Some("m"),
                Some("b"),
                None,
                Some("é"),
                Some("a"),
                Some("c"),
            ])),
            Arc::new(StringArray::from(vec![
                Some("k"),
                Some(long.as_str()),
                Some("a"),
                None,
                Some("b"),
                Some("c"),
            ])),
            Arc::new(Int64Array::from(vec![None::<i64>; 6])),
        ];
        let batch = RecordBatch::try_new(Arc::clone(&schema), columns).unwrap();
        let whole = column_stats(&batch);
        let rows = batch.num_rows();
        // 所有在行间切一刀或两刀的切法，另加逐行
        let mut cuts: Vec<Vec<usize>> = vec![(0..=rows).collect()];
        for i in 0..=rows {
            for j in i..=rows {
                cuts.push(vec![0, i, j, rows]);
            }
        }
        for cut in cuts {
            let mut acc = StatsAccumulator::new(&schema);
            for w in cut.windows(2) {
                acc.update(&batch.slice(w[0], w[1] - w[0]));
            }
            assert_eq!(acc.finish(), whole, "切法 {cut:?}");
        }
    }
}
