//! 按文件级列统计剪枝（REQ-AMB-QRY-COLUMN-PRUNING「可据以剪枝的条件」「剪枝承诺」）。
//!
//! 可判条件取自 `WHERE` 中以 `AND` 连接、逐层展开圆括号后的各合取项，形如 `c op v`（含字面量在左的对称形式）、
//! `c IN (…)`、`c IS [NOT] NULL`；`OR`、`NOT`、`!=`、`NOT IN`、`LIKE` 不作依据。判定是纯函数：输入可判条件与一个
//! `files` 条目，答「不可能成立」或「可能成立」。比较语义同 `_search`：Int64 列按精确值，Float64 列先把字面量就近舍入
//! 为双精度数，Utf8 按码点序，Boolean `false < true`。`_timestamp` 取条目的 `min_timestamp`、`max_timestamp`。
//!
//! 条目不含 `column_stats` 或其形状不合规时视为没有列统计（只有 `_timestamp` 可判）；统计值与列类型对不上时该项不作依据。

use std::cmp::Ordering;
use std::collections::HashMap;

use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use amberlog_storage::chain::FileEntry;
use amberlog_storage::schema::StreamColumns;
use bigdecimal::BigDecimal;
use serde_json::Value;

use crate::sql::{CmpOp, Cond, Literal, Operand};

/// 一个可判条件。
#[derive(Debug, Clone, PartialEq)]
pub enum Check {
    Cmp {
        column: String,
        op: CmpOp,
        value: Literal,
    },
    In {
        column: String,
        values: Vec<Literal>,
    },
    IsNull {
        column: String,
        negated: bool,
    },
}

/// `filter` 中的全部可判条件。
pub fn checks(filter: Option<&Cond>) -> Vec<Check> {
    let mut out = Vec::new();
    if let Some(f) = filter {
        collect(f, &mut out);
    }
    out
}

fn collect(cond: &Cond, out: &mut Vec<Check>) {
    match cond {
        Cond::And(v) => v.iter().for_each(|c| collect(c, out)),
        Cond::Cmp { op, left, right } if *op != CmpOp::NotEq => match (left, right) {
            (Operand::Column(c), Operand::Literal(l)) if *l != Literal::Null => {
                out.push(Check::Cmp {
                    column: c.clone(),
                    op: *op,
                    value: l.clone(),
                })
            }
            (Operand::Literal(l), Operand::Column(c)) if *l != Literal::Null => {
                out.push(Check::Cmp {
                    column: c.clone(),
                    op: op.flip(),
                    value: l.clone(),
                })
            }
            _ => {}
        },
        Cond::In {
            negated: false,
            operand: Operand::Column(c),
            list,
        } => {
            let values: Option<Vec<Literal>> = list
                .iter()
                .map(|o| match o {
                    Operand::Literal(l) if *l != Literal::Null => Some(l.clone()),
                    _ => None,
                })
                .collect();
            if let Some(values) = values {
                out.push(Check::In {
                    column: c.clone(),
                    values,
                });
            }
        }
        Cond::IsNull {
            negated,
            operand: Operand::Column(c),
        } => out.push(Check::IsNull {
            column: c.clone(),
            negated: *negated,
        }),
        _ => {}
    }
}

/// 一列在一个文件里的统计。
struct Stat<'a> {
    null_count: u64,
    /// `min`、`max` 都非 null 时为两者。
    range: Option<(&'a Value, &'a Value)>,
}

/// `file` 的列统计；没有或形状不合规为 `None`。
fn stats(file: &FileEntry) -> Option<HashMap<&str, Stat<'_>>> {
    let obj = file.column_stats.as_ref()?.as_object()?;
    let mut out = HashMap::new();
    for (name, v) in obj {
        let v = v.as_object()?;
        let null_count = v.get("null_count")?.as_u64()?;
        let (min, max) = (v.get("min")?, v.get("max")?);
        let range = (!min.is_null() && !max.is_null()).then_some((min, max));
        out.insert(name.as_str(), Stat { null_count, range });
    }
    Some(out)
}

/// 按 `checks` 判定 `file` 中是否不可能有满足条件的行（任一可判条件不可能成立即是）。
pub fn impossible(checks: &[Check], file: &FileEntry, schema: &StreamColumns) -> bool {
    if checks.is_empty() {
        return false;
    }
    let stats = stats(file);
    let ts_min = Value::from(file.min_timestamp);
    let ts_max = Value::from(file.max_timestamp);
    checks.iter().any(|check| {
        let column = match check {
            Check::Cmp { column, .. } | Check::In { column, .. } | Check::IsNull { column, .. } => {
                column.as_str()
            }
        };
        let Some(column_type) = schema.get(column).map(|c| c.column_type) else {
            return false;
        };
        let stat = if column == TIMESTAMP_COLUMN {
            Stat {
                null_count: 0,
                range: Some((&ts_min, &ts_max)),
            }
        } else {
            let Some(stats) = &stats else {
                return false;
            };
            match stats.get(column) {
                // 未出现在 `column_stats` 中：全为 NULL
                None => Stat {
                    null_count: file.record_count,
                    range: None,
                },
                Some(s) => Stat {
                    null_count: s.null_count,
                    range: s.range,
                },
            }
        };
        impossible_one(check, column_type, &stat, file.record_count)
    })
}

fn impossible_one(check: &Check, t: ColumnType, stat: &Stat<'_>, records: u64) -> bool {
    let all_null = stat.null_count >= records;
    match check {
        Check::IsNull { negated: false, .. } => stat.null_count == 0,
        _ if all_null => true,
        Check::IsNull { negated: true, .. } => false,
        Check::Cmp { op, value, .. } => {
            let Some((min, max)) = stat.range else {
                return false;
            };
            let (Some(lo), Some(hi)) = (order(t, min, value), order(t, max, value)) else {
                return false;
            };
            // lo = min 与 v 的次序，hi = max 与 v 的次序
            match op {
                CmpOp::Eq => lo.is_gt() || hi.is_lt(),
                CmpOp::Lt => lo.is_ge(),
                CmpOp::LtEq => lo.is_gt(),
                CmpOp::Gt => hi.is_le(),
                CmpOp::GtEq => hi.is_lt(),
                CmpOp::NotEq => false,
            }
        }
        Check::In { values, .. } => {
            let Some((min, max)) = stat.range else {
                return false;
            };
            values.iter().all(|v| {
                matches!(
                    (order(t, min, v), order(t, max, v)),
                    (Some(lo), Some(hi)) if lo.is_gt() || hi.is_lt()
                )
            })
        }
    }
}

/// 统计值 `stat`（该列类型的 JSON 呈现）与字面量 `lit` 的次序；类型对不上为 `None`。
fn order(t: ColumnType, stat: &Value, lit: &Literal) -> Option<Ordering> {
    match (t, lit) {
        (ColumnType::Int64, Literal::Number(x, _)) => Some(BigDecimal::from(stat.as_i64()?).cmp(x)),
        (ColumnType::Float64, Literal::Number(x, _)) => {
            let v = stat.as_f64()?;
            // Rust 的 f64 解析是正确舍入的（就近、恰在正中取偶），超出有限范围得同号无穷大
            let d: f64 = x.to_string().parse().ok()?;
            v.partial_cmp(&d)
        }
        (ColumnType::Utf8, Literal::String(s)) => Some(stat.as_str()?.cmp(s.as_str())),
        (ColumnType::Boolean, Literal::Bool(b)) => Some(stat.as_bool()?.cmp(b)),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_storage::chain::{Digest, SnapshotId};
    use amberlog_storage::schema::SchemaColumn;
    use serde_json::json;

    fn schema() -> StreamColumns {
        let cols = [
            (TIMESTAMP_COLUMN, ColumnType::Int64),
            ("status", ColumnType::Int64),
            ("f", ColumnType::Float64),
            ("s", ColumnType::Utf8),
            ("n", ColumnType::Utf8),
            ("long", ColumnType::Utf8),
            ("err", ColumnType::Utf8),
        ];
        StreamColumns::from_columns(
            cols.iter()
                .enumerate()
                .map(|(i, (name, t))| SchemaColumn {
                    name: (*name).into(),
                    column_type: *t,
                    field_id: i as u32 + 1,
                })
                .collect(),
        )
    }

    /// 可判条件对文件是否不可能成立。
    #[test]
    fn pruning_predicate_impossible_table() {
        let file = FileEntry {
            path: "data/2026/08/22/02/x.parquet".into(),
            min_timestamp: 1_787_364_000_000_000,
            max_timestamp: 1_787_364_999_000_000,
            record_count: 10,
            size_bytes: 1,
            content_digest: Digest([0; 32]),
            added_in: SnapshotId(1),
            column_stats: Some(json!({
                "status": {"null_count": 0, "min": 200, "max": 299},
                "f": {"null_count": 2, "min": 0.5, "max": 1.5},
                "s": {"null_count": 0, "min": "b", "max": "d"},
                "n": {"null_count": 10, "min": null, "max": null},
                "long": {"null_count": 0, "min": null, "max": null},
            })),
        };
        let table = [
            ("status = 199", true),
            ("status = 200", false),
            ("status < 200", true),
            ("status <= 200", false),
            ("status > 299", true),
            ("status >= 299", false),
            ("300 <= status", true),
            ("status IN (100, 300)", true),
            ("status IN (100, 250)", false),
            ("status IS NULL", true),
            ("status != 250", false),
            ("status = 100 OR status = 300", false),
            ("NOT (status = 250)", false),
            ("f < 0.5", true),
            ("f IS NULL", false),
            ("s = 'a'", true),
            ("s > 'd'", true),
            ("s LIKE 'z%'", false),
            ("n IS NOT NULL", true),
            ("n IS NULL", false),
            ("err IS NOT NULL", true),
            ("err IS NULL", false),
            ("long = 'x'", false),
            ("s = 'c' AND (status > 0 AND status < 100)", true),
            ("status > 0 AND (s = 'a' OR status = 250)", false),
            ("_timestamp < 1787364000000000", true),
            ("_timestamp >= 1787364999000000", false),
        ];
        let schema = schema();
        for (cond, want) in table {
            let q = crate::sql::parse(&format!("SELECT * FROM app WHERE {cond}")).unwrap();
            let got = impossible(&checks(q.filter.as_ref()), &file, &schema);
            assert_eq!(got, want, "{cond}");
        }

        // 无列统计、形状不符：只有 `_timestamp` 可判
        for stats in [
            None,
            Some(json!("bogus")),
            Some(json!({"status": {"min": 1}})),
        ] {
            let f = FileEntry {
                column_stats: stats.clone(),
                ..file.clone()
            };
            for (cond, want) in [
                ("status = 199", false),
                ("err IS NOT NULL", false),
                ("_timestamp < 1787364000000000", true),
            ] {
                let q = crate::sql::parse(&format!("SELECT * FROM app WHERE {cond}")).unwrap();
                assert_eq!(
                    impossible(&checks(q.filter.as_ref()), &f, &schema),
                    want,
                    "{stats:?} {cond}"
                );
            }
        }
    }

    /// 场景「与时间范围无交集的桶 manifest 不被读取」：第 1 小时那个桶的桶 manifest 一读即报完整性错误，
    /// 只覆盖第 5 小时的查询照常成功，即没有读它。
    #[tokio::test]
    async fn pruning_bucket_manifest_not_read() {
        use std::sync::Arc;

        use amberlog_common::dataset::Dataset;
        use amberlog_storage::chain::{
            BucketManifest, BucketRef, ChainError, ColdSummary, Snapshot, Summary,
        };
        use amberlog_storage::gate::DatasetGates;
        use amberlog_storage::testing::{FakeChain, FakeMemtable};
        use arrow::array::{ArrayRef, Int64Array, TimestampMicrosecondArray};
        use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
        use arrow::record_batch::RecordBatch;

        use crate::request::Limits;
        use crate::search::Engine;
        use crate::testing::FakeFiles;

        const H: i64 = 3_600_000_000;
        let base = 1_787_356_800_000_000; // 2026-08-22T00:00:00Z
        let app = Dataset::new("default", "app");
        let columns = StreamColumns::from_columns(vec![
            SchemaColumn {
                name: TIMESTAMP_COLUMN.into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
            SchemaColumn {
                name: "a".into(),
                column_type: ColumnType::Int64,
                field_id: 2,
            },
        ]);
        let memtable = Arc::new(FakeMemtable::new());
        memtable.add_dataset(&app, columns.clone());
        let chain = Arc::new(FakeChain::new());
        let files = Arc::new(FakeFiles::new());

        let bucket = |hour: i64, manifest: &str, file: &str| {
            let t = base + hour * H;
            let entry = FileEntry {
                path: file.into(),
                min_timestamp: t,
                max_timestamp: t,
                record_count: 1,
                size_bytes: 10,
                content_digest: Digest([0; 32]),
                added_in: SnapshotId(1),
                column_stats: None,
            };
            let r = BucketRef {
                bucket: format!("h{hour}"),
                manifest: manifest.into(),
                content_digest: Digest([0; 32]),
                record_count: 1,
                size_bytes: 10,
                time_min: Some(t),
                time_max: Some(t),
            };
            let m = BucketManifest {
                format_version: 1,
                bucket: format!("h{hour}"),
                files: vec![entry],
                deletions: Vec::new(),
                handovers: Vec::new(),
            };
            (r, m)
        };
        let (r1, _) = bucket(1, "manifest/buckets/h1.json", "data/h1.parquet");
        let (r5, m5) = bucket(5, "manifest/buckets/h5.json", "data/h5.parquet");
        chain.fail_bucket_manifest(
            &app,
            &r1.manifest,
            ChainError::Integrity("bucket manifest of hour 1 was read".into()),
        );
        chain.put_bucket_manifest(&app, &r5.manifest, m5);
        chain.put_snapshot(
            &app,
            Snapshot {
                format_version: 1,
                snapshot_id: SnapshotId(1),
                parent_snapshot_id: None,
                parent_digest: None,
                created_at: 0,
                schema: columns.columns().to_vec(),
                buckets: vec![r1, r5],
                summary: Summary {
                    dataset: "default/app".into(),
                    record_count: 2,
                    size_bytes: 20,
                    time_min: None,
                    time_max: None,
                    cold: ColdSummary::EMPTY,
                },
                retired_segment: None,
                cold_deletion_segment: None,
            },
        );
        chain.set_current(&app, Ok(Some(SnapshotId(1))));
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                TIMESTAMP_COLUMN,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("a", DataType::Int64, true),
        ]));
        let rows: Vec<ArrayRef> = vec![
            Arc::new(TimestampMicrosecondArray::from(vec![base + 5 * H]).with_timezone("UTC")),
            Arc::new(Int64Array::from(vec![5])),
        ];
        files.put(
            "data/h5.parquet",
            vec![RecordBatch::try_new(schema, rows).unwrap()],
        );

        let probe = Arc::clone(&chain);
        let engine = Engine {
            memtable,
            chain,
            gates: Arc::new(DatasetGates::new()),
            files,
            cold: Arc::new(amberlog_storage::testing::FakeColdReader::new()),
            limits: Limits::default(),
            admission: crate::testing::admission(),
            governance: crate::testing::governance().0,
        };
        let body = json!({"query": {
            "sql": "SELECT * FROM app",
            "start_time": base + 5 * H,
            "end_time": base + 6 * H,
        }})
        .to_string();
        let r = engine
            .search("default", body.as_bytes())
            .await
            .unwrap_or_else(|e| panic!("{}", e.message));
        assert_eq!(r.hits.len(), 1);
        assert_eq!(r.scan_files, 1);
        // 只读了 `_current`、快照与第 5 小时的桶 manifest：不靠「一读就报错」发现第 1 小时那份被读
        assert_eq!(probe.reads(), 3);
    }
}
