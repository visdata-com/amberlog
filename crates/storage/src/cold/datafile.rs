//! 冷层数据文件（REQ-AMB-STO-COLD-TABLE「数据文件」「清单条目与列统计」）：把一批记录写成冷层 Parquet，
//! 并由写出的页脚得到清单条目所需的统计（ADR-0042 第 13 条：页脚到 `DataFile` 的构造自写，iceberg-rust 没有公开接口）。
//!
//! - 物理列名取写出时该列在冷层 schema 中的列名，每列带 `PARQUET:field_id`（REQ-AMB-STO-COLD-TABLE-PHYS-NAME、-PHYS-ID）。
//!   输入批次的列按其 `PARQUET:field_id` 元数据对到冷层列，不按列名（热层列名与冷层列名可以不同）。
//! - 文件可以只含冷层 schema 的一部分列（入湖输入中出现过的列），列序取冷层 schema 中的先后。
//! - 统计按字段编号取（ADR-0041 第 3 条）：上下界取各行组统计的合并；字符串上界不精确（写入器截断过）的不写上界
//!   （REQ-AMB-STO-COLD-TABLE-ENTRY-BOUNDS）；`_timestamp` 的上下界是精确的最小值与最大值（-ENTRY-TS）。

use std::collections::HashMap;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use iceberg::spec::{
    DataContentType, DataFile, DataFileBuilder, DataFileFormat, Datum, Literal, PrimitiveType,
    Schema, Struct, Type,
};
use parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;

use super::error::{ColdError, invalid};
use super::layout::{self, ColdLayout};
use crate::bloom::{self, BloomColumns};
use crate::schema::TIMESTAMP_FIELD_ID;

/// 一个写好、待上传的冷层数据文件。
#[derive(Debug, Clone)]
pub struct ColdFile {
    /// 相对表目录的路径（`data/_timestamp_day=<YYYY-MM-DD>/<file_id>.parquet`）。
    pub path: String,
    /// 完整字节（上传的就是这些字节）。
    pub bytes: Bytes,
    pub record_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// 清单条目里的数据文件（`file_path` 为表位置 URI 加 `path`）。
    pub data_file: DataFile,
}

/// 批次中某列的字段编号（`PARQUET:field_id` 元数据）。
fn field_id_of(field: &Field) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

/// 把 `batch`（各列带 `PARQUET:field_id`，全部记录落在同一 UTC 日）按冷层 schema `schema` 写成冷层数据文件。
pub fn write(
    layout: &ColdLayout,
    dataset: &Dataset,
    schema: &Schema,
    batch: &RecordBatch,
    bloom: &BloomColumns,
) -> Result<ColdFile, ColdError> {
    write_parts(layout, dataset, schema, std::slice::from_ref(batch), bloom)
}

/// 同 [`write`]，记录按先后分在 `parts` 的各批次里（schema 相同）：依次写进同一个文件，不先拼成一个批次。
pub fn write_parts(
    layout: &ColdLayout,
    dataset: &Dataset,
    schema: &Schema,
    parts: &[RecordBatch],
    bloom: &BloomColumns,
) -> Result<ColdFile, ColdError> {
    let num_rows: usize = parts.iter().map(RecordBatch::num_rows).sum();
    if num_rows == 0 {
        return Err(invalid("a cold data file needs at least one record"));
    }
    let input = parts[0].schema();
    // 输入各列按字段编号对到冷层列，列序取冷层 schema 中的先后
    let mut by_id: HashMap<i32, usize> = HashMap::new();
    for (i, field) in input.fields().iter().enumerate() {
        let id = field_id_of(field)
            .ok_or_else(|| invalid(format!("column {} has no field id", field.name())))?;
        if schema.field_by_id(id).is_none() {
            return Err(invalid(format!(
                "field id {id} ({}) is not in the cold schema",
                field.name()
            )));
        }
        by_id.insert(id, i);
    }
    let ts_index = *by_id
        .get(&(TIMESTAMP_FIELD_ID as i32))
        .ok_or_else(|| invalid("the batch has no _timestamp column"))?;

    let mut fields = Vec::new();
    let mut picked = Vec::new();
    for cold in schema.as_struct().fields() {
        let Some(&i) = by_id.get(&cold.id) else {
            continue;
        };
        let source = input.field(i).clone();
        fields.push(
            Field::new(&cold.name, source.data_type().clone(), source.is_nullable()).with_metadata(
                HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), cold.id.to_string())]),
            ),
        );
        picked.push(i);
    }
    let out_schema = Arc::new(ArrowSchema::new(fields));

    let (mut min_timestamp, mut max_timestamp) = (i64::MAX, i64::MIN);
    for part in parts.iter().filter(|p| p.num_rows() > 0) {
        if part.schema() != input {
            return Err(invalid("the parts of a cold data file differ in schema"));
        }
        let ts = part
            .column(ts_index)
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .ok_or_else(|| invalid("_timestamp is not a microsecond timestamp column"))?;
        if ts.null_count() > 0 {
            return Err(invalid("_timestamp has nulls"));
        }
        min_timestamp = min_timestamp.min(*ts.values().iter().min().expect("至少一行"));
        max_timestamp = max_timestamp.max(*ts.values().iter().max().expect("至少一行"));
    }
    let day = layout::day_of(min_timestamp);
    if layout::day_of(max_timestamp) != day {
        // REQ-AMB-STO-COLD-TABLE-DATA-DAY：按 UTC 日拆分是调用方的事
        return Err(invalid(
            "records of a cold data file span more than one UTC day",
        ));
    }

    // 可写 bloom 的列按字段编号对到冷层物理列名（REQ-AMB-QRY-EQUALITY-BLOOM-TERM-DECLARED）
    let bloomed: Vec<&str> = schema
        .as_struct()
        .fields()
        .iter()
        .filter(|cold| by_id.contains_key(&cold.id))
        .filter(|cold| u32::try_from(cold.id).is_ok_and(|id| bloom.contains(id)))
        .map(|cold| cold.name.as_str())
        .collect();
    let props = bloom::with_blooms(
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())),
        bloomed,
        num_rows,
    )
    .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), Arc::clone(&out_schema), Some(props)).map_err(invalid)?;
    for part in parts.iter().filter(|p| p.num_rows() > 0) {
        let columns = picked.iter().map(|&i| Arc::clone(part.column(i))).collect();
        let out = RecordBatch::try_new(Arc::clone(&out_schema), columns).map_err(invalid)?;
        writer.write(&out).map_err(invalid)?;
    }
    let bytes = Bytes::from(writer.into_inner().map_err(invalid)?);

    let path = layout::new_data_file_path(day);
    let data_file = data_file_from_footer(layout.object_uri(dataset, &path), &bytes, schema, day)?;
    Ok(ColdFile {
        path,
        record_count: num_rows as u64,
        min_timestamp,
        max_timestamp,
        bytes,
        data_file,
    })
}

/// 由 Parquet 文件的完整字节得到清单条目里的数据文件：大小、记录数、按字段编号的列大小、值数、空值数与上下界、
/// 行组起点，分区值为 UTC 日 `day`。
pub fn data_file_from_footer(
    uri: String,
    bytes: &Bytes,
    schema: &Schema,
    day: i32,
) -> Result<DataFile, ColdError> {
    let meta = ParquetMetaDataReader::new()
        .parse_and_finish(bytes)
        .map_err(invalid)?;
    let stats = FooterStats::collect(&meta, schema)?;
    let split_offsets = meta
        .row_groups()
        .iter()
        .map(|rg| rg.file_offset())
        .collect::<Option<Vec<i64>>>();
    DataFileBuilder::default()
        .content(DataContentType::Data)
        .file_path(uri)
        .file_format(DataFileFormat::Parquet)
        .partition(Struct::from_iter([Some(Literal::date(day))]))
        .partition_spec_id(0)
        .record_count(meta.file_metadata().num_rows() as u64)
        .file_size_in_bytes(bytes.len() as u64)
        .column_sizes(stats.column_sizes)
        .value_counts(stats.value_counts)
        .null_value_counts(stats.null_counts)
        .lower_bounds(stats.lower)
        .upper_bounds(stats.upper)
        .split_offsets(split_offsets)
        .build()
        .map_err(invalid)
}

#[derive(Default)]
struct FooterStats {
    column_sizes: HashMap<i32, u64>,
    value_counts: HashMap<i32, u64>,
    null_counts: HashMap<i32, u64>,
    lower: HashMap<i32, Datum>,
    upper: HashMap<i32, Datum>,
}

/// 一列在各行组上合并中的上下界：某行组缺统计即整列不写该界。
struct Bounds {
    lower: Option<Option<Datum>>,
    upper: Option<Option<Datum>>,
}

impl FooterStats {
    fn collect(meta: &ParquetMetaData, schema: &Schema) -> Result<FooterStats, ColdError> {
        let descr = meta.file_metadata().schema_descr();
        let mut out = FooterStats::default();
        let mut bounds: HashMap<i32, Bounds> = HashMap::new();
        for rg in meta.row_groups() {
            for (i, chunk) in rg.columns().iter().enumerate() {
                let info = descr.column(i).self_type().get_basic_info().clone();
                if !info.has_id() {
                    return Err(invalid(format!("column {} has no field id", info.name())));
                }
                let id = info.id();
                let field_type = match schema.field_by_id(id).map(|f| f.field_type.as_ref()) {
                    Some(Type::Primitive(p)) => p.clone(),
                    _ => return Err(invalid(format!("field id {id} is not a cold column"))),
                };
                *out.column_sizes.entry(id).or_default() += chunk.compressed_size() as u64;
                *out.value_counts.entry(id).or_default() += chunk.num_values() as u64;
                let stats = chunk.statistics();
                let nulls = stats
                    .and_then(Statistics::null_count_opt)
                    .ok_or_else(|| invalid(format!("column {} has no null count", info.name())))?;
                *out.null_counts.entry(id).or_default() += nulls;

                let (lo, hi) = match stats {
                    Some(s) => datum_bounds(s, &field_type),
                    None => (None, None),
                };
                let b = bounds.entry(id).or_insert(Bounds {
                    lower: Some(None),
                    upper: Some(None),
                });
                // 行组全为空值时没有上下界，不影响合并
                let all_null = nulls == chunk.num_values() as u64;
                if !all_null {
                    b.lower = merge(b.lower.take(), lo, |new, old| new < old);
                    b.upper = merge(b.upper.take(), hi, |new, old| new > old);
                }
            }
        }
        for (id, b) in bounds {
            if let Some(Some(lo)) = b.lower {
                out.lower.insert(id, lo);
            }
            if let Some(Some(hi)) = b.upper {
                out.upper.insert(id, hi);
            }
        }
        Ok(out)
    }
}

/// 合并一个行组的界 `new` 进 `acc`：`acc` 为 `None` 表示已有行组缺该界（整列不写）；
/// `Some(None)` 表示尚未见到界；`better(new, old)` 为真时取 `new`。
fn merge(
    acc: Option<Option<Datum>>,
    new: Option<Datum>,
    better: impl Fn(&Datum, &Datum) -> bool,
) -> Option<Option<Datum>> {
    let acc = acc?;
    let new = new?;
    Some(Some(match acc {
        Some(old) if !better(&new, &old) => old,
        _ => new,
    }))
}

/// 一个行组某列统计的有效上下界；字符串上界不精确时不给上界。
fn datum_bounds(stats: &Statistics, field_type: &PrimitiveType) -> (Option<Datum>, Option<Datum>) {
    match (stats, field_type) {
        (Statistics::Int64(s), PrimitiveType::Timestamptz) => (
            s.min_opt().map(|v| Datum::timestamptz_micros(*v)),
            s.max_opt().map(|v| Datum::timestamptz_micros(*v)),
        ),
        (Statistics::Int64(s), PrimitiveType::Long) => (
            s.min_opt().map(|v| Datum::long(*v)),
            s.max_opt().map(|v| Datum::long(*v)),
        ),
        (Statistics::Double(s), PrimitiveType::Double) => (
            s.min_opt().map(|v| Datum::double(*v)),
            s.max_opt().map(|v| Datum::double(*v)),
        ),
        (Statistics::Boolean(s), PrimitiveType::Boolean) => (
            s.min_opt().map(|v| Datum::bool(*v)),
            s.max_opt().map(|v| Datum::bool(*v)),
        ),
        (Statistics::ByteArray(s), PrimitiveType::String) => {
            let text = |v: &parquet::data_type::ByteArray| {
                std::str::from_utf8(v.data()).ok().map(Datum::string)
            };
            // 截断过的下界是原值的前缀，仍不大于最小值；截断过的上界不写
            let lower = s.min_opt().and_then(text);
            let upper = if s.max_is_exact() {
                s.max_opt().and_then(text)
            } else {
                None
            };
            (lower, upper)
        }
        _ => (None, None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cold::names;
    use crate::schema::{SchemaColumn, StreamColumns};
    use crate::store::Location;
    use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
    use arrow::array::{Int64Array, StringArray};
    use object_store::path::Path;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// 2026-08-22T10:00:00Z
    const T: i64 = 1_787_392_800_000_000;

    fn layout() -> ColdLayout {
        ColdLayout::new(
            &Location::S3 {
                bucket: "b".into(),
                path: vec![],
            },
            Path::from("amberlog"),
        )
    }

    fn hot() -> StreamColumns {
        StreamColumns::from_columns(vec![
            SchemaColumn {
                name: TIMESTAMP_COLUMN.into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
            SchemaColumn {
                name: "Foo".into(),
                column_type: ColumnType::Utf8,
                field_id: 2,
            },
            SchemaColumn {
                name: "foo".into(),
                column_type: ColumnType::Utf8,
                field_id: 3,
            },
            SchemaColumn {
                name: "n".into(),
                column_type: ColumnType::Int64,
                field_id: 4,
            },
        ])
    }

    fn cold_schema() -> Schema {
        Schema::builder()
            .with_fields(names::evolve(None, hot().columns()))
            .build()
            .unwrap()
    }

    /// 热层写法的批次（列名取热层列名，带字段编号），不含 `n`。
    fn batch(foo: Vec<Option<&str>>) -> RecordBatch {
        let hot = hot();
        let ts: Vec<i64> = (0..foo.len() as i64).map(|i| T + i).collect();
        let fields = ["_timestamp", "foo", "Foo"]
            .map(|n| hot.get(n).unwrap().arrow_field())
            .to_vec();
        RecordBatch::try_new(
            Arc::new(ArrowSchema::new(fields)),
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts).with_timezone("UTC")),
                Arc::new(StringArray::from(foo.clone())),
                Arc::new(StringArray::from(foo)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn physical_names_are_cold_names_and_columns_follow_schema_order() {
        let f = write(
            &layout(),
            &Dataset::new("default", "case"),
            &cold_schema(),
            &batch(vec![Some("a"), None]),
            &BloomColumns::none(),
        )
        .unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(f.bytes.clone()).unwrap();
        let descr = reader.metadata().file_metadata().schema_descr();
        let got: Vec<_> = descr
            .columns()
            .iter()
            .map(|c| {
                let info = c.self_type().get_basic_info();
                (info.id(), info.name().to_owned())
            })
            .collect();
        assert_eq!(
            got,
            [
                (1, "_timestamp".to_owned()),
                (2, "Foo".to_owned()),
                (3, "foo__2".to_owned())
            ]
        );
        assert!(
            f.path.starts_with("data/_timestamp_day=2026-08-22/"),
            "{}",
            f.path
        );
        assert_eq!(
            f.data_file.file_path(),
            format!("s3://b/amberlog/_iceberg/default/case/{}", f.path)
        );
    }

    #[test]
    fn entry_stats_by_field_id() {
        let f = write(
            &layout(),
            &Dataset::new("default", "app"),
            &cold_schema(),
            &batch(vec![Some("b"), None, Some("a"), None, None]),
            &BloomColumns::none(),
        )
        .unwrap();
        let d = &f.data_file;
        assert_eq!(d.record_count(), 5);
        assert_eq!(d.file_size_in_bytes(), f.bytes.len() as u64);
        assert_eq!(d.null_value_counts().get(&2), Some(&3));
        assert_eq!(d.null_value_counts().get(&1), Some(&0));
        assert_eq!(
            d.lower_bounds().get(&1),
            Some(&Datum::timestamptz_micros(T))
        );
        assert_eq!(
            d.upper_bounds().get(&1),
            Some(&Datum::timestamptz_micros(T + 4))
        );
        assert_eq!(d.lower_bounds().get(&3), Some(&Datum::string("a")));
        assert_eq!(d.upper_bounds().get(&3), Some(&Datum::string("b")));
        // 文件不含 `n`：没有它的统计
        assert!(!d.null_value_counts().contains_key(&4));
        assert_eq!(
            d.partition(),
            &Struct::from_iter([Some(Literal::date(layout::day_of(T)))])
        );
        assert_eq!(f.min_timestamp, T);
        assert_eq!(f.max_timestamp, T + 4);
    }

    #[test]
    fn long_string_upper_bound_not_truncated() {
        // 场景「长字符串的上界不以截断值写出」
        let lo = "a".repeat(1000);
        let hi = "z".repeat(1000);
        let f = write(
            &layout(),
            &Dataset::new("default", "app"),
            &cold_schema(),
            &batch(vec![Some(&hi), Some(&lo)]),
            &BloomColumns::none(),
        )
        .unwrap();
        let d = &f.data_file;
        if let Some(Datum { .. }) = d.upper_bounds().get(&2) {
            let upper = d.upper_bounds()[&2].to_string();
            assert!(upper.as_str() >= hi.as_str(), "上界 {upper} 小于最大值");
        }
        if let Some(lower) = d.lower_bounds().get(&2) {
            assert!(lower.to_string().as_str() <= lo.as_str());
        }
    }

    #[test]
    fn rejects_records_spanning_two_days() {
        let hot = hot();
        let b = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                hot.get("_timestamp").unwrap().arrow_field(),
            ])),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![T, T + 86_400_000_000]).with_timezone("UTC"),
            )],
        )
        .unwrap();
        assert!(matches!(
            write(
                &layout(),
                &Dataset::new("default", "app"),
                &cold_schema(),
                &b,
                &BloomColumns::none()
            ),
            Err(ColdError::Invalid(_))
        ));
        let _ = Int64Array::from(vec![1]);
    }
}
