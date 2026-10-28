//! 物化（Flush）中与触发无关的一半：把一组已确认记录的批次按 UTC 小时桶拆分，写成带字段 id 的 Parquet
//! （REQ-AMB-STO-HOT-LAYOUT-V2「数据文件」「Parquet 列与字段 id」），并算出桶 manifest 条目所需的合计、极值与摘要
//! （REQ-AMB-STO-AUDIT-INVARIANTS：摘要取实际上传的完整字节，先算后传）。
//!
//! flush 的触发、冻结可见集的代与上传属物化的另一半，在 [`crate::flusher`]。
//!
//! 封口合并的输出用同一套写法，但边读边写（[`write_merged`]）：不把全部输入解码、拼成一个批次。

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use amberlog_common::schema::TIMESTAMP_COLUMN;
use arrow::array::{ArrayRef, TimestampMicrosecondArray, UInt32Array, new_null_array};
use arrow::compute::{concat_batches, take};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::bloom::{self, BloomColumns};
use crate::chain::{Digest, FileEntry, SnapshotId};
use crate::layout::{self, HourBucket};
use crate::schema::{SchemaColumn, StreamColumns};
use crate::stats;

/// 一个待上传的数据文件。
#[derive(Debug, Clone)]
pub struct DataFile {
    pub bucket: HourBucket,
    /// 文件含的列（带字段 id），按字段 id 升序：提交时据此把新列与引用它的文件并入同一快照的 `schema`。
    pub columns: Vec<SchemaColumn>,
    /// 相对数据集前缀的路径。
    pub path: String,
    /// 完整字节（上传的就是这些字节）。
    pub bytes: Bytes,
    pub digest: Digest,
    pub record_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    /// 列统计（REQ-AMB-QRY-COLUMN-PRUNING），与写出的字节同一遍算出。
    pub column_stats: serde_json::Value,
}

impl DataFile {
    /// 该文件在首次引用它的快照 `added_in` 的桶 manifest 中的条目。
    pub fn entry(&self, added_in: SnapshotId) -> FileEntry {
        FileEntry {
            path: self.path.clone(),
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            record_count: self.record_count,
            size_bytes: self.bytes.len() as u64,
            content_digest: self.digest,
            added_in,
            column_stats: Some(self.column_stats.clone()),
        }
    }
}

/// 物化失败：批次与 stream schema 不符（调用方的缺陷），或 Parquet 写入器报错。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlushError(pub String);

impl fmt::Display for FlushError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for FlushError {}

fn err(what: impl fmt::Display) -> FlushError {
    FlushError(what.to_string())
}

/// 把 `batches` 中的全部行按 `_timestamp` 所在的 UTC 小时桶拆成数据文件，按小时桶升序返回，每桶一个文件。
///
/// 每个批次的列都须是 `columns` 中的列、写法同 [`crate::schema::SchemaColumn::arrow_field`]（带字段 id 元数据）。
/// 一个文件含其各行所在批次的列之并，按字段 id 升序；批次里没有的列以 null 补齐。`bloom` 中本文件含有的列写等值
/// bloom（REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS；flush 传 [`BloomColumns::none`]，WRITE-ONLY）。
pub fn write_data_files(
    columns: &StreamColumns,
    batches: &[RecordBatch],
    bloom: &BloomColumns,
) -> Result<Vec<DataFile>, FlushError> {
    // 小时桶 → [(批次序号, 该批次落在该桶的行号)]
    let mut rows: BTreeMap<HourBucket, Vec<(usize, Vec<u32>)>> = BTreeMap::new();
    for (i, batch) in batches.iter().enumerate() {
        let ts = timestamps(batch)?;
        let mut by_bucket: BTreeMap<HourBucket, Vec<u32>> = BTreeMap::new();
        for (row, value) in ts.values().iter().enumerate() {
            by_bucket
                .entry(HourBucket::of(*value))
                .or_default()
                .push(row as u32);
        }
        for (bucket, idx) in by_bucket {
            rows.entry(bucket).or_default().push((i, idx));
        }
    }
    rows.into_iter()
        .map(|(bucket, parts)| write_bucket(columns, batches, bucket, &parts, bloom))
        .collect()
}

/// 合并输出的边读边写：把同一小时桶 `bucket` 的若干个数据文件（`inputs`，完整字节）的全部行依次写成一个数据文件，
/// 结果与把这些文件全部解码、拼成一个批次再 [`write_data_files`] 相同（行的先后、列、列统计、极值都相同）。
///
/// 只先读各输入的尾部元数据（列与行数），之后逐个输入、逐批解码，对齐到输出的列，写进同一个 Parquet 写入器，写完
/// 一批丢一批；一个输入读完即释放它的字节。内存因此约为输入与输出的压缩字节加上写入器正在编码的一个行组，不随解码后
/// 的大小增长。任一行不在 `bucket` 内即报错。
pub fn write_merged(
    columns: &StreamColumns,
    bucket: HourBucket,
    inputs: Vec<Bytes>,
    bloom: &BloomColumns,
) -> Result<DataFile, FlushError> {
    let mut readers = Vec::with_capacity(inputs.len());
    let mut file_columns: Vec<&SchemaColumn> = Vec::new();
    let mut rows = 0usize;
    for input in inputs {
        let builder = ParquetRecordBatchReaderBuilder::try_new(input).map_err(err)?;
        for field in builder.schema().fields() {
            let column = columns.get(field.name()).ok_or_else(|| {
                err(format!(
                    "column {} is not in the stream schema",
                    field.name()
                ))
            })?;
            if !file_columns.contains(&column) {
                file_columns.push(column);
            }
        }
        rows += usize::try_from(builder.metadata().file_metadata().num_rows()).map_err(err)?;
        readers.push(builder);
    }
    file_columns.sort_by_key(|c| c.field_id);
    let schema = Arc::new(Schema::new(
        file_columns
            .iter()
            .map(|c| c.arrow_field())
            .collect::<Vec<_>>(),
    ));
    let props = bloom::with_blooms(
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())),
        file_columns
            .iter()
            .filter(|c| bloom.contains(c.field_id))
            .map(|c| c.name.as_str()),
        rows,
    )
    .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props)).map_err(err)?;
    let mut stats = stats::StatsAccumulator::new(&schema);
    let mut range: Option<(i64, i64)> = None;
    let mut record_count = 0u64;
    for builder in readers {
        let reader = builder
            .with_batch_size(MERGE_BATCH_ROWS)
            .build()
            .map_err(err)?;
        for batch in reader {
            let batch = batch.map_err(err)?;
            let aligned = align(&schema, &batch)?;
            for &t in timestamps(&aligned)?.values().iter() {
                if HourBucket::of(t) != bucket {
                    return Err(err(format!("row at {t} is outside bucket {bucket}")));
                }
                range = Some(range.map_or((t, t), |(lo, hi)| (lo.min(t), hi.max(t))));
            }
            record_count += aligned.num_rows() as u64;
            stats.update(&aligned);
            writer.write(&aligned).map_err(err)?;
        }
    }
    let (min_timestamp, max_timestamp) = range.ok_or_else(|| err("merge inputs have no rows"))?;
    let bytes = Bytes::from(writer.into_inner().map_err(err)?);
    Ok(DataFile {
        bucket,
        columns: file_columns.into_iter().cloned().collect(),
        path: layout::data_file_path(bucket, &layout::new_object_id()),
        digest: Digest::of(&bytes),
        bytes,
        record_count,
        min_timestamp,
        max_timestamp,
        column_stats: stats.finish(),
    })
}

/// 合并边读边写时每批解码的行数。
const MERGE_BATCH_ROWS: usize = 8192;

/// 把 `batch` 的列按 `schema` 的列序排好，`batch` 里没有的列以 null 补齐；同名列的类型须与 `schema` 相同。
fn align(schema: &Arc<Schema>, batch: &RecordBatch) -> Result<RecordBatch, FlushError> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(array) if array.data_type() == field.data_type() => Ok(Arc::clone(array)),
            Some(array) => Err(err(format!(
                "column {} is {} in the batch, {} in the stream schema",
                field.name(),
                array.data_type(),
                field.data_type()
            ))),
            None => Ok(new_null_array(field.data_type(), batch.num_rows())),
        })
        .collect::<Result<Vec<ArrayRef>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), arrays).map_err(err)
}

fn timestamps(batch: &RecordBatch) -> Result<&TimestampMicrosecondArray, FlushError> {
    let column = batch
        .column_by_name(TIMESTAMP_COLUMN)
        .ok_or_else(|| err("batch has no _timestamp column"))?;
    if column.null_count() > 0 {
        return Err(err("_timestamp contains null"));
    }
    column
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| err("_timestamp is not Timestamp(Microsecond)"))
}

fn write_bucket(
    columns: &StreamColumns,
    batches: &[RecordBatch],
    bucket: HourBucket,
    parts: &[(usize, Vec<u32>)],
    bloom: &BloomColumns,
) -> Result<DataFile, FlushError> {
    // 文件的列：各批次列之并，按字段 id 升序
    let mut file_columns = Vec::new();
    for (i, _) in parts {
        for field in batches[*i].schema().fields() {
            let column = columns.get(field.name()).ok_or_else(|| {
                err(format!(
                    "column {} is not in the stream schema",
                    field.name()
                ))
            })?;
            if !file_columns.contains(&column) {
                file_columns.push(column);
            }
        }
    }
    file_columns.sort_by_key(|c| c.field_id);
    let schema = Arc::new(Schema::new(
        file_columns
            .iter()
            .map(|c| c.arrow_field())
            .collect::<Vec<_>>(),
    ));

    let mut aligned = Vec::with_capacity(parts.len());
    for (i, idx) in parts {
        let batch = &batches[*i];
        let idx = UInt32Array::from(idx.clone());
        let arrays = schema
            .fields()
            .iter()
            .map(|field| match batch.column_by_name(field.name()) {
                Some(array) => {
                    if array.data_type() != field.data_type() {
                        return Err(err(format!(
                            "column {} is {} in the batch, {} in the stream schema",
                            field.name(),
                            array.data_type(),
                            field.data_type()
                        )));
                    }
                    take(array.as_ref(), &idx, None).map_err(err)
                }
                None => Ok(new_null_array(field.data_type(), idx.len())),
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        aligned.push(RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(err)?);
    }
    let batch = concat_batches(&schema, &aligned).map_err(err)?;

    let ts = timestamps(&batch)?;
    let min_timestamp = *ts.values().iter().min().expect("每个桶至少一行");
    let max_timestamp = *ts.values().iter().max().expect("每个桶至少一行");
    debug_assert!(
        HourBucket::of(min_timestamp) == bucket && HourBucket::of(max_timestamp) == bucket
    );

    let props = bloom::with_blooms(
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())),
        file_columns
            .iter()
            .filter(|c| bloom.contains(c.field_id))
            .map(|c| c.name.as_str()),
        batch.num_rows(),
    )
    .build();
    let mut writer =
        ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props)).map_err(err)?;
    writer.write(&batch).map_err(err)?;
    let bytes = Bytes::from(writer.into_inner().map_err(err)?);

    Ok(DataFile {
        bucket,
        columns: file_columns.into_iter().cloned().collect(),
        path: layout::data_file_path(bucket, &layout::new_object_id()),
        digest: Digest::of(&bytes),
        bytes,
        record_count: batch.num_rows() as u64,
        min_timestamp,
        max_timestamp,
        column_stats: stats::column_stats(&batch),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_common::schema::ColumnType;
    use arrow::array::{Array, Int64Array, StringArray};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};

    /// 2026-08-22T10:59:59Z 与 11:00:00Z
    const T10: i64 = 1_787_396_399_000_000;
    const T11: i64 = 1_787_396_400_000_000;

    fn columns() -> StreamColumns {
        StreamColumns::from_columns(vec![
            SchemaColumn {
                name: TIMESTAMP_COLUMN.into(),
                column_type: ColumnType::Int64,
                field_id: 1,
            },
            SchemaColumn {
                name: "host".into(),
                column_type: ColumnType::Utf8,
                field_id: 2,
            },
            SchemaColumn {
                name: "n".into(),
                column_type: ColumnType::Int64,
                field_id: 3,
            },
        ])
    }

    fn batch(names: &[&str], ts: Vec<i64>) -> RecordBatch {
        let cols = columns();
        let fields: Vec<_> = names
            .iter()
            .map(|n| cols.get(n).unwrap().arrow_field())
            .collect();
        let len = ts.len();
        let arrays: Vec<ArrayRef> = names
            .iter()
            .map(|n| -> ArrayRef {
                match *n {
                    TIMESTAMP_COLUMN => {
                        Arc::new(TimestampMicrosecondArray::from(ts.clone()).with_timezone("UTC"))
                    }
                    "host" => Arc::new(StringArray::from(vec!["h"; len])),
                    _ => Arc::new(Int64Array::from(vec![7; len])),
                }
            })
            .collect();
        RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap()
    }

    #[test]
    fn splits_by_hour_and_writes_field_ids() {
        let batches = [
            batch(&[TIMESTAMP_COLUMN, "host"], vec![T10, T11]),
            batch(&[TIMESTAMP_COLUMN, "n"], vec![T11 + 1]),
        ];
        let files = write_data_files(&columns(), &batches, &BloomColumns::none()).unwrap();
        assert_eq!(files.len(), 2);
        let (f10, f11) = (&files[0], &files[1]);
        assert_eq!(f10.bucket.to_string(), "2026082210");
        assert!(f10.path.starts_with("data/2026/08/22/10/") && f10.path.ends_with(".parquet"));
        assert!(f11.path.starts_with("data/2026/08/22/11/"));
        assert_eq!(
            (f10.record_count, f10.min_timestamp, f10.max_timestamp),
            (1, T10, T10)
        );
        assert_eq!(
            (f11.record_count, f11.min_timestamp, f11.max_timestamp),
            (2, T11, T11 + 1)
        );
        assert_eq!(f11.digest, Digest::of(&f11.bytes));
        let names: Vec<&str> = f10.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(names, ["_timestamp", "host"]);
        let e = f11.entry(SnapshotId(4));
        assert_eq!(
            (e.size_bytes, e.added_in),
            (f11.bytes.len() as u64, SnapshotId(4))
        );

        // 11 点的文件含两个批次的列之并，缺的列补 null
        let reader = ParquetRecordBatchReaderBuilder::try_new(f11.bytes.clone()).unwrap();
        let descr = reader.parquet_schema().clone();
        let names: Vec<_> = (0..descr.num_columns())
            .map(|i| descr.column(i).name().to_owned())
            .collect();
        assert_eq!(names, ["_timestamp", "host", "n"]);
        let ts = descr.column(0);
        assert_eq!(ts.physical_type(), PhysicalType::INT64);
        assert_eq!(
            ts.logical_type_ref(),
            Some(&LogicalType::timestamp(true, TimeUnit::MICROS))
        );
        assert_eq!(
            ts.self_type().get_basic_info().repetition(),
            Repetition::REQUIRED
        );
        let fields = descr.root_schema().get_fields();
        let ids: Vec<i32> = fields.iter().map(|f| f.get_basic_info().id()).collect();
        assert_eq!(ids, [1, 2, 3]);
        assert_eq!(
            fields[1].get_basic_info().repetition(),
            Repetition::OPTIONAL
        );
        let rows = reader.build().unwrap().next().unwrap().unwrap();
        assert_eq!(rows.num_rows(), 2);
        assert_eq!(rows.column(1).null_count(), 1, "host 在第二个批次里没有");
        assert_eq!(rows.column(2).null_count(), 1, "n 在第一个批次里没有");
    }

    #[test]
    fn rejects_columns_outside_the_schema() {
        let mut b = batch(&[TIMESTAMP_COLUMN], vec![T10]);
        let extra = Arc::new(Schema::new(vec![
            b.schema().field(0).clone(),
            arrow::datatypes::Field::new("zz", arrow::datatypes::DataType::Int64, true),
        ]));
        b = RecordBatch::try_new(
            extra,
            vec![b.column(0).clone(), Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        assert!(write_data_files(&columns(), &[b], &BloomColumns::none()).is_err());
        assert!(
            write_data_files(&columns(), &[], &BloomColumns::none())
                .unwrap()
                .is_empty()
        );
    }

    fn decode(bytes: &Bytes) -> Vec<RecordBatch> {
        ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .unwrap()
            .build()
            .unwrap()
            .map(Result::unwrap)
            .collect()
    }

    /// 边读边写的合并输出与「全部解码、拼成一个批次再写」相同：列、行与先后、行数、极值、列统计。
    #[test]
    fn merged_output_equals_concatenated_write() {
        let cols = columns();
        let none = BloomColumns::none();
        let one = |names: &[&str], ts: Vec<i64>| {
            let mut files = write_data_files(&cols, &[batch(names, ts)], &none).unwrap();
            assert_eq!(files.len(), 1);
            files.remove(0)
        };
        // 三个输入的列各不相同，第二个输入的行跨过多个解码批次
        let many: Vec<i64> = (0..20_000).map(|i| T10 - 3_000_000_000 + i).collect();
        let inputs = [
            one(&[TIMESTAMP_COLUMN, "host"], vec![T10, T10 - 5]),
            one(&[TIMESTAMP_COLUMN, "n"], many),
            one(&[TIMESTAMP_COLUMN, "host", "n"], vec![T10 - 7, T10 - 1]),
        ];
        let decoded: Vec<RecordBatch> = inputs.iter().flat_map(|f| decode(&f.bytes)).collect();
        let mut want = write_data_files(&cols, &decoded, &none).unwrap();
        assert_eq!(want.len(), 1);
        let want = want.remove(0);

        let bucket = inputs[0].bucket;
        let got = write_merged(
            &cols,
            bucket,
            inputs.iter().map(|f| f.bytes.clone()).collect(),
            &none,
        )
        .unwrap();
        assert_eq!(got.bucket, want.bucket);
        assert_eq!(got.columns, want.columns);
        assert_eq!(got.record_count, want.record_count);
        assert_eq!(
            (got.min_timestamp, got.max_timestamp),
            (want.min_timestamp, want.max_timestamp)
        );
        assert_eq!(got.column_stats, want.column_stats);
        let rows = |f: &DataFile| {
            let batches = decode(&f.bytes);
            concat_batches(&batches[0].schema(), &batches).unwrap()
        };
        assert_eq!(rows(&got), rows(&want));

        // 不在该桶内的行即报错
        let other = one(&[TIMESTAMP_COLUMN], vec![T11]);
        assert!(write_merged(&cols, bucket, vec![other.bytes], &none).is_err());
    }
}
