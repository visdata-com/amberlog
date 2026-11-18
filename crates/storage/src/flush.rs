//! 物化（Flush）中与触发无关的一半：把一组已确认记录的批次按 UTC 小时桶拆分，写成带字段 id 的 Parquet
//! （REQ-AMB-STO-HOT-LAYOUT-V2「数据文件」「Parquet 列与字段 id」），并算出桶 manifest 条目所需的合计、极值与摘要
//! （REQ-AMB-STO-AUDIT-INVARIANTS：摘要取实际上传的完整字节，先算后传）。
//!
//! flush 的触发、冻结可见集的代与上传属物化的另一半，在 [`crate::flusher`]。
//!
//! 封口合并的输出用同一套写法，但边读边写（[`write_merged`]）：不把全部输入解码、拼成一个批次。
//!
//! 行组按时间段切：一个时间段为桶内与整 5 分钟对齐的一段（[`SPAN_US`]）。flush 的文件每个时间段一个行组；合并输出
//! 按时间段分多遍写出，行组的 `_timestamp` 上下界互不重叠。按时间查询时与查询范围不相交的行组整组跳过（[`crate::scan`]）。
//! 行组怎么切不是对外可观察的表面（REQ-AMB-STO-HOT-LAYOUT-V8 不规定行组），行在文件内的先后也不承诺。

use std::collections::BTreeMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use amberlog_common::schema::TIMESTAMP_COLUMN;
use arrow::array::{
    ArrayRef, BooleanArray, TimestampMicrosecondArray, UInt32Array, new_null_array,
};
use arrow::compute::{filter_record_batch, take};
use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::arrow_reader::{
    ArrowReaderMetadata, ArrowReaderOptions, ParquetRecordBatchReaderBuilder,
};
use parquet::arrow::{ArrowWriter, ProjectionMask};
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::{DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterProperties};

use crate::bloom::{self, BloomColumns};
use crate::chain::{Digest, FileEntry, SnapshotId};
use crate::layout::{self, HourBucket};
use crate::scan;
use crate::schema::{SchemaColumn, StreamColumns};
use crate::stats::StatsAccumulator;

/// 一个时间段的长度（微秒）：行组按桶内与整 5 分钟对齐的时间段切。
///
/// 越短，非整段对齐的查询多读得越少，但迟到记录分出的小行组越多（每个行组在文件尾部多一份元数据）。代码常量，
/// 不进配置（配置项是对外表面）。
pub(crate) const SPAN_US: i64 = 300 * 1_000_000;

/// `t` 所在时间段的序号（桶起点 `start` 起算）。
fn span_of(start: i64, t: i64) -> i64 {
    (t - start).div_euclid(SPAN_US)
}

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
///
/// 每个文件按时间段（[`SPAN_US`]）从早到晚写，每个时间段一个行组（超过写入器的行组行数上限时由写入器再切），段内
/// 行的先后同输入。
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

/// 合并输出的边读边写：把同一小时桶 `bucket` 的若干个数据文件（`inputs`，完整字节）的全部行写成一个数据文件，
/// 结果作为行的多重集与把这些文件全部解码、拼成一个批次再 [`write_data_files`] 相同（列、行数、极值、列统计都相同，
/// 行的先后不同）。
///
/// 按时间段分多遍写出（[`Merge::plan`]）：先只解码各输入的 `_timestamp`，定出若干互不相交的时间范围；再逐个范围、
/// 逐个输入只读与该范围相交的行组，逐批解码，留下 `_timestamp` 落在该范围内的行，对齐到输出的列写进同一个写入器，
/// 一个范围写完结束一个行组。各行组的 `_timestamp` 上下界因此互不重叠。输入的行组覆盖得宽（升级前写的文件一个行组
/// 覆盖整小时）、多遍要重复解码太多时，退回逐个输入一遍写完。
///
/// 每一遍都只用 `inputs` 的字节（REQ-AMB-STO-SEAL-COMPACTION-V4：写出合并输出只用核对过的字节）。一次只解码一个
/// 输入的一批；一个输入最后一次用到之后即释放它的字节。内存因此约为输入与输出的压缩字节加上写入器正在编码的一个
/// 行组，不随解码后的大小增长。任一行不在 `bucket` 内、或写出的行数与输入的行数之和不等，即报错。
pub fn write_merged(
    columns: &StreamColumns,
    bucket: HourBucket,
    inputs: Vec<Bytes>,
    bloom: &BloomColumns,
) -> Result<DataFile, FlushError> {
    let merge = Merge::open(columns, bucket, inputs, bloom)?;
    let plan = merge.plan()?;
    merge.write(plan)
}

/// 合并边读边写时每批解码的行数。
const MERGE_BATCH_ROWS: usize = 8192;

/// 多遍要解码的行数超过全部行数的这个倍数（分子、分母）时退回一遍写完。
const MAX_DECODE_RATIO: (usize, usize) = (3, 2);

/// 合并输出怎么写。
#[derive(Debug)]
enum Plan {
    /// 逐个输入一遍写完。
    OnePass,
    /// 按这些互不相交、从早到晚的时间范围（UTC 微秒，左闭右开）各写一遍，每个范围结束一个行组。
    Ranges(Vec<Range<i64>>),
}

/// 一次合并的输入与输出的写入器。
struct Merge {
    bucket: HourBucket,
    /// 各输入的完整字节；最后一次用到之后置为 `None`。
    inputs: Vec<Option<Bytes>>,
    /// 各输入的尾部元数据与 `_timestamp` 的叶子列序号。
    metas: Vec<(ArrowReaderMetadata, usize)>,
    /// 全部输入的行数之和。
    rows: usize,
    file_columns: Vec<SchemaColumn>,
    schema: Arc<Schema>,
    writer: ArrowWriter<Vec<u8>>,
    stats: StatsAccumulator,
    range: Option<(i64, i64)>,
    record_count: usize,
}

impl Merge {
    /// 只读各输入的尾部元数据（列、行数、行组统计），备好输出的写入器。
    fn open(
        columns: &StreamColumns,
        bucket: HourBucket,
        inputs: Vec<Bytes>,
        bloom: &BloomColumns,
    ) -> Result<Merge, FlushError> {
        let mut metas = Vec::with_capacity(inputs.len());
        let mut file_columns: Vec<&SchemaColumn> = Vec::new();
        let mut rows = 0usize;
        for input in &inputs {
            let meta =
                ArrowReaderMetadata::load(input, ArrowReaderOptions::default()).map_err(err)?;
            for field in meta.schema().fields() {
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
            let descr = meta.metadata().file_metadata().schema_descr();
            let ts = (0..descr.num_columns())
                .find(|&i| descr.column(i).name() == TIMESTAMP_COLUMN)
                .ok_or_else(|| err("merge input has no _timestamp column"))?;
            rows += usize::try_from(meta.metadata().file_metadata().num_rows()).map_err(err)?;
            metas.push((meta, ts));
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
        let writer =
            ArrowWriter::try_new(Vec::new(), Arc::clone(&schema), Some(props)).map_err(err)?;
        Ok(Merge {
            bucket,
            inputs: inputs.into_iter().map(Some).collect(),
            metas,
            rows,
            file_columns: file_columns.into_iter().cloned().collect(),
            stats: StatsAccumulator::new(&schema),
            schema,
            writer,
            range: None,
            record_count: 0,
        })
    }

    /// 定出写法。只解码各输入的 `_timestamp`，按秒计数，从早到晚走：越过一个时间段的边界、或再加一秒的行就超过写入器
    /// 的行组行数上限时，结束当前范围（一秒内的行超过上限时这一秒自成一个范围，写入器再按上限切）。估算各范围要解码
    /// 的行（与范围相交的各输入行组的行数之和），超过全部行数的 [`MAX_DECODE_RATIO`] 倍、或只有一个范围时一遍写完。
    /// 任一行不在 `bucket` 内即报错。
    fn plan(&self) -> Result<Plan, FlushError> {
        const SECONDS: usize = 3600;
        let start = self.bucket.start_us();
        let mut per_second = vec![0usize; SECONDS];
        for (input, (meta, ts)) in self.inputs.iter().zip(&self.metas) {
            let bytes = input.clone().expect("规划时全部输入都在");
            let mask =
                ProjectionMask::leaves(meta.metadata().file_metadata().schema_descr(), [*ts]);
            let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, meta.clone())
                .with_projection(mask)
                .with_batch_size(MERGE_BATCH_ROWS)
                .build()
                .map_err(err)?;
            for batch in reader {
                for &t in timestamps(&batch.map_err(err)?)?.values().iter() {
                    if HourBucket::of(t) != self.bucket {
                        return Err(err(format!("row at {t} is outside bucket {}", self.bucket)));
                    }
                    per_second[((t - start) / 1_000_000) as usize] += 1;
                }
            }
        }

        // 各范围起点所在的秒
        let span_seconds = (SPAN_US / 1_000_000) as usize;
        let mut starts: Vec<usize> = Vec::new();
        let mut current: Option<(usize, usize)> = None;
        for (second, &n) in per_second.iter().enumerate() {
            if n == 0 {
                continue;
            }
            current = match current {
                Some((first, count))
                    if first / span_seconds == second / span_seconds
                        && count + n <= DEFAULT_MAX_ROW_GROUP_ROW_COUNT =>
                {
                    Some((first, count + n))
                }
                _ => {
                    starts.push(second);
                    Some((second, n))
                }
            };
        }
        if starts.len() <= 1 {
            return Ok(Plan::OnePass);
        }
        let at = |second: usize| start + second as i64 * 1_000_000;
        let ranges: Vec<Range<i64>> = (0..starts.len())
            .map(|k| {
                let lo = if k == 0 { start } else { at(starts[k]) };
                let hi = starts
                    .get(k + 1)
                    .map_or(self.bucket.end_us() + 1, |&s| at(s));
                lo..hi
            })
            .collect();
        let decoded: usize = ranges
            .iter()
            .map(|range| {
                self.metas
                    .iter()
                    .map(|(meta, ts)| {
                        let m = meta.metadata();
                        scan::row_groups_in(m, *ts, range)
                            .into_iter()
                            .map(|g| m.row_group(g).num_rows() as usize)
                            .sum::<usize>()
                    })
                    .sum::<usize>()
            })
            .sum();
        let (num, den) = MAX_DECODE_RATIO;
        if decoded.saturating_mul(den) > self.rows.saturating_mul(num) {
            return Ok(Plan::OnePass);
        }
        Ok(Plan::Ranges(ranges))
    }

    /// 按 `plan` 写出合并输出。写出的行数与输入的行数之和不等即报错（不产生合并输出）。
    fn write(mut self, plan: Plan) -> Result<DataFile, FlushError> {
        match plan {
            Plan::OnePass => {
                for i in 0..self.inputs.len() {
                    let bytes = self.inputs[i].take().expect("每个输入只读一遍");
                    let meta = self.metas[i].0.clone();
                    let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(bytes, meta)
                        .with_batch_size(MERGE_BATCH_ROWS)
                        .build()
                        .map_err(err)?;
                    for batch in reader {
                        self.put(&batch.map_err(err)?)?;
                    }
                }
            }
            Plan::Ranges(ranges) => {
                // 各输入最后一次用到的范围
                let groups: Vec<Vec<Vec<usize>>> = ranges
                    .iter()
                    .map(|range| {
                        self.metas
                            .iter()
                            .map(|(meta, ts)| scan::row_groups_in(meta.metadata(), *ts, range))
                            .collect()
                    })
                    .collect();
                let last: Vec<Option<usize>> = (0..self.inputs.len())
                    .map(|i| (0..ranges.len()).rev().find(|&k| !groups[k][i].is_empty()))
                    .collect();
                for (k, range) in ranges.iter().enumerate() {
                    for i in 0..self.inputs.len() {
                        if groups[k][i].is_empty() {
                            continue;
                        }
                        let bytes = self.inputs[i].clone().expect("最后一次用到之前不释放");
                        let reader = ParquetRecordBatchReaderBuilder::new_with_metadata(
                            bytes,
                            self.metas[i].0.clone(),
                        )
                        .with_row_groups(groups[k][i].clone())
                        .with_batch_size(MERGE_BATCH_ROWS)
                        .build()
                        .map_err(err)?;
                        for batch in reader {
                            let batch = batch.map_err(err)?;
                            let ts = timestamps(&batch)?;
                            if ts.values().iter().all(|t| range.contains(t)) {
                                self.put(&batch)?;
                            } else {
                                let keep: BooleanArray = ts
                                    .values()
                                    .iter()
                                    .map(|t| Some(range.contains(t)))
                                    .collect();
                                let kept = filter_record_batch(&batch, &keep).map_err(err)?;
                                if kept.num_rows() > 0 {
                                    self.put(&kept)?;
                                }
                            }
                        }
                        if last[i] == Some(k) {
                            self.inputs[i] = None;
                        }
                    }
                    self.writer.flush().map_err(err)?;
                }
            }
        }
        if self.record_count != self.rows {
            return Err(err(format!(
                "merge wrote {} rows, inputs have {}",
                self.record_count, self.rows
            )));
        }
        let (min_timestamp, max_timestamp) =
            self.range.ok_or_else(|| err("merge inputs have no rows"))?;
        let bytes = Bytes::from(self.writer.into_inner().map_err(err)?);
        Ok(DataFile {
            bucket: self.bucket,
            columns: self.file_columns,
            path: layout::data_file_path(self.bucket, &layout::new_object_id()),
            digest: Digest::of(&bytes),
            bytes,
            record_count: self.record_count as u64,
            min_timestamp,
            max_timestamp,
            column_stats: self.stats.finish(),
        })
    }

    /// 把一个输入批次对齐到输出的列写进写入器，计入极值、行数与列统计。
    fn put(&mut self, batch: &RecordBatch) -> Result<(), FlushError> {
        let aligned = align(&self.schema, batch)?;
        for &t in timestamps(&aligned)?.values().iter() {
            if HourBucket::of(t) != self.bucket {
                return Err(err(format!("row at {t} is outside bucket {}", self.bucket)));
            }
            self.range = Some(self.range.map_or((t, t), |(lo, hi)| (lo.min(t), hi.max(t))));
        }
        self.record_count += aligned.num_rows();
        self.stats.update(&aligned);
        self.writer.write(&aligned).map_err(err)
    }
}

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

    // 时间段 → [(批次序号, 该批次落在该时间段的行号)]
    let start = bucket.start_us();
    let mut spans: BTreeMap<i64, Vec<(usize, Vec<u32>)>> = BTreeMap::new();
    let mut rows = 0usize;
    for (i, idx) in parts {
        let ts = timestamps(&batches[*i])?;
        let mut by_span: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
        for &row in idx {
            by_span
                .entry(span_of(start, ts.value(row as usize)))
                .or_default()
                .push(row);
        }
        for (span, idx) in by_span {
            spans.entry(span).or_default().push((*i, idx));
        }
        rows += idx.len();
    }

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
    let mut stats = StatsAccumulator::new(&schema);
    let mut range: Option<(i64, i64)> = None;
    // 每个时间段一个行组：逐个批次取出落在该段的行写进写入器，不拼成整个文件的批次
    for parts in spans.values() {
        for (i, idx) in parts {
            let batch = take_aligned(&schema, &batches[*i], idx)?;
            for &t in timestamps(&batch)?.values().iter() {
                range = Some(range.map_or((t, t), |(lo, hi)| (lo.min(t), hi.max(t))));
            }
            stats.update(&batch);
            writer.write(&batch).map_err(err)?;
        }
        writer.flush().map_err(err)?;
    }
    let bytes = Bytes::from(writer.into_inner().map_err(err)?);
    let (min_timestamp, max_timestamp) = range.expect("每个桶至少一行");
    debug_assert!(
        HourBucket::of(min_timestamp) == bucket && HourBucket::of(max_timestamp) == bucket
    );

    Ok(DataFile {
        bucket,
        columns: file_columns.into_iter().cloned().collect(),
        path: layout::data_file_path(bucket, &layout::new_object_id()),
        digest: Digest::of(&bytes),
        bytes,
        record_count: rows as u64,
        min_timestamp,
        max_timestamp,
        column_stats: stats.finish(),
    })
}

/// `batch` 中行号为 `idx` 的行，列按 `schema` 的列序排好，`batch` 里没有的列以 null 补齐；同名列的类型须与 `schema`
/// 相同。
fn take_aligned(
    schema: &Arc<Schema>,
    batch: &RecordBatch,
    idx: &[u32],
) -> Result<RecordBatch, FlushError> {
    let idx = UInt32Array::from(idx.to_vec());
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
    RecordBatch::try_new(Arc::clone(schema), arrays).map_err(err)
}

#[cfg(test)]
mod tests {
    use super::*;
    use amberlog_common::schema::ColumnType;
    use arrow::array::{Array, Int64Array, StringArray};
    use arrow::row::{RowConverter, SortField};
    use parquet::basic::{LogicalType, Repetition, TimeUnit, Type as PhysicalType};
    use parquet::file::statistics::Statistics;

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
        let arrays: Vec<ArrayRef> = names
            .iter()
            .map(|n| -> ArrayRef {
                match *n {
                    TIMESTAMP_COLUMN => {
                        Arc::new(TimestampMicrosecondArray::from(ts.clone()).with_timezone("UTC"))
                    }
                    "host" => Arc::new(StringArray::from(
                        ts.iter().map(|t| format!("h{}", t % 5)).collect::<Vec<_>>(),
                    )),
                    _ => Arc::new(Int64Array::from(
                        ts.iter().map(|t| t % 1000).collect::<Vec<_>>(),
                    )),
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

    /// 文件全部行的多重集（每行编成可比较的字节串，排序），连同列。
    fn row_multiset(f: &DataFile) -> (Vec<String>, Vec<Vec<u8>>) {
        let batches = decode(&f.bytes);
        let schema = batches[0].schema();
        let converter = RowConverter::new(
            schema
                .fields()
                .iter()
                .map(|f| SortField::new(f.data_type().clone()))
                .collect(),
        )
        .unwrap();
        let mut rows = Vec::new();
        for b in &batches {
            let converted = converter.convert_columns(b.columns()).unwrap();
            rows.extend(converted.iter().map(|r| r.as_ref().to_vec()));
        }
        rows.sort();
        let names = schema.fields().iter().map(|f| f.name().clone()).collect();
        (names, rows)
    }

    /// 文件各行组的（行数，`_timestamp` 下界，上界）。
    fn row_groups(f: &DataFile) -> Vec<(i64, i64, i64)> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(f.bytes.clone()).unwrap();
        reader
            .metadata()
            .row_groups()
            .iter()
            .map(|rg| match rg.column(0).statistics() {
                Some(Statistics::Int64(s)) => {
                    (rg.num_rows(), *s.min_opt().unwrap(), *s.max_opt().unwrap())
                }
                other => panic!("_timestamp 没有行组统计：{other:?}"),
            })
            .collect()
    }

    /// 各行组按时间从早到晚、互不重叠，每个行组落在一个时间段内。
    fn assert_disjoint_spans(f: &DataFile) {
        let start = f.bucket.start_us();
        let groups = row_groups(f);
        for (n, lo, hi) in &groups {
            assert!(*n > 0);
            assert_eq!(span_of(start, *lo), span_of(start, *hi), "{groups:?}");
            assert!(*n as usize <= DEFAULT_MAX_ROW_GROUP_ROW_COUNT);
        }
        for w in groups.windows(2) {
            assert!(w[0].2 < w[1].1, "行组互不重叠：{groups:?}");
        }
    }

    /// 一个只有一个行组的数据文件（升级前的写法）。
    fn one_group(cols: &StreamColumns, ts: Vec<i64>) -> DataFile {
        let b = batch(&[TIMESTAMP_COLUMN, "host", "n"], ts);
        let mut writer = ArrowWriter::try_new(Vec::new(), b.schema(), None).unwrap();
        writer.write(&b).unwrap();
        let bytes = Bytes::from(writer.into_inner().unwrap());
        let mut f = write_data_files(cols, &[b], &BloomColumns::none())
            .unwrap()
            .remove(0);
        f.digest = Digest::of(&bytes);
        f.bytes = bytes;
        f
    }

    /// 落盘文件按时间段切行组：每段一个行组、行组上下界在段内；只有一个段时一个行组；文件条目与列统计与不切时相同。
    #[test]
    fn flush_file_row_groups_follow_spans() {
        let cols = columns();
        let start = T10 - 3_599_000_000;
        // 两个批次，行跨三个时间段，先后打乱
        let ts1 = vec![T10, start + 5, T10 - 1, start + SPAN_US + 7];
        let ts2 = vec![start + SPAN_US, T10 - 2, start];
        let batches = [
            batch(&[TIMESTAMP_COLUMN, "host"], ts1.clone()),
            batch(&[TIMESTAMP_COLUMN, "host", "n"], ts2.clone()),
        ];
        let f = write_data_files(&cols, &batches, &BloomColumns::none())
            .unwrap()
            .remove(0);
        assert_eq!(
            row_groups(&f),
            [
                (2, start, start + 5),
                (2, start + SPAN_US, start + SPAN_US + 7),
                (3, T10 - 2, T10)
            ]
        );
        assert_disjoint_spans(&f);
        assert_eq!(
            (f.record_count, f.min_timestamp, f.max_timestamp),
            (7, start, T10)
        );
        // 列统计与把对齐后的全部行拼成一个批次再算的相同
        let schema = Arc::new(Schema::new(
            f.columns
                .iter()
                .map(|c| c.arrow_field())
                .collect::<Vec<_>>(),
        ));
        let whole: Vec<RecordBatch> = batches.iter().map(|b| align(&schema, b).unwrap()).collect();
        let whole = arrow::compute::concat_batches(&schema, &whole).unwrap();
        assert_eq!(f.column_stats, crate::stats::column_stats(&whole));
        assert_eq!(row_multiset(&f).1.len(), 7);

        // 只有一个时间段：一个行组
        let f = write_data_files(
            &cols,
            &[batch(&[TIMESTAMP_COLUMN], vec![T10, T10 - 9])],
            &BloomColumns::none(),
        )
        .unwrap()
        .remove(0);
        assert_eq!(row_groups(&f), [(2, T10 - 9, T10)]);
    }

    /// 合并输出与「全部解码、拼成一个批次再写」作为多重集相同：列、行、行数、极值、列统计；行组按时间段互不重叠。
    #[test]
    fn merged_output_equals_concatenated_write() {
        let cols = columns();
        let none = BloomColumns::none();
        let one = |names: &[&str], ts: Vec<i64>| {
            let mut files = write_data_files(&cols, &[batch(names, ts)], &none).unwrap();
            assert_eq!(files.len(), 1);
            files.remove(0)
        };
        // 三个输入的列各不相同，第二个输入的行跨过多个解码批次与多个时间段
        let many: Vec<i64> = (0..20_000)
            .map(|i| T10 - 3_000_000_000 + i * 50_000)
            .collect();
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
        let bytes = || inputs.iter().map(|f| f.bytes.clone()).collect::<Vec<_>>();
        let merge = Merge::open(&cols, bucket, bytes(), &none).unwrap();
        assert!(matches!(merge.plan().unwrap(), Plan::Ranges(_)));
        let got = write_merged(&cols, bucket, bytes(), &none).unwrap();
        assert_eq!(got.bucket, want.bucket);
        assert_eq!(got.columns, want.columns);
        assert_eq!(got.record_count, want.record_count);
        assert_eq!(
            (got.min_timestamp, got.max_timestamp),
            (want.min_timestamp, want.max_timestamp)
        );
        assert_eq!(got.column_stats, want.column_stats);
        assert_eq!(row_multiset(&got), row_multiset(&want));
        assert_disjoint_spans(&got);
        assert!(row_groups(&got).len() > 2);

        // 不在该桶内的行即报错
        let other = one(&[TIMESTAMP_COLUMN], vec![T11]);
        assert!(write_merged(&cols, bucket, vec![other.bytes], &none).is_err());
    }

    /// 输入是一个行组覆盖整小时的旧式文件：多遍要重复解码太多，退回一遍写完，结果作为多重集与拼接相同。
    #[test]
    fn merge_of_single_group_inputs_writes_in_one_pass() {
        let cols = columns();
        let none = BloomColumns::none();
        let start = T10 - 3_599_000_000;
        let inputs: Vec<DataFile> = (0..3)
            .map(|k| one_group(&cols, (0..12).map(|i| start + k + i * SPAN_US).collect()))
            .collect();
        assert!(inputs.iter().all(|f| row_groups(f).len() == 1));
        let bytes = || inputs.iter().map(|f| f.bytes.clone()).collect::<Vec<_>>();
        let merge = Merge::open(&cols, inputs[0].bucket, bytes(), &none).unwrap();
        assert!(matches!(merge.plan().unwrap(), Plan::OnePass));

        let decoded: Vec<RecordBatch> = inputs.iter().flat_map(|f| decode(&f.bytes)).collect();
        let want = write_data_files(&cols, &decoded, &none).unwrap().remove(0);
        let got = write_merged(&cols, inputs[0].bucket, bytes(), &none).unwrap();
        assert_eq!(got.record_count, 36);
        assert_eq!(got.column_stats, want.column_stats);
        assert_eq!(row_multiset(&got), row_multiset(&want));
    }

    /// 写出的行数与输入的行数之和不等即报错，不产生合并输出：用漏掉一段时间的写法造出不等。
    #[test]
    fn merge_rejects_row_count_mismatch() {
        let cols = columns();
        let none = BloomColumns::none();
        let start = T10 - 3_599_000_000;
        let f = write_data_files(
            &cols,
            &[batch(&[TIMESTAMP_COLUMN], vec![start, T10])],
            &none,
        )
        .unwrap()
        .remove(0);
        let merge = Merge::open(
            &cols,
            f.bucket,
            vec![f.bytes.clone(), f.bytes.clone()],
            &none,
        )
        .unwrap();
        let plan = Plan::Ranges(std::iter::once(start..start + SPAN_US).collect());
        let e = merge.write(plan).unwrap_err();
        assert!(e.0.contains("merge wrote 2 rows, inputs have 4"), "{e}");
        // 完整的写法照常写出
        let merge = Merge::open(&cols, f.bucket, vec![f.bytes.clone(), f.bytes], &none).unwrap();
        let plan = merge.plan().unwrap();
        assert_eq!(merge.write(plan).unwrap().record_count, 4);
    }

    /// 随机的时间戳、输入个数与列组合：合并输出作为多重集与拼接相同，行数相等，行组按时间段互不重叠（退回一遍写完的
    /// 除外）。
    #[test]
    fn merged_output_matches_concatenation_on_random_inputs() {
        use rand::{Rng, SeedableRng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(20261109);
        let cols = columns();
        let none = BloomColumns::none();
        let start = T10 - 3_599_000_000;
        let shapes: [&[&str]; 4] = [
            &[TIMESTAMP_COLUMN],
            &[TIMESTAMP_COLUMN, "host"],
            &[TIMESTAMP_COLUMN, "n"],
            &[TIMESTAMP_COLUMN, "host", "n"],
        ];
        for _ in 0..40 {
            let k = rng.random_range(1..6);
            let inputs: Vec<DataFile> = (0..k)
                .map(|_| {
                    // 大多集中在一小段时间，少量迟到的散在整小时
                    let at = rng.random_range(0..3_600_000_000i64);
                    let n = rng.random_range(1..3000);
                    let ts: Vec<i64> = (0..n)
                        .map(|_| {
                            if rng.random_bool(0.05) {
                                start + rng.random_range(0..3_600_000_000)
                            } else {
                                start + (at + rng.random_range(0..10_000_000)).min(3_599_999_999)
                            }
                        })
                        .collect();
                    let shape = shapes[rng.random_range(0..shapes.len())];
                    write_data_files(&cols, &[batch(shape, ts)], &none)
                        .unwrap()
                        .remove(0)
                })
                .collect();
            let decoded: Vec<RecordBatch> = inputs.iter().flat_map(|f| decode(&f.bytes)).collect();
            let want = write_data_files(&cols, &decoded, &none).unwrap().remove(0);
            let bytes: Vec<Bytes> = inputs.iter().map(|f| f.bytes.clone()).collect();
            let plan = Merge::open(&cols, want.bucket, bytes.clone(), &none)
                .unwrap()
                .plan()
                .unwrap();
            let got = write_merged(&cols, want.bucket, bytes, &none).unwrap();
            assert_eq!(got.record_count, want.record_count);
            assert_eq!(got.columns, want.columns);
            assert_eq!(
                (got.min_timestamp, got.max_timestamp),
                (want.min_timestamp, want.max_timestamp)
            );
            assert_eq!(got.column_stats, want.column_stats);
            assert_eq!(row_multiset(&got), row_multiset(&want));
            if matches!(plan, Plan::Ranges(_)) {
                assert_disjoint_spans(&got);
            }
        }
    }
}
