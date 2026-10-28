//! 入湖的冷层一半（REQ-AMB-STO-LAKE-ENTRY 第 ② 步，REQ-AMB-STO-COLD-TABLE「数据文件」）：把入湖输入（已核对摘要的
//! 热层 Parquet 完整字节）的全部记录重写为冷层数据文件，上传后以恰好一次冷层提交加入冷层表。
//!
//! - 记录恰为入湖输入中的全部记录，各列取值不变（-RECORDS）；各输入的列按字段编号对齐，某输入没有的列为空值，
//!   入湖输入中都没有出现的列不写进冷层数据文件（-COLD-TABLE-PHYS-ID 括注）。
//! - 全部记录按 `_timestamp` 升序排好后依次切分（-SORTED、-SORTED-ACROSS）：先按 UTC 日切（-COLD-TABLE-DATA-DAY），
//!   再对超过 `compaction.target_file_bytes` 的文件对半切，直到不超过或只剩一条记录（-SIZE）。
//! - 摘要属性 `amberlog.operation`、`amberlog.bucket`、`amberlog.hot_snapshot_id`、`amberlog.file_digests`
//!   （-PROPS、-HOT-SNAP、-FILE-DIGESTS）；`file_digests` 取写出各文件时对其完整字节算的 SHA-256。

use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use iceberg::spec::Schema;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::commit::{Change, Committed, Committer, OPERATION, Progress};
use super::current;
use super::datafile::{self, ColdFile};
use super::error::{ColdError, invalid};
use super::layout;
use super::names;
use super::sorted::Sorted;
use crate::bloom::BloomColumns;
use crate::chain::{Digest, SnapshotId};
use crate::fault;
use crate::layout::HourBucket;
use crate::schema::{SchemaColumn, StreamColumns, TIMESTAMP_FIELD_ID};

/// 一个已写出、已上传的冷层数据文件：交接记录 `files` 的一个元素所需的内容。
#[derive(Debug, Clone, PartialEq)]
pub struct WrittenFile {
    /// 相对表目录。
    pub path: String,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    pub digest: Digest,
}

/// 一次入湖写好的冷层数据文件（尚未提交）。
pub struct Rewritten {
    pub(super) files: Vec<ColdFile>,
}

impl Rewritten {
    pub fn files(&self) -> Vec<WrittenFile> {
        self.files
            .iter()
            .map(|f| WrittenFile {
                path: f.path.clone(),
                min_timestamp: f.min_timestamp,
                max_timestamp: f.max_timestamp,
                record_count: f.record_count,
                size_bytes: f.bytes.len() as u64,
                digest: Digest::of(&f.bytes),
            })
            .collect()
    }
}

/// 把入湖输入重写为冷层数据文件（不上传）。冷层列名取以冷层当前版本的 schema 并入 `hot` 之后的列名；
/// 调用方须已持有该数据集的冷层提交锁（改名按字段编号确定，换号重试时同一列得到同一个名字）。
pub async fn rewrite(
    committer: &Committer,
    dataset: &Dataset,
    hot: &[SchemaColumn],
    inputs: &[Bytes],
    target_file_bytes: u64,
    bloom: &BloomColumns,
) -> Result<Rewritten, ColdError> {
    let cur = current::current(&committer.store, &committer.layout, dataset).await?;
    let prev = cur
        .metadata
        .as_ref()
        .map(|(_, m)| Arc::clone(m.current_schema()));
    let rewritten = rewrite_records(
        committer,
        dataset,
        prev.as_deref(),
        hot,
        inputs,
        target_file_bytes,
        bloom,
    )
    .map_err(|e| match e {
        // 解码、对齐、排序与编码的失败是重写失败；冷层对象的读取失败不经这里
        ColdError::Invalid(m) => ColdError::Rewrite(m),
        e => e,
    })?;
    if fault::fail_once("lake.rewrite") {
        return Err(ColdError::Rewrite("injected rewrite failure".to_owned()));
    }
    Ok(rewritten)
}

/// [`rewrite`] 中读冷层当前版本之后的部分：按 `prev`（冷层当前 schema）并入 `hot` 定列名，解码、排序并写出各文件。
fn rewrite_records(
    committer: &Committer,
    dataset: &Dataset,
    prev: Option<&Schema>,
    hot: &[SchemaColumn],
    inputs: &[Bytes],
    target_file_bytes: u64,
    bloom: &BloomColumns,
) -> Result<Rewritten, ColdError> {
    let cold = Schema::builder()
        .with_fields(names::evolve(prev, hot))
        .build()
        .map_err(invalid)?;
    let all = sorted_records(hot, inputs)?;
    let mut files = Vec::new();
    write_by_day(
        committer,
        dataset,
        &cold,
        &all,
        target_file_bytes,
        bloom,
        &mut files,
    )?;
    Ok(Rewritten { files })
}

/// 上传 `rewritten` 的各文件并做入湖冷层提交。调用方须已持有该数据集的冷层提交锁；
/// `progress` 在调用方以同一请求再调时沿用（REQ-AMB-STO-COLD-TABLE-SAME-CONTENT）。
pub async fn commit(
    committer: &Committer,
    dataset: &Dataset,
    bucket: HourBucket,
    hot_snapshot_id: SnapshotId,
    hot: &[SchemaColumn],
    rewritten: &Rewritten,
    progress: &mut Progress,
) -> Result<Committed, ColdError> {
    for f in &rewritten.files {
        progress
            .upload(
                &committer.store,
                &committer.layout,
                &f.path,
                f.bytes.clone(),
            )
            .await?;
    }
    fault::pause("lake.after_cold_upload");
    fault::gate("lake.after_cold_upload").await;
    let digests: BTreeMap<String, String> = rewritten
        .files()
        .into_iter()
        .map(|f| (f.path, f.digest.to_string()))
        .collect();
    let summary = BTreeMap::from([
        (OPERATION.to_owned(), "lake".to_owned()),
        ("amberlog.bucket".to_owned(), bucket.to_string()),
        (
            "amberlog.hot_snapshot_id".to_owned(),
            hot_snapshot_id.0.to_string(),
        ),
        (
            "amberlog.file_digests".to_owned(),
            serde_json::to_string(&digests).map_err(invalid)?,
        ),
    ]);
    let change = Change::Append {
        schema: hot.to_vec(),
        files: rewritten
            .files
            .iter()
            .map(|f| f.data_file.clone())
            .collect(),
        summary,
    };
    committer.commit(dataset, &change, progress).await
}

/// 把排好的全部记录先按 UTC 日切开，再逐日按 [`split_write`] 写成文件。
pub(super) fn write_by_day(
    committer: &Committer,
    dataset: &Dataset,
    cold: &Schema,
    all: &Sorted,
    target_file_bytes: u64,
    bloom: &BloomColumns,
    out: &mut Vec<ColdFile>,
) -> Result<(), ColdError> {
    let mut start = 0;
    while start < all.len() {
        let day = layout::day_of(all.timestamp(start));
        let mut end = start + 1;
        while end < all.len() && layout::day_of(all.timestamp(end)) == day {
            end += 1;
        }
        split_write(
            committer,
            dataset,
            cold,
            all,
            start..end,
            target_file_bytes,
            bloom,
            out,
        )?;
        start = end;
    }
    Ok(())
}

/// 写排好的第 `rows` 行（同一 UTC 日）：超过目标大小且多于一条时对半切分后再写。
#[allow(clippy::too_many_arguments)]
fn split_write(
    committer: &Committer,
    dataset: &Dataset,
    cold: &Schema,
    all: &Sorted,
    rows: Range<usize>,
    target_file_bytes: u64,
    bloom: &BloomColumns,
    out: &mut Vec<ColdFile>,
) -> Result<(), ColdError> {
    let file = datafile::write_parts(
        &committer.layout,
        dataset,
        cold,
        &all.chunks(rows.clone())?,
        bloom,
    )?;
    if file.bytes.len() as u64 <= target_file_bytes || rows.len() == 1 {
        out.push(file);
        return Ok(());
    }
    let half = rows.start + rows.len() / 2;
    split_write(
        committer,
        dataset,
        cold,
        all,
        rows.start..half,
        target_file_bytes,
        bloom,
        out,
    )?;
    split_write(
        committer,
        dataset,
        cold,
        all,
        half..rows.end,
        target_file_bytes,
        bloom,
        out,
    )
}

/// 解码全部入湖输入，各批次按字段编号对齐到同一 schema（列为入湖输入中出现过的列，按字段编号升序，取热层的
/// Arrow 写法），再按 `_timestamp` 升序排出行序（不拼成一个批次，见 [`Sorted`]）。
fn sorted_records(hot: &[SchemaColumn], inputs: &[Bytes]) -> Result<Sorted, ColdError> {
    let columns = StreamColumns::from_columns(hot.to_vec());
    let by_id: HashMap<u32, &SchemaColumn> =
        columns.columns().iter().map(|c| (c.field_id, c)).collect();
    let mut batches = Vec::new();
    for (i, input) in inputs.iter().enumerate() {
        let reader = ParquetRecordBatchReaderBuilder::try_new(input.clone())
            .and_then(|b| b.build())
            .map_err(|e| invalid(format!("lake input {i}: {e}")))?;
        for b in reader {
            batches.push(b.map_err(|e| invalid(format!("lake input {i}: {e}")))?);
        }
    }
    let id_of = |f: &Field| -> Result<u32, ColdError> {
        f.metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .and_then(|v| v.parse().ok())
            .ok_or_else(|| invalid(format!("lake input column {} has no field id", f.name())))
    };
    let mut present: Vec<u32> = Vec::new();
    for b in &batches {
        for f in b.schema().fields() {
            let id = id_of(f)?;
            if !by_id.contains_key(&id) {
                return Err(invalid(format!(
                    "lake input column {} (field id {id}) is not in the hot schema",
                    f.name()
                )));
            }
            if !present.contains(&id) {
                present.push(id);
            }
        }
    }
    if !present.contains(&TIMESTAMP_FIELD_ID) {
        return Err(invalid("lake inputs have no records"));
    }
    present.sort_unstable();
    let schema = Arc::new(ArrowSchema::new(
        present
            .iter()
            .map(|id| by_id[id].arrow_field())
            .collect::<Vec<_>>(),
    ));
    let mut aligned = Vec::with_capacity(batches.len());
    for b in &batches {
        let bs = b.schema();
        let arrays = present
            .iter()
            .zip(schema.fields())
            .map(
                |(id, field)| match bs.fields().iter().position(|f| id_of(f).ok() == Some(*id)) {
                    Some(i) if b.column(i).data_type() == field.data_type() => {
                        Ok(Arc::clone(b.column(i)))
                    }
                    Some(i) => Err(invalid(format!(
                        "lake input column {} is {}, {} in the hot schema",
                        field.name(),
                        b.column(i).data_type(),
                        field.data_type()
                    ))),
                    None => Ok(new_null_array(field.data_type(), b.num_rows())),
                },
            )
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        aligned.push(RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(invalid)?);
    }
    if aligned.iter().all(|b| b.num_rows() == 0) {
        return Err(invalid("lake inputs have no records"));
    }
    let ts_index = present
        .iter()
        .position(|id| *id == TIMESTAMP_FIELD_ID)
        .expect("上面已确认");
    Sorted::new(aligned, ts_index)
}
