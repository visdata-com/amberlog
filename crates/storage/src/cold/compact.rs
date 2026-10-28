//! 冷层合并的冷层一半（REQ-AMB-STO-COLD-COMPACTION 的重写与一次冷层合并提交）：把合并输入（核对过摘要的冷层 Parquet
//! 完整字节）的全部记录重写为合并产出，上传后以恰好一次冷层合并提交换下全部输入、加入全部产出。
//!
//! - 记录恰为合并输入中的全部记录，各列取值不变（-RECORDS）；各输入的列按字段编号对齐，某输入没有的列为空值，
//!   合并输入中都没有出现的列不写进合并产出；冷层 schema 不变，物理列名取冷层当前版本的列名。
//! - 全部记录按 `_timestamp` 升序排好后依次切分（-SORTED、-SORTED-ACROSS），切分与入湖相同：超过
//!   `compaction.target_file_bytes` 的文件对半切，直到不超过或只剩一条记录（-SIZE）。合并输入同属一个日分区（-SAME-DAY）。
//! - 快照摘要 `amberlog.operation` 为 `compaction`、`amberlog.file_digests` 记全部合并产出写出时的摘要（-FILE-DIGESTS），
//!   标准 `operation` 为 `replace`（REQ-AMB-STO-COLD-TABLE-V2）。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use arrow::array::{ArrayRef, new_null_array};
use arrow::datatypes::{Field, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

use super::commit::{Change, Committed, Committer, OPERATION, Progress};
use super::current;
use super::error::{ColdError, invalid};
use super::lake::{Rewritten, write_by_day};
use super::sorted::Sorted;
use crate::bloom::BloomColumns;
use crate::schema::TIMESTAMP_FIELD_ID;

/// 把合并输入重写为合并产出（不上传）。不必持有冷层提交锁：冷层合并不改冷层 schema，列名取冷层当前版本的。
pub async fn rewrite(
    committer: &Committer,
    dataset: &Dataset,
    inputs: &[(String, Bytes)],
    target_file_bytes: u64,
    bloom: &BloomColumns,
) -> Result<Rewritten, ColdError> {
    let cur = current::current(&committer.store, &committer.layout, dataset).await?;
    let Some((_, metadata)) = cur.metadata.as_ref() else {
        return Err(ColdError::Stale);
    };
    let cold = metadata.current_schema().as_ref().clone();
    let all = sorted_records(inputs)?;
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

/// 上传 `rewritten` 的各文件（自上传第一个文件起登记为在途，冷层孤儿清理不删它们）。不必持有冷层提交锁，
/// 上传期间同一数据集的其他冷层提交照常进行。
pub async fn upload(
    committer: &Committer,
    rewritten: &Rewritten,
    progress: &mut Progress,
) -> Result<(), ColdError> {
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
    Ok(())
}

/// 冷层合并提交。调用方须已持有该数据集的冷层提交锁、已 [`upload`]；`progress` 在调用方以同一请求再调时沿用。
pub async fn commit(
    committer: &Committer,
    dataset: &Dataset,
    inputs: &[(String, Bytes)],
    rewritten: &Rewritten,
    progress: &mut Progress,
) -> Result<Committed, ColdError> {
    let digests: BTreeMap<String, String> = rewritten
        .files()
        .into_iter()
        .map(|f| (f.path, f.digest.to_string()))
        .collect();
    let summary = BTreeMap::from([
        (OPERATION.to_owned(), "compaction".to_owned()),
        (
            "amberlog.file_digests".to_owned(),
            serde_json::to_string(&digests).map_err(invalid)?,
        ),
    ]);
    let change = Change::Replace {
        inputs: inputs
            .iter()
            .map(|(p, _)| p.clone())
            .collect::<HashSet<_>>(),
        files: rewritten
            .files
            .iter()
            .map(|f| f.data_file.clone())
            .collect(),
        summary,
    };
    committer.commit(dataset, &change, progress).await
}

fn field_id(f: &Field) -> Result<u32, ColdError> {
    f.metadata()
        .get(PARQUET_FIELD_ID_META_KEY)
        .and_then(|v| v.parse().ok())
        .ok_or_else(|| {
            invalid(format!(
                "cold data file column {} has no field id",
                f.name()
            ))
        })
}

/// 解码全部合并输入，各批次按字段编号对齐到同一 schema（列为合并输入中出现过的列，按字段编号升序，取首次出现时的
/// Arrow 写法），再按 `_timestamp` 升序排出行序（不拼成一个批次，见 [`Sorted`]）。
fn sorted_records(inputs: &[(String, Bytes)]) -> Result<Sorted, ColdError> {
    let mut batches = Vec::new();
    for (path, bytes) in inputs {
        let broken = |e: parquet::errors::ParquetError| ColdError::Broken(format!("{path}: {e}"));
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
            .map_err(broken)?
            .build()
            .map_err(broken)?;
        for b in reader {
            batches.push(b.map_err(|e| ColdError::Broken(format!("{path}: {e}")))?);
        }
    }
    let mut present: BTreeMap<u32, Field> = BTreeMap::new();
    for b in &batches {
        for f in b.schema().fields() {
            let id = field_id(f)?;
            // 某输入没有的列补空值，故 `_timestamp` 以外的列都按可空
            present
                .entry(id)
                .or_insert_with(|| f.as_ref().clone().with_nullable(id != TIMESTAMP_FIELD_ID));
        }
    }
    if !present.contains_key(&TIMESTAMP_FIELD_ID) {
        return Err(invalid("compaction inputs have no records"));
    }
    let schema = Arc::new(ArrowSchema::new(
        present.values().cloned().collect::<Vec<_>>(),
    ));
    let mut aligned = Vec::with_capacity(batches.len());
    for b in &batches {
        let bs = b.schema();
        let arrays = present
            .iter()
            .map(|(id, field)| {
                match bs
                    .fields()
                    .iter()
                    .position(|f| field_id(f).ok() == Some(*id))
                {
                    Some(i) if b.column(i).data_type() == field.data_type() => {
                        Ok(Arc::clone(b.column(i)))
                    }
                    Some(i) => Err(invalid(format!(
                        "compaction input column {} is {}, {} elsewhere",
                        field.name(),
                        b.column(i).data_type(),
                        field.data_type()
                    ))),
                    None => Ok(new_null_array(field.data_type(), b.num_rows())),
                }
            })
            .collect::<Result<Vec<ArrayRef>, _>>()?;
        aligned.push(RecordBatch::try_new(Arc::clone(&schema), arrays).map_err(invalid)?);
    }
    if aligned.iter().all(|b| b.num_rows() == 0) {
        return Err(invalid("compaction inputs have no records"));
    }
    let ts_index = present
        .keys()
        .position(|id| *id == TIMESTAMP_FIELD_ID)
        .expect("上面已确认");
    Sorted::new(aligned, ts_index)
}
