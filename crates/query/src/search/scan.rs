//! stream 查询的输入（[`Source`]）：可见集中参与本查询的代，再依次是剪枝后留下的热层与冷层数据文件，一个文件读完
//! 再开下一个，逐批交出（REQ-AMB-QRY-RESOURCE-LIMITS-V2 METERED-SCOPE：同一时刻只留着一个文件的原始字节与各并行分区
//! 手上的一批，[`crate::input`]）。只解码本查询用到的列；文件中 `_timestamp` 上下界与查询范围不相交的行组不读。
//!
//! 读取失败的分类与读完全部文件之后再执行时相同（REQ-AMB-QRY-SEARCH-ENDPOINT「读取存储」、REQ-AMB-QRY-CROSS-TIER）：
//! 作答所需的数据文件不存在时先重判是否「数据已移动」（[`Engine::missing`]），热层内容无法解析回 500，冷层按
//! [`cold::error`]。

use std::collections::VecDeque;
use std::ops::Range;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_storage::BoxFuture;
use amberlog_storage::chain::FileEntry;
use amberlog_storage::cold::{ColdBatches, ColdDataFile, ColdReadError, ColdTable};
use amberlog_storage::schema::StreamColumns;
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;

use super::{Engine, Missing, align};
use crate::cold;
use crate::error::QueryError;
use crate::files::{FileBatches, FileError};
use crate::input::Source;

/// 要读的一个数据文件。
pub(super) enum Part {
    /// 热层数据文件与所在桶的标签。
    Hot { bucket: String, file: FileEntry },
    /// 冷层数据文件。
    Cold(ColdDataFile),
}

/// 正在读的文件。
enum Open {
    Hot(FileBatches),
    Cold(ColdBatches),
}

/// 一次 stream 查询的输入。
pub(super) struct Scan {
    engine: Engine,
    dataset: Dataset,
    /// stream 当前 schema（冷层按字段编号读取的目标由它取列）。
    stream: StreamColumns,
    /// 可见集中参与本查询的代的批次（已在内存里）。
    resident: VecDeque<RecordBatch>,
    parts: VecDeque<Part>,
    /// 冷层部分的冷层当前版本（有冷层数据文件时才有）。
    cold_table: Option<Arc<ColdTable>>,
    /// 查询范围 `[start_time, end_time)`。
    time: Range<i64>,
    open: Option<Open>,
}

impl Scan {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        engine: Engine,
        dataset: Dataset,
        stream: StreamColumns,
        resident: VecDeque<RecordBatch>,
        parts: VecDeque<Part>,
        cold_table: Option<Arc<ColdTable>>,
        time: Range<i64>,
    ) -> Scan {
        Scan {
            engine,
            dataset,
            stream,
            resident,
            parts,
            cold_table,
            time,
            open: None,
        }
    }

    /// 打开下一个文件；文件都读过了为 `None`。
    async fn open_next(&mut self, schema: &SchemaRef) -> Option<Result<(), QueryError>> {
        let part = self.parts.pop_front()?;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let opened = match part {
            Part::Hot { bucket, file } => {
                let read = self
                    .engine
                    .files
                    .read(&self.dataset, &file, &columns, self.time.clone())
                    .await;
                match read {
                    Ok(batches) => Ok(Open::Hot(batches)),
                    Err(FileError::NotFound(m)) => {
                        let missing = Missing::Hot {
                            bucket: &bucket,
                            path: &file.path,
                        };
                        Err(self.engine.missing(&self.dataset, missing, m).await)
                    }
                    Err(e) => Err(hot_error(e)),
                }
            }
            Part::Cold(file) => {
                let table = self
                    .cold_table
                    .as_ref()
                    .expect("有冷层数据文件即有冷层当前版本");
                let target = cold_target(&self.stream, &columns);
                let read = cold::read(
                    self.engine.cold.as_ref(),
                    &self.dataset,
                    table,
                    &file,
                    target,
                    self.time.clone(),
                )
                .await;
                match read {
                    Ok(batches) => Ok(Open::Cold(batches)),
                    Err(ColdReadError::NotFound(m)) => {
                        let missing = Missing::Cold {
                            path: &file.path,
                            day: file.day,
                        };
                        Err(self.engine.missing(&self.dataset, missing, m).await)
                    }
                    Err(e) => Err(cold::error(&self.engine.gates, &self.dataset, e)),
                }
            }
        };
        Some(opened.map(|o| self.open = Some(o)))
    }
}

/// 热层数据文件的读取失败（不存在的情形另由重判处理）：内容无法解析回 500，不可访问回 503。
fn hot_error(e: FileError) -> QueryError {
    match e {
        FileError::Unavailable(m) => QueryError::new(Reason::Unavailable, m),
        FileError::Integrity(m) | FileError::NotFound(m) => QueryError::internal(m),
    }
}

/// 冷层按字段编号读取的目标：stream 当前 schema 中列名在 `columns` 里的列（带字段编号），按 stream 的列序。
fn cold_target(stream: &StreamColumns, columns: &[String]) -> SchemaRef {
    let all = stream.arrow_schema();
    let fields: Vec<_> = all
        .fields()
        .iter()
        .filter(|f| columns.contains(f.name()))
        .cloned()
        .collect();
    Arc::new(Schema::new(fields))
}

impl Source for Scan {
    fn next<'a>(
        &'a mut self,
        schema: &'a SchemaRef,
    ) -> BoxFuture<'a, Option<Result<RecordBatch, QueryError>>> {
        Box::pin(async move {
            if let Some(b) = self.resident.pop_front() {
                return Some(align(schema, &b));
            }
            loop {
                let next = match &mut self.open {
                    Some(Open::Hot(batches)) => batches.next().map(|r| r.map_err(hot_error)),
                    Some(Open::Cold(batches)) => batches
                        .next()
                        .map(|r| r.map_err(|e| cold::error(&self.engine.gates, &self.dataset, e))),
                    None => {
                        if let Err(e) = self.open_next(schema).await? {
                            return Some(Err(e));
                        }
                        continue;
                    }
                };
                match next {
                    Some(Ok(b)) => return Some(align(schema, &b)),
                    Some(Err(e)) => {
                        self.open = None;
                        return Some(Err(e));
                    }
                    // 这个文件读完了，打开下一个
                    None => self.open = None,
                }
            }
        })
    }
}
