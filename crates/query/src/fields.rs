//! 字段列表（REQ-AMB-QRY-FIELDS）：数据集的「stream 当前 schema」各列的列名与类型。
//!
//! 作答只读可见集持有的当前 schema（已提交部分 ∪ 未提交部分，与查询判 `stream_not_found`、`unknown_column` 所用的
//! 同一份），不读对象存储、不找链尾，也不向 [`crate::limits::Admission`] 取执行名额。先过状态闸门，再判数据集是否存在。

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_common::schema::ColumnType;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::memtable::MemtableView;

use crate::error::QueryError;

/// `dataset` 当前 schema 的各列（列名与类型），按列名的 Unicode 码点序升序。
pub fn fields(
    memtable: &dyn MemtableView,
    gates: &DatasetGates,
    dataset: &Dataset,
) -> Result<Vec<(String, ColumnType)>, QueryError> {
    if let Some(reason) = gates.state(dataset).reason() {
        return Err(QueryError::new(reason, "dataset is disabled until restart"));
    }
    let schema = memtable.schema(dataset).ok_or_else(|| {
        QueryError::new(
            Reason::StreamNotFound,
            format!("stream `{}` does not exist", dataset.stream),
        )
    })?;
    let mut columns: Vec<(String, ColumnType)> = schema
        .columns()
        .iter()
        .map(|c| (c.name.clone(), c.column_type))
        .collect();
    // UTF-8 字符串按字节比较即按码点比较
    columns.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(columns)
}
