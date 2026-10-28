//! 查询逐批读取数据文件时热层与冷层共用的部分（REQ-AMB-QRY-RESOURCE-LIMITS-V2 METERED-SCOPE）：读出的批次逐批交给
//! 调用方，不在读取一侧攒成整份；按行组的 `_timestamp` 上下界跳过与查询范围不相交的行组。
//!
//! 跳过行组只省读取与解码：被跳过的行组里没有 `_timestamp` 落在查询范围内的记录，查询的附加时间过滤本来就会把它们滤掉，
//! 结果不变；扫描统计按文件计，也不变。行组没有 `_timestamp` 的上下界时照常读取。

use std::ops::Range;

use arrow::record_batch::RecordBatch;
use parquet::file::metadata::ParquetMetaData;
use parquet::file::statistics::Statistics;

/// 一个数据文件逐批读出的批次；解码失败作为某一项返回，此后不再有批次。
pub type Batches<E> = Box<dyn Iterator<Item = Result<RecordBatch, E>> + Send>;

/// 每批的行数（DataFusion 的缺省批大小）。
pub const BATCH_ROWS: usize = 8192;

/// `metadata` 中可能含 `_timestamp` 落在 `time`（UTC 微秒，左闭右开）内的记录的行组：第 `column` 个叶子列为
/// `_timestamp`，其行组统计的上下界（两端都含）与 `time` 相交，或没有上下界。
pub fn row_groups_in(metadata: &ParquetMetaData, column: usize, time: &Range<i64>) -> Vec<usize> {
    metadata
        .row_groups()
        .iter()
        .enumerate()
        .filter(|(_, rg)| {
            let Some(Statistics::Int64(s)) = rg.column(column).statistics() else {
                return true;
            };
            match (s.min_opt(), s.max_opt()) {
                (Some(&min), Some(&max)) => min < time.end && max >= time.start,
                _ => true,
            }
        })
        .map(|(i, _)| i)
        .collect()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use bytes::Bytes;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::properties::WriterProperties;

    use super::*;

    /// 三个行组：`_timestamp` 依次为 0..10、10..20、20..30。
    fn three_groups() -> Bytes {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("a", DataType::Int64, true),
        ]));
        let props = WriterProperties::builder()
            .set_max_row_group_row_count(Some(10))
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, Arc::clone(&schema), Some(props)).unwrap();
        let ts: Vec<i64> = (0..30).collect();
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts.clone()).with_timezone("UTC")),
                Arc::new(Int64Array::from(ts)),
            ],
        )
        .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        Bytes::from(buf)
    }

    #[test]
    fn keeps_only_row_groups_that_meet_the_range() {
        let b = ParquetRecordBatchReaderBuilder::try_new(three_groups()).unwrap();
        let m = b.metadata();
        assert_eq!(m.num_row_groups(), 3);
        assert_eq!(row_groups_in(m, 0, &(0..100)), vec![0, 1, 2]);
        assert_eq!(row_groups_in(m, 0, &(10..20)), vec![1]);
        // 上下界两端都含：9 落在第一组，20 落在第三组
        assert_eq!(row_groups_in(m, 0, &(9..21)), vec![0, 1, 2]);
        assert_eq!(row_groups_in(m, 0, &(30..40)), Vec::<usize>::new());
    }
}
