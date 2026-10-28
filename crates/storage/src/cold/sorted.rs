//! 入湖与冷层合并重写时的排序：只对 `_timestamp` 一列求行序，不把全部记录拼成一个批次。
//!
//! Arrow 的字符串与二进制数组用 32 位偏移，一个数组里全部值合计不能超过约 2 GiB。把一个桶（冷层合并是一天）的全部记录
//! 拼成一个批次再排序，某个字符串列解码后超过这个大小时就做不成。这里各输入批次原样留着，按 `(_timestamp, 批次, 行)`
//! 排出行序；写文件时再按有界的块从原批次取行（[`Sorted::chunks`]），一块里某列仍超出 32 位偏移时对半再取。

use std::collections::HashMap;
use std::ops::Range;

use arrow::array::{Array, TimestampMicrosecondArray};
use arrow::compute::interleave_record_batch;
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use super::error::{ColdError, invalid};

/// 写文件时一次从原批次取出的行数上限。
const CHUNK_ROWS: usize = 8192;

/// 一次重写的全部记录：已对齐到同一 schema 的各输入批次，与按 `_timestamp` 升序排好的行序。
pub(super) struct Sorted {
    batches: Vec<RecordBatch>,
    /// 排好的第 i 行：(`_timestamp`, 批次下标, 批次内行号)。同一时刻按输入先后。
    rows: Vec<(i64, u32, u32)>,
}

impl Sorted {
    /// `batches` 的 schema 相同，第 `ts_index` 列是没有空值的微秒时间戳。
    pub(super) fn new(batches: Vec<RecordBatch>, ts_index: usize) -> Result<Sorted, ColdError> {
        let mut rows = Vec::with_capacity(batches.iter().map(RecordBatch::num_rows).sum());
        for (b, batch) in batches.iter().enumerate() {
            let ts = batch
                .column(ts_index)
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .ok_or_else(|| invalid("_timestamp is not a microsecond timestamp column"))?;
            if ts.null_count() > 0 {
                return Err(invalid("_timestamp has nulls"));
            }
            let b = u32::try_from(b).map_err(invalid)?;
            for (r, t) in ts.values().iter().enumerate() {
                rows.push((*t, b, u32::try_from(r).map_err(invalid)?));
            }
        }
        rows.sort_by_key(|r| r.0);
        Ok(Sorted { batches, rows })
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len()
    }

    /// 排好的第 `i` 行的 `_timestamp`。
    pub(super) fn timestamp(&self, i: usize) -> i64 {
        self.rows[i].0
    }

    /// 排好的第 `range` 行，按先后分成若干批次取出（每批至多 [`CHUNK_ROWS`] 行）。
    pub(super) fn chunks(&self, range: Range<usize>) -> Result<Vec<RecordBatch>, ColdError> {
        let mut out = Vec::new();
        for window in self.rows[range].chunks(CHUNK_ROWS) {
            self.take(window, &mut out)?;
        }
        Ok(out)
    }

    fn take(
        &self,
        window: &[(i64, u32, u32)],
        out: &mut Vec<RecordBatch>,
    ) -> Result<(), ColdError> {
        // 只把这一块用到的批次交给 interleave
        let mut local: HashMap<u32, usize> = HashMap::new();
        let mut used: Vec<&RecordBatch> = Vec::new();
        let indices: Vec<(usize, usize)> = window
            .iter()
            .map(|&(_, b, r)| {
                let i = *local.entry(b).or_insert_with(|| {
                    used.push(&self.batches[b as usize]);
                    used.len() - 1
                });
                (i, r as usize)
            })
            .collect();
        match interleave_record_batch(&used, &indices) {
            Ok(batch) => {
                out.push(batch);
                Ok(())
            }
            Err(ArrowError::OffsetOverflowError(_)) if window.len() > 1 => {
                let (a, b) = window.split_at(window.len() / 2);
                self.take(a, out)?;
                self.take(b, out)
            }
            Err(e) => Err(invalid(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use super::*;

    fn batch(ts: &[i64], s: &[&str]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("t", DataType::Timestamp(TimeUnit::Microsecond, None), false),
            Field::new("s", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(TimestampMicrosecondArray::from(ts.to_vec())),
                Arc::new(StringArray::from(s.to_vec())),
            ],
        )
        .unwrap()
    }

    fn strings(batches: &[RecordBatch]) -> Vec<String> {
        batches
            .iter()
            .flat_map(|b| {
                let s = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
                (0..s.len())
                    .map(|i| s.value(i).to_owned())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    #[test]
    fn rows_come_out_in_timestamp_order_across_batches() {
        let sorted = Sorted::new(
            vec![
                batch(&[30, 10, 50], &["c", "a", "e"]),
                batch(&[20, 40, 10], &["b", "d", "a2"]),
            ],
            0,
        )
        .unwrap();
        assert_eq!(sorted.len(), 6);
        assert_eq!(
            (0..6).map(|i| sorted.timestamp(i)).collect::<Vec<_>>(),
            [10, 10, 20, 30, 40, 50]
        );
        // 同一时刻按输入先后
        assert_eq!(
            strings(&sorted.chunks(0..6).unwrap()),
            ["a", "a2", "b", "c", "d", "e"]
        );
        assert_eq!(strings(&sorted.chunks(2..4).unwrap()), ["b", "c"]);
    }

    #[test]
    fn chunks_are_bounded() {
        let ts: Vec<i64> = (0..(CHUNK_ROWS as i64 * 2 + 5)).rev().collect();
        let s: Vec<String> = ts.iter().map(|t| t.to_string()).collect();
        let refs: Vec<&str> = s.iter().map(String::as_str).collect();
        let sorted = Sorted::new(vec![batch(&ts, &refs)], 0).unwrap();
        let chunks = sorted.chunks(0..sorted.len()).unwrap();
        assert_eq!(
            chunks.iter().map(RecordBatch::num_rows).collect::<Vec<_>>(),
            [CHUNK_ROWS, CHUNK_ROWS, 5]
        );
        assert_eq!(strings(&chunks)[0], "0");
    }

    #[test]
    fn nulls_in_timestamp_are_rejected() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "t",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            true,
        )]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(TimestampMicrosecondArray::from(vec![
                Some(1),
                None,
            ]))],
        )
        .unwrap();
        assert!(Sorted::new(vec![b], 0).is_err());
    }
}
