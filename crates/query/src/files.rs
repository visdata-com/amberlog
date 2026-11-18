//! 数据文件的读取：查询执行扫描桶 manifest 所列、剪枝后留下的 Parquet 文件（REQ-AMB-STO-HOT-LAYOUT-V2 数据文件，
//! REQ-AMB-QRY-SEARCH-ENDPOINT-V3「读取存储」）。
//!
//! 读取结果按 REQ-AMB-STO-HOT-LAYOUT 分类：对象不存在为 [`FileError::NotFound`]（跨层查询先重判是否「数据已移动」，
//! REQ-AMB-QRY-CROSS-TIER-MOVED），不是合法的 Parquet 文件为 [`FileError::Integrity`]（回 500），其余一切没读到内容的
//! 情形为 [`FileError::Unavailable`]（回 503）。查询不在线核对数据文件摘要（REQ-AMB-STO-AUDIT-INVARIANTS）。

use std::ops::Range;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::TIMESTAMP_COLUMN;
use amberlog_storage::BoxFuture;
use amberlog_storage::bloom::{self, FileBlooms, Pick, StoreObject};
use amberlog_storage::chain::FileEntry;
use amberlog_storage::scan::{self, BATCH_ROWS, Batches};
use amberlog_storage::store::Store;
use parquet::arrow::ProjectionMask;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

/// 数据文件没有读成。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FileError {
    /// 对象不存在。
    NotFound(String),
    /// 内容无法解析。
    Integrity(String),
    /// 对象存储不可访问。
    Unavailable(String),
}

/// 一个数据文件逐批读出的批次（[`DataFiles::read`]）。
pub type FileBatches = Batches<FileError>;

/// 数据文件的读取。
pub trait DataFiles: Send + Sync {
    /// 逐批读出 `file` 的行。批次的列是文件所含的列（stream 当前 schema 的子集），由调用方对齐到表 schema；只需解码
    /// `columns` 所列的列（文件里没有的不算错）。`time`（UTC 微秒，左闭右开）之外的记录可以不交出，也可以照常交出
    /// （由调用方过滤）。文件中没有要读的行组（`_timestamp` 上下界都与 `time` 不相交）时为 `None`，即不读任何行组数据，
    /// 该文件不算被扫描（REQ-AMB-QRY-SEARCH-ENDPOINT-V10）。打开时的失败在返回之前报出，解码中途的失败作为迭代中的
    /// 一项报出（[`FileError::Integrity`]）。
    fn read<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a FileEntry,
        columns: &'a [String],
        time: Range<i64>,
    ) -> BoxFuture<'a, Result<Option<FileBatches>, FileError>>;

    /// `file` 中 `picks` 各列逐行组的等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM，只读尾部元数据与 bloom，不算扫描）；
    /// 读不到或尾部元数据无法解析为 `None`，即不下结论、照常整文件读取。缺省为 `None`（替身不提供 bloom）。
    fn blooms<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a FileEntry,
        picks: &'a [Pick],
    ) -> BoxFuture<'a, Option<FileBlooms>> {
        let _ = (dataset, file, picks);
        Box::pin(async { None })
    }
}

/// 从对象存储读取数据文件。对象状态的判定（不存在、不可访问）交给 [`Store::read`]。
pub struct StoreFiles {
    store: Store,
}

impl StoreFiles {
    pub fn new(store: Store) -> Self {
        StoreFiles { store }
    }

    /// 只解码 `columns` 所列的列（另加 `_timestamp`，以便没有别的列时仍按行数交出批次），只读 `_timestamp` 上下界与
    /// `time` 相交的行组（[`scan::row_groups_in`]）；一个也没有时为 `None`。
    async fn read_file(
        &self,
        dataset: &Dataset,
        file: &FileEntry,
        columns: &[String],
        time: Range<i64>,
    ) -> Result<Option<FileBatches>, FileError> {
        let path = self.store.object_path(dataset, &file.path);
        let bytes = match self.store.read(&path).await {
            Ok(Some(fetched)) => fetched.bytes,
            Ok(None) => {
                return Err(FileError::NotFound(format!(
                    "data file {path} does not exist"
                )));
            }
            Err(e) => return Err(FileError::Unavailable(e.to_string())),
        };
        let not_parquet = move |e: parquet::errors::ParquetError| {
            FileError::Integrity(format!("data file {path} is not a valid Parquet file: {e}"))
        };
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).map_err(&not_parquet)?;
        let roots: Vec<usize> = builder
            .schema()
            .fields()
            .iter()
            .enumerate()
            .filter(|(_, f)| f.name() == TIMESTAMP_COLUMN || columns.contains(f.name()))
            .map(|(i, _)| i)
            .collect();
        let ts = builder.schema().index_of(TIMESTAMP_COLUMN).ok();
        let mask = ProjectionMask::roots(builder.parquet_schema(), roots);
        let builder = builder.with_projection(mask).with_batch_size(BATCH_ROWS);
        let groups = match ts {
            Some(ts) => scan::row_groups_in(builder.metadata(), ts, &time),
            None => (0..builder.metadata().num_row_groups()).collect(),
        };
        if groups.is_empty() {
            return Ok(None);
        }
        let reader = builder
            .with_row_groups(groups)
            .build()
            .map_err(&not_parquet)?;
        Ok(Some(Box::new(
            reader.map(move |b| b.map_err(|e| not_parquet(e.into()))),
        )))
    }
}

impl DataFiles for StoreFiles {
    fn read<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a FileEntry,
        columns: &'a [String],
        time: Range<i64>,
    ) -> BoxFuture<'a, Result<Option<FileBatches>, FileError>> {
        Box::pin(self.read_file(dataset, file, columns, time))
    }

    fn blooms<'a>(
        &'a self,
        dataset: &'a Dataset,
        file: &'a FileEntry,
        picks: &'a [Pick],
    ) -> BoxFuture<'a, Option<FileBlooms>> {
        Box::pin(async move {
            let object = StoreObject {
                store: &self.store,
                path: self.store.object_path(dataset, &file.path),
            };
            bloom::probe(&object, file.size_bytes, picks).await
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use amberlog_storage::chain::{Digest, SnapshotId};
    use amberlog_storage::store::S3Options;
    use arrow::array::{Int64Array, TimestampMicrosecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::path::PathBuf;

    use amberlog_storage::store::Location;

    fn entry(path: &str) -> FileEntry {
        FileEntry {
            path: path.into(),
            min_timestamp: 1,
            max_timestamp: 2,
            record_count: 2,
            size_bytes: 0,
            content_digest: Digest([0; 32]),
            added_in: SnapshotId(1),
            column_stats: None,
        }
    }

    fn tempdir(tag: &str) -> PathBuf {
        let d =
            std::env::temp_dir().join(format!("amberlog-query-files-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[tokio::test]
    async fn reads_parquet_and_classifies_failures() {
        let dir = tempdir("classify");
        let location = Location::File { dir: dir.clone() };
        let store = Store::open(&location, &S3Options::default(), "amberlog").unwrap();
        let files = StoreFiles::new(store);
        let dataset = Dataset::new("default", "app");
        let data = dir.join("amberlog/default/app/data/2026/08/22/10");
        std::fs::create_dir_all(&data).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "_timestamp",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                false,
            ),
            Field::new("a", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(TimestampMicrosecondArray::from(vec![1, 2]).with_timezone("UTC")),
                Arc::new(Int64Array::from(vec![Some(7), None])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        std::fs::write(data.join("f1.parquet"), &buf).unwrap();
        std::fs::write(data.join("bad.parquet"), b"not parquet").unwrap();

        let read = |path: &'static str, columns: Vec<String>, time: Range<i64>| {
            let files = &files;
            let dataset = &dataset;
            async move {
                let e = entry(path);
                let batches = files.read(dataset, &e, &columns, time).await?;
                Ok(match batches {
                    Some(b) => Some(b.collect::<Result<Vec<_>, _>>()?),
                    None => None,
                })
            }
        };
        let f1 = "data/2026/08/22/10/f1.parquet";
        let all = i64::MIN..i64::MAX;
        let got = read(f1, vec!["a".into()], all.clone())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(got.iter().map(|b| b.num_rows()).sum::<usize>(), 2);
        assert_eq!(got[0].num_columns(), 2);
        // 只解码用到的列；`_timestamp` 总在
        let got = read(f1, vec![], all.clone()).await.unwrap().unwrap();
        assert_eq!(got[0].num_columns(), 1);
        assert_eq!(got[0].schema().field(0).name(), "_timestamp");
        // 行组的 `_timestamp` 上下界 [1, 2] 与查询范围不相交：整个行组跳过，没有要读的行组
        let got = read(f1, vec!["a".into()], 3..10).await.unwrap();
        assert!(got.is_none());

        let missing = read("data/2026/08/22/10/nope.parquet", vec![], all.clone()).await;
        assert!(
            matches!(missing, Err(FileError::NotFound(_))),
            "{missing:?}"
        );
        let bad = read("data/2026/08/22/10/bad.parquet", vec![], all.clone()).await;
        assert!(matches!(bad, Err(FileError::Integrity(_))), "{bad:?}");

        // `file://` 的根目录不在了：不可访问，不是对象不存在
        std::fs::remove_dir_all(&dir).unwrap();
        let gone = read(f1, vec![], all).await;
        assert!(matches!(gone, Err(FileError::Unavailable(_))), "{gone:?}");
    }

    /// 文件的 `_timestamp` 上下界与查询范围相交、但每个行组的上下界都不相交（两个行组之间的空档）时，没有要读的行组，
    /// 回 `None`：该文件不算被扫描（REQ-AMB-QRY-SEARCH-ENDPOINT-V10）。与某个行组相交时只读那个行组。
    #[tokio::test]
    async fn no_row_group_in_range_reads_none() {
        let dir = tempdir("gap");
        let location = Location::File { dir: dir.clone() };
        let store = Store::open(&location, &S3Options::default(), "amberlog").unwrap();
        let files = StoreFiles::new(store);
        let dataset = Dataset::new("default", "app");
        let data = dir.join("amberlog/default/app/data/2026/08/22/10");
        std::fs::create_dir_all(&data).unwrap();

        let schema = Arc::new(Schema::new(vec![Field::new(
            "_timestamp",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            false,
        )]));
        // 两个行组：`_timestamp` 依次为 0、9 与 100、109
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(
                TimestampMicrosecondArray::from(vec![0, 9, 100, 109]).with_timezone("UTC"),
            )],
        )
        .unwrap();
        let props = parquet::file::properties::WriterProperties::builder()
            .set_max_row_group_row_count(Some(2))
            .build();
        let mut buf = Vec::new();
        let mut w = ArrowWriter::try_new(&mut buf, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        std::fs::write(data.join("gap.parquet"), &buf).unwrap();

        let e = FileEntry {
            min_timestamp: 0,
            max_timestamp: 109,
            record_count: 4,
            ..entry("data/2026/08/22/10/gap.parquet")
        };
        let got = files.read(&dataset, &e, &[], 50..60).await.unwrap();
        assert!(got.is_none(), "查询范围落在两个行组之间的空档里");
        let rows: usize = files
            .read(&dataset, &e, &[], 5..10)
            .await
            .unwrap()
            .expect("第一个行组与查询范围相交")
            .map(|b| b.unwrap().num_rows())
            .sum();
        assert_eq!(rows, 2, "只读第一个行组");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
