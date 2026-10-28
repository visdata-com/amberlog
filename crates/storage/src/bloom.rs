//! 等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM）的写方取列与读方取 bloom。
//!
//! - 写方：[`bloom_columns`] 按写出进程启动时读入的索引提示（[`IndexHints`]）与热层快照 `schema` 算出可写 bloom 的列
//!   （TERM-DECLARED、TERM-ELIGIBLE），三种输出（封口合并输出、入湖写出的冷层数据文件、冷层合并产出）各自只为本文件
//!   含有的那些列打开 Parquet 标准的 Split Block Bloom Filter（[`with_blooms`]）；flush 不写。bloom 写在文件末尾、
//!   与尾部元数据相邻，误判率取 `parquet` 默认值，NDV 取「本文件记录数」与「行组行数上限」中较小者（ADR-0050 第 8 条）。
//! - 读方：[`probe`] 以范围读取取文件尾部元数据与所需列块的 bloom，逐行组给出每列的 bloom（判定在 `amberlog-query`）。
//!   尾部元数据读不到或无法解析时整体为 `None`（不下结论，交给整文件读取按原有分类回应）；某列块的 bloom 取不到完整
//!   字节或不是合法的 Split Block Bloom Filter 时该处为 `None`，即视为没有 bloom（READ-BROKEN）。
//!   不用 `Sbbf::read_from_column_chunk`：它对空位集与长度不是块整数倍的位集不报错（前者在 `check` 时越界 panic）。

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Range;

use amberlog_common::dataset::Dataset;
use amberlog_common::schema::{ColumnType, TIMESTAMP_COLUMN};
use bytes::Bytes;
use parquet::bloom_filter::{BITSET_MAX_LENGTH, Sbbf};
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{
    BloomFilterPosition, DEFAULT_MAX_ROW_GROUP_ROW_COUNT, WriterPropertiesBuilder,
};
use parquet::schema::types::ColumnPath;

use object_store::path::Path;

use crate::BoxFuture;
use crate::schema::{FieldId, SchemaColumn};
use crate::store::Store;

/// 本进程启动时读入的索引提示（REQ-AMB-DM-INDEX-HINTS-V2）：数据集名 `default/<stream>` → 要建索引的列名。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct IndexHints(BTreeMap<String, Vec<String>>);

impl IndexHints {
    pub fn new(hints: BTreeMap<String, Vec<String>>) -> IndexHints {
        IndexHints(hints)
    }

    /// `dataset` 被声明的列名；没有声明为空。
    pub fn declared(&self, dataset: &Dataset) -> &[String] {
        self.0.get(&dataset.to_string()).map_or(&[], Vec::as_slice)
    }
}

/// 一次写出要写 bloom 的列（字段编号）。写出方只为本文件含有的列写，即 TERM-ELIGIBLE「该数据文件含有该列」一项。
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BloomColumns(BTreeSet<FieldId>);

impl BloomColumns {
    /// 不写任何列（flush）。
    pub fn none() -> BloomColumns {
        BloomColumns::default()
    }

    pub fn from_ids(ids: impl IntoIterator<Item = FieldId>) -> BloomColumns {
        BloomColumns(ids.into_iter().collect())
    }

    pub fn contains(&self, id: FieldId) -> bool {
        self.0.contains(&id)
    }

    pub fn ids(&self) -> impl Iterator<Item = FieldId> + '_ {
        self.0.iter().copied()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// 可写 bloom 的列（TERM-DECLARED、TERM-ELIGIBLE）：`schema`（热层快照 `schema`）中被 `hints` 为 `dataset` 声明
/// （按热层列名逐字符比对，影子列按其自身列名）、`type` 为 `Utf8` 或 `Int64`、不是 `_timestamp` 的列。
pub fn bloom_columns(
    hints: &IndexHints,
    dataset: &Dataset,
    schema: &[SchemaColumn],
) -> BloomColumns {
    let declared = hints.declared(dataset);
    BloomColumns::from_ids(
        schema
            .iter()
            .filter(|c| c.name != TIMESTAMP_COLUMN)
            .filter(|c| matches!(c.column_type, ColumnType::Utf8 | ColumnType::Int64))
            .filter(|c| declared.contains(&c.name))
            .map(|c| c.field_id),
    )
}

/// 为 Parquet 列 `columns`（物理列名）打开等值 bloom；`rows` 为本文件记录数。没有列时原样返回。
pub fn with_blooms<'a>(
    mut builder: WriterPropertiesBuilder,
    columns: impl IntoIterator<Item = &'a str>,
    rows: usize,
) -> WriterPropertiesBuilder {
    // NDV 按列对整个写出器生效、不按行组，每次写出按本文件记录数设置（ADR-0050 第 8 条）；写库按它定初始大小，
    // 写完再折叠到误判率所需的最小大小
    let ndv = rows.min(DEFAULT_MAX_ROW_GROUP_ROW_COUNT) as u64;
    let mut any = false;
    for name in columns {
        let path = ColumnPath::from(name);
        builder = builder
            .set_column_bloom_filter_enabled(path.clone(), true)
            .set_column_bloom_filter_max_ndv(path, ndv);
        any = true;
    }
    if any {
        builder = builder.set_bloom_filter_position(BloomFilterPosition::End);
    }
    builder
}

/// 判定时要取 bloom 的一列：热层数据文件按列名（与热层读取按列名对齐一致），冷层数据文件按 `PARQUET:field_id`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pick {
    Name(String),
    FieldId(FieldId),
}

/// 一个数据文件逐行组、按 [`Pick`] 顺序的等值 bloom；`None` 为该行组该列没有 bloom（含无法解析）。
pub type FileBlooms = Vec<Vec<Option<Sbbf>>>;

/// 按字节范围读一个对象：`[range.start, range.end)` 的完整字节；对象不存在、范围越过对象末尾、对象存储不可访问
/// 或读到的字节数不符都为 `None`。
pub trait RangeRead: Send + Sync {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Option<Bytes>>;
}

/// 先读的尾部字节数：bloom 写在文件末尾、与尾部元数据相邻，小文件一次范围读取即可取全。
const TAIL_BYTES: u64 = 64 * 1024;

/// 取 `size` 字节的数据文件中 `picks` 各列逐行组的等值 bloom。尾部元数据读不到或无法解析为 `None`。
pub async fn probe(source: &dyn RangeRead, size: u64, picks: &[Pick]) -> Option<FileBlooms> {
    if size < 12 {
        return None;
    }
    let tail_start = size.saturating_sub(TAIL_BYTES);
    let tail = source.read_range(tail_start..size).await?;
    let footer = &tail[tail.len() - 8..];
    if &footer[4..] != b"PAR1" {
        return None;
    }
    let meta_len = u64::from(u32::from_le_bytes(footer[..4].try_into().ok()?));
    if meta_len + 8 > size {
        return None;
    }
    let meta_start = size - 8 - meta_len;
    let meta_bytes = if meta_start >= tail_start {
        tail.slice((meta_start - tail_start) as usize..(size - 8 - tail_start) as usize)
    } else {
        source.read_range(meta_start..size - 8).await?
    };
    let metadata = ParquetMetaDataReader::decode_metadata(&meta_bytes).ok()?;
    let columns = pick_columns(&metadata, picks);

    let mut out = Vec::with_capacity(metadata.num_row_groups());
    for group in metadata.row_groups() {
        let mut blooms = Vec::with_capacity(picks.len());
        for column in &columns {
            let bloom = match column {
                Some(i) => {
                    let chunk = group.column(*i);
                    match (chunk.bloom_filter_offset(), chunk.bloom_filter_length()) {
                        (Some(offset), Some(length)) => {
                            read_bloom(source, &tail, tail_start, meta_start, offset, length).await
                        }
                        // 未写长度（Parquet 2.10 之前的写法）：本系统写出的文件都带长度，按没有 bloom 处置
                        _ => None,
                    }
                }
                None => None,
            };
            blooms.push(bloom);
        }
        out.push(blooms);
    }
    Some(out)
}

/// `picks` 各列在 Parquet schema 中的叶子列序号；文件中没有该列为 `None`。只认顶层列。
fn pick_columns(metadata: &ParquetMetaData, picks: &[Pick]) -> Vec<Option<usize>> {
    let schema = metadata.file_metadata().schema_descr();
    picks
        .iter()
        .map(|pick| {
            (0..schema.num_columns()).find(|&i| {
                let column = schema.column(i);
                if column.path().parts().len() != 1 {
                    return false;
                }
                match pick {
                    Pick::Name(name) => column.name() == name,
                    Pick::FieldId(id) => {
                        let info = column.self_type().get_basic_info();
                        info.has_id() && i64::from(info.id()) == i64::from(*id)
                    }
                }
            })
        })
        .collect()
}

/// 读出并严格解析 `[offset, offset + length)` 处的 bloom：必须整个落在尾部元数据之前，头部合法、位集恰为头部所写
/// 字节数且是不少于一个块的整数倍（按库的写法重新序列化后与原字节逐字节相同）。不合即 `None`。
async fn read_bloom(
    source: &dyn RangeRead,
    tail: &Bytes,
    tail_start: u64,
    meta_start: u64,
    offset: i64,
    length: i32,
) -> Option<Sbbf> {
    let offset = u64::try_from(offset).ok()?;
    let length = u64::try_from(length).ok()?;
    if length == 0 || length > BITSET_MAX_LENGTH as u64 + 64 {
        return None;
    }
    let end = offset.checked_add(length)?;
    if end > meta_start {
        return None;
    }
    let bytes = if offset >= tail_start {
        tail.slice((offset - tail_start) as usize..(end - tail_start) as usize)
    } else {
        source.read_range(offset..end).await?
    };
    let bloom = Sbbf::from_bytes(&bytes).ok()?;
    if bloom.num_blocks() == 0 {
        return None;
    }
    let mut again = Vec::with_capacity(bytes.len());
    bloom.write(&mut again).ok()?;
    (again == bytes.as_ref()).then_some(bloom)
}

/// 对象存储上一个对象的 [`RangeRead`]。
pub struct StoreObject<'a> {
    pub store: &'a Store,
    pub path: Path,
}

impl RangeRead for StoreObject<'_> {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Option<Bytes>> {
        Box::pin(self.store.read_range(&self.path, range))
    }
}

/// 内存中完整字节的 [`RangeRead`]（单元测试与已读出整文件的调用方用）。
pub struct InMemory(pub Bytes);

impl RangeRead for InMemory {
    fn read_range(&self, range: Range<u64>) -> BoxFuture<'_, Option<Bytes>> {
        let got = (range.end <= self.0.len() as u64 && range.start <= range.end)
            .then(|| self.0.slice(range.start as usize..range.end as usize));
        Box::pin(async move { got })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    fn col(name: &str, t: ColumnType, id: FieldId) -> SchemaColumn {
        SchemaColumn {
            name: name.into(),
            column_type: t,
            field_id: id,
        }
    }

    #[test]
    fn bloom_columns_declared_eligible_only() {
        let hints = IndexHints::new(BTreeMap::from([(
            "default/app".to_owned(),
            [
                "trace_id",
                "user_id",
                "ratio",
                "ok",
                "_timestamp",
                "Trace_Id",
                "gone",
            ]
            .map(String::from)
            .to_vec(),
        )]));
        let schema = [
            col("_timestamp", ColumnType::Int64, 1),
            col("trace_id", ColumnType::Utf8, 2),
            col("user_id", ColumnType::Int64, 3),
            col("ratio", ColumnType::Float64, 4),
            col("ok", ColumnType::Boolean, 5),
            col("msg", ColumnType::Utf8, 6),
            col("trace_ID", ColumnType::Utf8, 7),
            col("Trace_Id", ColumnType::Utf8, 8),
        ];
        let app = Dataset::new("default", "app");
        assert_eq!(
            bloom_columns(&hints, &app, &schema),
            BloomColumns::from_ids([2, 3, 8])
        );
        let other = Dataset::new("default", "other");
        assert!(bloom_columns(&hints, &other, &schema).is_empty());
    }

    fn file(props: WriterProperties, trace: Vec<Option<&str>>) -> Bytes {
        let n = trace.len() as i64;
        let schema = Arc::new(Schema::new(vec![
            Field::new("trace_id", DataType::Utf8, true),
            Field::new("n", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(StringArray::from(trace)) as ArrayRef,
                Arc::new(Int64Array::from_iter_values(0..n)),
            ],
        )
        .unwrap();
        let mut w = ArrowWriter::try_new(Vec::new(), schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        Bytes::from(w.into_inner().unwrap())
    }

    fn bloomed(rows: usize) -> WriterProperties {
        with_blooms(WriterProperties::builder(), ["trace_id"], rows).build()
    }

    #[tokio::test]
    async fn probe_reads_written_bloom() {
        let bytes = file(
            bloomed(3),
            vec![Some("t-100"), Some("t-200"), Some("t-300")],
        );
        let picks = [Pick::Name("trace_id".into()), Pick::Name("n".into())];
        let got = probe(&InMemory(bytes.clone()), bytes.len() as u64, &picks)
            .await
            .unwrap();
        assert_eq!(got.len(), 1);
        let bloom = got[0][0].as_ref().unwrap();
        assert!(bloom.check("t-200"));
        assert!(got[0][1].is_none(), "未声明的列没有 bloom");
    }

    /// 写库对全为 NULL 的列块照写 bloom，且对任何字面量都判肯定不在（本测试锁定这一上游行为）。
    #[tokio::test]
    async fn all_null_column_chunk_has_bloom() {
        let bytes = file(bloomed(2), vec![None, None]);
        let got = probe(
            &InMemory(bytes.clone()),
            bytes.len() as u64,
            &[Pick::Name("trace_id".into())],
        )
        .await
        .unwrap();
        let bloom = got[0][0].as_ref().expect("全为 NULL 的列块也有 bloom");
        assert!(!bloom.check("t-200"));
    }

    #[tokio::test]
    async fn probe_footer_unreadable_none() {
        let bytes = file(bloomed(1), vec![Some("t-100")]);
        let truncated = bytes.slice(..bytes.len() - 3);
        assert!(
            probe(&InMemory(truncated.clone()), truncated.len() as u64, &[])
                .await
                .is_none()
        );
        // 所给大小比对象大：范围读取取不到
        assert!(
            probe(&InMemory(bytes.clone()), bytes.len() as u64 + 10, &[])
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn probe_corrupt_bloom_bytes_none() {
        let bytes = file(
            bloomed(3),
            vec![Some("t-100"), Some("t-200"), Some("t-300")],
        );
        let meta = ParquetMetaDataReader::new()
            .parse_and_finish(&bytes)
            .unwrap();
        let chunk = meta.row_group(0).column(0);
        let offset = chunk.bloom_filter_offset().unwrap() as usize;
        let mut broken = bytes.to_vec();
        // 头部改成不是合法 thrift 的内容
        broken[offset..offset + 4].copy_from_slice(&[0xff; 4]);
        let broken = Bytes::from(broken);
        let got = probe(
            &InMemory(broken.clone()),
            broken.len() as u64,
            &[Pick::Name("trace_id".into())],
        )
        .await
        .unwrap();
        assert!(got[0][0].is_none());
    }
}
