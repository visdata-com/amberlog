//! 证据的只读读取（导出与查验共用）：直接按块定义的读法读对象存储，**不经快照提交与 [`crate::reader::StoreChain`]、
//! 不登记停用状态**（证据不改变任何实例的状态，REQ-AMB-STO-EVIDENCE-EXPORT）。
//!
//! 每次读取都把结果按块分开：对象不存在、内容可读（交回完整字节，由调用方判摘要与能否解析）、对象存储不可访问
//! （[`Unavailable`]，调用方按 UNAVAILABLE 中止）。冷层对象的 URI 一律去掉所读元数据的 `location` 及其后的 `/`，接在
//! 按配置推算的冷层表目录之后定位（REQ-AMB-STO-EVIDENCE-EXPORT-PIN-COLD、REQ-AMB-STO-EVIDENCE-VERIFY-TABLE-LOCATION）。

use amberlog_common::dataset::Dataset;
use bytes::Bytes;
use iceberg::spec::{
    FormatVersion, Manifest, ManifestContentType, ManifestEntry, ManifestFile, ManifestList,
    ManifestStatus, TableMetadata,
};
use object_store::ObjectStoreExt;
use object_store::path::Path;

use crate::chain::{self, PieceRef, PieceWalk, Restored, SnapshotId};
use crate::cold::layout::ColdLayout;
use crate::format;
use crate::layout;
use crate::store::{Store, Unavailable};

/// 自头片 `head` 起读出桶 `bucket` 的全部清单片（逐片按引用者所记摘要核对，REQ-AMB-STO-HOT-LAYOUT-V8「还原」），
/// 返回各片的读取与核对结果，以及据此还原所得的桶 manifest（摘要不符的片照常使用其内容；有片不存在或无法解析时为
/// `None`）。`version` 为引用它的快照的格式版本。
pub async fn bucket_manifest(
    store: &Store,
    dataset: &Dataset,
    bucket: &str,
    head: &PieceRef,
    version: u64,
) -> Result<(PieceWalk, Option<Restored>), Unavailable> {
    let walk = chain::walk_pieces(
        head,
        version,
        bucket,
        |rel| async move { get(store, &store.object_path(dataset, &rel)).await },
        |_| None,
    )
    .await?;
    let restored = chain::restore(&walk, version, bucket).ok();
    Ok((walk, restored))
}

/// 读取整个对象；不存在为 `None`。
pub async fn get(store: &Store, path: &Path) -> Result<Option<Bytes>, Unavailable> {
    Ok(store.read(path).await?.map(|f| f.bytes))
}

/// 读取对象元数据（HEAD）得字节数；不存在为 `None`。不读内容（REQ-AMB-STO-EVIDENCE-EXPORT-CHECK-LIVE-HEAD）。
pub async fn head_size(store: &Store, path: &Path) -> Result<Option<u64>, Unavailable> {
    match store.inner.head(path).await {
        Ok(meta) => Ok(Some(meta.size)),
        Err(object_store::Error::NotFound { .. }) => {
            if let Some(dir) = &store.local_dir
                && !dir.is_dir()
            {
                return Err(Unavailable(format!(
                    "object store directory {} does not exist",
                    dir.display()
                )));
            }
            Ok(None)
        }
        Err(e) => Err(Unavailable(format!(
            "object store head of {path} failed: {e}"
        ))),
    }
}

/// 找链尾的结果（REQ-AMB-STO-MANIFEST-COMMIT 的找链尾规则；提示无法解析按缺失处理，REQ-AMB-STO-EVIDENCE-EXPORT-HINT-UNUSABLE）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tail {
    /// 当前快照的编号。
    Current(SnapshotId),
    /// 提示缺失或无法解析、从 1 号起探测尚无任何快照，而列举 `manifest/` 见到了快照文件。
    HintUnusable,
    /// 提示缺失或无法解析、尚无任何快照，列举也没见到快照文件。
    NoSnapshot,
    /// 提示所指快照之后没有快照（当前快照即提示所指的 h）；h 是否存在由读取它时判定。
    Hint(SnapshotId),
}

/// 数据集 `dataset` 此刻的热层链尾。
pub async fn find_tail(store: &Store, dataset: &Dataset) -> Result<Tail, Unavailable> {
    let hint = get(store, &store.object_path(dataset, layout::CURRENT))
        .await?
        .and_then(|b| format::decode_current(&b).ok());
    let mut current = hint;
    loop {
        let next = SnapshotId(current.map_or(1, |c| c.0 + 1));
        let p = store.object_path(dataset, &layout::snapshot_path(next));
        if !store.exists(&p).await? {
            break;
        }
        current = Some(next);
    }
    Ok(match (hint, current) {
        (Some(h), Some(c)) if c == h => Tail::Hint(h),
        (_, Some(c)) => Tail::Current(c),
        (_, None) => {
            let listed = store
                .list(&store.object_path(dataset, layout::MANIFEST_DIR))
                .await?;
            let ds = store.dataset_prefix(dataset);
            let any = listed.iter().any(|m| {
                rel_of(&ds, &m.location).is_some_and(|r| layout::parse_snapshot_path(&r).is_some())
            });
            if any {
                Tail::HintUnusable
            } else {
                Tail::NoSnapshot
            }
        }
    })
}

/// 对象键 `path` 相对前缀 `prefix` 的路径（不在其下为 `None`）。
pub fn rel_of(prefix: &Path, path: &Path) -> Option<String> {
    let rest: Vec<_> = path
        .prefix_match(prefix)?
        .map(|p| p.as_ref().to_owned())
        .collect();
    (!rest.is_empty()).then(|| rest.join("/"))
}

/// 数据集 `dataset` 在对象存储中是否存在（REQ-AMB-DM-NAMESPACE-V2：其 `manifest/` 下有提示或符合快照路径模板的对象）。
/// 只发一次列举请求（离线子命令的对象存储读取探测，REQ-AMB-CFG-STARTUP-V7）。
pub async fn dataset_exists(store: &Store, dataset: &Dataset) -> Result<bool, Unavailable> {
    let listed = store
        .list(&store.object_path(dataset, layout::MANIFEST_DIR))
        .await?;
    let ds = store.dataset_prefix(dataset);
    Ok(listed.iter().any(|m| {
        rel_of(&ds, &m.location)
            .is_some_and(|r| r == layout::CURRENT || layout::parse_snapshot_path(&r).is_some())
    }))
}

/// 冷层表元数据所记 URI 相对其 `location` 的路径（去掉 `location` 及其后的 `/`）；不以 `location` 开头为 `None`。
pub fn rel_of_uri<'a>(location: &str, uri: &'a str) -> Option<&'a str> {
    uri.strip_prefix(location.trim_end_matches('/'))?
        .strip_prefix('/')
        .filter(|r| !r.is_empty())
}

/// 冷层的一次读取结果。
#[derive(Debug)]
pub enum ColdRead<T> {
    Ok(T),
    /// 对象不存在（相对冷层表目录的路径）。
    Missing(String),
    /// 内容无法解析（相对冷层表目录的路径，原因）。
    Unparseable(String, String),
    /// URI 不以元数据的 `location` 开头（只可能是外部写入）：不读取。
    Outside(String),
}

/// 读冷层表目录下 `metadata/v<n>.metadata.json` 的完整字节与解析结果。
pub async fn cold_metadata(
    store: &Store,
    cold: &ColdLayout,
    dataset: &Dataset,
    n: u64,
) -> Result<ColdRead<(Bytes, TableMetadata)>, Unavailable> {
    let rel = crate::cold::layout::metadata_path(n);
    let Some(bytes) = get(store, &cold.object_path(dataset, &rel)).await? else {
        return Ok(ColdRead::Missing(rel));
    };
    Ok(match crate::cold::current::parse_metadata(&bytes) {
        Ok(m) => ColdRead::Ok((bytes, m)),
        Err(e) => ColdRead::Unparseable(rel, e),
    })
}

/// 读元数据 `location` 之下的 URI `uri` 所指对象（按配置推算的冷层表目录定位）。
async fn cold_uri(
    store: &Store,
    cold: &ColdLayout,
    dataset: &Dataset,
    location: &str,
    uri: &str,
) -> Result<ColdRead<(String, Bytes)>, Unavailable> {
    let Some(rel) = rel_of_uri(location, uri) else {
        return Ok(ColdRead::Outside(uri.to_owned()));
    };
    Ok(match get(store, &cold.object_path(dataset, rel)).await? {
        Some(b) => ColdRead::Ok((rel.to_owned(), b)),
        None => ColdRead::Missing(rel.to_owned()),
    })
}

/// 读冷层快照的清单列表。
pub async fn manifest_list(
    store: &Store,
    cold: &ColdLayout,
    dataset: &Dataset,
    location: &str,
    uri: &str,
) -> Result<ColdRead<Vec<ManifestFile>>, Unavailable> {
    Ok(match cold_uri(store, cold, dataset, location, uri).await? {
        ColdRead::Ok((rel, bytes)) => {
            match ManifestList::parse_with_version(&bytes, FormatVersion::V2) {
                Ok(list) => ColdRead::Ok(list.entries().to_vec()),
                Err(e) => ColdRead::Unparseable(rel, e.to_string()),
            }
        }
        ColdRead::Missing(r) => ColdRead::Missing(r),
        ColdRead::Unparseable(r, e) => ColdRead::Unparseable(r, e),
        ColdRead::Outside(u) => ColdRead::Outside(u),
    })
}

/// 读清单 `file` 的全部条目，按 Iceberg 第 2 版的继承规则补上快照号与序号。
pub async fn manifest_entries(
    store: &Store,
    cold: &ColdLayout,
    dataset: &Dataset,
    location: &str,
    file: &ManifestFile,
) -> Result<ColdRead<Vec<ManifestEntry>>, Unavailable> {
    Ok(
        match cold_uri(store, cold, dataset, location, &file.manifest_path).await? {
            ColdRead::Ok((rel, bytes)) => {
                if file.content != ManifestContentType::Data {
                    return Ok(ColdRead::Unparseable(rel, "delete manifest".into()));
                }
                match Manifest::parse_avro(&bytes) {
                    Ok(m) => {
                        let (entries, _) = m.into_parts();
                        ColdRead::Ok(
                            entries
                                .into_iter()
                                .map(|e| {
                                    let mut e = (*e).clone();
                                    e.snapshot_id.get_or_insert(file.added_snapshot_id);
                                    e.sequence_number.get_or_insert(file.sequence_number);
                                    e.file_sequence_number.get_or_insert(file.sequence_number);
                                    e
                                })
                                .collect(),
                        )
                    }
                    Err(e) => ColdRead::Unparseable(rel, e.to_string()),
                }
            }
            ColdRead::Missing(r) => ColdRead::Missing(r),
            ColdRead::Unparseable(r, e) => ColdRead::Unparseable(r, e),
            ColdRead::Outside(u) => ColdRead::Outside(u),
        },
    )
}

/// 清单条目所记的一个冷层数据文件。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntryFile {
    /// 相对冷层表位置；URI 不以元数据 `location` 开头时为 `None`。
    pub rel: Option<String>,
    /// 清单条目的 `file_path`（完整 URI）。
    pub uri: String,
    pub status: ManifestStatus,
    /// 写入该条目的冷层快照（已按继承规则补上）。
    pub snapshot_id: i64,
    pub record_count: u64,
    pub size_bytes: u64,
    /// `_timestamp`（字段编号 1）的下界与上界；条目没有时为 `None`。
    pub min_timestamp: Option<i64>,
    pub max_timestamp: Option<i64>,
}

impl EntryFile {
    pub fn of(location: &str, e: &ManifestEntry) -> EntryFile {
        use iceberg::spec::{Datum, PrimitiveLiteral};
        let d = e.data_file();
        let ts = |b: Option<&Datum>| match b.map(Datum::literal) {
            Some(PrimitiveLiteral::Long(v)) => Some(*v),
            _ => None,
        };
        EntryFile {
            rel: rel_of_uri(location, e.file_path()).map(str::to_owned),
            uri: e.file_path().to_owned(),
            status: e.status(),
            snapshot_id: e.snapshot_id().unwrap_or_default(),
            record_count: d.record_count(),
            size_bytes: d.file_size_in_bytes(),
            min_timestamp: ts(d.lower_bounds().get(&1)),
            max_timestamp: ts(d.upper_bounds().get(&1)),
        }
    }

    /// 与 [from, to) 相交（全时段一律相交；没有上下界时按相交处理）。
    pub fn intersects(&self, range: Option<(i64, i64)>) -> bool {
        match (range, self.min_timestamp, self.max_timestamp) {
            (Some((from, to)), Some(lo), Some(hi)) => lo < to && hi >= from,
            _ => true,
        }
    }
}
