//! 证据导出的核心（REQ-AMB-STO-EVIDENCE-EXPORT B～F 组）：钉住链头、确定证据范围、核对元数据、写出证据包各成员。
//!
//! 证据包的成员经 [`Sink`] 逐个写出：先写钉住的快照副本，在暂停点 `evidence.after_partial_write` 之后再读其余对象，
//! 所以对象存储在途中不可访问、收到信号时临时文件里已有一部分内容（块场景「失败退出时不留半个证据包」）。
//! 临时文件、改名、信号与退出码由调用方（`amberlog-server` 的离线子命令编排）处置。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io;

use amberlog_common::dataset::Dataset;
use iceberg::spec::{ManifestStatus, TableMetadata};
use serde_json::{Value, json};

use super::package::{
    self, Check, ColdHead, DeletionInfo, EVIDENCE_FORMAT_VERSION, FileLine, Heads, OBJECTS_DIR,
    ObjectRef, Receipt, SnapshotHead, Tier, note, problem, transition,
};
use super::read::{self, ColdRead, EntryFile, Tail};
use crate::chain::{
    BucketManifest, ColdChangeSegment, ColdDeletionReason, ColdFileEntry, DeletionReason, Digest,
    PieceProblem, PieceRef, RetiredSegment, SegmentRef, Snapshot, SnapshotId,
};
use crate::cold::layout::ColdLayout;
use crate::format::{self, SnapshotContent};
use crate::layout::{self, HourBucket};
use crate::store::{Store, Unavailable};

/// 证据包成员的写出处（调用方把它们写进临时文件里的 tar）。
pub trait Sink {
    fn member(&mut self, name: &str, data: &[u8]) -> io::Result<()>;
    /// 把已写出的成员推到文件里（暂停点之前调用，使临时文件里确有内容）。
    fn flush(&mut self) -> io::Result<()>;
}

/// 一次导出的请求。
#[derive(Debug, Clone)]
pub struct Request {
    pub dataset: Dataset,
    /// 时间段 [from, to)（UTC 微秒）；`None` 为全时段。
    pub range: Option<(i64, i64)>,
    /// 导出程序的版本（`amberlog --version` 中的 semver）。
    pub amberlog_version: String,
}

/// 写完证据包的结果。
#[derive(Debug, Clone)]
pub struct Exported {
    /// `receipt.json` 的字节（stdout 输出同一份字节再加换行符）。
    pub receipt: Vec<u8>,
    /// `check.json` 的 `problems` 非空（退出码 5）。
    pub problems_found: bool,
}

/// 导出没有做完（退出码 6，写本地文件失败为 1）。
#[derive(Debug)]
pub enum Failed {
    /// 对象存储不可访问（REQ-AMB-STO-EVIDENCE-EXPORT-UNAVAILABLE）。
    Unavailable(String),
    /// 提示缺失或读不懂、而 1 号快照已不在（HINT-UNUSABLE）：提示的路径。
    HintUnusable(String),
    /// 钉住的快照的格式版本不是本程序认识的版本（4）（FORMAT-UNSUPPORTED）：快照路径与格式版本。
    FormatUnsupported(String, u64),
    /// 链头读不到或读不懂（HEAD-UNREADABLE）：对象路径与原因。
    HeadUnreadable(String, String),
    /// 导出期间链已前进、钉住的对象已不存在（PINNED-VANISH）：对象路径。
    Advanced(String),
    /// 冷层清单列表或清单的 URI 不以元数据的 `location` 开头（PIN-COLD，按 UNAVAILABLE 处置）。
    Outside(String),
    /// 写本地证据包失败（WRITE-FAIL）。
    Write(io::Error),
}

impl From<Unavailable> for Failed {
    fn from(e: Unavailable) -> Self {
        Failed::Unavailable(e.0)
    }
}

impl From<io::Error> for Failed {
    fn from(e: io::Error) -> Self {
        Failed::Write(e)
    }
}

/// 读取的一个桶 manifest 从哪里被引用。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Source {
    /// 钉住的快照 `buckets`。
    Snapshot,
    /// 某个留痕归档段的 `buckets`。
    Retired,
}

/// 读到并能使用的桶 manifest。
struct Bucket {
    /// 相对数据集前缀。
    path: String,
    source: Source,
    manifest: BucketManifest,
}

/// 冷层数据文件的一条写入记录（交接记录 `files` 元素或合并记录）。
#[derive(Clone)]
struct Record {
    entry: ColdFileEntry,
    added_in: Option<u64>,
    /// 写入记录所在对象相对 `<storage.prefix>/` 的路径。
    source: String,
}

/// 导出一次的状态。
struct Export<'a> {
    store: &'a Store,
    cold: ColdLayout,
    dataset: &'a Dataset,
    range: Option<(i64, i64)>,
    sink: &'a mut dyn Sink,
    /// 已写出的成员与摘要（`SHA256SUMS` 用）。
    written: Vec<(String, Digest)>,
    check: Check,
    /// 钉住的热层快照号与冷层版本（PINNED-VANISH 判链是否前进）。
    pinned_hot: SnapshotId,
    pinned_cold: u64,
}

/// 导出 `request` 所指数据集的证据（调用方已判定数据集存在、已建好临时文件）。
pub async fn export(
    store: &Store,
    request: &Request,
    sink: &mut dyn Sink,
) -> Result<Exported, Failed> {
    let dataset = &request.dataset;
    let cold = ColdLayout::new(&store.location, store.root.clone());
    // EXPORTED-AT：钉住链头之前读取的系统时钟
    let exported_at = crate::lifecycle::now_us();

    // PIN-SNAPSHOT、HINT-UNUSABLE、FORMAT-UNSUPPORTED、HEAD-UNREADABLE
    let id = match read::find_tail(store, dataset).await? {
        Tail::Current(c) | Tail::Hint(c) => c,
        Tail::HintUnusable => {
            return Err(Failed::HintUnusable(ds_rel(dataset, layout::CURRENT)));
        }
        Tail::NoSnapshot => {
            return Err(Failed::HeadUnreadable(
                ds_rel(dataset, layout::CURRENT),
                "the dataset has no snapshot".into(),
            ));
        }
    };
    let snapshot_rel = layout::snapshot_path(id);
    let snapshot_path = ds_rel(dataset, &snapshot_rel);
    let Some(snapshot_bytes) = read::get(store, &store.object_path(dataset, &snapshot_rel)).await?
    else {
        return Err(Failed::HeadUnreadable(
            snapshot_path,
            "does not exist".into(),
        ));
    };
    let snapshot = match format::decode_snapshot(&snapshot_bytes, dataset, id) {
        Ok(SnapshotContent::Supported(s)) => *s,
        Ok(SnapshotContent::Unsupported { format_version }) => {
            return Err(Failed::FormatUnsupported(snapshot_path, format_version));
        }
        Err(e) => return Err(Failed::HeadUnreadable(snapshot_path, e.0)),
    };

    // PIN-COLD
    let (n, _) = crate::cold::current::version(store, &cold, dataset)
        .await
        .map_err(|e| Failed::Unavailable(e.to_string()))?;
    let cold_meta = if n == 0 {
        None
    } else {
        match read::cold_metadata(store, &cold, dataset, n).await? {
            ColdRead::Ok(m) => Some(m),
            ColdRead::Missing(rel) => {
                return Err(Failed::HeadUnreadable(
                    cold_rel(dataset, &rel),
                    "does not exist".into(),
                ));
            }
            ColdRead::Unparseable(rel, e) => {
                return Err(Failed::HeadUnreadable(cold_rel(dataset, &rel), e));
            }
            ColdRead::Outside(uri) => return Err(Failed::Outside(uri)),
        }
    };

    let heads = Heads {
        snapshot: SnapshotHead {
            snapshot_id: id.0,
            path: snapshot_rel.clone(),
            content_digest: Digest::of(&snapshot_bytes),
        },
        retired_segment: snapshot.retired_segment.as_ref().map(object_ref),
        cold_deletion_segment: snapshot.cold_deletion_segment.as_ref().map(object_ref),
        cold: cold_meta.as_ref().map(|(bytes, _)| ColdHead {
            metadata_version: n,
            path: crate::cold::layout::metadata_path(n),
            content_digest: Digest::of(bytes),
        }),
    };
    let receipt = Receipt {
        dataset: dataset.to_string(),
        exported_at,
        amberlog_version: request.amberlog_version.clone(),
        heads,
    };
    crate::fault::gate("evidence.after_pin").await;

    let mut ex = Export {
        store,
        cold,
        dataset,
        range: request.range,
        sink,
        written: Vec::new(),
        check: Check::default(),
        pinned_hot: id,
        pinned_cold: n,
    };
    ex.copy(&snapshot_path, &snapshot_bytes)?;
    ex.sink.flush()?;
    crate::fault::gate("evidence.after_partial_write").await;
    if let Some((bytes, _)) = &cold_meta {
        ex.copy(
            &cold_rel(dataset, &crate::cold::layout::metadata_path(n)),
            bytes,
        )?;
    }

    // SCOPE-SEGMENTS、CHECK-METADATA（段）
    let retired = ex
        .walk_segments(snapshot.retired_segment.as_ref(), Segment::Retired)
        .await?;
    let cold_segments = ex
        .walk_segments(snapshot.cold_deletion_segment.as_ref(), Segment::Cold)
        .await?;
    let retired: Vec<RetiredSegment> = retired
        .into_iter()
        .filter_map(|(_, s)| match s {
            Parsed::Retired(s) => Some(s),
            Parsed::Cold(_) => None,
        })
        .collect();
    let cold_segments: Vec<(String, ColdChangeSegment)> = cold_segments
        .into_iter()
        .filter_map(|(p, s)| match s {
            Parsed::Cold(s) => Some((p, s)),
            Parsed::Retired(_) => None,
        })
        .collect();

    // SCOPE-BUCKETS
    let buckets = ex.read_buckets(&snapshot, &retired).await?;

    // 冷层存活文件集与当前版本元数据所列的快照
    let cold_view = match &cold_meta {
        Some((_, meta)) => Some(ex.cold_view(meta).await?),
        None => None,
    };

    let lines = ex
        .lines(&buckets, &cold_segments, cold_view.as_ref())
        .await?;
    let mut lines: Vec<FileLine> = lines;
    ex.check_live(&mut lines).await?;

    let map = ex
        .iceberg_snapshots(
            &buckets,
            &cold_segments,
            cold_meta.as_ref().map(|(_, m)| m),
            n,
        )
        .await?;
    let retirement = ex.retirement(&receipt).await?;

    let receipt_bytes = receipt.to_bytes();
    let evidence = {
        let mut m = serde_json::Map::new();
        m.insert(
            "evidence_format_version".into(),
            EVIDENCE_FORMAT_VERSION.into(),
        );
        m.insert("dataset".into(), dataset.to_string().into());
        m.insert("from".into(), request.range.map(|r| r.0).into());
        m.insert("to".into(), request.range.map(|r| r.1).into());
        m.insert("exported_at".into(), exported_at.into());
        m.insert(
            "amberlog_version".into(),
            request.amberlog_version.clone().into(),
        );
        m.insert("receipt".into(), receipt.to_json());
        m.insert("iceberg_snapshots".into(), map);
        m.insert("retirement".into(), retirement);
        serde_json::to_vec(&Value::Object(m)).expect("可序列化")
    };
    ex.put(package::EVIDENCE_JSON, &evidence)?;
    ex.put(package::RECEIPT_JSON, &receipt_bytes)?;
    ex.put(package::FILES_JSONL, &package::files_jsonl(&lines))?;
    let problems_found = !ex.check.problems.is_empty();
    let check = ex.check.to_bytes();
    ex.put(package::CHECK_JSON, &check)?;
    let sums = package::sha256sums(ex.written.iter().map(|(n, d)| (n.as_str(), d)));
    ex.sink.member(package::SHA256SUMS, &sums)?;
    ex.sink.flush()?;
    Ok(Exported {
        receipt: receipt_bytes,
        problems_found,
    })
}

fn object_ref(r: &SegmentRef) -> ObjectRef {
    ObjectRef {
        path: r.path.clone(),
        content_digest: r.content_digest,
    }
}

/// 数据集前缀下相对路径 `rel` 相对 `<storage.prefix>/` 的路径。
pub fn ds_rel(dataset: &Dataset, rel: &str) -> String {
    format!("{}/{}/{rel}", dataset.org, dataset.stream)
}

/// 冷层表目录下相对路径 `rel` 相对 `<storage.prefix>/` 的路径。
pub fn cold_rel(dataset: &Dataset, rel: &str) -> String {
    format!(
        "{}/{}/{}/{rel}",
        crate::cold::layout::ICEBERG_DIR,
        dataset.org,
        dataset.stream
    )
}

#[derive(Clone, Copy)]
enum Segment {
    Retired,
    Cold,
}

enum Parsed {
    Retired(RetiredSegment),
    Cold(ColdChangeSegment),
}

impl Parsed {
    fn id(&self) -> u64 {
        match self {
            Parsed::Retired(s) => s.segment_id.0,
            Parsed::Cold(s) => s.segment_id.0,
        }
    }

    fn previous(&self) -> Option<&SegmentRef> {
        match self {
            Parsed::Retired(s) => s.previous.as_ref(),
            Parsed::Cold(s) => s.previous.as_ref(),
        }
    }
}

/// 冷层当前版本中与证据有关的内容。
struct ColdView {
    location: String,
    /// 冷层当前快照的存活文件（相对冷层表位置；URI 不在表位置之下的以完整 URI 为键）。
    live: BTreeMap<String, EntryFile>,
    /// 当前版本元数据所列的快照：快照号 → 摘要的 `amberlog.*` 属性。
    snapshots: Vec<(i64, HashMap<String, String>)>,
    /// 各快照的清单列表 URI。
    lists: HashMap<i64, String>,
}

impl Export<'_> {
    fn in_range_bucket(&self, bucket: HourBucket) -> bool {
        self.range
            .is_none_or(|(from, to)| bucket.start_us() >= from && bucket.start_us() < to)
    }

    /// 闭区间 [lo, hi] 与时间段相交。
    fn in_range_span(&self, lo: i64, hi: i64) -> bool {
        self.range.is_none_or(|(from, to)| lo < to && hi >= from)
    }

    /// 写出元数据副本 `objects/<path>`（`path` 相对 `<storage.prefix>/`）。
    fn copy(&mut self, path: &str, bytes: &[u8]) -> Result<(), Failed> {
        if self
            .written
            .iter()
            .any(|(n, _)| n == &format!("{OBJECTS_DIR}{path}"))
        {
            return Ok(());
        }
        self.put(&format!("{OBJECTS_DIR}{path}"), bytes)
    }

    fn put(&mut self, name: &str, bytes: &[u8]) -> Result<(), Failed> {
        self.sink.member(name, bytes)?;
        self.written.push((name.to_owned(), Digest::of(bytes)));
        Ok(())
    }

    /// 钉住的链头所引用的对象读到不存在时（PINNED-VANISH）：链已前进即整次失败，否则记 `missing`。
    async fn vanished(&mut self, path: String) -> Result<(), Failed> {
        let hot = match read::find_tail(self.store, self.dataset).await? {
            Tail::Current(c) | Tail::Hint(c) => Some(c),
            _ => None,
        };
        let (cold, _) = crate::cold::current::version(self.store, &self.cold, self.dataset)
            .await
            .map_err(|e| Failed::Unavailable(e.to_string()))?;
        if hot.is_some_and(|h| h > self.pinned_hot) || cold > self.pinned_cold {
            return Err(Failed::Advanced(path));
        }
        self.check
            .problems
            .add(problem::MISSING, path, "the object does not exist");
        Ok(())
    }

    /// 自 `start` 沿 `previous` 读全部段，逐段核对摘要与段号（CHECK-METADATA）；返回能解析的段（自最新起）。
    async fn walk_segments(
        &mut self,
        start: Option<&SegmentRef>,
        kind: Segment,
    ) -> Result<Vec<(String, Parsed)>, Failed> {
        let mut out: Vec<(String, Parsed)> = Vec::new();
        let mut next = start.cloned();
        let mut newer: Option<u64> = None;
        let mut seen = HashSet::new();
        while let Some(r) = next.take() {
            let path = ds_rel(self.dataset, &r.path);
            if !seen.insert(r.path.clone()) {
                self.check
                    .problems
                    .add(problem::CHAIN_BROKEN, path, "the segment chain loops");
                break;
            }
            let id = match kind {
                Segment::Retired => layout::parse_retired_segment_path(&r.path),
                Segment::Cold => layout::parse_cold_change_segment_path(&r.path),
            };
            let Some(bytes) =
                read::get(self.store, &self.store.object_path(self.dataset, &r.path)).await?
            else {
                // 段永不删除：读到不存在一律记 `missing`
                self.check
                    .problems
                    .add(problem::MISSING, path, "the segment does not exist");
                break;
            };
            if Digest::of(&bytes) != r.content_digest {
                self.check.problems.add(
                    problem::DIGEST_MISMATCH,
                    path.clone(),
                    format!("does not match content_digest {}", r.content_digest),
                );
            }
            let parsed = match (kind, id) {
                (Segment::Retired, Some(id)) => {
                    format::decode_retired_segment(&bytes, id).map(Parsed::Retired)
                }
                (Segment::Cold, Some(id)) => {
                    format::decode_cold_change_segment(&bytes, id).map(Parsed::Cold)
                }
                (_, None) => Err(format::Unparseable(format!(
                    "segment path {:?} does not match the template",
                    r.path
                ))),
            };
            let parsed = match parsed {
                Ok(p) => p,
                Err(e) => {
                    self.check.problems.add(problem::UNPARSEABLE, path, e.0);
                    break;
                }
            };
            self.copy(&path, &bytes)?;
            if let Some(n) = newer
                && parsed.id() >= n
            {
                self.check.problems.add(
                    problem::CHAIN_BROKEN,
                    path.clone(),
                    format!(
                        "segment_id {} is not less than that of the segment referencing it ({n})",
                        parsed.id()
                    ),
                );
            }
            newer = Some(parsed.id());
            next = parsed.previous().cloned();
            out.push((path, parsed));
        }
        Ok(out)
    }

    /// 证据所涉的桶 manifest（SCOPE-BUCKETS）：自头片起逐片读取、核对并写出全部清单片的副本（OBJECTS、CHECK-METADATA、
    /// CHAIN-INCOMPLETE），按 REQ-AMB-STO-HOT-LAYOUT-V8 还原。只是摘要不符的清单片照常放入副本、照常使用其内容；有清单片
    /// 不存在或无法解析的桶 manifest 不用。
    async fn read_buckets(
        &mut self,
        snapshot: &Snapshot,
        retired: &[RetiredSegment],
    ) -> Result<Vec<Bucket>, Failed> {
        let mut refs: Vec<(String, PieceRef, Source)> = Vec::new();
        let head = |manifest: &str, content_digest: Digest| PieceRef {
            path: manifest.to_owned(),
            content_digest,
        };
        for b in &snapshot.buckets {
            if HourBucket::parse(&b.bucket).is_some_and(|h| self.in_range_bucket(h)) {
                refs.push((
                    b.bucket.clone(),
                    head(&b.manifest, b.content_digest),
                    Source::Snapshot,
                ));
            }
        }
        for s in retired {
            for b in &s.buckets {
                if HourBucket::parse(&b.bucket).is_some_and(|h| self.in_range_bucket(h)) {
                    refs.push((
                        b.bucket.clone(),
                        head(&b.manifest, b.content_digest),
                        Source::Retired,
                    ));
                }
            }
        }
        // 清单片的格式版本与经其读到它的快照相等：经留痕归档段引用的也按钉住的快照
        let version = snapshot.format_version;
        let mut out = Vec::new();
        // 已记过问题的清单片（同一片经两处读到只记一次）
        let mut reported: HashSet<String> = HashSet::new();
        for (label, head, source) in refs {
            let (walk, restored) =
                read::bucket_manifest(self.store, self.dataset, &label, &head, version).await?;
            for v in &walk.visits {
                let path = ds_rel(self.dataset, &v.reference.path);
                if let Some(bytes) = &v.bytes
                    && v.piece.is_some()
                {
                    self.copy(&path, bytes)?;
                }
                if v.problem.is_none() || !reported.insert(v.reference.path.clone()) {
                    continue;
                }
                let mismatch = v
                    .bytes
                    .as_ref()
                    .is_some_and(|b| Digest::of(b) != v.reference.content_digest);
                if mismatch {
                    self.check.problems.add(
                        problem::DIGEST_MISMATCH,
                        path.clone(),
                        format!(
                            "does not match content_digest {}",
                            v.reference.content_digest
                        ),
                    );
                }
                match v.problem.as_ref().expect("上面已判") {
                    PieceProblem::Missing => match source {
                        Source::Snapshot => self.vanished(path).await?,
                        Source::Retired => self.check.problems.add(
                            problem::MISSING,
                            path,
                            "the piece of the bucket manifest does not exist",
                        ),
                    },
                    PieceProblem::DigestMismatch => {}
                    PieceProblem::Unparseable(e) => {
                        self.check
                            .problems
                            .add(problem::UNPARSEABLE, path, e.clone())
                    }
                }
            }
            if let Some(r) = restored
                && !out
                    .iter()
                    .any(|b: &Bucket| b.path == head.path && b.source == source)
            {
                out.push(Bucket {
                    path: head.path,
                    source,
                    manifest: (*r.manifest).clone(),
                });
            }
        }
        Ok(out)
    }

    async fn cold_view(&mut self, meta: &TableMetadata) -> Result<ColdView, Failed> {
        let location = meta.location().to_owned();
        let mut snapshots: Vec<_> = meta.snapshots().collect();
        snapshots.sort_by_key(|s| s.sequence_number());
        let lists = snapshots
            .iter()
            .map(|s| (s.snapshot_id(), s.manifest_list().to_owned()))
            .collect();
        let snapshots = snapshots
            .iter()
            .map(|s| (s.snapshot_id(), s.summary().additional_properties.clone()))
            .collect();
        let mut live = BTreeMap::new();
        if let Some(s) = meta.current_snapshot() {
            for e in self
                .snapshot_entries(&location, s.manifest_list(), true)
                .await?
            {
                if e.status != ManifestStatus::Deleted {
                    live.insert(e.rel.clone().unwrap_or_else(|| e.uri.clone()), e);
                }
            }
        }
        Ok(ColdView {
            location,
            live,
            snapshots,
            lists,
        })
    }

    /// 清单列表 `list_uri` 之下全部清单的条目。`pinned` 为真时读的是钉住的冷层当前快照（读到不存在按 PINNED-VANISH）。
    async fn snapshot_entries(
        &mut self,
        location: &str,
        list_uri: &str,
        pinned: bool,
    ) -> Result<Vec<EntryFile>, Failed> {
        let list =
            match read::manifest_list(self.store, &self.cold, self.dataset, location, list_uri)
                .await?
            {
                ColdRead::Ok(l) => l,
                other => {
                    self.cold_problem(other, pinned).await?;
                    return Ok(Vec::new());
                }
            };
        let mut out = Vec::new();
        for f in &list {
            match read::manifest_entries(self.store, &self.cold, self.dataset, location, f).await? {
                ColdRead::Ok(entries) => {
                    out.extend(entries.iter().map(|e| EntryFile::of(location, e)));
                }
                other => self.cold_problem(other, pinned).await?,
            }
        }
        Ok(out)
    }

    async fn cold_problem<T>(&mut self, r: ColdRead<T>, pinned: bool) -> Result<(), Failed> {
        match r {
            ColdRead::Ok(_) => Ok(()),
            ColdRead::Outside(uri) => Err(Failed::Outside(uri)),
            ColdRead::Missing(rel) => {
                let path = cold_rel(self.dataset, &rel);
                if pinned {
                    self.vanished(path).await
                } else {
                    self.check
                        .problems
                        .add(problem::MISSING, path, "the object does not exist");
                    Ok(())
                }
            }
            ColdRead::Unparseable(rel, e) => {
                self.check
                    .problems
                    .add(problem::UNPARSEABLE, cold_rel(self.dataset, &rel), e);
                Ok(())
            }
        }
    }

    /// `files.jsonl` 的各行，并做 SCOPE-COLD、SCOPE-PENDING、CHECK-COLD-RECORD、CHECK-UNTRACED-REMOVAL、
    /// CHECK-LAKE-HANDOVER、CHECK-LIVE-AFTER-REMOVAL 的核对。
    async fn lines(
        &mut self,
        buckets: &[Bucket],
        cold_segments: &[(String, ColdChangeSegment)],
        cold: Option<&ColdView>,
    ) -> Result<Vec<FileLine>, Failed> {
        let mut lines: BTreeMap<(Tier, String), FileLine> = BTreeMap::new();

        // 热层：存活文件（钉住的快照所引用的桶 manifest），再删除留痕
        for b in buckets.iter().filter(|b| b.source == Source::Snapshot) {
            for f in &b.manifest.files {
                if !self.hot_in_range(&f.path, &b.manifest.bucket) {
                    continue;
                }
                lines
                    .entry((Tier::Hot, f.path.clone()))
                    .or_insert_with(|| FileLine {
                        tier: Tier::Hot,
                        path: f.path.clone(),
                        content_digest: f.content_digest,
                        record_count: Some(f.record_count),
                        size_bytes: Some(f.size_bytes),
                        min_timestamp: Some(f.min_timestamp),
                        max_timestamp: Some(f.max_timestamp),
                        added_in: Some(f.added_in.0),
                        source: ds_rel(self.dataset, &b.path),
                        deletion: None,
                    });
            }
        }
        for b in buckets {
            for d in &b.manifest.deletions {
                if !self.hot_in_range(&d.path, &b.manifest.bucket) {
                    continue;
                }
                let handover_files = if d.reason == DeletionReason::Lake {
                    b.manifest
                        .handovers
                        .iter()
                        .filter(|h| h.snapshot_id == d.removed_in)
                        .flat_map(|h| h.files.iter().map(|f| f.path.clone()))
                        .collect()
                } else {
                    Vec::new()
                };
                if d.reason == DeletionReason::Lake && handover_files.is_empty() {
                    self.check.problems.add(
                        problem::LAKE_WITHOUT_HANDOVER,
                        ds_rel(self.dataset, &d.path),
                        format!(
                            "the lake deletion record (removed_in {}) has no handover record",
                            d.removed_in
                        ),
                    );
                }
                lines
                    .entry((Tier::Hot, d.path.clone()))
                    .or_insert_with(|| FileLine {
                        tier: Tier::Hot,
                        path: d.path.clone(),
                        content_digest: d.content_digest,
                        record_count: None,
                        size_bytes: None,
                        min_timestamp: None,
                        max_timestamp: d.max_timestamp,
                        added_in: d.added_in.map(|a| a.0),
                        source: ds_rel(self.dataset, &b.path),
                        deletion: Some(DeletionInfo {
                            reason: hot_reason(d.reason),
                            cutoff: d.cutoff,
                            deleted_at: d.deleted_at,
                            removed_in: d.removed_in.0,
                            cold_removed_in: None,
                            handover_files,
                        }),
                    });
            }
        }

        // 冷层：写入记录（交接记录与合并记录）、冷层留痕
        let mut records: BTreeMap<String, Record> = BTreeMap::new();
        let mut recorded_ids: HashSet<i64> = HashSet::new();
        for b in buckets {
            for h in &b.manifest.handovers {
                recorded_ids.insert(h.iceberg_snapshot_id);
                for f in &h.files {
                    records.entry(f.path.clone()).or_insert_with(|| Record {
                        entry: f.clone(),
                        added_in: Some(h.snapshot_id.0),
                        source: ds_rel(self.dataset, &b.path),
                    });
                }
            }
        }
        let mut removals: BTreeMap<String, (crate::chain::ColdDeletion, String)> = BTreeMap::new();
        let mut segment_ids: HashSet<i64> = HashSet::new();
        for (path, s) in cold_segments {
            for f in &s.additions {
                records.entry(f.path.clone()).or_insert_with(|| Record {
                    entry: f.clone(),
                    added_in: None,
                    source: path.clone(),
                });
            }
            for d in &s.deletions {
                segment_ids.insert(d.cold_removed_in);
                removals
                    .entry(d.path.clone())
                    .or_insert_with(|| (d.clone(), path.clone()));
            }
        }

        // SCOPE-PENDING：热层链尚未记下的冷层提交所涉文件
        let mut pending: BTreeSet<String> = BTreeSet::new();
        if let Some(cold) = cold {
            let location = cold.location.clone();
            for (sid, props) in &cold.snapshots {
                let op = props.get("amberlog.operation").map(String::as_str);
                let kind = match op {
                    Some("lake")
                        if !recorded_ids.contains(sid) && self.in_window(buckets, props) =>
                    {
                        transition::HANDOVER_WINDOW
                    }
                    Some("ttl" | "compaction") if !segment_ids.contains(sid) => {
                        transition::COLD_SEGMENT_PENDING
                    }
                    _ => continue,
                };
                // 新增的文件：当前存活集中由它新增者
                for (key, e) in &cold.live {
                    if e.snapshot_id == *sid {
                        pending.insert(key.clone());
                        if e.intersects(self.range) {
                            self.check.transitions.add(
                                kind,
                                self.cold_object(e),
                                "the hot chain has not recorded this cold commit yet",
                            );
                        }
                    }
                }
                // 移除的文件：该提交所写清单中 `status` 为 DELETED、快照号为它的条目
                if kind == transition::COLD_SEGMENT_PENDING
                    && let Some(list) = cold.lists.get(sid)
                {
                    let entries = self.snapshot_entries(&location, list, false).await?;
                    for e in entries
                        .iter()
                        .filter(|e| e.status == ManifestStatus::Deleted && e.snapshot_id == *sid)
                    {
                        let key = e.rel.clone().unwrap_or_else(|| e.uri.clone());
                        pending.insert(key);
                        if e.intersects(self.range) {
                            self.check.transitions.add(
                                kind,
                                self.cold_object(e),
                                "the hot chain has not recorded this cold commit yet",
                            );
                        }
                    }
                }
            }
        }

        // 冷层留痕的行
        for (p, (d, seg)) in &removals {
            let record = records.get(p);
            let in_range = match record {
                Some(r) => self.in_range_span(r.entry.min_timestamp, r.entry.max_timestamp),
                None => self
                    .range
                    .is_none_or(|(from, to)| d.max_timestamp >= from && d.max_timestamp < to),
            };
            if !in_range {
                continue;
            }
            if record.is_none() {
                self.check.problems.add(
                    problem::NO_WRITE_RECORD,
                    cold_rel(self.dataset, p),
                    "the cold deletion record has no write record in the hot chain",
                );
            }
            if cold.is_some_and(|c| c.live.contains_key(p)) {
                self.check.problems.add(
                    problem::LIVE_AFTER_REMOVAL,
                    cold_rel(self.dataset, p),
                    "a cold data file recorded as removed is live again",
                );
            }
            let deletion = Some(DeletionInfo {
                reason: match d.reason {
                    ColdDeletionReason::Ttl => "ttl",
                    ColdDeletionReason::Compaction => "compaction",
                },
                cutoff: d.cutoff,
                deleted_at: d.deleted_at,
                removed_in: d.removed_in.0,
                cold_removed_in: Some(d.cold_removed_in),
                handover_files: Vec::new(),
            });
            let line = match record {
                Some(r) => FileLine {
                    tier: Tier::Cold,
                    path: p.clone(),
                    content_digest: r.entry.content_digest,
                    record_count: Some(r.entry.record_count),
                    size_bytes: Some(r.entry.size_bytes),
                    min_timestamp: Some(r.entry.min_timestamp),
                    max_timestamp: Some(r.entry.max_timestamp),
                    added_in: r.added_in,
                    source: r.source.clone(),
                    deletion,
                },
                None => FileLine {
                    tier: Tier::Cold,
                    path: p.clone(),
                    content_digest: d.content_digest,
                    record_count: None,
                    size_bytes: None,
                    min_timestamp: None,
                    max_timestamp: Some(d.max_timestamp),
                    added_in: None,
                    source: seg.clone(),
                    deletion,
                },
            };
            lines.insert((Tier::Cold, p.clone()), line);
        }

        // 冷层存活文件的行
        if let Some(cold) = cold {
            for (key, e) in &cold.live {
                if removals.contains_key(key) || pending.contains(key) {
                    continue;
                }
                match records.get(key) {
                    Some(r) => {
                        if !self.in_range_span(r.entry.min_timestamp, r.entry.max_timestamp) {
                            continue;
                        }
                        lines.insert(
                            (Tier::Cold, key.clone()),
                            FileLine {
                                tier: Tier::Cold,
                                path: key.clone(),
                                content_digest: r.entry.content_digest,
                                record_count: Some(r.entry.record_count),
                                size_bytes: Some(r.entry.size_bytes),
                                min_timestamp: Some(r.entry.min_timestamp),
                                max_timestamp: Some(r.entry.max_timestamp),
                                added_in: r.added_in,
                                source: r.source.clone(),
                                deletion: None,
                            },
                        );
                    }
                    None => {
                        if e.intersects(self.range) {
                            self.check.problems.add(
                                problem::NO_WRITE_RECORD,
                                self.cold_object(e),
                                "the live cold data file has no write record in the hot chain",
                            );
                        }
                    }
                }
            }
        }

        // CHECK-UNTRACED-REMOVAL
        for (p, r) in &records {
            if !self.in_range_span(r.entry.min_timestamp, r.entry.max_timestamp) {
                continue;
            }
            let live = cold.is_some_and(|c| c.live.contains_key(p));
            if !live && !removals.contains_key(p) && !pending.contains(p) {
                self.check.problems.add(
                    problem::UNTRACED_REMOVAL,
                    cold_rel(self.dataset, p),
                    "the cold data file left the live cold set without a cold deletion record",
                );
            }
        }

        Ok(lines.into_values().collect())
    }

    /// 热层数据文件落在时间段内：按路径所示的小时桶（路径不合模板时按所在桶 manifest 的桶）。
    fn hot_in_range(&self, path: &str, bucket: &str) -> bool {
        let hour = layout::parse_data_file_path(path).or_else(|| HourBucket::parse(bucket));
        hour.is_some_and(|h| self.in_range_bucket(h))
    }

    /// 入湖冷层提交（摘要 `props`）是否处于交接窗口：钉住的快照中其桶仍有 `added_in` 不大于 H 的存活文件
    /// （REQ-AMB-STO-LAKE-ENTRY 所称交接窗口）。
    fn in_window(&self, buckets: &[Bucket], props: &HashMap<String, String>) -> bool {
        let (Some(bucket), Some(h)) = (
            props.get("amberlog.bucket"),
            props
                .get("amberlog.hot_snapshot_id")
                .and_then(|h| h.parse::<u64>().ok()),
        ) else {
            return false;
        };
        buckets
            .iter()
            .filter(|b| b.source == Source::Snapshot && &b.manifest.bucket == bucket)
            .any(|b| b.manifest.files.iter().any(|f| f.added_in.0 <= h))
    }

    /// 冷层数据文件在核对结果中的 `object`：相对 `<storage.prefix>/`；URI 不在表位置之下时为完整 URI。
    fn cold_object(&self, e: &EntryFile) -> String {
        match &e.rel {
            Some(rel) => cold_rel(self.dataset, rel),
            None => e.uri.clone(),
        }
    }
}

/// 某个热层快照的完整删除台账（REQ-AMB-STO-AUDIT-INVARIANTS）中各留痕条目的 (`path`, `content_digest`)。
#[derive(Default)]
struct Ledger {
    snapshot: Option<SnapshotId>,
    hot: HashSet<(String, Digest)>,
    cold: HashSet<(String, Digest)>,
}

impl Export<'_> {
    /// CHECK-LIVE-HEAD 与 LEFT-LIVE：对每个存活数据文件发一次读取对象元数据的请求，不读内容。
    async fn check_live(&mut self, lines: &mut [FileLine]) -> Result<(), Failed> {
        let mut ledger: Option<Ledger> = None;
        for l in lines.iter().filter(|l| l.is_live()) {
            let (key, object) = match l.tier {
                Tier::Hot => (
                    self.store.object_path(self.dataset, &l.path),
                    ds_rel(self.dataset, &l.path),
                ),
                Tier::Cold => (
                    self.cold.object_path(self.dataset, &l.path),
                    cold_rel(self.dataset, &l.path),
                ),
            };
            match read::head_size(self.store, &key).await? {
                Some(size) => {
                    if Some(size) != l.size_bytes {
                        self.check.problems.add(
                            problem::SIZE_MISMATCH,
                            object,
                            format!(
                                "the object has {size} bytes, the record says {}",
                                l.size_bytes.map_or("null".into(), |s| s.to_string())
                            ),
                        );
                    }
                }
                None => {
                    let mut found = false;
                    for _ in 0..2 {
                        let current = ledger.as_ref().and_then(|g| g.snapshot);
                        let tail = self.current_tail().await?;
                        if ledger.is_none() || tail != current {
                            ledger = Some(self.ledger(tail).await?);
                        }
                        let g = ledger.as_ref().expect("刚建好");
                        let set = match l.tier {
                            Tier::Hot => &g.hot,
                            Tier::Cold => &g.cold,
                        };
                        if set.contains(&(l.path.clone(), l.content_digest)) {
                            found = true;
                            break;
                        }
                    }
                    if found {
                        self.check.transitions.add(
                            transition::LEFT_LIVE,
                            object,
                            "the file left the live set during the export and has a deletion record",
                        );
                    } else {
                        self.check.problems.add(
                            problem::MISSING,
                            object,
                            "the live data file does not exist and has no deletion record",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    /// 重新确定链头（热层链尾与冷层当前版本，LEFT-LIVE），返回热层当前快照号。
    async fn current_tail(&self) -> Result<Option<SnapshotId>, Failed> {
        let tail = match read::find_tail(self.store, self.dataset).await? {
            Tail::Current(c) | Tail::Hint(c) => Some(c),
            _ => None,
        };
        crate::cold::current::version(self.store, &self.cold, self.dataset)
            .await
            .map_err(|e| Failed::Unavailable(e.to_string()))?;
        Ok(tail)
    }

    /// 快照 `id` 的完整删除台账；读不到或读不懂的部分略过（只用来找留痕，找不到即记 `missing`）。
    async fn ledger(&self, id: Option<SnapshotId>) -> Result<Ledger, Failed> {
        let mut g = Ledger {
            snapshot: id,
            ..Ledger::default()
        };
        let Some(id) = id else { return Ok(g) };
        let Some(bytes) = read::get(
            self.store,
            &self
                .store
                .object_path(self.dataset, &layout::snapshot_path(id)),
        )
        .await?
        else {
            return Ok(g);
        };
        let Ok(SnapshotContent::Supported(s)) = format::decode_snapshot(&bytes, self.dataset, id)
        else {
            return Ok(g);
        };
        let head = |manifest: &str, content_digest: Digest| PieceRef {
            path: manifest.to_owned(),
            content_digest,
        };
        let mut manifests: Vec<(String, PieceRef)> = s
            .buckets
            .iter()
            .map(|b| (b.bucket.clone(), head(&b.manifest, b.content_digest)))
            .collect();
        let mut next = s.retired_segment.clone();
        let mut seen = HashSet::new();
        while let Some(r) = next.take() {
            if !seen.insert(r.path.clone()) {
                break;
            }
            let Some(segment_id) = layout::parse_retired_segment_path(&r.path) else {
                break;
            };
            let Some(b) =
                read::get(self.store, &self.store.object_path(self.dataset, &r.path)).await?
            else {
                break;
            };
            let Ok(seg) = format::decode_retired_segment(&b, segment_id) else {
                break;
            };
            manifests.extend(
                seg.buckets
                    .iter()
                    .map(|b| (b.bucket.clone(), head(&b.manifest, b.content_digest))),
            );
            next = seg.previous;
        }
        for (label, head) in manifests {
            let (_, restored) =
                read::bucket_manifest(self.store, self.dataset, &label, &head, s.format_version)
                    .await?;
            if let Some(m) = restored {
                g.hot.extend(
                    m.manifest
                        .deletions
                        .iter()
                        .map(|d| (d.path.clone(), d.content_digest)),
                );
            }
        }
        let mut next = s.cold_deletion_segment.clone();
        let mut seen = HashSet::new();
        while let Some(r) = next.take() {
            if !seen.insert(r.path.clone()) {
                break;
            }
            let Some(segment_id) = layout::parse_cold_change_segment_path(&r.path) else {
                break;
            };
            let Some(b) =
                read::get(self.store, &self.store.object_path(self.dataset, &r.path)).await?
            else {
                break;
            };
            let Ok(seg) = format::decode_cold_change_segment(&b, segment_id) else {
                break;
            };
            g.cold.extend(
                seg.deletions
                    .iter()
                    .map(|d| (d.path.clone(), d.content_digest)),
            );
            next = seg.previous;
        }
        Ok(g)
    }

    /// MAP：证据包中出现的每个 Iceberg 快照号 → 含它的最小 `v<N>.metadata.json` 与摘要的 `amberlog.operation`。
    async fn iceberg_snapshots(
        &mut self,
        buckets: &[Bucket],
        cold_segments: &[(String, ColdChangeSegment)],
        pinned: Option<&TableMetadata>,
        n: u64,
    ) -> Result<Value, Failed> {
        let mut wanted: BTreeMap<i64, String> = BTreeMap::new();
        for b in buckets {
            for h in &b.manifest.handovers {
                wanted
                    .entry(h.iceberg_snapshot_id)
                    .or_insert_with(|| ds_rel(self.dataset, &b.path));
            }
        }
        for (path, s) in cold_segments {
            for d in &s.deletions {
                wanted
                    .entry(d.cold_removed_in)
                    .or_insert_with(|| path.clone());
            }
        }
        if let Some(sid) = pinned.and_then(TableMetadata::current_snapshot_id) {
            wanted
                .entry(sid)
                .or_insert_with(|| cold_rel(self.dataset, &crate::cold::layout::metadata_path(n)));
        }
        let mut found: BTreeMap<i64, (u64, Option<String>)> = BTreeMap::new();
        let mut k = 1;
        while k <= n && found.len() < wanted.len() {
            let owned;
            let meta = if k == n {
                pinned
            } else {
                match read::cold_metadata(self.store, &self.cold, self.dataset, k).await? {
                    ColdRead::Ok((_, m)) => {
                        owned = m;
                        Some(&owned)
                    }
                    ColdRead::Missing(rel) => {
                        self.check.problems.add(
                            problem::MISSING,
                            cold_rel(self.dataset, &rel),
                            "the cold metadata file does not exist",
                        );
                        None
                    }
                    ColdRead::Unparseable(rel, e) => {
                        self.check.problems.add(
                            problem::UNPARSEABLE,
                            cold_rel(self.dataset, &rel),
                            e,
                        );
                        None
                    }
                    ColdRead::Outside(_) => None,
                }
            };
            if let Some(meta) = meta {
                for s in meta.snapshots() {
                    let id = s.snapshot_id();
                    if wanted.contains_key(&id) && !found.contains_key(&id) {
                        let op = s
                            .summary()
                            .additional_properties
                            .get("amberlog.operation")
                            .cloned();
                        found.insert(id, (k, op));
                    }
                }
            }
            k += 1;
        }
        let mut out = Vec::new();
        for (id, referrer) in &wanted {
            match found.get(id) {
                Some((k, op)) => out.push(json!({
                    "iceberg_snapshot_id": id,
                    "metadata_version": k,
                    "operation": op,
                })),
                None => {
                    self.check.problems.add(
                        problem::MISSING,
                        referrer.clone(),
                        format!(
                            "no cold metadata version up to v{n} contains Iceberg snapshot {id}"
                        ),
                    );
                    out.push(json!({
                        "iceberg_snapshot_id": id,
                        "metadata_version": null,
                        "operation": null,
                    }));
                }
            }
        }
        Ok(Value::Array(out))
    }

    /// RETIREMENT 与 RETIREMENT-COMPARE：抄录退役记录，与回执比对（不一致只记提示）。
    async fn retirement(&mut self, receipt: &Receipt) -> Result<Value, Failed> {
        const PATH: &str = "_system/retirement.json";
        let Some(bytes) = read::get(self.store, &self.store.system_path("retirement.json")).await?
        else {
            return Ok(Value::Null);
        };
        self.copy(PATH, &bytes)?;
        let parsed: Option<Value> = serde_json::from_slice(&bytes).ok().filter(Value::is_object);
        let Some(record) = parsed else {
            self.check.notes.add(
                note::RETIREMENT_UNREADABLE,
                PATH,
                "the retirement record is not a JSON object",
            );
            return Ok(Value::Null);
        };
        let r = receipt.to_json();
        let ds = receipt.dataset.as_str();
        let matches = record["datasets"].as_array().is_some_and(|all| {
            all.iter().any(|e| {
                e["dataset"] == ds && e["snapshot"] == r["snapshot"] && e["cold"] == r["cold"]
            })
        });
        if !matches {
            self.check.notes.add(
                note::RETIREMENT_MISMATCH,
                PATH,
                format!("the retirement record does not match the heads of {ds} in this receipt"),
            );
        }
        Ok(record)
    }
}

fn hot_reason(r: DeletionReason) -> &'static str {
    match r {
        DeletionReason::Orphan => "orphan",
        DeletionReason::Compaction => "compaction",
        DeletionReason::Ttl => "ttl",
        DeletionReason::Rollback => "rollback",
        DeletionReason::Lake => "lake",
    }
}
