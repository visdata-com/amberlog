//! 证据查验的核心（REQ-AMB-STO-EVIDENCE-VERIFY B～H 组）：确定此刻的链头，核对证据包自身、链的延长、冷层提交的来历、
//! 元数据副本、文件级延长、数据文件摘要（读全部字节、不抽样）、冷层摘要属性、没有提前删除与退役记录，写出报告。
//!
//! 「此刻」的链、存活文件集与完整删除台账一律以开始时确定的链头为准；所读的每个清单片与留痕段都按引用它的此刻快照、段或
//! 清单片所记的摘要核对（REQ-AMB-STO-EVIDENCE-VERIFY-V2-LEDGER-DIGESTS），桶 manifest 按 REQ-AMB-STO-HOT-LAYOUT-V8 由清单片
//! 还原。参数与证据包形状的校验、退出码与信号由调用方（`amberlog-server`）处置。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use amberlog_common::dataset::Dataset;
use bytes::Bytes;
use futures::TryStreamExt;
use iceberg::spec::{ManifestStatus, TableMetadata};
use object_store::ObjectStoreExt;
use serde_json::{Value, json};

use super::export::{cold_rel, ds_rel};
use super::package::{self, EVIDENCE_FORMAT_VERSION, Findings, Heads, Previous, Unpacked};
use super::read::{self, ColdRead, EntryFile, Tail};
use crate::chain::{
    self, BucketManifest, ColdChangeSegment, ColdDeletion, ColdDeletionReason, Deletion,
    DeletionReason, Digest, PieceProblem, PieceRef, RetiredSegment, SegmentId, SegmentRef,
    Snapshot, SnapshotId,
};
use crate::cold::layout::{ColdLayout, metadata_path};
use crate::format::{self, SnapshotContent};
use crate::layout;
use crate::store::{Store, Unavailable};

/// 报告的 `kind` 闭集（REQ-AMB-STO-EVIDENCE-VERIFY-KINDS）。
pub mod kind {
    pub const PACKAGE_DIGEST_MISMATCH: &str = "package_digest_mismatch";
    pub const PACKAGE_INCONSISTENT: &str = "package_inconsistent";
    pub const CHAIN_NOT_EXTENDED: &str = "chain_not_extended";
    pub const CHAIN_BROKEN: &str = "chain_broken";
    pub const MISSING: &str = "missing";
    pub const DIGEST_MISMATCH: &str = "digest_mismatch";
    pub const SIZE_MISMATCH: &str = "size_mismatch";
    pub const VANISHED: &str = "vanished";
    pub const TRACE_ALTERED: &str = "trace_altered";
    pub const HANDOVER_MISSING: &str = "handover_missing";
    pub const UNTRACED_COLD_COMMIT: &str = "untraced_cold_commit";
    pub const COLD_RECORD_MISMATCH: &str = "cold_record_mismatch";
    pub const TTL_CUTOFF_MISMATCH: &str = "ttl_cutoff_mismatch";
    pub const COUNT_MISMATCH: &str = "count_mismatch";
    pub const EARLY_DELETION: &str = "early_deletion";

    pub const FORMAT_UNSUPPORTED: &str = "format_unsupported";
    pub const SNAPSHOT_ANCHOR_RECYCLED: &str = "snapshot_anchor_recycled";
    pub const UNAVAILABLE: &str = "unavailable";
    pub const DATA_UNREADABLE: &str = "data_unreadable";

    pub const LEFT_LIVE: &str = "left_live";
    pub const TRACED_STILL_PRESENT: &str = "traced_still_present";
    pub const COLD_SEGMENT_PENDING: &str = "cold_segment_pending";
    pub const HANDOVER_WINDOW: &str = "handover_window";

    pub const COPY_RECYCLED: &str = "copy_recycled";
    pub const TABLE_LOCATION_DIFFERS: &str = "table_location_differs";
    pub const RETIREMENT_MISMATCH: &str = "retirement_mismatch";
    pub const RETIREMENT_UNREADABLE: &str = "retirement_unreadable";
    pub const COUNT_NOT_CHECKED: &str = "count_not_checked";
    pub const EXPORT_REPORTED_PROBLEMS: &str = "export_reported_problems";
}

/// 一次查验的输入（已通过 A 组的校验）。
pub struct Request {
    pub dataset: Dataset,
    pub evidence: Unpacked,
    pub previous: Option<Previous>,
    /// `--retention`（微秒）；没给为 `None`。
    pub retention: Option<i64>,
}

/// 查验报告（REQ-AMB-STO-EVIDENCE-VERIFY-REPORT）。
#[derive(Debug, Default)]
pub struct Report {
    pub dataset: String,
    pub verified_at: i64,
    pub evidence_exported_at: Option<i64>,
    pub anchors: Vec<Value>,
    pub current: Option<Heads>,
    pub retention_checked: bool,
    pub data_files: u64,
    pub digests_recomputed: u64,
    pub problems: Findings,
    pub unprovable: Findings,
    pub transitions: Findings,
    pub notes: Findings,
}

impl Report {
    /// 查验结果：`pass`、`inconsistent` 或 `unprovable`（TERM-RESULT）。
    pub fn result(&self) -> &'static str {
        if !self.problems.is_empty() {
            "inconsistent"
        } else if !self.unprovable.is_empty() {
            "unprovable"
        } else {
            "pass"
        }
    }

    pub fn to_json(&self) -> Value {
        let mut m = serde_json::Map::new();
        m.insert("result".into(), self.result().into());
        m.insert("dataset".into(), self.dataset.clone().into());
        m.insert("verified_at".into(), self.verified_at.into());
        m.insert(
            "evidence_exported_at".into(),
            self.evidence_exported_at.into(),
        );
        m.insert("anchors".into(), Value::Array(self.anchors.clone()));
        m.insert(
            "current".into(),
            match &self.current {
                Some(h) => Value::Object(
                    h.members()
                        .into_iter()
                        .map(|(k, v)| (k.to_owned(), v))
                        .collect(),
                ),
                None => Value::Null,
            },
        );
        m.insert("retention_checked".into(), self.retention_checked.into());
        m.insert(
            "checked".into(),
            json!({"data_files": self.data_files, "digests_recomputed": self.digests_recomputed}),
        );
        m.insert("problems".into(), self.problems.to_json());
        m.insert("unprovable".into(), self.unprovable.to_json());
        m.insert("transitions".into(), self.transitions.to_json());
        m.insert("notes".into(), self.notes.to_json());
        Value::Object(m)
    }
}

/// 查验提前结束（报告照常输出已得的部分）。
struct Stop;

type Step<T> = Result<T, Stop>;

/// 所核对的一个证据包。
struct Package<'a> {
    unpacked: &'a Unpacked,
    lines: Vec<Value>,
    range: Option<(i64, i64)>,
    /// Iceberg 快照号 → 冷层元数据版本（`iceberg_snapshots`；版本为 null 时为 `None`）。
    map: HashMap<i64, Option<u64>>,
}

/// 此刻的链与台账。
#[derive(Default)]
struct Now {
    snapshot: Option<Snapshot>,
    /// 此刻快照所引用的桶 manifest（头片相对数据集前缀的路径 → 还原所得）。
    buckets: Vec<(String, BucketManifest)>,
    /// 此刻经 `retired_segment` 可达的留痕归档段所引用的桶 manifest。
    retired_buckets: Vec<(String, BucketManifest)>,
    /// 此刻经留痕归档段引用的桶 manifest 的各清单片（永不删除）。
    retired_manifest_paths: HashSet<String>,
    cold_segments: Vec<ColdChangeSegment>,
    /// 冷层当前版本 N 与其元数据。
    cold: Option<(u64, TableMetadata)>,
    cold_live: BTreeMap<String, EntryFile>,
    /// 热层链尚未记下的冷层提交所移除的冷层文件。
    pending_removed: BTreeSet<String>,
}

impl Now {
    fn hot_live(&self) -> HashSet<(&str, Digest)> {
        self.buckets
            .iter()
            .flat_map(|(_, m)| m.files.iter().map(|f| (f.path.as_str(), f.content_digest)))
            .collect()
    }

    fn hot_ledger(&self) -> impl Iterator<Item = &Deletion> {
        self.buckets
            .iter()
            .chain(self.retired_buckets.iter())
            .flat_map(|(_, m)| m.deletions.iter())
    }

    fn cold_ledger(&self) -> impl Iterator<Item = &ColdDeletion> {
        self.cold_segments.iter().flat_map(|s| s.deletions.iter())
    }
}

struct Verify<'a> {
    store: &'a Store,
    cold: ColdLayout,
    dataset: &'a Dataset,
    report: Report,
    now: Now,
    /// 已读的冷层元数据版本（核对冷层摘要属性用）。
    versions: HashMap<u64, Option<TableMetadata>>,
}

/// 查验 `request`。`verified_at` 由本函数在开始确定此刻链头之前读取。
pub async fn verify(store: &Store, request: &Request) -> Report {
    let mut v = Verify {
        store,
        cold: ColdLayout::new(&store.location, store.root.clone()),
        dataset: &request.dataset,
        report: Report {
            dataset: request.dataset.to_string(),
            retention_checked: request.retention.is_some(),
            ..Report::default()
        },
        now: Now::default(),
        versions: HashMap::new(),
    };
    let _ = v.run(request).await;
    v.report
}

impl Verify<'_> {
    async fn run(&mut self, request: &Request) -> Step<()> {
        // 锚点（证据包自带的回执在前）
        let evidence_receipt = request.evidence.receipt();
        let mut anchors: Vec<(&'static str, &'static str, Value, u64)> = vec![(
            "--evidence",
            "package",
            evidence_receipt,
            request.evidence.format_version(),
        )];
        if let Some(p) = &request.previous {
            let kind = match p {
                Previous::Package(_) => "package",
                Previous::Receipt(_) => "receipt",
            };
            anchors.push(("--previous", kind, p.receipt(), p.format_version()));
        }
        let evidence_newer = request.evidence.format_version() > EVIDENCE_FORMAT_VERSION;
        self.report.evidence_exported_at = if evidence_newer {
            None
        } else {
            request.evidence.evidence["exported_at"].as_i64()
        };
        for (param, kind, receipt, version) in &anchors {
            let newer = *version > EVIDENCE_FORMAT_VERSION;
            self.report.anchors.push(json!({
                "source": if *param == "--evidence" { "evidence" } else { "previous" },
                "kind": kind,
                "snapshot_id": if newer { Value::Null } else { receipt["snapshot"]["snapshot_id"].clone() },
                "exported_at": if newer { Value::Null } else { receipt["exported_at"].clone() },
            }));
        }
        // FORMAT-VERSION：不认识更新的证据格式，不猜
        let mut newer = false;
        for (param, _, _, version) in &anchors {
            if *version > EVIDENCE_FORMAT_VERSION {
                self.report.unprovable.add(
                    kind::FORMAT_UNSUPPORTED,
                    *param,
                    format!(
                        "evidence_format_version {version} is newer than this program understands"
                    ),
                );
                newer = true;
            }
        }
        if newer {
            self.report.verified_at = crate::lifecycle::now_us();
            return Err(Stop);
        }
        // PKG-FORMAT：证据包里有本程序不认识的格式版本的快照或清单片副本（如旧程序导出的第 2 版对象），不猜
        let mut old = false;
        let mut packaged: Vec<(&str, &Unpacked)> = vec![("--evidence", &request.evidence)];
        if let Some(Previous::Package(p)) = &request.previous {
            packaged.push(("--previous", p));
        }
        for (param, u) in packaged {
            for (name, version) in self.copy_versions(u) {
                if version != format::FORMAT_VERSION {
                    self.report.unprovable.add(
                        kind::FORMAT_UNSUPPORTED,
                        format!("{param}:{name}"),
                        format!("the copy has format_version {version}, which this program does not read"),
                    );
                    old = true;
                }
            }
        }
        if old {
            self.report.verified_at = crate::lifecycle::now_us();
            return Err(Stop);
        }

        // B 组：证据包自身
        let mut packages = vec![self.package("--evidence", &request.evidence)];
        if let Some(Previous::Package(p)) = &request.previous {
            packages.push(self.package("--previous", p));
        }

        // CURRENT-HEADS
        self.report.verified_at = crate::lifecycle::now_us();
        self.current_heads().await?;
        crate::fault::gate("verify.after_current_heads").await;
        // 此刻台账属其余核对，在暂停点之后读（链若在此期间前进、所读的桶 manifest 已回收，按 LEDGER-UNREADABLE 请重跑）
        if let Some(snapshot) = self.now.snapshot.take() {
            let read = self.ledger(&snapshot).await;
            self.now.snapshot = Some(snapshot);
            read?;
        }

        // C 组：链的延长
        let anchor_heads: Vec<(&'static str, Option<Heads>)> = anchors
            .iter()
            .map(|(p, _, r, _)| (*p, package::heads_of(r)))
            .collect();
        for (param, heads) in &anchor_heads {
            match heads {
                Some(h) => {
                    self.snapshot_chain(h).await?;
                    self.segment_chains(h).await?;
                    self.cold_version(h).await?;
                }
                None => self.report.problems.add(
                    kind::PACKAGE_INCONSISTENT,
                    format!("{param}:receipt.json"),
                    "the receipt does not record the chain heads in the expected shape",
                ),
            }
        }
        self.table_location();
        let min_cold = anchor_heads
            .iter()
            .map(|(_, h)| {
                h.as_ref()
                    .and_then(|h| h.cold.as_ref())
                    .map_or(0, |c| c.metadata_version)
            })
            .min()
            .unwrap_or(0);
        self.cold_commits(min_cold).await?;

        // D 组
        self.copies(&packages[0]).await?;
        for p in &packages {
            self.file_extension(p);
            self.handover_trace(p);
        }
        self.handover_window(&packages[0]);

        // E 组
        let mut seen = BTreeSet::new();
        for p in &packages {
            for l in &p.lines {
                seen.insert((
                    l["tier"].as_str().unwrap_or_default().to_owned(),
                    l["path"].as_str().unwrap_or_default().to_owned(),
                ));
            }
        }
        self.report.data_files = seen.len() as u64;
        crate::fault::gate("verify.before_data_digest").await;
        self.data_digests(&packages).await?;
        self.traced_present(&packages).await;
        for p in &packages {
            self.cold_records(p).await?;
            self.ttl_cutoffs(p).await?;
            self.count_conserve(p).await;
            // F 组
            self.ttl_order(p, request.retention);
        }
        // G 组
        self.retirement(&packages).await?;
        Ok(())
    }

    fn unavailable(&mut self, object: impl Into<String>, e: Unavailable) -> Stop {
        self.report.unprovable.add(kind::UNAVAILABLE, object, e.0);
        Stop
    }

    /// PKG-SUMS、PKG-CONSISTENT、EXPORT-PROBLEMS-NOTE，并解析出所需的部分。
    fn package<'u>(&mut self, param: &'static str, u: &'u Unpacked) -> Package<'u> {
        let sums = package::parse_sums(&u.members[package::SHA256SUMS]);
        let members: BTreeSet<&String> = u
            .members
            .keys()
            .filter(|n| n.as_str() != package::SHA256SUMS)
            .collect();
        for name in members.iter().chain(
            sums.keys()
                .filter(|n| !members.contains(n))
                .collect::<Vec<_>>()
                .iter(),
        ) {
            let ok = match (u.members.get(name.as_str()), sums.get(name.as_str())) {
                (Some(bytes), Some(hex)) => package::hex(&Digest::of(bytes)) == *hex,
                _ => false,
            };
            if !ok {
                self.report.problems.add(
                    kind::PACKAGE_DIGEST_MISMATCH,
                    format!("{param}:{name}"),
                    "the member does not match SHA256SUMS",
                );
            }
        }
        let receipt = u.receipt();
        if receipt != u.evidence["receipt"]
            || receipt["dataset"] != u.evidence["dataset"]
            || u.evidence["receipt"]["dataset"] != u.evidence["dataset"]
        {
            self.report.problems.add(
                kind::PACKAGE_INCONSISTENT,
                format!("{param}:receipt.json"),
                "receipt.json differs from the receipt in evidence.json",
            );
        }
        let check: Value =
            serde_json::from_slice(&u.members[package::CHECK_JSON]).unwrap_or(Value::Null);
        if check["problems"].as_array().is_some_and(|p| !p.is_empty()) {
            self.report.notes.add(
                kind::EXPORT_REPORTED_PROBLEMS,
                format!("{param}:check.json"),
                "the export already reported problems; this verification checks independently",
            );
        }
        let lines = String::from_utf8_lossy(&u.members[package::FILES_JSONL])
            .lines()
            .filter_map(|l| serde_json::from_str::<Value>(l).ok())
            .collect();
        let range = match (u.evidence["from"].as_i64(), u.evidence["to"].as_i64()) {
            (Some(f), Some(t)) => Some((f, t)),
            _ => None,
        };
        let map = u.evidence["iceberg_snapshots"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|e| {
                        Some((
                            e["iceberg_snapshot_id"].as_i64()?,
                            e["metadata_version"].as_u64(),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default();
        Package {
            unpacked: u,
            lines,
            range,
            map,
        }
    }

    /// CURRENT-HEADS、CURRENT-UNREADABLE、CURRENT-HINT、CURRENT-FORMAT：确定此刻的链头（此刻台账由调用方在暂停点之后读）。
    async fn current_heads(&mut self) -> Step<()> {
        let ds = self.dataset.clone();
        let manifest_dir = format!("{}/{}/manifest/", ds.org, ds.stream);
        match read::dataset_exists(self.store, &ds).await {
            Ok(true) => {}
            Ok(false) => {
                self.report.problems.add(
                    kind::MISSING,
                    manifest_dir,
                    "the dataset no longer exists in the object store",
                );
                return Err(Stop);
            }
            Err(e) => return Err(self.unavailable(manifest_dir, e)),
        }
        let id = match read::find_tail(self.store, &ds).await {
            Ok(Tail::Current(c) | Tail::Hint(c)) => c,
            Ok(Tail::HintUnusable | Tail::NoSnapshot) => {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    ds_rel(&ds, layout::CURRENT),
                    "the hint is missing or unparseable and snapshot 1 does not exist",
                );
                return Err(Stop);
            }
            Err(e) => return Err(self.unavailable(manifest_dir, e)),
        };
        let rel = layout::snapshot_path(id);
        let path = ds_rel(&ds, &rel);
        let bytes = match read::get(self.store, &self.store.object_path(&ds, &rel)).await {
            Ok(Some(b)) => b,
            Ok(None) => {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    path,
                    "the current snapshot does not exist",
                );
                return Err(Stop);
            }
            Err(e) => return Err(self.unavailable(path, e)),
        };
        let snapshot = match format::decode_snapshot(&bytes, &ds, id) {
            Ok(SnapshotContent::Supported(s)) => *s,
            Ok(SnapshotContent::Unsupported { format_version }) => {
                self.report.unprovable.add(
                    kind::FORMAT_UNSUPPORTED,
                    path,
                    format!("the current snapshot has format_version {format_version}"),
                );
                return Err(Stop);
            }
            Err(e) => {
                self.report.problems.add(kind::CHAIN_BROKEN, path, e.0);
                return Err(Stop);
            }
        };
        let (n, _) = crate::cold::current::version(self.store, &self.cold, &ds)
            .await
            .map_err(|e| {
                let e = Unavailable(e.to_string());
                self.unavailable(cold_rel(&ds, "metadata/"), e)
            })?;
        let mut cold_head = None;
        if n > 0 {
            match read::cold_metadata(self.store, &self.cold, &ds, n).await {
                Ok(ColdRead::Ok((b, m))) => {
                    cold_head = Some(package::ColdHead {
                        metadata_version: n,
                        path: metadata_path(n),
                        content_digest: Digest::of(&b),
                    });
                    self.now.cold = Some((n, m));
                }
                Ok(ColdRead::Missing(rel)) => {
                    self.report
                        .problems
                        .add(kind::MISSING, cold_rel(&ds, &rel), "does not exist");
                }
                Ok(ColdRead::Unparseable(rel, e)) => {
                    self.report
                        .problems
                        .add(kind::CHAIN_BROKEN, cold_rel(&ds, &rel), e);
                }
                Ok(ColdRead::Outside(_)) => {}
                Err(e) => return Err(self.unavailable(cold_rel(&ds, &metadata_path(n)), e)),
            }
        }
        self.report.current = Some(Heads {
            snapshot: package::SnapshotHead {
                snapshot_id: id.0,
                path: rel,
                content_digest: Digest::of(&bytes),
            },
            retired_segment: snapshot.retired_segment.as_ref().map(obj_ref),
            cold_deletion_segment: snapshot.cold_deletion_segment.as_ref().map(obj_ref),
            cold: cold_head,
        });
        self.now.snapshot = Some(snapshot);
        Ok(())
    }

    /// 读一个对象；不可访问即中止（UNAVAILABLE）。
    async fn get(&mut self, rel_root: &str, key: &object_store::path::Path) -> Step<Option<Bytes>> {
        match read::get(self.store, key).await {
            Ok(b) => Ok(b),
            Err(e) => Err(self.unavailable(rel_root, e)),
        }
    }

    /// 证据包 `objects/` 之下本数据集的快照副本与清单片副本中带整数 `format_version` 者：(成员名, 版本)。
    fn copy_versions<'u>(&self, u: &'u Unpacked) -> Vec<(&'u str, u64)> {
        let ds_prefix = format!(
            "{}{}/{}/",
            package::OBJECTS_DIR,
            self.dataset.org,
            self.dataset.stream
        );
        u.members
            .iter()
            .filter(|(name, _)| {
                name.strip_prefix(&ds_prefix).is_some_and(|rel| {
                    layout::parse_snapshot_path(rel).is_some()
                        || layout::parse_bucket_manifest_path(rel).is_some()
                })
            })
            .filter_map(|(name, bytes)| {
                let v: Value = serde_json::from_slice(bytes).ok()?;
                Some((name.as_str(), v.get("format_version")?.as_u64()?))
            })
            .collect()
    }

    /// 构成此刻的存活文件集与完整删除台账（LEDGER-DIGESTS、LEDGER-UNREADABLE）。
    async fn ledger(&mut self, snapshot: &Snapshot) -> Step<()> {
        for b in &snapshot.buckets {
            let head = PieceRef {
                path: b.manifest.clone(),
                content_digest: b.content_digest,
            };
            if let Some(m) = self
                .manifest(snapshot, &b.bucket, &head, Some(snapshot.snapshot_id))
                .await?
            {
                self.now.buckets.push((b.manifest.clone(), m));
            }
        }
        // 留痕归档段与它们引用的桶 manifest（永不删除）
        let retired = self
            .segments(snapshot.retired_segment.as_ref(), false)
            .await?;
        for seg in retired.iter().filter_map(|s| match s {
            Seg::Retired(r) => Some(r),
            Seg::Cold(_) => None,
        }) {
            for b in &seg.buckets {
                let head = PieceRef {
                    path: b.manifest.clone(),
                    content_digest: b.content_digest,
                };
                if let Some(m) = self.manifest(snapshot, &b.bucket, &head, None).await? {
                    self.now.retired_buckets.push((b.manifest.clone(), m));
                }
            }
        }
        let cold = self
            .segments(snapshot.cold_deletion_segment.as_ref(), true)
            .await?;
        self.now.cold_segments = cold
            .into_iter()
            .filter_map(|s| match s {
                Seg::Cold(c) => Some(c),
                Seg::Retired(_) => None,
            })
            .collect();
        self.cold_live().await
    }

    /// 自头片 `head` 读出、逐片核对并还原此刻的一个桶 manifest；能用其内容时返回之。`pinned` 为此刻快照的编号时该桶
    /// manifest 是此刻快照直接引用的（其清单片不存在而链已前进即请重跑）；为 `None` 时是经留痕归档段引用的（永不删除）。
    async fn manifest(
        &mut self,
        snapshot: &Snapshot,
        bucket: &str,
        head: &PieceRef,
        pinned: Option<SnapshotId>,
    ) -> Step<Option<BucketManifest>> {
        let ds = self.dataset.clone();
        let (walk, restored) =
            match read::bucket_manifest(self.store, &ds, bucket, head, snapshot.format_version)
                .await
            {
                Ok(r) => r,
                Err(e) => return Err(self.unavailable(ds_rel(&ds, &head.path), e)),
            };
        for v in &walk.visits {
            if pinned.is_none() {
                self.now
                    .retired_manifest_paths
                    .insert(v.reference.path.clone());
            }
            let path = ds_rel(&ds, &v.reference.path);
            let mismatch = v
                .bytes
                .as_ref()
                .is_some_and(|b| Digest::of(b) != v.reference.content_digest);
            if mismatch {
                self.report.problems.add(
                    kind::DIGEST_MISMATCH,
                    path.clone(),
                    format!(
                        "does not match content_digest {}",
                        v.reference.content_digest
                    ),
                );
            }
            match &v.problem {
                Some(PieceProblem::Missing) => {
                    if let Some(id) = pinned {
                        // 此刻快照直接引用的桶 manifest 的清单片不存在：链已前进即请重跑，否则记 `missing`
                        let tail = match read::find_tail(self.store, &ds).await {
                            Ok(Tail::Current(c) | Tail::Hint(c)) => Some(c),
                            Ok(_) => None,
                            Err(e) => return Err(self.unavailable(path, e)),
                        };
                        if tail.is_some_and(|t| t > id) {
                            self.report.unprovable.add(
                                kind::UNAVAILABLE,
                                path,
                                "查验期间链已前进，请重跑 (the chain advanced during the verification; please rerun)",
                            );
                            return Err(Stop);
                        }
                    }
                    self.report.problems.add(
                        kind::MISSING,
                        path,
                        "the piece of the bucket manifest does not exist",
                    );
                }
                Some(PieceProblem::Unparseable(e)) if !mismatch => {
                    self.report
                        .problems
                        .add(kind::CHAIN_BROKEN, path, e.clone());
                }
                _ => {}
            }
        }
        Ok(restored.map(|r| (*r.manifest).clone()))
    }

    /// 自 `start` 沿 `previous` 读段（构成台账用；摘要按引用处核对）。
    async fn segments(&mut self, start: Option<&SegmentRef>, cold: bool) -> Step<Vec<Seg>> {
        let ds = self.dataset.clone();
        let mut out = Vec::new();
        let mut next = start.cloned();
        let mut seen = HashSet::new();
        while let Some(r) = next.take() {
            if !seen.insert(r.path.clone()) {
                break;
            }
            let path = ds_rel(&ds, &r.path);
            let Some(bytes) = self
                .get(&path, &self.store.object_path(&ds, &r.path))
                .await?
            else {
                self.report
                    .problems
                    .add(kind::MISSING, path, "the segment does not exist");
                break;
            };
            let mismatch = Digest::of(&bytes) != r.content_digest;
            if mismatch {
                self.report.problems.add(
                    kind::DIGEST_MISMATCH,
                    path.clone(),
                    format!("does not match content_digest {}", r.content_digest),
                );
            }
            let parsed = if cold {
                layout::parse_cold_change_segment_path(&r.path)
                    .ok_or_else(|| format::Unparseable("bad segment path".into()))
                    .and_then(|id| format::decode_cold_change_segment(&bytes, id))
                    .map(Seg::Cold)
            } else {
                layout::parse_retired_segment_path(&r.path)
                    .ok_or_else(|| format::Unparseable("bad segment path".into()))
                    .and_then(|id| format::decode_retired_segment(&bytes, id))
                    .map(Seg::Retired)
            };
            match parsed {
                Ok(s) => {
                    next = s.previous().cloned();
                    out.push(s);
                }
                Err(e) => {
                    if !mismatch {
                        self.report.problems.add(kind::CHAIN_BROKEN, path, e.0);
                    }
                    break;
                }
            }
        }
        Ok(out)
    }

    /// 此刻冷层当前快照的存活文件集，与热层链尚未记下的冷层提交所移除的文件。
    async fn cold_live(&mut self) -> Step<()> {
        let Some((_, meta)) = self.now.cold.clone() else {
            return Ok(());
        };
        let location = meta.location().to_owned();
        if let Some(s) = meta.current_snapshot() {
            for e in self.entries(&location, s.manifest_list()).await? {
                if e.status != ManifestStatus::Deleted {
                    self.now
                        .cold_live
                        .insert(e.rel.clone().unwrap_or_else(|| e.uri.clone()), e);
                }
            }
        }
        let recorded: HashSet<i64> = self.now.cold_ledger().map(|d| d.cold_removed_in).collect();
        for s in meta.snapshots() {
            let op = s
                .summary()
                .additional_properties
                .get("amberlog.operation")
                .map(String::as_str);
            if matches!(op, Some("ttl" | "compaction")) && !recorded.contains(&s.snapshot_id()) {
                let sid = s.snapshot_id();
                for e in self.entries(&location, s.manifest_list()).await? {
                    if e.status == ManifestStatus::Deleted && e.snapshot_id == sid {
                        self.now
                            .pending_removed
                            .insert(e.rel.clone().unwrap_or_else(|| e.uri.clone()));
                    }
                }
            }
        }
        Ok(())
    }

    /// 清单列表 `list_uri` 之下全部清单的条目；清单列表或清单读不到、URI 不在表位置之下按 UNAVAILABLE 中止。
    async fn entries(&mut self, location: &str, list_uri: &str) -> Step<Vec<EntryFile>> {
        let ds = self.dataset.clone();
        let list = match read::manifest_list(self.store, &self.cold, &ds, location, list_uri).await
        {
            Ok(ColdRead::Ok(l)) => l,
            Ok(other) => return Err(self.cold_broken(other)),
            Err(e) => return Err(self.unavailable(list_uri, e)),
        };
        let mut out = Vec::new();
        for f in &list {
            match read::manifest_entries(self.store, &self.cold, &ds, location, f).await {
                Ok(ColdRead::Ok(es)) => out.extend(es.iter().map(|e| EntryFile::of(location, e))),
                Ok(other) => return Err(self.cold_broken(other)),
                Err(e) => return Err(self.unavailable(f.manifest_path.clone(), e)),
            }
        }
        Ok(out)
    }

    fn cold_broken<T>(&mut self, r: ColdRead<T>) -> Stop {
        let (object, why) = match r {
            ColdRead::Missing(rel) => (cold_rel(self.dataset, &rel), "does not exist".to_owned()),
            ColdRead::Unparseable(rel, e) => (cold_rel(self.dataset, &rel), e),
            ColdRead::Outside(uri) => (uri, "outside the table location".to_owned()),
            ColdRead::Ok(_) => unreachable!("只对失败调用"),
        };
        self.report.unprovable.add(kind::UNAVAILABLE, object, why);
        Stop
    }

    /// SNAPSHOT：沿 `parent_snapshot_id` 回走到旧锚点所记快照，逐个核对 `parent_digest`。
    async fn snapshot_chain(&mut self, anchor: &Heads) -> Step<()> {
        let ds = self.dataset.clone();
        let Some(current) = self.now.snapshot.clone() else {
            return Ok(());
        };
        let target = SnapshotId(anchor.snapshot.snapshot_id);
        let target_path = ds_rel(&ds, &layout::snapshot_path(target));
        if current.snapshot_id < target {
            self.report.problems.add(
                kind::CHAIN_NOT_EXTENDED,
                target_path,
                "the current snapshot is older than the anchor",
            );
            return Ok(());
        }
        let current_bytes_digest = self
            .report
            .current
            .as_ref()
            .map(|h| h.snapshot.content_digest)
            .expect("已确定此刻链头");
        let mut cur = current;
        let mut cur_digest = current_bytes_digest;
        while cur.snapshot_id > target {
            let cur_path = ds_rel(&ds, &layout::snapshot_path(cur.snapshot_id));
            let (Some(pid), Some(pdigest)) = (cur.parent_snapshot_id, cur.parent_digest) else {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    cur_path,
                    "the chain ends before reaching the anchor snapshot",
                );
                return Ok(());
            };
            let prel = layout::snapshot_path(pid);
            let ppath = ds_rel(&ds, &prel);
            let Some(bytes) = self
                .get(&ppath, &self.store.object_path(&ds, &prel))
                .await?
            else {
                let anchor_exists = match self
                    .store
                    .exists(&self.store.object_path(&ds, &layout::snapshot_path(target)))
                    .await
                {
                    Ok(x) => x,
                    Err(e) => return Err(self.unavailable(target_path, e)),
                };
                if anchor_exists {
                    self.report.problems.add(
                        kind::CHAIN_BROKEN,
                        ppath,
                        "a snapshot between the anchor and the current snapshot does not exist",
                    );
                } else {
                    self.report.unprovable.add(
                        kind::SNAPSHOT_ANCHOR_RECYCLED,
                        target_path,
                        "the anchor snapshot has been recycled; the snapshot chain cannot be checked back to it",
                    );
                }
                return Ok(());
            };
            let d = Digest::of(&bytes);
            if d != pdigest {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    ppath,
                    format!("does not match parent_digest {pdigest} recorded in {cur_path}"),
                );
                return Ok(());
            }
            match format::decode_snapshot(&bytes, &ds, pid) {
                Ok(SnapshotContent::Supported(s)) => {
                    cur = *s;
                    cur_digest = d;
                }
                _ => {
                    self.report.problems.add(
                        kind::CHAIN_BROKEN,
                        ppath,
                        "the snapshot is unparseable",
                    );
                    return Ok(());
                }
            }
        }
        if cur.snapshot_id != target || cur_digest != anchor.snapshot.content_digest {
            self.report.problems.add(
                kind::CHAIN_BROKEN,
                target_path,
                format!(
                    "does not match the anchor content_digest {}",
                    anchor.snapshot.content_digest
                ),
            );
        }
        Ok(())
    }

    /// SEGMENTS：两种留痕段沿 `previous` 走到旧锚点所记的段。
    async fn segment_chains(&mut self, anchor: &Heads) -> Step<()> {
        let Some(current) = self.now.snapshot.clone() else {
            return Ok(());
        };
        for (cold, want, from) in [
            (
                false,
                &anchor.retired_segment,
                current.retired_segment.as_ref(),
            ),
            (
                true,
                &anchor.cold_deletion_segment,
                current.cold_deletion_segment.as_ref(),
            ),
        ] {
            let Some(want) = want else { continue };
            self.segment_chain(cold, want, from).await?;
        }
        Ok(())
    }

    async fn segment_chain(
        &mut self,
        cold: bool,
        want: &package::ObjectRef,
        from: Option<&SegmentRef>,
    ) -> Step<()> {
        let ds = self.dataset.clone();
        let parse_id = |p: &str| {
            if cold {
                layout::parse_cold_change_segment_path(p)
            } else {
                layout::parse_retired_segment_path(p)
            }
        };
        let want_path = ds_rel(&ds, &want.path);
        let want_id = parse_id(&want.path).map_or(0, |i| i.0);
        let mut next = from.cloned();
        let mut newer: Option<u64> = None;
        let mut seen = HashSet::new();
        loop {
            let Some(r) = next.take() else {
                self.report.problems.add(
                    kind::CHAIN_NOT_EXTENDED,
                    want_path,
                    "the segment chain does not reach the anchor segment",
                );
                return Ok(());
            };
            if !seen.insert(r.path.clone()) {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    ds_rel(&ds, &r.path),
                    "the segment chain loops",
                );
                return Ok(());
            }
            let path = ds_rel(&ds, &r.path);
            let id = parse_id(&r.path).map_or(0, |i| i.0);
            if r.path != want.path && id < want_id {
                self.report.problems.add(
                    kind::CHAIN_NOT_EXTENDED,
                    want_path,
                    "the segment chain passed the anchor segment without meeting it",
                );
                return Ok(());
            }
            let Some(bytes) = self
                .get(&path, &self.store.object_path(&ds, &r.path))
                .await?
            else {
                self.report
                    .problems
                    .add(kind::MISSING, path, "the segment does not exist");
                return Ok(());
            };
            let d = Digest::of(&bytes);
            if d != r.content_digest {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    path.clone(),
                    format!("does not match content_digest {}", r.content_digest),
                );
            }
            if newer.is_some_and(|n| id >= n) {
                self.report.problems.add(
                    kind::CHAIN_BROKEN,
                    path.clone(),
                    "segment_id does not decrease along previous",
                );
            }
            newer = Some(id);
            if r.path == want.path {
                if d != want.content_digest {
                    self.report.problems.add(
                        kind::CHAIN_BROKEN,
                        path,
                        format!(
                            "does not match the anchor content_digest {}",
                            want.content_digest
                        ),
                    );
                }
                return Ok(());
            }
            let previous = if cold {
                format::decode_cold_change_segment(&bytes, SegmentId(id)).map(|s| s.previous)
            } else {
                format::decode_retired_segment(&bytes, SegmentId(id)).map(|s| s.previous)
            };
            match previous {
                Ok(p) => next = p,
                Err(e) => {
                    self.report.problems.add(kind::CHAIN_BROKEN, path, e.0);
                    return Ok(());
                }
            }
        }
    }

    /// COLD-VERSION：旧锚点所记的 `v<M>` 仍在、字节不变，`v<M+1>`～`v<N>` 都在。
    async fn cold_version(&mut self, anchor: &Heads) -> Step<()> {
        let Some(c) = &anchor.cold else { return Ok(()) };
        let ds = self.dataset.clone();
        let m = c.metadata_version;
        let vm = cold_rel(&ds, &metadata_path(m));
        let Some((n, _)) = self.now.cold.as_ref().map(|(n, m)| (*n, m.clone())) else {
            self.report
                .problems
                .add(kind::CHAIN_NOT_EXTENDED, vm, "there is no cold table now");
            return Ok(());
        };
        if n < m {
            self.report.problems.add(
                kind::CHAIN_NOT_EXTENDED,
                vm,
                format!("the current cold version is v{n}"),
            );
            return Ok(());
        }
        match self
            .get(&vm, &self.cold.object_path(&ds, &metadata_path(m)))
            .await?
        {
            None => self
                .report
                .problems
                .add(kind::MISSING, vm, "does not exist"),
            Some(b) if Digest::of(&b) != c.content_digest => self.report.problems.add(
                kind::CHAIN_BROKEN,
                vm,
                format!(
                    "does not match the anchor content_digest {}",
                    c.content_digest
                ),
            ),
            Some(_) => {}
        }
        for k in m + 1..=n {
            let rel = metadata_path(k);
            let exists = match self.store.exists(&self.cold.object_path(&ds, &rel)).await {
                Ok(x) => x,
                Err(e) => return Err(self.unavailable(cold_rel(&ds, &rel), e)),
            };
            if !exists {
                self.report
                    .problems
                    .add(kind::MISSING, cold_rel(&ds, &rel), "does not exist");
            }
        }
        Ok(())
    }

    /// TABLE-LOCATION：此刻元数据的 `location` 与按配置推算的表位置不同只记提示。
    fn table_location(&mut self) {
        let Some((n, meta)) = &self.now.cold else {
            return;
        };
        let expected = self.cold.table_location(self.dataset);
        if meta.location().trim_end_matches('/') != expected {
            self.report.notes.add(
                kind::TABLE_LOCATION_DIFFERS,
                cold_rel(self.dataset, &metadata_path(*n)),
                format!(
                    "the table location is {}, the configuration gives {expected}",
                    meta.location()
                ),
            );
        }
    }

    /// 冷层版本 `k` 的元数据（缓存；读不到或读不懂为 `None`）。
    async fn version(&mut self, k: u64) -> Step<Option<TableMetadata>> {
        if let Some(m) = self.versions.get(&k) {
            return Ok(m.clone());
        }
        let ds = self.dataset.clone();
        let m = match read::cold_metadata(self.store, &self.cold, &ds, k).await {
            Ok(ColdRead::Ok((_, m))) => Some(m),
            Ok(_) => None,
            Err(e) => return Err(self.unavailable(cold_rel(&ds, &metadata_path(k)), e)),
        };
        self.versions.insert(k, m.clone());
        Ok(m)
    }

    /// COLD-COMMITS：旧锚点之后新增的每个冷层快照都是 Amberlog 的三种冷层提交之一，且热层链上有对应记录。
    async fn cold_commits(&mut self, from: u64) -> Step<()> {
        let Some((n, current)) = self.now.cold.clone() else {
            return Ok(());
        };
        let ds = self.dataset.clone();
        let handovers: HashSet<i64> = self
            .now
            .buckets
            .iter()
            .chain(self.now.retired_buckets.iter())
            .flat_map(|(_, m)| m.handovers.iter().map(|h| h.iceberg_snapshot_id))
            .collect();
        let recorded: HashSet<i64> = self.now.cold_ledger().map(|d| d.cold_removed_in).collect();
        let in_current: HashSet<i64> = current.snapshots().map(|s| s.snapshot_id()).collect();
        let mut prev: Option<TableMetadata> = if from == 0 {
            None
        } else {
            self.version(from).await?
        };
        for k in from + 1..=n {
            let Some(meta) = self.version(k).await? else {
                prev = None;
                continue;
            };
            let object = cold_rel(&ds, &metadata_path(k));
            let before: HashSet<i64> = prev
                .as_ref()
                .map(|p| p.snapshots().map(|s| s.snapshot_id()).collect())
                .unwrap_or_default();
            for s in meta
                .snapshots()
                .filter(|s| !before.contains(&s.snapshot_id()))
            {
                let props = &s.summary().additional_properties;
                let sid = s.snapshot_id();
                let ok = match props.get("amberlog.operation").map(String::as_str) {
                    Some("lake") => handovers.contains(&sid) || self.in_window(props),
                    Some("ttl" | "compaction") => {
                        recorded.contains(&sid) || in_current.contains(&sid)
                    }
                    _ => false,
                };
                if !ok {
                    self.report.problems.add(
                        kind::UNTRACED_COLD_COMMIT,
                        object.clone(),
                        format!("Iceberg snapshot {sid} is not an Amberlog cold commit recorded in the hot chain"),
                    );
                }
            }
            if let (Some(p), Some(cur)) = (&prev, meta.current_snapshot())
                && p.snapshot_by_id(cur.snapshot_id()).is_some()
                && p.current_snapshot().map(|c| c.manifest_list()) != Some(cur.manifest_list())
            {
                self.report.problems.add(
                    kind::UNTRACED_COLD_COMMIT,
                    object.clone(),
                    "the current snapshot was moved to an existing snapshot with another manifest list",
                );
            }
            prev = Some(meta);
        }
        Ok(())
    }

    /// 入湖冷层提交（摘要 `props`）是否处于交接窗口（REQ-AMB-STO-LAKE-ENTRY）：此刻快照中其桶仍有 `added_in` 不大于 H 的存活文件。
    fn in_window(&self, props: &HashMap<String, String>) -> bool {
        let (Some(bucket), Some(h)) = (
            props.get("amberlog.bucket"),
            props
                .get("amberlog.hot_snapshot_id")
                .and_then(|h| h.parse::<u64>().ok()),
        ) else {
            return false;
        };
        self.now
            .buckets
            .iter()
            .filter(|(_, m)| &m.bucket == bucket)
            .any(|(_, m)| m.files.iter().any(|f| f.added_in.0 <= h))
    }

    /// COPIES：证据包的元数据副本与桶里同一路径的对象比对。
    async fn copies(&mut self, p: &Package<'_>) -> Step<()> {
        let ds = self.dataset.clone();
        let ds_prefix = format!("{}/{}/", ds.org, ds.stream);
        for (name, bytes) in &p.unpacked.members {
            let Some(rel) = name.strip_prefix(package::OBJECTS_DIR) else {
                continue;
            };
            if rel == "_system/retirement.json" {
                continue;
            }
            let key = rel
                .split('/')
                .fold(self.store.root.clone(), |k, part| k.join(part));
            let got = self.get(rel, &key).await?;
            match got {
                Some(b) => {
                    if Digest::of(&b) != Digest::of(bytes) {
                        self.report.problems.add(
                            kind::DIGEST_MISMATCH,
                            rel,
                            "the object differs from its copy in the package",
                        );
                    }
                }
                None => {
                    let in_ds = rel.strip_prefix(&ds_prefix);
                    let never_deleted = in_ds.is_some_and(|r| {
                        layout::parse_retired_segment_path(r).is_some()
                            || layout::parse_cold_change_segment_path(r).is_some()
                            || self.now.retired_manifest_paths.contains(r)
                    }) || rel.ends_with(".metadata.json");
                    if never_deleted {
                        self.report
                            .problems
                            .add(kind::MISSING, rel, "the object does not exist");
                    } else {
                        self.report.notes.add(
                            kind::COPY_RECYCLED,
                            rel,
                            "the object has been removed, as snapshot recycling does",
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn object_of(&self, l: &Value) -> String {
        let path = l["path"].as_str().unwrap_or_default();
        if l["tier"] == "cold" {
            cold_rel(self.dataset, path)
        } else {
            ds_rel(self.dataset, path)
        }
    }

    /// FILE-EXT 与 COLD-PENDING。
    fn file_extension(&mut self, p: &Package<'_>) {
        let hot_live: HashSet<(String, Digest)> = self
            .now
            .hot_live()
            .into_iter()
            .map(|(a, d)| (a.to_owned(), d))
            .collect();
        for l in &p.lines {
            let path = l["path"].as_str().unwrap_or_default();
            let Some(digest) = l["content_digest"].as_str().and_then(Digest::parse) else {
                continue;
            };
            let cold = l["tier"] == "cold";
            let object = self.object_of(l);
            if l["state"] == "live" {
                let live = if cold {
                    self.now.cold_live.contains_key(path)
                } else {
                    hot_live.contains(&(path.to_owned(), digest))
                };
                let traced = if cold {
                    self.now
                        .cold_ledger()
                        .any(|d| d.path == path && d.content_digest == digest)
                } else {
                    self.now
                        .hot_ledger()
                        .any(|d| d.path == path && d.content_digest == digest)
                };
                if live || traced {
                    continue;
                }
                if cold && self.now.pending_removed.contains(path) {
                    self.report.transitions.add(
                        kind::COLD_SEGMENT_PENDING,
                        object,
                        "the cold commit removing it has not been recorded in the hot chain yet",
                    );
                } else {
                    self.report.problems.add(
                        kind::VANISHED,
                        object,
                        "the file is neither live nor traced now",
                    );
                }
            } else {
                let del = &l["deletion"];
                let found = if cold {
                    self.now.cold_ledger().any(|d| {
                        d.path == path
                            && d.content_digest == digest
                            && json!(d.max_timestamp) == l["max_timestamp"]
                            && cold_reason(d.reason) == del["reason"]
                            && json!(d.cutoff) == del["cutoff"]
                            && json!(d.deleted_at) == del["deleted_at"]
                            && json!(d.removed_in.0) == del["removed_in"]
                            && json!(d.cold_removed_in) == del["cold_removed_in"]
                    })
                } else {
                    self.now.hot_ledger().any(|d| {
                        d.path == path
                            && d.content_digest == digest
                            && json!(d.max_timestamp) == l["max_timestamp"]
                            && hot_reason(d.reason) == del["reason"]
                            && json!(d.cutoff) == del["cutoff"]
                            && json!(d.deleted_at) == del["deleted_at"]
                            && json!(d.removed_in.0) == del["removed_in"]
                    })
                };
                if !found {
                    self.report.problems.add(
                        kind::TRACE_ALTERED,
                        object,
                        "the deletion record is not in the current ledger with the same fields",
                    );
                }
            }
        }
    }

    /// HANDOVER-TRACE：入湖留痕所列的冷层文件此刻仍存活、有冷层留痕或属补写窗口。
    fn handover_trace(&mut self, p: &Package<'_>) {
        for l in p.lines.iter().filter(|l| l["deletion"]["reason"] == "lake") {
            let files = l["deletion"]["handover_files"]
                .as_array()
                .cloned()
                .unwrap_or_default();
            if files.is_empty() {
                let o = self.object_of(l);
                self.report.problems.add(
                    kind::HANDOVER_MISSING,
                    o,
                    "the lake deletion record lists no cold data file",
                );
            }
            for f in files.iter().filter_map(Value::as_str) {
                let ok = self.now.cold_live.contains_key(f)
                    || self.now.cold_ledger().any(|d| d.path == f)
                    || self.now.pending_removed.contains(f);
                if !ok {
                    self.report.problems.add(
                        kind::HANDOVER_MISSING,
                        cold_rel(self.dataset, f),
                        "the cold data file it became is neither live nor traced",
                    );
                }
            }
        }
    }

    /// HANDOVER-WINDOW：交接窗口内的入湖冷层提交新增的冷层文件只记过渡状态。
    fn handover_window(&mut self, p: &Package<'_>) {
        let Some((_, meta)) = self.now.cold.clone() else {
            return;
        };
        let handovers: HashSet<i64> = self
            .now
            .buckets
            .iter()
            .flat_map(|(_, m)| m.handovers.iter().map(|h| h.iceberg_snapshot_id))
            .collect();
        for s in meta.snapshots() {
            let props = &s.summary().additional_properties;
            if props.get("amberlog.operation").map(String::as_str) != Some("lake")
                || handovers.contains(&s.snapshot_id())
                || !self.in_window(props)
            {
                continue;
            }
            let files: Vec<(String, EntryFile)> = self
                .now
                .cold_live
                .iter()
                .filter(|(_, e)| e.snapshot_id == s.snapshot_id() && e.intersects(p.range))
                .map(|(k, e)| (k.clone(), e.clone()))
                .collect();
            for (k, e) in files {
                let object = match &e.rel {
                    Some(_) => cold_rel(self.dataset, &k),
                    None => e.uri.clone(),
                };
                self.report.transitions.add(
                    kind::HANDOVER_WINDOW,
                    object,
                    "the lake commit is in its handover window",
                );
            }
        }
    }

    /// DATA-DIGEST：读出每个存活数据文件的全部字节重算摘要（同一文件只读一次）。
    async fn data_digests(&mut self, packages: &[Package<'_>]) -> Step<()> {
        let mut files: BTreeMap<(String, String), Vec<Value>> = BTreeMap::new();
        for p in packages {
            for l in p.lines.iter().filter(|l| l["state"] == "live") {
                files
                    .entry((
                        l["tier"].as_str().unwrap_or_default().to_owned(),
                        l["path"].as_str().unwrap_or_default().to_owned(),
                    ))
                    .or_default()
                    .push(l.clone());
            }
        }
        let ds = self.dataset.clone();
        let mut ledger: Option<Traced> = None;
        for ((tier, path), lines) in files {
            let (key, object) = if tier == "cold" {
                (self.cold.object_path(&ds, &path), cold_rel(&ds, &path))
            } else {
                (self.store.object_path(&ds, &path), ds_rel(&ds, &path))
            };
            match digest_of(self.store, &key).await {
                Ok(Some((digest, size))) => {
                    self.report.digests_recomputed += 1;
                    for l in &lines {
                        if l["content_digest"].as_str() != Some(digest.to_string().as_str()) {
                            self.report.problems.add(
                                kind::DIGEST_MISMATCH,
                                object.clone(),
                                "the recomputed SHA-256 differs from the recorded content_digest",
                            );
                        }
                        if l["size_bytes"].as_u64() != Some(size) {
                            self.report.problems.add(
                                kind::SIZE_MISMATCH,
                                object.clone(),
                                format!("the object has {size} bytes"),
                            );
                        }
                    }
                }
                Ok(None) => {
                    for l in &lines {
                        let Some(d) = l["content_digest"].as_str().and_then(Digest::parse) else {
                            continue;
                        };
                        let mut found = false;
                        for _ in 0..2 {
                            let tail = match read::find_tail(self.store, &ds).await {
                                Ok(Tail::Current(c) | Tail::Hint(c)) => Some(c),
                                Ok(_) => None,
                                Err(e) => {
                                    return Err(self.unavailable(
                                        format!("{}/{}/manifest/", ds.org, ds.stream),
                                        e,
                                    ));
                                }
                            };
                            if let Err(e) =
                                crate::cold::current::version(self.store, &self.cold, &ds).await
                            {
                                return Err(self.unavailable(
                                    cold_rel(&ds, "metadata/"),
                                    Unavailable(e.to_string()),
                                ));
                            }
                            if ledger.as_ref().is_none_or(|(id, _)| *id != tail) {
                                let set = match tail {
                                    Some(t) => self.full_ledger(t).await?,
                                    None => HashSet::new(),
                                };
                                ledger = Some((tail, set));
                            }
                            if ledger
                                .as_ref()
                                .is_some_and(|(_, s)| s.contains(&(path.clone(), d)))
                            {
                                found = true;
                                break;
                            }
                        }
                        if found {
                            self.report.transitions.add(
                                kind::LEFT_LIVE,
                                object.clone(),
                                "the file left the live set and has a deletion record now",
                            );
                        } else {
                            self.report.problems.add(
                                kind::MISSING,
                                object.clone(),
                                "the live data file does not exist and has no deletion record",
                            );
                        }
                    }
                }
                Err(e) => self
                    .report
                    .unprovable
                    .add(kind::DATA_UNREADABLE, object, e.0),
            }
        }
        Ok(())
    }

    /// 快照 `id` 的完整删除台账中各条目的 (`path`, `content_digest`)（热层与冷层合在一起；读不到的部分略过）。
    async fn full_ledger(&mut self, id: SnapshotId) -> Step<HashSet<(String, Digest)>> {
        let ds = self.dataset.clone();
        let mut out = HashSet::new();
        let rel = layout::snapshot_path(id);
        let Some(bytes) = self
            .get(&ds_rel(&ds, &rel), &self.store.object_path(&ds, &rel))
            .await?
        else {
            return Ok(out);
        };
        let Ok(SnapshotContent::Supported(s)) = format::decode_snapshot(&bytes, &ds, id) else {
            return Ok(out);
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
        while let Some(r) = next.take() {
            let Some(b) = self
                .get(&ds_rel(&ds, &r.path), &self.store.object_path(&ds, &r.path))
                .await?
            else {
                break;
            };
            let Some(Ok(seg)) = layout::parse_retired_segment_path(&r.path)
                .map(|i| format::decode_retired_segment(&b, i))
            else {
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
            let restored =
                match read::bucket_manifest(self.store, &ds, &label, &head, s.format_version).await
                {
                    Ok((_, r)) => r,
                    Err(e) => return Err(self.unavailable(ds_rel(&ds, &head.path), e)),
                };
            if let Some(m) = restored {
                out.extend(
                    m.manifest
                        .deletions
                        .iter()
                        .map(|d| (d.path.clone(), d.content_digest)),
                );
            }
        }
        let mut next = s.cold_deletion_segment.clone();
        while let Some(r) = next.take() {
            let Some(b) = self
                .get(&ds_rel(&ds, &r.path), &self.store.object_path(&ds, &r.path))
                .await?
            else {
                break;
            };
            let Some(Ok(seg)) = layout::parse_cold_change_segment_path(&r.path)
                .map(|i| format::decode_cold_change_segment(&b, i))
            else {
                break;
            };
            out.extend(
                seg.deletions
                    .iter()
                    .map(|d| (d.path.clone(), d.content_digest)),
            );
            next = seg.previous;
        }
        Ok(out)
    }

    /// TRACED-PRESENT：已留痕而文件仍在只记过渡状态，不读内容。
    async fn traced_present(&mut self, packages: &[Package<'_>]) {
        let ds = self.dataset.clone();
        let mut seen = BTreeSet::new();
        for p in packages {
            for l in p.lines.iter().filter(|l| l["state"] == "deleted") {
                let path = l["path"].as_str().unwrap_or_default().to_owned();
                let cold = l["tier"] == "cold";
                if !seen.insert((cold, path.clone())) {
                    continue;
                }
                let (key, object) = if cold {
                    (self.cold.object_path(&ds, &path), cold_rel(&ds, &path))
                } else {
                    (self.store.object_path(&ds, &path), ds_rel(&ds, &path))
                };
                match read::head_size(self.store, &key).await {
                    Ok(Some(_)) => self.report.transitions.add(
                        kind::TRACED_STILL_PRESENT,
                        object,
                        "the deleted file is still in the object store (delayed physical deletion)",
                    ),
                    Ok(None) => {}
                    Err(e) => self
                        .report
                        .unprovable
                        .add(kind::DATA_UNREADABLE, object, e.0),
                }
            }
        }
    }

    /// 某证据包中的冷层快照号 `sid` 所在的元数据里的快照摘要属性；元数据不含它或版本为 null 为 `None`。
    async fn summary_of(
        &mut self,
        p: &Package<'_>,
        sid: i64,
    ) -> Step<Option<HashMap<String, String>>> {
        let Some(Some(k)) = p.map.get(&sid) else {
            return Ok(None);
        };
        Ok(self.version(*k).await?.and_then(|m| {
            m.snapshot_by_id(sid)
                .map(|s| s.summary().additional_properties.clone())
        }))
    }

    fn in_range(range: Option<(i64, i64)>, lo: i64, hi: i64) -> bool {
        range.is_none_or(|(f, t)| lo < t && hi >= f)
    }

    /// 由证据包的清单片副本还原所得的各桶 manifest（自包内快照副本与留痕归档段副本所引用的头片起；所需的清单片不在包内
    /// 或无法解析的不还原），与包内的冷层变更段副本。
    async fn copies_of(
        &self,
        p: &Package<'_>,
    ) -> (Vec<BucketManifest>, Vec<(String, ColdChangeSegment)>) {
        let ds_prefix = format!(
            "{}{}/{}/",
            package::OBJECTS_DIR,
            self.dataset.org,
            self.dataset.stream
        );
        let mut pieces: HashMap<&str, &[u8]> = HashMap::new();
        let mut heads: Vec<(String, PieceRef)> = Vec::new();
        let mut segments = Vec::new();
        for (name, bytes) in &p.unpacked.members {
            let Some(rel) = name.strip_prefix(&ds_prefix) else {
                continue;
            };
            if layout::parse_bucket_manifest_path(rel).is_some() {
                pieces.insert(rel, bytes);
            } else if let Some(id) = layout::parse_snapshot_path(rel) {
                if let Ok(SnapshotContent::Supported(snap)) =
                    format::decode_snapshot(bytes, self.dataset, id)
                {
                    heads.extend(snap.buckets.iter().map(|b| {
                        (
                            b.bucket.clone(),
                            PieceRef {
                                path: b.manifest.clone(),
                                content_digest: b.content_digest,
                            },
                        )
                    }));
                }
            } else if let Some(id) = layout::parse_retired_segment_path(rel) {
                if let Ok(seg) = format::decode_retired_segment(bytes, id) {
                    heads.extend(seg.buckets.iter().map(|b| {
                        (
                            b.bucket.clone(),
                            PieceRef {
                                path: b.manifest.clone(),
                                content_digest: b.content_digest,
                            },
                        )
                    }));
                }
            } else if let Some(id) = layout::parse_cold_change_segment_path(rel)
                && let Ok(s) = format::decode_cold_change_segment(bytes, id)
            {
                segments.push((ds_rel(self.dataset, rel), s));
            }
        }
        let mut manifests = Vec::new();
        let mut done: HashSet<String> = HashSet::new();
        for (bucket, head) in heads {
            if !pieces.contains_key(head.path.as_str()) || !done.insert(head.path.clone()) {
                continue;
            }
            let walk = chain::walk_pieces(
                &head,
                format::FORMAT_VERSION,
                &bucket,
                |rel| {
                    let got = pieces.get(rel.as_str()).map(|b| Bytes::copy_from_slice(b));
                    async move { Ok::<_, std::convert::Infallible>(got) }
                },
                |_| None,
            )
            .await
            .unwrap_or_else(|e| match e {});
            if let Ok(r) = chain::restore(&walk, format::FORMAT_VERSION, &bucket) {
                manifests.push((*r.manifest).clone());
            }
        }
        (manifests, segments)
    }

    /// COLD-RECORD：交接记录与合并记录的摘要等于对应冷层快照的 `amberlog.file_digests`。
    async fn cold_records(&mut self, p: &Package<'_>) -> Step<()> {
        let (manifests, segments) = self.copies_of(p).await;
        let mut records: Vec<(i64, crate::chain::ColdFileEntry)> = Vec::new();
        for m in &manifests {
            for h in &m.handovers {
                records.extend(h.files.iter().map(|f| (h.iceberg_snapshot_id, f.clone())));
            }
        }
        for (_, s) in &segments {
            if let Some(d) = s.deletions.first() {
                records.extend(s.additions.iter().map(|f| (d.cold_removed_in, f.clone())));
            }
        }
        for (sid, f) in records {
            if !Self::in_range(p.range, f.min_timestamp, f.max_timestamp) {
                continue;
            }
            let ok = self.summary_of(p, sid).await?.is_some_and(|props| {
                props
                    .get("amberlog.file_digests")
                    .and_then(|j| serde_json::from_str::<BTreeMap<String, String>>(j).ok())
                    .is_some_and(|m| {
                        m.get(&f.path).map(String::as_str)
                            == Some(f.content_digest.to_string().as_str())
                    })
            });
            if !ok {
                self.report.problems.add(
                    kind::COLD_RECORD_MISMATCH,
                    cold_rel(self.dataset, &f.path),
                    format!("amberlog.file_digests of Iceberg snapshot {sid} does not record content_digest {}", f.content_digest),
                );
            }
        }
        Ok(())
    }

    /// TTL-CUTOFF：冷层变更段 `ttl` 留痕的 `cutoff` 等于对应冷层快照的 `amberlog.ttl_cutoff`。
    async fn ttl_cutoffs(&mut self, p: &Package<'_>) -> Step<()> {
        let (manifests, segments) = self.copies_of(p).await;
        let spans: HashMap<String, (i64, i64)> = manifests
            .iter()
            .flat_map(|m| m.handovers.iter().flat_map(|h| h.files.iter()))
            .chain(segments.iter().flat_map(|(_, s)| s.additions.iter()))
            .map(|f| (f.path.clone(), (f.min_timestamp, f.max_timestamp)))
            .collect();
        for (_, s) in &segments {
            for d in s
                .deletions
                .iter()
                .filter(|d| d.reason == ColdDeletionReason::Ttl)
            {
                let in_range = match spans.get(&d.path) {
                    Some((lo, hi)) => Self::in_range(p.range, *lo, *hi),
                    None => p
                        .range
                        .is_none_or(|(f, t)| d.max_timestamp >= f && d.max_timestamp < t),
                };
                if !in_range {
                    continue;
                }
                let ok = self
                    .summary_of(p, d.cold_removed_in)
                    .await?
                    .is_some_and(|props| {
                        props
                            .get("amberlog.ttl_cutoff")
                            .and_then(|c| c.parse::<i64>().ok())
                            == d.cutoff
                    });
                if !ok {
                    self.report.problems.add(
                        kind::TTL_CUTOFF_MISMATCH,
                        cold_rel(self.dataset, &d.path),
                        "amberlog.ttl_cutoff of the cold snapshot differs from the cutoff of the deletion record",
                    );
                }
            }
        }
        Ok(())
    }

    /// COUNT-CONSERVE：冷层合并前后条数守恒（输入都在本包 `files.jsonl` 且条数已知时才核对）。
    async fn count_conserve(&mut self, p: &Package<'_>) {
        let (_, segments) = self.copies_of(p).await;
        for (path, s) in segments.iter().filter(|(_, s)| !s.additions.is_empty()) {
            let inputs: Vec<&ColdDeletion> = s
                .deletions
                .iter()
                .filter(|d| d.reason == ColdDeletionReason::Compaction)
                .collect();
            let counts: Option<u64> = inputs
                .iter()
                .map(|d| {
                    p.lines
                        .iter()
                        .find(|l| l["tier"] == "cold" && l["path"] == d.path.as_str())
                        .and_then(|l| l["record_count"].as_u64())
                })
                .sum();
            match counts {
                Some(n) => {
                    let out: u64 = s.additions.iter().map(|a| a.record_count).sum();
                    if n != out {
                        self.report.problems.add(
                            kind::COUNT_MISMATCH,
                            path.clone(),
                            format!("{n} records in, {out} records out"),
                        );
                    }
                }
                None => self.report.notes.add(
                    kind::COUNT_NOT_CHECKED,
                    path.clone(),
                    "not every compaction input is in this package with a known record count",
                ),
            }
        }
    }

    /// TTL-ORDER 与 RETENTION：没有提前删除。
    fn ttl_order(&mut self, p: &Package<'_>, retention: Option<i64>) {
        for l in p.lines.iter().filter(|l| l["deletion"]["reason"] == "ttl") {
            let object = self.object_of(l);
            let (Some(max), Some(cutoff)) = (
                l["max_timestamp"].as_i64(),
                l["deletion"]["cutoff"].as_i64(),
            ) else {
                continue;
            };
            if max >= cutoff {
                self.report.problems.add(
                    kind::EARLY_DELETION,
                    object.clone(),
                    format!("max_timestamp {max} is not earlier than cutoff {cutoff}"),
                );
            }
            if let (Some(r), Some(deleted_at)) = (retention, l["deletion"]["deleted_at"].as_i64())
                && cutoff > deleted_at - r
            {
                self.report.problems.add(
                    kind::EARLY_DELETION,
                    object,
                    format!("cutoff {cutoff} is later than deleted_at {deleted_at} minus the retention of {r} microseconds"),
                );
            }
        }
    }

    /// RETIREMENT：退役记录只作提示。
    async fn retirement(&mut self, packages: &[Package<'_>]) -> Step<()> {
        const PATH: &str = "_system/retirement.json";
        let got = self
            .get(PATH, &self.store.system_path("retirement.json"))
            .await?;
        let copies: Vec<Value> = packages
            .iter()
            .filter_map(|p| p.unpacked.members.get("objects/_system/retirement.json"))
            .map(|b| serde_json::from_slice(b).unwrap_or(Value::Null))
            .collect();
        let Some(bytes) = got else {
            if !copies.is_empty() {
                self.report.notes.add(kind::RETIREMENT_MISMATCH, PATH, "证据包记有退役记录，桶里已没有 (the package has a retirement record, the bucket no longer does)");
            }
            return Ok(());
        };
        let Some(record) = serde_json::from_slice::<Value>(&bytes)
            .ok()
            .filter(Value::is_object)
        else {
            self.report.notes.add(
                kind::RETIREMENT_UNREADABLE,
                PATH,
                "the retirement record is not a JSON object",
            );
            return Ok(());
        };
        let current = self.report.to_json()["current"].clone();
        let ds = self.report.dataset.clone();
        let matches = record["datasets"].as_array().is_some_and(|all| {
            all.iter().any(|e| {
                e["dataset"] == ds.as_str()
                    && e["snapshot"] == current["snapshot"]
                    && e["cold"] == current["cold"]
            })
        });
        if !matches || copies.iter().any(|c| *c != record) {
            self.report.notes.add(
                kind::RETIREMENT_MISMATCH,
                PATH,
                "the retirement record does not match the current heads or the copy in the package",
            );
        }
        Ok(())
    }
}

/// 某个热层快照（编号）的完整删除台账中各条目的 (`path`, `content_digest`)。
type Traced = (Option<SnapshotId>, HashSet<(String, Digest)>);

enum Seg {
    Retired(RetiredSegment),
    Cold(ColdChangeSegment),
}

impl Seg {
    fn previous(&self) -> Option<&SegmentRef> {
        match self {
            Seg::Retired(s) => s.previous.as_ref(),
            Seg::Cold(s) => s.previous.as_ref(),
        }
    }
}

fn obj_ref(r: &SegmentRef) -> package::ObjectRef {
    package::ObjectRef {
        path: r.path.clone(),
        content_digest: r.content_digest,
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

fn cold_reason(r: ColdDeletionReason) -> &'static str {
    match r {
        ColdDeletionReason::Ttl => "ttl",
        ColdDeletionReason::Compaction => "compaction",
    }
}

/// 读出对象的全部字节、边读边算 SHA-256：(摘要, 字节数)；不存在为 `None`。
async fn digest_of(
    store: &Store,
    key: &object_store::path::Path,
) -> Result<Option<(Digest, u64)>, Unavailable> {
    use sha2::{Digest as _, Sha256};
    let got = match store.inner.get(key).await {
        Ok(g) => g,
        Err(object_store::Error::NotFound { .. }) => {
            return read::head_size(store, key).await.map(|_| None);
        }
        Err(e) => {
            return Err(Unavailable(format!(
                "object store read of {key} failed: {e}"
            )));
        }
    };
    let mut stream = got.into_stream();
    let mut h = Sha256::new();
    let mut n = 0u64;
    loop {
        match stream.try_next().await {
            Ok(Some(chunk)) => {
                n += chunk.len() as u64;
                h.update(&chunk);
            }
            Ok(None) => break,
            Err(object_store::Error::NotFound { .. }) => return Ok(None),
            Err(e) => {
                return Err(Unavailable(format!(
                    "object store read of {key} failed: {e}"
                )));
            }
        }
    }
    Ok(Some((Digest(h.finalize().into()), n)))
}
