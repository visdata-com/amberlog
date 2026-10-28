//! 热层巡检（REQ-AMB-STO-HOT-FSCK-V4）：清理崩溃留下的孤儿数据文件与孤儿清单片，补删已留痕而未删成的入湖、到期、
//! 合并输入与 `rollback` 文件；同一周期、同一数据集上先做热层快照回收（[`crate::snapshot_gc`]），待其结束再巡检。
//!
//! 自开始受理起每个 `hot_fsck.interval` 对每个存在且正常的数据集依次回收、巡检一次（上一次未结束时顺延）。一次巡检：
//! 1. 记下开始时刻，列出 `data/` 与 `manifest/buckets/` 下的对象（候选）；
//! 2. 取本进程在途提交已上传的对象（在列举之后、读快照之前取，使「列举时已上传、读快照时已提交」的对象一定出现在读到的
//!    快照里，「仍在提交中」的对象一定在这份在途表里）；
//! 3. 列出 `manifest/` 下的快照与 `manifest/retired/` 下的留痕归档段，按找链尾确定当前快照（自行读提示与探测，不登记
//!    停用状态）；自当前快照沿 `parent_snapshot_id` 回溯读出全部**可达快照**（遇到创世快照或列举中没有的父快照即止，
//!    止处为**链底**），读出可达快照与全部现存留痕归档段 `buckets` 引用的每个头片，沿 `previous` 与 `ledger_previous`
//!    逐片读到底、按 REQ-AMB-STO-HOT-LAYOUT-V8 还原（与提交器、证据导出共用 [`crate::chain::walk_pieces`]、
//!    [`crate::chain::restore`]），逐片核对清单片与被引用段完整字节的摘要；这些清单片都是**可达清单片**。任一读不到、
//!    无法解析或摘要不符，或对象存储不可访问，即放弃本次巡检、不删除任何对象。链底不是创世快照、
//!    列举所见有编号小于链底的快照、且链底编号大于保留起点（取同一周期回收所定；回收未定出时按同一定义自行确定，定不出
//!    亦放弃）时，**断档即放弃**。读不全与断档放弃时输出一行非稳定 warn 日志。
//!    快照、段与清单片写出后不可变，已读并已核对的内容跨巡检缓存（只会使巡检少删）；可达快照集合每次按回溯重新确定。
//!    头片按快照编号从小到大还原，每桶记住最近还原的一份，下一份头片的 `previous` 是它时只多读一片；
//!    缓存不存还原所得的整份桶 manifest，只存各清单片自己的内容（相邻版本共用增量链上的片），快照的 `buckets` 只存与父快照
//!    的差异，路径各存一份：缓存随清单片与快照的个数增长，不随「版本数 × 每版文件数」增长；
//! 4. 孤儿数据文件（路径符合模板、不在任一可达桶 manifest 的 `files` 与 `deletions` 中、不在途、上传完成早于开始时刻减
//!    `hot_fsck.min_orphan_age`）读出完整字节算摘要，经快照提交记 `orphan` 留痕，留痕所在快照创建成功之后再删除；
//!    当前可达桶 manifest 中已有 `orphan` 留痕、仍存在且不在任一可达 `files` 中的数据文件直接删除；已有 `lake`、`ttl`、
//!    `compaction` 或 `rollback` 留痕、`deleted_at` 早于开始时刻减 `storage.delete_delay`、仍存在且不在当前可达 `files`
//!    中的数据文件直接删除（重启后的自行补删见 [`crate::purge`]，这里是兜底）；孤儿清单片（路径合模板、不是可达清单片，
//!    含只被已回收快照引用的）直接删除。
//!
//! 巡检只列举与删除数据集前缀下 `data/` 与 `manifest/buckets/` 的对象，提示、快照、两种段、`counters/`、`_system/` 与
//! `<storage.prefix>/_iceberg/` 之下的对象都不在候选之列。巡检不修复、不回滚，不核对存活数据文件的摘要。按周期运行
//! （[`BucketTask`]）；停机过程开始后不再开始新的回收与巡检，进行中的由 [`crate::maintenance::BucketTasks`] 中止。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{
    self, CHAIN_LIMIT, DeletionReason, Digest, Piece, PieceLink, PieceProblem, PieceRef, PieceWalk,
    Restored, RetiredSegment, SegmentRef, SnapshotId,
};
use crate::committer::{CommitError, TombstoneRequest};
use crate::fault;
use crate::flusher::Flusher;
use crate::format::{self, FORMAT_VERSION, SnapshotContent};
use crate::gate::DatasetState;
use crate::layout::{self, HourBucket};
use crate::maintenance::BucketTask;
use crate::registry::Persistence;
use crate::snapshot_gc::{self, Abandon, Links, RetentionConfig};
use crate::store::Store;

/// 巡检的配置。
#[derive(Debug, Clone, Copy)]
pub struct FsckConfig {
    /// `hot_fsck.interval`（也是快照回收的周期）
    pub interval: Duration,
    /// `hot_fsck.min_orphan_age`
    pub min_orphan_age: Duration,
    /// `storage.delete_delay`：已留痕的入湖、到期、合并输入与 `rollback` 文件至少过这么久才补删。
    pub delete_delay: Duration,
    /// `hot_retention.*`：快照回收的保留设置。
    pub retention: RetentionConfig,
}

/// 巡检与快照回收的可观察结果（非稳定指标与日志）。
pub trait FsckEvents: Send + Sync {
    /// 巡检删除了一个对象：`data_file` 为真即数据文件，否则为清单片。
    fn deleted(&self, dataset: &Dataset, data_file: bool);

    /// 快照回收删除了快照 `id`；`resurrected` 为真即重现快照。
    fn snapshot_deleted(&self, _dataset: &Dataset, _id: SnapshotId, _resurrected: bool) {}

    /// 因读不全或断档放弃本次快照回收（`task` 为 `snapshot_gc`）或巡检（`hot_fsck`），`reason` 为英文短语。
    fn abandoned(&self, _dataset: &Dataset, _task: &str, _reason: &str) {}
}

/// 一个桶 manifest 的头片引用（快照 `buckets` 的一个元素）。相邻快照中不变的桶共用同一份。
#[derive(Debug, PartialEq, Eq)]
struct Head {
    bucket: Arc<str>,
    path: Arc<str>,
    digest: Digest,
}

impl Head {
    fn piece_ref(&self) -> PieceRef {
        PieceRef {
            path: self.path.to_string(),
            content_digest: self.digest,
        }
    }
}

/// 一个快照的 `buckets`：桶 → 头片。
type Heads = BTreeMap<Arc<str>, Arc<Head>>;

/// 由一个快照的 `buckets` 得到其父快照的 `buckets` 所要改的桶：(桶, 父快照中该桶的头片；父快照没有该桶为 `None`)。
type ToParent = Vec<(Arc<str>, Option<Arc<Head>>)>;

/// 一个已读并核对过（其所引用的段已核对）的快照中巡检要用到的内容。`buckets` 不整份存，只存与父快照的差异：
/// 自当前快照（整份 `buckets` 见 [`Known::top`]）逐个回推。
#[derive(Clone)]
struct SnapshotFacts {
    parent: Option<SnapshotId>,
    format_version: u64,
    retired_segment: Option<Arc<SegmentRef>>,
    /// 读到它时父快照不可达（它是链底）为 `None`。
    to_parent: Option<ToParent>,
}

/// 一个清单片中巡检要用到的内容。桶 manifest 由各片按 REQ-AMB-STO-HOT-LAYOUT-V8 还原，缓存只存各片自己的内容。
struct PieceFacts {
    link: Link,
    /// 全量片的 `files` 或增量片的 `files_added` 中各条目的路径。
    files: Vec<Arc<str>>,
    /// 本片的 `deletions`。
    deletions: Vec<Tomb>,
}

enum Link {
    /// 增量片的 `previous`。
    Delta(Arc<str>),
    /// 全量片的 `ledger_previous`。
    Full(Option<Arc<str>>),
}

/// 一个留痕条目中巡检要用到的内容。
#[derive(Debug, Clone, PartialEq)]
struct Tomb {
    path: Arc<str>,
    reason: DeletionReason,
    deleted_at: i64,
}

/// 一个数据集已读并核对过的内容（跨巡检缓存）。
#[derive(Default)]
struct Known {
    snapshots: HashMap<SnapshotId, SnapshotFacts>,
    /// 上一次巡检的当前快照及其 `buckets`。
    top: Option<(SnapshotId, Heads)>,
    /// 已读并核对过的留痕归档段（按路径）及其完整字节摘要。
    segments: HashMap<String, (Digest, Arc<RetiredSegment>)>,
    /// 已核对过的头片的路径与完整字节摘要（其整条链都已核对，链上各片都在 `pieces` 中）。
    manifests: HashMap<Arc<str>, Digest>,
    /// 已核对过的清单片（按路径）。
    pieces: HashMap<Arc<str>, PieceFacts>,
    /// 上一次巡检时当前快照各头片的还原结果（不含列统计）：下一次还原新头片时从它接起。
    restored: HashMap<String, Arc<Restored>>,
    /// 上面各处用到的桶、清单片与数据文件路径，每个只存一份。
    paths: HashSet<Arc<str>>,
}

/// 巡检缓存的规模（各项条目数），测试与诊断用。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheSize {
    /// 缓存的快照个数。
    pub snapshots: usize,
    /// 各快照记下的与父快照的差异条数之和。
    pub snapshot_changes: usize,
    /// 缓存的清单片个数。
    pub pieces: usize,
    /// 各清单片中文件条目与留痕条目的条数之和。
    pub piece_entries: usize,
    /// 当前快照各头片还原结果中文件条目与留痕条目的条数之和。
    pub restored_entries: usize,
    /// 共用的路径个数。
    pub paths: usize,
}

/// 本次巡检所见的可达情况。
#[derive(Default)]
struct Reach {
    /// 可达桶 manifest 的头片（可达快照与留痕归档段所引用的）。
    manifests: HashSet<Arc<str>>,
    /// 可达清单片：可达头片，与经其 `previous`、`ledger_previous` 可达的清单片。
    pieces: HashSet<Arc<str>>,
    /// 可达头片增量链上各片（含基准片）加入的文件路径：含可达桶 manifest 的 `files` 中出现过的全部路径；多出的只是
    /// 加入后又被同一链上更晚的增量片删除的，都在 `deleted` 中。只凭它与 `deleted` 判孤儿与凭 `orphan` 留痕删除
    /// 都不受多出的影响；凭 `orphan` 留痕删除时对其中的路径另按 [`Known::in_files`] 精确判定。
    files: HashSet<Arc<str>>,
    /// 可达桶 manifest 的 `deletions` 中出现过的路径。
    deleted: HashSet<Arc<str>>,
    /// 当前可达桶 manifest 的头片：当前快照 `buckets` 所引用的，与经其 `retired_segment` 沿 `previous` 可达的段所引用的。
    current: Vec<Arc<str>>,
}

impl Known {
    /// 把 `walk` 读到的清单片记入缓存（已有的不再记）。
    fn add_pieces(&mut self, walk: &PieceWalk) {
        for v in &walk.visits {
            if let Some(p) = &v.piece
                && !self.pieces.contains_key(v.reference.path.as_str())
            {
                let path = intern(&mut self.paths, &v.reference.path);
                let facts = PieceFacts::of(p, &mut self.paths);
                self.pieces.insert(path, facts);
            }
        }
    }

    /// 自 `reach.manifests` 中各头片起，沿 `previous` 与 `ledger_previous` 取全可达清单片，并取其留痕路径与增量链上
    /// 加入的文件路径；有片不在缓存中为 `false`。
    fn reach_pieces(&self, reach: &mut Reach) -> bool {
        let Reach {
            manifests,
            pieces,
            files,
            deleted,
            ..
        } = reach;
        // 已按增量链走过的片：其下的增量链与留痕链都已取过
        let mut on_chain: HashSet<&str> = HashSet::new();
        for head in manifests.iter() {
            let mut next = Some(head);
            while let Some(path) = next.take() {
                if !on_chain.insert(path) {
                    break;
                }
                let Some(p) = self.pieces.get(path) else {
                    return false;
                };
                files.extend(p.files.iter().cloned());
                match &p.link {
                    Link::Delta(previous) => {
                        if pieces.insert(Arc::clone(path)) {
                            deleted.extend(p.deletions.iter().map(|t| Arc::clone(&t.path)));
                        }
                        next = Some(previous);
                    }
                    Link::Full(_) => {
                        // 基准片与其留痕链
                        let mut ledger = Some(path);
                        while let Some(path) = ledger.take() {
                            let Some(p) = self.pieces.get(path) else {
                                return false;
                            };
                            if !pieces.insert(Arc::clone(path)) {
                                break;
                            }
                            deleted.extend(p.deletions.iter().map(|t| Arc::clone(&t.path)));
                            ledger = match &p.link {
                                Link::Full(previous) => previous.as_ref(),
                                Link::Delta(_) => return false,
                            };
                        }
                    }
                }
            }
        }
        true
    }

    /// 由缓存中的清单片按 REQ-AMB-STO-HOT-LAYOUT-V8「还原」头片 `head` 所指桶 manifest 的 `files`（路径）与
    /// `deletions`（与 [`chain::restore`] 同序）；有片不在缓存中为 `None`。
    fn restore_facts(&self, head: &str) -> Option<(Vec<Arc<str>>, Vec<&Tomb>)> {
        let mut deltas: Vec<&PieceFacts> = Vec::new();
        let mut next = head;
        let (base, mut ledger_next) = loop {
            let p = self.pieces.get(next)?;
            match &p.link {
                Link::Delta(previous) => {
                    deltas.push(p);
                    next = previous;
                }
                Link::Full(previous) => break (p, previous.as_deref()),
            }
            if deltas.len() > CHAIN_LIMIT {
                return None;
            }
        };
        let mut ledger: Vec<&PieceFacts> = Vec::new();
        while let Some(path) = ledger_next {
            let p = self.pieces.get(path)?;
            ledger.push(p);
            if ledger.len() > CHAIN_LIMIT {
                return None;
            }
            ledger_next = match &p.link {
                Link::Full(previous) => previous.as_deref(),
                Link::Delta(_) => return None,
            };
        }
        let mut tombs: Vec<&Tomb> = ledger.iter().rev().flat_map(|p| &p.deletions).collect();
        tombs.extend(&base.deletions);
        let mut files = base.files.clone();
        for d in deltas.iter().rev() {
            if !d.deletions.is_empty() {
                let gone: HashSet<&str> = d.deletions.iter().map(|t| &*t.path).collect();
                files.retain(|f| !gone.contains(&**f));
            }
            files.extend(d.files.iter().cloned());
            tombs.extend(&d.deletions);
        }
        Some((files, tombs))
    }

    /// `path` 是否在头片属 `heads` 的某个桶 manifest 还原所得的 `files` 中：自头片沿 `previous` 往回找第一份提到它的片——
    /// 增量片的 `files_added` 含它即在、`deletions` 含它即不在（同一增量片先删后加），到基准片则看其 `files`。
    /// 每片的结论记下，走到记过的片即止。有片不在缓存中按「在」（只会使巡检少删）。
    fn in_files<'a>(&'a self, heads: &'a HashSet<Arc<str>>, path: &str) -> bool {
        let mut seen: HashSet<&'a str> = HashSet::new();
        for head in heads {
            let mut next: &str = head;
            loop {
                if !seen.insert(next) {
                    break;
                }
                let Some(p) = self.pieces.get(next) else {
                    return true;
                };
                if p.files.iter().any(|f| &**f == path) {
                    return true;
                }
                match &p.link {
                    Link::Full(_) => break,
                    Link::Delta(previous) => {
                        if p.deletions.iter().any(|t| &*t.path == path) {
                            break;
                        }
                        next = previous;
                    }
                }
            }
        }
        false
    }
}

impl PieceFacts {
    fn of(p: &Piece, paths: &mut HashSet<Arc<str>>) -> PieceFacts {
        PieceFacts {
            link: match &p.link {
                PieceLink::Delta { previous } => Link::Delta(intern(paths, &previous.path)),
                PieceLink::Full { ledger_previous } => {
                    Link::Full(ledger_previous.as_ref().map(|r| intern(paths, &r.path)))
                }
            },
            files: p.files.iter().map(|f| intern(paths, &f.path)).collect(),
            deletions: p
                .deletions
                .iter()
                .map(|d| Tomb {
                    path: intern(paths, &d.path),
                    reason: d.reason,
                    deleted_at: d.deleted_at,
                })
                .collect(),
        }
    }
}

/// `paths` 中与 `s` 相同的那一份；没有就加进去。
fn intern(paths: &mut HashSet<Arc<str>>, s: &str) -> Arc<str> {
    if let Some(p) = paths.get(s) {
        return Arc::clone(p);
    }
    let p: Arc<str> = Arc::from(s);
    paths.insert(Arc::clone(&p));
    p
}

/// 由 `child`（一个快照的 `buckets`）得到 `parent`（其父快照的）所要改的桶。
fn diff(child: &Heads, parent: &Heads) -> ToParent {
    let changed = child
        .iter()
        .filter(|(b, h)| parent.get(*b).is_none_or(|p| !Arc::ptr_eq(p, h) && p != *h))
        .map(|(b, _)| (Arc::clone(b), parent.get(b).cloned()));
    let added = parent
        .iter()
        .filter(|(b, _)| !child.contains_key(*b))
        .map(|(b, h)| (Arc::clone(b), Some(Arc::clone(h))));
    changed.chain(added).collect()
}

/// 热层巡检（连同快照回收）的驱动。
pub struct HotFsck {
    flusher: Arc<Flusher>,
    config: FsckConfig,
    events: Arc<dyn FsckEvents>,
    known: Mutex<HashMap<Dataset, Known>>,
    links: Links,
    stopping: AtomicBool,
}

impl BucketTask for HotFsck {
    fn period(&self) -> Duration {
        self.config.interval
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(HotFsck::run_once(self))
    }

    fn stop(&self) {
        HotFsck::stop(self);
    }
}

impl HotFsck {
    pub fn new(flusher: Arc<Flusher>, config: FsckConfig, events: Arc<dyn FsckEvents>) -> HotFsck {
        HotFsck {
            flusher,
            config,
            events,
            known: Mutex::default(),
            links: Links::default(),
            stopping: AtomicBool::new(false),
        }
    }

    /// 数据集 `dataset` 的巡检缓存的规模。
    pub fn cache_size(&self, dataset: &Dataset) -> CacheSize {
        self.known(dataset, |k| CacheSize {
            snapshots: k.snapshots.len(),
            snapshot_changes: k
                .snapshots
                .values()
                .map(|f| f.to_parent.as_ref().map_or(0, Vec::len))
                .sum(),
            pieces: k.pieces.len(),
            piece_entries: k
                .pieces
                .values()
                .map(|p| p.files.len() + p.deletions.len())
                .sum(),
            restored_entries: k
                .restored
                .values()
                .map(|r| r.manifest.files.len() + r.manifest.deletions.len())
                .sum(),
            paths: k.paths.len(),
        })
    }

    /// 停机过程开始：此后不再开始新的回收与巡检。
    pub fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    /// 对每个存在且正常的数据集依次回收、巡检一次（BEFORE-FSCK：同一数据集回收结束后才巡检）。
    pub async fn run_once(&self) {
        let chain = self.flusher.committer().chain();
        for dataset in chain.registry.datasets() {
            if self.stopping() {
                return;
            }
            if !self.eligible(&dataset) {
                continue;
            }
            let retain = snapshot_gc::collect(
                &chain.store,
                &self.links,
                chain.in_use(),
                &dataset,
                self.config.retention,
                &*self.events,
                &|| self.stopping(),
            )
            .await;
            if self.stopping() {
                return;
            }
            if !self.eligible(&dataset) {
                continue;
            }
            if let Err(Abandon::Incomplete(reason)) = self.check(&dataset, retain).await {
                self.events.abandoned(&dataset, "hot_fsck", reason);
            }
        }
    }

    /// 数据集存在且未停用。
    fn eligible(&self, dataset: &Dataset) -> bool {
        let chain = self.flusher.committer().chain();
        chain.registry.persistence(dataset) == Some(Persistence::Stored)
            && chain.gates.state(dataset) == DatasetState::Normal
    }

    fn known<R>(&self, dataset: &Dataset, f: impl FnOnce(&mut Known) -> R) -> R {
        let mut all = self.known.lock().expect("巡检缓存锁未中毒");
        f(all.entry(dataset.clone()).or_default())
    }

    async fn check(&self, dataset: &Dataset, retain: Option<SnapshotId>) -> Result<(), Abandon> {
        let committer = self.flusher.committer();
        let store = &committer.chain().store;
        // 上传完成时刻是对象存储报告的修改时间，与本机系统时钟直接比较；`deleted_at` 取自快照的 `created_at`，
        // 与桶生命周期同一个时钟比较
        let cutoff = system_now_us().saturating_sub(micros(self.config.min_orphan_age));
        let due = crate::lifecycle::now_us().saturating_sub(micros(self.config.delete_delay));
        let prefix = store.dataset_prefix(dataset);
        let rel = |p: &object_store::path::Path| -> Option<String> {
            p.prefix_match(&prefix).map(|parts| {
                parts
                    .map(|x| x.as_ref().to_owned())
                    .collect::<Vec<_>>()
                    .join("/")
            })
        };

        // 1. 候选
        let mut data: HashMap<String, (HourBucket, i64)> = HashMap::new();
        for o in store
            .list(&prefix.clone().join("data"))
            .await
            .map_err(|_| Abandon::Silent)?
        {
            if let Some(path) = rel(&o.location)
                && let Some(hour) = layout::parse_data_file_path(&path)
            {
                data.insert(path, (hour, o.last_modified.timestamp_micros()));
            }
        }
        let mut bucket_manifests: HashMap<String, i64> = HashMap::new();
        let buckets_prefix = prefix.clone().join(layout::MANIFEST_DIR).join("buckets");
        for o in store
            .list(&buckets_prefix)
            .await
            .map_err(|_| Abandon::Silent)?
        {
            if let Some(path) = rel(&o.location)
                && layout::parse_bucket_manifest_path(&path).is_some()
            {
                bucket_manifests.insert(path, o.last_modified.timestamp_micros());
            }
        }
        // 2. 在途
        let inflight = committer.inflight().snapshot(dataset);

        // 3. 读全可达快照、现存留痕归档段与可达清单片
        let reach = self.read_chain(dataset, retain).await?;
        let (orphans, to_delete, orphan_manifests) = self.known(dataset, |known| {
            let orphans: Vec<(String, HourBucket)> = data
                .iter()
                .filter(|(p, (_, modified))| {
                    *modified < cutoff
                        && !reach.files.contains(p.as_str())
                        && !reach.deleted.contains(p.as_str())
                        && !inflight.contains(*p)
                })
                .map(|(p, (hour, _))| (p.clone(), *hour))
                .collect();
            let current: Vec<(Vec<Arc<str>>, Vec<&Tomb>)> = reach
                .current
                .iter()
                .filter_map(|m| known.restore_facts(m))
                .collect();
            let current_files: HashSet<&str> = current
                .iter()
                .flat_map(|(files, _)| files.iter().map(|f| &**f))
                .collect();
            let mut to_delete: Vec<String> = Vec::new();
            for t in current.iter().flat_map(|(_, tombs)| tombs) {
                let path: &str = &t.path;
                if !data.contains_key(path) || to_delete.iter().any(|p| p == path) {
                    continue;
                }
                let delete = match t.reason {
                    DeletionReason::Orphan => {
                        !reach.files.contains(path) || !known.in_files(&reach.manifests, path)
                    }
                    DeletionReason::Lake
                    | DeletionReason::Ttl
                    | DeletionReason::Compaction
                    | DeletionReason::Rollback => {
                        t.deleted_at < due && !current_files.contains(path)
                    }
                };
                if delete {
                    to_delete.push(path.to_owned());
                }
            }
            let orphan_manifests: Vec<String> = bucket_manifests
                .iter()
                .filter(|(p, modified)| {
                    **modified < cutoff
                        && !reach.pieces.contains(p.as_str())
                        && !inflight.contains(*p)
                })
                .map(|(p, _)| p.clone())
                .collect();
            (orphans, to_delete, orphan_manifests)
        });

        // 4. 孤儿数据文件：先留痕、后删除
        if self.stopping() {
            return Err(Abandon::Silent);
        }
        let mut to_delete = to_delete;
        if !orphans.is_empty() {
            let mut request = TombstoneRequest {
                orphans: Vec::new(),
            };
            for (path, hour) in &orphans {
                match store.read(&store.object_path(dataset, path)).await {
                    Err(_) => return Err(Abandon::Silent),
                    Ok(None) => {}
                    Ok(Some(got)) => {
                        request
                            .orphans
                            .push((*hour, path.clone(), Digest::of(&got.bytes)))
                    }
                }
            }
            if !request.orphans.is_empty() {
                match committer.commit_tombstones(dataset, &request).await {
                    Ok(_) => {
                        self.flusher.record_ok(dataset);
                        fault::pause("hot_fsck.after_tombstone");
                        to_delete.extend(request.orphans.into_iter().map(|(_, p, _)| p));
                    }
                    Err(CommitError::Exhausted | CommitError::Disabled(_)) => {
                        self.flusher.record_failed(dataset);
                        return Err(Abandon::Silent);
                    }
                    Err(CommitError::Unavailable | CommitError::Wal(_)) => {
                        return Err(Abandon::Silent);
                    }
                    Err(CommitError::Stale) => {}
                }
            }
        }
        for path in to_delete {
            if store
                .delete(&store.object_path(dataset, &path))
                .await
                .map_err(|_| Abandon::Silent)?
            {
                self.events.deleted(dataset, true);
            }
        }
        for path in orphan_manifests {
            if store
                .delete(&store.object_path(dataset, &path))
                .await
                .map_err(|_| Abandon::Silent)?
            {
                self.events.deleted(dataset, false);
            }
        }
        Ok(())
    }

    /// 读全数据集的可达快照、现存留痕归档段与可达清单片（已核对过的取缓存），判断档即放弃；`retain` 为同一周期
    /// 快照回收定出的保留起点。缓存与快照链对不上（只在快照被外部删改时可能）时清空该数据集的缓存、全部重读一次。
    async fn read_chain(
        &self,
        dataset: &Dataset,
        retain: Option<SnapshotId>,
    ) -> Result<Reach, Abandon> {
        if let Some(reach) = self.read_chain_once(dataset, retain).await? {
            return Ok(reach);
        }
        self.known(dataset, |k| *k = Known::default());
        self.read_chain_once(dataset, retain)
            .await?
            .ok_or(Abandon::Silent)
    }

    /// [`Self::read_chain`] 的一遍：缓存与快照链对不上为 `Ok(None)`。快照的缓存只在本遍读全之后才更新。
    async fn read_chain_once(
        &self,
        dataset: &Dataset,
        retain: Option<SnapshotId>,
    ) -> Result<Option<Reach>, Abandon> {
        let store = &self.flusher.committer().chain().store;
        let listed = snapshot_gc::list_snapshots(store, dataset).await?;
        let retired_dir = store
            .dataset_prefix(dataset)
            .join(layout::MANIFEST_DIR)
            .join("retired");
        let (objects, _) = store
            .list_level(&retired_dir)
            .await
            .map_err(|_| Abandon::Silent)?;
        let segment_paths: Vec<String> = objects
            .iter()
            .filter_map(|o| o.location.filename())
            .map(|name| format!("{}/retired/{name}", layout::MANIFEST_DIR))
            .filter(|p| layout::parse_retired_segment_path(p).is_some())
            .collect();

        let current = find_current(store, dataset, &listed).await?;

        // 现存的留痕归档段：读出、解析并缓存
        for path in &segment_paths {
            if self.known(dataset, |k| k.segments.contains_key(path)) {
                continue;
            }
            let id = layout::parse_retired_segment_path(path).expect("已按模板筛过");
            let bytes = read_existing(store, dataset, path).await?;
            let segment = format::decode_retired_segment(&bytes, id)
                .map_err(|_| Abandon::Incomplete("a retired segment is unparseable"))?;
            self.known(dataset, |k| {
                k.segments
                    .insert(path.clone(), (Digest::of(&bytes), Arc::new(segment)))
            });
        }
        // 现存段的 `previous`（所引用的头片在可达快照之后还原）
        let mut segments: BTreeSet<String> = segment_paths.iter().cloned().collect();
        let listed_segments: Vec<Arc<RetiredSegment>> = self.known(dataset, |k| {
            segment_paths
                .iter()
                .filter_map(|p| k.segments.get(p).map(|(_, s)| Arc::clone(s)))
                .collect()
        });
        for s in &listed_segments {
            if let Some(previous) = &s.previous {
                self.check_segment_ref(dataset, previous, &mut segments)
                    .await?;
            }
        }

        // 可达快照：自当前快照沿父指针回溯。`heads` 是正看着的快照的 `buckets`：缓存中的快照按记下的差异推出父快照的；
        // 新读的快照与父快照比出差异，本遍读全后一并记入缓存。新读的快照中与父快照不同的头片（链底则为全部头片）待还原核对
        let top = self.known(dataset, |k| k.top.clone());
        let mut fresh: Vec<(SnapshotId, SnapshotFacts)> = Vec::new();
        let mut backs: Vec<(SnapshotId, ToParent)> = Vec::new();
        let mut to_check: Vec<(SnapshotId, u64, Arc<Head>)> = Vec::new();
        let mut reach = Reach::default();
        let mut reachable: Vec<SnapshotId> = Vec::new();
        let (mut facts, mut heads, mut is_fresh) = match self.cached(dataset, current) {
            Some(f) => match &top {
                Some((id, h)) if *id == current => {
                    if let Some(r) = &f.retired_segment {
                        self.check_segment_ref(dataset, r, &mut segments).await?;
                    }
                    (f, h.clone(), false)
                }
                _ => return Ok(None),
            },
            None => {
                let shared = top.as_ref().map(|(_, h)| h);
                let (f, h) = self
                    .read_snapshot(dataset, current, shared, None, &mut segments)
                    .await?;
                (f, h, true)
            }
        };
        let current_heads = heads.clone();
        reach
            .manifests
            .extend(heads.values().map(|h| Arc::clone(&h.path)));
        reach
            .current
            .extend(heads.values().map(|h| Arc::clone(&h.path)));
        // 当前快照经 `retired_segment` 沿 `previous` 可达的段（都已核对过引用）
        let mut next = facts.retired_segment.as_deref().cloned();
        while let Some(r) = next {
            let s = self
                .known(dataset, |k| {
                    k.segments.get(&r.path).map(|(_, s)| Arc::clone(s))
                })
                .ok_or(Abandon::Incomplete("a retired segment does not exist"))?;
            self.known(dataset, |k| {
                reach
                    .current
                    .extend(s.buckets.iter().map(|b| intern(&mut k.paths, &b.manifest)))
            });
            next = s.previous.clone();
        }
        let mut id = current;
        let (bottom, genesis) = loop {
            reachable.push(id);
            let parent = match facts.parent {
                None => break (id, true),
                Some(p) if listed.contains(&p) => p,
                Some(_) => break (id, false),
            };
            let had_back = facts.to_parent.is_some();
            let cached_parent = self.cached(dataset, parent);
            let (parent_facts, back, parent_fresh) = match (facts.to_parent.take(), cached_parent) {
                (Some(back), Some(pf)) => (pf, back, false),
                (Some(_), None) => return Ok(None),
                (None, Some(pf)) => match &top {
                    // 本快照是新读的、父快照是上一次巡检的当前快照（缓存中别的快照没有整份 `buckets`）
                    Some((t, h)) if *t == parent && is_fresh => (pf, diff(&heads, h), false),
                    _ => return Ok(None),
                },
                (None, None) => {
                    let (pf, h) = self
                        .read_snapshot(
                            dataset,
                            parent,
                            Some(&heads),
                            facts.retired_segment.as_ref(),
                            &mut segments,
                        )
                        .await?;
                    (pf, diff(&heads, &h), true)
                }
            };
            if !parent_fresh && let Some(r) = &parent_facts.retired_segment {
                self.check_segment_ref(dataset, r, &mut segments).await?;
            }
            if is_fresh {
                to_check.extend(back.iter().filter_map(|(b, _)| {
                    heads
                        .get(b)
                        .map(|h| (id, facts.format_version, Arc::clone(h)))
                }));
            }
            for (b, old) in &back {
                match old {
                    Some(h) => {
                        reach.manifests.insert(Arc::clone(&h.path));
                        heads.insert(Arc::clone(b), Arc::clone(h));
                    }
                    None => {
                        heads.remove(b);
                    }
                }
            }
            if is_fresh {
                facts.to_parent = Some(back);
                fresh.push((id, facts));
            } else if !had_back {
                backs.push((id, back));
            }
            (facts, is_fresh, id) = (parent_facts, parent_fresh, parent);
        };
        if is_fresh {
            to_check.extend(
                heads
                    .values()
                    .map(|h| (id, facts.format_version, Arc::clone(h))),
            );
            fresh.push((id, facts));
        }

        // 断档即放弃
        if !genesis && listed.range(..bottom).next().is_some() {
            let retain = match retain {
                Some(r) => Some(r),
                None => snapshot_gc::retain_start(
                    store,
                    &self.links,
                    dataset,
                    self.config.retention,
                    crate::lifecycle::now_us(),
                )
                .await
                .ok()
                .map(|r| r.start),
            };
            if retain.is_none_or(|r| bottom > r) {
                return Err(Abandon::Incomplete(
                    "a snapshot above the retain start is missing",
                ));
            }
        }

        // 可达桶 manifest：新读的快照（按编号从小到大）与可达留痕归档段所引用的头片，逐个还原并核对
        let mut seeds = Seeds {
            latest: HashMap::new(),
            earlier: self.known(dataset, |k| k.restored.clone()),
        };
        to_check.sort_by_key(|(id, _, _)| *id);
        for (_, version, head) in &to_check {
            self.check_manifest(
                dataset,
                &head.bucket,
                &head.piece_ref(),
                *version,
                &mut seeds,
            )
            .await?;
        }
        let segment_heads: Vec<(String, PieceRef)> = self.known(dataset, |k| {
            segments
                .iter()
                .filter_map(|path| k.segments.get(path))
                .flat_map(|(_, s)| {
                    s.buckets.iter().map(|b| {
                        (
                            b.bucket.clone(),
                            PieceRef {
                                path: b.manifest.clone(),
                                content_digest: b.content_digest,
                            },
                        )
                    })
                })
                .collect()
        });
        for (bucket, head) in &segment_heads {
            self.check_manifest(dataset, bucket, head, FORMAT_VERSION, &mut seeds)
                .await?;
            self.known(dataset, |k| {
                reach.manifests.insert(intern(&mut k.paths, &head.path))
            });
        }
        let current_paths: HashSet<Arc<str>> = current_heads
            .values()
            .map(|h| Arc::clone(&h.path))
            .collect();
        let complete = self.known(dataset, |k| {
            if !k.reach_pieces(&mut reach) {
                return false;
            }
            // 下一次从当前快照的各头片接起
            let mut restored = HashMap::new();
            for r in seeds
                .latest
                .into_values()
                .chain(seeds.earlier.into_values())
            {
                if current_paths.contains(r.head.path.as_str()) {
                    restored.entry(r.head.path.clone()).or_insert(r);
                }
            }
            k.restored = restored;
            k.snapshots.extend(fresh);
            for (id, back) in backs {
                if let Some(f) = k.snapshots.get_mut(&id) {
                    f.to_parent = Some(back);
                }
            }
            k.top = Some((current, current_heads));
            // 已不可达的不再缓存
            let keep: HashSet<SnapshotId> = reachable.iter().copied().collect();
            k.snapshots.retain(|id, _| keep.contains(id));
            k.segments.retain(|p, _| segments.contains(p));
            k.manifests.retain(|p, _| reach.manifests.contains(p));
            k.pieces.retain(|p, _| reach.pieces.contains(p));
            k.paths.retain(|p| Arc::strong_count(p) > 1);
            true
        });
        Ok(complete.then_some(reach))
    }

    /// 缓存中的快照 `id`。
    fn cached(&self, dataset: &Dataset, id: SnapshotId) -> Option<SnapshotFacts> {
        self.known(dataset, |k| k.snapshots.get(&id).cloned())
    }

    /// 读出、解析可达快照 `id`，核对其所引用的段（头片由调用方还原核对）。与 `shared`（相邻快照的 `buckets`）、
    /// `shared_segment` 相同的头片与段引用共用其一份。
    async fn read_snapshot(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
        shared: Option<&Heads>,
        shared_segment: Option<&Arc<SegmentRef>>,
        segments: &mut BTreeSet<String>,
    ) -> Result<(SnapshotFacts, Heads), Abandon> {
        let store = &self.flusher.committer().chain().store;
        let snapshot = match store
            .read(&store.object_path(dataset, &layout::snapshot_path(id)))
            .await
            .map_err(|_| Abandon::Silent)?
        {
            Some(f) => match format::decode_snapshot(&f.bytes, dataset, id) {
                Ok(SnapshotContent::Supported(s)) => *s,
                _ => return Err(Abandon::Incomplete("a snapshot is unparseable")),
            },
            None => return Err(Abandon::Incomplete("a reachable snapshot does not exist")),
        };
        if let Some(r) = &snapshot.retired_segment {
            self.check_segment_ref(dataset, r, segments).await?;
        }
        let heads: Heads = self.known(dataset, |k| {
            snapshot
                .buckets
                .iter()
                .map(|b| {
                    let same = shared
                        .and_then(|s| s.get(b.bucket.as_str()))
                        .filter(|h| *h.path == *b.manifest && h.digest == b.content_digest);
                    let head = match same {
                        Some(h) => Arc::clone(h),
                        None => Arc::new(Head {
                            bucket: intern(&mut k.paths, &b.bucket),
                            path: intern(&mut k.paths, &b.manifest),
                            digest: b.content_digest,
                        }),
                    };
                    (Arc::clone(&head.bucket), head)
                })
                .collect()
        });
        let retired_segment = snapshot.retired_segment.map(|r| match shared_segment {
            Some(s) if **s == r => Arc::clone(s),
            _ => Arc::new(r),
        });
        let facts = SnapshotFacts {
            parent: snapshot.parent_snapshot_id,
            format_version: snapshot.format_version,
            retired_segment,
            to_parent: None,
        };
        Ok((facts, heads))
    }
    /// 被快照或段引用的段须现存、已读到且摘要相符；它与沿 `previous` 可达的段记入 `segments`。
    async fn check_segment_ref(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
        segments: &mut BTreeSet<String>,
    ) -> Result<(), Abandon> {
        segments.insert(r.path.clone());
        let seen = self.known(dataset, |k| k.segments.get(&r.path).map(|(d, _)| *d));
        match seen {
            Some(d) if d == r.content_digest => Ok(()),
            // 不在本次列举中（已被删除，或列举之后才写出的）：读一次，读得到、摘要相符且能解析才接受
            None => {
                let store = &self.flusher.committer().chain().store;
                let id = layout::parse_retired_segment_path(&r.path)
                    .ok_or(Abandon::Incomplete("a retired segment path is malformed"))?;
                let bytes = read_existing(store, dataset, &r.path).await?;
                if Digest::of(&bytes) != r.content_digest {
                    return Err(Abandon::Incomplete("a retired segment digest mismatches"));
                }
                let segment = format::decode_retired_segment(&bytes, id)
                    .map_err(|_| Abandon::Incomplete("a retired segment is unparseable"))?;
                let previous = segment.previous.clone();
                self.known(dataset, |k| {
                    k.segments
                        .insert(r.path.clone(), (r.content_digest, Arc::new(segment)))
                });
                match previous {
                    Some(p) => Box::pin(self.check_segment_ref(dataset, &p, segments)).await,
                    None => Ok(()),
                }
            }
            Some(_) => Err(Abandon::Incomplete("a retired segment digest mismatches")),
        }
    }

    /// 还原并核对头片 `head` 所指的可达桶 manifest（已核对过的不再读），链上各片记入缓存：沿 `previous` 与
    /// `ledger_previous` 逐片读到底、逐片核对摘要，任一片不存在、无法解析或摘要不符即读不全。
    async fn check_manifest(
        &self,
        dataset: &Dataset,
        bucket: &str,
        head: &PieceRef,
        version: u64,
        seeds: &mut Seeds,
    ) -> Result<(), Abandon> {
        match self.known(dataset, |k| k.manifests.get(head.path.as_str()).copied()) {
            Some(d) if d == head.content_digest => return Ok(()),
            Some(_) => return Err(Abandon::Incomplete("a piece digest mismatches")),
            None => {}
        }
        let walk = self
            .walk(dataset, bucket, head, version, Some(seeds))
            .await?;
        let mut restored = chain::restore(&walk, version, bucket)
            .map_err(|_| Abandon::Incomplete("a piece is unparseable"))?;
        let missing = self.known(dataset, |k| {
            k.add_pieces(&walk);
            restored
                .pieces
                .iter()
                .any(|p| !k.pieces.contains_key(p.as_str()))
        });
        if missing {
            // 接起的已还原结果所含的片应当都已在缓存中；不在时不接起、重读一遍补上
            let walk = self.walk(dataset, bucket, head, version, None).await?;
            self.known(dataset, |k| k.add_pieces(&walk));
        }
        // 巡检只用路径：不留列统计
        for f in &mut Arc::make_mut(&mut restored.manifest).files {
            f.column_stats = None;
        }
        seeds.latest.insert(bucket.to_owned(), Arc::new(restored));
        self.known(dataset, |k| {
            let path = intern(&mut k.paths, &head.path);
            k.manifests.insert(path, head.content_digest);
        });
        Ok(())
    }

    /// 自头片 `head` 起读清单片（给了 `seeds` 时遇到可接起的已还原结果即止），任一片有问题即读不全。
    async fn walk(
        &self,
        dataset: &Dataset,
        bucket: &str,
        head: &PieceRef,
        version: u64,
        seeds: Option<&Seeds>,
    ) -> Result<PieceWalk, Abandon> {
        let store = &self.flusher.committer().chain().store;
        let walk = chain::walk_pieces(
            head,
            version,
            bucket,
            |rel| async move {
                store
                    .read(&store.object_path(dataset, &rel))
                    .await
                    .map(|f| f.map(|f| f.bytes))
            },
            |r| seeds.and_then(|s| s.find(r)),
        )
        .await
        .map_err(|_| Abandon::Silent)?;
        if let Some((problem, _)) = walk.problem() {
            return Err(Abandon::Incomplete(match problem {
                PieceProblem::Missing => "a referenced object does not exist",
                PieceProblem::DigestMismatch => "a piece digest mismatches",
                PieceProblem::Unparseable(_) => "a piece is unparseable",
            }));
        }
        Ok(walk)
    }
}

/// 一次巡检中还原头片时可以接起的已还原结果：本次每桶最近还原的一份，与上一次巡检当前快照的各头片。
struct Seeds {
    latest: HashMap<String, Arc<Restored>>,
    earlier: HashMap<String, Arc<Restored>>,
}

impl Seeds {
    fn find(&self, r: &PieceRef) -> Option<Arc<Restored>> {
        self.latest
            .values()
            .find(|s| s.head == *r)
            .or_else(|| self.earlier.get(&r.path))
            .cloned()
    }
}

/// 读一个应当存在的对象：不存在即读不全，对象存储不可访问另计。
async fn read_existing(
    store: &Store,
    dataset: &Dataset,
    rel: &str,
) -> Result<bytes::Bytes, Abandon> {
    match store.read(&store.object_path(dataset, rel)).await {
        Ok(Some(f)) => Ok(f.bytes),
        Ok(None) => Err(Abandon::Incomplete("a referenced object does not exist")),
        Err(_) => Err(Abandon::Silent),
    }
}

/// 按找链尾（REQ-AMB-STO-MANIFEST-COMMIT-V4）确定当前快照，不登记停用状态。尚无任何快照、提示不存在而当前快照编号
/// 大于 1、或提示不存在而列举所见有编号更大的快照（manifest 不可读情形 (b)），都放弃本次巡检。
async fn find_current(
    store: &Store,
    dataset: &Dataset,
    listed: &BTreeSet<SnapshotId>,
) -> Result<SnapshotId, Abandon> {
    let hint = snapshot_gc::read_hint(store, dataset).await?;
    let current = snapshot_gc::find_tail(store, dataset, hint).await?;
    match current {
        Some(c) if hint.is_some() => Ok(c),
        Some(c) if c.0 == 1 && listed.last().is_none_or(|m| *m <= c) => Ok(c),
        _ => Err(Abandon::Incomplete(
            "the current snapshot cannot be determined",
        )),
    }
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

fn system_now_us() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as i64)
}

#[cfg(test)]
mod tests;
