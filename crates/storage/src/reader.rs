//! 快照链读取（[`ChainReader`]、[`SegmentReader`]）的对象存储实现，与快照提交共用的读取与缓存
//! （REQ-AMB-STO-MANIFEST-COMMIT-V4「提示、找链尾与当前快照」，REQ-AMB-STO-HOT-LAYOUT-V5「对象状态的判定」
//! 「格式版本不兼容情形」「manifest 不可读情形」）。
//!
//! - **找链尾**（[`StoreChain::find_tail`]）：读提示得 h（不存在为 0），从 h+1 起按编号逐个探测快照，直到某编号不存在；
//!   提示存在时不列举 `manifest/`，提示不存在时另做一次**核对列举**。查询、提交与启动装载都按它确定当前快照。
//! - 找链尾时判出的 manifest 不可读情形：提示内容无法解析 (a)；提示不存在而当前快照编号大于 1，或核对列举所见有编号
//!   大于找链尾所得的快照 (b)；本进程已知的最大编号 T 之后找链尾得到尚无快照或编号小于 T (c)。h+1 不存在、而提示所指
//!   快照 h 不存在 (a) 在随后读取快照 h 时判出。(b) 的核对列举一支使「已有过快照」（核对列举所见有快照文件）的数据集
//!   不会再被当作新数据集写出 1 号快照；本进程读到或创建成功过快照的，由 (c) 与 [`StoreChain::known_max`] 保证。
//! - 本进程首次读到或创建成功某快照时记下其完整字节的摘要；再读到同一编号时摘要不符即判内容无法解析（再读核对）；
//!   写方以它为父提交前另行读取核对（[`StoreChain::read_parent`]）。
//!
//! 读到使数据集进入停用状态的结果时，先把状态记入 [`DatasetGates`] 再返回；数据集已停用时不发出任何对象存储请求。
//! 快照与清单片写出后不再改变，按路径缓存。桶 manifest 按 REQ-AMB-STO-HOT-LAYOUT-V8「还原」由头片沿 `previous` 与
//! `ledger_previous` 逐片读出、逐片核对摘要后合成（[`crate::chain::walk_pieces`]、[`crate::chain::restore`]），共用读取
//! 一律还原全集（含留痕链）。每个数据集只缓存已知最新的快照及其 `buckets` 引用的头片的还原结果：每个还原结果含该桶
//! 全部存活文件，若旧头片的也一直留在缓存里，内存会随提交次数平方增长。新头片的 `previous` 在缓存中时只多读这一片；
//! 旧快照与旧头片不在缓存中，用到时从对象存储重读、重新还原并核对。
//!
//! 段链的读取（[`SegmentReader`]）不改变数据集状态，结果按段路径缓存。
//!
//! 全进程的在用快照登记（[`InUseSnapshots`]）随本结构创建：查询经 [`ChainReader::reading`]、后台任务经
//! [`StoreChain::pin_latest`] 登记所用快照。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{
    self, BucketManifest, BucketRef, ChainError, ChainReader, ColdChangeSegment, Digest, PieceRef,
    Restored, RetiredSegment, SegmentReader, SegmentRef, Snapshot, SnapshotId,
};
use crate::format::{self, SnapshotContent};
use crate::gate::{DatasetGates, DatasetState};
use crate::in_use::{InUseSnapshots, SnapshotPin};
use crate::layout;
use crate::registry::{DatasetRegistry, Persistence};
use crate::store::{Store, Unavailable};

/// 每个数据集记下摘要的快照个数上限（再读核对只比本进程记得的；父快照总是最近的那几个）。
const KEPT_DIGESTS: usize = 16;

/// 一个数据集的快照链在本进程里的已知情况。
#[derive(Debug, Default, Clone)]
struct Known {
    /// 本进程已读到或已创建成功的最大编号 T（manifest 不可读情形 (c) 据此判定）。
    max: Option<SnapshotId>,
    /// 本进程首次读到或创建成功时所记的完整字节摘要（只留最近 [`KEPT_DIGESTS`] 个）。
    digests: BTreeMap<SnapshotId, Digest>,
}

/// 找链尾的结果。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Tail {
    /// 提示所记的编号；提示不存在为 `None`。
    pub hint: Option<SnapshotId>,
    /// 当前快照；尚无任何快照为 `None`。
    pub current: Option<SnapshotId>,
}

/// 快照链的读取与缓存，查询（经 [`ChainReader`]）与快照提交共用。
pub struct StoreChain {
    pub(crate) store: Store,
    pub(crate) registry: Arc<DatasetRegistry>,
    pub(crate) gates: Arc<DatasetGates>,
    /// 每个数据集已知最新的快照。
    snapshots: Mutex<HashMap<Dataset, Arc<Snapshot>>>,
    /// 最新快照引用的头片的还原结果（按头片路径）。
    manifests: Mutex<HashMap<(Dataset, String), Arc<Restored>>>,
    known: Mutex<HashMap<Dataset, Known>>,
    /// 已读到并核对过的段（按数据集与路径）。
    retired: Mutex<HashMap<(Dataset, String), Arc<RetiredSegment>>>,
    cold_changes: Mutex<HashMap<(Dataset, String), Arc<ColdChangeSegment>>>,
    /// 全进程的在用快照登记。
    in_use: Arc<InUseSnapshots>,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().expect("快照链缓存锁未中毒")
}

impl StoreChain {
    pub fn new(store: Store, registry: Arc<DatasetRegistry>, gates: Arc<DatasetGates>) -> Self {
        StoreChain {
            store,
            registry,
            gates,
            snapshots: Mutex::default(),
            manifests: Mutex::default(),
            known: Mutex::default(),
            retired: Mutex::default(),
            cold_changes: Mutex::default(),
            in_use: Arc::new(InUseSnapshots::new()),
        }
    }

    /// 全进程的在用快照登记（快照回收读它）。
    pub fn in_use(&self) -> &Arc<InUseSnapshots> {
        &self.in_use
    }

    /// 数据集 `dataset` 已知最新的快照，并登记为在用（后台任务取缓存而不找链尾时用；取缓存与登记之间没有 `.await`）。
    /// 返回的登记持有到读完该快照所引用的对象。
    pub fn pin_latest(&self, dataset: &Dataset) -> Option<(Arc<Snapshot>, SnapshotPin)> {
        let snapshot = self.latest_snapshot(dataset)?;
        let pin = self.in_use.pin(dataset, snapshot.snapshot_id);
        Some((snapshot, pin))
    }

    /// 缓存中数据集 `dataset` 的（快照数，桶 manifest 数），供测试核对缓存不随提交次数增长。
    #[cfg(feature = "testing")]
    pub fn cached(&self, dataset: &Dataset) -> (usize, usize) {
        (
            usize::from(lock(&self.snapshots).contains_key(dataset)),
            lock(&self.manifests)
                .keys()
                .filter(|(d, _)| d == dataset)
                .count(),
        )
    }

    /// 各数据集已知最新的快照（本进程读到或写出的），按标识排序。
    pub fn latest_snapshots(&self) -> Vec<(Dataset, Arc<Snapshot>)> {
        let mut all: Vec<(Dataset, Arc<Snapshot>)> = lock(&self.snapshots)
            .iter()
            .map(|(d, s)| (d.clone(), Arc::clone(s)))
            .collect();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        all
    }

    /// 数据集 `dataset` 已知最新的快照。
    pub fn latest_snapshot(&self, dataset: &Dataset) -> Option<Arc<Snapshot>> {
        lock(&self.snapshots).get(dataset).cloned()
    }

    /// 本进程已读到或已创建成功的该数据集最大快照编号。
    pub(crate) fn known_max(&self, dataset: &Dataset) -> Option<SnapshotId> {
        lock(&self.known).get(dataset).and_then(|k| k.max)
    }

    /// 记下本进程读到或创建成功的快照 `id` 及其摘要：首次记下的为准；已记的不符时返回 `false`（再读核对不过）。
    pub(crate) fn note_snapshot(&self, dataset: &Dataset, id: SnapshotId, digest: Digest) -> bool {
        let mut known = lock(&self.known);
        let k = known.entry(dataset.clone()).or_default();
        k.max = k.max.max(Some(id));
        match k.digests.get(&id) {
            Some(d) => *d == digest,
            None => {
                k.digests.insert(id, digest);
                while k.digests.len() > KEPT_DIGESTS {
                    k.digests.pop_first();
                }
                true
            }
        }
    }

    /// 使数据集进入停用状态并返回对应的错误。
    pub(crate) fn disable(&self, dataset: &Dataset, state: DatasetState) -> ChainError {
        self.gates.enter(dataset, state);
        ChainError::Disabled(self.gates.state(dataset))
    }

    fn check_gate(&self, dataset: &Dataset) -> Result<(), ChainError> {
        match self.gates.state(dataset) {
            DatasetState::Normal => Ok(()),
            state => Err(ChainError::Disabled(state)),
        }
    }

    /// 读取提示：不存在为 `None`；内容无法解析使数据集进入 manifest 不可读情形 (a)。
    pub(crate) async fn read_hint(
        &self,
        dataset: &Dataset,
    ) -> Result<Option<SnapshotId>, ChainError> {
        let path = self.store.object_path(dataset, layout::CURRENT);
        let Some(fetched) = self.store.read(&path).await.map_err(unavailable)? else {
            return Ok(None);
        };
        match format::decode_current(&fetched.bytes) {
            Ok(id) => Ok(Some(id)),
            Err(_) => Err(self.disable(dataset, DatasetState::ManifestUnreadable)),
        }
    }

    /// 快照 `id` 是否存在（探测）。
    pub(crate) async fn snapshot_exists(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
    ) -> Result<bool, Unavailable> {
        self.store
            .exists(&self.store.object_path(dataset, &layout::snapshot_path(id)))
            .await
    }

    /// 找链尾（REQ-AMB-STO-MANIFEST-COMMIT-V4），并按 REQ-AMB-STO-HOT-LAYOUT-V5 判 manifest 不可读情形 (a)～(c)
    /// 中能在这一步判出的。提示不存在时另做一次核对列举。
    pub(crate) async fn find_tail(&self, dataset: &Dataset) -> Result<Tail, ChainError> {
        self.check_gate(dataset)?;
        let hint = self.read_hint(dataset).await?;
        let mut current = hint;
        loop {
            let next = SnapshotId(current.map_or(1, |c| c.0 + 1));
            if !self
                .snapshot_exists(dataset, next)
                .await
                .map_err(unavailable)?
            {
                break;
            }
            current = Some(next);
        }
        if hint.is_none() && current.is_some_and(|c| c.0 > 1) {
            // (b)
            return Err(self.disable(dataset, DatasetState::ManifestUnreadable));
        }
        if hint.is_none()
            && self
                .listed_max(dataset)
                .await
                .map_err(unavailable)?
                .is_some_and(|m| current.is_none_or(|c| m > c))
        {
            // (b) 的核对列举一支（REQ-AMB-STO-MANIFEST-COMMIT-V4、REQ-AMB-STO-HOT-LAYOUT-V5）：提示不存在时列举一次
            // `manifest/`，所见有编号大于找链尾所得的快照（如 1 号快照已被快照回收）
            return Err(self.disable(dataset, DatasetState::ManifestUnreadable));
        }
        if let Some(t) = self.known_max(dataset)
            && current.is_none_or(|c| c < t)
        {
            // (c)
            return Err(self.disable(dataset, DatasetState::ManifestUnreadable));
        }
        if let Some(c) = current {
            let mut known = lock(&self.known);
            let k = known.entry(dataset.clone()).or_default();
            k.max = k.max.max(Some(c));
        }
        Ok(Tail { hint, current })
    }

    /// 核对列举：列举一次 `manifest/`（不含子前缀），返回所见快照文件的最大编号；没有快照文件为 `None`。
    async fn listed_max(&self, dataset: &Dataset) -> Result<Option<SnapshotId>, Unavailable> {
        let dir = self
            .store
            .dataset_prefix(dataset)
            .join(layout::MANIFEST_DIR);
        let (objects, _) = self.store.list_level(&dir).await?;
        Ok(objects
            .iter()
            .filter_map(|o| o.location.filename())
            .filter_map(|name| {
                layout::parse_snapshot_path(&format!("{}/{name}", layout::MANIFEST_DIR))
            })
            .max())
    }

    /// 读取快照 `id` 的完整字节与内容（不经缓存、不改闸门）；不存在为 `None`。
    pub(crate) async fn read_snapshot_raw(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
    ) -> Result<Option<(bytes::Bytes, Result<SnapshotContent, format::Unparseable>)>, Unavailable>
    {
        let path = self.store.object_path(dataset, &layout::snapshot_path(id));
        Ok(self.store.read(&path).await?.map(|f| {
            let content = format::decode_snapshot(&f.bytes, dataset, id);
            (f.bytes, content)
        }))
    }

    /// 读取作为当前快照的快照 `id`：不存在、内容无法解析（含再读核对不过）进入 manifest 不可读情形，
    /// 格式版本不兼容进入格式版本不兼容情形。本进程已缓存的最新快照即是它时不再读取。
    pub(crate) async fn load_snapshot(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
    ) -> Result<Arc<Snapshot>, ChainError> {
        self.check_gate(dataset)?;
        if let Some(s) = lock(&self.snapshots)
            .get(dataset)
            .filter(|s| s.snapshot_id == id)
        {
            return Ok(Arc::clone(s));
        }
        self.read_snapshot_checked(dataset, id)
            .await
            .map(|(s, _)| s)
    }

    /// 读取快照 `id` 并做再读核对，返回它与其完整字节的摘要。
    async fn read_snapshot_checked(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
    ) -> Result<(Arc<Snapshot>, Digest), ChainError> {
        match self
            .read_snapshot_raw(dataset, id)
            .await
            .map_err(unavailable)?
        {
            None | Some((_, Err(_))) => {
                Err(self.disable(dataset, DatasetState::ManifestUnreadable))
            }
            Some((bytes, Ok(content))) => {
                let digest = Digest::of(&bytes);
                if !self.note_snapshot(dataset, id, digest) {
                    return Err(self.disable(dataset, DatasetState::ManifestUnreadable));
                }
                match content {
                    SnapshotContent::Unsupported { .. } => {
                        Err(self.disable(dataset, DatasetState::IncompatibleFormat))
                    }
                    SnapshotContent::Supported(s) => {
                        Ok((self.remember_snapshot(dataset, *s), digest))
                    }
                }
            }
        }
    }

    /// 写方的父快照核对（REQ-AMB-STO-HOT-LAYOUT-V4）：创建新快照之前读取父快照 `id`，其完整字节的摘要须等于本进程
    /// 所记；不符即判内容无法解析、数据集进入 manifest 不可读情形。返回父快照与其摘要（新快照的 `parent_digest`）。
    pub(crate) async fn read_parent(
        &self,
        dataset: &Dataset,
        id: SnapshotId,
    ) -> Result<(Arc<Snapshot>, Digest), ChainError> {
        self.check_gate(dataset)?;
        self.read_snapshot_checked(dataset, id).await
    }

    /// 记入快照 `s`：比已缓存的更新时取代之，并从缓存中去掉该数据集不再被引用的桶 manifest；更旧的不记入。
    pub(crate) fn remember_snapshot(&self, dataset: &Dataset, s: Snapshot) -> Arc<Snapshot> {
        let s = Arc::new(s);
        let mut snapshots = lock(&self.snapshots);
        if snapshots
            .get(dataset)
            .is_some_and(|c| c.snapshot_id > s.snapshot_id)
        {
            return s;
        }
        snapshots.insert(dataset.clone(), Arc::clone(&s));
        let live: HashSet<&str> = s.buckets.iter().map(|b| b.manifest.as_str()).collect();
        lock(&self.manifests).retain(|(d, path), _| d != dataset || live.contains(path.as_str()));
        s
    }

    /// `snapshot` 不比已缓存的最新快照旧：只有这时它引用的桶 manifest 才记入缓存。
    fn is_latest(&self, dataset: &Dataset, snapshot: &Snapshot) -> bool {
        lock(&self.snapshots)
            .get(dataset)
            .is_none_or(|c| c.snapshot_id <= snapshot.snapshot_id)
    }

    /// 读取并核对 `snapshot` 的 `buckets` 元素 `bucket` 所引用的桶 manifest：组成它的任一清单片不存在、内容无法解析
    /// （含摘要不符、格式版本不等于快照、链超过 64 片）进入 manifest 不可读情形，返回 [`ChainError::Integrity`]。
    pub(crate) async fn load_bucket_manifest(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Result<Arc<BucketManifest>, ChainError> {
        Ok(Arc::clone(
            &self
                .load_restored(dataset, snapshot, bucket)
                .await?
                .manifest,
        ))
    }

    /// 同 [`StoreChain::load_bucket_manifest`]，另带头片的链况（提交器选片用）。
    pub(crate) async fn load_restored(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Result<Arc<Restored>, ChainError> {
        self.check_gate(dataset)?;
        match self.read_restored(dataset, snapshot, bucket).await? {
            Ok(m) => Ok(m),
            Err(problem) => {
                self.gates.enter(dataset, DatasetState::ManifestUnreadable);
                Err(ChainError::Integrity(problem))
            }
        }
    }

    /// 同 [`StoreChain::load_bucket_manifest`]，但读不到、无法解析或对象存储不可访问都只返回 `None`，不登记停用状态
    /// （合并决定是否合并之前的检查读取用；停用只由查询、提交与装载触发）。
    pub(crate) async fn peek_bucket_manifest(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Option<Arc<BucketManifest>> {
        match self.read_bucket_manifest(dataset, snapshot, bucket).await {
            Ok(Ok(m)) => Some(m),
            Ok(Err(_)) | Err(_) => None,
        }
    }

    /// 经缓存读取、还原并核对桶 manifest，不改闸门；内层 `Err` 为无法使用的原因。
    pub(crate) async fn read_bucket_manifest(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Result<Result<Arc<BucketManifest>, String>, ChainError> {
        Ok(self
            .read_restored(dataset, snapshot, bucket)
            .await?
            .map(|r| Arc::clone(&r.manifest)))
    }

    /// 经缓存读取、还原并核对桶 manifest 及其头片的链况，不改闸门；内层 `Err` 为无法使用的原因。
    pub(crate) async fn read_restored(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        bucket: &BucketRef,
    ) -> Result<Result<Arc<Restored>, String>, ChainError> {
        let head = PieceRef {
            path: bucket.manifest.clone(),
            content_digest: bucket.content_digest,
        };
        let cached = |r: &PieceRef| {
            lock(&self.manifests)
                .get(&(dataset.clone(), r.path.clone()))
                .cloned()
        };
        if let Some(m) = cached(&head).filter(|m| m.head == head) {
            return Ok(Ok(m));
        }
        let store = &self.store;
        let walk = chain::walk_pieces(
            &head,
            snapshot.format_version,
            &bucket.bucket,
            |rel| async move {
                store
                    .read(&store.object_path(dataset, &rel))
                    .await
                    .map(|f| f.map(|f| f.bytes))
            },
            cached,
        )
        .await
        .map_err(unavailable)?;
        Ok(
            match chain::restore_checked(&walk, snapshot.format_version, &bucket.bucket) {
                Ok(r) => {
                    let r = Arc::new(r);
                    if self.is_latest(dataset, snapshot) {
                        lock(&self.manifests).insert((dataset.clone(), head.path), Arc::clone(&r));
                    }
                    Ok(r)
                }
                Err(e) => Err(format!("bucket manifest {}: {e}", bucket.manifest)),
            },
        )
    }

    /// 本进程刚写出、已知完整字节摘要的头片的还原结果，记入缓存（视同已核对）。
    pub(crate) fn remember_bucket_manifest(&self, dataset: &Dataset, path: String, m: Restored) {
        lock(&self.manifests).insert((dataset.clone(), path), Arc::new(m));
    }

    /// 本进程刚写出的留痕归档段，记入缓存（视同已核对）。
    pub(crate) fn remember_retired_segment(
        &self,
        dataset: &Dataset,
        path: String,
        s: RetiredSegment,
    ) {
        lock(&self.retired).insert((dataset.clone(), path), Arc::new(s));
    }

    /// 本进程刚写出的冷层变更段，记入缓存（视同已核对）。
    pub(crate) fn remember_cold_change_segment(
        &self,
        dataset: &Dataset,
        path: String,
        s: ColdChangeSegment,
    ) {
        lock(&self.cold_changes).insert((dataset.clone(), path), Arc::new(s));
    }

    /// 读取并核对段引用 `r` 所指的留痕归档段（不改闸门）。
    pub(crate) async fn read_retired_segment(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
    ) -> Result<Arc<RetiredSegment>, ChainError> {
        let key = (dataset.clone(), r.path.clone());
        if let Some(s) = lock(&self.retired).get(&key) {
            return Ok(Arc::clone(s));
        }
        self.fetch_retired_segment(dataset, r).await
    }

    /// 同 [`Self::read_retired_segment`]，但不先查缓存：每次都读对象存储（读到的照常记入缓存）。
    async fn fetch_retired_segment(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
    ) -> Result<Arc<RetiredSegment>, ChainError> {
        let key = (dataset.clone(), r.path.clone());
        let id = layout::parse_retired_segment_path(&r.path)
            .ok_or_else(|| ChainError::Integrity(format!("segment path {:?}", r.path)))?;
        let bytes = self.read_segment_bytes(dataset, r).await?;
        let s = format::decode_retired_segment(&bytes, id)
            .map_err(|e| ChainError::Integrity(format!("{}: {e}", r.path)))?;
        let s = Arc::new(s);
        lock(&self.retired).insert(key, Arc::clone(&s));
        Ok(s)
    }

    /// 读取并核对段引用 `r` 所指的冷层变更段（不改闸门）。
    pub(crate) async fn read_cold_change_segment(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
    ) -> Result<Arc<ColdChangeSegment>, ChainError> {
        let key = (dataset.clone(), r.path.clone());
        if let Some(s) = lock(&self.cold_changes).get(&key) {
            return Ok(Arc::clone(s));
        }
        self.fetch_cold_change_segment(dataset, r).await
    }

    /// 同 [`Self::read_cold_change_segment`]，但不先查缓存：每次都读对象存储（读到的照常记入缓存）。
    async fn fetch_cold_change_segment(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
    ) -> Result<Arc<ColdChangeSegment>, ChainError> {
        let key = (dataset.clone(), r.path.clone());
        let id = layout::parse_cold_change_segment_path(&r.path)
            .ok_or_else(|| ChainError::Integrity(format!("segment path {:?}", r.path)))?;
        let bytes = self.read_segment_bytes(dataset, r).await?;
        let s = format::decode_cold_change_segment(&bytes, id)
            .map_err(|e| ChainError::Integrity(format!("{}: {e}", r.path)))?;
        let s = Arc::new(s);
        lock(&self.cold_changes).insert(key, Arc::clone(&s));
        Ok(s)
    }

    async fn read_segment_bytes(
        &self,
        dataset: &Dataset,
        r: &SegmentRef,
    ) -> Result<bytes::Bytes, ChainError> {
        let path = self.store.object_path(dataset, &r.path);
        match self.store.read(&path).await.map_err(unavailable)? {
            None => Err(ChainError::Integrity(format!(
                "segment {} does not exist",
                r.path
            ))),
            Some(f) if Digest::of(&f.bytes) != r.content_digest => {
                Err(ChainError::Integrity(format!(
                    "segment {} does not match content_digest {}",
                    r.path, r.content_digest
                )))
            }
            Some(f) => Ok(f.bytes),
        }
    }
}

fn unavailable(e: Unavailable) -> ChainError {
    ChainError::Unavailable(e.0)
}

impl ChainReader for StoreChain {
    fn current<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<SnapshotId>, ChainError>> {
        Box::pin(async move {
            self.check_gate(dataset)?;
            // 存在尚未持久化或只在本地 WAL 中：Q 按 0 计，不读对象存储
            if self.registry.persistence(dataset) != Some(Persistence::Stored) {
                return Ok(None);
            }
            Ok(self.find_tail(dataset).await?.current)
        })
    }

    fn snapshot<'a>(
        &'a self,
        dataset: &'a Dataset,
        id: SnapshotId,
    ) -> BoxFuture<'a, Result<Arc<Snapshot>, ChainError>> {
        Box::pin(self.load_snapshot(dataset, id))
    }

    fn bucket_manifest<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
        bucket: &'a BucketRef,
    ) -> BoxFuture<'a, Result<Arc<BucketManifest>, ChainError>> {
        Box::pin(self.load_bucket_manifest(dataset, snapshot, bucket))
    }

    fn reading(&self, dataset: &Dataset) -> SnapshotPin {
        self.in_use.begin(dataset, self.known_max(dataset))
    }
}

impl StoreChain {
    async fn retired_chain(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        fresh: bool,
    ) -> Result<Vec<Arc<RetiredSegment>>, ChainError> {
        let mut out: Vec<Arc<RetiredSegment>> = Vec::new();
        let mut next = snapshot.retired_segment.clone();
        while let Some(r) = next {
            let s = if fresh {
                self.fetch_retired_segment(dataset, &r).await?
            } else {
                self.read_retired_segment(dataset, &r).await?
            };
            // 沿 `previous` 段号须严格递减（否则链成环，按内容无法解析）
            if out
                .last()
                .is_some_and(|p: &Arc<_>| p.segment_id <= s.segment_id)
            {
                return Err(ChainError::Integrity(format!(
                    "segment chain does not descend at {}",
                    r.path
                )));
            }
            next = s.previous.clone();
            out.push(s);
        }
        Ok(out)
    }

    async fn cold_change_chain(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        fresh: bool,
    ) -> Result<Vec<Arc<ColdChangeSegment>>, ChainError> {
        let mut out: Vec<Arc<ColdChangeSegment>> = Vec::new();
        let mut next = snapshot.cold_deletion_segment.clone();
        while let Some(r) = next {
            let s = if fresh {
                self.fetch_cold_change_segment(dataset, &r).await?
            } else {
                self.read_cold_change_segment(dataset, &r).await?
            };
            // 沿 `previous` 段号须严格递减（否则链成环，按内容无法解析）
            if out
                .last()
                .is_some_and(|p: &Arc<_>| p.segment_id <= s.segment_id)
            {
                return Err(ChainError::Integrity(format!(
                    "segment chain does not descend at {}",
                    r.path
                )));
            }
            next = s.previous.clone();
            out.push(s);
        }
        Ok(out)
    }
}

impl SegmentReader for StoreChain {
    fn retired_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<RetiredSegment>>, ChainError>> {
        Box::pin(self.retired_chain(dataset, snapshot, false))
    }

    fn cold_change_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<ColdChangeSegment>>, ChainError>> {
        Box::pin(self.cold_change_chain(dataset, snapshot, false))
    }
}

/// 不经缓存的段链读取：每段都读对象存储并核对（不改闸门）。冷层合并为确定合并候选文件与写入时摘要所读的段要看得见
/// 外部删改（REQ-AMB-STO-COLD-COMPACTION-HOT-READ），不用 [`StoreChain`] 按路径缓存的那一份。
pub struct FreshSegments(pub Arc<StoreChain>);

impl SegmentReader for FreshSegments {
    fn retired_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<RetiredSegment>>, ChainError>> {
        Box::pin(self.0.retired_chain(dataset, snapshot, true))
    }

    fn cold_change_segments<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
    ) -> BoxFuture<'a, Result<Vec<Arc<ColdChangeSegment>>, ChainError>> {
        Box::pin(self.0.cold_change_chain(dataset, snapshot, true))
    }
}
