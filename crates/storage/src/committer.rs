//! 快照提交（Commit）的对象存储实现：装载时按找链尾读入基线、创世快照、一次提交的写入顺序与冲突重试、
//! WAL 重放中提交意图的判明（REQ-AMB-STO-MANIFEST-COMMIT-V3、REQ-AMB-STO-HOT-LAYOUT-V4、REQ-AMB-DM-NAMESPACE；
//! ADR-0038，ADR-0021 第 2、4 条经 ADR-0043 第 6 条改写）。
//!
//! 一次提交的步骤依次为：上传数据文件 → 找链尾得父快照 N → 提示落后于 N 时先补写（补写未成功即本轮按对象存储不可访问
//! 结束）→ 读回父快照核对其摘要 → 为受影响的桶各上传一份清单片（按 REQ-AMB-STO-HOT-LAYOUT-V8「写哪一种清单片」选片，
//! 见 [`StoreCommitter::rewrite_buckets`]）与本次的段 → 在 WAL 中 fsync 提交意图 (stream, P, b, d)
//! → 把所含各代绑定到 P → 以「仅当不存在时创建」写快照 P（写成即提交完成）→ 随即尝试把提示写为 P（失败不使提交失败）。
//! - 创建得到「已存在」：读回快照 P 比对完整字节的摘要，等于 d 即本提交先前一次尝试已写成；不等即**真冲突**，
//!   下一次尝试重新找链尾、以新编号重写本次提交（清单片与段一并换号重写，增量片的 `previous` 随新父快照重定）。
//! - 创建或读回遇对象存储不可访问即结束本轮，返回 [`CommitError::Unavailable`]：所绑定的编号与快照字节留在调用方的
//!   [`CommitProgress`] 里，下一次尝试以逐字节相同的内容再发创建请求。这样一轮失败的耗时不超过一次请求的超时，
//!   按周期的失败计数（REQ-AMB-OBS-HEALTH-METRICS）从失败所在的周期起算。
//! - 真冲突的重试间隔按指数退避（起始 50 毫秒、每次翻倍、单次上限 2 秒），次数上限取 `storage.commit_max_retries`；
//!   耗尽（此时各次尝试都未遇到对象存储不可访问）即数据集进入 manifest 不可读情形 (d)。
//! - 段（留痕归档段、冷层变更段）以「仅当不存在时创建」写出，段号自父快照所引用同类段的号加 1 起；遇「已存在」一律换
//!   下一个段号重写，不读回比对。
//!
//! WAL 里的提交意图（供重放一侧解释）：每定下一个新的快照号 P 就在创建快照 P 之前追加一条 (stream, P, b, d)，
//! 同号重发时不再追加；真冲突换号后以新的 P 再追加一条；判明结果不另写条目。不带新记录的提交（创世、合并、巡检留痕、
//! 入湖交接、TTL 热层、冷层变更段与归档）的 b 为 `WalPosition(0)`，不覆盖任何记录（ADR-0021）。提交成功后发 [`Release::Records`]，1 号快照创建成功后发 [`Release::Existence`]。
//!
//! 同一数据集的各种提交在进程内经同一把每数据集的锁串行（REQ-AMB-STO-MANIFEST-COMMIT-V3）：后一次在前一次得出结果
//! 之后才找链尾，进程内的提交之间不因「仅当不存在时创建」相互冲突。flush 的触发、可见集代的冻结与回执、
//! 提交失败的计数由调用方负责。
//!
//! 在途对象：一次提交（含其各次尝试）上传的数据文件、桶 manifest 与段自上传之前起登记在 [`InFlight`] 中，直到持有它们的
//! [`CommitProgress`] 被丢弃（提交成功、或调用方放弃这批内容）；巡检不删除、不留痕在途对象。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::{DEFAULT_ORG, Dataset, is_valid_stream_name};
use bytes::Bytes;

use crate::BoxFuture;
use crate::chain::{
    BucketManifest, BucketRef, CHAIN_LIMIT, ChainError, ColdChangeSegment, ColdDeletion,
    ColdFileEntry, ColdSummary, Deletion, DeletionReason, Digest, FileEntry, Handover, Piece,
    PieceLink, PieceRef, Restored, RetiredBucket, RetiredSegment, SegmentId, SegmentReader,
    SegmentRef, Snapshot, SnapshotId, Summary,
};
use crate::commit::{
    ArchiveRequest, Baseline, ColdChangeRequest, CommitIntent, Committed, Existence,
    HandoverRequest, HotCommits, IntentOutcome, TtlHotRequest,
};
use crate::fault;
use crate::flush::DataFile;
use crate::format::{self, FORMAT_VERSION, SEGMENT_FORMAT_VERSION};
use crate::gate::DatasetState;
use crate::layout::{self, HourBucket};
use crate::objects::Created;
use crate::reader::{StoreChain, Tail};
use crate::registry::Persistence;
use crate::schema::{SchemaColumn, StreamColumns};
use crate::store::Unavailable;
use crate::wal::{Release, Wal, WalEntry, WalError, WalPosition};

/// 一次提交的内容。
pub struct CommitRequest {
    /// 本次新增的数据文件（尚未上传或已上传均可；按路径去重上传）。
    pub files: Vec<DataFile>,
    /// b：本次所含最后一代在该 stream 记录子序列中的末位 WAL 位置（记入提交意图）。
    pub through: WalPosition,
    /// b 所指条目只有前 k 条在本次及更早的提交里时为 `Some(k)`（记入提交意图；分段提交切在该条目中间）。
    pub through_partial: Option<u32>,
    /// 把本次所含各代绑定到快照号 P：在创建快照 P 之前调用，以新号重绑时再次调用（ADR-0021 第 2 条经 ADR-0043 改写）。
    pub bind: Option<Box<dyn Fn(SnapshotId) + Send + Sync>>,
}

/// 提交没有完成。这些情形下都没有创建新快照；待提交数据由调用方留在 WAL 与可见集中。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitError {
    /// 数据集处于停用状态（此前已读到，或本次读到而进入），不写出新的快照、桶 manifest、段或提示。
    Disabled(DatasetState),
    /// 真冲突重试用尽（计入 `amberlog_manifest_commits_total{result="failed"}`；数据集随之进入 manifest 不可读情形）。
    Exhausted,
    /// 本轮某次尝试遇到对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT-V4），本轮就此结束。
    Unavailable,
    /// 提交意图写不进 WAL：没有创建快照。
    Wal(WalError),
    /// 请求所依据的内容已不在父快照中（合并输入已离开存活集、要留痕的文件已有条目、要移走的文件都已不存活、
    /// 要归档的桶仍有存活文件等），或数据集尚无快照，本次放弃。
    Stale,
}

/// 一次合并提交的内容（REQ-AMB-STO-SEAL-COMPACTION）。
pub struct CompactionRequest {
    pub bucket: HourBucket,
    /// 合并输入：最新快照中该桶 `files` 的条目。
    pub inputs: Vec<FileEntry>,
    /// 合并输出（尚未上传或已上传均可）。
    pub outputs: Vec<DataFile>,
    /// 该桶处于 `sealed` 的截止时刻（入湖水位时刻，UTC 微秒，与 [`crate::lifecycle::now_us`] 同一时钟）。快照的
    /// `created_at` 不早于它时，该桶在提交时已不处于 `sealed`，放弃这次合并（[`CommitError::Stale`]；
    /// REQ-AMB-STO-SEAL-COMPACTION-V2「提交一次合并时，若该桶已不处于 `sealed` 状态……放弃」）。
    pub sealed_until: i64,
}

/// 一次留痕提交的内容（REQ-AMB-STO-HOT-FSCK）：为孤儿数据文件在其路径所示小时桶记 `orphan` 留痕。
pub struct TombstoneRequest {
    /// (路径所示小时桶, 相对数据集前缀的路径, 删除前对完整字节计算的摘要)
    pub orphans: Vec<(HourBucket, String, Digest)>,
}

/// 本进程正在进行的提交已上传、尚未被快照引用的对象（相对数据集前缀的路径），按数据集。
#[derive(Default)]
pub struct InFlight {
    paths: Mutex<HashMap<(Dataset, String), usize>>,
}

impl InFlight {
    fn add(&self, dataset: &Dataset, path: &str) {
        *self
            .paths
            .lock()
            .expect("在途表锁未中毒")
            .entry((dataset.clone(), path.to_owned()))
            .or_default() += 1;
    }

    fn remove(&self, dataset: &Dataset, path: &str) {
        let mut paths = self.paths.lock().expect("在途表锁未中毒");
        let key = (dataset.clone(), path.to_owned());
        if let Some(n) = paths.get_mut(&key) {
            *n -= 1;
            if *n == 0 {
                paths.remove(&key);
            }
        }
    }

    /// 数据集 `dataset` 此刻的在途对象。
    pub fn snapshot(&self, dataset: &Dataset) -> HashSet<String> {
        self.paths
            .lock()
            .expect("在途表锁未中毒")
            .keys()
            .filter(|(d, _)| d == dataset)
            .map(|(_, p)| p.clone())
            .collect()
    }
}

/// 某快照下的已记入冷层文件集（REQ-AMB-STO-HOT-LAYOUT-V4 `summary.cold`）：路径（相对冷层表位置）→ 记录其新增的
/// 交接记录或合并记录元素。
type ColdSet = BTreeMap<String, ColdFileEntry>;

/// 快照提交。
pub struct StoreCommitter {
    chain: Arc<StoreChain>,
    wal: Arc<dyn Wal>,
    max_retries: u32,
    inflight: Arc<InFlight>,
    locks: Mutex<HashMap<Dataset, Arc<tokio::sync::Mutex<()>>>>,
    /// 每个数据集最近算出的已记入冷层文件集及其所属快照号（冷层变更段提交取摘要与 `summary.cold` 用）。
    cold: Mutex<HashMap<Dataset, (SnapshotId, Arc<ColdSet>)>>,
}

/// 一次尝试没有成功的原因。
enum Fail {
    /// 本次尝试放弃，按退避重试；`unavailable` 为本次尝试的对象存储请求出现了不可访问。
    Retry {
        unavailable: bool,
    },
    Fatal(CommitError),
}

impl From<ChainError> for Fail {
    fn from(e: ChainError) -> Fail {
        match e {
            ChainError::Unavailable(_) => Fail::Retry { unavailable: true },
            ChainError::Disabled(state) => Fail::Fatal(CommitError::Disabled(state)),
            // 读快照或桶 manifest 得到的完整性错误已使数据集进入 manifest 不可读情形
            ChainError::Integrity(_) => {
                Fail::Fatal(CommitError::Disabled(DatasetState::ManifestUnreadable))
            }
        }
    }
}

impl From<Unavailable> for Fail {
    fn from(_: Unavailable) -> Fail {
        Fail::Retry { unavailable: true }
    }
}

/// 一次提交的工作。
#[derive(Clone, Copy)]
enum Job<'a> {
    /// 创世快照（`buckets` 为空、`schema` 仅 `_timestamp`）。
    Genesis,
    /// 写入提交。
    Data(&'a CommitRequest),
    /// 合并提交。
    Compact(&'a CompactionRequest),
    /// 巡检留痕提交。
    Tombstone(&'a TombstoneRequest),
    /// 入湖交接提交。
    Handover(&'a HandoverRequest),
    /// TTL 热层提交。
    TtlHot(&'a TtlHotRequest),
    /// 冷层变更段提交。
    ColdChange(&'a ColdChangeRequest),
    /// 归档提交。
    Archive(&'a ArchiveRequest),
}

/// 一次提交在各次尝试之间保留的进度。调用方为同一批内容多次调用 [`StoreCommitter::commit_with`] 时须传同一个进度：
/// 快照 P 的创建结果不明时，同一绑定编号下只能以逐字节相同的快照内容再发（REQ-AMB-STO-MANIFEST-COMMIT-V3），
/// 否则其实已写成的那批数据会被判为真冲突、换号再进链一次。
#[derive(Default)]
pub struct CommitProgress {
    uploaded: HashSet<String>,
    prepared: Option<Prepared>,
    /// 本批已有提交意图记入 WAL（此后重提必须以同一批内容）。
    intent_written: bool,
    /// 本批登记为在途的对象，丢弃时注销。
    registered: Vec<(Arc<InFlight>, Dataset, String)>,
}

impl CommitProgress {
    /// 本批已有提交意图记入 WAL：此后它可能已被写成快照，只能以同一批内容重提或判明，不能换成别的内容。
    pub fn intent_written(&self) -> bool {
        self.intent_written
    }

    fn register(&mut self, inflight: &Arc<InFlight>, dataset: &Dataset, path: &str) {
        inflight.add(dataset, path);
        self.registered
            .push((Arc::clone(inflight), dataset.clone(), path.to_owned()));
    }
}

impl Drop for CommitProgress {
    fn drop(&mut self) {
        for (inflight, dataset, path) in &self.registered {
            inflight.remove(dataset, path);
        }
    }
}

/// 已定下编号与字节的快照 P（其桶 manifest 与段已上传、意图已记入 WAL）。
struct Prepared {
    snapshot: Snapshot,
    bytes: Bytes,
    digest: Digest,
    manifests: Vec<(String, Restored)>,
    retired: Option<(String, RetiredSegment)>,
    cold_change: Option<(String, ColdChangeSegment)>,
    /// 本次提交之后的已记入冷层文件集（本次改变了它、且提交前已知时）。
    cold_after: Option<Arc<ColdSet>>,
}

fn next(n: Option<SnapshotId>) -> SnapshotId {
    SnapshotId(n.map_or(1, |n| n.0 + 1))
}

fn backoff(attempt: u32) -> Duration {
    let ms = 50u64.saturating_mul(1 << (attempt - 1).min(16));
    Duration::from_millis(ms.min(2_000))
}

/// 段引用 `r` 的段号；`None` 时为 0（下一个段号即 1）。
fn segment_number(r: Option<&SegmentRef>, parse: fn(&str) -> Option<SegmentId>) -> u64 {
    r.and_then(|r| parse(&r.path)).map_or(0, |id| id.0)
}

impl StoreCommitter {
    pub fn new(chain: Arc<StoreChain>, wal: Arc<dyn Wal>, max_retries: u32) -> Self {
        StoreCommitter {
            chain,
            wal,
            max_retries,
            inflight: Arc::default(),
            locks: Mutex::default(),
            cold: Mutex::default(),
        }
    }

    /// 提交所用的 WAL（flush 读回溢出记录）。
    pub fn wal(&self) -> &Arc<dyn Wal> {
        &self.wal
    }

    pub fn chain(&self) -> &Arc<StoreChain> {
        &self.chain
    }

    /// 本进程在途提交已上传、尚未被快照引用的对象。
    pub fn inflight(&self) -> &Arc<InFlight> {
        &self.inflight
    }

    /// 本进程是否已知该数据集至少有 1 号快照（读到或创建成功过）。
    pub fn has_snapshot(&self, dataset: &Dataset) -> bool {
        self.chain.known_max(dataset).is_some()
    }

    async fn lock(&self, dataset: &Dataset) -> tokio::sync::OwnedMutexGuard<()> {
        let m = Arc::clone(
            self.locks
                .lock()
                .expect("提交锁表未中毒")
                .entry(dataset.clone())
                .or_default(),
        );
        m.lock_owned().await
    }

    // -----------------------------------------------------------------------
    // 装载
    // -----------------------------------------------------------------------

    /// 开始受理 `/api/` 请求之前，找出全部在对象存储中存在的数据集（`manifest/` 下有提示或符合快照路径模板的对象），
    /// 登记其存在并按找链尾读入当前快照作基线。读到的停用状态记入闸门。列举或读取遇对象存储不可访问时返回 `Err`。
    pub async fn load(&self) -> Result<Vec<Baseline>, Unavailable> {
        let store = &self.chain.store;
        let (_, streams) = store
            .list_level(&store.root.clone().join(DEFAULT_ORG))
            .await?;
        let mut baselines = Vec::new();
        for prefix in streams {
            let Some(name) = prefix.filename() else {
                continue;
            };
            if !is_valid_stream_name(name) {
                continue;
            }
            let dataset = Dataset::new(DEFAULT_ORG, name);
            let manifest_dir = store.dataset_prefix(&dataset).join(layout::MANIFEST_DIR);
            let (objects, _) = store.list_level(&manifest_dir).await?;
            let exists = objects.iter().any(|o| {
                o.location.filename().is_some_and(|file| {
                    let rel = format!("{}/{file}", layout::MANIFEST_DIR);
                    rel == layout::CURRENT || layout::parse_snapshot_path(&rel).is_some()
                })
            });
            if !exists {
                continue;
            }
            self.chain.registry.record(&dataset, Persistence::Stored);
            baselines.push(self.load_dataset(&dataset).await?);
        }
        Ok(baselines)
    }

    async fn load_dataset(&self, dataset: &Dataset) -> Result<Baseline, Unavailable> {
        let disabled = |snapshot_id| Baseline {
            dataset: dataset.clone(),
            snapshot_id,
            columns: None,
        };
        let tail = match self.chain.find_tail(dataset).await {
            Ok(tail) => tail,
            Err(ChainError::Unavailable(m)) => return Err(Unavailable(m)),
            Err(_) => return Ok(disabled(None)),
        };
        let Some(n) = tail.current else {
            // 提示存在而所指快照及其后都不存在时已判 (a)、提示不存在而有快照文件时已判 (b)，到这里只剩尚无任何快照
            return Ok(Baseline {
                dataset: dataset.clone(),
                snapshot_id: None,
                columns: Some(StreamColumns::genesis()),
            });
        };
        match self.chain.load_snapshot(dataset, n).await {
            Ok(s) => Ok(baseline(dataset, &s)),
            Err(ChainError::Unavailable(m)) => Err(Unavailable(m)),
            Err(_) => Ok(disabled(Some(n))),
        }
    }

    // -----------------------------------------------------------------------
    // 创世快照与提交
    // -----------------------------------------------------------------------

    /// 确保数据集存在（REQ-AMB-DM-NAMESPACE）：放进登记表（尚未持久化时记为未持久化），本进程已知其 1 号快照则直接返回
    /// [`Existence::Committed`]；否则尝试一次创世快照提交（意图先记入 WAL），成功为 `Committed`，失败为
    /// [`Existence::Pending`]（创世快照按普通提交失败继续重试，由调用方安排）。返回 `Committed` 时向 WAL 发
    /// [`Release::Existence`]。
    pub async fn ensure_dataset(&self, dataset: &Dataset) -> Existence {
        let persistence = self
            .chain
            .registry
            .record(dataset, Persistence::Unpersisted);
        if persistence == Persistence::Stored && self.has_snapshot(dataset) {
            self.release_existence(dataset);
            return Existence::Committed;
        }
        let _guard = self.lock(dataset).await;
        let mut progress = CommitProgress::default();
        match self.attempt(dataset, Job::Genesis, &mut progress).await {
            Ok(_) => {
                self.release_existence(dataset);
                Existence::Committed
            }
            Err(_) => Existence::Pending,
        }
    }

    /// 1 号快照已存在：该数据集的存在性条目都已体现，可随段回收（段回收判据③）。
    fn release_existence(&self, dataset: &Dataset) {
        self.wal.release(Release::Existence {
            dataset: dataset.clone(),
        });
    }

    /// 提交一批数据文件，返回写成的快照号。本进程尚不知该数据集有任何快照时先提交其创世快照。
    /// 成功后向 WAL 发段回收通知 [`Release::Records`]（`through` 取 `request.through`）；
    /// 可见集的回执由调用方在此之后发出。
    pub async fn commit(
        &self,
        dataset: &Dataset,
        request: &CommitRequest,
    ) -> Result<SnapshotId, CommitError> {
        self.commit_with(dataset, request, &mut CommitProgress::default())
            .await
    }

    /// 同 [`StoreCommitter::commit`]，在同一批内容的多次调用之间以 `progress` 保留进度。
    pub async fn commit_with(
        &self,
        dataset: &Dataset,
        request: &CommitRequest,
        progress: &mut CommitProgress,
    ) -> Result<SnapshotId, CommitError> {
        let _guard = self.lock(dataset).await;
        if !self.has_snapshot(dataset) {
            self.run(dataset, Job::Genesis).await?;
            self.release_existence(dataset);
        }
        let done = self.run_with(dataset, Job::Data(request), progress).await?;
        // 位置不超过 b 的该数据集记录都已进入已提交快照（段回收判据①）；b 所指条目只提交了一部分时只到它之前
        let through = match request.through_partial {
            Some(_) => WalPosition(request.through.0.saturating_sub(1)),
            None => request.through,
        };
        if through.0 > 0 {
            self.wal.release(Release::Records {
                dataset: dataset.clone(),
                through,
            });
        }
        Ok(done.snapshot_id)
    }

    /// 补完数据集待办的创世快照（没有待提交记录时也要做；按提交失败的规则重试）。
    /// 没有这类待办、或数据集已停用时为 `None`；有则返回这一次提交的结果。
    pub async fn settle(&self, dataset: &Dataset) -> Option<Result<SnapshotId, CommitError>> {
        if self.chain.gates.state(dataset) != DatasetState::Normal || self.has_snapshot(dataset) {
            return None;
        }
        self.chain.registry.persistence(dataset)?;
        let _guard = self.lock(dataset).await;
        let result = self.run(dataset, Job::Genesis).await.map(|c| c.snapshot_id);
        if result.is_ok() {
            self.release_existence(dataset);
        }
        Some(result)
    }

    /// 提交一次合并：在该桶新桶 manifest 中以合并输出取代合并输入，并为各输入记 `compaction` 留痕。
    /// 数据集尚无快照、或合并输入已不全在父快照该桶的 `files` 中时为 [`CommitError::Stale`]。
    pub async fn commit_compaction(
        &self,
        dataset: &Dataset,
        request: &CompactionRequest,
    ) -> Result<SnapshotId, CommitError> {
        self.run_existing(dataset, Job::Compact(request))
            .await
            .map(|c| c.snapshot_id)
    }

    /// 提交一次留痕：在各孤儿数据文件路径所示小时桶的新桶 manifest 中记 `orphan` 留痕（该桶不在快照中时新增一个
    /// 无存活文件的桶）。全部文件都已在所涉桶的 `files` 或 `deletions` 中时为 [`CommitError::Stale`]。
    pub async fn commit_tombstones(
        &self,
        dataset: &Dataset,
        request: &TombstoneRequest,
    ) -> Result<SnapshotId, CommitError> {
        self.run_existing(dataset, Job::Tombstone(request))
            .await
            .map(|c| c.snapshot_id)
    }

    /// 不带新记录的提交：数据集须已有快照（否则 [`CommitError::Stale`]）。
    async fn run_existing(
        &self,
        dataset: &Dataset,
        job: Job<'_>,
    ) -> Result<Committed, CommitError> {
        let _guard = self.lock(dataset).await;
        if !self.has_snapshot(dataset) {
            return Err(CommitError::Stale);
        }
        self.run(dataset, job).await
    }

    async fn run(&self, dataset: &Dataset, job: Job<'_>) -> Result<Committed, CommitError> {
        self.run_with(dataset, job, &mut CommitProgress::default())
            .await
    }

    async fn run_with(
        &self,
        dataset: &Dataset,
        job: Job<'_>,
        progress: &mut CommitProgress,
    ) -> Result<Committed, CommitError> {
        for attempt in 0..=self.max_retries {
            if attempt > 0 {
                tokio::time::sleep(backoff(attempt)).await;
            }
            match self.attempt(dataset, job, progress).await {
                Ok(done) => return Ok(done),
                Err(Fail::Fatal(e)) => return Err(e),
                Err(Fail::Retry { unavailable: true }) => return Err(CommitError::Unavailable),
                Err(Fail::Retry { unavailable: false }) => {}
            }
        }
        // 各次尝试都没遇到不可访问（遇到即已返回）
        self.chain
            .gates
            .enter(dataset, DatasetState::ManifestUnreadable);
        Err(CommitError::Exhausted)
    }

    async fn attempt(
        &self,
        dataset: &Dataset,
        job: Job<'_>,
        progress: &mut CommitProgress,
    ) -> Result<Committed, Fail> {
        let state = self.chain.gates.state(dataset);
        if state != DatasetState::Normal {
            return Err(Fail::Fatal(CommitError::Disabled(state)));
        }
        let store = &self.chain.store;
        let uploads = match job {
            Job::Data(request) => Some(&request.files),
            Job::Compact(request) => Some(&request.outputs),
            _ => None,
        };
        if let Some(files) = uploads {
            for f in files {
                if !progress.uploaded.contains(&f.path) {
                    progress.register(&self.inflight, dataset, &f.path);
                    // 已存在只可能是本次提交此前一次结果不明的上传（文件名是新生成的 ULID）
                    store
                        .create(&store.object_path(dataset, &f.path), f.bytes.clone())
                        .await?;
                    progress.uploaded.insert(f.path.clone());
                }
            }
            if let Job::Data(_) = job {
                fault::pause("commit.after_data_files");
            }
        }

        if progress.prepared.is_none() {
            let tail = self.chain.find_tail(dataset).await?;
            if let Job::Genesis = job
                && let Some(n) = tail.current
            {
                // 1 号快照已存在（此前的进程或尝试已写出）
                self.chain.registry.record(dataset, Persistence::Stored);
                return Ok(Committed {
                    snapshot_id: n,
                    created_at: self
                        .chain
                        .latest_snapshot(dataset)
                        .map_or(0, |s| s.created_at),
                });
            }
            let prepared = self.prepare(dataset, job, tail, progress).await?;
            progress.prepared = Some(prepared);
            progress.intent_written = true;
        }
        let p = progress.prepared.as_ref().expect("已定下");
        let id = p.snapshot.snapshot_id;

        fault::pause("commit.before_snapshot_create");
        if let Job::Data(CommitRequest {
            bind: Some(bind), ..
        }) = job
        {
            bind(id);
        }
        let path = store.object_path(dataset, &layout::snapshot_path(id));
        let written = match store.create(&path, p.bytes.clone()).await? {
            Created::Done => true,
            // 「已存在」：读回比对；读回不可访问时保持绑定的编号，`?` 结束本轮
            Created::AlreadyExists => store
                .read(&path)
                .await?
                .is_some_and(|f| Digest::of(&f.bytes) == p.digest),
        };
        if !written {
            // 真冲突：下一次尝试重新找链尾、以新编号重写本次提交
            progress.prepared = None;
            return Err(Fail::Retry { unavailable: false });
        }
        fault::pause("commit.after_snapshot_create");
        // 随即尝试把提示写为该快照的编号；失败不使提交失败，下一次提交补写
        let _ = store
            .put(
                &store.object_path(dataset, layout::CURRENT),
                Bytes::from(format::encode_current(id)),
            )
            .await;
        let p = progress.prepared.take().expect("刚写出");
        Ok(self.succeed(dataset, p))
    }

    /// 以链尾 n 为父定下快照 P = n + 1：读回父快照核对摘要，确保提示不小于 n，上传受影响桶的新桶 manifest 与本次的段，
    /// 算出快照字节与摘要，把意图记入 WAL。
    async fn prepare(
        &self,
        dataset: &Dataset,
        job: Job<'_>,
        tail: Tail,
        progress: &mut CommitProgress,
    ) -> Result<Prepared, Fail> {
        let n = tail.current;
        let id = next(n);
        // 与桶状态、归档与延后删除取同一时钟（`fault-injection` 下含测试时钟偏移），快照的 `created_at` 与它们的判定一致
        let created_at = crate::lifecycle::now_us();
        let parent = match (job, n) {
            (Job::Genesis, _) => None,
            // 写入提交之前创世快照已提交；此刻找不到快照而本进程未读到过它：留给下一次尝试
            (Job::Data(_), None) => return Err(Fail::Retry { unavailable: false }),
            (_, None) => return Err(Fail::Fatal(CommitError::Stale)),
            (_, Some(n)) => Some(self.chain.read_parent(dataset, n).await?),
        };
        if let Some(n) = n
            && tail.hint < Some(n)
        {
            // 创建快照 N+1 之前先确保提示不小于 N（父快照核对通过之后才写，提示不指向读不出的快照）；
            // 补写未成功即按对象存储不可访问结束本轮
            let store = &self.chain.store;
            store
                .put(
                    &store.object_path(dataset, layout::CURRENT),
                    Bytes::from(format::encode_current(n)),
                )
                .await?;
        }
        let Some((parent, parent_digest)) = parent else {
            let snapshot = Snapshot {
                format_version: FORMAT_VERSION,
                snapshot_id: id,
                parent_snapshot_id: None,
                parent_digest: None,
                created_at,
                schema: StreamColumns::genesis().columns().to_vec(),
                buckets: Vec::new(),
                summary: Summary::of(dataset, &[], ColdSummary::EMPTY),
                retired_segment: None,
                cold_deletion_segment: None,
            };
            return self
                .finish_prepare(dataset, job, snapshot, Vec::new(), None, None, None)
                .await;
        };

        let mut edits: BTreeMap<HourBucket, BucketEdit> = BTreeMap::new();
        let mut schema = parent.schema.clone();
        let mut cold = parent.summary.cold;
        let mut cold_after = None;
        let mut retired = None;
        let mut cold_change = None;
        let mut retired_segment = parent.retired_segment.clone();
        let mut cold_deletion_segment = parent.cold_deletion_segment.clone();
        let mut archived: HashSet<String> = HashSet::new();
        match job {
            Job::Genesis => unreachable!("创世快照没有父快照"),
            Job::Data(request) => {
                for f in &request.files {
                    edits.entry(f.bucket).or_default().add.push(f.entry(id));
                }
                let mut new_columns: Vec<SchemaColumn> = Vec::new();
                for c in request.files.iter().flat_map(|f| &f.columns) {
                    if !schema.iter().chain(&new_columns).any(|s| s.name == c.name) {
                        new_columns.push(c.clone());
                    }
                }
                new_columns.sort_by_key(|c| c.field_id);
                schema.extend(new_columns);
            }
            Job::Compact(request) => {
                if created_at >= request.sealed_until {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
                let current = self.bucket_of(dataset, &parent, request.bucket).await?;
                // 合并输入须仍全部在存活集中（条目原样）
                if !request
                    .inputs
                    .iter()
                    .all(|i| current.files.iter().any(|f| f == i))
                {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
                let edit = edits.entry(request.bucket).or_default();
                edit.add = request.outputs.iter().map(|f| f.entry(id)).collect();
                edit.remove = request
                    .inputs
                    .iter()
                    .map(|i| removal(i, DeletionReason::Compaction, None, created_at, id))
                    .collect();
            }
            Job::Tombstone(request) => {
                for (hour, path, digest) in &request.orphans {
                    edits.entry(*hour).or_default().tombstones.push(Deletion {
                        path: path.clone(),
                        content_digest: *digest,
                        reason: DeletionReason::Orphan,
                        added_in: None,
                        max_timestamp: None,
                        cutoff: None,
                        deleted_at: created_at,
                        removed_in: id,
                    });
                }
            }
            Job::Handover(request) => {
                let current = self.bucket_of(dataset, &parent, request.bucket).await?;
                let inputs: Vec<&FileEntry> = current
                    .files
                    .iter()
                    .filter(|f| f.added_in <= request.hot_snapshot_id)
                    .collect();
                if inputs.is_empty() {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
                let edit = edits.entry(request.bucket).or_default();
                edit.remove = inputs
                    .into_iter()
                    .map(|f| removal(f, DeletionReason::Lake, None, created_at, id))
                    .collect();
                edit.handovers
                    .push(request.handover.clone().into_handover(id));
                cold = extend_cold(cold, &request.handover.files);
                // 已知父快照的已记入冷层文件集时随之更新
                if let Some(set) = self.cached_cold_set(dataset, parent.snapshot_id) {
                    let mut set = (*set).clone();
                    for f in &request.handover.files {
                        set.insert(f.path.clone(), f.clone());
                    }
                    cold_after = Some(Arc::new(set));
                }
            }
            Job::TtlHot(request) => {
                let mut by_bucket: BTreeMap<HourBucket, Vec<&str>> = BTreeMap::new();
                for (hour, path) in &request.expired {
                    by_bucket.entry(*hour).or_default().push(path);
                }
                for (hour, paths) in by_bucket {
                    let label = hour.to_string();
                    if !parent.buckets.iter().any(|b| b.bucket == label) {
                        continue;
                    }
                    let current = self.bucket_of(dataset, &parent, hour).await?;
                    let removed: Vec<Deletion> = current
                        .files
                        .iter()
                        .filter(|f| paths.contains(&f.path.as_str()))
                        .map(|f| {
                            removal(f, DeletionReason::Ttl, Some(request.cutoff), created_at, id)
                        })
                        .collect();
                    if !removed.is_empty() {
                        edits.entry(hour).or_default().remove = removed;
                    }
                }
                if edits.is_empty() {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
            }
            Job::ColdChange(request) => {
                let set = self.cold_set(dataset, &parent).await?;
                let mut deletions = Vec::with_capacity(request.paths.len());
                for path in &request.paths {
                    let Some(f) = set.get(path) else {
                        return Err(Fail::Fatal(CommitError::Stale));
                    };
                    deletions.push(ColdDeletion {
                        path: path.clone(),
                        content_digest: f.content_digest,
                        max_timestamp: f.max_timestamp,
                        reason: request.reason,
                        cutoff: request.cutoff,
                        deleted_at: created_at,
                        removed_in: id,
                        cold_removed_in: request.cold_removed_in,
                    });
                }
                if deletions.is_empty() {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
                let mut after = (*set).clone();
                for d in &deletions {
                    after.remove(&d.path);
                }
                for a in &request.additions {
                    after.insert(a.path.clone(), a.clone());
                }
                cold = ColdSummary::of(after.values());
                cold_after = Some(Arc::new(after));
                let first = segment_number(
                    parent.cold_deletion_segment.as_ref(),
                    layout::parse_cold_change_segment_path,
                ) + 1;
                let previous = parent.cold_deletion_segment.clone();
                let (path, reference, segment) = self
                    .write_segment(
                        dataset,
                        first,
                        layout::cold_change_segment_path,
                        progress,
                        |segment_id| {
                            let s = ColdChangeSegment {
                                format_version: SEGMENT_FORMAT_VERSION,
                                segment_id,
                                previous: previous.clone(),
                                created_at,
                                deletions: deletions.clone(),
                                additions: request.additions.clone(),
                            };
                            (format::encode_cold_change_segment(&s), s)
                        },
                    )
                    .await?;
                cold_deletion_segment = Some(reference);
                cold_change = Some((path, segment));
            }
            Job::Archive(request) => {
                let mut entries = Vec::new();
                for hour in &request.buckets {
                    let label = hour.to_string();
                    let Some(r) = parent.buckets.iter().find(|b| b.bucket == label) else {
                        return Err(Fail::Fatal(CommitError::Stale));
                    };
                    if r.record_count > 0 {
                        return Err(Fail::Fatal(CommitError::Stale));
                    }
                    archived.insert(label);
                    entries.push(RetiredBucket {
                        bucket: r.bucket.clone(),
                        manifest: r.manifest.clone(),
                        content_digest: r.content_digest,
                    });
                }
                if entries.is_empty() {
                    return Err(Fail::Fatal(CommitError::Stale));
                }
                let first = segment_number(
                    parent.retired_segment.as_ref(),
                    layout::parse_retired_segment_path,
                ) + 1;
                let previous = parent.retired_segment.clone();
                let (path, reference, segment) = self
                    .write_segment(
                        dataset,
                        first,
                        layout::retired_segment_path,
                        progress,
                        |segment_id| {
                            let s = RetiredSegment {
                                format_version: SEGMENT_FORMAT_VERSION,
                                segment_id,
                                previous: previous.clone(),
                                created_at,
                                buckets: entries.clone(),
                            };
                            (format::encode_retired_segment(&s), s)
                        },
                    )
                    .await?;
                retired_segment = Some(reference);
                retired = Some((path, segment));
            }
        }

        let (mut buckets, manifests) = self
            .rewrite_buckets(dataset, &parent, edits, progress)
            .await?;
        if matches!(job, Job::Tombstone(_)) && manifests.is_empty() {
            return Err(Fail::Fatal(CommitError::Stale));
        }
        buckets.retain(|b| !archived.contains(&b.bucket));
        if let Job::Data(_) = job {
            fault::pause("commit.after_bucket_manifests");
            fault::gate("commit.after_bucket_manifests").await;
        }
        if let Job::Compact(_) = job {
            // 已以 n 为父定下新桶 manifest、尚未写快照：测试在此挡住合并提交，验证同数据集的 flush 提交排在其后
            fault::gate("commit.compaction_before_snapshot").await;
        }
        let snapshot = Snapshot {
            format_version: FORMAT_VERSION,
            snapshot_id: id,
            parent_snapshot_id: Some(parent.snapshot_id),
            parent_digest: Some(parent_digest),
            created_at,
            schema,
            summary: Summary::of(dataset, &buckets, cold),
            buckets,
            retired_segment,
            cold_deletion_segment,
        };
        self.finish_prepare(
            dataset,
            job,
            snapshot,
            manifests,
            retired,
            cold_change,
            cold_after,
        )
        .await
    }

    /// 算出快照字节与摘要，把意图记入 WAL。
    #[allow(clippy::too_many_arguments)]
    async fn finish_prepare(
        &self,
        dataset: &Dataset,
        job: Job<'_>,
        snapshot: Snapshot,
        manifests: Vec<(String, Restored)>,
        retired: Option<(String, RetiredSegment)>,
        cold_change: Option<(String, ColdChangeSegment)>,
        cold_after: Option<Arc<ColdSet>>,
    ) -> Result<Prepared, Fail> {
        let bytes = Bytes::from(format::encode_snapshot(&snapshot));
        let digest = Digest::of(&bytes);
        let (through, through_partial) = match job {
            Job::Data(request) => (request.through, request.through_partial),
            // 不带新记录的提交：意图不覆盖任何记录
            _ => (WalPosition(0), None),
        };
        // 上传数据文件的提交记下所上传的路径（快照 P 被回收后据此按 `added_in` 判明，REQ-AMB-STO-MANIFEST-COMMIT-V4）
        let files = match job {
            Job::Data(request) => request.files.iter().map(|f| f.path.clone()).collect(),
            Job::Compact(request) => request.outputs.iter().map(|f| f.path.clone()).collect(),
            _ => Vec::new(),
        };
        self.wal
            .append(vec![WalEntry::CommitIntent(CommitIntent {
                dataset: dataset.clone(),
                snapshot_id: snapshot.snapshot_id,
                through,
                through_partial,
                digest,
                files,
            })])
            .await
            .map_err(|e| Fail::Fatal(CommitError::Wal(e)))?;
        fault::pause("commit.after_intent");
        Ok(Prepared {
            snapshot,
            bytes,
            digest,
            manifests,
            retired,
            cold_change,
            cold_after,
        })
    }

    /// 父快照中小时桶 `hour` 的桶 manifest；该桶不在父快照中为 [`CommitError::Stale`]。
    async fn bucket_of(
        &self,
        dataset: &Dataset,
        parent: &Snapshot,
        hour: HourBucket,
    ) -> Result<Arc<BucketManifest>, Fail> {
        let label = hour.to_string();
        let Some(r) = parent.buckets.iter().find(|b| b.bucket == label) else {
            return Err(Fail::Fatal(CommitError::Stale));
        };
        Ok(self.chain.load_bucket_manifest(dataset, parent, r).await?)
    }

    /// 以「仅当不存在时创建」写出一个段：自段号 `first` 起，遇「已存在」一律换下一个段号重写（不读回比对）。
    /// 返回段路径、引用与写出的段。
    async fn write_segment<S>(
        &self,
        dataset: &Dataset,
        first: u64,
        path_of: fn(SegmentId) -> String,
        progress: &mut CommitProgress,
        build: impl Fn(SegmentId) -> (Vec<u8>, S),
    ) -> Result<(String, SegmentRef, S), Fail> {
        let store = &self.chain.store;
        let mut number = first;
        loop {
            let segment_id = SegmentId(number);
            let (bytes, segment) = build(segment_id);
            let path = path_of(segment_id);
            progress.register(&self.inflight, dataset, &path);
            let digest = Digest::of(&bytes);
            match store
                .create(&store.object_path(dataset, &path), Bytes::from(bytes))
                .await?
            {
                Created::Done => {
                    fault::pause("commit.after_segment_write");
                    let reference = SegmentRef {
                        path: path.clone(),
                        content_digest: digest,
                    };
                    return Ok((path, reference, segment));
                }
                Created::AlreadyExists => number += 1,
            }
        }
    }

    /// 本进程算出的、属于快照 `at` 的已记入冷层文件集。
    fn cached_cold_set(&self, dataset: &Dataset, at: SnapshotId) -> Option<Arc<ColdSet>> {
        self.cold
            .lock()
            .expect("冷层文件集锁未中毒")
            .get(dataset)
            .filter(|(id, _)| *id == at)
            .map(|(_, set)| Arc::clone(set))
    }

    /// 快照 `parent` 的已记入冷层文件集：自该快照的桶 manifest、经 `retired_segment` 可达的留痕归档段所引用的桶 manifest
    /// 中的交接记录，与经 `cold_deletion_segment` 可达的冷层变更段累加得出（REQ-AMB-STO-HOT-LAYOUT-V4）。
    async fn cold_set(&self, dataset: &Dataset, parent: &Snapshot) -> Result<Arc<ColdSet>, Fail> {
        if let Some(set) = self.cached_cold_set(dataset, parent.snapshot_id) {
            return Ok(set);
        }
        let mut set = ColdSet::new();
        let mut add = |m: &BucketManifest| {
            for h in &m.handovers {
                for f in &h.files {
                    set.insert(f.path.clone(), f.clone());
                }
            }
        };
        for r in &parent.buckets {
            add(&*self.chain.load_bucket_manifest(dataset, parent, r).await?);
        }
        let retired = self
            .chain
            .retired_segments(dataset, parent)
            .await
            .map_err(|e| self.segment_fail(dataset, e))?;
        for segment in &retired {
            for b in &segment.buckets {
                let r = BucketRef {
                    bucket: b.bucket.clone(),
                    manifest: b.manifest.clone(),
                    content_digest: b.content_digest,
                    record_count: 0,
                    size_bytes: 0,
                    time_min: None,
                    time_max: None,
                };
                match self.chain.read_bucket_manifest(dataset, parent, &r).await? {
                    Ok(m) => add(&m),
                    Err(_) => {
                        return Err(Fail::Fatal(CommitError::Disabled(
                            self.chain_disable(dataset, DatasetState::ManifestUnreadable),
                        )));
                    }
                }
            }
        }
        let changes = self
            .chain
            .cold_change_segments(dataset, parent)
            .await
            .map_err(|e| self.segment_fail(dataset, e))?;
        // 自链首起依次累加
        for segment in changes.iter().rev() {
            for d in &segment.deletions {
                set.remove(&d.path);
            }
            for a in &segment.additions {
                set.insert(a.path.clone(), a.clone());
            }
        }
        let set = Arc::new(set);
        self.cold
            .lock()
            .expect("冷层文件集锁未中毒")
            .insert(dataset.clone(), (parent.snapshot_id, Arc::clone(&set)));
        Ok(set)
    }

    /// 读段链失败：不可访问按本轮结束处置；段丢失或被改动属外部删改，该数据集停用。
    fn segment_fail(&self, dataset: &Dataset, e: ChainError) -> Fail {
        match e {
            ChainError::Unavailable(_) => Fail::Retry { unavailable: true },
            _ => Fail::Fatal(CommitError::Disabled(
                self.chain_disable(dataset, DatasetState::ManifestUnreadable),
            )),
        }
    }

    fn chain_disable(&self, dataset: &Dataset, state: DatasetState) -> DatasetState {
        self.chain.gates.enter(dataset, state);
        self.chain.gates.state(dataset)
    }

    /// 按 `edits` 为受影响的桶各写一份清单片并上传（读出其现有桶 manifest 即逐片核对摘要），未受影响的桶原样引用。
    /// 返回新的 `buckets` 与写出的头片（连同还原结果）；留痕编辑中已在该桶 `files` 或 `deletions` 里的路径跳过，整桶都被
    /// 跳过时不为该桶写片。写哪一种清单片按 REQ-AMB-STO-HOT-LAYOUT-V8「写哪一种清单片」，见 [`choose_piece`]。
    async fn rewrite_buckets(
        &self,
        dataset: &Dataset,
        parent: &Snapshot,
        edits: BTreeMap<HourBucket, BucketEdit>,
        progress: &mut CommitProgress,
    ) -> Result<(Vec<BucketRef>, Vec<(String, Restored)>), Fail> {
        let store = &self.chain.store;
        let mut buckets: BTreeMap<String, BucketRef> = parent
            .buckets
            .iter()
            .map(|b| (b.bucket.clone(), b.clone()))
            .collect();
        let mut manifests = Vec::new();
        for (hour, edit) in edits {
            let label = hour.to_string();
            let before = match buckets.get(&label) {
                Some(r) => Some(self.chain.load_restored(dataset, parent, r).await?),
                None => None,
            };
            let empty = BucketManifest {
                format_version: FORMAT_VERSION,
                bucket: label.clone(),
                files: Vec::new(),
                deletions: Vec::new(),
                handovers: Vec::new(),
            };
            let m: &BucketManifest = before.as_ref().map_or(&empty, |r| &r.manifest);
            let tombstones: Vec<Deletion> = edit
                .tombstones
                .into_iter()
                .filter(|t| {
                    !m.files.iter().any(|f| f.path == t.path)
                        && !m.deletions.iter().any(|d| d.path == t.path)
                })
                .collect();
            if edit.add.is_empty()
                && edit.remove.is_empty()
                && tombstones.is_empty()
                && edit.handovers.is_empty()
            {
                continue;
            }
            let mut recorded = edit.remove;
            recorded.extend(tombstones);
            let mut after = m.clone();
            after.format_version = FORMAT_VERSION;
            after
                .files
                .retain(|f| !recorded.iter().any(|r| r.path == f.path));
            after.files.extend(edit.add.iter().cloned());
            after.deletions.extend(recorded.iter().cloned());
            after.handovers.extend(edit.handovers.iter().cloned());
            let path = layout::bucket_manifest_path(hour, &layout::new_object_id());
            let (piece, restored) = choose_piece(
                before.as_deref(),
                after,
                Chosen {
                    files_added: edit.add,
                    recorded,
                    handovers_added: edit.handovers,
                },
            );
            let bytes = Bytes::from(format::encode_piece(&piece));
            let digest = Digest::of(&bytes);
            progress.register(&self.inflight, dataset, &path);
            if store
                .create(&store.object_path(dataset, &path), bytes)
                .await?
                != Created::Done
            {
                return Err(Fail::Retry { unavailable: false });
            }
            let restored = restored(PieceRef {
                path: path.clone(),
                content_digest: digest,
            });
            buckets.insert(
                label,
                BucketRef::of(&restored.manifest, path.clone(), digest),
            );
            manifests.push((path, restored));
        }
        Ok((buckets.into_values().collect(), manifests))
    }

    fn succeed(&self, dataset: &Dataset, p: Prepared) -> Committed {
        let id = p.snapshot.snapshot_id;
        let parent = p.snapshot.parent_snapshot_id;
        let created_at = p.snapshot.created_at;
        for (path, m) in p.manifests {
            self.chain.remember_bucket_manifest(dataset, path, m);
        }
        if let Some((path, s)) = p.retired {
            self.chain.remember_retired_segment(dataset, path, s);
        }
        if let Some((path, s)) = p.cold_change {
            self.chain.remember_cold_change_segment(dataset, path, s);
        }
        self.chain.note_snapshot(dataset, id, p.digest);
        self.chain.remember_snapshot(dataset, p.snapshot);
        {
            let mut cold = self.cold.lock().expect("冷层文件集锁未中毒");
            match (p.cold_after, cold.get_mut(dataset)) {
                (Some(set), _) => {
                    cold.insert(dataset.clone(), (id, set));
                }
                // 本次没有改变已记入冷层文件集：父快照的那份照样属于新快照
                (None, Some(entry)) if Some(entry.0) == parent => entry.0 = id,
                _ => {}
            }
        }
        self.chain.registry.record(dataset, Persistence::Stored);
        Committed {
            snapshot_id: id,
            created_at,
        }
    }

    // -----------------------------------------------------------------------
    // 提交意图的判明
    // -----------------------------------------------------------------------

    /// 判明 WAL 重放中遇到的一条未判明的提交意图（ADR-0021 第 4 条经 ADR-0043 第 6 条改写，REQ-AMB-STO-MANIFEST-COMMIT-V4）：
    /// 快照 P 存在而摘要不是 d 为 [`IntentOutcome::Foreign`]；摘要是 d 为 [`IntentOutcome::Committed`]（快照创建成功即
    /// 提交完成，落后的提示由此后的提交补写）。P 不存在时：P 大于当前快照为 [`IntentOutcome::Absent`]；P 不大于当前快照
    /// （已被快照回收）时，当前快照的存活文件与完整删除台账中 `added_in` 等于 P 的条目含意图所记的数据文件路径即
    /// `Committed`，否则为 `Absent`（不上传数据文件的提交没有路径，只解绑；创世快照的意图由此在已有过快照时视为判明）。
    pub async fn resolve_intent(&self, intent: CommitIntent) -> Result<IntentOutcome, Unavailable> {
        let dataset = &intent.dataset;
        if self.chain.gates.state(dataset) != DatasetState::Normal {
            return Ok(IntentOutcome::Disabled);
        }
        let _guard = self.lock(dataset).await;
        let store = &self.chain.store;
        let Some(f) = store
            .read(&store.object_path(dataset, &layout::snapshot_path(intent.snapshot_id)))
            .await?
        else {
            return self.resolve_collected(&intent).await;
        };
        if Digest::of(&f.bytes) != intent.digest {
            return Ok(IntentOutcome::Foreign);
        }
        Ok(IntentOutcome::Committed)
    }

    /// 快照 P 不存在时的判明（见 [`StoreCommitter::resolve_intent`]）。读取所得使数据集停用为 `Disabled`。
    async fn resolve_collected(&self, intent: &CommitIntent) -> Result<IntentOutcome, Unavailable> {
        let dataset = &intent.dataset;
        let p = intent.snapshot_id;
        let outcome = |e: ChainError| match e {
            ChainError::Unavailable(m) => Err(Unavailable(m)),
            _ => Ok(IntentOutcome::Disabled),
        };
        let current = match self.chain.find_tail(dataset).await {
            Ok(tail) => tail.current,
            Err(e) => return outcome(e),
        };
        if current.is_none_or(|c| p > c) || intent.files.is_empty() {
            return Ok(IntentOutcome::Absent);
        }
        let current = match self
            .chain
            .load_snapshot(dataset, current.expect("上面已判"))
            .await
        {
            Ok(s) => s,
            Err(e) => return outcome(e),
        };
        let wanted: HashSet<&str> = intent.files.iter().map(String::as_str).collect();
        let hit = |m: &BucketManifest| {
            m.files
                .iter()
                .any(|f| f.added_in == p && wanted.contains(f.path.as_str()))
                || m.deletions
                    .iter()
                    .any(|d| d.added_in == Some(p) && wanted.contains(d.path.as_str()))
        };
        for b in &current.buckets {
            match self.chain.load_bucket_manifest(dataset, &current, b).await {
                Ok(m) if hit(&m) => return Ok(IntentOutcome::Committed),
                Ok(_) => {}
                Err(e) => return outcome(e),
            }
        }
        // 可达留痕归档段中各桶最后一份桶 manifest 的 `files` 与 `deletions`；冷层变更段只记冷层文件，不含热层路径
        let retired = match self.chain.retired_segments(dataset, &current).await {
            Ok(r) => r,
            Err(ChainError::Unavailable(m)) => return Err(Unavailable(m)),
            Err(_) => {
                self.chain_disable(dataset, DatasetState::ManifestUnreadable);
                return Ok(IntentOutcome::Disabled);
            }
        };
        for segment in &retired {
            for b in &segment.buckets {
                let r = BucketRef {
                    bucket: b.bucket.clone(),
                    manifest: b.manifest.clone(),
                    content_digest: b.content_digest,
                    record_count: 0,
                    size_bytes: 0,
                    time_min: None,
                    time_max: None,
                };
                match self.chain.read_bucket_manifest(dataset, &current, &r).await {
                    Ok(Ok(m)) if hit(&m) => return Ok(IntentOutcome::Committed),
                    Ok(Ok(_)) => {}
                    Ok(Err(_)) => {
                        // 与提交读留痕归档段所引用的桶 manifest 同样处置：进入 manifest 不可读情形
                        self.chain_disable(dataset, DatasetState::ManifestUnreadable);
                        return Ok(IntentOutcome::Disabled);
                    }
                    Err(e) => return outcome(e),
                }
            }
        }
        Ok(IntentOutcome::Absent)
    }
}

/// 一次提交为某桶带来的改动：增量片写出的正是这三项。
struct Chosen {
    /// 本次进入存活文件集的文件条目。
    files_added: Vec<FileEntry>,
    /// 本次新记入的留痕条目。
    recorded: Vec<Deletion>,
    /// 本次新追加的交接记录。
    handovers_added: Vec<Handover>,
}

/// 按 REQ-AMB-STO-HOT-LAYOUT-V8「写哪一种清单片」为一个受影响的桶选片（ADR-0057 第 4 条）：`before` 为父快照下该桶
/// manifest 的还原结果（父快照不含该桶为 `None`），`after` 为本次提交生效之后的桶 manifest。返回要写出的清单片，与给定
/// 其引用之后的还原结果。先后取第一条成立的：
/// 1. 父快照不含该桶：全量片，`ledger_previous` 为 null，`deletions` 只含本次新记入的；
/// 2. 提交之后该桶没有存活数据文件：合拢全量片，`ledger_previous` 为 null，`deletions` 为全部留痕；
/// 3. 父快照中该桶头片的增量链已有 64 片：全量片，`deletions` 为基准片 B 之后各增量片的与本次的，`ledger_previous` 在 B
///    自身的 `deletions` 非空时为 B、否则为 B 的 `ledger_previous`；所得留痕链超过 64 片时改写合拢全量片；
/// 4. 其余：增量片，`previous` 为父快照中该桶的头片。
fn choose_piece(
    before: Option<&Restored>,
    after: BucketManifest,
    chosen: Chosen,
) -> (Piece, impl FnOnce(PieceRef) -> Restored + Send) {
    enum Kind {
        /// 全量片：`ledger_previous`、留痕链片数、自身的 `deletions`。
        Full(Option<PieceRef>, usize, Vec<Deletion>),
        Delta(Restored),
    }
    let kind = match before {
        None => Kind::Full(None, 1, chosen.recorded.clone()),
        Some(_) if after.files.is_empty() => Kind::Full(None, 1, after.deletions.clone()),
        Some(r) if r.delta_len >= CHAIN_LIMIT => {
            let (ledger_previous, ledger_len) = if r.base_has_deletions {
                (Some(r.base.clone()), r.base_ledger_len + 1)
            } else {
                (r.base_ledger_previous.clone(), r.base_ledger_len)
            };
            if ledger_len > CHAIN_LIMIT {
                Kind::Full(None, 1, after.deletions.clone())
            } else {
                Kind::Full(
                    ledger_previous,
                    ledger_len,
                    after.deletions[r.base_deletions..].to_vec(),
                )
            }
        }
        Some(r) => Kind::Delta(r.clone()),
    };
    let piece = |link, files, deletions, handovers| Piece {
        format_version: FORMAT_VERSION,
        bucket: after.bucket.clone(),
        link,
        files,
        deletions,
        handovers,
    };
    match kind {
        Kind::Full(ledger_previous, ledger_len, own) => {
            let p = piece(
                PieceLink::Full {
                    ledger_previous: ledger_previous.clone(),
                },
                after.files.clone(),
                own.clone(),
                after.handovers.clone(),
            );
            // 留痕链上 `ledger_previous` 之后的各片：B 本身在链上时取 B 起，否则取 B 的留痕链（不含 B）
            let tail: Vec<String> = match (before, &ledger_previous) {
                (Some(r), Some(lp)) if *lp == r.base => r.pieces[r.delta_len - 1..].to_vec(),
                (Some(r), Some(_)) => r.pieces[r.delta_len..].to_vec(),
                _ => Vec::new(),
            };
            let base_deletions = after.deletions.len();
            let restored = move |head: PieceRef| Restored {
                pieces: std::iter::once(head.path.clone()).chain(tail).collect(),
                base: head.clone(),
                head,
                delta_len: 1,
                base_has_deletions: !own.is_empty(),
                base_ledger_previous: ledger_previous,
                base_ledger_len: ledger_len,
                base_deletions,
                manifest: Arc::new(after),
            };
            (
                p,
                Box::new(restored) as Box<dyn FnOnce(PieceRef) -> Restored + Send>,
            )
        }
        Kind::Delta(r) => {
            let p = piece(
                PieceLink::Delta {
                    previous: r.head.clone(),
                },
                chosen.files_added,
                chosen.recorded,
                chosen.handovers_added,
            );
            let restored = move |head: PieceRef| {
                let mut out = r;
                out.pieces.insert(0, head.path.clone());
                out.head = head;
                out.delta_len += 1;
                out.manifest = Arc::new(after);
                out
            };
            (
                p,
                Box::new(restored) as Box<dyn FnOnce(PieceRef) -> Restored + Send>,
            )
        }
    }
}

/// 存活文件 `f` 离开存活集时的留痕条目（`path`、`content_digest`、`added_in` 与 `max_timestamp` 照抄其 `files` 条目，
/// REQ-AMB-STO-AUDIT-INVARIANTS-V3）。
fn removal(
    f: &FileEntry,
    reason: DeletionReason,
    cutoff: Option<i64>,
    deleted_at: i64,
    removed_in: SnapshotId,
) -> Deletion {
    Deletion {
        path: f.path.clone(),
        content_digest: f.content_digest,
        reason,
        added_in: Some(f.added_in),
        max_timestamp: Some(f.max_timestamp),
        cutoff,
        deleted_at,
        removed_in,
    }
}

/// `cold` 加上新记入的冷层文件 `files` 之后的合计与极值。
fn extend_cold(cold: ColdSummary, files: &[ColdFileEntry]) -> ColdSummary {
    files.iter().fold(cold, |acc, f| ColdSummary {
        record_count: acc.record_count + f.record_count,
        size_bytes: acc.size_bytes + f.size_bytes,
        time_min: Some(
            acc.time_min
                .map_or(f.min_timestamp, |t| t.min(f.min_timestamp)),
        ),
        time_max: Some(
            acc.time_max
                .map_or(f.max_timestamp, |t| t.max(f.max_timestamp)),
        ),
    })
}

/// 一个桶的改动：新增的文件条目、离开存活集的文件（连同其留痕）、孤儿文件的留痕、新的交接记录。
#[derive(Default)]
struct BucketEdit {
    add: Vec<FileEntry>,
    remove: Vec<Deletion>,
    tombstones: Vec<Deletion>,
    handovers: Vec<Handover>,
}

fn baseline(dataset: &Dataset, s: &Snapshot) -> Baseline {
    Baseline {
        dataset: dataset.clone(),
        snapshot_id: Some(s.snapshot_id),
        columns: Some(StreamColumns::from_columns(s.schema.clone())),
    }
}

impl HotCommits for StoreCommitter {
    fn commit_handover<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: HandoverRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        Box::pin(async move { self.run_existing(dataset, Job::Handover(&request)).await })
    }

    fn commit_ttl_hot<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: TtlHotRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        Box::pin(async move { self.run_existing(dataset, Job::TtlHot(&request)).await })
    }

    fn commit_cold_change<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ColdChangeRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        Box::pin(async move { self.run_existing(dataset, Job::ColdChange(&request)).await })
    }

    fn commit_archive<'a>(
        &'a self,
        dataset: &'a Dataset,
        request: ArchiveRequest,
    ) -> BoxFuture<'a, Result<Committed, CommitError>> {
        Box::pin(async move { self.run_existing(dataset, Job::Archive(&request)).await })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const B: &str = "2026082210";

    fn piece_ref(n: u32) -> PieceRef {
        PieceRef {
            path: format!("manifest/buckets/{B}-01k6{n:022}.json"),
            content_digest: Digest::of(&n.to_le_bytes()),
        }
    }

    fn file(n: u32) -> FileEntry {
        FileEntry {
            path: format!("data/2026/08/22/10/01k7{n:022}.parquet"),
            min_timestamp: 1,
            max_timestamp: 2,
            record_count: 1,
            size_bytes: 10,
            content_digest: Digest::of(&n.to_le_bytes()),
            added_in: SnapshotId(2),
            column_stats: None,
        }
    }

    fn tomb(n: u32) -> Deletion {
        removal(&file(n), DeletionReason::Compaction, None, 5, SnapshotId(3))
    }

    fn manifest(files: Vec<FileEntry>, deletions: Vec<Deletion>) -> BucketManifest {
        BucketManifest {
            format_version: FORMAT_VERSION,
            bucket: B.into(),
            files,
            deletions,
            handovers: Vec::new(),
        }
    }

    /// 父快照下的桶 manifest：头片 1（增量链 `delta_len` 片，基准片 2），已有留痕 `deletions`（其中前 `base_deletions` 条属
    /// 基准片的还原）。
    fn before(delta_len: usize, deletions: Vec<Deletion>, base_deletions: usize) -> Restored {
        Restored {
            manifest: Arc::new(manifest(vec![file(1)], deletions)),
            head: piece_ref(1),
            delta_len,
            base: piece_ref(2),
            base_has_deletions: base_deletions > 0,
            base_ledger_previous: Some(piece_ref(3)),
            base_ledger_len: 2,
            base_deletions,
            // 增量链：头片 1、中间各片、基准片 2；再接基准片的留痕链：3
            pieces: std::iter::once(1)
                .chain((0..delta_len as u32 - 2).map(|i| 100 + i))
                .chain([2, 3])
                .map(|n| piece_ref(n).path)
                .collect(),
        }
    }

    fn chosen(add: Vec<FileEntry>, recorded: Vec<Deletion>) -> Chosen {
        Chosen {
            files_added: add,
            recorded,
            handovers_added: Vec::new(),
        }
    }

    #[test]
    fn new_bucket_gets_a_full_piece_with_only_new_tombstones() {
        let (p, r) = choose_piece(
            None,
            manifest(vec![file(1)], vec![tomb(9)]),
            chosen(vec![file(1)], vec![tomb(9)]),
        );
        assert_eq!(
            p.link,
            PieceLink::Full {
                ledger_previous: None
            }
        );
        assert_eq!(p.deletions, vec![tomb(9)]);
        let r = r(piece_ref(7));
        assert_eq!((r.delta_len, r.base_ledger_len), (1, 1));
        assert_eq!(r.pieces, vec![piece_ref(7).path]);
    }

    #[test]
    fn emptied_bucket_gets_a_consolidated_full_piece() {
        let b = before(5, vec![tomb(8)], 1);
        let (p, _) = choose_piece(
            Some(&b),
            manifest(Vec::new(), vec![tomb(8), tomb(1)]),
            chosen(Vec::new(), vec![tomb(1)]),
        );
        assert_eq!(
            p.link,
            PieceLink::Full {
                ledger_previous: None
            }
        );
        assert_eq!(p.deletions, vec![tomb(8), tomb(1)]);
        assert!(p.files.is_empty());
    }

    #[test]
    fn short_chain_gets_a_delta_piece() {
        let b = before(63, Vec::new(), 0);
        let (p, r) = choose_piece(
            Some(&b),
            manifest(vec![file(1), file(4)], Vec::new()),
            chosen(vec![file(4)], Vec::new()),
        );
        assert_eq!(
            p.link,
            PieceLink::Delta {
                previous: piece_ref(1)
            }
        );
        assert_eq!(p.files, vec![file(4)]);
        let r = r(piece_ref(7));
        assert_eq!(r.delta_len, 64);
        assert_eq!(r.base, piece_ref(2));
        assert_eq!(r.pieces[0], piece_ref(7).path);
    }

    #[test]
    fn full_chain_gets_a_full_piece_linked_to_the_base() {
        // 基准片自身有留痕：`ledger_previous` 取基准片，`deletions` 只含其后的
        let b = before(64, vec![tomb(5), tomb(6)], 1);
        let (p, r) = choose_piece(
            Some(&b),
            manifest(vec![file(1), file(4)], vec![tomb(5), tomb(6), tomb(7)]),
            chosen(vec![file(4)], vec![tomb(7)]),
        );
        assert_eq!(
            p.link,
            PieceLink::Full {
                ledger_previous: Some(piece_ref(2))
            }
        );
        assert_eq!(p.deletions, vec![tomb(6), tomb(7)]);
        assert_eq!(p.files, vec![file(1), file(4)]);
        let r = r(piece_ref(7));
        assert_eq!(r.base_ledger_len, 3);
        assert_eq!(r.base_deletions, 3);
        // 新片、基准片、基准片的留痕链
        assert_eq!(
            r.pieces,
            [piece_ref(7).path, piece_ref(2).path, piece_ref(3).path]
        );

        // 基准片自身没有留痕：`ledger_previous` 取基准片的 `ledger_previous`
        let b = before(64, vec![tomb(6)], 0);
        let (p, r) = choose_piece(
            Some(&b),
            manifest(vec![file(1)], vec![tomb(6)]),
            chosen(Vec::new(), Vec::new()),
        );
        assert_eq!(
            p.link,
            PieceLink::Full {
                ledger_previous: Some(piece_ref(3))
            }
        );
        assert_eq!(r(piece_ref(7)).base_ledger_len, 2);
    }

    #[test]
    fn too_long_ledger_chain_gets_a_consolidated_full_piece() {
        let mut b = before(64, vec![tomb(5), tomb(6)], 1);
        b.base_ledger_len = CHAIN_LIMIT;
        let (p, _) = choose_piece(
            Some(&b),
            manifest(vec![file(1)], vec![tomb(5), tomb(6)]),
            chosen(Vec::new(), Vec::new()),
        );
        assert_eq!(
            p.link,
            PieceLink::Full {
                ledger_previous: None
            }
        );
        assert_eq!(p.deletions, vec![tomb(5), tomb(6)]);
    }
}
