//! 入湖（REQ-AMB-STO-LAKE-ENTRY）：越过入湖水位、仍含存活热层数据文件的小时桶，核对摘要后重写进冷层表，再以一次入湖
//! 交接提交使入湖输入离开热层存活文件集；冷层提交之后、交接之前中断的，补做交接。编排归桶生命周期。
//!
//! 桶生命周期的一项周期工作（[`BucketTask`]），每个检查周期（远小于 60 秒）对每个存在且正常的数据集：
//! 1. 取本进程已知最新快照（单写者下即热层当前快照），算出待入湖桶与入湖积压时长，必要时输出 `lake_backlog_high`；
//! 2. 本进程对该数据集还没确认过「没有处于交接窗口的冷层提交」时（开始受理之后的第一轮，或此后某次交接提交没有成功），
//!    读冷层当前版本的元数据，按 [`ColdTable::handover_window`] 找出处于交接窗口的冷层提交，逐个补做交接：交接记录的
//!    `files` 取冷层当前快照中该提交新增的文件（清单条目），`content_digest` 取 `amberlog.file_digests`，不读冷层数据文件；
//!    `metadata_version` 取首次列出该快照的版本；
//! 3. 补做都成功之后，对最早的待入湖桶（本进程已因摘要核对停止入湖的桶跳过）入湖一次：登记入湖输入
//!    （[`LakeInputs::begin`]，TTL 守卫持有时本轮不入湖）→ 读出全部入湖输入并核对摘要（不符各报一次 `digest_mismatch`，
//!    任一不符或不存在即停止该桶的入湖）→ [`ColdWriter::append`]（重写只用核对过的字节）→ 入湖交接提交。物理删除由
//!    [`crate::purge`] 按 `storage.delete_delay` 延后进行。
//!
//! 同一桶的入湖与封口合并经 [`BucketClaims`] 在进程内互斥；`digest_mismatch` 的去重（同一数据集同一路径至多一次）与合并
//! 共用 [`DigestReports`]。为入湖读热层桶 manifest 走提交的读路径（落入 (a) 即停用）；为入湖读冷层元数据得到不存在或内容
//! 无法解析、或尚无冷层表而热层快照 `summary.cold.record_count` 大于 0 时，数据集进入 manifest 不可读情形
//! （REQ-AMB-STO-COLD-TABLE-READ-BROKEN、-TABLE-GONE）。停用的数据集不入湖；停机过程开始后不再开始新的一轮。
//! 每次入湖结束后，分配器的空闲内存多就还给系统（[`memory::release_if_much_free`]）。

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ops::RangeInclusive;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use bytes::Bytes;

use crate::BoxFuture;
use crate::bloom::{self, IndexHints};
use crate::chain::{
    BucketRef, ChainError, ColdFileEntry, Digest, FileEntry, HandoverDraft, Snapshot,
};
use crate::cold::{
    ColdAppend, ColdOperation, ColdReadError, ColdReader, ColdScope, ColdTable, ColdWriteError,
    ColdWriter, WindowCommit,
};
use crate::commit::{HandoverRequest, HotCommits};
use crate::committer::CommitError;
use crate::fault;
use crate::flusher::Flusher;
use crate::gate::DatasetState;
use crate::layout::HourBucket;
use crate::lifecycle::{self, BucketState, Lifecycle};
use crate::maintenance::{BucketTask, LakeInputs};
use crate::memory;
use crate::registry::Persistence;

const DAY_US: i64 = 86_400_000_000;

/// 只读冷层元数据、不读任何清单时给的日期范围（没有清单的分区摘要落在这一天）。
const NO_DAYS: RangeInclusive<i32> = i32::MIN..=i32::MIN;

/// 占用一个桶的一方。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Claimant {
    Compaction,
    Lake,
}

/// 同一进程内同一数据集同一桶的合并与入湖不同时进行（REQ-AMB-STO-SEAL-COMPACTION-V2）：合并自确定合并输入起、入湖自
/// 确定入湖输入起占用该桶，至提交或放弃止。
#[derive(Debug, Default)]
pub struct BucketClaims {
    held: Mutex<HashMap<(Dataset, String), Claimant>>,
}

impl BucketClaims {
    /// 占用数据集 `dataset` 的桶 `bucket`（`YYYYMMDDHH`）；已被占用时为 `None`。丢弃返回的 [`Claim`] 即释放。
    pub fn claim(
        self: &Arc<Self>,
        dataset: &Dataset,
        bucket: &str,
        who: Claimant,
    ) -> Option<Claim> {
        let key = (dataset.clone(), bucket.to_owned());
        let mut held = self.held.lock().expect("桶占用表锁未中毒");
        if held.contains_key(&key) {
            return None;
        }
        held.insert(key.clone(), who);
        Some(Claim {
            claims: Arc::clone(self),
            key,
        })
    }
}

/// 见 [`BucketClaims::claim`]。
#[derive(Debug)]
pub struct Claim {
    claims: Arc<BucketClaims>,
    key: (Dataset, String),
}

impl Drop for Claim {
    fn drop(&mut self) {
        self.claims
            .held
            .lock()
            .expect("桶占用表锁未中毒")
            .remove(&self.key);
    }
}

/// 本进程已输出过 `digest_mismatch` 的（数据集，路径），封口合并与入湖共用（REQ-AMB-STO-LAKE-ENTRY-DIGEST-EVENT）。
#[derive(Debug, Default)]
pub struct DigestReports {
    reported: Mutex<HashSet<(Dataset, String)>>,
}

impl DigestReports {
    /// 记下（数据集，路径）；此前没有记过（应当输出 `digest_mismatch`）时为 `true`。
    pub fn first(&self, dataset: &Dataset, path: &str) -> bool {
        self.reported
            .lock()
            .expect("摘要事件表锁未中毒")
            .insert((dataset.clone(), path.to_owned()))
    }
}

/// 入湖的配置。
#[derive(Debug, Clone, Copy)]
pub struct LakeConfig {
    pub lifecycle: Lifecycle,
    /// `lifecycle.lake_alert_after`
    pub alert_after: Duration,
    /// 检查周期（不大于 60 秒）。
    pub check_interval: Duration,
}

/// 入湖的可观察结果，由调用方接到稳定日志事件。
pub trait LakeEvents: Send + Sync {
    /// 入湖输入完整字节的摘要与其 `content_digest` 不符（经 [`DigestReports`] 去重之后）。
    fn digest_mismatch(&self, dataset: &Dataset, path: &str);
    /// 为桶 `bucket`（`YYYYMMDDHH`）入湖时发生重写失败（REQ-AMB-STO-LAKE-ENTRY-V2-REWRITE-FAIL），本进程已停止该桶的入湖；
    /// 同一数据集的同一桶在本进程里至多一次。
    fn rewrite_failed(&self, dataset: &Dataset, bucket: &str, reason: &str);
    /// 入湖积压时长越过 `lifecycle.lake_alert_after`。
    fn backlog_high(
        &self,
        dataset: &Dataset,
        bucket: &str,
        lag_seconds: i64,
        alert_after_seconds: i64,
    );
}

/// 入湖的各个协作方。
pub struct LakeParts {
    pub flusher: Arc<Flusher>,
    pub reader: Arc<dyn ColdReader>,
    pub writer: Arc<dyn ColdWriter>,
    pub inputs: Arc<LakeInputs>,
    pub claims: Arc<BucketClaims>,
    pub reports: Arc<DigestReports>,
    pub events: Arc<dyn LakeEvents>,
    /// 本进程启动时读入的索引提示：入湖写出的冷层数据文件按它写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ②）。
    pub hints: Arc<IndexHints>,
}

/// 一个数据集在本进程中的入湖情况。
#[derive(Default)]
struct DatasetLake {
    /// 已确认（开始受理之后、且其后的交接提交都成功）冷层没有处于交接窗口的提交。
    window_clear: bool,
    /// 本进程已停止入湖的桶：摘要核对不符（含输入不存在）或重写失败（REQ-AMB-STO-LAKE-ENTRY-V2-STOPPED）。
    stopped: HashSet<String>,
    /// 上一次测得的入湖积压时长超过 `lifecycle.lake_alert_after`。
    over: bool,
}

/// 对该数据集的入湖没有进行下去：对象存储不可访问，或其他原因（数据集停用、重试耗尽、停机等）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LakeHalt {
    Unavailable,
    Other,
}

type Skip = LakeHalt;

/// 读热层快照链失败：不可访问与其他分开。
fn chain_halt(e: &ChainError) -> Skip {
    match e {
        ChainError::Unavailable(_) => Skip::Unavailable,
        _ => Skip::Other,
    }
}

/// 对一个桶入湖一次的结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Laked {
    /// 写出了入湖交接提交。
    Handed,
    /// 有入湖输入摘要不符或不存在，未写冷层提交（REQ-AMB-STO-LAKE-ENTRY-DIGEST-ABORT），本进程停止该桶的入湖。
    Stopped {
        mismatched: Vec<String>,
        missing: Vec<String>,
    },
    /// 本次没有入湖：桶已没有存活文件、被合并占用、TTL 正在判定、输入已不全部存活，或交接已由他人写出。
    NotNow,
}

/// 入湖的驱动。
pub struct Lake {
    parts: LakeParts,
    config: LakeConfig,
    state: Mutex<HashMap<Dataset, DatasetLake>>,
    stopping: AtomicBool,
}

impl BucketTask for Lake {
    fn period(&self) -> Duration {
        self.config.check_interval
    }

    fn first_delay(&self) -> Duration {
        Duration::from_secs(2).min(self.config.check_interval)
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let chain = self.parts.flusher.committer().chain();
            for dataset in chain.registry.datasets() {
                if self.stopping() {
                    return;
                }
                if chain.registry.persistence(&dataset) != Some(Persistence::Stored)
                    || !self.normal(&dataset)
                {
                    continue;
                }
                let _ = self.round(&dataset).await;
            }
        })
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

/// 数据集在快照 `snapshot` 下、此刻 `now_us` 的待入湖桶（含存活数据文件、不早于入湖水位时刻），从早到晚。
pub fn pending_buckets(snapshot: &Snapshot, lifecycle: &Lifecycle, now_us: i64) -> Vec<HourBucket> {
    let mut out: Vec<HourBucket> = snapshot
        .buckets
        .iter()
        .filter(|b| b.record_count > 0)
        .filter_map(|b| HourBucket::parse(&b.bucket))
        .filter(|h| lifecycle.state(*h, now_us) == BucketState::PendingLake)
        .collect();
    out.sort();
    out
}

/// 入湖积压时长（REQ-AMB-STO-LAKE-ENTRY-LAG）：此刻减最早的待入湖桶的入湖水位时刻，向下取整的秒数；没有待入湖桶为 0。
pub fn lag_seconds(snapshot: &Snapshot, lifecycle: &Lifecycle, now_us: i64) -> i64 {
    pending_buckets(snapshot, lifecycle, now_us)
        .first()
        .map_or(0, |h| {
            (now_us - lifecycle.lake_at_us(*h))
                .div_euclid(1_000_000)
                .max(0)
        })
}

impl Lake {
    pub fn new(parts: LakeParts, config: LakeConfig) -> Lake {
        Lake {
            parts,
            config,
            state: Mutex::default(),
            stopping: AtomicBool::new(false),
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn normal(&self, dataset: &Dataset) -> bool {
        self.parts.flusher.committer().chain().gates.state(dataset) == DatasetState::Normal
    }

    fn state<R>(&self, dataset: &Dataset, f: impl FnOnce(&mut DatasetLake) -> R) -> R {
        let mut all = self.state.lock().expect("入湖状态锁未中毒");
        f(all.entry(dataset.clone()).or_default())
    }

    fn disable(&self, dataset: &Dataset) {
        self.parts
            .flusher
            .committer()
            .chain()
            .gates
            .enter(dataset, DatasetState::ManifestUnreadable);
    }

    /// 冷层读取失败：不存在或内容无法解析使数据集停用（REQ-AMB-STO-COLD-TABLE-READ-BROKEN），不可访问只结束本轮。
    fn cold_read_failed(&self, dataset: &Dataset, e: ColdReadError) -> Skip {
        if matches!(e, ColdReadError::Unavailable(_)) {
            return Skip::Unavailable;
        }
        self.disable(dataset);
        Skip::Other
    }

    async fn round(&self, dataset: &Dataset) -> Result<(), Skip> {
        let chain = self.parts.flusher.committer().chain();
        let Some((snapshot, _pin)) = chain.pin_latest(dataset) else {
            return Ok(());
        };
        let now = lifecycle::now_us();
        let pending = pending_buckets(&snapshot, &self.config.lifecycle, now);
        self.measure(dataset, &snapshot, &pending, now);

        let window_clear = self.state(dataset, |s| s.window_clear);
        if pending.is_empty() && window_clear {
            return Ok(());
        }
        // 冷层当前版本（只读元数据）
        let version = self
            .parts
            .reader
            .version(dataset)
            .await
            .map_err(|e| self.cold_read_failed(dataset, e))?;
        let table = match version {
            None if snapshot.summary.cold.record_count > 0 => {
                // 冷层表已被整体删除（REQ-AMB-STO-COLD-TABLE-TABLE-GONE）
                self.disable(dataset);
                return Err(Skip::Other);
            }
            None => None,
            Some(v) => Some(
                self.parts
                    .reader
                    .table_at(dataset, v, ColdScope::Days(NO_DAYS))
                    .await
                    .map_err(|e| self.cold_read_failed(dataset, e))?,
            ),
        };

        // 补做处于交接窗口的冷层提交的交接
        let mut windows = Vec::new();
        if let Some(table) = &table {
            windows = self.windows(dataset, &snapshot, table).await?;
            for w in &windows {
                if self.stopping() {
                    return Err(Skip::Other);
                }
                if let Err(skip) = self.recover(dataset, table, w).await {
                    self.state(dataset, |s| s.window_clear = false);
                    return Err(skip);
                }
            }
        }
        self.state(dataset, |s| s.window_clear = true);
        if !windows.is_empty() {
            // 补做改变了热层快照：新入湖下一轮按新快照判定
            return Ok(());
        }

        // 最早的待入湖桶（已停止入湖的跳过）
        let stopped = self.state(dataset, |s| s.stopped.clone());
        let Some(hour) = pending
            .iter()
            .copied()
            .find(|h| !stopped.contains(&h.to_string()))
        else {
            return Ok(());
        };
        if self.stopping() || !self.normal(dataset) {
            return Err(Skip::Other);
        }
        let result = self.lake(dataset, &snapshot, hour).await.map(|_| ());
        memory::release_if_much_free();
        result
    }

    /// 优雅退役（REQ-AMB-LIFE-RETIREMENT-RECOVER-WINDOW）：为数据集冷层当前版本中处于交接窗口的冷层提交逐个补做交接，
    /// 返回本次写出的入湖交接提交个数。尚无冷层表时为 0；冷层读取与补做的处置同周期工作。
    pub async fn recover_windows(&self, dataset: &Dataset) -> Result<u64, LakeHalt> {
        let chain = self.parts.flusher.committer().chain();
        let Some((snapshot, _pin)) = chain.pin_latest(dataset) else {
            return Ok(0);
        };
        let version = self
            .parts
            .reader
            .version(dataset)
            .await
            .map_err(|e| self.cold_read_failed(dataset, e))?;
        let table = match version {
            None if snapshot.summary.cold.record_count > 0 => {
                self.disable(dataset);
                return Err(Skip::Other);
            }
            None => return Ok(0),
            Some(v) => self
                .parts
                .reader
                .table_at(dataset, v, ColdScope::Days(NO_DAYS))
                .await
                .map_err(|e| self.cold_read_failed(dataset, e))?,
        };
        let mut handed = 0;
        for w in &self.windows(dataset, &snapshot, &table).await? {
            if self.stopping() {
                return Err(Skip::Other);
            }
            if self.recover(dataset, &table, w).await? {
                handed += 1;
            }
        }
        Ok(handed)
    }

    /// 优雅退役（REQ-AMB-LIFE-RETIREMENT-LAKE-ALL）：按本进程已知最新快照对桶 `hour` 入湖一次，不看桶状态与入湖水位。
    pub async fn lake_bucket(
        &self,
        dataset: &Dataset,
        hour: HourBucket,
    ) -> Result<Laked, LakeHalt> {
        let chain = self.parts.flusher.committer().chain();
        let Some((snapshot, _pin)) = chain.pin_latest(dataset) else {
            return Ok(Laked::NotNow);
        };
        if self.stopping() || !self.normal(dataset) {
            return Err(Skip::Other);
        }
        let result = self.lake(dataset, &snapshot, hour).await;
        memory::release_if_much_free();
        result
    }

    /// 入湖积压时长与告警（REQ-AMB-STO-LAKE-ENTRY-ALERT、-ALERT-ONCE）。
    fn measure(&self, dataset: &Dataset, snapshot: &Snapshot, pending: &[HourBucket], now: i64) {
        let lag = lag_seconds(snapshot, &self.config.lifecycle, now);
        let alert_after = i64::try_from(self.config.alert_after.as_secs()).unwrap_or(i64::MAX);
        let over = lag > alert_after;
        let was = self.state(dataset, |s| std::mem::replace(&mut s.over, over));
        if over
            && !was
            && let Some(first) = pending.first()
        {
            self.parts
                .events
                .backlog_high(dataset, &first.to_string(), lag, alert_after);
        }
    }

    /// 处于交接窗口的冷层提交：读热层当前快照中各 `lake` 提交所记桶的桶 manifest（提交的读路径）。
    async fn windows(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        table: &ColdTable,
    ) -> Result<Vec<WindowCommit>, Skip> {
        let chain = self.parts.flusher.committer().chain();
        let mut live: BTreeMap<HourBucket, Vec<FileEntry>> = BTreeMap::new();
        for hour in table.lake_buckets() {
            let label = hour.to_string();
            let Some(r) = snapshot.buckets.iter().find(|b| b.bucket == label) else {
                continue;
            };
            if r.record_count == 0 {
                continue;
            }
            let m = chain
                .load_bucket_manifest(dataset, snapshot, r)
                .await
                .map_err(|e| chain_halt(&e))?;
            live.insert(hour, m.files.clone());
        }
        let mut w = table.handover_window(&live);
        w.sort_by_key(|c| c.bucket);
        Ok(w)
    }

    /// 补做一个冷层提交的入湖交接提交（REQ-AMB-STO-LAKE-ENTRY-RECOVER、-RECOVER-RUNNING、-RECOVER-NO-READ）；返回是否由本次
    /// 写出。
    async fn recover(
        &self,
        dataset: &Dataset,
        table: &ColdTable,
        w: &WindowCommit,
    ) -> Result<bool, Skip> {
        let reader = &self.parts.reader;
        let day = i32::try_from(w.bucket.start_us().div_euclid(DAY_US)).unwrap_or(i32::MAX);
        let current = reader
            .table_at(dataset, table.metadata_version, ColdScope::Days(day..=day))
            .await
            .map_err(|e| self.cold_read_failed(dataset, e))?;
        let Some(digests) = current.snapshots.iter().find_map(|s| match &s.operation {
            ColdOperation::Lake { file_digests, .. } if s.snapshot_id == w.iceberg_snapshot_id => {
                Some(file_digests.clone())
            }
            _ => None,
        }) else {
            return Err(Skip::Other);
        };
        let mut files = Vec::new();
        for f in current
            .files
            .iter()
            .filter(|f| f.added_snapshot_id == w.iceberg_snapshot_id)
        {
            let Some(digest) = digests.get(&f.path) else {
                // `amberlog.file_digests` 缺了新增的文件：写不出交接记录，按内容无法解析处置
                self.disable(dataset);
                return Err(Skip::Other);
            };
            files.push(ColdFileEntry {
                path: f.path.clone(),
                min_timestamp: f.min_timestamp,
                max_timestamp: f.max_timestamp,
                record_count: f.record_count,
                size_bytes: f.size_bytes,
                content_digest: *digest,
            });
        }
        // 写出该快照的版本：自当前版本向前，直到前一版本不再列出它
        let mut version = current.metadata_version;
        while version > 1 {
            let earlier = reader
                .table_at(dataset, version - 1, ColdScope::Days(NO_DAYS))
                .await
                .map_err(|e| self.cold_read_failed(dataset, e))?;
            if !earlier
                .snapshots
                .iter()
                .any(|s| s.snapshot_id == w.iceberg_snapshot_id)
            {
                break;
            }
            version -= 1;
        }
        let handover = HandoverDraft {
            table_location: current.location.clone(),
            metadata_version: version,
            iceberg_snapshot_id: w.iceberg_snapshot_id,
            files,
        };
        fault::pause("lake.before_recover_commit");
        fault::gate("lake.before_recover_commit").await;
        self.handover(dataset, w.bucket, w.hot_snapshot_id, handover)
            .await
    }

    /// 入湖交接提交；返回是否由本次写出（交接已写过为 `false`）。成功之外都使下一轮先检查交接窗口。
    async fn handover(
        &self,
        dataset: &Dataset,
        bucket: HourBucket,
        hot_snapshot_id: crate::chain::SnapshotId,
        handover: HandoverDraft,
    ) -> Result<bool, Skip> {
        let flusher = &self.parts.flusher;
        match flusher
            .committer()
            .commit_handover(
                dataset,
                HandoverRequest {
                    bucket,
                    hot_snapshot_id,
                    handover,
                },
            )
            .await
        {
            Ok(_) => {
                flusher.record_ok(dataset);
                Ok(true)
            }
            // 交接已写过（入湖输入都已不存活）
            Err(CommitError::Stale) => Ok(false),
            Err(CommitError::Exhausted | CommitError::Disabled(_)) => {
                flusher.record_failed(dataset);
                self.state(dataset, |s| s.window_clear = false);
                Err(Skip::Other)
            }
            Err(CommitError::Unavailable) => {
                // 就地计一次失败（REQ-AMB-OBS-HEALTH-METRICS-V4）
                flusher.record_background_unavailable(dataset);
                self.state(dataset, |s| s.window_clear = false);
                Err(Skip::Unavailable)
            }
            Err(CommitError::Wal(_)) => {
                self.state(dataset, |s| s.window_clear = false);
                Err(Skip::Other)
            }
        }
    }

    /// 对桶 `hour` 入湖一次。
    async fn lake(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        hour: HourBucket,
    ) -> Result<Laked, Skip> {
        let label = hour.to_string();
        let Some(r): Option<&BucketRef> = snapshot.buckets.iter().find(|b| b.bucket == label)
        else {
            return Ok(Laked::NotNow);
        };
        // 合并占着该桶时本轮不入湖（不越过它去入湖更晚的桶）
        let Some(_claim) = self.parts.claims.claim(dataset, &label, Claimant::Lake) else {
            return Ok(Laked::NotNow);
        };
        let chain = self.parts.flusher.committer().chain();
        let manifest = chain
            .load_bucket_manifest(dataset, snapshot, r)
            .await
            .map_err(|e| chain_halt(&e))?;
        let inputs: Vec<FileEntry> = manifest.files.clone();
        if inputs.is_empty() {
            return Ok(Laked::NotNow);
        }
        if !self
            .parts
            .inputs
            .begin(dataset, inputs.iter().map(|f| f.path.clone()))
        {
            // TTL 正在判定该数据集：下一轮再来
            return Ok(Laked::NotNow);
        }
        let _registered = Registered {
            inputs: &self.parts.inputs,
            dataset,
        };
        // 登记之前读到的快照可能已被 TTL 的热层提交越过：登记之后入湖输入须仍全部存活，否则本轮放弃
        // （TTL 与入湖确定各自输入在进程内互斥，REQ-AMB-STO-TTL-V2）
        if let Some(latest) = chain.latest_snapshot(dataset)
            && latest.snapshot_id != snapshot.snapshot_id
        {
            let alive: HashSet<String> = match latest.buckets.iter().find(|b| b.bucket == label) {
                None => HashSet::new(),
                Some(r) => chain
                    .load_bucket_manifest(dataset, &latest, r)
                    .await
                    .map_err(|e| chain_halt(&e))?
                    .files
                    .iter()
                    .map(|f| f.path.clone())
                    .collect(),
            };
            if !inputs.iter().all(|f| alive.contains(&f.path)) {
                return Ok(Laked::NotNow);
            }
        }

        // ① 核对全部入湖输入的摘要
        let store = &chain.store;
        let mut bytes: Vec<Bytes> = Vec::with_capacity(inputs.len());
        let (mut mismatched, mut missing) = (Vec::new(), Vec::new());
        for f in &inputs {
            match store.read(&store.object_path(dataset, &f.path)).await {
                Err(_) => return Err(Skip::Unavailable),
                Ok(None) => missing.push(f.path.clone()),
                Ok(Some(got)) => {
                    if Digest::of(&got.bytes) != f.content_digest {
                        mismatched.push(f.path.clone());
                        if self.parts.reports.first(dataset, &f.path) {
                            self.parts.events.digest_mismatch(dataset, &f.path);
                        }
                    }
                    bytes.push(got.bytes);
                }
            }
        }
        if !mismatched.is_empty() || !missing.is_empty() {
            self.state(dataset, |s| s.stopped.insert(label));
            return Ok(Laked::Stopped {
                mismatched,
                missing,
            });
        }
        fault::pause("lake.after_digest_check");
        fault::gate("lake.after_digest_check").await;
        if self.stopping() {
            return Err(Skip::Other);
        }

        // ② 重写并以一次冷层提交加入冷层表
        let draft = match self
            .parts
            .writer
            .append(
                dataset,
                ColdAppend {
                    bucket: hour,
                    hot_snapshot_id: snapshot.snapshot_id,
                    schema: snapshot.schema.clone(),
                    inputs: bytes,
                    bloom: bloom::bloom_columns(&self.parts.hints, dataset, &snapshot.schema),
                },
            )
            .await
        {
            Ok(draft) => draft,
            // 不可访问：下一轮以同一请求续做（冷层写入按请求认同一次提交）；提交结果不明而其实已写成的，按交接窗口补做
            Err(ColdWriteError::Unavailable(_)) => {
                self.state(dataset, |s| s.window_clear = false);
                return Err(Skip::Unavailable);
            }
            // 重写失败：本进程不再为该桶入湖，更晚的桶照常（-REWRITE-STOP）；停机过程中的不算
            Err(ColdWriteError::Rewrite(reason)) if !self.stopping() => {
                if self.state(dataset, |s| s.stopped.insert(label.clone())) {
                    self.parts.events.rewrite_failed(dataset, &label, &reason);
                }
                return Err(Skip::Other);
            }
            Err(_) => return Err(Skip::Other),
        };
        fault::pause("lake.after_cold_commit");
        fault::gate("lake.after_cold_commit").await;

        // ③ 入湖交接提交
        Ok(
            if self
                .handover(dataset, hour, snapshot.snapshot_id, draft)
                .await?
            {
                Laked::Handed
            } else {
                Laked::NotNow
            },
        )
    }
}

/// 入湖结束（交接提交成功或放弃）时注销进行中的入湖输入。
struct Registered<'a> {
    inputs: &'a LakeInputs,
    dataset: &'a Dataset,
}

impl Drop for Registered<'_> {
    fn drop(&mut self) {
        self.inputs.end(self.dataset);
    }
}
