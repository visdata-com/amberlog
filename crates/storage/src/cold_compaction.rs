//! 冷层合并（REQ-AMB-STO-COLD-COMPACTION）：入湖之后，把冷层同一日分区的小文件合并成较大的文件，行集不变，被合并的
//! 文件留痕。编排归桶生命周期，提交经冷层写入。
//!
//! 桶生命周期的一项周期工作（[`BucketTask`]），每轮（不长于 30 秒，也不长于 `cold_compaction.check_interval`）对每个存在
//! 且正常、已有冷层表的数据集：
//! 1. 本进程对该数据集还没确认过「没有冷层变更段尚未写出的冷层合并提交」时（开始受理之后的第一轮，或此后某次写段没有
//!    成功），读冷层当前版本，按 [`ColdTable::untraced`] 找出这些 `compaction` 快照，依次补写冷层变更段
//!    （-RECOVER、-RETRY）：被移除的文件取其父快照与它的存活文件之差，合并记录取它新增的文件，摘要取 `amberlog.file_digests`，
//!    不读合并产出。补写没有全部成功时不为该数据集开始新的合并。
//! 2. 到了检查时刻（开始受理后第一轮，此后每隔 `cold_compaction.check_interval`；-CHECK）时，对各够格的日分区判合并候选
//!    文件与合并条件（-TERM-*），对满足的分区依次合并一次：读出全部合并输入并核对写入时摘要（不符各报一次 `digest_mismatch`，
//!    `tier` 为 `cold`，该分区本进程不再合并）→ [`ColdUpkeep::replace`]（重写只用核对过的字节，提交前按所基于的版本重核）→
//!    冷层变更段提交（`deletions` 记各输入、`reason` 为 `compaction`，`additions` 记合并记录）。物理删除由 [`crate::purge`]
//!    按冷层变更段与 `storage.delete_delay` 延后进行（-PHYS-DELETE）。
//!
//! 为判定所作的热层读取（-HOT-READ）：热层当前快照与其 `buckets` 引用的桶 manifest 走读坏即停用的读法；留痕归档段、冷层
//! 变更段与留痕归档段引用的桶 manifest 走不改闸门的读法，读不到或无法解析只放弃本次合并，并经
//! [`ColdCompactionEvents::abandoned`] 输出一行非稳定的 warn 日志。冷层对象读到不存在或无法解析时按冷层表块停用，合并
//! 输入读到不存在而重新确定的冷层当前版本中它已不存活时只放弃（-READ-MOVED）。对象存储不可访问只放弃本次（-INACCESSIBLE）。
//! 停用的数据集不合并；停机过程开始后不开始新的合并。
//! 每次合并结束后，分配器的空闲内存多就还给系统（[`memory::release_if_much_free`]）。

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use amberlog_common::dataset::Dataset;
use bytes::Bytes;

use crate::BoxFuture;
use crate::bloom::{self, IndexHints};
use crate::chain::{
    BucketRef, ChainError, ChainReader, ColdDeletionReason, ColdFileEntry, Digest, FileEntry,
    SegmentReader, Snapshot,
};
use crate::cold::{
    self, ColdDataFile, ColdOperation, ColdReadError, ColdReader, ColdReplace, ColdScope,
    ColdSnapshot, ColdState, ColdTable, ColdUpkeep, ColdWriteError,
};
use crate::commit::{ColdChangeRequest, Committed, HotCommits};
use crate::committer::CommitError;
use crate::fault;
use crate::gate::{DatasetGates, DatasetState};
use crate::in_use::SnapshotPin;
use crate::layout::HourBucket;
use crate::lifecycle::{self, Lifecycle};
use crate::maintenance::BucketTask;
use crate::memory;
use crate::registry::{DatasetRegistry, Persistence};
use crate::schema::SchemaColumn;
use crate::ttl::CommitTally;

const DAY_US: i64 = 86_400_000_000;
const HOUR_US: i64 = 3_600_000_000;

/// 一轮的最长间隔：补写须在开始受理起 120 秒内、运行中写段失败后的下一次检查开始起 120 秒内完成。
const MAX_TICK: Duration = Duration::from_secs(30);

/// 冷层合并的配置。
#[derive(Debug, Clone, Copy)]
pub struct ColdCompactionConfig {
    /// 判够格的日分区（`lifecycle.lake_watermark`）。
    pub lifecycle: Lifecycle,
    /// `cold_compaction.check_interval`
    pub check_interval: Duration,
    /// `compaction.target_file_bytes`
    pub target_file_bytes: u64,
    /// `retention.period`；不配为 `None`。
    pub retention: Option<Duration>,
}

/// 冷层合并的可观察结果，由调用方接到日志。
pub trait ColdCompactionEvents: Send + Sync {
    /// 合并输入完整字节的摘要与其写入时摘要不符（本进程对同一数据集同一冷层路径至多一次）：稳定事件 `digest_mismatch`，
    /// `tier` 为 `cold`，`path` 相对冷层表位置。
    fn digest_mismatch(&self, dataset: &Dataset, path: &str);
    /// 因所读留痕归档段、冷层变更段或留痕归档段引用的桶 manifest 不存在或无法解析而放弃本次合并：一行非稳定 warn 日志。
    fn abandoned(&self, dataset: &Dataset, reason: &str);
}

/// 冷层合并用到的其他元素。
pub struct ColdCompactionDeps {
    pub registry: Arc<DatasetRegistry>,
    pub gates: Arc<DatasetGates>,
    /// 热层当前快照与其 `buckets` 引用的桶 manifest：读坏即停用（生产为 [`crate::reader::StoreChain`]）。
    pub chain: Arc<dyn ChainReader>,
    /// 留痕归档段引用的桶 manifest：不改闸门（生产为 [`crate::ttl::QuietChain`]）。
    pub quiet: Arc<dyn ChainReader>,
    pub segments: Arc<dyn SegmentReader>,
    pub commits: Arc<dyn HotCommits>,
    pub tally: Arc<dyn CommitTally>,
    pub cold_reader: Arc<dyn ColdReader>,
    pub upkeep: Arc<dyn ColdUpkeep>,
    pub events: Arc<dyn ColdCompactionEvents>,
    /// 本进程启动时读入的索引提示：合并产出按它写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM-WRITE-OUTPUTS ③）。
    pub hints: Arc<IndexHints>,
}

/// 一个数据集在本进程中的冷层合并情况。
#[derive(Default)]
struct PerDataset {
    /// 已确认（开始受理之后、且其后的写段都成功）没有冷层变更段尚未写出的冷层合并提交。
    traced: bool,
    /// 因摘要不符停止合并的日分区（-DIGEST-STOP）。
    stopped: HashSet<i32>,
    /// 已输出过 `digest_mismatch` 的冷层路径（-DIGEST-EVENT）。
    reported: HashSet<String>,
}

/// 本次放弃（原因已处置）。
struct Skip;

/// 已记入冷层文件集中的一个文件，与记载其写入的是交接记录还是合并记录。
#[derive(Debug, Clone)]
struct Recorded {
    entry: ColdFileEntry,
    merged: bool,
}

/// 冷层合并的驱动。
pub struct ColdCompaction {
    deps: ColdCompactionDeps,
    config: ColdCompactionConfig,
    state: Mutex<HashMap<Dataset, PerDataset>>,
    /// 下一次检查的时刻；`None` 即开始受理之后的第一轮就检查。
    next_check: Mutex<Option<Instant>>,
    stopping: AtomicBool,
}

impl BucketTask for ColdCompaction {
    fn period(&self) -> Duration {
        self.config.check_interval.min(MAX_TICK)
    }

    fn first_delay(&self) -> Duration {
        Duration::from_secs(2).min(self.period())
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(self.round())
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

impl ColdCompaction {
    pub fn new(deps: ColdCompactionDeps, config: ColdCompactionConfig) -> ColdCompaction {
        ColdCompaction {
            deps,
            config,
            state: Mutex::default(),
            next_check: Mutex::default(),
            stopping: AtomicBool::new(false),
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
    }

    fn normal(&self, dataset: &Dataset) -> bool {
        self.deps.gates.state(dataset) == DatasetState::Normal
    }

    fn disable(&self, dataset: &Dataset) {
        self.deps
            .gates
            .enter(dataset, DatasetState::ManifestUnreadable);
    }

    fn state<R>(&self, dataset: &Dataset, f: impl FnOnce(&mut PerDataset) -> R) -> R {
        let mut all = self.state.lock().expect("冷层合并状态锁未中毒");
        f(all.entry(dataset.clone()).or_default())
    }

    /// 计入一次热层提交的结果，返回是否成功。
    fn tally(&self, dataset: &Dataset, result: &Result<Committed, CommitError>) -> bool {
        let tally = &self.deps.tally;
        match result {
            Ok(_) => tally.ok(dataset),
            Err(CommitError::Exhausted | CommitError::Disabled(_)) => tally.failed(dataset),
            Err(CommitError::Unavailable) => tally.unavailable(dataset),
            Err(CommitError::Stale | CommitError::Wal(_)) => {}
        }
        result.is_ok()
    }

    /// 本轮是否检查；是则定下下一次检查的时刻（自本次检查开始起算）。
    fn check_due(&self) -> bool {
        let mut next = self.next_check.lock().expect("检查时刻锁未中毒");
        let now = Instant::now();
        if next.is_some_and(|t| now < t) {
            return false;
        }
        *next = Some(now + self.config.check_interval);
        true
    }

    async fn round(&self) {
        let due = self.check_due();
        if due {
            fault::gate("cold_compaction.before_run").await;
        }
        for dataset in self.deps.registry.datasets() {
            if self.stopping() {
                return;
            }
            if self.deps.registry.persistence(&dataset) != Some(Persistence::Stored)
                || !self.normal(&dataset)
            {
                continue;
            }
            if !self.state(&dataset, |s| s.traced) && self.recover(&dataset).await.is_err() {
                continue;
            }
            if due {
                let _ = self.check(&dataset).await;
            }
        }
    }

    /// 冷层读取失败：不存在或内容无法解析使数据集停用（REQ-AMB-STO-COLD-TABLE-V2），不可访问只放弃。
    fn cold_read_failed(&self, dataset: &Dataset, e: ColdReadError) -> Skip {
        if !matches!(e, ColdReadError::Unavailable(_)) {
            self.disable(dataset);
        }
        Skip
    }

    /// 热层当前快照（找链尾、读快照；读坏即停用）与它在在用快照登记里的登记：调用方持有登记，冷层合并持有到
    /// 冷层变更段提交写成或放弃为止（REQ-AMB-STO-SNAPSHOT-GC-IN-USE）。尚无快照为 `None`。
    async fn hot_current(
        &self,
        dataset: &Dataset,
    ) -> Result<Option<(Arc<Snapshot>, SnapshotPin)>, Skip> {
        let chain = &self.deps.chain;
        let mut pin = chain.reading(dataset);
        let Some(id) = chain.current(dataset).await.map_err(|_| Skip)? else {
            return Ok(None);
        };
        pin.settle(id);
        Ok(Some((
            chain.snapshot(dataset, id).await.map_err(|_| Skip)?,
            pin,
        )))
    }

    /// 冷层当前版本（全部清单）。尚无冷层表而热层记有冷层数据时停用（冷层表整体消失）。
    async fn cold_current(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
    ) -> Result<Option<Arc<ColdTable>>, Skip> {
        match self
            .deps
            .cold_reader
            .table(dataset, ColdScope::All)
            .await
            .map_err(|e| self.cold_read_failed(dataset, e))?
        {
            ColdState::Table(t) => Ok(Some(t)),
            ColdState::NoTable if snapshot.summary.cold.record_count > 0 => {
                self.disable(dataset);
                Err(Skip)
            }
            ColdState::NoTable => Ok(None),
        }
    }

    /// 读 `snapshot` 经 `cold_deletion_segment` 可达的冷层变更段（不改闸门；读不到或无法解析放弃并输出 warn 日志）。
    async fn change_segments(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
    ) -> Result<Vec<Arc<crate::chain::ColdChangeSegment>>, Skip> {
        match self
            .deps
            .segments
            .cold_change_segments(dataset, snapshot)
            .await
        {
            Ok(s) => Ok(s),
            Err(ChainError::Unavailable(_)) => Err(Skip),
            Err(e) => {
                self.deps.events.abandoned(
                    dataset,
                    &format!("cold change segment cannot be read: {e:?}"),
                );
                Err(Skip)
            }
        }
    }

    /// 补写冷层变更段尚未写出的冷层合并提交的冷层变更段（-RECOVER、-RETRY）。全部写出（或没有）时记下已确认。
    async fn recover(&self, dataset: &Dataset) -> Result<(), Skip> {
        let Some((snapshot, _pin)) = self.hot_current(dataset).await? else {
            self.state(dataset, |s| s.traced = true);
            return Ok(());
        };
        let Some(table) = self.cold_current(dataset, &snapshot).await? else {
            self.state(dataset, |s| s.traced = true);
            return Ok(());
        };
        let pending: Vec<ColdSnapshot> = {
            let has_compaction = table
                .snapshots
                .iter()
                .any(|s| matches!(s.operation, ColdOperation::Compaction { .. }));
            if has_compaction {
                let segments = self.change_segments(dataset, &snapshot).await?;
                let traced = cold::traced(&segments);
                table
                    .untraced(&traced)
                    .into_iter()
                    .filter(|s| matches!(s.operation, ColdOperation::Compaction { .. }))
                    .cloned()
                    .collect()
            } else {
                Vec::new()
            }
        };
        for c in &pending {
            if self.stopping() {
                return Err(Skip);
            }
            self.trace_again(dataset, &table, c).await?;
        }
        self.state(dataset, |s| s.traced = true);
        Ok(())
    }

    /// 按冷层合并提交 `c` 补写一个冷层变更段。
    async fn trace_again(
        &self,
        dataset: &Dataset,
        table: &ColdTable,
        c: &ColdSnapshot,
    ) -> Result<(), Skip> {
        match trace_request(self.deps.cold_reader.as_ref(), dataset, table, c).await {
            Ok(None) => Ok(()),
            Ok(Some(request)) => self.write_trace(dataset, request).await,
            Err(TraceError::Read(e)) => Err(self.cold_read_failed(dataset, e)),
            Err(TraceError::MissingDigest) => {
                // `amberlog.file_digests` 缺了新增的文件：写不出合并记录，按内容无法解析处置
                self.disable(dataset);
                Err(Skip)
            }
        }
    }

    /// 冷层变更段提交（`reason` 为 `compaction`）；成功之外都使下一轮先补写。
    async fn write_trace(&self, dataset: &Dataset, request: ColdChangeRequest) -> Result<(), Skip> {
        let result = self.deps.commits.commit_cold_change(dataset, request).await;
        if self.tally(dataset, &result) {
            Ok(())
        } else {
            self.state(dataset, |s| s.traced = false);
            Err(Skip)
        }
    }

    /// 一次检查：对各够格且满足合并条件的日分区依次合并一次。
    async fn check(&self, dataset: &Dataset) -> Result<(), Skip> {
        let now = lifecycle::now_us();
        let Some((snapshot, _pin)) = self.hot_current(dataset).await? else {
            return Ok(());
        };
        let Some(table) = self.cold_current(dataset, &snapshot).await? else {
            return Ok(());
        };
        // 先只按冷层清单粗筛：没有哪个够格分区有两个小于目标大小的文件，就不必读热层
        let target = self.config.target_file_bytes;
        let mut by_day: BTreeMap<i32, Vec<&ColdDataFile>> = BTreeMap::new();
        for f in table
            .files
            .iter()
            .filter(|f| f.size_bytes < target && self.eligible(f.day, now))
        {
            by_day.entry(f.day).or_default().push(f);
        }
        let stopped = self.state(dataset, |s| s.stopped.clone());
        by_day.retain(|day, files| files.len() >= 2 && !stopped.contains(day));
        if by_day.is_empty() {
            return Ok(());
        }

        let recorded = self.recorded(dataset, &snapshot).await?;
        let window = self.window_snapshots(dataset, &snapshot, &table).await?;
        let cutoff = self
            .config
            .retention
            .map(|p| now.saturating_sub(i64::try_from(p.as_micros()).unwrap_or(i64::MAX)));
        for (day, files) in by_day {
            let candidates: Vec<(&ColdDataFile, &Recorded)> = files
                .into_iter()
                .filter_map(|f| Some((f, recorded.get(&f.path)?)))
                .filter(|(f, r)| !r.merged || f.size_bytes < target / 2)
                .filter(|(f, _)| !window.contains(&f.added_snapshot_id))
                .filter(|(f, _)| cutoff.is_none_or(|c| f.max_timestamp >= c))
                .collect();
            if candidates.len() < 2 || candidates.iter().all(|(_, r)| r.merged) {
                continue;
            }
            if self.stopping() || !self.normal(dataset) {
                return Err(Skip);
            }
            let result = self
                .merge(dataset, day, &candidates, &snapshot.schema)
                .await;
            memory::release_if_much_free();
            // 某一天没有合并成，照样合并后面的日子（各日分区各自受时限约束）。停机、数据集已不正常，或提交结果不明、
            // 下一轮要先补写冷层变更段时才停下
            if let Err(skip) = result
                && (self.stopping() || !self.normal(dataset) || !self.state(dataset, |s| s.traced))
            {
                return Err(skip);
            }
        }
        Ok(())
    }

    /// 日分区 `day` 此刻是否够格：不早于该日结束时刻加入湖水位（即该日最后一个小时桶的入湖水位时刻，-TERM-ELIGIBLE）。
    fn eligible(&self, day: i32, now: i64) -> bool {
        let last_hour = HourBucket::of(i64::from(day) * DAY_US + 23 * HOUR_US);
        now >= self.config.lifecycle.lake_at_us(last_hour)
    }

    /// 热层快照 `snapshot` 的已记入冷层文件集（REQ-AMB-STO-HOT-LAYOUT）与各文件的写入时摘要来源：交接记录取自
    /// `buckets` 与留痕归档段引用的桶 manifest，自链首依次累加冷层变更段（先去掉 `deletions`，再加入 `additions`）。
    async fn recorded(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
    ) -> Result<HashMap<String, Recorded>, Skip> {
        let mut set: HashMap<String, Recorded> = HashMap::new();
        let mut add = |files: &[ColdFileEntry]| {
            for f in files {
                set.insert(
                    f.path.clone(),
                    Recorded {
                        entry: f.clone(),
                        merged: false,
                    },
                );
            }
        };
        for r in &snapshot.buckets {
            let m = self
                .deps
                .chain
                .bucket_manifest(dataset, snapshot, r)
                .await
                .map_err(|_| Skip)?;
            for h in &m.handovers {
                add(&h.files);
            }
        }
        let retired = match self.deps.segments.retired_segments(dataset, snapshot).await {
            Ok(s) => s,
            Err(ChainError::Unavailable(_)) => return Err(Skip),
            Err(e) => {
                self.deps
                    .events
                    .abandoned(dataset, &format!("retired segment cannot be read: {e:?}"));
                return Err(Skip);
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
                match self.deps.quiet.bucket_manifest(dataset, snapshot, &r).await {
                    Ok(m) => {
                        for h in &m.handovers {
                            add(&h.files);
                        }
                    }
                    Err(ChainError::Unavailable(_)) => return Err(Skip),
                    Err(e) => {
                        self.deps.events.abandoned(
                            dataset,
                            &format!("bucket manifest of a retired bucket cannot be read: {e:?}"),
                        );
                        return Err(Skip);
                    }
                }
            }
        }
        let changes = self.change_segments(dataset, snapshot).await?;
        for segment in changes.iter().rev() {
            for d in &segment.deletions {
                set.remove(&d.path);
            }
            for a in &segment.additions {
                set.insert(
                    a.path.clone(),
                    Recorded {
                        entry: a.clone(),
                        merged: true,
                    },
                );
            }
        }
        Ok(set)
    }

    /// 处于交接窗口的冷层提交的 Iceberg 快照号（读热层当前快照中各 `lake` 提交所记桶的桶 manifest，读坏即停用）。
    async fn window_snapshots(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
        table: &ColdTable,
    ) -> Result<BTreeSet<i64>, Skip> {
        let mut live: BTreeMap<HourBucket, Vec<FileEntry>> = BTreeMap::new();
        for hour in table.lake_buckets() {
            let label = hour.to_string();
            let Some(r) = snapshot.buckets.iter().find(|b| b.bucket == label) else {
                continue;
            };
            if r.record_count == 0 {
                continue;
            }
            let m = self
                .deps
                .chain
                .bucket_manifest(dataset, snapshot, r)
                .await
                .map_err(|_| Skip)?;
            live.insert(hour, m.files.clone());
        }
        Ok(table
            .handover_window(&live)
            .into_iter()
            .map(|w| w.iceberg_snapshot_id)
            .collect())
    }

    /// 对日分区 `day` 合并一次，合并输入为 `inputs`。
    async fn merge(
        &self,
        dataset: &Dataset,
        day: i32,
        inputs: &[(&ColdDataFile, &Recorded)],
        hot: &[SchemaColumn],
    ) -> Result<(), Skip> {
        fault::gate("cold_compaction.after_select").await;
        // ① 读出全部合并输入并核对写入时摘要（全部都核对，不在首个不符处停止）
        let reader = &self.deps.cold_reader;
        let mut bytes: Vec<(String, Bytes)> = Vec::with_capacity(inputs.len());
        let mut mismatched = Vec::new();
        for (f, r) in inputs {
            match reader.file_bytes(dataset, &f.path).await {
                Ok(b) => {
                    if Digest::of(&b) != r.entry.content_digest {
                        mismatched.push(f.path.clone());
                    }
                    bytes.push((f.path.clone(), b));
                }
                Err(ColdReadError::NotFound(_)) => return Err(self.input_gone(dataset, f).await),
                Err(e) => return Err(self.cold_read_failed(dataset, e)),
            }
        }
        if !mismatched.is_empty() {
            for path in &mismatched {
                if self.state(dataset, |s| s.reported.insert(path.clone())) {
                    self.deps.events.digest_mismatch(dataset, path);
                }
            }
            self.state(dataset, |s| s.stopped.insert(day));
            return Err(Skip);
        }
        fault::gate("cold_compaction.after_digest_check").await;
        if self.stopping() {
            return Err(Skip);
        }

        // ② ③ 重写、上传并以一次冷层合并提交换下全部输入
        let paths: Vec<String> = bytes.iter().map(|(p, _)| p.clone()).collect();
        let replaced = match self
            .deps
            .upkeep
            .replace(
                dataset,
                ColdReplace {
                    day,
                    inputs: bytes,
                    bloom: bloom::bloom_columns(&self.deps.hints, dataset, hot),
                },
            )
            .await
        {
            Ok(r) => r,
            Err(ColdWriteError::Unavailable(_)) => {
                // 提交结果可能不明：下一轮先按冷层当前版本补写
                self.state(dataset, |s| s.traced = false);
                return Err(Skip);
            }
            Err(_) => return Err(Skip),
        };
        fault::pause("cold_compaction.after_cold_commit");
        fault::gate("cold_compaction.after_cold_commit").await;

        // ④ 冷层变更段（合并输入与合并记录）
        let mut additions = replaced.outputs;
        additions.sort_by(|a, b| a.path.cmp(&b.path));
        self.write_trace(
            dataset,
            ColdChangeRequest {
                cold_removed_in: replaced.iceberg_snapshot_id,
                reason: ColdDeletionReason::Compaction,
                cutoff: None,
                paths,
                additions,
            },
        )
        .await
    }

    /// 合并输入读到不存在：重新确定的冷层当前版本中它已不存活即只放弃（-READ-MOVED），否则按冷层对象读到不存在停用。
    async fn input_gone(&self, dataset: &Dataset, f: &ColdDataFile) -> Skip {
        let day = f.day;
        match self
            .deps
            .cold_reader
            .table(dataset, ColdScope::Days(day..=day))
            .await
        {
            Ok(ColdState::Table(t)) if t.files.iter().any(|x| x.path == f.path) => {
                self.disable(dataset);
                Skip
            }
            Ok(_) => Skip,
            Err(e) => self.cold_read_failed(dataset, e),
        }
    }
}

/// [`trace_request`] 写不出请求的原因。
#[derive(Debug)]
pub enum TraceError {
    /// 读冷层快照的文件失败。
    Read(ColdReadError),
    /// `amberlog.file_digests` 缺了该次合并新增的文件（按内容无法解析处置）。
    MissingDigest,
}

/// 冷层合并提交 `c` 的冷层变更段提交请求（合并记录的补写，REQ-AMB-STO-COLD-COMPACTION；优雅退役
/// REQ-AMB-LIFE-RETIREMENT-RECOVER-TRACE 同用）：移除的文件为其父快照与它的存活文件之差，合并记录取它新增的文件与
/// `amberlog.file_digests`。不是冷层合并提交、没有父快照或没有移除的文件时为 `Ok(None)`。
pub async fn trace_request(
    reader: &dyn ColdReader,
    dataset: &Dataset,
    table: &ColdTable,
    c: &ColdSnapshot,
) -> Result<Option<ColdChangeRequest>, TraceError> {
    let ColdOperation::Compaction { file_digests } = &c.operation else {
        return Ok(None);
    };
    let Some(parent) = c.parent_snapshot_id else {
        return Ok(None);
    };
    let before = reader
        .snapshot_files(dataset, table, parent)
        .await
        .map_err(TraceError::Read)?;
    let after = reader
        .snapshot_files(dataset, table, c.snapshot_id)
        .await
        .map_err(TraceError::Read)?;
    let kept: HashSet<&str> = after.iter().map(|f| f.path.as_str()).collect();
    let mut removed: Vec<String> = before
        .iter()
        .filter(|f| !kept.contains(f.path.as_str()))
        .map(|f| f.path.clone())
        .collect();
    removed.sort();
    let mut additions = Vec::new();
    for f in after
        .iter()
        .filter(|f| f.added_snapshot_id == c.snapshot_id)
    {
        let Some(digest) = file_digests.get(&f.path) else {
            return Err(TraceError::MissingDigest);
        };
        additions.push(merge_record(f, *digest));
    }
    additions.sort_by(|a, b| a.path.cmp(&b.path));
    if removed.is_empty() {
        return Ok(None);
    }
    Ok(Some(ColdChangeRequest {
        cold_removed_in: c.snapshot_id,
        reason: ColdDeletionReason::Compaction,
        cutoff: None,
        paths: removed,
        additions,
    }))
}

/// 冷层数据文件 `f` 的合并记录（六个字段，REQ-AMB-STO-COLD-COMPACTION-TRACE-VALUES）。
fn merge_record(f: &ColdDataFile, digest: Digest) -> ColdFileEntry {
    ColdFileEntry {
        path: f.path.clone(),
        min_timestamp: f.min_timestamp,
        max_timestamp: f.max_timestamp,
        record_count: f.record_count,
        size_bytes: f.size_bytes,
        content_digest: digest,
    }
}
