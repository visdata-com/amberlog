//! TTL 到期删除（REQ-AMB-STO-TTL）：桶生命周期的一项周期工作。
//!
//! - 配置了 `retention.period` 时才启动，周期为 `retention.check_interval`；第一轮在开始受理 `/api/` 请求时立即开始，
//!   以满足重启后 120 秒内补写冷层变更段（COLD-TRACE-RECOVER）。每轮先过暂停点 `ttl.before_run`；停机过程开始后不再
//!   开始新的执行，进行中的由 [`crate::maintenance::BucketTasks::stop`] 中止。
//! - 对每个存在且未停用的数据集做一次执行：`cutoff` 为执行开始时 [`lifecycle::now_us`] 减去保留期。一个数据文件全部记录
//!   都早于 `cutoff` 才到期（热层看 `files` 条目的 `max_timestamp`，冷层看清单条目中 `_timestamp` 的上界）。
//! - 先读后删：按找链尾所得的当前快照、冷层当前版本（全部清单）、`time_min` 早于 `cutoff` 的桶与判定交接窗口所需的桶的
//!   桶 manifest；冷层当前版本有 `ttl` 或 `compaction` 冷层提交时另读冷层变更段链。任一读取未成功，或得到「尚无冷层表」而当前快照
//!   `summary.cold.record_count` 大于 0，本次对该数据集什么都不移走（INCOMPLETE）。这些读取都不改变数据集状态
//!   （NO-DISABLE；热层经 [`QuietChain`]，段链与冷层读取本就不改）。
//! - 冷层当前版本中有尚未记入冷层变更段的 `ttl` 冷层提交时，先逐个补写（`cutoff` 取其 `amberlog.ttl_cutoff`，移除的文件
//!   为其父快照与它的存活文件之差），补写都成功之后重读再判定；补写未成功则本次不移走任何文件（COLD-TRACE-RETRY、
//!   COLD-TRACE-RECOVER 共用这段）。
//! - 避让入湖（LAKE-GUARD）：进行中入湖已确定的输入（[`LakeInputs::contains`]）、处于交接窗口的冷层提交的入湖输入
//!   （[`ColdTable::handover_window`]）不移走，该冷层提交新增的冷层文件（`added_snapshot_id` 等于其快照号）也不移走。
//!   热层判定之后过暂停点 `ttl.before_hot_commit`，再持有该数据集的 [`crate::maintenance::TtlGuard`] 直到 TTL 热层提交
//!   返回（其间不开始新的入湖），持有后重读、核对避让（见 `Ttl::commit_hot`）。
//! - 避让冷层合并（REQ-AMB-STO-TTL-V2 第 23 条）：合并记录尚未写出的 `compaction` 冷层提交（[`ColdTable::untraced`]，
//!   与冷层合并的补写、冷层保留同一判定）新增的合并产出不移走。冷层合并先提交、TTL 后提交时，[`ColdWriter::delete`]
//!   按所基于的冷层当前版本重核，已被换下的文件不再移出（第 24 条）。
//! - 依次做 TTL 热层提交、TTL 冷层提交（[`ColdWriter::delete`]）与冷层变更段提交，某层没有要移走的文件时该层的提交不写，
//!   前一提交未成功时不做其后的（ORDER、NO-EMPTY-HOT、NO-EMPTY-COLD）。TTL 热层提交由快照提交按父快照重核，只移出仍存活的，
//!   一个都不存活时不写。冷层变更段提交未成功时由下一次执行先补写。
//! - 冷层删除提交返回不可访问（结果不明）时不保留该请求：下一次执行读到的冷层当前版本即判明它是否已提交，已提交的按上面
//!   补写冷层变更段，未提交的按新的判定重做。

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::{
    BucketManifest, BucketRef, ChainError, ChainReader, ColdDeletionReason, FileEntry,
    SegmentReader, Snapshot, SnapshotId,
};
use crate::cold::{
    self, ColdDelete, ColdOperation, ColdReadError, ColdReader, ColdScope, ColdState, ColdTable,
    ColdWriter, WindowCommit,
};
use crate::commit::{ColdChangeRequest, Committed, HotCommits, TtlHotRequest};
use crate::committer::CommitError;
use crate::fault;
use crate::flusher::Flusher;
use crate::format::{self, SnapshotContent};
use crate::gate::{DatasetGates, DatasetState};
use crate::in_use::SnapshotPin;
use crate::layout::{self, HourBucket};
use crate::lifecycle;
use crate::maintenance::{BucketTask, LakeInputs};
use crate::reader::StoreChain;
use crate::registry::{DatasetRegistry, Persistence};

/// TTL 的配置。
#[derive(Debug, Clone, Copy)]
pub struct TtlConfig {
    /// `retention.period`
    pub retention: Duration,
    /// `retention.check_interval`（大于 0）
    pub check_interval: Duration,
}

/// TTL 用到的其他元素。
pub struct TtlDeps {
    pub registry: Arc<DatasetRegistry>,
    pub gates: Arc<DatasetGates>,
    /// 热层快照与桶 manifest 的读取：须不改变数据集状态（生产用 [`QuietChain`]）。
    pub chain: Arc<dyn ChainReader>,
    pub segments: Arc<dyn SegmentReader>,
    pub commits: Arc<dyn HotCommits>,
    pub cold_reader: Arc<dyn ColdReader>,
    pub cold_writer: Arc<dyn ColdWriter>,
    pub lake_inputs: Arc<LakeInputs>,
    /// TTL 热层提交与冷层变更段提交结果的计数（生产为 [`Flusher`]）。
    pub tally: Arc<dyn CommitTally>,
}

/// 提交结果计入 `amberlog_manifest_commits_total`（REQ-AMB-OBS-HEALTH-METRICS-V4）：成功计 `ok`，重试耗尽或数据集停用计
/// `failed`，一轮因对象存储不可访问而未能完成时就地计一次 `failed`（与按周期的计数互不抵充）；`Stale` 与 WAL 写不进不计。
pub trait CommitTally: Send + Sync {
    fn ok(&self, dataset: &Dataset);
    fn failed(&self, dataset: &Dataset);
    fn unavailable(&self, dataset: &Dataset);
}

impl CommitTally for Flusher {
    fn ok(&self, dataset: &Dataset) {
        self.record_ok(dataset);
    }

    fn failed(&self, dataset: &Dataset) {
        self.record_failed(dataset);
    }

    fn unavailable(&self, dataset: &Dataset) {
        self.record_background_unavailable(dataset);
    }
}

/// TTL 的驱动。
pub struct Ttl {
    deps: TtlDeps,
    config: TtlConfig,
    stopping: AtomicBool,
}

impl BucketTask for Ttl {
    fn period(&self) -> Duration {
        self.config.check_interval
    }

    fn first_delay(&self) -> Duration {
        Duration::ZERO
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(Ttl::run_once(self))
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

/// 一次执行为判定所读到的。
struct View {
    /// 冷层当前版本；尚无冷层表（且热层未记入任何冷层文件）为 `None`。
    table: Option<Arc<ColdTable>>,
    /// `time_min` 早于 `cutoff` 的桶与判定交接窗口所需的桶的桶 manifest。
    manifests: BTreeMap<HourBucket, Arc<BucketManifest>>,
    /// 冷层当前版本中尚未记入冷层变更段的 `ttl` 冷层提交：(Iceberg 快照号, 父快照号, `amberlog.ttl_cutoff`)。
    unrecorded: Vec<(i64, Option<i64>, i64)>,
    /// 冷层当前版本中合并记录尚未写出的 `compaction` 冷层提交的快照号：其合并产出不移走（REQ-AMB-STO-TTL-V2 第 23 条）。
    merging: HashSet<i64>,
    /// 所据热层快照的在用登记（REQ-AMB-STO-SNAPSHOT-GC-IN-USE），随本次判定放下。
    _pin: SnapshotPin,
}

impl View {
    /// 处于交接窗口的冷层提交（按本次读到的冷层当前版本与桶 manifest）。
    fn window(&self) -> Vec<WindowCommit> {
        let Some(table) = &self.table else {
            return Vec::new();
        };
        let live: BTreeMap<HourBucket, Vec<FileEntry>> = table
            .lake_buckets()
            .into_iter()
            .filter_map(|b| self.manifests.get(&b).map(|m| (b, m.files.clone())))
            .collect();
        table.handover_window(&live)
    }

    /// 到期、且不是 `window` 中冷层提交之入湖输入的热层文件。
    fn expired_hot(&self, cutoff: i64, window: &[WindowCommit]) -> Vec<(HourBucket, String)> {
        let inputs: HashSet<&str> = window
            .iter()
            .flat_map(|w| w.inputs.iter().map(String::as_str))
            .collect();
        self.manifests
            .iter()
            .flat_map(|(hour, m)| m.files.iter().map(move |f| (*hour, f)))
            .filter(|(_, f)| f.max_timestamp < cutoff && !inputs.contains(f.path.as_str()))
            .map(|(hour, f)| (hour, f.path.clone()))
            .collect()
    }
}

/// 补写冷层变更段的结果。
enum Recovery {
    /// 没有要补写的。
    Nothing,
    /// 都已补写（其 Iceberg 快照号），须重读再判定。
    Done(HashSet<i64>),
    /// 补写未成功：本次不移走任何文件。
    Failed,
}

impl Ttl {
    pub fn new(deps: TtlDeps, config: TtlConfig) -> Ttl {
        Ttl {
            deps,
            config,
            stopping: AtomicBool::new(false),
        }
    }

    fn stopping(&self) -> bool {
        self.stopping.load(Ordering::SeqCst)
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

    /// 对每个存在且未停用的数据集执行一次。
    pub async fn run_once(&self) {
        fault::gate("ttl.before_run").await;
        for dataset in self.deps.registry.datasets() {
            if self.stopping() {
                return;
            }
            if self.deps.registry.persistence(&dataset) != Some(Persistence::Stored)
                || self.deps.gates.state(&dataset) != DatasetState::Normal
            {
                continue;
            }
            self.run_dataset(&dataset).await;
        }
    }

    fn period_us(&self) -> i64 {
        i64::try_from(self.config.retention.as_micros()).unwrap_or(i64::MAX)
    }

    /// 对数据集 `dataset` 执行一次。
    async fn run_dataset(&self, dataset: &Dataset) {
        let cutoff = lifecycle::now_us().saturating_sub(self.period_us());
        let Some(mut view) = self.read(dataset, cutoff).await else {
            return;
        };
        match self.recover(dataset, &view).await {
            Recovery::Nothing => {}
            Recovery::Failed => return,
            Recovery::Done(recovered) => {
                let Some(mut again) = self.read(dataset, cutoff).await else {
                    return;
                };
                // 刚补写的按已记入算（重读到的快照可能还早于补写的那次提交）
                again
                    .unrecorded
                    .retain(|(id, _, _)| !recovered.contains(id));
                if !again.unrecorded.is_empty() {
                    return;
                }
                view = again;
            }
        }
        if self.stopping() {
            return;
        }

        let window = view.window();
        let expired_hot = view.expired_hot(cutoff, &window);
        let window_commits: HashSet<i64> = window.iter().map(|w| w.iceberg_snapshot_id).collect();
        let expired_cold: Vec<String> = view
            .table
            .iter()
            .flat_map(|t| t.files.iter())
            .filter(|f| {
                f.max_timestamp < cutoff
                    && !window_commits.contains(&f.added_snapshot_id)
                    && !view.merging.contains(&f.added_snapshot_id)
            })
            .map(|f| f.path.clone())
            .collect();

        if !expired_hot.is_empty() && !self.commit_hot(dataset, cutoff, expired_hot).await {
            return;
        }
        if expired_cold.is_empty() || self.stopping() {
            return;
        }
        let deleted = match self
            .deps
            .cold_writer
            .delete(
                dataset,
                ColdDelete {
                    cutoff,
                    paths: expired_cold,
                },
            )
            .await
        {
            Ok(deleted) => deleted,
            Err(_) => return,
        };
        fault::pause("ttl.after_cold_commit");
        fault::gate("ttl.after_cold_commit").await;
        // 未成功时由下一次执行在移走任何文件之前补写
        let result = self
            .deps
            .commits
            .commit_cold_change(
                dataset,
                ColdChangeRequest {
                    cold_removed_in: deleted.iceberg_snapshot_id,
                    reason: ColdDeletionReason::Ttl,
                    cutoff: Some(cutoff),
                    paths: deleted.removed,
                    additions: Vec::new(),
                },
            )
            .await;
        self.tally(dataset, &result);
    }

    /// TTL 热层提交：`judged` 为本次判为到期、不在交接窗口中的热层文件。返回是否可以进行其后的提交（提交成功，或按父快照
    /// 重核后没有要移出的文件而不写）。
    ///
    /// 自核对避让起持有该数据集的入湖守卫，到热层提交返回为止，其间不开始新的入湖（TTL 确定要移出的文件与
    /// 入湖确定入湖输入在进程内互斥）。持有守卫之后重读一次再核对：判定之后、持有守卫之前完成冷层提交的入湖，其交接窗口
    /// 只在重读中可见。只移出两次都判为到期、且不是进行中入湖输入的文件；其间新出现的留给下一次执行。
    async fn commit_hot(
        &self,
        dataset: &Dataset,
        cutoff: i64,
        judged: Vec<(HourBucket, String)>,
    ) -> bool {
        fault::gate("ttl.before_hot_commit").await;
        let _guard = self.deps.lake_inputs.ttl_guard(dataset);
        let Some(view) = self.read(dataset, cutoff).await else {
            return false;
        };
        let still: HashSet<(HourBucket, String)> = view
            .expired_hot(cutoff, &view.window())
            .into_iter()
            .collect();
        let expired: Vec<(HourBucket, String)> = judged
            .into_iter()
            .filter(|e| still.contains(e) && !self.deps.lake_inputs.contains(dataset, &e.1))
            .collect();
        if expired.is_empty() {
            return true;
        }
        let result = self
            .deps
            .commits
            .commit_ttl_hot(dataset, TtlHotRequest { cutoff, expired })
            .await;
        // Stale：按父快照重核后没有要移出的文件，快照提交不写这次提交
        self.tally(dataset, &result) || matches!(result, Err(CommitError::Stale))
    }

    /// 判定所需的全部读取；任一未成功为 `None`（INCOMPLETE）。数据集尚无快照时也为 `None`。
    async fn read(&self, dataset: &Dataset, cutoff: i64) -> Option<View> {
        let chain = &self.deps.chain;
        let mut pin = chain.reading(dataset);
        let id = chain.current(dataset).await.ok()??;
        pin.settle(id);
        let snapshot = chain.snapshot(dataset, id).await.ok()?;
        let table = match self
            .deps
            .cold_reader
            .table(dataset, ColdScope::All)
            .await
            .ok()?
        {
            ColdState::Table(table) => Some(table),
            // 冷层表整体消失（REQ-AMB-STO-COLD-TABLE-TABLE-GONE）按读不全处置
            ColdState::NoTable if snapshot.summary.cold.record_count > 0 => return None,
            ColdState::NoTable => None,
        };

        let lake_buckets: HashSet<HourBucket> =
            table.iter().flat_map(|t| t.lake_buckets()).collect();
        let mut manifests = BTreeMap::new();
        for b in &snapshot.buckets {
            let Some(hour) = HourBucket::parse(&b.bucket) else {
                continue;
            };
            let expiring = b.time_min.is_some_and(|t| t < cutoff);
            if !expiring && !lake_buckets.contains(&hour) {
                continue;
            }
            let m = chain.bucket_manifest(dataset, &snapshot, b).await.ok()?;
            manifests.insert(hour, m);
        }

        // 冷层当前版本有 `ttl` 或 `compaction` 冷层提交时读冷层变更段链，按与冷层合并、冷层保留同一判定找出冷层变更段
        // 尚未写出者
        let mut unrecorded = Vec::new();
        let mut merging = HashSet::new();
        if let Some(table) = &table
            && table.snapshots.iter().any(|s| {
                matches!(
                    s.operation,
                    ColdOperation::Ttl { .. } | ColdOperation::Compaction { .. }
                )
            })
        {
            let segments = self
                .deps
                .segments
                .cold_change_segments(dataset, &snapshot)
                .await
                .ok()?;
            for s in table.untraced(&cold::traced(&segments)) {
                match s.operation {
                    ColdOperation::Ttl { cutoff } => {
                        unrecorded.push((s.snapshot_id, s.parent_snapshot_id, cutoff));
                    }
                    ColdOperation::Compaction { .. } => {
                        merging.insert(s.snapshot_id);
                    }
                    _ => {}
                }
            }
        }
        Some(View {
            table,
            manifests,
            unrecorded,
            merging,
            _pin: pin,
        })
    }

    /// 为 `view` 中尚未记入冷层变更段的 `ttl` 冷层提交依次补写冷层变更段。
    async fn recover(&self, dataset: &Dataset, view: &View) -> Recovery {
        let Some(table) = &view.table else {
            return Recovery::Nothing;
        };
        let mut done = HashSet::new();
        for &(snapshot_id, parent, cutoff) in &view.unrecorded {
            let trace = Trace {
                snapshot_id,
                parent,
                cutoff,
            };
            match write_trace(
                self.deps.cold_reader.as_ref(),
                self.deps.commits.as_ref(),
                dataset,
                table,
                trace,
            )
            .await
            {
                Ok(None) => {}
                Ok(Some(result)) => {
                    if !self.tally(dataset, &result) {
                        return Recovery::Failed;
                    }
                    done.insert(snapshot_id);
                }
                Err(_) => return Recovery::Failed,
            }
        }
        if done.is_empty() {
            Recovery::Nothing
        } else {
            Recovery::Done(done)
        }
    }
}

/// 冷层变更段尚未写出的一个 `ttl` 冷层提交。
#[derive(Debug, Clone, Copy)]
pub struct Trace {
    /// 其 Iceberg 快照号
    pub snapshot_id: i64,
    pub parent: Option<i64>,
    /// `amberlog.ttl_cutoff`
    pub cutoff: i64,
}

/// 为 `ttl` 冷层提交 `trace` 补写冷层变更段（COLD-TRACE-RETRY、-RECOVER；优雅退役 REQ-AMB-LIFE-RETIREMENT-RECOVER-TRACE 同用）：
/// 移除的文件为其父快照与它的存活文件之差，`cutoff` 取其 `amberlog.ttl_cutoff`。没有父快照或没有移除的文件时不写
/// （`Ok(None)`），否则返回冷层变更段提交的结果；读冷层快照的文件失败为 `Err`。
pub async fn write_trace(
    reader: &dyn ColdReader,
    commits: &dyn HotCommits,
    dataset: &Dataset,
    table: &ColdTable,
    trace: Trace,
) -> Result<Option<Result<Committed, CommitError>>, ColdReadError> {
    let Some(parent) = trace.parent else {
        return Ok(None);
    };
    let before = reader.snapshot_files(dataset, table, parent).await?;
    let after = reader
        .snapshot_files(dataset, table, trace.snapshot_id)
        .await?;
    let kept: HashSet<&str> = after.iter().map(|f| f.path.as_str()).collect();
    let mut removed: Vec<String> = before
        .iter()
        .filter(|f| !kept.contains(f.path.as_str()))
        .map(|f| f.path.clone())
        .collect();
    if removed.is_empty() {
        return Ok(None);
    }
    removed.sort();
    let request = ColdChangeRequest {
        cold_removed_in: trace.snapshot_id,
        reason: ColdDeletionReason::Ttl,
        cutoff: Some(trace.cutoff),
        paths: removed,
        additions: Vec::new(),
    };
    Ok(Some(commits.commit_cold_change(dataset, request).await))
}

/// 不改变数据集状态的热层读取（REQ-AMB-STO-TTL-NO-DISABLE）：找链尾、读快照与桶 manifest 时，内容无法解析、对象不存在
/// 与格式版本不兼容都只返回 [`ChainError::Integrity`]，不记入 [`DatasetGates`]；停用只由查询、提交与装载触发。
pub struct QuietChain(pub Arc<StoreChain>);

impl ChainReader for QuietChain {
    fn current<'a>(
        &'a self,
        dataset: &'a Dataset,
    ) -> BoxFuture<'a, Result<Option<SnapshotId>, ChainError>> {
        Box::pin(async move {
            let chain = &self.0;
            if chain.registry.persistence(dataset) != Some(Persistence::Stored) {
                return Ok(None);
            }
            let store = &chain.store;
            let hint = match store
                .read(&store.object_path(dataset, layout::CURRENT))
                .await
                .map_err(|e| ChainError::Unavailable(e.0))?
            {
                None => None,
                Some(f) => Some(format::decode_current(&f.bytes).map_err(|_| {
                    ChainError::Integrity(format!("{} cannot be parsed", layout::CURRENT))
                })?),
            };
            let mut current = hint;
            loop {
                let next = SnapshotId(current.map_or(1, |c| c.0 + 1));
                if !chain
                    .snapshot_exists(dataset, next)
                    .await
                    .map_err(|e| ChainError::Unavailable(e.0))?
                {
                    return Ok(current);
                }
                current = Some(next);
            }
        })
    }

    fn snapshot<'a>(
        &'a self,
        dataset: &'a Dataset,
        id: SnapshotId,
    ) -> BoxFuture<'a, Result<Arc<Snapshot>, ChainError>> {
        Box::pin(async move {
            let chain = &self.0;
            if let Some(s) = chain
                .latest_snapshot(dataset)
                .filter(|s| s.snapshot_id == id)
            {
                return Ok(s);
            }
            match chain
                .read_snapshot_raw(dataset, id)
                .await
                .map_err(|e| ChainError::Unavailable(e.0))?
            {
                Some((_, Ok(SnapshotContent::Supported(s)))) => Ok(Arc::new(*s)),
                Some((_, Ok(SnapshotContent::Unsupported { .. }))) => Err(ChainError::Integrity(
                    format!("snapshot {id} has an unsupported format_version"),
                )),
                Some((_, Err(_))) => Err(ChainError::Integrity(format!(
                    "snapshot {id} cannot be parsed"
                ))),
                None => Err(ChainError::Integrity(format!(
                    "snapshot {id} does not exist"
                ))),
            }
        })
    }

    fn bucket_manifest<'a>(
        &'a self,
        dataset: &'a Dataset,
        snapshot: &'a Snapshot,
        bucket: &'a BucketRef,
    ) -> BoxFuture<'a, Result<Arc<BucketManifest>, ChainError>> {
        Box::pin(async move {
            self.0
                .read_bucket_manifest(dataset, snapshot, bucket)
                .await?
                .map_err(ChainError::Integrity)
        })
    }

    fn reading(&self, dataset: &Dataset) -> SnapshotPin {
        self.0.reading(dataset)
    }
}

#[cfg(test)]
mod tests;
