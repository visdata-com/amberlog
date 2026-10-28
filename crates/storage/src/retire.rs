//! 优雅退役中归桶生命周期的部分（REQ-AMB-LIFE-RETIREMENT 的 E、F、G 组）：对一个已重放、已提交的数据集，依次
//! 补做交接窗口中的冷层提交、补写 `ttl` 冷层提交的冷层变更段、按桶从早到晚全量入湖（复用 [`Lake`] 的一次入湖）、补写
//! 冷层提示，再等到删除时刻后物理删除完整删除台账中的热层数据文件与冷层变更段所记的冷层数据文件；最后按块的「已收尾」
//! 判据核对。总次序、WAL 重放与提交、退役记录与报告归生命周期编排（`amberlog-server` 的 `startup::retire`）。
//!
//! 各步都按对象存储的现状判定（交接窗口、冷层变更段的 `cold_removed_in`、存活文件集、完整删除台账与对象是否存在），不记
//! 进度：中途终止后重跑，已做的步骤因判据不再成立而自然跳过（REQ-AMB-LIFE-RETIREMENT-RERUN）。遇对象存储不可访问放弃该
//! 数据集此后的各步（INACCESSIBLE）；数据集停用即不再为它做任何一步（DISABLED）。收到停机信号后不开始新的入湖、提交与
//! 物理删除，不再等待删除时刻（SIGNAL）。

use std::collections::{BTreeSet, HashSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use object_store::path::Path;
use tokio::sync::watch;

use crate::chain::{BucketRef, ChainError, DeletionReason, Digest, SegmentReader, Snapshot};
use crate::cold::store::StoreCold;
use crate::cold::{
    self, ColdOperation, ColdReadError, ColdReader, ColdScope, ColdSnapshot, current,
};
use crate::cold_compaction::{self, TraceError};
use crate::commit::HotCommits;
use crate::committer::CommitError;
use crate::fault;
use crate::flusher::Flusher;
use crate::gate::{DatasetGates, DatasetState};
use crate::lake::{Lake, LakeHalt, Laked};
use crate::layout::{self, HourBucket};
use crate::lifecycle;
use crate::reader::StoreChain;
use crate::store::Store;
use crate::ttl::{self, Trace};

/// 只读冷层元数据、不读任何清单时给的日期范围。
const NO_DAYS: RangeInclusive<i32> = i32::MIN..=i32::MIN;

/// 停机信号（第一次 SIGTERM 或 SIGINT）：各步在请求边界查看，等待删除时刻时立即结束等待。
#[derive(Debug)]
pub struct Cancel(watch::Sender<bool>);

impl Default for Cancel {
    fn default() -> Cancel {
        Cancel(watch::channel(false).0)
    }
}

impl Cancel {
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }

    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }

    async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        let _ = rx.wait_for(|c| *c).await;
    }
}

/// 例外的种类（REQ-AMB-LIFE-RETIREMENT-TERM-EXCEPTION；`record_write_failed` 不属任何数据集，由编排一侧给出）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum ExceptionKind {
    DigestMismatch,
    InputMissing,
    DatasetDisabled,
    DeleteFailed,
    StoreUnavailable,
    Unsettled,
}

impl ExceptionKind {
    /// 报告中 `kind` 的取值。
    pub fn label(self) -> &'static str {
        match self {
            ExceptionKind::DigestMismatch => "digest_mismatch",
            ExceptionKind::InputMissing => "input_missing",
            ExceptionKind::DatasetDisabled => "dataset_disabled",
            ExceptionKind::DeleteFailed => "delete_failed",
            ExceptionKind::StoreUnavailable => "store_unavailable",
            ExceptionKind::Unsettled => "unsettled",
        }
    }
}

/// 一个数据集的一个例外。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Exception {
    pub kind: ExceptionKind,
    /// `digest_mismatch` 与 `input_missing` 的桶（`YYYYMMDDHH`）。
    pub bucket: Option<String>,
    /// 相对数据集前缀的路径；冷层对象写作相对 `<storage.prefix>/` 的路径（`_iceberg/...`）。
    pub paths: Vec<String>,
}

impl Exception {
    pub fn of(kind: ExceptionKind) -> Exception {
        Exception {
            kind,
            bucket: None,
            paths: Vec::new(),
        }
    }
}

/// 对一个数据集收尾的结果。
#[derive(Debug, Default)]
pub struct Settled {
    pub exceptions: Vec<Exception>,
    /// 本次为它写出入湖交接提交的次数（含补做的）。
    pub buckets_laked: u64,
    /// 本次为它物理删除的热层与冷层数据文件个数。
    pub files_deleted: u64,
}

/// 按「已收尾」判据核对的结果（REQ-AMB-LIFE-RETIREMENT-TERM-SETTLED 的 ①、③～⑧；② 由编排一侧按 WAL 判定）。
#[derive(Debug, Default)]
pub struct Check {
    pub settled: bool,
    /// 能确定的妨碍收尾的对象（报告 `unsettled` 例外的 `paths`）。
    pub paths: Vec<String>,
}

/// 退役记录中一个数据集的快照与冷层版本（REQ-AMB-LIFE-RETIREMENT-RECORD-DATASETS）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Anchor {
    pub snapshot_id: u64,
    /// 相对数据集前缀：`manifest/<20 位编号>.json`。
    pub snapshot_path: String,
    pub snapshot_digest: Digest,
    /// 尚无冷层表为 `None`；否则为（冷层当前版本号 N，`metadata/v<N>.metadata.json`，其摘要）。
    pub cold: Option<(u64, String, Digest)>,
}

/// 读取所需对象时的失败。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadFailure {
    Unavailable,
    /// 对象不存在或无法解析。
    Broken(String),
}

/// 对一个数据集的一步没有进行下去。
enum Halt {
    Unavailable,
    DeleteFailed(String),
    /// 数据集停用、提交重试耗尽、WAL 写不进等：该数据集此后各步不做，由核对报为停用或 `unsettled`。
    Other,
    Cancelled,
}

impl From<LakeHalt> for Halt {
    fn from(h: LakeHalt) -> Halt {
        match h {
            LakeHalt::Unavailable => Halt::Unavailable,
            LakeHalt::Other => Halt::Other,
        }
    }
}

fn chain_halt(e: &ChainError) -> Halt {
    match e {
        ChainError::Unavailable(_) => Halt::Unavailable,
        _ => Halt::Other,
    }
}

fn cold_halt(e: &ColdReadError) -> Halt {
    match e {
        ColdReadError::Unavailable(_) => Halt::Unavailable,
        _ => Halt::Other,
    }
}

/// 物理删除的一个待删文件。
struct Pending {
    /// 删除时刻（热层）或可删时刻（冷层），UTC 微秒。
    due: i64,
    key: Path,
    /// 报告中的写法。
    shown: String,
}

/// 退役中对数据集的收尾。
pub struct Retire {
    flusher: Arc<Flusher>,
    cold: Arc<StoreCold>,
    lake: Arc<Lake>,
    delete_delay: Duration,
    cancel: Arc<Cancel>,
}

impl Retire {
    pub fn new(
        flusher: Arc<Flusher>,
        cold: Arc<StoreCold>,
        lake: Arc<Lake>,
        delete_delay: Duration,
        cancel: Arc<Cancel>,
    ) -> Retire {
        Retire {
            flusher,
            cold,
            lake,
            delete_delay,
            cancel,
        }
    }

    fn chain(&self) -> &Arc<StoreChain> {
        self.flusher.committer().chain()
    }

    fn store(&self) -> &Store {
        &self.chain().store
    }

    fn gates(&self) -> &DatasetGates {
        &self.chain().gates
    }

    fn normal(&self, dataset: &Dataset) -> bool {
        self.gates().state(dataset) == DatasetState::Normal
    }

    /// 对数据集做 E、F 两组（重放、提交与计数写出须已对它完成）。停用的数据集只报 `dataset_disabled`。
    pub async fn settle(&self, dataset: &Dataset) -> Settled {
        let mut out = Settled::default();
        let halt = match self.steps(dataset, &mut out).await {
            Ok(()) => return out,
            Err(h) => h,
        };
        match halt {
            Halt::Cancelled => {}
            _ if !self.normal(dataset) => {}
            Halt::Unavailable => out
                .exceptions
                .push(Exception::of(ExceptionKind::StoreUnavailable)),
            Halt::DeleteFailed(path) => out.exceptions.push(Exception {
                kind: ExceptionKind::DeleteFailed,
                bucket: None,
                paths: vec![path],
            }),
            Halt::Other => {}
        }
        if !self.normal(dataset) {
            out.exceptions
                .push(Exception::of(ExceptionKind::DatasetDisabled));
        }
        out
    }

    async fn steps(&self, dataset: &Dataset, out: &mut Settled) -> Result<(), Halt> {
        self.go(dataset)?;
        // E：先补做交接、补写冷层变更段，再按桶从早到晚入湖
        out.buckets_laked += self.lake.recover_windows(dataset).await?;
        self.go(dataset)?;
        self.traces(dataset).await?;
        self.lake_all(dataset, out).await?;
        // F：补写冷层提示，等到删除时刻后物理删除
        self.go(dataset)?;
        self.hint(dataset).await?;
        fault::pause("retire.before_physical_delete");
        fault::gate("retire.before_physical_delete").await;
        self.go(dataset)?;
        self.purge(dataset, out).await
    }

    /// 可以开始下一步：未收到停机信号，数据集未停用。
    fn go(&self, dataset: &Dataset) -> Result<(), Halt> {
        if self.cancel.is_cancelled() {
            return Err(Halt::Cancelled);
        }
        if !self.normal(dataset) {
            return Err(Halt::Other);
        }
        Ok(())
    }

    /// 本进程已知的最新快照（单写者下即热层当前快照）。
    fn snapshot(&self, dataset: &Dataset) -> Option<Arc<Snapshot>> {
        self.chain().latest_snapshot(dataset)
    }

    /// 冷层当前版本中冷层变更段尚未写出的冷层提交逐个补写（RECOVER-TRACE）：`ttl` 按 TTL 的补写，`compaction` 按冷层合并
    /// 的补写。
    async fn traces(&self, dataset: &Dataset) -> Result<(), Halt> {
        let Some(snapshot) = self.snapshot(dataset) else {
            return Ok(());
        };
        let Some(v) = self
            .cold
            .version(dataset)
            .await
            .map_err(|e| cold_halt(&e))?
        else {
            return Ok(());
        };
        let table = self
            .cold
            .table_at(dataset, v, ColdScope::Days(NO_DAYS))
            .await
            .map_err(|e| cold_halt(&e))?;
        let segments = self
            .chain()
            .cold_change_segments(dataset, &snapshot)
            .await
            .map_err(|e| chain_halt(&e))?;
        let traced = cold::traced(&segments);
        let pending: Vec<ColdSnapshot> = table.untraced(&traced).into_iter().cloned().collect();
        let commits = self.flusher.committer();
        // 按冷层快照的先后逐个补写：`ttl` 按 TTL 的补写，`compaction` 按冷层合并的补写（合并记录）
        for s in &pending {
            self.go(dataset)?;
            let result = match s.operation {
                ColdOperation::Ttl { cutoff } => {
                    let trace = Trace {
                        snapshot_id: s.snapshot_id,
                        parent: s.parent_snapshot_id,
                        cutoff,
                    };
                    ttl::write_trace(self.cold.as_ref(), commits.as_ref(), dataset, &table, trace)
                        .await
                        .map_err(|e| cold_halt(&e))?
                }
                ColdOperation::Compaction { .. } => {
                    match cold_compaction::trace_request(self.cold.as_ref(), dataset, &table, s)
                        .await
                    {
                        Ok(None) => None,
                        Ok(Some(request)) => {
                            Some(commits.commit_cold_change(dataset, request).await)
                        }
                        Err(TraceError::Read(e)) => return Err(cold_halt(&e)),
                        Err(TraceError::MissingDigest) => {
                            // 写不出合并记录：按冷层内容无法解析停用（同冷层合并）
                            self.gates()
                                .enter(dataset, DatasetState::ManifestUnreadable);
                            return Err(Halt::Other);
                        }
                    }
                }
                _ => None,
            };
            match result {
                None | Some(Ok(_) | Err(CommitError::Stale)) => {}
                Some(Err(CommitError::Unavailable)) => return Err(Halt::Unavailable),
                Some(Err(_)) => return Err(Halt::Other),
            }
        }
        Ok(())
    }

    /// 为热层当前快照中每个含存活数据文件的桶入湖一次，从早到晚，每个桶至多尝试一次（LAKE-ALL、LAKE-ORDER）。
    async fn lake_all(&self, dataset: &Dataset, out: &mut Settled) -> Result<(), Halt> {
        let mut tried: HashSet<HourBucket> = HashSet::new();
        loop {
            self.go(dataset)?;
            let Some(snapshot) = self.snapshot(dataset) else {
                return Ok(());
            };
            let Some(hour) = snapshot
                .buckets
                .iter()
                .filter(|b| b.record_count > 0)
                .filter_map(|b| HourBucket::parse(&b.bucket))
                .filter(|h| !tried.contains(h))
                .min()
            else {
                return Ok(());
            };
            tried.insert(hour);
            match self.lake.lake_bucket(dataset, hour).await? {
                Laked::Handed => {
                    out.buckets_laked += 1;
                    fault::pause("retire.after_bucket_lake");
                    fault::gate("retire.after_bucket_lake").await;
                }
                Laked::Stopped {
                    mismatched,
                    missing,
                } => {
                    let kind = if mismatched.is_empty() {
                        ExceptionKind::InputMissing
                    } else {
                        ExceptionKind::DigestMismatch
                    };
                    let mut paths: Vec<String> = mismatched.into_iter().chain(missing).collect();
                    paths.sort();
                    out.exceptions.push(Exception {
                        kind,
                        bucket: Some(hour.to_string()),
                        paths,
                    });
                }
                Laked::NotNow => {}
            }
        }
    }

    /// 已有冷层表时，冷层提示不存在、无法解析或不等于冷层当前版本 N 的，以普通覆盖写写为 N（HINT）。
    async fn hint(&self, dataset: &Dataset) -> Result<(), Halt> {
        let store = self.store();
        let layout = self.cold.layout();
        let (n, hint) = current::version(store, layout, dataset)
            .await
            .map_err(|e| match e {
                cold::error::ColdError::Unavailable(_) => Halt::Unavailable,
                _ => Halt::Other,
            })?;
        if n == 0 || hint == Some(n) {
            return Ok(());
        }
        store
            .put(
                &layout.object_path(dataset, cold::layout::HINT),
                n.to_string().into(),
            )
            .await
            .map_err(|_| Halt::Unavailable)
    }

    /// 物理删除完整删除台账中 `reason` 为 `lake`、`ttl`、`compaction` 或 `rollback` 的热层数据文件，与冷层变更段所记的
    /// 冷层数据文件：只删仍存在的，每个都等到其删除时刻（可删时刻）（PHYS-DELETE、WAIT）。
    async fn purge(&self, dataset: &Dataset, out: &mut Settled) -> Result<(), Halt> {
        let mut pending = self.pending(dataset).await?;
        pending.sort_by_key(|p| p.due);
        let store = self.store();
        for p in pending {
            self.go(dataset)?;
            let wait = p.due.saturating_sub(lifecycle::now_us());
            if wait > 0 {
                let wait = Duration::from_micros(u64::try_from(wait).unwrap_or(u64::MAX));
                tokio::select! {
                    () = tokio::time::sleep(wait) => {}
                    () = self.cancel.cancelled() => return Err(Halt::Cancelled),
                }
                self.go(dataset)?;
            }
            match store.delete(&p.key).await {
                Ok(true) => out.files_deleted += 1,
                Ok(false) => {}
                Err(_) => return Err(Halt::DeleteFailed(p.shown)),
            }
        }
        Ok(())
    }

    /// 仍存在的待删文件。
    async fn pending(&self, dataset: &Dataset) -> Result<Vec<Pending>, Halt> {
        let Some(snapshot) = self.snapshot(dataset) else {
            return Ok(Vec::new());
        };
        let (hot, live) = self.hot_ledger(dataset, &snapshot).await?;
        let cold_ledger = self.cold_ledger(dataset, &snapshot).await?;
        let store = self.store();
        let delay = i64::try_from(self.delete_delay.as_micros()).unwrap_or(i64::MAX);
        let mut out = Vec::new();
        if !hot.is_empty() {
            let existing = self.existing(&store.dataset_prefix(dataset)).await?;
            for (path, deleted_at) in hot {
                if existing.contains(&path) && !live.contains(&path) {
                    out.push(Pending {
                        due: deleted_at.saturating_add(delay),
                        key: store.object_path(dataset, &path),
                        shown: path,
                    });
                }
            }
        }
        if !cold_ledger.is_empty() {
            let table = store.cold_table_prefix(dataset);
            let existing = self.existing(&table).await?;
            for (path, deleted_at) in cold_ledger {
                if existing.contains(&path) {
                    out.push(Pending {
                        due: deleted_at.saturating_add(delay),
                        key: path.split('/').fold(table.clone(), |k, part| k.join(part)),
                        shown: format!(
                            "{}/{}/{}/{path}",
                            layout::COLD_ROOT,
                            dataset.org,
                            dataset.stream
                        ),
                    });
                }
            }
        }
        Ok(out)
    }

    /// `prefix` 之下 `data/` 中存在的对象（相对 `prefix`）。
    async fn existing(&self, prefix: &Path) -> Result<HashSet<String>, Halt> {
        let objects = self
            .store()
            .list(&prefix.clone().join("data"))
            .await
            .map_err(|_| Halt::Unavailable)?;
        Ok(objects
            .into_iter()
            .filter_map(|o| {
                o.location.prefix_match(prefix).map(|parts| {
                    parts
                        .map(|x| x.as_ref().to_owned())
                        .collect::<Vec<_>>()
                        .join("/")
                })
            })
            .collect())
    }

    /// 完整删除台账中要物理删除的热层数据文件（路径，`deleted_at`）与热层当前快照的存活数据文件：当前快照 `buckets` 所引用
    /// 的桶 manifest 与经 `retired_segment` 可达的留痕归档段所引用的桶 manifest 中的留痕。
    async fn hot_ledger(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
    ) -> Result<(Vec<(String, i64)>, HashSet<String>), Halt> {
        let chain = self.chain();
        let mut manifests = Vec::new();
        let mut live = HashSet::new();
        for b in &snapshot.buckets {
            let m = chain
                .load_bucket_manifest(dataset, snapshot, b)
                .await
                .map_err(|e| chain_halt(&e))?;
            live.extend(m.files.iter().map(|f| f.path.clone()));
            manifests.push(m);
        }
        let segments = chain
            .retired_segments(dataset, snapshot)
            .await
            .map_err(|e| chain_halt(&e))?;
        for s in &segments {
            for r in &s.buckets {
                let reference = BucketRef {
                    bucket: r.bucket.clone(),
                    manifest: r.manifest.clone(),
                    content_digest: r.content_digest,
                    record_count: 0,
                    size_bytes: 0,
                    time_min: None,
                    time_max: None,
                };
                match chain
                    .read_bucket_manifest(dataset, snapshot, &reference)
                    .await
                    .map_err(|e| chain_halt(&e))?
                {
                    Ok(m) => manifests.push(m),
                    Err(_) => {
                        self.gates()
                            .enter(dataset, DatasetState::ManifestUnreadable);
                        return Err(Halt::Other);
                    }
                }
            }
        }
        let files = manifests
            .iter()
            .flat_map(|m| m.deletions.iter())
            .filter(|d| {
                matches!(
                    d.reason,
                    DeletionReason::Lake
                        | DeletionReason::Ttl
                        | DeletionReason::Compaction
                        | DeletionReason::Rollback
                )
            })
            .map(|d| (d.path.clone(), d.deleted_at))
            .collect();
        Ok((files, live))
    }

    /// 当前快照经 `cold_deletion_segment` 沿 `previous` 可达的冷层变更段所记的冷层数据文件（相对表目录，`deleted_at`）。
    async fn cold_ledger(
        &self,
        dataset: &Dataset,
        snapshot: &Snapshot,
    ) -> Result<Vec<(String, i64)>, Halt> {
        if snapshot.cold_deletion_segment.is_none() {
            return Ok(Vec::new());
        }
        let segments = self
            .chain()
            .cold_change_segments(dataset, snapshot)
            .await
            .map_err(|e| chain_halt(&e))?;
        Ok(segments
            .iter()
            .flat_map(|s| s.deletions.iter())
            .map(|d| (d.path.clone(), d.deleted_at))
            .collect())
    }

    /// 按「已收尾」判据核对数据集（① 未停用；③ 热层当前快照中没有存活数据文件，④ 随之成立；⑤ 冷层当前版本中
    /// `amberlog.operation` 为 `ttl` 或 `compaction` 的冷层提交都已记入冷层变更段；⑥⑦ 待删文件都已不存在；⑧ 冷层提示
    /// 等于冷层当前版本）。读取遇对象存储不可访问为 `Err`。
    pub async fn check(&self, dataset: &Dataset) -> Result<Check, ReadFailure> {
        let unavailable = |_| ReadFailure::Unavailable;
        if !self.normal(dataset) {
            return Ok(Check::default());
        }
        let Some(snapshot) = self.snapshot(dataset) else {
            return Ok(Check {
                settled: true,
                paths: Vec::new(),
            });
        };
        let mut settled = true;
        let mut paths: BTreeSet<String> = BTreeSet::new();
        // ③
        for b in snapshot.buckets.iter().filter(|b| b.record_count > 0) {
            settled = false;
            if let Ok(m) = self
                .chain()
                .load_bucket_manifest(dataset, &snapshot, b)
                .await
            {
                paths.extend(m.files.iter().map(|f| f.path.clone()));
            }
        }
        // ⑥⑦
        match self.pending(dataset).await {
            Ok(left) => {
                if !left.is_empty() {
                    settled = false;
                    paths.extend(left.into_iter().map(|p| p.shown));
                }
            }
            Err(Halt::Unavailable) => return Err(ReadFailure::Unavailable),
            Err(_) => settled = false,
        }
        // ⑤⑧
        let store = self.store();
        let current = current::current(store, self.cold.layout(), dataset)
            .await
            .map_err(|e| match e {
                cold::error::ColdError::Unavailable(u) => unavailable(u),
                e => ReadFailure::Broken(format!("{e:?}")),
            })?;
        if let Some((_, metadata)) = &current.metadata {
            if current.hint != Some(current.version) {
                settled = false;
            }
            let segments = self
                .chain()
                .cold_change_segments(dataset, &snapshot)
                .await
                .map_err(|e| match e {
                    ChainError::Unavailable(_) => ReadFailure::Unavailable,
                    e => ReadFailure::Broken(format!("{e:?}")),
                })?;
            let traced = cold::traced(&segments);
            let untraced = metadata.snapshots().any(|s| {
                let op = s
                    .summary()
                    .additional_properties
                    .get("amberlog.operation")
                    .map(String::as_str);
                matches!(op, Some("ttl" | "compaction")) && !traced.contains(&s.snapshot_id())
            });
            if untraced {
                settled = false;
            }
        }
        Ok(Check {
            settled,
            paths: paths.into_iter().collect(),
        })
    }

    /// 按找链尾重新确定热层当前快照，与按冷层表的同一方法重新确定的冷层当前版本（REPORT-DATASETS）：（快照号，冷层当前
    /// 版本号，尚无冷层表为 0）。
    pub async fn versions(&self, dataset: &Dataset) -> Result<(u64, u64), ReadFailure> {
        let tail = self.chain().find_tail(dataset).await.map_err(|e| match e {
            ChainError::Unavailable(_) => ReadFailure::Unavailable,
            e => ReadFailure::Broken(format!("{e:?}")),
        })?;
        let (n, _) = current::version(self.store(), self.cold.layout(), dataset)
            .await
            .map_err(|e| match e {
                cold::error::ColdError::Unavailable(_) => ReadFailure::Unavailable,
                e => ReadFailure::Broken(format!("{e:?}")),
            })?;
        Ok((tail.current.map_or(0, |s| s.0), n))
    }

    /// 退役记录中该数据集的元素：按找链尾重新确定的当前快照与按冷层表的同一方法重新确定的冷层当前版本，及其完整字节的
    /// SHA-256（RECORD-DATASETS）。
    pub async fn anchor(&self, dataset: &Dataset) -> Result<Anchor, ReadFailure> {
        let store = self.store();
        let tail = self.chain().find_tail(dataset).await.map_err(|e| match e {
            ChainError::Unavailable(_) => ReadFailure::Unavailable,
            e => ReadFailure::Broken(format!("{e:?}")),
        })?;
        let Some(id) = tail.current else {
            return Err(ReadFailure::Broken(format!("{dataset} has no snapshot")));
        };
        let snapshot_path = layout::snapshot_path(id);
        let bytes = store
            .read(&store.object_path(dataset, &snapshot_path))
            .await
            .map_err(|_| ReadFailure::Unavailable)?
            .ok_or_else(|| ReadFailure::Broken(format!("{snapshot_path} does not exist")))?
            .bytes;
        let layout = self.cold.layout();
        let (n, _) = current::version(store, layout, dataset)
            .await
            .map_err(|e| match e {
                cold::error::ColdError::Unavailable(_) => ReadFailure::Unavailable,
                e => ReadFailure::Broken(format!("{e:?}")),
            })?;
        let cold = if n == 0 {
            None
        } else {
            let rel = cold::layout::metadata_path(n);
            let meta = store
                .read(&layout.object_path(dataset, &rel))
                .await
                .map_err(|_| ReadFailure::Unavailable)?
                .ok_or_else(|| ReadFailure::Broken(format!("{rel} does not exist")))?
                .bytes;
            Some((n, rel, Digest::of(&meta)))
        };
        Ok(Anchor {
            snapshot_id: id.0,
            snapshot_path,
            snapshot_digest: Digest::of(&bytes),
            cold,
        })
    }
}
