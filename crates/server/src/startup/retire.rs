//! 优雅退役（`amberlog retire`，[`Form::Retire`](crate::form::Form::Retire)）的启动流程与执行（REQ-AMB-LIFE-RETIREMENT、
//! REQ-AMB-CFG-STARTUP-V7）。退役不是一个新部件，而是生命周期编排的一种运行方式：本模块掌握总次序、退役记录
//! 与报告；逐数据集的收尾归桶生命周期（`amberlog_storage::retire`），复用入湖、TTL 补写与快照提交的代码。
//!
//! 启动流程：配置（[`super::launch`]）→ `wal.dir` → 对象存储校验（与 `serve` 相同的探测）→ 读退役记录 → 列举退役范围内的
//! 数据集（失败以退出码 3 退出）。此后：① 重放 WAL，以写入提交使恢复出的记录进入已提交快照，写出计数，删 WAL 段；② 逐个
//! 数据集补做、补写、全量入湖、补写冷层提示、等到删除时刻后物理删除；③ 全部已收尾、没有例外、未收到信号时以「仅当不存在时
//! 创建」写出退役记录；④ 向 stdout 输出一行 JSON 报告，按其 `status` 退出（0、5、6）。只启动写数据所需的元素：不监听、
//! 不受理请求，不挂任何周期工作（NO-LISTEN、NOT-DO），不输出 `ready`、`lake_backlog_high`、`wal_backlog_high`。
//!
//! 第一次 SIGTERM 或 SIGINT 置取消标志：不开始新的提交、入湖与物理删除，不再等待删除时刻，不写退役记录，输出 `interrupted`
//! 为 true 的报告（SIGNAL）；再次收到由 [`Lifecycle`] 立即以 1 退出（SIGNAL-TWICE）。

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_ingest::counters::Counters;
use amberlog_storage::bloom::IndexHints;
use amberlog_storage::cold::store::StoreCold;
use amberlog_storage::committer::{CommitError, StoreCommitter};
use amberlog_storage::flusher::{FlushConfig, FlushFailure, FlushOutcome, Flusher};
use amberlog_storage::gate::{DatasetGates, DatasetState};
use amberlog_storage::lake::{
    BucketClaims, DigestReports, Lake, LakeConfig, LakeEvents, LakeParts,
};
use amberlog_storage::lifecycle::Lifecycle as BucketLifecycle;
use amberlog_storage::maintenance::LakeInputs;
use amberlog_storage::memtable::Memtable;
use amberlog_storage::objects::Created;
use amberlog_storage::reader::StoreChain;
use amberlog_storage::recovery::{self, Parts};
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::retire::{Anchor, Cancel, Exception, ExceptionKind, ReadFailure, Retire};
use amberlog_storage::schema::StreamColumns;
use amberlog_storage::store::{ProbeError, Store};
use amberlog_storage::wal::{FileWal, Wal};
use serde_json::{Value, json};

use super::{
    EXIT_CONFIG, EXIT_FINDINGS, EXIT_INCOMPLETE, EXIT_OK, EXIT_STORAGE, EXIT_UNCLASSIFIED, Failure,
    check_wal_dir, fail, open_store,
};
use crate::config::Config;
use crate::logging::{self, Level};
use crate::shutdown::Lifecycle;
use crate::version;

/// 退役记录在 `<storage.prefix>/_system/` 下的名字。
pub(super) const RECORD: &str = "retirement.json";

/// 退役记录的对象键（相对 `object_store.url` 的路径部分，报告的 `record` 与 `serve` 的 stderr 都这样写）。
pub(super) fn record_key(cfg: &Config) -> String {
    format!("{}/_system/{RECORD}", cfg.storage_prefix)
}

/// 报告的 `status`（按此先后取第一个成立者）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    AlreadyRetired,
    Retired,
    Incomplete,
    Exceptions,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Status::AlreadyRetired => "already_retired",
            Status::Retired => "retired",
            Status::Incomplete => "incomplete",
            Status::Exceptions => "exceptions",
        }
    }

    fn exit_code(self) -> u8 {
        match self {
            Status::AlreadyRetired | Status::Retired => EXIT_OK,
            Status::Exceptions => EXIT_FINDINGS,
            Status::Incomplete => EXIT_INCOMPLETE,
        }
    }
}

/// 一个数据集在本次运行中的情况。
#[derive(Default)]
struct Entry {
    exceptions: Vec<Exception>,
    /// 重放、提交与计数写出对它都已完成（已收尾第 ② 项）。
    replayed: bool,
    settled: bool,
    buckets_laked: u64,
    files_deleted: u64,
}

impl Entry {
    fn has(&self, kinds: &[ExceptionKind]) -> bool {
        self.exceptions.iter().any(|e| kinds.contains(&e.kind))
    }

    fn add(&mut self, kind: ExceptionKind) {
        if !self.has(&[kind]) {
            self.exceptions.push(Exception::of(kind));
        }
    }
}

/// 退役进程的各元素：只装配写数据所需的（耐久缓冲、提交、冷层写入、桶生命周期的入湖），不装配接入面、查询与周期工作。
struct Elements {
    gates: Arc<DatasetGates>,
    registry: Arc<DatasetRegistry>,
    wal: Arc<FileWal>,
    memtable: Arc<Memtable>,
    flusher: Arc<Flusher>,
    counters: Arc<Counters>,
    cold: Arc<StoreCold>,
}

impl Elements {
    fn new(cfg: &Config, wal_dir: &Path, store: &Store) -> Elements {
        let registry = Arc::new(DatasetRegistry::new());
        // 数据集首次进入停用状态时输出稳定事件 `dataset_disabled`（REQ-AMB-CFG-STARTUP-V7，`serve` 与 `retire` 都输出）
        let gates = Arc::new(DatasetGates::with_observer(Arc::new(|d, state| {
            logging::event(
                Level::Error,
                "dataset_disabled",
                "the dataset is disabled: it is not moved to the cold tier and not retired",
                &[
                    ("dataset", Value::from(d.to_string())),
                    ("state", Value::from(state.label().unwrap_or_default())),
                ],
            );
        })));
        let wal = Arc::new(FileWal::open(wal_dir));
        let shared: Arc<dyn Wal> = wal.clone();
        let chain = Arc::new(StoreChain::new(
            store.clone(),
            Arc::clone(&registry),
            Arc::clone(&gates),
        ));
        let retries = u32::try_from(cfg.commit_max_retries).unwrap_or(u32::MAX);
        let committer = Arc::new(StoreCommitter::new(chain, Arc::clone(&shared), retries));
        // 重放写回的记录同样受内存中积压的上限约束（REQ-AMB-ING-HOT-BACKPRESSURE），按 `flush.max_records` 成批提交
        let memtable = Arc::new(Memtable::with_backlog_limit(cfg.max_backlog_memory_bytes));
        let flusher = Arc::new(
            Flusher::new(
                committer,
                Arc::clone(&memtable),
                FlushConfig {
                    interval: cfg.flush_interval,
                    max_records: cfg.flush_max_records,
                },
            )
            // 读出溢出记录时 WAL 损坏：以退出码 3 退出、不输出报告（REQ-AMB-LIFE-RETIREMENT-REPLAY-WAL-FAIL）
            .with_corruption_handler(Arc::new(|why| {
                logging::log(
                    Level::Error,
                    why,
                    &[("exit_code", Value::from(EXIT_STORAGE))],
                );
                std::process::exit(EXIT_STORAGE.into());
            })),
        );
        let counters = Arc::new(Counters::new(store.clone(), shared));
        let cold = Arc::new(StoreCold::new(
            store.clone(),
            Arc::clone(&gates),
            retries,
            cfg.compaction_target_file_bytes,
        ));
        Elements {
            gates,
            registry,
            wal,
            memtable,
            flusher,
            counters,
            cold,
        }
    }

    fn normal(&self, dataset: &Dataset) -> bool {
        self.gates.state(dataset) == DatasetState::Normal
    }
}

/// 入湖的可观察结果：退役只输出 `digest_mismatch`（不输出 `lake_backlog_high`，NO-LISTEN）。
struct RetireEvents;

impl LakeEvents for RetireEvents {
    fn digest_mismatch(&self, dataset: &Dataset, path: &str) {
        logging::event(
            Level::Error,
            "digest_mismatch",
            "a data file does not match its recorded content digest; the bucket is not moved to the cold tier",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("tier", Value::from("hot")),
                ("path", Value::from(path)),
            ],
        );
    }

    /// 退役的入湖遇到重写失败时只是留下该桶、不写出退役记录（REQ-AMB-LIFE-RETIREMENT），不输出日志、不计数。
    fn rewrite_failed(&self, _dataset: &Dataset, _bucket: &str, _reason: &str) {}

    fn backlog_high(&self, _: &Dataset, _: &str, _: i64, _: i64) {}
}

pub(super) async fn run(cfg: Config, lifecycle: Arc<Lifecycle>) -> Result<u8, Failure> {
    // 由配置装载按 `Form::Retire` 的必填项保证
    let Some(wal_dir) = cfg.wal_dir.clone() else {
        return Err(fail(EXIT_UNCLASSIFIED, "retire requires wal.dir"));
    };
    check_wal_dir(&wal_dir)?;
    let store = open_store(&cfg)?;
    // 测试专用暂停点：离线子命令的对象存储校验之前（REQ-AMB-CFG-STARTUP-V7 场景「离线子命令在启动流程中收到信号以退出码
    // 6 退出」）
    amberlog_storage::fault::gate("offline.before_probe").await;
    store.probe().await.map_err(|e| match e {
        ProbeError::Unsupported(m) => fail(EXIT_CONFIG, m),
        ProbeError::Unavailable(u) => fail(EXIT_STORAGE, u.to_string()),
    })?;
    let record = record_key(&cfg);
    let record_path = store.system_path(RECORD);
    // 读退役记录：存在即已退役（内容能否解析不论），不重放、不写不删（ALREADY）
    if store
        .exists(&record_path)
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?
    {
        print_report(Status::AlreadyRetired, false, Some(&record), &[], &[]);
        return Ok(EXIT_OK);
    }

    let el = Elements::new(&cfg, &wal_dir, &store);
    // 列举退役范围内的数据集：启动流程的最后一步（SCOPE-LIST），失败以 3 退出
    let baselines = el
        .flusher
        .committer()
        .load()
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    for b in &baselines {
        let columns = b.columns.clone().unwrap_or_else(StreamColumns::genesis);
        amberlog_storage::memtable::MemtableWriter::register(
            el.flusher.as_ref(),
            &b.dataset,
            columns,
        );
    }

    // 启动流程到此完成：此后第一次信号置取消标志
    lifecycle.serving();
    let cancel = Arc::new(Cancel::default());
    let lake = Arc::new(Lake::new(
        LakeParts {
            flusher: Arc::clone(&el.flusher),
            reader: el.cold.clone(),
            writer: el.cold.clone(),
            inputs: Arc::new(LakeInputs::new()),
            claims: Arc::new(BucketClaims::default()),
            reports: Arc::new(DigestReports::default()),
            events: Arc::new(RetireEvents),
            // 退役入湖与 `serve` 的入湖一样按本进程读入的索引提示写等值 bloom（REQ-AMB-QRY-EQUALITY-BLOOM）
            hints: Arc::new(IndexHints::new(cfg.index_hints.clone())),
        },
        LakeConfig {
            lifecycle: BucketLifecycle {
                seal_grace: cfg.seal_grace,
                lake_watermark: cfg.lake_watermark,
            },
            alert_after: cfg.lake_alert_after,
            check_interval: cfg.flush_interval,
        },
    ));
    {
        let (cancel, lake, lifecycle) = (
            Arc::clone(&cancel),
            Arc::clone(&lake),
            Arc::clone(&lifecycle),
        );
        tokio::spawn(async move {
            lifecycle.stopping().await;
            cancel.cancel();
            amberlog_storage::maintenance::BucketTask::stop(lake.as_ref());
        });
    }

    let mut entries: BTreeMap<Dataset, Entry> = BTreeMap::new();
    replay(&cfg, &el, &baselines, &cancel, &mut entries).await?;
    amberlog_storage::fault::pause("retire.after_replay");
    amberlog_storage::fault::gate("retire.after_replay").await;

    // 逐个数据集收尾
    let retire = Retire::new(
        Arc::clone(&el.flusher),
        Arc::clone(&el.cold),
        lake,
        cfg.delete_delay,
        Arc::clone(&cancel),
    );
    for (dataset, entry) in &mut entries {
        if cancel.is_cancelled() {
            break;
        }
        if !entry.replayed
            || entry.has(&[
                ExceptionKind::StoreUnavailable,
                ExceptionKind::DatasetDisabled,
            ])
        {
            if !el.normal(dataset) {
                entry.add(ExceptionKind::DatasetDisabled);
            }
            continue;
        }
        let settled = retire.settle(dataset).await;
        entry.buckets_laked += settled.buckets_laked;
        entry.files_deleted += settled.files_deleted;
        entry.exceptions.extend(settled.exceptions);
    }
    let interrupted = cancel.is_cancelled();
    // 收尾中的提交（入湖交接、补写冷层变更段）又在 WAL 中写了提交意图：它们不阻止删段，再删一次（REPLAY 的删段条件）
    el.wal
        .drain()
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;

    // 核对是否已收尾；本块各步都执行过而仍未收尾、又没有其他例外的记 `unsettled`
    for (dataset, entry) in &mut entries {
        if !el.normal(dataset) {
            entry.add(ExceptionKind::DatasetDisabled);
        }
        match retire.check(dataset).await {
            Ok(check) => {
                entry.settled = check.settled && entry.replayed;
                if !entry.settled && entry.exceptions.is_empty() && !interrupted {
                    entry.exceptions.push(Exception {
                        kind: ExceptionKind::Unsettled,
                        bucket: None,
                        paths: check.paths,
                    });
                }
            }
            Err(ReadFailure::Unavailable) => {
                entry.settled = false;
                if !interrupted {
                    entry.add(ExceptionKind::StoreUnavailable);
                }
            }
            Err(ReadFailure::Broken(_)) => {
                entry.settled = false;
                if entry.exceptions.is_empty() && !interrupted {
                    entry.add(ExceptionKind::Unsettled);
                }
            }
        }
    }

    // 退役记录：全部已收尾、没有例外、未收到信号时，作为最后一个写入对象存储的对象
    let mut status = None;
    let mut record_failed = false;
    if !interrupted
        && entries
            .values()
            .all(|e| e.settled && e.exceptions.is_empty())
    {
        amberlog_storage::fault::pause("retire.before_record_write");
        amberlog_storage::fault::gate("retire.before_record_write").await;
        if !cancel.is_cancelled() {
            match record_body(&cfg, &retire, &mut entries).await {
                None => {}
                Some(body) => match store.create(&record_path, body.into()).await {
                    Ok(Created::Done) => status = Some(Status::Retired),
                    // 只在违反单进程约束、两个 `retire` 同时运行时出现
                    Ok(Created::AlreadyExists) => status = Some(Status::AlreadyRetired),
                    Err(e) => {
                        logging::log(
                            Level::Error,
                            &format!("cannot write the retirement record {record}: {e}"),
                            &[],
                        );
                        record_failed = true;
                    }
                },
            }
        }
    }
    let interrupted = cancel.is_cancelled();

    let mut datasets = Vec::new();
    for (dataset, entry) in &entries {
        let (snapshot_id, cold_version) = if entry.has(&[
            ExceptionKind::StoreUnavailable,
            ExceptionKind::DeleteFailed,
            ExceptionKind::DatasetDisabled,
        ]) {
            (Value::Null, Value::Null)
        } else {
            match retire.versions(dataset).await {
                Ok((s, c)) => (Value::from(s), Value::from(c)),
                Err(_) => (Value::Null, Value::Null),
            }
        };
        datasets.push(json!({
            "dataset": dataset.to_string(),
            "settled": entry.settled,
            "snapshot_id": snapshot_id,
            "cold_metadata_version": cold_version,
            "buckets_laked": entry.buckets_laked,
            "files_deleted": entry.files_deleted,
        }));
    }
    let mut exceptions: Vec<(Option<String>, Option<String>, Value)> = entries
        .iter()
        .flat_map(|(d, e)| {
            e.exceptions.iter().map(move |x| {
                (
                    Some(d.to_string()),
                    x.bucket.clone(),
                    json!({
                        "dataset": d.to_string(),
                        "kind": x.kind.label(),
                        "bucket": x.bucket,
                        "paths": x.paths,
                    }),
                )
            })
        })
        .collect();
    if record_failed {
        exceptions.push((
            None,
            None,
            json!({
                "dataset": Value::Null,
                "kind": "record_write_failed",
                "bucket": Value::Null,
                "paths": [format!("_system/{RECORD}")],
            }),
        ));
    }
    // 按 `dataset` 字节序（`null` 排在最后）、再按 `bucket`（`null` 在前）
    exceptions.sort_by(|a, b| (a.0.is_none(), &a.0, &a.1).cmp(&(b.0.is_none(), &b.0, &b.1)));
    let exceptions: Vec<Value> = exceptions.into_iter().map(|(_, _, v)| v).collect();

    let status = status.unwrap_or_else(|| {
        let incomplete = interrupted
            || record_failed
            || entries
                .values()
                .any(|e| e.has(&[ExceptionKind::StoreUnavailable, ExceptionKind::DeleteFailed]));
        if incomplete {
            Status::Incomplete
        } else {
            Status::Exceptions
        }
    });
    let written = matches!(status, Status::Retired | Status::AlreadyRetired);
    print_report(
        status,
        interrupted,
        written.then_some(record.as_str()),
        &datasets,
        &exceptions,
    );
    Ok(status.exit_code())
}

/// 重放 WAL 并提交（D 组）：重放、恢复，再对退役范围内每个数据集补提创世快照、以写入提交使恢复出的记录进入已提交快照，
/// 写出计数，最后删除能删的 WAL 段。读不了 WAL 或删段失败以退出码 3 退出（REPLAY-WAL-FAIL）。
async fn replay(
    cfg: &Config,
    el: &Elements,
    baselines: &[amberlog_storage::commit::Baseline],
    cancel: &Cancel,
    entries: &mut BTreeMap<Dataset, Entry>,
) -> Result<(), Failure> {
    let mut log = Vec::new();
    el.wal
        .replay(&mut |position, entry| log.push((position, entry)))
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    el.counters.restore(&log);
    let recovered = recovery::recover(
        log,
        baselines,
        Parts {
            wal: el.wal.as_ref(),
            committer: el.flusher.as_ref(),
            memtable: el.memtable.as_ref(),
            writer: el.flusher.as_ref(),
            gates: &el.gates,
            registry: &el.registry,
            max_columns: usize::try_from(cfg.max_columns).unwrap_or(usize::MAX),
        },
    )
    .await;
    for r in &recovered.stale {
        logging::event(
            Level::Warn,
            "wal_stale",
            "wal.dir is stale for this dataset: residual records were re-judged against the current schema",
            &[
                ("dataset", Value::from(r.dataset.to_string())),
                ("recovered", Value::from(r.recovered)),
                ("discarded", Value::from(r.discarded)),
            ],
        );
    }
    // 退役范围：列举所得的数据集与重放恢复出记录、`_sent` 元素或计数贡献的数据集（TERM-SCOPE）
    let unresolved: BTreeSet<Dataset> = recovered.unresolved.into_iter().collect();
    let mut scope: BTreeSet<Dataset> = el.registry.datasets().into_iter().collect();
    scope.extend(el.counters.pending().into_iter().map(|(d, _)| d));
    for dataset in scope {
        let entry = entries.entry(dataset.clone()).or_default();
        if unresolved.contains(&dataset) {
            // 提交意图判不明：不提交它的记录（留在 WAL 中，重跑再判）
            entry.add(ExceptionKind::StoreUnavailable);
            continue;
        }
        if cancel.is_cancelled() {
            continue;
        }
        if !el.normal(&dataset) {
            entry.add(ExceptionKind::DatasetDisabled);
            continue;
        }
        entry.replayed = commit(el, &dataset, entry).await;
    }
    // 计数：WAL 中全部 `_sent` 元素与计数贡献写进计数对象，每个有变更的对象尝试一次
    if !cancel.is_cancelled() {
        el.counters.flush_all().await;
    }
    for (dataset, incompatible) in el.counters.pending() {
        let entry = entries.entry(dataset).or_default();
        entry.replayed = false;
        if !incompatible && !cancel.is_cancelled() {
            entry.add(ExceptionKind::StoreUnavailable);
        }
    }
    el.wal
        .drain()
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    Ok(())
}

/// 补提创世快照并提交恢复出的全部记录；返回是否都已提交。遇对象存储不可访问记 `store_unavailable`，停用记
/// `dataset_disabled`。
async fn commit(el: &Elements, dataset: &Dataset, entry: &mut Entry) -> bool {
    let unavailable = |e: &CommitError| matches!(e, CommitError::Unavailable);
    if let Some(Err(e)) = el.flusher.committer().settle(dataset).await {
        if unavailable(&e) {
            entry.add(ExceptionKind::StoreUnavailable);
        }
        if !el.normal(dataset) {
            entry.add(ExceptionKind::DatasetDisabled);
        }
        return false;
    }
    match el.flusher.flush_dataset(dataset).await {
        FlushOutcome::Idle | FlushOutcome::Committed(_) => true,
        FlushOutcome::Failed(f) => {
            if matches!(&f, FlushFailure::Commit(e) if unavailable(e)) {
                entry.add(ExceptionKind::StoreUnavailable);
            }
            if !el.normal(dataset) {
                entry.add(ExceptionKind::DatasetDisabled);
            }
            false
        }
    }
}

/// 退役记录的内容（RECORD-FIELDS、RECORD-DATASETS）；为某数据集重新确定快照与冷层版本遇对象存储不可访问时记
/// `store_unavailable`，此时不写退役记录（`None`）。
async fn record_body(
    cfg: &Config,
    retire: &Retire,
    entries: &mut BTreeMap<Dataset, Entry>,
) -> Option<String> {
    let mut datasets = Vec::new();
    let mut complete = true;
    for (dataset, entry) in entries.iter_mut() {
        match retire.anchor(dataset).await {
            Ok(Anchor {
                snapshot_id,
                snapshot_path,
                snapshot_digest,
                cold,
            }) => datasets.push(json!({
                "dataset": dataset.to_string(),
                "snapshot": {
                    "snapshot_id": snapshot_id,
                    "path": snapshot_path,
                    "content_digest": snapshot_digest.to_string(),
                },
                "cold": cold.map(|(n, path, digest)| json!({
                    "metadata_version": n,
                    "path": path,
                    "content_digest": digest.to_string(),
                })),
            })),
            Err(failure) => {
                complete = false;
                entry.settled = false;
                entry.add(match failure {
                    ReadFailure::Unavailable => ExceptionKind::StoreUnavailable,
                    ReadFailure::Broken(_) => ExceptionKind::Unsettled,
                });
            }
        }
    }
    complete.then(|| {
        json!({
            "format_version": 1,
            "retired_at": amberlog_storage::lifecycle::now_us(),
            "amberlog_version": version::SEMVER,
            "retention_period": cfg.retention_period.as_ref().map(|r| r.text.clone()),
            "datasets": datasets,
        })
        .to_string()
    })
}

/// 向 stdout 输出恰好一行 JSON 报告（REPORT）。
fn print_report(
    status: Status,
    interrupted: bool,
    record: Option<&str>,
    datasets: &[Value],
    exceptions: &[Value],
) {
    let report = json!({
        "status": status.label(),
        "interrupted": interrupted,
        "record": record,
        "datasets": datasets,
        "exceptions": exceptions,
    });
    println!("{report}");
}
