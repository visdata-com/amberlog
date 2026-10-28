//! 写入实例（`amberlog serve`，[`Form::Serve`](crate::form::Form::Serve)）的启动流程与停机：按「`wal.dir` → 对象存储 → 读取已存在数据集的基线
//! 与 WAL 重放 → 监听」的顺序校验（配置一步在 [`super::launch`]），首个失败项决定退出码（REQ-AMB-CFG-STARTUP-V2 B 组，ADR-0016）。
//! 写入实例启动全部元素；停机顺序见 [`run`] 末段。

use std::path::Path;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_ingest::counters::Counters;
use amberlog_ingest::timestamp::TimestampWindow;
use amberlog_query::files::StoreFiles;
use amberlog_query::limits::Admission;
use amberlog_query::request::Limits;
use amberlog_query::system::AlertHistory;
use amberlog_query::system::backfill::BackfillTracker;
use amberlog_query::system::background::Background;
use amberlog_query::system::verdict::Backfill;
use amberlog_storage::archive::Archiver;
use amberlog_storage::bloom::IndexHints;
use amberlog_storage::chain::SnapshotId;
use amberlog_storage::cold::store::{ColdOrphans, StoreCold};
use amberlog_storage::cold_compaction::{
    ColdCompaction, ColdCompactionConfig, ColdCompactionDeps, ColdCompactionEvents,
};
use amberlog_storage::cold_retention::{
    ColdRetention, ColdRetentionConfig, ColdRetentionDeps, ColdRetentionEvents,
};
use amberlog_storage::commit::Baseline;
use amberlog_storage::committer::StoreCommitter;
use amberlog_storage::compaction::{
    CompactionConfig, CompactionEvents, CompactionShared, Compactor,
};
use amberlog_storage::flusher::{FlushConfig, Flusher};
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::hot_fsck::{FsckConfig, FsckEvents, HotFsck};
use amberlog_storage::lake::{
    BucketClaims, DigestReports, Lake, LakeConfig, LakeEvents, LakeParts,
};
use amberlog_storage::lifecycle::Lifecycle as BucketLifecycle;
use amberlog_storage::maintenance::{BucketTasks, LakeInputs};
use amberlog_storage::memtable::Memtable;
use amberlog_storage::purge::Purge;
use amberlog_storage::reader::{FreshSegments, StoreChain};
use amberlog_storage::recovery::{self, Parts};
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::schema::StreamColumns;
use amberlog_storage::snapshot_gc::RetentionConfig;
use amberlog_storage::store::{ProbeError, Store};
use amberlog_storage::ttl::{QuietChain, Ttl, TtlConfig, TtlDeps};
use amberlog_storage::wal::{FileWal, Wal, WalEvents};
use serde_json::Value;

use super::{
    EXIT_CONFIG, EXIT_LISTEN, EXIT_STORAGE, EXIT_UNCLASSIFIED, Failure, Http, check_wal_dir, fail,
    open_store, refuse_if_retired,
};
use crate::alerting::history::History;
use crate::alerting::metrics::AlertCounters;
use crate::alerting::schedule::{Deps, MissedWindows, Scheduler};
use crate::alerting::webhook::Webhook;
use crate::alerting::{Delivery, Recorder, Stop};
use crate::api::search::governance::PersistedCounters;
use crate::api::{self, Settings, Storage};
use crate::catalog::Catalog;
use crate::config::Config;
use crate::gateway::tls::Inbound;
use crate::gateway::{self, AppState};
use crate::logging::{self, Level};
use crate::shutdown::Lifecycle;
use crate::telemetry::{self, AlertingView, Health, Metrics, StorageView};
use crate::verdicts;

pub(super) async fn run(
    cfg: Config,
    inbound: Option<Inbound>,
    lifecycle: Arc<Lifecycle>,
) -> Result<(), Failure> {
    // 两项都由配置装载按 `Form::Serve` 的必填项保证
    let (Some(wal_dir), Some(auth)) = (cfg.wal_dir.clone(), cfg.auth.clone()) else {
        return Err(fail(
            EXIT_UNCLASSIFIED,
            "serve requires wal.dir and authentication settings",
        ));
    };
    check_wal_dir(&wal_dir)?;

    let store = open_store(&cfg)?;
    store.probe().await.map_err(|e| match e {
        // 存储不支持本系统的提交协议：重试不会好转，归配置一类（REQ-AMB-CFG-STARTUP-V5）
        ProbeError::Unsupported(m) => fail(EXIT_CONFIG, m),
        ProbeError::Unavailable(u) => fail(EXIT_STORAGE, u.to_string()),
    })?;
    refuse_if_retired(&cfg, &store).await?;

    let metrics = Arc::new(Metrics::default());
    metrics.attach_wal_dir(wal_dir.clone());
    // 评估记录的写出与读取（REQ-AMB-ALR-EVALUATIONS）：只在写入实例构造
    let history = History::new(
        store.clone(),
        cfg.alerts.history_retention,
        cfg.hot_fsck_interval,
    );
    let (storage, baselines, flusher, cold) = open_storage(
        &cfg,
        &wal_dir,
        store.clone(),
        Arc::clone(&metrics),
        Arc::clone(&history) as Arc<dyn AlertHistory>,
    )
    .await?;

    amberlog_storage::fault::pause("开始监听之前");
    let listener = tokio::net::TcpListener::bind(cfg.http_addr)
        .await
        .map_err(|e| {
            fail(
                EXIT_LISTEN,
                format!("cannot listen on {}: {e}", cfg.http_addr),
            )
        })?;
    let local_addr = listener.local_addr().map_err(|e| {
        fail(
            EXIT_LISTEN,
            format!("cannot read the listening address: {e}"),
        )
    })?;

    let health = Arc::new(Health::new(wal_dir.clone(), cfg.object_store_url.clone()));
    // 封口合并与入湖共用：同一桶二者不同时进行，`digest_mismatch` 同一路径至多一次；入湖输入的登记供 TTL 避让
    let claims = Arc::new(BucketClaims::default());
    let reports = Arc::new(DigestReports::default());
    let lake_inputs = Arc::new(LakeInputs::new());
    // 本进程启动时读入的索引提示：只交给写等值 bloom 的三处写出（REQ-AMB-DM-INDEX-HINTS-V2、REQ-AMB-QRY-EQUALITY-BLOOM）
    let hints = Arc::new(IndexHints::new(cfg.index_hints.clone()));
    let compactor = Arc::new(Compactor::new(
        Arc::clone(&flusher),
        CompactionConfig {
            lifecycle: bucket_lifecycle(&cfg),
            target_file_bytes: cfg.compaction_target_file_bytes,
            check_interval: COMPACTION_CHECK_INTERVAL,
        },
        Arc::new(StorageEvents(Arc::clone(&metrics))),
        CompactionShared {
            claims: Arc::clone(&claims),
            reports: Arc::clone(&reports),
            cold: cold.clone(),
            hints: Arc::clone(&hints),
        },
    ));
    let lake = Arc::new(Lake::new(
        LakeParts {
            flusher: Arc::clone(&flusher),
            reader: cold.clone(),
            writer: cold.clone(),
            inputs: Arc::clone(&lake_inputs),
            claims,
            reports,
            events: Arc::new(StorageEvents(Arc::clone(&metrics))),
            hints: Arc::clone(&hints),
        },
        LakeConfig {
            lifecycle: bucket_lifecycle(&cfg),
            alert_after: cfg.lake_alert_after,
            check_interval: LAKE_CHECK_INTERVAL,
        },
    ));
    let fsck = Arc::new(HotFsck::new(
        Arc::clone(&flusher),
        FsckConfig {
            interval: cfg.hot_fsck_interval,
            min_orphan_age: cfg.hot_fsck_min_orphan_age,
            delete_delay: cfg.delete_delay,
            retention: RetentionConfig {
                snapshots: cfg.hot_retention_snapshots,
                age: cfg.hot_retention_age,
            },
        },
        Arc::new(StorageEvents(Arc::clone(&metrics))),
    ));
    let state = AppState {
        health: Arc::clone(&health),
        metrics,
        auth: Arc::new(auth),
    };
    health.set_listening(local_addr, inbound.is_some());
    // 写入实例同一监听始终提供目录端点（REQ-AMB-STO-COLD-CATALOG-SERVE-PROVIDES）
    let catalog = Catalog::new(store.clone(), cfg.storage_prefix.clone());
    health.spawn_probe(store);
    health.evaluate();

    lifecycle.serving();

    let settings = Settings {
        max_body_bytes: cfg.max_body_bytes,
        max_columns: usize::try_from(cfg.max_columns).unwrap_or(usize::MAX),
        window: TimestampWindow {
            max_future: cfg.max_future,
            max_past: cfg.max_past,
        },
        query: Limits {
            default_size: cfg.query_default_size,
            max_size: cfg.query_max_size,
        },
        baselines: baselines
            .into_iter()
            .map(|b| (b.dataset, b.snapshot_id))
            .collect(),
        lifecycle: bucket_lifecycle(&cfg),
        retention: cfg.retention_period.clone(),
        admission: Admission::new(
            usize::try_from(cfg.query_max_concurrency).unwrap_or(usize::MAX),
            usize::try_from(cfg.query_max_memory_bytes).unwrap_or(usize::MAX),
        ),
        backfill: Arc::new(BackfillTracker::new(
            i64::try_from(cfg.reconciliation_grace.as_micros()).unwrap_or(i64::MAX),
        )),
    };
    // 开始受理请求之前接上：`/metrics` 从一开始就输出查询资源上限的两个指标（REQ-AMB-OBS-HEALTH-METRICS-V5）
    state.metrics.attach_search(Arc::clone(&settings.admission));
    // 告警的两个指标自开始响应 `/metrics` 起为每条启用的规则输出（REQ-AMB-ALR-RULES-EVAL-METRIC-INIT、
    // REQ-AMB-ALR-WEBHOOK-DELIVERY-METRIC-INIT），未写出的评估记录条数同时输出（REQ-AMB-ALR-EVALUATIONS-UNWRITTEN-METRIC）
    let alert_counters = Arc::new(AlertCounters::new(
        cfg.alerts.rules.iter().map(|r| r.name.as_str()),
    ));
    state.metrics.attach_alerting(AlertingView {
        counters: Arc::clone(&alert_counters),
        unwritten: {
            let h = Arc::clone(&history);
            Arc::new(move || h.unwritten())
        },
    });
    let alert_query = Arc::new(api::search::engine(&storage, &settings));
    // 后台判定（REQ-AMB-GOV-MISSED-COLLECTION C 组）只在写入实例构造；六个序列从一开始就输出
    let background = {
        let backfill = Arc::clone(&settings.backfill);
        Arc::new(Background::new(
            Arc::clone(&storage.registry),
            Arc::new(PersistedCounters(Arc::clone(&storage.counters))),
            Arc::clone(&settings.backfill) as Arc<dyn Backfill>,
            Arc::new(verdicts::LogEvents),
            settings.backfill.grace_us(),
            Box::new(move || backfill.accepting_since()),
        ))
    };
    state
        .metrics
        .attach_reconciliation(Arc::clone(&background), Arc::clone(&storage.registry));
    let mut modules = api::routes(&state, &storage, &settings);
    modules.push(crate::catalog::router(catalog));
    let app = gateway::app(state.clone(), modules);
    let mut bucket_tasks = BucketTasks::new();
    bucket_tasks.add(compactor);
    bucket_tasks.add(fsck);
    bucket_tasks.add(lake);
    bucket_tasks.add(Arc::new(Archiver::new(
        Arc::clone(&flusher),
        amberlog_storage::archive::MIN_EMPTY,
    )));
    // 冷层孤儿清理（REQ-AMB-STO-COLD-TABLE）：与热层巡检同周期、同最小年龄；共用唯一的冷层读写实例，才看得到在途上传
    bucket_tasks.add(Arc::new(ColdOrphans::new(
        cold.clone(),
        Arc::clone(&storage.registry),
        cfg.hot_fsck_interval,
        cfg.hot_fsck_min_orphan_age,
    )));
    bucket_tasks.add(Arc::new(Purge::new(
        Arc::clone(flusher.committer().chain()),
        cfg.delete_delay,
    )));
    // TTL 到期删除（REQ-AMB-STO-TTL）：只在配置了 `retention.period` 时运行；与入湖共用入湖输入的登记与冷层读写实例
    if let Some(retention) = &cfg.retention_period {
        let chain = flusher.committer().chain();
        bucket_tasks.add(Arc::new(Ttl::new(
            TtlDeps {
                registry: Arc::clone(&storage.registry),
                gates: Arc::clone(&storage.gates),
                chain: Arc::new(QuietChain(Arc::clone(chain))),
                segments: chain.clone(),
                commits: flusher.committer().clone(),
                cold_reader: cold.clone(),
                cold_writer: cold.clone(),
                lake_inputs: Arc::clone(&lake_inputs),
                tally: flusher.clone(),
            },
            TtlConfig {
                retention: retention.period,
                check_interval: cfg.retention_check_interval,
            },
        )));
    }
    // 冷层合并（REQ-AMB-STO-COLD-COMPACTION）：编排归桶生命周期，提交经同一个冷层读写实例
    {
        let chain = flusher.committer().chain();
        bucket_tasks.add(Arc::new(ColdCompaction::new(
            ColdCompactionDeps {
                registry: Arc::clone(&storage.registry),
                gates: Arc::clone(&storage.gates),
                chain: chain.clone(),
                quiet: Arc::new(QuietChain(Arc::clone(chain))),
                segments: Arc::new(FreshSegments(Arc::clone(chain))),
                commits: flusher.committer().clone(),
                tally: flusher.clone(),
                cold_reader: cold.clone(),
                upkeep: cold.clone(),
                events: Arc::new(StorageEvents(Arc::clone(&state.metrics))),
                hints: Arc::clone(&hints),
            },
            ColdCompactionConfig {
                lifecycle: bucket_lifecycle(&cfg),
                check_interval: cfg.cold_compaction_check_interval,
                target_file_bytes: cfg.compaction_target_file_bytes,
                retention: cfg.retention_period.as_ref().map(|r| r.period),
            },
        )));
    }
    // 冷层保留（REQ-AMB-STO-COLD-RETENTION）：冷层过期提交经同一个冷层读写实例；被释放的对象由冷层孤儿清理删除
    {
        let chain = flusher.committer().chain();
        bucket_tasks.add(Arc::new(ColdRetention::new(
            ColdRetentionDeps {
                registry: Arc::clone(&storage.registry),
                gates: Arc::clone(&storage.gates),
                chain: Arc::new(QuietChain(Arc::clone(chain))),
                segments: Arc::new(FreshSegments(Arc::clone(chain))),
                cold_reader: cold.clone(),
                upkeep: cold.clone(),
                events: Arc::new(StorageEvents(Arc::clone(&state.metrics))),
            },
            ColdRetentionConfig {
                snapshots: cfg.cold_retention_snapshots,
                age: cfg.cold_retention_age,
                check_interval: cfg.cold_retention_check_interval,
                delete_delay: cfg.delete_delay,
            },
        )));
    }
    let flushing = flusher.spawn();
    bucket_tasks.start();
    let counting = storage.counters.spawn();
    let history_writing = history.spawn();
    let watching = telemetry::spawn_wal_watch(wal_dir, cfg.wal_alert_bytes);
    // 开始受理 `/api/` 请求：补发中「刚开始受理」与后台判定的基线都从这一刻算起
    let accepting_since = amberlog_storage::lifecycle::now_us();
    settings.backfill.set_accepting_since(accepting_since);
    let missed = Arc::clone(&background) as Arc<dyn MissedWindows>;
    let judging = verdicts::spawn(background, verdicts::PERIOD);
    // 配了证书时只以 TLS 提供服务，证书重读随之开始（REQ-AMB-ARCH-ENCRYPTION）
    let mut server = match &inbound {
        Some(tls) => {
            let listener = tls.listener(listener).map_err(|e| {
                fail(
                    EXIT_LISTEN,
                    format!("cannot listen on {}: {e}", cfg.http_addr),
                )
            })?;
            tls.spawn_reload();
            Http::spawn(listener, app)
        }
        None => Http::spawn(listener, app),
    };
    // 告警（REQ-AMB-ALR-RULES-FIRST-TICK）：自开始受理 `/api/` 请求之时起调度启用的规则，进入停机过程即停
    let (alert_stop, stop) = Stop::pair();
    let scheduler = Scheduler::start(
        Deps {
            rules: cfg.alerts.rules.clone(),
            max_concurrency: usize::try_from(cfg.alerts.max_concurrency).unwrap_or(usize::MAX),
            query: alert_query,
            delivery: Arc::new(Webhook::new(cfg.alerts.ca_pem.as_deref())) as Arc<dyn Delivery>,
            recorder: Arc::clone(&history) as Arc<dyn Recorder>,
            counters: alert_counters,
            // 内置规则的结果取自后台判定（REQ-AMB-ALR-RULES-BUILTIN-RESULT）
            missed: Some(missed),
            max_size: cfg.query_max_size,
        },
        accepting_since,
        stop,
    );
    tokio::select! {
        failure = server.failed() => return Err(failure),
        () = lifecycle.stopping() => {}
    }

    // 优雅停机：停止受理（新的 `/api/` 请求 503，`/readyz` 报 shutting_down，监听照旧）与告警（不再开始评估，
    // 在途的按阶段结束）→ 排空在途请求（至多 `server.shutdown_timeout`）→ flush 与提交（停用数据集除外；失败的留在 WAL
    // 供下次重放）→ 写出计数与评估记录（每个有变更的对象尝试一次；计数写不成的，已获 200 的贡献留在 WAL 供下次重放后补齐）
    // → 等连接写完应答（至多到停机开始后 `server.shutdown_timeout` 届满）→ 退出码 0。
    let deadline = tokio::time::Instant::now() + cfg.shutdown_timeout;
    health.begin_shutdown();
    // 告警不再取名额、不再开始查询；在途评估按阶段结束并交出评估记录，投递中止（REQ-AMB-ALR-RULES-SHUTDOWN-ABANDON）
    alert_stop.stop();
    // 进入停机过程后不再开始新的后台判定，进行中的一次随之放弃（REQ-AMB-GOV-MISSED-COLLECTION-SHUTDOWN）
    judging.abort();
    // 停机过程中不开始桶生命周期的新一轮工作（合并、巡检、归档、延后物理删除），进行中的直接中止（已上传未提交的合并输出按孤儿处置）
    bucket_tasks.stop().await;
    let ((), drained) = tokio::join!(scheduler.stopped(), health.drained(cfg.shutdown_timeout));
    if !drained {
        logging::log(
            Level::Warn,
            "in-flight requests did not finish within server.shutdown_timeout",
            &[],
        );
    }
    flushing.abort();
    let _ = flushing.await;
    storage.committer.flush_all().await;
    counting.abort();
    let _ = counting.await;
    history_writing.abort();
    let _ = history_writing.await;
    watching.abort();
    let _ = watching.await;
    // 评估记录每个对象尝试写出一次，与计数写出同时进行、不另行等待（REQ-AMB-ALR-EVALUATIONS-SHUTDOWN-FLUSH）
    tokio::join!(storage.counters.flush_all(), history.flush_once());
    // 测试专用暂停点：停机过程中、进程退出之前（验证停机过程中不开始新的入湖与物理删除）
    amberlog_storage::fault::pause("shutdown.before_exit");
    // 监听留到最后：停机全程新到的 `/api/` 请求都得 503
    server.close(deadline).await;
    Ok(())
}

/// 装配存储各元素，并在开始受理 `/api/` 请求之前完成「读入已存在数据集的基线 → 登记进可见集 → 重放 WAL 并解释」。
/// 读取因对象存储不可访问而失败、WAL 非尾部损坏归退出码 3；为数据集写出对象失败不使启动失败。
/// 返回的 [`Flusher`] 既是 `storage` 中的快照提交与可见集写入一侧，也是 flush 任务的本体，由调用方在开始受理时启动。
async fn open_storage(
    cfg: &Config,
    wal_dir: &Path,
    store: Store,
    metrics: Arc<Metrics>,
    alerts: Arc<dyn AlertHistory>,
) -> Result<(Storage, Vec<Baseline>, Arc<Flusher>, Arc<StoreCold>), Failure> {
    let registry = Arc::new(DatasetRegistry::new());
    // 数据集首次进入停用状态时输出稳定事件 `dataset_disabled`（REQ-AMB-CFG-STARTUP-V4，含启动时读取已存在数据集）
    let gates = Arc::new(DatasetGates::with_observer(Arc::new(|d, state| {
        logging::event(
            Level::Error,
            "dataset_disabled",
            "the dataset is disabled until restart: ingestion and queries for it are rejected",
            &[
                ("dataset", Value::from(d.to_string())),
                ("state", Value::from(state.label().unwrap_or_default())),
            ],
        );
    })));
    let wal: Arc<dyn Wal> = Arc::new(FileWal::open(wal_dir).with_events(Arc::new(WalLog)));
    let chain = Arc::new(StoreChain::new(
        store.clone(),
        Arc::clone(&registry),
        Arc::clone(&gates),
    ));
    let committer = Arc::new(StoreCommitter::new(
        Arc::clone(&chain),
        Arc::clone(&wal),
        u32::try_from(cfg.commit_max_retries).unwrap_or(u32::MAX),
    ));
    metrics.attach_storage(StorageView {
        gates: Arc::clone(&gates),
        chain: Arc::clone(&chain),
        lifecycle: bucket_lifecycle(cfg),
    });
    // 内存中积压的上限（REQ-AMB-ING-HOT-BACKPRESSURE）：重放写回的记录同样受它约束
    let memtable = Arc::new(Memtable::with_backlog_limit(cfg.max_backlog_memory_bytes));
    // `amberlog_backlog_memory_bytes` 取它此刻占用的额度（REQ-AMB-ING-HOT-BACKPRESSURE-V5）
    metrics.attach_backlog(Arc::clone(&memtable));
    // 写入经 Flusher 转交可见集，以便按 `flush.max_records` 触发 flush（重放写回的记录同样计入）
    // 提交结果计入 `/metrics` 的 amberlog_manifest_commits_total（REQ-AMB-OBS-HEALTH-METRICS）
    let flusher = Arc::new(
        Flusher::new(
            committer,
            Arc::clone(&memtable),
            FlushConfig {
                interval: cfg.flush_interval,
                max_records: cfg.flush_max_records,
            },
        )
        .with_commit_observer(Arc::new(move |d, ok| {
            metrics.record_commit(&d.org, &d.stream, ok)
        }))
        // 读出溢出记录时 WAL 损坏：不跳过已确认的记录，以退出码 3 退出（REQ-AMB-ING-HOT-BACKPRESSURE-V2），
        // 日志与启动失败同一写法
        .with_corruption_handler(Arc::new(|why| {
            logging::log(
                Level::Error,
                why,
                &[("exit_code", Value::from(EXIT_STORAGE))],
            );
            std::process::exit(EXIT_STORAGE.into());
        })),
    );
    let counters = Arc::new(Counters::new(store.clone(), Arc::clone(&wal)));
    // 冷层表的读写（REQ-AMB-STO-COLD-TABLE）：全进程只有这一个实例——在途对象、各数据集的冷层提交锁与重试进度都在其中，
    // 冷层孤儿清理要看到写者的在途对象。冷层停用数据集经热层共用的闸门。跨层查询的冷侧经它读冷层表。
    let cold = Arc::new(StoreCold::new(
        store.clone(),
        Arc::clone(&gates),
        u32::try_from(cfg.commit_max_retries).unwrap_or(u32::MAX),
        cfg.compaction_target_file_bytes,
    ));
    let storage = Storage {
        wal,
        memtable,
        committer: flusher.clone(),
        chain,
        gates,
        writer: flusher.clone(),
        registry,
        files: Arc::new(StoreFiles::new(store)),
        cold: cold.clone(),
        counters,
        alerts,
    };

    // 基线（按链尾定当前快照、停用状态记入闸门；登记表由装载记为 Stored）：停用的数据集同样进入可见集目录，
    // 查询据此先报停用状态而不是 `stream_not_found`，其 schema 不参与判定，取创世 schema 占位。
    let baselines = storage
        .committer
        .load()
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    for b in &baselines {
        let columns = b.columns.clone().unwrap_or_else(StreamColumns::genesis);
        storage.writer.register(&b.dataset, columns);
    }

    // WAL 重放在监听之前完成（ADR-0016）：`/readyz` 首次返回 200 时重放必然已完成。
    let mut entries = Vec::new();
    storage
        .wal
        .replay(&mut |position, entry| entries.push((position, entry)))
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    // 窗口恢复（ADR-0023 第 6 条）：计数增量、`_sent` 元素与计数意图交给治理计数，写出前按本地计数水位筛选
    storage.counters.restore(&entries);
    let recovered = recovery::recover(
        entries,
        &baselines,
        Parts {
            wal: storage.wal.as_ref(),
            committer: storage.committer.as_ref(),
            memtable: storage.memtable.as_ref(),
            writer: storage.writer.as_ref(),
            gates: &storage.gates,
            registry: &storage.registry,
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
    for d in &recovered.unresolved {
        logging::log(
            Level::Warn,
            "a commit intent could not be resolved during WAL replay; it will be retried",
            &[("dataset", Value::from(d.to_string()))],
        );
    }
    Ok((storage, baselines, flusher, cold))
}

/// 合并的检查周期：远小于块规定的 60 秒时限。
const COMPACTION_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// 入湖的检查周期：块要求至少每 60 秒一次，取得短些使入湖在计时起点后 120 秒内完成。
const LAKE_CHECK_INTERVAL: std::time::Duration = std::time::Duration::from_secs(10);

/// 存储后台任务（合并、巡检）的可观察结果：接到 `/metrics` 与稳定日志事件。
struct StorageEvents(Arc<Metrics>);

/// WAL 追加失败接到日志：一行非稳定 warn 日志，含失败的操作与操作系统给出的原因，同一操作同一原因每秒至多一行。
struct WalLog;

impl WalEvents for WalLog {
    fn append_failed(&self, op: &str, reason: &str, failures: u64) {
        logging::log(
            Level::Warn,
            &format!("WAL write failed ({op}): {reason}"),
            &[
                ("op", Value::from(op)),
                ("reason", Value::from(reason)),
                ("failures", Value::from(failures)),
            ],
        );
    }
}

impl CompactionEvents for StorageEvents {
    fn compaction(&self, dataset: &Dataset, ok: bool) {
        self.0.record_compaction(&dataset.org, &dataset.stream, ok);
    }

    fn digest_mismatch(&self, dataset: &Dataset, path: &str) {
        logging::event(
            Level::Error,
            "digest_mismatch",
            "a data file does not match its recorded content digest; the bucket is not compacted or moved to the cold tier until restart",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("tier", Value::from("hot")),
                ("path", Value::from(path)),
            ],
        );
    }
}

impl ColdCompactionEvents for StorageEvents {
    fn digest_mismatch(&self, dataset: &Dataset, path: &str) {
        logging::event(
            Level::Error,
            "digest_mismatch",
            "a cold data file does not match its recorded content digest; its day partition is not compacted until restart",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("tier", Value::from("cold")),
                ("path", Value::from(path)),
            ],
        );
    }

    fn abandoned(&self, dataset: &Dataset, reason: &str) {
        logging::log(
            Level::Warn,
            "cold compaction abandoned for this check",
            &[
                ("org", Value::from(dataset.org.as_str())),
                ("stream", Value::from(dataset.stream.as_str())),
                ("reason", Value::from(reason)),
            ],
        );
    }
}

impl ColdRetentionEvents for StorageEvents {
    fn abandoned(&self, dataset: &Dataset, reason: &str) {
        logging::log(
            Level::Warn,
            "cold retention abandoned for this run",
            &[
                ("org", Value::from(dataset.org.as_str())),
                ("stream", Value::from(dataset.stream.as_str())),
                ("reason", Value::from(reason)),
            ],
        );
    }
}

impl LakeEvents for StorageEvents {
    fn digest_mismatch(&self, dataset: &Dataset, path: &str) {
        CompactionEvents::digest_mismatch(self, dataset, path);
    }

    fn rewrite_failed(&self, dataset: &Dataset, bucket: &str, reason: &str) {
        self.0
            .record_lake_rewrite_failure(&dataset.org, &dataset.stream);
        // 非稳定日志（REQ-AMB-STO-LAKE-ENTRY-V2-REWRITE-LOG）
        logging::log(
            Level::Warn,
            "rewriting an hour bucket into the cold tier failed; this process will not retry it",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("bucket", Value::from(bucket)),
                ("reason", Value::from(logging::redact_urls(reason))),
            ],
        );
    }

    fn backlog_high(
        &self,
        dataset: &Dataset,
        bucket: &str,
        lag_seconds: i64,
        alert_after_seconds: i64,
    ) {
        logging::event(
            Level::Warn,
            "lake_backlog_high",
            "hour buckets past the lake watermark have not entered the cold tier for longer than lifecycle.lake_alert_after",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("bucket", Value::from(bucket)),
                ("lag_seconds", Value::from(lag_seconds)),
                ("alert_after_seconds", Value::from(alert_after_seconds)),
            ],
        );
    }
}

impl FsckEvents for StorageEvents {
    fn deleted(&self, dataset: &Dataset, data_file: bool) {
        self.0
            .record_fsck_deleted(&dataset.org, &dataset.stream, data_file);
    }

    fn snapshot_deleted(&self, dataset: &Dataset, id: SnapshotId, resurrected: bool) {
        self.0
            .record_snapshot_gc_deleted(&dataset.org, &dataset.stream, resurrected);
        if resurrected {
            // 非稳定日志（REQ-AMB-STO-SNAPSHOT-GC-RESURRECTED-LOG）
            logging::log(
                Level::Warn,
                "snapshot collection deleted a resurrected snapshot",
                &[
                    ("dataset", Value::from(dataset.to_string())),
                    ("snapshot_id", Value::from(id.0)),
                ],
            );
        }
    }

    fn abandoned(&self, dataset: &Dataset, task: &str, reason: &str) {
        // 非稳定日志（REQ-AMB-STO-SNAPSHOT-GC-ABANDON-WARN、REQ-AMB-STO-HOT-FSCK-V3）
        logging::log(
            Level::Warn,
            &format!("{task} abandoned this run: {reason}"),
            &[
                ("org", Value::from(dataset.org.as_str())),
                ("stream", Value::from(dataset.stream.as_str())),
                ("dataset", Value::from(dataset.to_string())),
                ("reason", Value::from(reason)),
            ],
        );
    }
}

/// 决定桶状态的两个配置项（REQ-AMB-STO-BUCKET-LIFECYCLE）。
fn bucket_lifecycle(cfg: &Config) -> BucketLifecycle {
    BucketLifecycle {
        seal_grace: cfg.seal_grace,
        lake_watermark: cfg.lake_watermark,
    }
}
