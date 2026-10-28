//! 健康与指标（Telemetry）：`/healthz`、`/readyz`、`/metrics` 与 `ready` 事件（REQ-AMB-OBS-HEALTH-METRICS、
//! REQ-AMB-CFG-STARTUP-V2「稳定的启动事件」）。
//!
//! `/readyz` 与 `amberlog_ready` 由同一个函数求值：读后台对象存储探测的最近结果、当场检查 `wal.dir` 可写、
//! 读停机标志；请求路径上不访问对象存储。

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_common::error::Reason;
use amberlog_query::limits::Admission;
use amberlog_query::system::background::Background;
use amberlog_query::system::verdict::Verdict;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::layout::HourBucket;
use amberlog_storage::lifecycle::{self, BucketState, Lifecycle};
use amberlog_storage::memtable::Memtable;
use amberlog_storage::reader::StoreChain;
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::store::Store;
use axum::Json;
use axum::extract::State;
use axum::http::{HeaderValue, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value, json};

use crate::alerting::metrics::AlertCounters;
use crate::gateway::ApiError;
use crate::logging::{self, Level};
use crate::version;

/// 对象存储就绪探测的周期；单次探测超过一个周期未完成即算失败（块上限 30 秒）。
pub const PROBE_INTERVAL: Duration = Duration::from_secs(10);

/// `wal.dir` 是否可写：在其下新建并删除一个临时文件。
pub fn check_writable(dir: &Path) -> std::io::Result<()> {
    static N: AtomicU64 = AtomicU64::new(0);
    let probe = dir.join(format!(
        ".amberlog-write-probe-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::write(&probe, b"")?;
    std::fs::remove_file(&probe)
}

/// 就绪状态。
pub struct Health {
    /// 写入实例为 `wal.dir`（就绪条件当场检查它可写）；只读目录模式为 `None`（不用 WAL，`wal` 恒为 true，
    /// REQ-AMB-OBS-HEALTH-METRICS-V5）。
    wal_dir: Option<PathBuf>,
    /// `ready` 事件的 `mode`：`serve` 或 `catalog`（REQ-AMB-CFG-STARTUP-V7）。
    mode: &'static str,
    object_store_ok: AtomicBool,
    shutting_down: AtomicBool,
    ready_logged: AtomicBool,
    http_addr: RwLock<Option<SocketAddr>>,
    /// 是否以 TLS 监听（`ready` 的 `tls`，REQ-AMB-ARCH-ENCRYPTION-READY-TLS）。
    tls: AtomicBool,
    object_store_url: String,
    /// 已通过停机闸门、尚未得出响应的 `/api/` 请求数（优雅停机排空的对象）。
    in_flight: AtomicUsize,
    idle: tokio::sync::Notify,
}

/// 一个已被受理的 `/api/` 请求；析构即不再在途。
pub struct Admitted(Arc<Health>);

impl Drop for Admitted {
    fn drop(&mut self) {
        if self.0.in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

/// 三项就绪条件的当前取值（`/readyz` 503 时的 `details.checks`）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Checks {
    pub wal: bool,
    pub object_store: bool,
    pub shutting_down: bool,
}

impl Checks {
    pub fn ready(self) -> bool {
        self.wal && self.object_store && !self.shutting_down
    }
}

impl Health {
    /// 写入实例的就绪状态。`object_store_ok` 为启动探测的结果（启动探测成功才会走到这里，故初始为 true）。
    pub fn new(wal_dir: PathBuf, object_store_url: String) -> Health {
        Health::with(Some(wal_dir), "serve", object_store_url)
    }

    /// 只读目录模式（REQ-AMB-STO-COLD-CATALOG）的就绪状态：就绪只取最近一次只读探测与停机标志，`wal` 恒为 true。
    pub fn catalog(object_store_url: String) -> Health {
        Health::with(None, "catalog", object_store_url)
    }

    fn with(wal_dir: Option<PathBuf>, mode: &'static str, object_store_url: String) -> Health {
        Health {
            wal_dir,
            mode,
            object_store_ok: AtomicBool::new(true),
            shutting_down: AtomicBool::new(false),
            ready_logged: AtomicBool::new(false),
            http_addr: RwLock::new(None),
            tls: AtomicBool::new(false),
            object_store_url,
            in_flight: AtomicUsize::new(0),
            idle: tokio::sync::Notify::new(),
        }
    }

    pub fn checks(&self) -> Checks {
        Checks {
            wal: self
                .wal_dir
                .as_deref()
                .is_none_or(|dir| check_writable(dir).is_ok()),
            object_store: self.object_store_ok.load(Ordering::SeqCst),
            shutting_down: self.shutting_down.load(Ordering::SeqCst),
        }
    }

    /// 停机开始时由生命周期编排置位（REQ-AMB-LIFE-DURABILITY）。
    pub fn begin_shutdown(&self) {
        self.shutting_down.store(true, Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutting_down.load(Ordering::SeqCst)
    }

    /// 停机闸门：未在停机时受理一个 `/api/` 或目录前缀下的请求并计入在途，停机中返回 `None`。
    /// 先计入再查停机标志，与 [`Health::drained`] 的「先置位再数在途」配对，受理与排空之间没有空隙。
    pub fn admit(self: &Arc<Self>) -> Option<Admitted> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        let admitted = Admitted(Arc::clone(self));
        (!self.is_shutting_down()).then_some(admitted)
    }

    /// 等在途的 `/api/` 请求全部结束，至多 `limit`；返回是否排空。须在 [`Health::begin_shutdown`] 之后调用。
    pub async fn drained(&self, limit: Duration) -> bool {
        tokio::time::timeout(limit, async {
            loop {
                let idle = self.idle.notified();
                if self.in_flight.load(Ordering::SeqCst) == 0 {
                    return;
                }
                idle.await;
            }
        })
        .await
        .is_ok()
    }

    /// 监听建立后记下实际地址与是否以 TLS 监听；此后才可能输出 `ready`。
    pub fn set_listening(&self, addr: SocketAddr, tls: bool) {
        self.tls.store(tls, Ordering::SeqCst);
        *self.http_addr.write().unwrap() = Some(addr);
    }

    /// 求值就绪；自进程启动后首次就绪（且监听已建立）时输出一次 `ready` 事件。
    pub fn evaluate(&self) -> Checks {
        let checks = self.checks();
        if checks.ready() {
            self.log_ready_once();
        }
        checks
    }

    fn log_ready_once(&self) {
        let Some(addr) = *self.http_addr.read().unwrap() else {
            return;
        };
        if self.ready_logged.swap(true, Ordering::SeqCst) {
            return;
        }
        logging::event(
            Level::Info,
            "ready",
            "amberlog is ready",
            &[
                ("version", Value::from(version::SEMVER)),
                ("http_addr", Value::from(addr.to_string())),
                ("object_store", Value::from(self.object_store_url.clone())),
                ("mode", Value::from(self.mode)),
                ("tls", Value::from(self.tls.load(Ordering::SeqCst))),
            ],
        );
    }

    /// 后台探测对象存储，每个周期一次；每次探测后顺带求值就绪（使 `ready` 不依赖有人来请求 `/readyz`）。写入实例写入并读回
    /// `_system/readyz-probe`；只读目录模式只做只读探测，不写任何对象（REQ-AMB-STO-COLD-CATALOG-RO-READY-PROBE）。
    pub fn spawn_probe(self: &Arc<Self>, store: Store) {
        let readonly = self.wal_dir.is_none();
        let health = Arc::clone(self);
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(PROBE_INTERVAL);
            tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                tick.tick().await;
                let probe = async {
                    if readonly {
                        store.probe_readonly().await
                    } else {
                        store.probe_readyz().await
                    }
                };
                let ok = matches!(
                    tokio::time::timeout(PROBE_INTERVAL, probe).await,
                    Ok(Ok(()))
                );
                health.object_store_ok.store(ok, Ordering::SeqCst);
                health.evaluate();
            }
        });
    }
}

pub async fn healthz() -> Json<Value> {
    Json(json!({"status": "ok"}))
}

pub async fn readyz(State(state): State<crate::gateway::AppState>) -> Response {
    let checks = state.health.evaluate();
    if checks.ready() {
        return Json(json!({"status": "ready"})).into_response();
    }
    let mut details = Map::new();
    details.insert(
        "checks".into(),
        json!({
            "wal": checks.wal,
            "object_store": checks.object_store,
            "shutting_down": checks.shutting_down,
        }),
    );
    ApiError::new(Reason::NotReady, "not ready")
        .with_details(details)
        .into_response()
}

/// `wal.dir` 字节数：其下（含子目录）全部普通文件的文件长度之和（REQ-AMB-ING-HOT-BACKPRESSURE），读不到的项不计。
pub fn wal_dir_bytes(dir: &Path) -> u64 {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    entries
        .filter_map(Result::ok)
        .map(|e| match e.path().symlink_metadata() {
            Ok(m) if m.is_file() => m.len(),
            Ok(m) if m.is_dir() => wal_dir_bytes(&e.path()),
            _ => 0,
        })
        .sum()
}

/// `wal.dir` 字节数的测量周期（块：至少每 10 秒一次）。
const WAL_WATCH_INTERVAL: Duration = Duration::from_secs(10);

/// 每 10 秒测量一次 `wal.dir` 字节数；某次测量超过 `alert_bytes`、而上一次不超过（启动后的首次测量视为上一次不超过）时
/// 输出稳定事件 `wal_backlog_high`，回落到不超过之前不再输出（REQ-AMB-ING-HOT-BACKPRESSURE）。
pub fn spawn_wal_watch(dir: PathBuf, alert_bytes: u64) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(WAL_WATCH_INTERVAL);
        let mut above = false;
        loop {
            tick.tick().await;
            let bytes = wal_dir_bytes(&dir);
            let now_above = bytes > alert_bytes;
            if now_above && !above {
                logging::event(
                    Level::Warn,
                    "wal_backlog_high",
                    "wal.dir has grown past wal.alert_bytes: confirmed records are piling up locally",
                    &[
                        ("wal_bytes", serde_json::Value::from(bytes)),
                        ("alert_bytes", serde_json::Value::from(alert_bytes)),
                    ],
                );
            }
            above = now_above;
        }
    })
}

/// 进程内指标：块列出的稳定指标，以及各块规定的非稳定指标；不启用任何库自带的进程级指标。
#[derive(Default)]
pub struct Metrics {
    datasets: RwLock<BTreeMap<(String, String), Arc<DatasetCounters>>>,
    /// `amberlog_wal_bytes` 当场统计的目录（`wal.dir`）。
    wal_dir: OnceLock<PathBuf>,
    /// `amberlog_backlog_memory_bytes` 取值的可见集；写入实例在开始受理请求之前接上（REQ-AMB-ING-HOT-BACKPRESSURE-V5）。
    backlog: OnceLock<Arc<Memtable>>,
    /// 现算指标（数据集停用、桶状态）读取的存储侧状态；启动装配存储后接上。
    storage: OnceLock<StorageView>,
    /// `_search` 的执行名额（`amberlog_search_running`）；写入实例在开始受理请求之前接上，接上后两个查询资源上限指标
    /// 才输出（REQ-AMB-QRY-RESOURCE-LIMITS、REQ-AMB-OBS-HEALTH-METRICS-V5）。
    search: OnceLock<Arc<Admission>>,
    /// 后台判定的结论与存在的数据集（`amberlog_reconciliation_windows`）；写入实例在开始响应 `/metrics` 之前接上，
    /// 只读目录模式与离线子命令不接（REQ-AMB-GOV-MISSED-COLLECTION-GAUGE-INIT、SERVE-ONLY）。
    reconciliation: OnceLock<(Arc<Background>, Arc<DatasetRegistry>)>,
    /// `amberlog_search_rejected_total{reason="too_many_queries"}`。
    search_rejected_concurrency: AtomicU64,
    /// `amberlog_search_rejected_total{reason="query_memory_exceeded"}`。
    search_rejected_memory: AtomicU64,
    /// `amberlog_catalog_requests_total{endpoint,code}`（非稳定，REQ-AMB-STO-COLD-CATALOG-METRIC）：只有出现过的序列。
    catalog_requests: RwLock<BTreeMap<(&'static str, u16), u64>>,
    /// 只读目录模式：不渲染摄取与提交两个指标族（REQ-AMB-OBS-HEALTH-METRICS-V5，该模式不做这些事）。
    catalog_mode: bool,
    /// 告警的指标；写入实例在开始受理请求之前接上。
    alerting: OnceLock<AlertingView>,
}

/// `/metrics` 读取的告警状态：两个告警指标（REQ-AMB-ALR-RULES-EVAL-METRIC、REQ-AMB-ALR-WEBHOOK-DELIVERY-METRIC）与
/// 此刻已结束、尚未写进评估记录对象的评估记录条数（`amberlog_alert_history_unwritten`，REQ-AMB-ALR-EVALUATIONS-UNWRITTEN-METRIC）。
pub struct AlertingView {
    pub counters: Arc<AlertCounters>,
    pub unwritten: Arc<dyn Fn() -> u64 + Send + Sync>,
}

/// `/metrics` 被读取时现算的指标所读的存储侧状态。
pub struct StorageView {
    pub gates: Arc<DatasetGates>,
    pub chain: Arc<StoreChain>,
    pub lifecycle: Lifecycle,
}

#[derive(Default)]
struct DatasetCounters {
    accepted: AtomicU64,
    rejected: AtomicU64,
    commits_ok: AtomicU64,
    commits_failed: AtomicU64,
    late_sealed: AtomicU64,
    late_pending_lake: AtomicU64,
    compactions_ok: AtomicU64,
    compactions_aborted: AtomicU64,
    fsck_deleted_data_file: AtomicU64,
    fsck_deleted_bucket_manifest: AtomicU64,
    gc_deleted_snapshot: AtomicU64,
    gc_deleted_resurrected: AtomicU64,
    lake_rewrite_failures: AtomicU64,
}

impl Metrics {
    fn dataset(&self, org: &str, stream: &str) -> Arc<DatasetCounters> {
        let key = (org.to_owned(), stream.to_owned());
        if let Some(c) = self.datasets.read().unwrap().get(&key) {
            return Arc::clone(c);
        }
        Arc::clone(self.datasets.write().unwrap().entry(key).or_default())
    }

    /// 只读目录模式的指标：只有 `amberlog_build_info`、`amberlog_ready` 与目录请求计数。
    pub fn catalog() -> Metrics {
        Metrics {
            catalog_mode: true,
            ..Metrics::default()
        }
    }

    /// 接上现算指标所读的存储侧状态（只接一次）。
    pub fn attach_storage(&self, view: StorageView) {
        let _ = self.storage.set(view);
    }

    /// 接上 `_search` 的执行名额（只接一次）：此后 `/metrics` 输出 `amberlog_search_rejected_total` 的两个序列
    /// （尚未拒绝时为 0）与 `amberlog_search_running`。
    pub fn attach_search(&self, admission: Arc<Admission>) {
        let _ = self.search.set(admission);
    }

    /// 一个 `_search` 以错误信封作答：`reason` 为 `too_many_queries` 或 `query_memory_exceeded` 时计入
    /// `amberlog_search_rejected_total`，其他 `reason` 不计（REQ-AMB-QRY-RESOURCE-LIMITS-REJECTED-METRIC）。
    pub fn record_search_error(&self, reason: Reason) {
        match reason {
            Reason::TooManyQueries => {
                self.search_rejected_concurrency
                    .fetch_add(1, Ordering::Relaxed);
            }
            Reason::QueryMemoryExceeded => {
                self.search_rejected_memory.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// 目录前缀下一个已作答的请求：`endpoint` 为匹配的目录端点名或 `other`，`code` 为响应的状态码。
    pub fn record_catalog(&self, endpoint: &'static str, code: u16) {
        *self
            .catalog_requests
            .write()
            .unwrap()
            .entry((endpoint, code))
            .or_default() += 1;
    }

    /// 接上告警的指标（只接一次）：此后 `/metrics` 输出每条启用的规则的两个告警指标族与 `amberlog_alert_history_unwritten`。
    pub fn attach_alerting(&self, view: AlertingView) {
        let _ = self.alerting.set(view);
    }

    /// 接上 `amberlog_backlog_memory_bytes` 取值的可见集（只接一次）：此后 `/metrics` 输出它此刻的内存积压字节数。
    pub fn attach_backlog(&self, memtable: Arc<Memtable>) {
        let _ = self.backlog.set(memtable);
    }

    /// 接上后台判定（只接一次）：此后 `/metrics` 对每个存在的数据集输出 `amberlog_reconciliation_windows` 的六个序列，
    /// 第一次后台判定之前取值为 0。
    pub fn attach_reconciliation(
        &self,
        background: Arc<Background>,
        registry: Arc<DatasetRegistry>,
    ) {
        let _ = self.reconciliation.set((background, registry));
    }

    /// 接上 `amberlog_wal_bytes` 所统计的 `wal.dir`（只接一次）。
    pub fn attach_wal_dir(&self, dir: PathBuf) {
        let _ = self.wal_dir.set(dir);
    }

    /// `_json` 应答中的 `successful` 与 `failed`（REQ-AMB-OBS-HEALTH-METRICS「摄取计数」）。
    pub fn record_ingest(&self, org: &str, stream: &str, accepted: u64, rejected: u64) {
        let c = self.dataset(org, stream);
        c.accepted.fetch_add(accepted, Ordering::Relaxed);
        c.rejected.fetch_add(rejected, Ordering::Relaxed);
    }

    /// 一次提交的最终结果（REQ-AMB-OBS-HEALTH-METRICS「提交计数」；按周期去重由提交路径负责）。
    pub fn record_commit(&self, org: &str, stream: &str, ok: bool) {
        let c = self.dataset(org, stream);
        if ok {
            c.commits_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            c.commits_failed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 被确认的迟到记录，按接收时刻目标桶的状态（REQ-AMB-STO-BUCKET-LIFECYCLE「迟到记录」）。
    pub fn record_late(&self, org: &str, stream: &str, sealed: u64, pending_lake: u64) {
        if sealed == 0 && pending_lake == 0 {
            return;
        }
        let c = self.dataset(org, stream);
        c.late_sealed.fetch_add(sealed, Ordering::Relaxed);
        c.late_pending_lake
            .fetch_add(pending_lake, Ordering::Relaxed);
    }

    /// 一次合并的结果：提交成功为 `ok`，因摘要核对放弃为 `aborted`（REQ-AMB-STO-SEAL-COMPACTION）。
    pub fn record_compaction(&self, org: &str, stream: &str, ok: bool) {
        let c = self.dataset(org, stream);
        if ok {
            c.compactions_ok.fetch_add(1, Ordering::Relaxed);
        } else {
            c.compactions_aborted.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 入湖发生一次重写失败（`amberlog_lake_rewrite_failures_total`，非稳定，REQ-AMB-STO-LAKE-ENTRY-V2-REWRITE-METRIC）。
    pub fn record_lake_rewrite_failure(&self, org: &str, stream: &str) {
        self.dataset(org, stream)
            .lake_rewrite_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// 巡检删除了一个对象（REQ-AMB-STO-HOT-FSCK「计数」）：`data_file` 为真即数据文件，否则为桶 manifest。
    /// 快照回收删除了一个快照文件（`amberlog_snapshot_gc_deleted_total`，非稳定，REQ-AMB-STO-SNAPSHOT-GC-METRICS）。
    pub fn record_snapshot_gc_deleted(&self, org: &str, stream: &str, resurrected: bool) {
        let c = self.dataset(org, stream);
        if resurrected {
            c.gc_deleted_resurrected.fetch_add(1, Ordering::Relaxed);
        } else {
            c.gc_deleted_snapshot.fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn record_fsck_deleted(&self, org: &str, stream: &str, data_file: bool) {
        let c = self.dataset(org, stream);
        if data_file {
            c.fsck_deleted_data_file.fetch_add(1, Ordering::Relaxed);
        } else {
            c.fsck_deleted_bucket_manifest
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Prometheus 文本暴露格式 0.0.4。
    pub fn render(&self, ready: bool) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# HELP amberlog_build_info Build information.");
        let _ = writeln!(out, "# TYPE amberlog_build_info gauge");
        let _ = writeln!(
            out,
            "amberlog_build_info{{version=\"{}\",commit=\"{}\"}} 1",
            escape(version::SEMVER),
            escape(version::COMMIT)
        );

        let datasets = self.datasets.read().unwrap();
        let counter_family = |out: &mut String,
                              name: &str,
                              help: &str,
                              label: &str,
                              pick: &dyn Fn(&DatasetCounters) -> Vec<(&'static str, u64)>,
                              always: bool| {
            let mut lines = String::new();
            for ((org, stream), c) in datasets.iter() {
                let values = pick(c);
                if !always && values.iter().all(|(_, n)| *n == 0) {
                    continue;
                }
                for (value, n) in values {
                    let _ = writeln!(
                        lines,
                        "{name}{{org=\"{}\",stream=\"{}\",{label}=\"{value}\"}} {n}",
                        escape(org),
                        escape(stream),
                    );
                }
            }
            if always || !lines.is_empty() {
                let _ = writeln!(out, "# HELP {name} {help}");
                let _ = writeln!(out, "# TYPE {name} counter");
                out.push_str(&lines);
            }
        };
        let load = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let writer = !self.catalog_mode;
        counter_family(
            &mut out,
            "amberlog_ingest_records_total",
            "Records answered by _json, by per-record outcome.",
            "outcome",
            &|c| {
                vec![
                    ("accepted", load(&c.accepted)),
                    ("rejected", load(&c.rejected)),
                ]
            },
            writer,
        );
        counter_family(
            &mut out,
            "amberlog_manifest_commits_total",
            "Snapshot commits by final result.",
            "result",
            &|c| {
                vec![
                    ("ok", load(&c.commits_ok)),
                    ("failed", load(&c.commits_failed)),
                ]
            },
            writer,
        );
        let _ = writeln!(
            out,
            "# HELP amberlog_ready 1 when GET /readyz would answer 200, else 0."
        );
        let _ = writeln!(out, "# TYPE amberlog_ready gauge");
        let _ = writeln!(out, "amberlog_ready {}", u8::from(ready));

        if let Some(dir) = self.wal_dir.get() {
            let _ = writeln!(
                out,
                "# HELP amberlog_wal_bytes Total length of the regular files under wal.dir."
            );
            let _ = writeln!(out, "# TYPE amberlog_wal_bytes gauge");
            let _ = writeln!(out, "amberlog_wal_bytes {}", wal_dir_bytes(dir));
        }

        if let Some(memtable) = self.backlog.get() {
            let _ = writeln!(
                out,
                "# HELP amberlog_backlog_memory_bytes Bytes of uncommitted records held in memory, as counted against ingest.max_backlog_memory_bytes."
            );
            let _ = writeln!(out, "# TYPE amberlog_backlog_memory_bytes gauge");
            let _ = writeln!(
                out,
                "amberlog_backlog_memory_bytes {}",
                memtable.backlog_memory_bytes()
            );
        }

        let storage = self.storage.get();
        // 没有停用的数据集时不输出该指标族（REQ-AMB-OBS-HEALTH-METRICS-V3 两种做法都允许）
        let disabled = storage.map(|v| v.gates.disabled()).unwrap_or_default();
        if !disabled.is_empty() {
            let _ = writeln!(
                out,
                "# HELP amberlog_dataset_disabled 1 for each dataset disabled until restart, by the state it entered first."
            );
            let _ = writeln!(out, "# TYPE amberlog_dataset_disabled gauge");
            for (d, state) in &disabled {
                let _ = writeln!(
                    out,
                    "amberlog_dataset_disabled{{org=\"{}\",stream=\"{}\",state=\"{}\"}} 1",
                    escape(&d.org),
                    escape(&d.stream),
                    state.label().unwrap_or_default()
                );
            }
        }

        if let Some(view) = storage {
            render_lake_lag(&mut out, view, &disabled);
        }

        let search = self.search.get();
        if search.is_some() {
            let _ = writeln!(
                out,
                "# HELP amberlog_search_rejected_total _search requests refused by a query resource limit, by reason."
            );
            let _ = writeln!(out, "# TYPE amberlog_search_rejected_total counter");
            for (reason, n) in [
                (Reason::TooManyQueries, &self.search_rejected_concurrency),
                (Reason::QueryMemoryExceeded, &self.search_rejected_memory),
            ] {
                let _ = writeln!(
                    out,
                    "amberlog_search_rejected_total{{reason=\"{}\"}} {}",
                    reason.as_str(),
                    load(n)
                );
            }
        }

        // 非稳定指标
        counter_family(
            &mut out,
            "amberlog_late_records_total",
            "Late records acknowledged, by the state of their target bucket when received.",
            "route",
            &|c| {
                vec![
                    ("sealed", load(&c.late_sealed)),
                    ("pending_lake", load(&c.late_pending_lake)),
                ]
            },
            false,
        );
        if let Some(view) = storage {
            render_hot_buckets(&mut out, view, &disabled);
        }
        if let Some((background, registry)) = self.reconciliation.get() {
            render_reconciliation(&mut out, background, registry);
        }
        if let Some(admission) = search {
            let _ = writeln!(
                out,
                "# HELP amberlog_search_running _search requests holding an execution slot now."
            );
            let _ = writeln!(out, "# TYPE amberlog_search_running gauge");
            let _ = writeln!(out, "amberlog_search_running {}", admission.running());
        }
        if let Some(alerting) = self.alerting.get() {
            alerting.counters.render(&mut out);
            let _ = writeln!(
                out,
                "# HELP amberlog_alert_history_unwritten Finished alert evaluations whose records are not yet written."
            );
            let _ = writeln!(out, "# TYPE amberlog_alert_history_unwritten gauge");
            let _ = writeln!(
                out,
                "amberlog_alert_history_unwritten {}",
                (alerting.unwritten)()
            );
        }
        counter_family(
            &mut out,
            "amberlog_compactions_total",
            "Compactions of sealed buckets, by result.",
            "result",
            &|c| {
                vec![
                    ("ok", load(&c.compactions_ok)),
                    ("aborted", load(&c.compactions_aborted)),
                ]
            },
            false,
        );
        counter_family(
            &mut out,
            "amberlog_hot_fsck_deleted_total",
            "Objects deleted by the hot-tier check, by kind.",
            "kind",
            &|c| {
                vec![
                    ("data_file", load(&c.fsck_deleted_data_file)),
                    ("bucket_manifest", load(&c.fsck_deleted_bucket_manifest)),
                ]
            },
            false,
        );
        let catalog = self.catalog_requests.read().unwrap();
        if !catalog.is_empty() {
            let _ = writeln!(
                out,
                "# HELP amberlog_catalog_requests_total Requests answered under /iceberg/, by catalog endpoint and status code."
            );
            let _ = writeln!(out, "# TYPE amberlog_catalog_requests_total counter");
            for ((endpoint, code), n) in catalog.iter() {
                let _ = writeln!(
                    out,
                    "amberlog_catalog_requests_total{{endpoint=\"{endpoint}\",code=\"{code}\"}} {n}"
                );
            }
        }
        counter_family(
            &mut out,
            "amberlog_snapshot_gc_deleted_total",
            "Snapshot files deleted by hot-tier snapshot collection, by kind.",
            "kind",
            &|c| {
                vec![
                    ("snapshot", load(&c.gc_deleted_snapshot)),
                    ("resurrected_snapshot", load(&c.gc_deleted_resurrected)),
                ]
            },
            false,
        );
        // 非稳定，没有第三个标签：尚未发生重写失败的数据集不输出
        let mut lines = String::new();
        for ((org, stream), c) in datasets.iter() {
            let n = load(&c.lake_rewrite_failures);
            if n > 0 {
                let _ = writeln!(
                    lines,
                    "amberlog_lake_rewrite_failures_total{{org=\"{}\",stream=\"{}\"}} {n}",
                    escape(org),
                    escape(stream),
                );
            }
        }
        if !lines.is_empty() {
            let _ = writeln!(
                out,
                "# HELP amberlog_lake_rewrite_failures_total Lake entries that failed to rewrite an hour bucket into the cold tier."
            );
            let _ = writeln!(out, "# TYPE amberlog_lake_rewrite_failures_total counter");
            out.push_str(&lines);
        }
        out
    }
}

/// `amberlog_lake_lag_seconds`（稳定）：各数据集按本进程已知最新快照、此刻的入湖积压时长，不访问对象存储；
/// 停用的数据集不输出（REQ-AMB-STO-LAKE-ENTRY-LAG-METRIC、REQ-AMB-OBS-HEALTH-METRICS-V4）。
fn render_lake_lag(out: &mut String, view: &StorageView, disabled: &[(Dataset, impl Sized)]) {
    let now = lifecycle::now_us();
    let mut lines = String::new();
    for (d, snapshot) in view.chain.latest_snapshots() {
        if disabled.iter().any(|(x, _)| *x == d) {
            continue;
        }
        let lag = amberlog_storage::lake::lag_seconds(&snapshot, &view.lifecycle, now);
        let _ = writeln!(
            lines,
            "amberlog_lake_lag_seconds{{org=\"{}\",stream=\"{}\"}} {lag}",
            escape(&d.org),
            escape(&d.stream),
        );
    }
    if !lines.is_empty() {
        let _ = writeln!(
            out,
            "# HELP amberlog_lake_lag_seconds Seconds the oldest hot-tier hour bucket past the lake watermark has waited to enter the cold tier."
        );
        let _ = writeln!(out, "# TYPE amberlog_lake_lag_seconds gauge");
        out.push_str(&lines);
    }
}

/// `amberlog_hot_buckets`：各数据集已知最新快照中 `record_count` 大于 0 的桶按此刻的状态计数，三个状态都输出；
/// 停用的数据集不输出（REQ-AMB-STO-BUCKET-LIFECYCLE「入湖桩」）。
fn render_hot_buckets(out: &mut String, view: &StorageView, disabled: &[(Dataset, impl Sized)]) {
    let now = lifecycle::now_us();
    let mut lines = String::new();
    for (d, snapshot) in view.chain.latest_snapshots() {
        if disabled.iter().any(|(x, _)| *x == d) {
            continue;
        }
        let mut counts = [0u64; 3];
        for b in snapshot.buckets.iter().filter(|b| b.record_count > 0) {
            let Some(hour) = HourBucket::parse(&b.bucket) else {
                continue;
            };
            let i = BucketState::ALL
                .iter()
                .position(|s| *s == view.lifecycle.state(hour, now))
                .expect("三种状态之一");
            counts[i] += 1;
        }
        for (state, n) in BucketState::ALL.iter().zip(counts) {
            let _ = writeln!(
                lines,
                "amberlog_hot_buckets{{org=\"{}\",stream=\"{}\",state=\"{state}\"}} {n}",
                escape(&d.org),
                escape(&d.stream),
            );
        }
    }
    if !lines.is_empty() {
        let _ = writeln!(
            out,
            "# HELP amberlog_hot_buckets Hot-tier hour buckets holding records, by current state."
        );
        let _ = writeln!(out, "# TYPE amberlog_hot_buckets gauge");
        out.push_str(&lines);
    }
}

/// `amberlog_reconciliation_windows{org,stream,verdict}`（稳定，REQ-AMB-GOV-MISSED-COLLECTION-GAUGE）：最近一次后台判定中
/// 各数据集被计算判定的窗口按判定的个数；每个存在的数据集（含停用的）恰六个序列。
fn render_reconciliation(out: &mut String, background: &Background, registry: &DatasetRegistry) {
    let mut lines = String::new();
    for d in registry.datasets() {
        let counts = background.gauge(&d);
        for verdict in Verdict::ALL {
            let _ = writeln!(
                lines,
                "amberlog_reconciliation_windows{{org=\"{}\",stream=\"{}\",verdict=\"{}\"}} {}",
                escape(&d.org),
                escape(&d.stream),
                verdict.as_str(),
                counts[verdict.index()],
            );
        }
    }
    if !lines.is_empty() {
        let _ = writeln!(
            out,
            "# HELP amberlog_reconciliation_windows Windows of the last 24 hours by the verdict of the latest background reconciliation, computed by the grace period."
        );
        let _ = writeln!(out, "# TYPE amberlog_reconciliation_windows gauge");
        out.push_str(&lines);
    }
}

/// 标签值转义：反斜杠、双引号、换行。
fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}

pub async fn metrics(State(state): State<crate::gateway::AppState>) -> Response {
    let ready = state.health.evaluate().ready();
    let mut res = state.metrics.render(ready).into_response();
    res.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; version=0.0.4; charset=utf-8"),
    );
    res
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_only_stable_metrics() {
        let m = Metrics::default();
        m.record_ingest("default", "m", 1, 1);
        m.record_commit("default", "m", true);
        let text = m.render(true);
        let names: std::collections::BTreeSet<&str> = text
            .lines()
            .filter(|l| !l.starts_with('#'))
            .map(|l| l.split(['{', ' ']).next().unwrap())
            .collect();
        assert_eq!(
            names.into_iter().collect::<Vec<_>>(),
            vec![
                "amberlog_build_info",
                "amberlog_ingest_records_total",
                "amberlog_manifest_commits_total",
                "amberlog_ready"
            ]
        );
        assert!(text.contains(
            "amberlog_ingest_records_total{org=\"default\",stream=\"m\",outcome=\"accepted\"} 1"
        ));
        assert!(text.contains(
            "amberlog_manifest_commits_total{org=\"default\",stream=\"m\",result=\"failed\"} 0"
        ));
        assert!(text.contains("amberlog_ready 1\n"));
    }

    #[test]
    fn search_limit_metrics_after_attach() {
        let m = Metrics::default();
        assert!(!m.render(true).contains("amberlog_search_"));
        m.attach_search(Admission::new(4, 1 << 20));
        let text = m.render(true);
        assert!(text.contains("amberlog_search_rejected_total{reason=\"too_many_queries\"} 0\n"));
        assert!(
            text.contains("amberlog_search_rejected_total{reason=\"query_memory_exceeded\"} 0\n")
        );
        assert!(text.contains("amberlog_search_running 0\n"));
        m.record_search_error(Reason::TooManyQueries);
        m.record_search_error(Reason::InvalidSql);
        let text = m.render(true);
        assert!(text.contains("amberlog_search_rejected_total{reason=\"too_many_queries\"} 1\n"));
        assert!(
            text.contains("amberlog_search_rejected_total{reason=\"query_memory_exceeded\"} 0\n")
        );
    }

    #[test]
    fn label_escaping() {
        assert_eq!(escape("a\"b\\c\nd"), "a\\\"b\\\\c\\nd");
    }
}
