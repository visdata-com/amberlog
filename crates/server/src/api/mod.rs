//! `/api/` 下的端点：一个模块一个文件，各自注册路由，经 [`routes`] 交给 [`crate::gateway::app`]。
//!
//! 挂载约定见 [`crate::gateway::app`]。模块只做 HTTP 一侧的适配（路径参数、请求体字节、应答组装），
//! 业务交给 amberlog-ingest 与 amberlog-query；摄取管道与治理计数的输入不含 HTTP 概念。

pub mod body;
pub mod fields;
pub mod ingest;
pub mod namespace;
pub mod search;

use std::collections::HashMap;
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_ingest::counters::Counters;
use amberlog_ingest::timestamp::TimestampWindow;
use amberlog_query::files::DataFiles;
use amberlog_query::limits::Admission;
use amberlog_query::request::Limits;
use amberlog_query::system::AlertHistory;
use amberlog_query::system::backfill::BackfillTracker;
use amberlog_storage::chain::{ChainReader, SnapshotId};
use amberlog_storage::cold::ColdReader;
use amberlog_storage::commit::Committer;
use amberlog_storage::gate::DatasetGates;
use amberlog_storage::lifecycle::Lifecycle;
use amberlog_storage::memtable::{MemtableView, MemtableWriter};
use amberlog_storage::registry::DatasetRegistry;
use amberlog_storage::wal::Wal;
use axum::Router;

use crate::config::RetentionPeriod;
use crate::gateway::AppState;

/// 各模块用到的存储侧句柄，全部是 amberlog-storage（与查询读数据文件的）接口；测试里换成替身。
#[derive(Clone)]
pub struct Storage {
    pub wal: Arc<dyn Wal>,
    pub memtable: Arc<dyn MemtableView>,
    pub committer: Arc<dyn Committer>,
    pub chain: Arc<dyn ChainReader>,
    pub gates: Arc<DatasetGates>,
    /// 可见集的写入一侧（摄取）。
    pub writer: Arc<dyn MemtableWriter>,
    /// 数据集的存在持久到了哪一步。
    pub registry: Arc<DatasetRegistry>,
    /// 查询读数据文件。
    pub files: Arc<dyn DataFiles>,
    /// 查询读冷层表（跨层查询的冷侧）。
    pub cold: Arc<dyn ColdReader>,
    /// 治理计数（`_sent` 与计数增量）。
    pub counters: Arc<Counters>,
    /// 已写出的评估记录（`_alert_evaluations` 的行集，REQ-AMB-ALR-EVALUATIONS）。
    pub alerts: Arc<dyn AlertHistory>,
}

/// 各模块的受理配置与启动时读到的状态。
#[derive(Debug, Clone, Default)]
pub struct Settings {
    /// `ingest.max_body_bytes`。
    pub max_body_bytes: u64,
    /// `ingest.max_columns`。
    pub max_columns: usize,
    /// `ingest.max_future` 与 `ingest.max_past`。
    pub window: TimestampWindow,
    /// `query.default_size` 与 `query.max_size`。
    pub query: Limits,
    /// 启动时读到的各数据集 schema 基线快照号（摄取管道据此记基线快照号，ADR-0021 第 4 条）。
    pub baselines: HashMap<Dataset, Option<SnapshotId>>,
    /// `lifecycle.seal_grace` 与 `lifecycle.lake_watermark`（迟到记录的判定、台账判定桶状态）。
    pub lifecycle: Lifecycle,
    /// `retention.period`（台账的保留期与到期日）。
    pub retention: Option<RetentionPeriod>,
    /// `query.max_concurrency` 与 `query.max_memory_bytes`：`_search` 的执行名额与单查询内存上限（全进程一份，
    /// `/metrics` 读同一份）。
    pub admission: Arc<Admission>,
    /// 漏采判定的补发中状态（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL）：摄取端点记下迟到的确认与 503，
    /// 对账表、台账与后台判定读它；宽限期 `reconciliation.grace` 随它。全进程一份。
    pub backfill: Arc<BackfillTracker>,
}

/// 全部 `/api/` 模块的路由表。
pub fn routes(state: &AppState, storage: &Storage, settings: &Settings) -> Vec<Router> {
    vec![
        ingest::routes(state, storage, settings),
        search::routes(state, storage, settings),
        fields::routes(storage),
    ]
}
