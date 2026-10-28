//! 留痕归档（REQ-AMB-STO-HOT-LAYOUT-V6「留痕归档段的写出节奏」）：不再含存活数据文件的桶，在变空之后 48 小时内
//! 经一次归档提交移入留痕归档段；同一快照沿链可达的任两段的 `created_at` 不在同一 UTC 日。V6 的例外（已退役的存储前缀在
//! 退役记录存在期间不归档）不需要这里判断：退役记录存在时 `serve` 拒绝启动，本工作根本不运行（REQ-AMB-LIFE-RETIREMENT）；
//! 退役记录被删除（人工复活）后，下面的节奏使变空已久的桶在开始受理 `/api/` 请求起约 1 天 1 分钟之内归档，在 V6 所要求的
//! 「自开始受理起 48 小时内」之内。
//!
//! 桶生命周期的一项周期工作（[`BucketTask`]），每分钟一轮，对每个存在且正常的数据集：本进程已知最新快照的 `retired_segment`
//! 所指的段（最新一段）写于今天（UTC）则本轮不归档；否则把 `buckets` 中 `record_count` 为 0、变空已满
//! [`MIN_EMPTY`] 的桶一次归档。变空时刻取该桶 manifest 中使存活文件离开的留痕（`orphan` 以外）的 `deleted_at` 最大值，
//! 即最后一个存活文件离开的那个快照的 `created_at`；只有 `orphan` 留痕的桶自其最早一条留痕起为空。这样一个桶自变空起
//! 至多等 [`MIN_EMPTY`] 加一天就被归档，在 48 小时之内。
//!
//! 为归档读取桶 manifest 走提交的读路径：读到不存在或内容无法解析即使数据集进入 manifest 不可读情形（与其他提交同口径）。
//! 读最新一段不改变数据集状态，读不到只结束本轮。停用的数据集不归档；停机过程开始后不再开始新的一轮。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use amberlog_common::dataset::Dataset;

use crate::BoxFuture;
use crate::chain::DeletionReason;
use crate::commit::{ArchiveRequest, HotCommits};
use crate::committer::CommitError;
use crate::flusher::Flusher;
use crate::gate::DatasetState;
use crate::layout::HourBucket;
use crate::maintenance::BucketTask;
use crate::registry::Persistence;

/// 两轮之间的间隔。
const PERIOD: Duration = Duration::from_secs(60);
/// 桶变空之后至少过这么久才归档：迟到记录与延后物理删除先有机会看到它（延后物理删除读最新快照中各桶的留痕）。
pub const MIN_EMPTY: Duration = Duration::from_secs(3600);

const DAY_US: i64 = 86_400_000_000;

/// 留痕归档的驱动。
pub struct Archiver {
    flusher: Arc<Flusher>,
    min_empty: Duration,
    stopping: AtomicBool,
}

impl BucketTask for Archiver {
    fn period(&self) -> Duration {
        PERIOD
    }

    fn run_once(&self) -> BoxFuture<'_, ()> {
        Box::pin(async move {
            let chain = self.flusher.committer().chain();
            for dataset in chain.registry.datasets() {
                if self.stopping.load(Ordering::SeqCst) {
                    return;
                }
                if chain.registry.persistence(&dataset) != Some(Persistence::Stored)
                    || chain.gates.state(&dataset) != DatasetState::Normal
                {
                    continue;
                }
                self.archive(&dataset).await;
            }
        })
    }

    fn stop(&self) {
        self.stopping.store(true, Ordering::SeqCst);
    }
}

impl Archiver {
    /// `min_empty`：桶变空之后至少过这么久才归档，运行时取 [`MIN_EMPTY`]。
    pub fn new(flusher: Arc<Flusher>, min_empty: Duration) -> Archiver {
        Archiver {
            flusher,
            min_empty,
            stopping: AtomicBool::new(false),
        }
    }

    async fn archive(&self, dataset: &Dataset) {
        let committer = self.flusher.committer();
        let chain = committer.chain();
        let Some((snapshot, _pin)) = chain.pin_latest(dataset) else {
            return;
        };
        let empty: Vec<_> = snapshot
            .buckets
            .iter()
            .filter(|b| b.record_count == 0)
            .collect();
        if empty.is_empty() {
            return;
        }
        let now = crate::lifecycle::now_us();
        if let Some(r) = &snapshot.retired_segment {
            let Ok(latest) = chain.read_retired_segment(dataset, r).await else {
                return;
            };
            if latest.created_at.div_euclid(DAY_US) >= now.div_euclid(DAY_US) {
                return;
            }
        }
        let min_empty = i64::try_from(self.min_empty.as_micros()).unwrap_or(i64::MAX);
        let mut buckets = Vec::new();
        for b in empty {
            let Ok(m) = chain.load_bucket_manifest(dataset, &snapshot, b).await else {
                // 读到 (a) 所列者已使数据集停用；不可访问留给下一轮
                return;
            };
            let emptied = m
                .deletions
                .iter()
                .filter(|d| d.reason != DeletionReason::Orphan)
                .map(|d| d.deleted_at)
                .max()
                .or_else(|| m.deletions.iter().map(|d| d.deleted_at).min());
            if let Some(at) = emptied
                && now.saturating_sub(at) >= min_empty
                && let Some(hour) = HourBucket::parse(&b.bucket)
            {
                buckets.push(hour);
            }
        }
        if buckets.is_empty() || self.stopping.load(Ordering::SeqCst) {
            return;
        }
        match committer
            .commit_archive(dataset, ArchiveRequest { buckets })
            .await
        {
            Ok(_) => self.flusher.record_ok(dataset),
            Err(CommitError::Exhausted | CommitError::Disabled(_)) => {
                self.flusher.record_failed(dataset)
            }
            // 不可访问就地计一次失败（REQ-AMB-OBS-HEALTH-METRICS-V4）；不可访问、WAL 写不进或桶已不为空：下一轮重新判定
            Err(CommitError::Unavailable) => self.flusher.record_background_unavailable(dataset),
            Err(CommitError::Wal(_) | CommitError::Stale) => {}
        }
    }
}
