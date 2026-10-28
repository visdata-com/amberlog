//! 后台判定的周期任务（REQ-AMB-GOV-MISSED-COLLECTION C 组）：只由写入实例的启动流程（`startup/serve.rs`）构造并启动，
//! 自开始受理 `/api/` 请求起每 [`PERIOD`] 开始一次（块：每隔至多 60 秒），进入停机过程时由启动流程中止——
//! 不再开始新的一次，进行中的一次随之放弃，不延长停机过程（SHUTDOWN）。只读目录模式与离线子命令不构造它（SERVE-ONLY）。
//!
//! 每一次的计算与内存结论在 amberlog-query 的治理视图（[`Background`]）；这里只管节拍，以及把两条稳定事件交给日志出口。
//! 后台判定在自己的任务里运行，不经 `_search`、不取查询执行名额，读取失败不进入就绪判定。

use std::sync::Arc;
use std::time::Duration;

use amberlog_common::dataset::Dataset;
use amberlog_query::system::background::{Background, VerdictEvents};
use amberlog_storage::lifecycle;
use serde_json::Value;

use crate::logging::{self, Level};

/// 两次后台判定开始的间隔（不对外、不设配置键）。
pub const PERIOD: Duration = Duration::from_secs(30);

/// 以 [`PERIOD`] 为周期做后台判定，直到任务被中止。一次做得比周期长时，下一次在它结束后立即开始。
pub fn spawn(background: Arc<Background>, period: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let started = tokio::time::Instant::now();
            background
                .round(lifecycle::now_us(), lifecycle::now_us)
                .await;
            tokio::time::sleep_until(started + period).await;
        }
    })
}

/// 两条稳定事件经不受 `log.level` 过滤的出口输出（REQ-AMB-CFG-STARTUP-V8 登记的字段）。
pub struct LogEvents;

impl VerdictEvents for LogEvents {
    fn missing(&self, dataset: &Dataset, window_start: i64, sources: &str, missing: u64) {
        logging::event(
            Level::Warn,
            "reconciliation_missing",
            "after the grace period, the stored timestamp-based count of this window is lower than the sent count reported by these sources",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("window_start", Value::from(window_start)),
                ("sources", Value::from(sources)),
                ("missing", Value::from(missing)),
            ],
        );
    }

    fn silent(&self, dataset: &Dataset, source: &str, last_window_start: i64) {
        logging::event(
            Level::Warn,
            "source_silent",
            "the source has not reported a timestamp-based sent count for the minute after its last report, past the grace period",
            &[
                ("dataset", Value::from(dataset.to_string())),
                ("source", Value::from(source)),
                ("last_window_start", Value::from(last_window_start)),
            ],
        );
    }
}

#[cfg(test)]
mod tests;
