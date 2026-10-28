//! 评估记录对象的保留期删除（REQ-AMB-ALR-EVALUATIONS C 组）：由告警自己的后台任务做，不由热层巡检做（ADR-0051 第 8 条）。

use std::time::Duration;

use amberlog_storage::lifecycle::now_us;
use amberlog_storage::store::Store;

use super::object::{self, ObjectKey};
use super::timed;

/// 一轮检查：列举 `_system/alerts/` 之下的全部对象，删除路径合模板、且计划时刻所在小时的结束时刻早于「此刻减
/// `retention`」的对象（不看规则此刻是否仍在配置中，REQ-AMB-ALR-EVALUATIONS-RETENTION）。不合模板的对象不动
/// （-RETENTION-ONLY），保留期内的不删（-RETENTION-KEEP）。遇对象存储不可访问即结束本轮，下一轮再来。
pub(super) async fn sweep(store: &Store, retention: Duration) {
    let Some(Ok(paths)) = timed(store.system_list(object::ROOT)).await else {
        return;
    };
    let cutoff = now_us().saturating_sub(i64::try_from(retention.as_micros()).unwrap_or(i64::MAX));
    for path in paths {
        let Some(key) = ObjectKey::parse(&path) else {
            continue;
        };
        if key.hour_end() < cutoff {
            let Some(Ok(_)) = timed(store.system_delete(&path)).await else {
                return;
            };
        }
    }
}
