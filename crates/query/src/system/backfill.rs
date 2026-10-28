//! 补发中的状态（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL、TERM-LATE-RECORD、TERM-LATE-REPORT）：只在本进程内存，
//! 不写进对象存储或 `wal.dir`（NOT-PERSISTED）。
//!
//! 按块的三种情形分别记：① 按 (数据集, 来源) 记最近一次确认迟到记录或受理迟到的按时间戳发送数上报的时刻；
//! ② 按数据集记最近一次对 `_json` 或 `_sent` 回 503 的时刻（不论来源，只看本进程的应答）；③ 本进程开始受理
//! `/api/` 请求的时刻。迟到与否在系统接收该请求之时，用 Amberlog 自己的时钟判定。由接入面在应答时记下。

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicI64, Ordering};

use amberlog_common::dataset::Dataset;

use super::verdict::{Backfill, MINUTE_US};

/// 补发中的状态。
#[derive(Debug)]
pub struct BackfillTracker {
    grace_us: i64,
    /// 开始受理 `/api/` 请求的时刻；尚未开始为 `i64::MAX`。
    accepting_since: AtomicI64,
    late: Mutex<HashMap<(Dataset, String), i64>>,
    unavailable: Mutex<HashMap<Dataset, i64>>,
}

impl BackfillTracker {
    pub fn new(grace_us: i64) -> BackfillTracker {
        BackfillTracker {
            grace_us,
            accepting_since: AtomicI64::new(i64::MAX),
            late: Mutex::new(HashMap::new()),
            unavailable: Mutex::new(HashMap::new()),
        }
    }

    /// 宽限期，微秒。
    pub fn grace_us(&self) -> i64 {
        self.grace_us
    }

    /// 本进程开始受理 `/api/` 请求的时刻。
    pub fn set_accepting_since(&self, at: i64) {
        self.accepting_since.store(at, Ordering::Relaxed);
    }

    pub fn accepting_since(&self) -> Option<i64> {
        let at = self.accepting_since.load(Ordering::Relaxed);
        (at != i64::MAX).then_some(at)
    }

    /// 一个带来源标识的 `_json` 请求被 200 确认（时刻 `at`）：被接受的记录中最早的 `_timestamp` 为 `earliest_us`，
    /// 请求的接收时刻为 `received_at`。有迟到记录（所在分钟结束加 G 早于接收时刻）时记下。
    pub fn note_records(
        &self,
        dataset: &Dataset,
        source: &str,
        earliest_us: i64,
        received_at: i64,
        at: i64,
    ) {
        let window_end = earliest_us.div_euclid(MINUTE_US) * MINUTE_US + MINUTE_US;
        if window_end.saturating_add(self.grace_us) < received_at {
            self.note_late(dataset, source, at);
        }
    }

    /// 一个 `_sent` 请求被 200 受理（时刻 `at`）：其中按时间戳的元素各为 (来源, `window_start`)，请求的接收时刻为
    /// `received_at`。`window_end` 加 G 早于接收时刻的元素是迟到的发送数上报，按其来源记下。
    pub fn note_reports<'a>(
        &self,
        dataset: &Dataset,
        timestamped: impl IntoIterator<Item = (&'a str, i64)>,
        received_at: i64,
        at: i64,
    ) {
        for (source, window_start) in timestamped {
            if (window_start + MINUTE_US).saturating_add(self.grace_us) < received_at {
                self.note_late(dataset, source, at);
            }
        }
    }

    fn note_late(&self, dataset: &Dataset, source: &str, at: i64) {
        let mut late = self.late.lock().expect("补发中状态未中毒");
        let v = late
            .entry((dataset.clone(), source.to_owned()))
            .or_insert(at);
        *v = (*v).max(at);
    }

    /// 对发往 `dataset` 的 `_json` 或 `_sent` 请求以 HTTP 503 应答（时刻 `at`）。
    pub fn note_unavailable(&self, dataset: &Dataset, at: i64) {
        let mut m = self.unavailable.lock().expect("补发中状态未中毒");
        let v = m.entry(dataset.clone()).or_insert(at);
        *v = (*v).max(at);
    }
}

/// 缺省的宽限期 5 分钟（`reconciliation.grace` 的缺省值）。
impl Default for BackfillTracker {
    fn default() -> Self {
        BackfillTracker::new(300_000_000)
    }
}

impl Backfill for BackfillTracker {
    fn backfilling(&self, dataset: &Dataset, source: &str, t: i64) -> bool {
        let within = |at: i64| at > t - self.grace_us && at <= t;
        if t < self
            .accepting_since
            .load(Ordering::Relaxed)
            .saturating_add(self.grace_us)
        {
            return true;
        }
        if self
            .unavailable
            .lock()
            .expect("补发中状态未中毒")
            .get(dataset)
            .is_some_and(|at| within(*at))
        {
            return true;
        }
        self.late
            .lock()
            .expect("补发中状态未中毒")
            .get(&(dataset.clone(), source.to_owned()))
            .is_some_and(|at| within(*at))
    }
}
