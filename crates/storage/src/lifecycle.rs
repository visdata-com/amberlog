//! 小时桶的状态（REQ-AMB-STO-BUCKET-LIFECYCLE「两个时刻」「三种状态」）。
//!
//! 桶的状态只由桶的时间范围、系统时钟与 `lifecycle.seal_grace`、`lifecycle.lake_watermark` 的当前取值推算，
//! 不写进快照或桶 manifest。摄取（迟到路由）、合并、指标与台账都经 [`Lifecycle::state`] 判定，不各自比较时刻。

use std::fmt;
use std::time::Duration;

use crate::layout::HourBucket;

/// 桶在某一时刻的状态。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BucketState {
    /// 早于封口时刻。
    Open,
    /// 不早于封口时刻、早于入湖水位时刻。
    Sealed,
    /// 不早于入湖水位时刻（已达入湖水位、待入湖）。
    PendingLake,
}

impl BucketState {
    /// 三种状态，按时间先后。
    pub const ALL: [BucketState; 3] = [
        BucketState::Open,
        BucketState::Sealed,
        BucketState::PendingLake,
    ];

    /// 指标标签 `state`／`route` 与台账里的写法。
    pub fn as_str(self) -> &'static str {
        match self {
            BucketState::Open => "open",
            BucketState::Sealed => "sealed",
            BucketState::PendingLake => "pending_lake",
        }
    }
}

impl fmt::Display for BucketState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// 决定桶状态的两个配置项。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lifecycle {
    /// `lifecycle.seal_grace`
    pub seal_grace: Duration,
    /// `lifecycle.lake_watermark`
    pub lake_watermark: Duration,
}

impl Default for Lifecycle {
    /// 两个配置项的默认值 `10m`、`72h`。
    fn default() -> Self {
        Lifecycle {
            seal_grace: Duration::from_secs(600),
            lake_watermark: Duration::from_secs(72 * 3600),
        }
    }
}

impl Lifecycle {
    /// 桶终点：该小时的起点加 1 小时（UTC 微秒）。
    pub fn bucket_end_us(bucket: HourBucket) -> i64 {
        bucket.end_us() + 1
    }

    /// 封口时刻：桶终点加 `lifecycle.seal_grace`。
    pub fn seal_at_us(&self, bucket: HourBucket) -> i64 {
        Self::bucket_end_us(bucket).saturating_add(micros(self.seal_grace))
    }

    /// 入湖水位时刻：桶终点加 `lifecycle.lake_watermark`。
    pub fn lake_at_us(&self, bucket: HourBucket) -> i64 {
        Self::bucket_end_us(bucket).saturating_add(micros(self.lake_watermark))
    }

    /// `bucket` 在 `now_us`（UTC 微秒）时刻的状态。
    pub fn state(&self, bucket: HourBucket, now_us: i64) -> BucketState {
        if now_us < self.seal_at_us(bucket) {
            BucketState::Open
        } else if now_us < self.lake_at_us(bucket) {
            BucketState::Sealed
        } else {
            BucketState::PendingLake
        }
    }
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

/// 系统时钟此刻的 UTC 微秒。桶状态、接收时刻与合并时机都取这个时钟。
///
/// 以 `fault-injection` feature 编译时，环境变量 `TESTBED_CLOCK_OFFSET`（带符号的秒数）给出加在系统时钟上的偏移，
/// 供测试把进程放到整点附近（REQ-AMB-STO-BUCKET-LIFECYCLE 的测试用）；不开该 feature 的构建里没有这段代码。
pub fn now_us() -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, micros);
    now.saturating_add(clock_offset_us())
}

#[cfg(feature = "fault-injection")]
fn clock_offset_us() -> i64 {
    std::env::var("TESTBED_CLOCK_OFFSET")
        .ok()
        .and_then(|s| s.trim().parse::<i64>().ok())
        .map_or(0, |secs| secs.saturating_mul(1_000_000))
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
fn clock_offset_us() -> i64 {
    0
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000_000;
    const SEC: i64 = 1_000_000;

    #[test]
    fn states_follow_the_two_instants() {
        let lc = Lifecycle {
            seal_grace: Duration::from_secs(1),
            lake_watermark: Duration::from_secs(2 * 3600),
        };
        let b = HourBucket::of(1_787_393_700_000_000);
        let end = b.start_us() + HOUR;
        assert_eq!(Lifecycle::bucket_end_us(b), end);
        assert_eq!(lc.seal_at_us(b), end + SEC);
        assert_eq!(lc.lake_at_us(b), end + 2 * HOUR);
        assert_eq!(lc.state(b, b.start_us()), BucketState::Open);
        assert_eq!(lc.state(b, end), BucketState::Open);
        assert_eq!(lc.state(b, end + SEC - 1), BucketState::Open);
        assert_eq!(lc.state(b, end + SEC), BucketState::Sealed);
        assert_eq!(lc.state(b, end + 2 * HOUR - 1), BucketState::Sealed);
        assert_eq!(lc.state(b, end + 2 * HOUR), BucketState::PendingLake);
        assert_eq!(lc.state(b, i64::MAX), BucketState::PendingLake);
    }

    #[test]
    fn defaults_and_labels() {
        let lc = Lifecycle::default();
        let b = HourBucket::of(0);
        assert_eq!(lc.seal_at_us(b), HOUR + 600 * SEC);
        assert_eq!(lc.lake_at_us(b), 73 * HOUR);
        let labels: Vec<_> = BucketState::ALL.iter().map(|s| s.as_str()).collect();
        assert_eq!(labels, ["open", "sealed", "pending_lake"]);
    }
}
