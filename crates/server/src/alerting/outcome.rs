//! 评估的结局与评估记录（REQ-AMB-ALR-RULES C 组「结局」、REQ-AMB-ALR-EVALUATIONS-ENTRY-VALUES）。

use amberlog_common::error::Reason;

/// 一次评估的结局（REQ-AMB-ALR-RULES-OUTCOMES）；`reason` 的取值是闭集（-REASON-SET），由变体保证。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Matched,
    NotMatched,
    Skipped(SkipReason),
    Error(ErrorReason),
}

/// 结局为 `skipped` 时的 `reason`。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    PreviousRunning,
    AlertSlotsFull,
    QuerySlotsFull,
    ShuttingDown,
}

/// 结局为 `error` 时的 `reason`：查询的错误信封的 `reason`（不会是 `too_many_queries`，名额满记作
/// [`SkipReason::QuerySlotsFull`]），或告警专用的三个值。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorReason {
    Query(Reason),
    ConditionColumnMissing,
    ConditionColumnNotNumeric,
    ShuttingDown,
}

impl Outcome {
    /// 评估记录的 `outcome` 与指标的 `outcome` 标签。
    pub fn label(self) -> &'static str {
        match self {
            Outcome::Matched => "matched",
            Outcome::NotMatched => "not_matched",
            Outcome::Skipped(_) => "skipped",
            Outcome::Error(_) => "error",
        }
    }

    /// 评估记录的 `reason`：`matched` 与 `not_matched` 没有。
    pub fn reason(self) -> Option<&'static str> {
        match self {
            Outcome::Matched | Outcome::NotMatched => None,
            Outcome::Skipped(r) => Some(match r {
                SkipReason::PreviousRunning => "previous_running",
                SkipReason::AlertSlotsFull => "alert_slots_full",
                SkipReason::QuerySlotsFull => "query_slots_full",
                SkipReason::ShuttingDown => "shutting_down",
            }),
            Outcome::Error(r) => Some(match r {
                ErrorReason::Query(reason) => reason.as_str(),
                ErrorReason::ConditionColumnMissing => "condition_column_missing",
                ErrorReason::ConditionColumnNotNumeric => "condition_column_not_numeric",
                ErrorReason::ShuttingDown => "shutting_down",
            }),
        }
    }

    /// 四种结局的标签，按 `amberlog_alert_evaluations_total` 启动时登记的次序（REQ-AMB-ALR-RULES-EVAL-METRIC-INIT）。
    pub const LABELS: [&'static str; 4] = ["matched", "not_matched", "skipped", "error"];
}

/// 投递结果（REQ-AMB-ALR-WEBHOOK-RESULT）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryResult {
    Delivered,
    DeliveredAfterRetry,
    Abandoned,
    NotAttempted,
}

impl DeliveryResult {
    /// 评估记录的 `delivery`。
    pub fn label(self) -> &'static str {
        match self {
            DeliveryResult::Delivered => "delivered",
            DeliveryResult::DeliveredAfterRetry => "delivered_after_retry",
            DeliveryResult::Abandoned => "abandoned",
            DeliveryResult::NotAttempted => "not_attempted",
        }
    }

    /// `amberlog_alert_deliveries_total` 的 `result` 标签：两种送达合为 `delivered`，没有投递时不计
    /// （REQ-AMB-ALR-WEBHOOK-DELIVERY-METRIC）。
    pub fn metric_label(self) -> Option<&'static str> {
        match self {
            DeliveryResult::Delivered | DeliveryResult::DeliveredAfterRetry => Some("delivered"),
            DeliveryResult::Abandoned => Some("abandoned"),
            DeliveryResult::NotAttempted => None,
        }
    }
}

/// 一次已结束的评估交给评估记录写出的内容：REQ-AMB-ALR-EVALUATIONS-ENTRY-SHAPE 的十二个字段，另带它所属的评估记录对象的
/// `org` 与 `rule`。时刻都是 UTC 微秒。不含 `webhook_url` 与 `webhook_headers`（-NO-SECRETS）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EvaluationRecord {
    pub org: String,
    pub rule: String,
    pub scheduled_at: i64,
    /// 开始执行查询（内置规则为开始求结果）之时；未开始时为 `None`。
    pub started_at: Option<i64>,
    pub finished_at: i64,
    pub window_start: i64,
    pub window_end: i64,
    pub outcome: Outcome,
    /// 结局为 `skipped` 或 `error` 时为 `None`。
    pub matched_rows: Option<u64>,
    /// 结局为 `skipped` 或 `error` 时为 `None`。
    pub truncated: Option<bool>,
    pub delivery: DeliveryResult,
    pub attempts: u32,
    pub last_http_status: Option<u16>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reasons_follow_the_closed_set() {
        assert_eq!(Outcome::Matched.reason(), None);
        assert_eq!(Outcome::NotMatched.reason(), None);
        assert_eq!(
            Outcome::Skipped(SkipReason::QuerySlotsFull).reason(),
            Some("query_slots_full")
        );
        assert_eq!(
            Outcome::Error(ErrorReason::Query(Reason::StreamNotFound)).reason(),
            Some("stream_not_found")
        );
        assert_eq!(Outcome::Error(ErrorReason::ShuttingDown).label(), "error");
    }

    #[test]
    fn delivery_metric_label_merges_delivered() {
        assert_eq!(
            DeliveryResult::DeliveredAfterRetry.metric_label(),
            Some("delivered")
        );
        assert_eq!(DeliveryResult::NotAttempted.metric_label(), None);
    }
}
