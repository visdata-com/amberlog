//! 告警的两个稳定指标（REQ-AMB-ALR-RULES-EVAL-METRIC、REQ-AMB-ALR-WEBHOOK-DELIVERY-METRIC，登记在
//! REQ-AMB-OBS-HEALTH-METRICS-V6）：`amberlog_alert_evaluations_total{rule,outcome}` 与
//! `amberlog_alert_deliveries_total{rule,result}`。
//!
//! 启动时为每条启用的规则（含启用的内置规则）登记全部序列、取值 0（-EVAL-METRIC-INIT、-DELIVERY-METRIC-INIT）；
//! 停用的规则不登记，此后也不会有它们的序列。

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::alerting::outcome::{DeliveryResult, Outcome};

/// `amberlog_alert_deliveries_total` 的 `result` 标签。
const DELIVERY_LABELS: [&str; 2] = ["delivered", "abandoned"];

/// 各启用规则的计数。规则集在构造时定下，此后不变。
#[derive(Debug, Default)]
pub struct AlertCounters {
    rules: BTreeMap<String, RuleCounters>,
}

#[derive(Debug, Default)]
struct RuleCounters {
    /// 按 [`Outcome::LABELS`] 的次序。
    evaluations: [AtomicU64; 4],
    /// 按 [`DELIVERY_LABELS`] 的次序。
    deliveries: [AtomicU64; 2],
}

impl AlertCounters {
    /// 为 `rules`（启用的规则的名字）登记全部序列，取值 0。
    pub fn new<'a>(rules: impl IntoIterator<Item = &'a str>) -> AlertCounters {
        AlertCounters {
            rules: rules
                .into_iter()
                .map(|r| (r.to_owned(), RuleCounters::default()))
                .collect(),
        }
    }

    /// 规则 `rule` 的一次评估以 `outcome` 结束。
    pub fn evaluation(&self, rule: &str, outcome: Outcome) {
        if let Some(c) = self.rules.get(rule) {
            let i = Outcome::LABELS
                .iter()
                .position(|l| *l == outcome.label())
                .expect("四种结局都在 LABELS 中");
            c.evaluations[i].fetch_add(1, Ordering::Relaxed);
        }
    }

    /// 规则 `rule` 的一次投递有了投递结果 `result`（`not_attempted` 不计）。
    pub fn delivery(&self, rule: &str, result: DeliveryResult) {
        let (Some(c), Some(label)) = (self.rules.get(rule), result.metric_label()) else {
            return;
        };
        let i = DELIVERY_LABELS
            .iter()
            .position(|l| *l == label)
            .expect("两种结果都在 DELIVERY_LABELS 中");
        c.deliveries[i].fetch_add(1, Ordering::Relaxed);
    }

    /// 按 Prometheus 文本暴露格式写出两个指标族；没有启用的规则时什么都不写。
    pub fn render(&self, out: &mut String) {
        if self.rules.is_empty() {
            return;
        }
        let _ = writeln!(
            out,
            "# HELP amberlog_alert_evaluations_total Alert rule evaluations by outcome."
        );
        let _ = writeln!(out, "# TYPE amberlog_alert_evaluations_total counter");
        for (rule, c) in &self.rules {
            for (label, n) in Outcome::LABELS.iter().zip(&c.evaluations) {
                let _ = writeln!(
                    out,
                    "amberlog_alert_evaluations_total{{rule=\"{rule}\",outcome=\"{label}\"}} {}",
                    n.load(Ordering::Relaxed)
                );
            }
        }
        let _ = writeln!(
            out,
            "# HELP amberlog_alert_deliveries_total Alert webhook deliveries by result."
        );
        let _ = writeln!(out, "# TYPE amberlog_alert_deliveries_total counter");
        for (rule, c) in &self.rules {
            for (label, n) in DELIVERY_LABELS.iter().zip(&c.deliveries) {
                let _ = writeln!(
                    out,
                    "amberlog_alert_deliveries_total{{rule=\"{rule}\",result=\"{label}\"}} {}",
                    n.load(Ordering::Relaxed)
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::alerting::outcome::SkipReason;

    #[test]
    fn series_start_at_zero_and_count() {
        let c = AlertCounters::new(["r1"]);
        let mut out = String::new();
        c.render(&mut out);
        assert!(
            out.contains("amberlog_alert_evaluations_total{rule=\"r1\",outcome=\"matched\"} 0\n")
        );
        assert!(
            out.contains("amberlog_alert_evaluations_total{rule=\"r1\",outcome=\"error\"} 0\n")
        );
        assert!(
            out.contains("amberlog_alert_deliveries_total{rule=\"r1\",result=\"abandoned\"} 0\n")
        );

        c.evaluation("r1", Outcome::Skipped(SkipReason::PreviousRunning));
        c.delivery("r1", DeliveryResult::DeliveredAfterRetry);
        c.delivery("r1", DeliveryResult::NotAttempted);
        c.evaluation("off", Outcome::Matched);
        let mut out = String::new();
        c.render(&mut out);
        assert!(
            out.contains("amberlog_alert_evaluations_total{rule=\"r1\",outcome=\"skipped\"} 1\n")
        );
        assert!(
            out.contains("amberlog_alert_deliveries_total{rule=\"r1\",result=\"delivered\"} 1\n")
        );
        assert!(
            out.contains("amberlog_alert_deliveries_total{rule=\"r1\",result=\"abandoned\"} 0\n")
        );
        assert!(!out.contains("off"));
    }

    #[test]
    fn no_rules_no_families() {
        let mut out = String::new();
        AlertCounters::default().render(&mut out);
        assert!(out.is_empty());
    }
}
