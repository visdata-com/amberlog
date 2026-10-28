//! 告警（Alerting）：定时告警的规则、调度、条件判定与结局（REQ-AMB-ALR-RULES），
//! 命中后的 webhook 投递（REQ-AMB-ALR-WEBHOOK），评估记录的写出、保留与系统表 `_alert_evaluations` 的行集
//! （REQ-AMB-ALR-EVALUATIONS）。裁决记录 ADR-0051。
//!
//! 只在写入实例（`amberlog serve`）下运行；只读目录模式与离线子命令只经配置一步校验规则（REQ-AMB-ALR-RULES-SERVE-ONLY、
//! -VALIDATE-ALL-MODES）。
//!
//! 一次评估在模块之间的交接：
//! 1. 规则（[`rule::Rule`]）在配置一步解析与校验，启用的规则交给调度；
//! 2. 调度到计划时刻取告警名额，再经查询规划与执行求结果（评估取查询执行名额的入口在 amberlog-query，与 `_search`
//!    共用同一份 [`amberlog_query::limits::Admission`]），按 [`rule::Condition`] 判定结局（[`outcome::Outcome`]）；
//! 3. 命中的评估交给投递（[`Delivery`]，交出 [`Notice`]），投递有了结果（[`DeliveryOutcome`]）评估才结束
//!    （REQ-AMB-ALR-RULES-TERM-ENDED）；
//! 4. 每次评估结束交出一条评估记录（[`outcome::EvaluationRecord`]）给评估记录的写出（[`Recorder`]，实现为
//!    [`history::History`]，它同时是 `_alert_evaluations` 的行集来源）；
//! 5. 两个告警指标在 [`metrics::AlertCounters`]。
//!
//! 进入停机过程时由 [`Stop`] 通知各在途评估与投递（REQ-AMB-ALR-RULES-SHUTDOWN-ABANDON）。

pub mod history;
pub mod metrics;
pub mod outcome;
pub mod rule;
pub mod schedule;
pub mod webhook;

use std::sync::Arc;

use amberlog_query::search::Hit;
use amberlog_storage::BoxFuture;
use tokio::sync::watch;

use crate::alerting::outcome::{DeliveryResult, EvaluationRecord};
use crate::alerting::rule::Rule;

/// 停机通知：进入停机过程时置位一次，此后不复位。可克隆，各在途评估与投递各持一份。
#[derive(Clone)]
pub struct Stop(watch::Receiver<bool>);

/// 置位 [`Stop`] 的一方（写入实例的停机流程持有）。
pub struct StopTrigger(watch::Sender<bool>);

impl Stop {
    /// 一对新的停机通知：未置位。
    pub fn pair() -> (StopTrigger, Stop) {
        let (tx, rx) = watch::channel(false);
        (StopTrigger(tx), Stop(rx))
    }

    /// 是否已进入停机过程。
    pub fn is_stopped(&self) -> bool {
        *self.0.borrow()
    }

    /// 等到进入停机过程（已置位时立即返回）。
    pub async fn stopped(&mut self) {
        // 发送方在置位之前被丢弃也按停机处理：不会再有人置位，等下去没有意义
        let _ = self.0.wait_for(|stopped| *stopped).await;
    }
}

impl StopTrigger {
    /// 进入停机过程。
    pub fn stop(&self) {
        let _ = self.0.send(true);
    }
}

/// 一次命中的评估交给投递的内容（REQ-AMB-ALR-WEBHOOK-BODY 的十个字段由投递据此组装）。
#[derive(Debug, Clone)]
pub struct Notice {
    /// 规则（`name`、`interval`、`window`、`sql` 原文与 webhook 目标都从这里取；内置规则的 `sql` 见 [`Rule::sql_text`]）。
    pub rule: Arc<Rule>,
    /// 计划时刻 T（UTC 微秒）。截止时刻为 T 加 `interval`（REQ-AMB-ALR-WEBHOOK-DEADLINE），查询范围为
    /// `[T − window, T)`（REQ-AMB-ALR-RULES-TERM-RANGE）。
    pub scheduled_at: i64,
    /// REQ-AMB-ALR-WEBHOOK-MATCHED-ROWS。
    pub matched_rows: u64,
    /// REQ-AMB-ALR-RULES-TRUNCATED。
    pub truncated: bool,
    /// 请求体的 `rows`：已按条件筛好、按查询结果的最终排序取的至多 10 行（REQ-AMB-ALR-WEBHOOK-ROWS），
    /// 每行的写法同 `_search` 的一个 hit。
    pub rows: Vec<Hit>,
}

/// 一次投递的结果：投递结果、已开始的尝试数与最后一次收到响应的状态码（REQ-AMB-ALR-WEBHOOK-RESULT、-ATTEMPTS、
/// -LAST-STATUS）。`result` 不会是 [`DeliveryResult::NotAttempted`]：不投递的评估由调度自己记。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeliveryOutcome {
    pub result: DeliveryResult,
    pub attempts: u32,
    pub last_status: Option<u16>,
}

/// 投递（REQ-AMB-ALR-WEBHOOK）：调度在评估任务里等它返回（投递期间不占告警名额，REQ-AMB-ALR-RULES-TERM-ALERT-SLOT）。
/// 实现负责第一次尝试的 1 秒时限、超时、重试、截止与 `stop` 置位时中止进行中的尝试；返回即该次投递已有投递结果。
pub trait Delivery: Send + Sync {
    fn deliver(&self, notice: Notice, stop: Stop) -> BoxFuture<'_, DeliveryOutcome>;
}

/// 评估记录的写出（REQ-AMB-ALR-EVALUATIONS B 组）：每次评估结束时调用一次，不等待写出（写出时限、重试与停机时的
/// 写出由实现负责）。
pub trait Recorder: Send + Sync {
    fn record(&self, record: EvaluationRecord);
}
