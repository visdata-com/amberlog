//! 发出查询的主体在查询一侧的可见性（REQ-AMB-AUTH-AUTHZ 的请求体一步与系统表的行过滤，ADR-0056）。
//!
//! 授权的判定集中在接入面的授权模块，query 不依赖它：接入面以请求的主体实现 [`Viewer`] 交给
//! [`crate::search::Engine::search`]；告警评估的查询以系统身份 [`System`] 执行，不经判定
//! （REQ-AMB-AUTH-AUTHZ-ALERT-SYSTEM-IDENTITY）。`auth.enabled` 为 false 时接入面交来的主体按管理员作答，与 [`System`] 相同。

use amberlog_common::dataset::Dataset;

/// `_search` 的 `FROM` 所指的对象（请求体一步的判定对象）。
#[derive(Debug, Clone, Copy)]
pub enum Target<'a> {
    /// 普通数据集（名称已合 `{stream}` 正则）。
    Stream(&'a Dataset),
    /// 系统表（`_ledger`、`_reconciliation`、`_alert_evaluations`）。
    System(&'a str),
}

/// 请求体一步的结论。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    Allow,
    /// HTTP 403 `forbidden`（REQ-AMB-AUTH-AUTHZ-FORBIDDEN-RESPONSE）。
    Forbidden,
    /// 与数据集不存在时完全相同的应答（REQ-AMB-AUTH-AUTHZ-HIDE-READ-SCOPE）。
    Absent,
}

/// 发出查询的主体。
pub trait Viewer: Send + Sync {
    /// 请求体一步（REQ-AMB-AUTH-AUTHZ-STEP-BODY）：在 SQL 解析、`FROM` 名称合规之后，按 REQ-AMB-QRY-SEARCH-ENDPOINT-V10
    /// 的报告顺序调用一次。
    fn decide(&self, target: Target<'_>) -> Decision;

    /// 数据集 `dataset` 在主体对查询者或对治理查看者的范围内（`_ledger`、`_reconciliation` 的行可见，
    /// REQ-AMB-GOV-LEDGER-V4、REQ-AMB-GOV-RECONCILIATION-V4）。
    fn sees(&self, dataset: &Dataset) -> bool;

    /// 主体对查询者与治理查看者这一角色集合的范围为全部（REQ-AMB-AUTH-AUTHZ-TERM-SCOPE-ALL；`_alert_evaluations` 中
    /// `target` 不是数据集的行只对这样的主体可见，REQ-AMB-ALR-EVALUATIONS-V2-ROW-VISIBLE）。
    fn sees_all(&self) -> bool;
}

/// 系统身份：范围为全部，判定一律放行（告警评估的查询、认证关闭时）。
#[derive(Debug, Clone, Copy, Default)]
pub struct System;

impl Viewer for System {
    fn decide(&self, _: Target<'_>) -> Decision {
        Decision::Allow
    }

    fn sees(&self, _: &Dataset) -> bool {
        true
    }

    fn sees_all(&self) -> bool {
        true
    }
}
