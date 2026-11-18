//! 把请求的主体交给查询一侧（[`amberlog_query::viewer::Viewer`]）：`_search` 的请求体一步与系统表的行过滤在 query 里按
//! 报告顺序调用，判定仍在本模块（REQ-AMB-AUTH-AUTHZ-STEP-BODY、-ROW-FILTER）。

use amberlog_common::dataset::Dataset;
use amberlog_query::viewer::{Decision, Target, Viewer};

use super::{Access, Denied, Role, SearchTarget};

/// 台账、对账的行与评估记录中数据集一行的可见性所依的两种角色（REQ-AMB-GOV-LEDGER-V4、REQ-AMB-GOV-RECONCILIATION-V4、
/// REQ-AMB-ALR-EVALUATIONS-V2-ROW-VISIBLE）。
const GOVERNANCE_ROLES: [Role; 2] = [Role::Querier, Role::GovernanceViewer];

impl Viewer for Access {
    fn decide(&self, target: Target<'_>) -> Decision {
        let target = match target {
            Target::Stream(d) => SearchTarget::Stream(d),
            Target::System(name) => SearchTarget::System(name),
        };
        match self.search_target(target) {
            Ok(()) => Decision::Allow,
            Err(Denied::Forbidden) => Decision::Forbidden,
            Err(Denied::Absent) => Decision::Absent,
        }
    }

    fn sees(&self, dataset: &Dataset) -> bool {
        self.principal()
            .in_any_scope(&GOVERNANCE_ROLES, &dataset.stream)
    }

    fn sees_all(&self) -> bool {
        self.principal().scope_all(&GOVERNANCE_ROLES)
    }
}
