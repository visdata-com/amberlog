//! 权限矩阵（REQ-AMB-AUTH-AUTHZ B 组「权限矩阵」）：认证区内每个端点的路由模板、端点种类与所需角色。
//!
//! 登记的办法：认证区内的端点照常经 [`crate::gateway::Endpoint`] 注册路由，再在 [`MATRIX`] 里按路由模板登记一行。接入面对
//! 合并后的路由表施加 [`registered`]（只作用于匹配到的路由）：认证区内匹配到的路由模板不在 [`MATRIX`] 里时以 500 作答、
//! 不进端点，所以未登记的端点不能作答（REQ-AMB-AUTH-AUTHZ-MATRIX-CLOSED）；登记了的把端点种类回填进请求的 [`super::Access`]。

use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::{Access, Role};
use crate::gateway::ApiError;

/// 认证区内的端点种类：访问事件的 `endpoint` 取值（REQ-AMB-AUTH-AUTHZ-EVT-ACCESS-CLASSES）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum EndpointKind {
    /// `POST /api/{org}/{stream}/_json`（REQ-AMB-AUTH-AUTHZ-MATRIX-JSON）。
    Json,
    /// `POST /api/{org}/{stream}/_sent`（REQ-AMB-AUTH-AUTHZ-MATRIX-SENT）。
    Sent,
    /// `POST /api/{org}/_search`（REQ-AMB-AUTH-AUTHZ-MATRIX-SEARCH-STREAM、-MATRIX-SEARCH-SYSTEM）。
    Search,
    /// `GET /api/{org}/{stream}/_fields`（REQ-AMB-AUTH-AUTHZ-MATRIX-FIELDS）。
    Fields,
    /// 目录前缀下已注册的端点（REQ-AMB-AUTH-AUTHZ-MATRIX-CATALOG）。
    Catalog,
    /// `GET /metrics`（REQ-AMB-AUTH-AUTHZ-MATRIX-METRICS）。
    Metrics,
}

impl EndpointKind {
    /// 访问事件的 `endpoint` 取值。
    pub fn as_str(self) -> &'static str {
        match self {
            EndpointKind::Json => "json",
            EndpointKind::Sent => "sent",
            EndpointKind::Search => "search",
            EndpointKind::Fields => "fields",
            EndpointKind::Catalog => "catalog",
            EndpointKind::Metrics => "metrics",
        }
    }

    /// 属读类（查询、字段列表、目录；`log.access` 为 `reads` 时只为读类输出 `access`）。
    pub fn is_read(self) -> bool {
        matches!(
            self,
            EndpointKind::Search | EndpointKind::Fields | EndpointKind::Catalog
        )
    }

    /// 路径一步所需的角色：主体没有其中任一角色（管理员除外）即 403（REQ-AMB-AUTH-AUTHZ-DENY-ENDPOINT）。
    /// `_search` 按路径一步的口径：查询者或治理查看者。
    pub fn roles(self) -> &'static [Role] {
        match self {
            EndpointKind::Json | EndpointKind::Sent => &[Role::Writer],
            EndpointKind::Search => &[Role::Querier, Role::GovernanceViewer],
            EndpointKind::Fields => &[Role::Querier],
            EndpointKind::Catalog => &[Role::Querier, Role::CatalogReader],
            EndpointKind::Metrics => &[Role::MetricsReader],
        }
    }
}

/// 认证区内全部端点的路由模板（axum 的写法）与端点种类。写入实例与只读目录模式共用；只读目录模式只挂其中 `/metrics`
/// 与目录前缀两类。新端点要先在 REQ-AMB-AUTH-AUTHZ 的权限矩阵登记，再在这里加一行。
pub const MATRIX: &[(&str, EndpointKind)] = &[
    ("/api/{org}/{stream}/_json", EndpointKind::Json),
    ("/api/{org}/{stream}/_sent", EndpointKind::Sent),
    ("/api/{org}/_search", EndpointKind::Search),
    ("/api/{org}/{stream}/_fields", EndpointKind::Fields),
    ("/metrics", EndpointKind::Metrics),
    // 目录前缀下的七个模板由目录投影自己匹配（`crate::catalog::route`），路由表上只有这两条
    ("/iceberg/", EndpointKind::Catalog),
    ("/iceberg/{*rest}", EndpointKind::Catalog),
];

/// 路由模板 `template` 登记的端点种类。
pub fn kind_of(template: &str) -> Option<EndpointKind> {
    MATRIX
        .iter()
        .find(|(t, _)| *t == template)
        .map(|(_, kind)| *kind)
}

/// 请求路径 `path`（未经百分号解码）在认证区内：`/api/` 前缀、路径 `/metrics`、目录前缀（REQ-AMB-AUTH-CREDENTIAL-V3）。
pub fn in_auth_zone(path: &str) -> bool {
    path.starts_with("/api/") || path == "/metrics" || crate::catalog::in_prefix(path)
}

/// 施加在合并后的路由表上（`route_layer`，只作用于匹配到的路由）：认证区内的路由须在 [`MATRIX`] 登记，否则 500；
/// 登记了的把端点种类回填进请求的 [`Access`]（认证区内的请求都由认证一步带上它）。
pub(crate) async fn registered(matched: Option<MatchedPath>, req: Request, next: Next) -> Response {
    if !in_auth_zone(req.uri().path()) {
        return next.run(req).await;
    }
    let kind = matched.as_ref().and_then(|m| kind_of(m.as_str()));
    match (kind, req.extensions().get::<Access>()) {
        (Some(kind), Some(access)) => {
            access.set_endpoint(kind);
            next.run(req).await
        }
        (None, _) => ApiError::internal()
            .with_cause(format!(
                "route {:?} is in the authenticated zone but not registered in the permission matrix",
                matched.as_ref().map(MatchedPath::as_str)
            ))
            .into_response(),
        (Some(_), None) => ApiError::internal()
            .with_cause("request in the authenticated zone reached routing without a principal")
            .into_response(),
    }
}
