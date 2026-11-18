//! 授权判定的请求扩展（[`Access`]）与访问事件（REQ-AMB-AUTH-AUTHZ B 组「两步判定」「应答」「来源限定」、C 组）。
//!
//! 认证一步把主体连同一份空的访问记录放进请求扩展；路由一步回填端点种类（[`super::matrix::registered`]）；端点处理器以
//! 提取器取得 [`Access`]，在块所定的位置调用路径一步与请求体一步，判定时顺带记下 `dataset`；接入面在得出最终响应后据此
//! 输出 `access` 或 `access_denied`（[`Access::emit`]）。

use std::sync::{Arc, Mutex};

use amberlog_common::dataset::Dataset;
use axum::extract::FromRequestParts;
use axum::http::request::Parts;
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::Value;

use super::{EndpointKind, LogAccess, Principal, PrincipalKind, Role};
use crate::gateway::ApiError;
use crate::logging::{self, Level};

/// 来源标识请求头（REQ-AMB-ING-JSON-ENDPOINT-V4）。
const SOURCE_HEADER: &str = "x-amberlog-source";

/// 一次判定的否决。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Denied {
    /// 以 403 `forbidden` 作答（REQ-AMB-AUTH-AUTHZ-FORBIDDEN-RESPONSE；[`ApiError::forbidden`]，目录前缀下由出口改写为目录错误体）。
    Forbidden,
    /// 以与数据集不存在时完全相同的应答作答（REQ-AMB-AUTH-AUTHZ-HIDE-READ-SCOPE）：应答由端点按它自己的「不存在」给出。
    Absent,
}

/// `_search` 的 `FROM` 所指的对象（请求体一步）。
#[derive(Debug, Clone, Copy)]
pub enum SearchTarget<'a> {
    /// 普通数据集。
    Stream(&'a Dataset),
    /// 系统表（`_ledger`、`_reconciliation`、`_alert_evaluations`）。
    System(&'a str),
}

/// 访问记录：端点种类（路由一步回填）与 `dataset`（判定时记下）。
#[derive(Debug, Default)]
struct Record {
    endpoint: Option<EndpointKind>,
    dataset: Option<String>,
}

/// 一次请求的主体与访问记录，由认证一步放进请求扩展。端点处理器以提取器取得它。
#[derive(Debug, Clone)]
pub struct Access {
    principal: Arc<Principal>,
    record: Arc<Mutex<Record>>,
}

impl Access {
    pub fn new(principal: Principal) -> Access {
        Access {
            principal: Arc::new(principal),
            record: Arc::default(),
        }
    }

    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    fn record(&self) -> std::sync::MutexGuard<'_, Record> {
        self.record.lock().unwrap_or_else(|p| p.into_inner())
    }

    pub(super) fn set_endpoint(&self, kind: EndpointKind) {
        self.record().endpoint = Some(kind);
    }

    /// 本请求匹配到的端点种类（路由一步回填；未匹配到认证区内的端点为 `None`）。
    pub fn endpoint(&self) -> Option<EndpointKind> {
        self.record().endpoint
    }

    /// 已记下的 `dataset`。
    pub fn dataset(&self) -> Option<String> {
        self.record().dataset.clone()
    }

    /// 记下访问事件的 `dataset`（REQ-AMB-AUTH-AUTHZ-EVT-ACCESS-FIELDS）。路径一步与请求体一步已按所判定的对象记下，
    /// 端点另有取值时（如 `_search` 未解析出 `FROM`）由它自己调。
    pub fn note_dataset(&self, dataset: Option<String>) {
        self.record().dataset = dataset;
    }

    /// 授权的路径一步（REQ-AMB-AUTH-AUTHZ-STEP-PATH）：在名字校验之后、状态闸门与读取请求体之前调用。
    /// `dataset` 为路径所指的数据集（`_json`、`_sent`、`_fields`、目录的读表与表存在）；端点种类取自路由一步。
    ///
    /// - 主体没有该端点所需的任一角色：403（DENY-ENDPOINT）。
    /// - 写入（`_json`、`_sent`）的数据集不在写入者范围内：403，不论数据集是否存在（DENY-WRITE-SCOPE）。
    /// - `_json` 的主体带来源限定而 `X-Amberlog-Source` 缺失、多于一个或不在名单内：403（SOURCE-RESTRICT-JSON）。
    /// - 字段列表、目录读表的数据集不在相应范围内：按不存在作答（HIDE-READ-SCOPE）。
    pub fn path_step(&self, dataset: Option<&Dataset>, headers: &HeaderMap) -> Result<(), Denied> {
        if let Some(d) = dataset {
            self.note_dataset(Some(d.to_string()));
        }
        let Some(kind) = self.endpoint() else {
            // 路由一步对认证区内的每个端点都回填；走到这里是端点没经权限矩阵挂上
            return Err(Denied::Forbidden);
        };
        let p = &self.principal;
        if !kind.roles().iter().any(|r| p.has_role(*r)) {
            return Err(Denied::Forbidden);
        }
        let Some(dataset) = dataset else {
            return Ok(());
        };
        let stream = dataset.stream.as_str();
        match kind {
            EndpointKind::Json | EndpointKind::Sent => {
                if !p.in_scope(Role::Writer, stream) {
                    return Err(Denied::Forbidden);
                }
                if kind == EndpointKind::Json
                    && let Some(allowed) = p.sources()
                    && !source_allowed(headers, allowed)
                {
                    return Err(Denied::Forbidden);
                }
                Ok(())
            }
            EndpointKind::Fields => p
                .in_scope(Role::Querier, stream)
                .then_some(())
                .ok_or(Denied::Absent),
            EndpointKind::Catalog => p
                .in_catalog_scope(stream)
                .then_some(())
                .ok_or(Denied::Absent),
            EndpointKind::Search | EndpointKind::Metrics => Ok(()),
        }
    }

    /// `_search` 的请求体一步（REQ-AMB-AUTH-AUTHZ-STEP-BODY；位置按 REQ-AMB-QRY-SEARCH-ENDPOINT-V10 的报告顺序）：
    ///
    /// - 系统表：有查询者或治理查看者角色即可（MATRIX-SEARCH-SYSTEM；可见的行另按 ROW-FILTER）。
    /// - 普通数据集：有查询者角色时，在查询者范围内才作答、范围外按不存在作答；只有治理查看者角色时 403，不论数据集是否
    ///   存在（DENY-ENDPOINT 括注）。
    pub fn search_target(&self, target: SearchTarget<'_>) -> Result<(), Denied> {
        let p = &self.principal;
        match target {
            SearchTarget::System(name) => {
                self.note_dataset(Some(name.to_owned()));
                if p.has_role(Role::Querier) || p.has_role(Role::GovernanceViewer) {
                    Ok(())
                } else {
                    Err(Denied::Forbidden)
                }
            }
            SearchTarget::Stream(dataset) => {
                self.note_dataset(Some(dataset.to_string()));
                if !p.has_role(Role::Querier) {
                    Err(Denied::Forbidden)
                } else if p.in_scope(Role::Querier, &dataset.stream) {
                    Ok(())
                } else {
                    Err(Denied::Absent)
                }
            }
        }
    }

    /// `_sent` 的请求体一步（REQ-AMB-AUTH-AUTHZ-SOURCE-RESTRICT-SENT；位置按 REQ-AMB-OBS-INGEST-COUNTERS-V4）：主体带来源
    /// 限定时，任一元素的 `source` 不在名单内即 403。
    pub fn sent_sources<'a>(
        &self,
        sources: impl IntoIterator<Item = &'a str>,
    ) -> Result<(), Denied> {
        match self.principal.sources() {
            None => Ok(()),
            Some(allowed) => sources
                .into_iter()
                .all(|s| allowed.iter().any(|a| a == s))
                .then_some(())
                .ok_or(Denied::Forbidden),
        }
    }

    /// 在得出最终响应之后输出访问事件（REQ-AMB-AUTH-AUTHZ-EVT-ACCESS、-EVT-DENIED、-EVT-NO-401、-EVT-DISABLED-NONE）：
    /// 403 一律 `access_denied`；其余（404、405 除外）在端点类别属于 `log_access` 时 `access`。未匹配到认证区内的端点、
    /// 认证关闭时都不输出。
    pub fn emit(&self, log_access: LogAccess, method: &str, status: StatusCode, request_id: &str) {
        let p = &self.principal;
        if p.kind() == PrincipalKind::Unauthenticated {
            return;
        }
        let (endpoint, dataset) = {
            let r = self.record();
            (r.endpoint, r.dataset.clone())
        };
        let Some(endpoint) = endpoint else {
            return;
        };
        let (event, level, msg) = match status {
            StatusCode::FORBIDDEN => ("access_denied", Level::Warn, "request denied"),
            StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED => return,
            _ => {
                let logged = match log_access {
                    LogAccess::None => false,
                    LogAccess::Reads => endpoint.is_read(),
                    LogAccess::All => true,
                };
                if !logged {
                    return;
                }
                ("access", Level::Info, "request served")
            }
        };
        logging::event(
            level,
            event,
            msg,
            &[
                ("principal", Value::from(p.id())),
                // REQ-AMB-AUTH-AUTHZ-EVT-PRINCIPAL-NAME
                ("principal_name", p.name().map_or(Value::Null, Value::from)),
                ("roles", Value::from(p.roles())),
                ("endpoint", Value::from(endpoint.as_str())),
                ("method", Value::from(method)),
                ("dataset", dataset.map_or(Value::Null, Value::from)),
                ("status", Value::from(status.as_u16())),
                ("request_id", Value::from(request_id)),
            ],
        );
    }
}

/// `X-Amberlog-Source` 恰有一个，且与名单 `allowed` 中某项逐字节相等。
fn source_allowed(headers: &HeaderMap, allowed: &[String]) -> bool {
    let mut values = headers.get_all(SOURCE_HEADER).iter();
    match (values.next(), values.next()) {
        (Some(v), None) => allowed.iter().any(|a| a.as_bytes() == v.as_bytes()),
        _ => false,
    }
}

/// 认证区内的请求都由认证一步带上 [`Access`]；端点挂在接入面之外（或认证区之外）时取不到，以 500 作答。
impl<S: Send + Sync> FromRequestParts<S> for Access {
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, _: &S) -> Result<Self, Self::Rejection> {
        parts.extensions.get::<Access>().cloned().ok_or_else(|| {
            ApiError::internal()
                .with_cause("the endpoint is not behind the authentication step")
                .into_response()
        })
    }
}
