//! 授权（REQ-AMB-AUTH-AUTHZ；模型见 ADR-0056）：主体、六种内置角色、数据集范围、授权文件、权限矩阵与判定、访问事件。
//!
//! 授权的判定只在本模块：各端点经 [`Access`] 调用路径一步与请求体一步，不在端点处理器里自写判定。
//!
//! - 主体（[`Principal`]）由接入面的认证一步（`gateway/auth.rs`）按凭据得出：先与 `auth.token` 比较，再按服务令牌的
//!   格式查授权文件（[`Authz::authenticate`]），最后按身份平台令牌验证（[`Authz::authenticate_idp`]）。主体与授予只取自这一步，同一请求的认证与授权用同一份授权文件内容。
//! - 权限矩阵（[`matrix`]）按路由模板登记认证区内的每个端点；未登记的端点不得作答。
//! - 判定的入口是请求扩展 [`Access`]：路径一步 [`Access::path_step`]，请求体一步 [`Access::search_target`]、
//!   [`Access::sent_sources`]；系统表的行过滤与目录的列举读 [`Principal`] 的范围判定。
//! - 访问事件（`access`、`access_denied`）由接入面在得出最终响应后输出，端点种类与 `dataset` 经 [`Access`] 回填。

pub mod access;
pub mod file;
pub mod matrix;
mod reload;
pub mod token;
mod viewer;

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use axum::http::{HeaderMap, header};
use sha2::{Digest, Sha256};

pub use access::{Access, Denied, SearchTarget};
pub use matrix::EndpointKind;
pub use reload::AuthCounters;

/// 六种内置角色（REQ-AMB-AUTH-AUTHZ-TERM-ROLE）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    Writer,
    Querier,
    CatalogReader,
    GovernanceViewer,
    MetricsReader,
    Admin,
}

impl Role {
    pub const ALL: [Role; 6] = [
        Role::Writer,
        Role::Querier,
        Role::CatalogReader,
        Role::GovernanceViewer,
        Role::MetricsReader,
        Role::Admin,
    ];

    /// 授权文件与访问事件 `roles` 里的标识。
    pub fn as_str(self) -> &'static str {
        match self {
            Role::Writer => "writer",
            Role::Querier => "querier",
            Role::CatalogReader => "catalog_reader",
            Role::GovernanceViewer => "governance_viewer",
            Role::MetricsReader => "metrics_reader",
            Role::Admin => "admin",
        }
    }

    /// 按标识逐字取角色；不认识的标识不是任何一种角色。
    pub fn parse(s: &str) -> Option<Role> {
        Role::ALL.into_iter().find(|r| r.as_str() == s)
    }

    /// 授予是否带范围（管理员与指标读者不带，REQ-AMB-AUTH-AUTHZ-TERM-GRANT）。
    pub fn scoped(self) -> bool {
        !matches!(self, Role::Admin | Role::MetricsReader)
    }
}

/// 数据集名模式（REQ-AMB-AUTH-AUTHZ-TERM-SCOPE）：精确名、以 `*` 结尾的前缀、单独一个 `*`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pattern {
    Exact(String),
    Prefix(String),
    All,
}

impl Pattern {
    /// 按 TERM-SCOPE 的三种写法解析；都不是为 `None`。
    pub fn parse(s: &str) -> Option<Pattern> {
        if s == "*" {
            return Some(Pattern::All);
        }
        let (body, max, prefix) = match s.strip_suffix('*') {
            Some(body) => (body, 63, true),
            None => (s, 64, false),
        };
        let b = body.as_bytes();
        let valid = (1..=max).contains(&b.len())
            && (b[0].is_ascii_lowercase() || b[0].is_ascii_digit())
            && b[1..]
                .iter()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || *c == b'_');
        valid.then(|| {
            if prefix {
                Pattern::Prefix(body.to_owned())
            } else {
                Pattern::Exact(body.to_owned())
            }
        })
    }

    /// 模式与数据集名 `stream` 匹配：逐字节相等、是其字节前缀，或是 `*`（REQ-AMB-AUTH-AUTHZ-TERM-IN-SCOPE）。
    pub fn matches(&self, stream: &str) -> bool {
        match self {
            Pattern::Exact(name) => name == stream,
            Pattern::Prefix(prefix) => stream.as_bytes().starts_with(prefix.as_bytes()),
            Pattern::All => true,
        }
    }
}

/// 一个授予：角色与范围（管理员与指标读者的 `streams` 为空，REQ-AMB-AUTH-AUTHZ-TERM-GRANT）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    pub role: Role,
    pub streams: Vec<Pattern>,
}

/// 主体的种类。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrincipalKind {
    /// 以 `auth.token` 认证：管理员（REQ-AMB-AUTH-AUTHZ-PRINCIPAL-AUTH-TOKEN）。
    AuthToken,
    /// 授权文件里登记的服务令牌（REQ-AMB-AUTH-AUTHZ-PRINCIPAL-SERVICE-TOKEN）。
    ServiceToken,
    /// 按 REQ-AMB-AUTH-OIDC 验证通过的身份平台令牌（REQ-AMB-AUTH-AUTHZ-PRINCIPAL-IDP）。
    Idp,
    /// `auth.enabled` 为 false：不认证也不授权，全部按管理员的权限作答，不输出访问事件（REQ-AMB-AUTH-AUTHZ-DISABLED-ALLOW）。
    Unauthenticated,
}

/// 一次请求代表的主体（REQ-AMB-AUTH-AUTHZ-TERM-PRINCIPAL）：主体标识、授予与来源限定，只取自凭据的验证结果。
#[derive(Debug, Clone)]
pub struct Principal {
    kind: PrincipalKind,
    id: String,
    grants: Arc<[Grant]>,
    sources: Option<Arc<[String]>>,
    /// 身份平台令牌的可读名（REQ-AMB-AUTH-AUTHZ-EVT-PRINCIPAL-NAME）；其他主体为 `None`。
    name: Option<String>,
}

impl Principal {
    /// `auth.token` 的主体：管理员，主体标识 `token:auth.token`。
    pub fn auth_token() -> Principal {
        Principal {
            kind: PrincipalKind::AuthToken,
            id: "token:auth.token".to_owned(),
            grants: Arc::from([Grant {
                role: Role::Admin,
                streams: Vec::new(),
            }]),
            sources: None,
            name: None,
        }
    }

    /// 认证关闭时的请求：按管理员的权限作答。
    pub fn unauthenticated() -> Principal {
        Principal {
            kind: PrincipalKind::Unauthenticated,
            id: String::new(),
            grants: Arc::from([]),
            sources: None,
            name: None,
        }
    }

    fn service_token(entry: &TokenEntry) -> Principal {
        Principal {
            kind: PrincipalKind::ServiceToken,
            id: format!("token:{}", entry.name),
            grants: Arc::clone(&entry.grants),
            sources: entry.sources.clone(),
            name: None,
        }
    }

    /// 身份平台令牌的主体：主体标识 `oidc:<主体声明的值>`，授予是 `idp_grants` 中与令牌匹配的元素的授予之并集
    /// （REQ-AMB-AUTH-AUTHZ-PRINCIPAL-IDP、-IDP-MAPPING）；没有任何授予时仍是认证成功的主体（-IDP-NO-GRANT）。
    fn idp(claims: crate::oidc::Claims, table: Option<&Table>) -> Principal {
        let grants: Vec<Grant> = table
            .into_iter()
            .flat_map(|t| &t.idp_grants)
            .filter(|g| g.matches(&claims))
            .flat_map(|g| g.grants.iter().cloned())
            .collect();
        Principal {
            kind: PrincipalKind::Idp,
            id: format!("oidc:{}", claims.subject),
            grants: Arc::from(grants),
            sources: None,
            name: claims.preferred_username,
        }
    }

    pub fn kind(&self) -> PrincipalKind {
        self.kind
    }

    /// 主体标识（访问事件的 `principal`）。认证关闭时为空串（不输出访问事件）。
    pub fn id(&self) -> &str {
        &self.id
    }

    /// 是否按管理员作答：管理员角色，或认证关闭（REQ-AMB-AUTH-AUTHZ-ADMIN-ALL、-DISABLED-ALLOW）。
    pub fn is_admin(&self) -> bool {
        self.kind == PrincipalKind::Unauthenticated
            || self.grants.iter().any(|g| g.role == Role::Admin)
    }

    /// 主体有角色 `role`（任何范围；管理员拥有其余五种角色的全部权限）。
    pub fn has_role(&self, role: Role) -> bool {
        self.is_admin() || self.grants.iter().any(|g| g.role == role)
    }

    /// 数据集 `stream` 在主体对角色 `role` 的范围内（REQ-AMB-AUTH-AUTHZ-TERM-IN-SCOPE）。
    pub fn in_scope(&self, role: Role, stream: &str) -> bool {
        self.is_admin()
            || self
                .grants
                .iter()
                .any(|g| g.role == role && g.streams.iter().any(|p| p.matches(stream)))
    }

    /// 数据集 `stream` 在主体对 `roles` 中任一角色的范围内。
    pub fn in_any_scope(&self, roles: &[Role], stream: &str) -> bool {
        roles.iter().any(|r| self.in_scope(*r, stream))
    }

    /// 主体对角色集合 `roles` 的范围为全部（REQ-AMB-AUTH-AUTHZ-TERM-SCOPE-ALL）。
    pub fn scope_all(&self, roles: &[Role]) -> bool {
        self.is_admin()
            || self
                .grants
                .iter()
                .any(|g| roles.contains(&g.role) && g.streams.contains(&Pattern::All))
    }

    /// 数据集 `stream` 在主体的目录范围内：查询者与目录读者两种角色的范围的并集（REQ-AMB-AUTH-AUTHZ-TERM-CATALOG-SCOPE）。
    pub fn in_catalog_scope(&self, stream: &str) -> bool {
        self.in_any_scope(&[Role::Querier, Role::CatalogReader], stream)
    }

    /// 主体此刻拥有的角色标识，去重后按字母序（访问事件的 `roles`，REQ-AMB-AUTH-AUTHZ-EVT-ACCESS-FIELDS）。
    pub fn roles(&self) -> Vec<&'static str> {
        let mut roles: Vec<_> = self.grants.iter().map(|g| g.role.as_str()).collect();
        roles.sort_unstable();
        roles.dedup();
        roles
    }

    /// 令牌条目的来源限定（`sources`）；不限定为 `None`（REQ-AMB-AUTH-AUTHZ-SOURCE-UNRESTRICTED）。
    pub fn sources(&self) -> Option<&[String]> {
        self.sources.as_deref()
    }

    /// 访问事件的 `principal_name`：身份平台令牌的主体且可读名存在时为可读名（REQ-AMB-AUTH-AUTHZ-EVT-PRINCIPAL-NAME）。
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }
}

/// 授权文件 `idp_grants` 的一个元素：按组、主体声明的值或客户端编号之一匹配身份平台令牌
/// （REQ-AMB-AUTH-AUTHZ-FILE-IDP-GRANTS）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdpMatch {
    Group(String),
    Subject(String),
    ClientId(String),
}

/// `idp_grants` 的一个元素：匹配条件与授予。
#[derive(Debug, Clone)]
pub struct IdpGrant {
    pub matcher: IdpMatch,
    pub grants: Arc<[Grant]>,
}

impl IdpGrant {
    /// 令牌满足本元素的条件：组之一、主体声明的值或客户端编号与之按字节相等（REQ-AMB-AUTH-AUTHZ-IDP-MAPPING）。
    fn matches(&self, claims: &crate::oidc::Claims) -> bool {
        match &self.matcher {
            IdpMatch::Group(g) => claims.groups.iter().any(|x| x == g),
            IdpMatch::Subject(s) => claims.subject == *s,
            IdpMatch::ClientId(c) => claims.client_id.as_deref() == Some(c.as_str()),
        }
    }
}

/// 授权文件中的一个令牌条目（REQ-AMB-AUTH-AUTHZ-FILE-TOKENS）。
#[derive(Debug, Clone)]
pub struct TokenEntry {
    pub name: String,
    /// `hashes` 各项的 SHA-256 摘要。
    pub digests: Vec<[u8; 32]>,
    pub grants: Arc<[Grant]>,
    pub expires_at: Option<jiff::Timestamp>,
    pub sources: Option<Arc<[String]>>,
}

/// 一份有效的授权文件内容。
#[derive(Debug, Clone, Default)]
pub struct Table {
    pub tokens: Vec<TokenEntry>,
    /// 摘要 → 令牌条目下标（摘要在整个文件内两两不同）。
    by_digest: HashMap<[u8; 32], usize>,
    /// 身份平台的组、主体声明、客户端编号到角色的映射（`idp_grants`）。
    pub idp_grants: Vec<IdpGrant>,
}

/// 按授权文件判定一个凭据的结果。
#[derive(Debug)]
pub enum Lookup {
    /// 有效的服务令牌。
    Valid(Principal),
    /// 不是有效凭据：不符合格式、摘要未登记，或条目已过期（REQ-AMB-AUTH-AUTHZ-TOKEN-MATCH、-TOKEN-EXPIRY）。
    Invalid,
}

impl Table {
    /// 在时刻 `now` 判定凭据 `credential`：符合服务令牌格式、摘要登记在某个条目的 `hashes` 中、该条目未过期。
    pub fn lookup(&self, credential: &[u8], now: jiff::Timestamp) -> Lookup {
        if !token::is_service_token(credential) {
            return Lookup::Invalid;
        }
        let Some(&i) = self.by_digest.get(&token::digest(credential)) else {
            return Lookup::Invalid;
        };
        let entry = &self.tokens[i];
        match entry.expires_at {
            Some(at) if at <= now => Lookup::Invalid,
            _ => Lookup::Valid(Principal::service_token(entry)),
        }
    }
}

/// `log.access`：为哪些端点类别输出 `access`（REQ-AMB-AUTH-AUTHZ-EVT-ACCESS-CLASSES；取值按 REQ-AMB-CFG-STARTUP-V10）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LogAccess {
    None,
    #[default]
    Reads,
    All,
}

impl LogAccess {
    pub fn parse(s: &str) -> Option<LogAccess> {
        match s {
            "none" => Some(LogAccess::None),
            "reads" => Some(LogAccess::Reads),
            "all" => Some(LogAccess::All),
            _ => None,
        }
    }
}

/// 授权文件的来源：配置取值原文（`authz_reload_failed` 的字段）与按工作目录解析后的路径。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FileSource {
    pub text: String,
    pub path: PathBuf,
}

/// 进程的认证与授权设置：`auth.enabled`、`auth.token`、授权文件的当前内容与 `log.access`。全进程一份，接入面与各端点共用。
pub struct Authz {
    enabled: bool,
    token: Option<String>,
    file: Option<(FileSource, RwLock<Arc<Table>>)>,
    log_access: LogAccess,
    /// 配置了 `auth.oidc.issuer` 时的身份平台（REQ-AMB-AUTH-OIDC）；认证关闭时不设。
    idp: Option<Arc<crate::oidc::Idp>>,
    counters: AuthCounters,
}

impl std::fmt::Debug for Authz {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Authz")
            .field("enabled", &self.enabled)
            .field("token", &self.token.as_ref().map(|_| "***"))
            .field("file", &self.file.as_ref().map(|(s, _)| &s.text))
            .field("log_access", &self.log_access)
            .finish()
    }
}

impl Authz {
    /// 认证开启：`token` 为 `auth.token`（未配置为 `None`），`file` 为授权文件的来源与启动时读到的有效内容。
    pub fn enabled(
        token: Option<String>,
        file: Option<(FileSource, Table)>,
        log_access: LogAccess,
    ) -> Authz {
        Authz {
            enabled: true,
            token,
            file: file.map(|(src, table)| (src, RwLock::new(Arc::new(table)))),
            log_access,
            idp: None,
            counters: AuthCounters::default(),
        }
    }

    /// 认证关闭：全部放行，授权文件不生效（REQ-AMB-AUTH-AUTHZ-DISABLED-FILE-IGNORED）。
    pub fn disabled() -> Authz {
        Authz {
            enabled: false,
            token: None,
            file: None,
            log_access: LogAccess::default(),
            idp: None,
            counters: AuthCounters::default(),
        }
    }

    /// 只配 `auth.token`、不配授权文件（测试与进程内装配用）。
    pub fn with_token(token: impl Into<String>) -> Authz {
        Authz::enabled(Some(token.into()), None, LogAccess::default())
    }

    /// 接上身份平台（认证开启、配置了 `auth.oidc.issuer` 时；REQ-AMB-AUTH-OIDC-ENABLED）。
    pub fn with_idp(mut self, idp: Arc<crate::oidc::Idp>) -> Authz {
        if self.enabled {
            self.idp = Some(idp);
        }
        self
    }

    /// 身份平台；未配置或认证关闭时为 `None`。
    pub fn idp(&self) -> Option<&Arc<crate::oidc::Idp>> {
        self.idp.as_ref()
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn log_access(&self) -> LogAccess {
        self.log_access
    }

    /// 认证与授权的计数（`/metrics` 读它）。
    pub fn counters(&self) -> &AuthCounters {
        &self.counters
    }

    /// 授权文件的来源；未配置为 `None`。
    pub fn file(&self) -> Option<&FileSource> {
        self.file.as_ref().map(|(src, _)| src)
    }

    /// 授权文件的当前内容（请求开始时取一次，同一请求的认证与授权用同一份）。
    pub fn table(&self) -> Option<Arc<Table>> {
        self.file
            .as_ref()
            .map(|(_, t)| Arc::clone(&t.read().unwrap_or_else(|p| p.into_inner())))
    }

    /// 换入重读到的有效内容（REQ-AMB-AUTH-AUTHZ-RELOAD）。
    pub fn replace_table(&self, table: Table) {
        if let Some((_, current)) = &self.file {
            *current.write().unwrap_or_else(|p| p.into_inner()) = Arc::new(table);
        }
    }

    /// 认证一步：按请求头得出主体；凭据缺失或无效为 `None`（以 401 作答）。认证关闭时恒为不认证的主体。
    ///
    /// 恰有一行 `Authorization`，方案名不区分大小写地等于 `Bearer`，其后一个或多个空格，余下部分为 token；token 先与
    /// `auth.token` 逐字节比较，不等时按服务令牌查授权文件（REQ-AMB-AUTH-CREDENTIAL-V3）。
    pub fn authenticate(&self, headers: &HeaderMap) -> Option<Principal> {
        if !self.enabled {
            return Some(Principal::unauthenticated());
        }
        let presented = bearer(headers)?;
        if let Some(token) = &self.token
            && constant_time_eq(presented, token.as_bytes())
        {
            return Some(Principal::auth_token());
        }
        match self.table()?.lookup(presented, jiff::Timestamp::now()) {
            Lookup::Valid(p) => Some(p),
            Lookup::Invalid => None,
        }
    }
}

impl Authz {
    /// 认证一步的最后一种凭据：token 与 `auth.token` 不等、也不是有效的服务令牌（[`Authz::authenticate`] 为
    /// `None`），配置了 `auth.oidc.issuer` 且 token 形如 JWS 紧凑序列时，按 REQ-AMB-AUTH-OIDC 验证（REQ-AMB-AUTH-CREDENTIAL-V3）；
    /// 验证通过时按授权文件的 `idp_grants` 得出主体。其余情形与 [`Authz::authenticate`] 相同。
    pub async fn authenticate_idp(&self, headers: &HeaderMap) -> Option<Principal> {
        if let Some(p) = self.authenticate(headers) {
            return Some(p);
        }
        let idp = self.idp.as_ref()?;
        let presented = bearer(headers)?;
        if !crate::oidc::jwt::looks_like_jws(presented) {
            return None;
        }
        let claims = idp.verify(presented).await?;
        Some(Principal::idp(claims, self.table().as_deref()))
    }
}

/// `Authorization: Bearer <token>` 中的 token；头缺失、多于一行或方案不是 `Bearer` 为 `None`。
fn bearer(headers: &HeaderMap) -> Option<&[u8]> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return None;
    };
    // 字段值首尾的空白不属于字段值（RFC 9110 §5.5）
    let v = value.as_bytes().trim_ascii();
    let sp = v.iter().position(|&b| b == b' ')?;
    let (scheme, rest) = v.split_at(sp);
    scheme
        .eq_ignore_ascii_case(b"bearer")
        .then(|| rest.trim_ascii_start())
}

/// 两边各取 SHA-256 摘要再逐字节比较：摘要定长，耗时既不随首个不同字节的位置变化，也不随口令的长度变化。
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    let (a, b) = (Sha256::digest(a), Sha256::digest(b));
    let diff = a.iter().zip(b.iter()).fold(0u8, |d, (x, y)| d | (x ^ y));
    std::hint::black_box(diff) == 0
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod idp_tests;
