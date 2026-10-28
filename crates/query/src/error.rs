//! 查询的失败：对外的 `reason` 加一句说明（`message` 不是稳定契约，REQ-AMB-ERR-ENVELOPE）。

use std::fmt;

use amberlog_common::error::Reason;
use amberlog_storage::chain::ChainError;

/// 一次 `_search` 的失败。`reason` 决定状态码；5xx 的 `message` 是底层错误的说明，只进日志，不进响应
/// （REQ-AMB-ERR-ENVELOPE-V7）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    pub reason: Reason,
    pub message: String,
    /// 503 `unavailable` 是因为作答所需的数据在查询期间已移动（可重试，REQ-AMB-QRY-CROSS-TIER-MOVED），
    /// 而不是对象存储不可访问。
    pub moved: bool,
}

impl QueryError {
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        QueryError {
            reason,
            message: message.into(),
            moved: false,
        }
    }

    /// 作答所需的数据在查询期间已移动：503 `unavailable`，`message` 为读取时的说明。
    pub fn moved(message: impl Into<String>) -> Self {
        QueryError {
            moved: true,
            ..Self::new(Reason::Unavailable, message)
        }
    }

    pub fn invalid_request(message: impl Into<String>) -> Self {
        Self::new(Reason::InvalidRequest, message)
    }

    pub fn invalid_sql(message: impl Into<String>) -> Self {
        Self::new(Reason::InvalidSql, message)
    }

    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(Reason::SqlUnsupported, message)
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self::new(Reason::Internal, message)
    }
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason.as_str(), self.message)
    }
}

impl std::error::Error for QueryError {}

/// 读取快照链或数据文件的失败按 REQ-AMB-QRY-SEARCH-ENDPOINT-V3 作答：不可访问 503 `unavailable`，
/// 不存在或内容无法解析 500 `internal`，停用状态按状态作答。
impl From<ChainError> for QueryError {
    fn from(e: ChainError) -> Self {
        match e {
            ChainError::Unavailable(m) => QueryError::new(Reason::Unavailable, m),
            ChainError::Integrity(m) => QueryError::internal(m),
            ChainError::Disabled(state) => match state.reason() {
                Some(reason) => QueryError::new(reason, "dataset is disabled until restart"),
                None => QueryError::internal("dataset reported disabled in normal state"),
            },
        }
    }
}
