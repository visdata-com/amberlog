//! 错误分类：`reason` 注册表与错误信封体（REQ-AMB-ERR-ENVELOPE-V7）。
//!
//! `type_conflict` 自 REQ-AMB-DM-RECORD-MODEL-V3 起不再产生，按注册表「只增不改」留在枚举中。
//!
//! 注册表发布后只增不改：已有取值不得删除或改变含义，新增取值须先进块的注册表条款。
//! 序列化值与块逐字一致，由单元测试 `error_reason_registry_matches_block` 对照测试内的注册表列表，内部仓库里还对照块原文。

use serde::Serialize;
use serde_json::{Map, Value};

/// 请求级 `reason`：出现在错误信封顶层。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    InvalidJson,
    InvalidEncoding,
    InvalidRequest,
    AllRecordsRejected,
    InvalidStreamName,
    OrgNotFound,
    StreamNotFound,
    InvalidSql,
    SqlUnsupported,
    InvalidTimeRange,
    Unauthorized,
    NotFound,
    MethodNotAllowed,
    PayloadTooLarge,
    UnsupportedEncoding,
    UnsupportedFormatVersion,
    Unavailable,
    NotReady,
    UnknownColumn,
    NumericOverflow,
    TooManyQueries,
    QueryMemoryExceeded,
    NamespaceNotFound,
    TableNotFound,
    Internal,
}

impl Reason {
    pub const ALL: [Reason; 25] = [
        Reason::InvalidJson,
        Reason::InvalidEncoding,
        Reason::InvalidRequest,
        Reason::AllRecordsRejected,
        Reason::InvalidStreamName,
        Reason::OrgNotFound,
        Reason::StreamNotFound,
        Reason::InvalidSql,
        Reason::SqlUnsupported,
        Reason::InvalidTimeRange,
        Reason::Unauthorized,
        Reason::NotFound,
        Reason::MethodNotAllowed,
        Reason::PayloadTooLarge,
        Reason::UnsupportedEncoding,
        Reason::UnsupportedFormatVersion,
        Reason::Unavailable,
        Reason::NotReady,
        Reason::UnknownColumn,
        Reason::NumericOverflow,
        Reason::TooManyQueries,
        Reason::QueryMemoryExceeded,
        Reason::NamespaceNotFound,
        Reason::TableNotFound,
        Reason::Internal,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Reason::InvalidJson => "invalid_json",
            Reason::InvalidEncoding => "invalid_encoding",
            Reason::InvalidRequest => "invalid_request",
            Reason::AllRecordsRejected => "all_records_rejected",
            Reason::InvalidStreamName => "invalid_stream_name",
            Reason::OrgNotFound => "org_not_found",
            Reason::StreamNotFound => "stream_not_found",
            Reason::InvalidSql => "invalid_sql",
            Reason::SqlUnsupported => "sql_unsupported",
            Reason::InvalidTimeRange => "invalid_time_range",
            Reason::Unauthorized => "unauthorized",
            Reason::NotFound => "not_found",
            Reason::MethodNotAllowed => "method_not_allowed",
            Reason::PayloadTooLarge => "payload_too_large",
            Reason::UnsupportedEncoding => "unsupported_encoding",
            Reason::UnsupportedFormatVersion => "unsupported_format_version",
            Reason::Unavailable => "unavailable",
            Reason::NotReady => "not_ready",
            Reason::UnknownColumn => "unknown_column",
            Reason::NumericOverflow => "numeric_overflow",
            Reason::TooManyQueries => "too_many_queries",
            Reason::QueryMemoryExceeded => "query_memory_exceeded",
            Reason::NamespaceNotFound => "namespace_not_found",
            Reason::TableNotFound => "table_not_found",
            Reason::Internal => "internal",
        }
    }

    /// 该 `reason` 对应的 HTTP 状态码。
    ///
    /// 各 `reason` 的状态码分散定义在各块（本块、REQ-AMB-DM-NAMESPACE、REQ-AMB-QRY-SEARCH-ENDPOINT-V2、
    /// REQ-AMB-STO-HOT-LAYOUT 等），每个 `reason` 恰对应一个状态码，且全部落在本块的闭集
    /// {400, 401, 404, 405, 413, 415, 429, 500, 503} 内（REQ-AMB-ERR-ENVELOPE-V6 起含 429，只用于 `too_many_queries`）；
    /// 由 `code` 取自这里，信封的 `code` 与状态码不会错位。
    pub fn status(self) -> u16 {
        match self {
            Reason::InvalidJson
            | Reason::InvalidEncoding
            | Reason::InvalidRequest
            | Reason::AllRecordsRejected
            | Reason::InvalidStreamName
            | Reason::StreamNotFound
            | Reason::InvalidSql
            | Reason::SqlUnsupported
            | Reason::InvalidTimeRange
            | Reason::UnknownColumn
            | Reason::NumericOverflow
            | Reason::QueryMemoryExceeded => 400,
            Reason::Unauthorized => 401,
            Reason::OrgNotFound
            | Reason::NotFound
            | Reason::NamespaceNotFound
            | Reason::TableNotFound => 404,
            Reason::MethodNotAllowed => 405,
            Reason::PayloadTooLarge => 413,
            Reason::UnsupportedEncoding => 415,
            Reason::TooManyQueries => 429,
            Reason::Internal => 500,
            Reason::UnsupportedFormatVersion | Reason::Unavailable | Reason::NotReady => 503,
        }
    }
}

/// 逐条记录错误的 `reason`：出现在逐条状态的 `errors[]` 条目里（REQ-AMB-ING-ACK-RESPONSE）。
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RecordReason {
    MalformedRecord,
    NotAnObject,
    ReservedField,
    InvalidFieldName,
    TypeConflict,
    TimestampInvalid,
    TimestampOutOfRange,
    TooManyColumns,
}

impl RecordReason {
    pub const ALL: [RecordReason; 8] = [
        RecordReason::MalformedRecord,
        RecordReason::NotAnObject,
        RecordReason::ReservedField,
        RecordReason::InvalidFieldName,
        RecordReason::TypeConflict,
        RecordReason::TimestampInvalid,
        RecordReason::TimestampOutOfRange,
        RecordReason::TooManyColumns,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            RecordReason::MalformedRecord => "malformed_record",
            RecordReason::NotAnObject => "not_an_object",
            RecordReason::ReservedField => "reserved_field",
            RecordReason::InvalidFieldName => "invalid_field_name",
            RecordReason::TypeConflict => "type_conflict",
            RecordReason::TimestampInvalid => "timestamp_invalid",
            RecordReason::TimestampOutOfRange => "timestamp_out_of_range",
            RecordReason::TooManyColumns => "too_many_columns",
        }
    }
}

/// 错误信封的响应体：`code` 恒等于 HTTP 状态码，`message` 不是稳定契约，`details` 按 `reason` 逐项定义。
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct ErrorBody {
    pub code: u16,
    pub reason: Reason,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<Map<String, Value>>,
}

impl ErrorBody {
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        ErrorBody {
            code: reason.status(),
            reason,
            message: message.into(),
            details: None,
        }
    }

    pub fn with_details(mut self, details: Map<String, Value>) -> Self {
        self.details = Some(details);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    /// 块（现行版本 REQ-AMB-ERR-ENVELOPE-V7；块再被 supersede 时改指新文件）注册表条款所列的全部取值，按条款中的出现顺序
    /// （条款末尾括注里再次出现的 `type_conflict` 只记一次）。
    const REGISTRY: [&str; 33] = [
        "invalid_json",
        "invalid_encoding",
        "invalid_request",
        "all_records_rejected",
        "invalid_stream_name",
        "org_not_found",
        "stream_not_found",
        "invalid_sql",
        "sql_unsupported",
        "invalid_time_range",
        "unauthorized",
        "not_found",
        "method_not_allowed",
        "payload_too_large",
        "unsupported_encoding",
        "unsupported_format_version",
        "unavailable",
        "not_ready",
        "unknown_column",
        "numeric_overflow",
        "too_many_queries",
        "query_memory_exceeded",
        "namespace_not_found",
        "table_not_found",
        "internal",
        "not_an_object",
        "reserved_field",
        "invalid_field_name",
        "type_conflict",
        "timestamp_invalid",
        "timestamp_out_of_range",
        "malformed_record",
        "too_many_columns",
    ];

    /// 取出块中列举注册表的那一条条款里、「注册表：」之后的全部反引号取值。工作区根目录下有 `knowledge/` 时块文件必须在，
    /// 读不到即失败；源码树里没有 `knowledge/` 目录时返回 `None`，只比对上面的 `REGISTRY`。
    fn registry_in_block() -> Option<BTreeSet<String>> {
        let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        if !root.join("knowledge").is_dir() {
            return None;
        }
        let path = root.join("knowledge/requirements/req-amb-err-envelope-v7.md");
        let block = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("读不到块文件 {}：{e}", path.display()));
        let clause = block
            .lines()
            .find(|l| l.starts_with("[REQ-AMB-ERR-ENVELOPE-V7]") && l.contains("注册表："))
            .expect("块中找不到列举 reason 注册表的条款");
        let list = clause.split_once("注册表：").unwrap().1;
        Some(
            list.split('`')
                .skip(1)
                .step_by(2)
                .map(str::to_owned)
                .collect(),
        )
    }

    // 编译期守卫：`ALL` 是手写的常量数组，枚举新增取值时下面两个 match 不穷尽而编译失败；补上的分支照样写
    // `const { …_index(…) }`，新取值没有加进 `ALL` 时在编译期报错。由 `error_reason_all_exhaustive` 调用。
    const fn reason_index(r: Reason) -> usize {
        let mut i = 0;
        while i < Reason::ALL.len() {
            if Reason::ALL[i] as u8 == r as u8 {
                return i;
            }
            i += 1;
        }
        panic!("Reason 的这个取值没有加进 Reason::ALL");
    }

    const fn record_reason_index(r: RecordReason) -> usize {
        let mut i = 0;
        while i < RecordReason::ALL.len() {
            if RecordReason::ALL[i] as u8 == r as u8 {
                return i;
            }
            i += 1;
        }
        panic!("RecordReason 的这个取值没有加进 RecordReason::ALL");
    }

    fn reason_guard(r: Reason) -> usize {
        match r {
            Reason::InvalidJson => const { reason_index(Reason::InvalidJson) },
            Reason::InvalidEncoding => const { reason_index(Reason::InvalidEncoding) },
            Reason::InvalidRequest => const { reason_index(Reason::InvalidRequest) },
            Reason::AllRecordsRejected => const { reason_index(Reason::AllRecordsRejected) },
            Reason::InvalidStreamName => const { reason_index(Reason::InvalidStreamName) },
            Reason::OrgNotFound => const { reason_index(Reason::OrgNotFound) },
            Reason::StreamNotFound => const { reason_index(Reason::StreamNotFound) },
            Reason::InvalidSql => const { reason_index(Reason::InvalidSql) },
            Reason::SqlUnsupported => const { reason_index(Reason::SqlUnsupported) },
            Reason::InvalidTimeRange => const { reason_index(Reason::InvalidTimeRange) },
            Reason::Unauthorized => const { reason_index(Reason::Unauthorized) },
            Reason::NotFound => const { reason_index(Reason::NotFound) },
            Reason::MethodNotAllowed => const { reason_index(Reason::MethodNotAllowed) },
            Reason::PayloadTooLarge => const { reason_index(Reason::PayloadTooLarge) },
            Reason::UnsupportedEncoding => const { reason_index(Reason::UnsupportedEncoding) },
            Reason::UnsupportedFormatVersion => {
                const { reason_index(Reason::UnsupportedFormatVersion) }
            }
            Reason::Unavailable => const { reason_index(Reason::Unavailable) },
            Reason::NotReady => const { reason_index(Reason::NotReady) },
            Reason::UnknownColumn => const { reason_index(Reason::UnknownColumn) },
            Reason::NumericOverflow => const { reason_index(Reason::NumericOverflow) },
            Reason::TooManyQueries => const { reason_index(Reason::TooManyQueries) },
            Reason::QueryMemoryExceeded => const { reason_index(Reason::QueryMemoryExceeded) },
            Reason::NamespaceNotFound => const { reason_index(Reason::NamespaceNotFound) },
            Reason::TableNotFound => const { reason_index(Reason::TableNotFound) },
            Reason::Internal => const { reason_index(Reason::Internal) },
        }
    }

    fn record_reason_guard(r: RecordReason) -> usize {
        match r {
            RecordReason::MalformedRecord => {
                const { record_reason_index(RecordReason::MalformedRecord) }
            }
            RecordReason::NotAnObject => const { record_reason_index(RecordReason::NotAnObject) },
            RecordReason::ReservedField => {
                const { record_reason_index(RecordReason::ReservedField) }
            }
            RecordReason::InvalidFieldName => {
                const { record_reason_index(RecordReason::InvalidFieldName) }
            }
            RecordReason::TypeConflict => const { record_reason_index(RecordReason::TypeConflict) },
            RecordReason::TimestampInvalid => {
                const { record_reason_index(RecordReason::TimestampInvalid) }
            }
            RecordReason::TimestampOutOfRange => {
                const { record_reason_index(RecordReason::TimestampOutOfRange) }
            }
            RecordReason::TooManyColumns => {
                const { record_reason_index(RecordReason::TooManyColumns) }
            }
        }
    }

    #[test]
    fn error_reason_all_exhaustive() {
        for (i, r) in Reason::ALL.into_iter().enumerate() {
            assert_eq!(reason_guard(r), i, "{r:?} 在 Reason::ALL 中重复");
        }
        for (i, r) in RecordReason::ALL.into_iter().enumerate() {
            assert_eq!(
                record_reason_guard(r),
                i,
                "{r:?} 在 RecordReason::ALL 中重复"
            );
        }
    }

    #[test]
    fn error_reason_registry_matches_block() {
        let listed: BTreeSet<String> = REGISTRY.iter().map(|s| (*s).to_owned()).collect();
        if let Some(block) = registry_in_block() {
            assert_eq!(listed, block, "REGISTRY 与块的注册表条款不一致");
        }
        let block = listed;
        let code: BTreeSet<String> = Reason::ALL
            .iter()
            .map(|r| r.as_str().to_owned())
            .chain(RecordReason::ALL.iter().map(|r| r.as_str().to_owned()))
            .collect();
        assert_eq!(
            block.len(),
            33,
            "块注册表应为 25 个请求级值与 8 个逐条记录值"
        );
        assert_eq!(code, block);
        // as_str 与 serde 序列化值一致
        for r in Reason::ALL {
            assert_eq!(serde_json::to_value(r).unwrap(), r.as_str());
        }
        for r in RecordReason::ALL {
            assert_eq!(serde_json::to_value(r).unwrap(), r.as_str());
        }
    }

    #[test]
    fn reason_status_within_closed_set() {
        const CLOSED: [u16; 9] = [400, 401, 404, 405, 413, 415, 429, 500, 503];
        for r in Reason::ALL {
            assert!(CLOSED.contains(&r.status()), "{r:?} 的状态码不在闭集内");
            // 429 只用于 `too_many_queries`（REQ-AMB-ERR-ENVELOPE-V6）
            assert_eq!(r.status() == 429, r == Reason::TooManyQueries, "{r:?}");
        }
        assert_eq!(Reason::QueryMemoryExceeded.status(), 400);
        assert_eq!(Reason::NamespaceNotFound.status(), 404);
        assert_eq!(Reason::TableNotFound.status(), 404);
        assert_eq!(Reason::UnsupportedFormatVersion.status(), 503);
    }

    #[test]
    fn error_body_shape() {
        let body = ErrorBody::new(Reason::NotFound, "no such route");
        let v = serde_json::to_value(&body).unwrap();
        assert_eq!(
            v,
            serde_json::json!({"code": 404, "reason": "not_found", "message": "no such route"})
        );
        let mut details = Map::new();
        details.insert("status".into(), Value::Array(vec![]));
        let v = serde_json::to_value(
            ErrorBody::new(Reason::AllRecordsRejected, "x").with_details(details),
        )
        .unwrap();
        assert_eq!(v["code"], 400);
        assert_eq!(v["details"]["status"], serde_json::json!([]));
    }
}
