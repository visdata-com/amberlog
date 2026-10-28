//! 错误信封响应（REQ-AMB-ERR-ENVELOPE）。

use amberlog_common::error::{ErrorBody, Reason};
use axum::Json;
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde_json::{Map, Value};

/// HTTP 500 的固定文案：不带内部路径、堆栈或依赖库的原始错误文本。
const INTERNAL_MESSAGE: &str = "internal server error";

/// 标记一个响应已是错误信封，[`super::routing::normalize`] 见到它就不再改写。带着状态码、`reason` 与 `message`，
/// 目录前缀下的出口据此改写为目录错误体（[`crate::catalog::shape`]）；5xx 另带底层错误原文与是否写日志，
/// 由最外层写那一行 `error` 日志（[`super::request_id::propagate`]）。
#[derive(Clone)]
pub(crate) struct Enveloped {
    pub code: u16,
    pub reason: Reason,
    pub message: String,
    /// 底层错误的原文：只进日志，不进响应。
    pub cause: Option<String>,
    /// 5xx 是否写 `error` 日志（停机闸门的 503 不写）。
    pub logged: bool,
}

/// 对外的错误响应。各端点返回它，而不是直接拼状态码与响应体。
///
/// 5xx 的 `message` 一律是写定的文字，不由底层错误的文本拼成（REQ-AMB-ERR-ENVELOPE-V7）；底层错误的原文经
/// [`ApiError::with_cause`] 交给出口写进日志。
#[derive(Debug)]
pub struct ApiError {
    body: ErrorBody,
    allow: Option<HeaderValue>,
    cause: Option<String>,
    logged: bool,
}

impl ApiError {
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        ApiError {
            body: ErrorBody::new(reason, message),
            allow: None,
            cause: None,
            logged: true,
        }
    }

    /// 未归类的服务端错误。原始错误经 [`ApiError::with_cause`] 进日志，不进响应。
    pub fn internal() -> Self {
        ApiError::new(Reason::Internal, INTERNAL_MESSAGE)
    }

    /// 405，`allow` 为该路径支持的方法（逗号分隔）。
    pub fn method_not_allowed(allow: HeaderValue) -> Self {
        ApiError {
            allow: Some(allow),
            ..ApiError::new(Reason::MethodNotAllowed, "method not allowed for this path")
        }
    }

    /// 出错原因的原文（底层错误的文本）：只写进这个响应的 `error` 日志，不进响应体。
    pub fn with_cause(mut self, cause: impl Into<String>) -> Self {
        self.cause = Some(cause.into());
        self
    }

    /// 这个 5xx 不写 `error` 日志：只用于停机闸门的 503（REQ-AMB-ERR-ENVELOPE-V7；`/readyz` 未就绪的 503 由出口按
    /// `reason` 认出）。
    pub(crate) fn unlogged(mut self) -> Self {
        self.logged = false;
        self
    }

    pub fn with_details(mut self, details: Map<String, Value>) -> Self {
        self.body = self.body.with_details(details);
        self
    }

    pub fn reason(&self) -> Reason {
        self.body.reason
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.body.code).expect("Reason::status 只返回闭集内的状态码");
        let unauthorized = self.body.reason == Reason::Unauthorized;
        let mark = Enveloped {
            code: self.body.code,
            reason: self.body.reason,
            message: self.body.message.clone(),
            cause: self.cause,
            logged: self.logged,
        };
        // axum 的 Json 响应头为 `application/json`，不带参数。
        let mut res = (status, Json(self.body)).into_response();
        if unauthorized {
            res.headers_mut()
                .insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
        }
        if let Some(allow) = self.allow {
            res.headers_mut().insert(header::ALLOW, allow);
        }
        res.extensions_mut().insert(mark);
        res
    }
}
