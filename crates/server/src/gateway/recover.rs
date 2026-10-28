//! 处理请求时的 panic：按未归类的服务端错误作答（REQ-AMB-ERR-ENVELOPE），而不是让连接被关闭。
//!
//! axum 不接 panic：处理函数 panic 时 hyper 丢掉这个连接，客户端收不到任何响应。这一层在路由表与 404 兜底之外、
//! 认证与目录前缀出口之内，所以目录前缀下的 panic 也由出口改写为目录错误体（[`crate::catalog::shape`]）。
//! 写入路径把后台任务里的 panic 转抛给调用方，同样在这里被接住。panic 文字只作出错原因进这个 5xx 的 `error` 日志
//! （出口写，[`super::request_id`]），不进响应。

use std::any::Any;
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::task::Poll;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use super::ApiError;

pub(crate) async fn catch_panic(req: Request, next: Next) -> Response {
    let mut fut = std::pin::pin!(next.run(req));
    let outcome =
        std::future::poll_fn(
            |cx| match catch_unwind(AssertUnwindSafe(|| fut.as_mut().poll(cx))) {
                Ok(Poll::Ready(res)) => Poll::Ready(Ok(res)),
                Ok(Poll::Pending) => Poll::Pending,
                Err(payload) => Poll::Ready(Err(payload)),
            },
        )
        .await;
    match outcome {
        Ok(res) => res,
        Err(payload) => ApiError::internal()
            .with_cause(format!(
                "request handler panicked: {}",
                detail(payload.as_ref())
            ))
            .into_response(),
    }
}

fn detail(payload: &(dyn Any + Send)) -> &str {
    if let Some(s) = payload.downcast_ref::<&'static str>() {
        s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s
    } else {
        "non-string panic payload"
    }
}
