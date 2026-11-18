//! 接收连接并作答（REQ-AMB-CFG-STARTUP-V10）：只以 HTTP/1.1（与 HTTP/1.0）受理请求，不做 HTTP/2 的协议探测（以 HTTP/2
//! 连接序言开头的连接按无法按 HTTP/1.1 解析的报文处理）；连接建立（TLS 时握手完成）或上一个响应发完起 30 秒收不全请求头
//! 即关闭该连接、不作响应，不妨碍其他连接。
//!
//! 明文 TCP 与 [`super::tls`] 交出的 TLS 连接共用这里。收到关闭信号后与 `axum::serve(...).with_graceful_shutdown` 相同：
//! 不再接受新连接，每个连接写完手上的应答后关闭（空闲连接与还没收全请求头的连接立即关闭），全部连接关闭后才返回；等多久
//! 由调用方限定（REQ-AMB-LIFE-DURABILITY-V4）。

use std::future::Future;
use std::io;
use std::pin::pin;
use std::time::Duration;

use axum::Router;
use axum::serve::Listener;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper_util::rt::{TokioIo, TokioTimer};
use hyper_util::service::TowerToHyperService;
use tokio::sync::watch;

/// 收全请求头的时限（块定，不可配置）。hyper 从开始等待下一个请求头时计时：新连接自接收起，keep-alive 连接自上一个
/// 响应发完起。
const HEADER_READ_TIMEOUT: Duration = Duration::from_secs(30);

/// 在 `listener` 上接收连接，以 `app` 作答，直到 `closing` 完成；然后等全部连接关闭再返回。接收出错由 `listener` 自己
/// 处置并重试。
pub async fn serve<L: Listener>(
    mut listener: L,
    app: Router,
    closing: impl Future<Output = ()> + Send,
) -> io::Result<()> {
    // 每个连接任务持有一个接收端：发送即通知它们关闭，接收端全部析构即全部连接已关闭
    let (close, closed) = watch::channel(());
    let mut closing = pin!(closing);
    loop {
        let (io, _) = tokio::select! {
            accepted = listener.accept() => accepted,
            () = &mut closing => break,
        };
        let service = TowerToHyperService::new(app.clone().into_service::<Incoming>());
        let mut closed = closed.clone();
        tokio::spawn(async move {
            // 超时、对端断开与坏报文都只结束这一个连接
            let mut conn = pin!(
                http1::Builder::new()
                    .timer(TokioTimer::new())
                    .header_read_timeout(HEADER_READ_TIMEOUT)
                    .serve_connection(TokioIo::new(io), service)
            );
            tokio::select! {
                _ = conn.as_mut() => return,
                _ = closed.changed() => conn.as_mut().graceful_shutdown(),
            }
            let _ = conn.await;
            drop(closed);
        });
    }
    drop(listener);
    drop(closed);
    close.send_replace(());
    close.closed().await;
    Ok(())
}
