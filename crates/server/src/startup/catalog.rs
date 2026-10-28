//! 只读目录模式（`amberlog catalog`，[`Form::Catalog`](crate::form::Form::Catalog)）的启动流程与停机
//! （REQ-AMB-STO-COLD-CATALOG I 组、REQ-AMB-CFG-STARTUP-V7）：配置（在 [`super::launch`]）→ 只读探测 → 监听。
//!
//! 只启动生命周期编排、接入面、目录投影、健康与指标：不碰 `wal.dir`，不做写探测与条件写探测，不读热层，
//! 不重放 WAL，不起任何后台任务；对象存储只收到 `<storage.prefix>/_iceberg/` 之下的读取与列举请求。

use std::sync::Arc;

use super::{EXIT_LISTEN, EXIT_STORAGE, EXIT_UNCLASSIFIED, Failure, Http, fail, open_store};
use crate::catalog::{self, Catalog};
use crate::config::Config;
use crate::gateway::tls::Inbound;
use crate::gateway::{self, AppState};
use crate::logging::{self, Level};
use crate::shutdown::Lifecycle;
use crate::telemetry::{Health, Metrics};

pub(super) async fn run(
    cfg: Config,
    inbound: Option<Inbound>,
    lifecycle: Arc<Lifecycle>,
) -> Result<(), Failure> {
    // 由配置装载按 `Form::Catalog` 的必填项保证
    let Some(auth) = cfg.auth.clone() else {
        return Err(fail(
            EXIT_UNCLASSIFIED,
            "catalog requires authentication settings",
        ));
    };

    // 只读探测失败（含 `file://` 所指目录不存在）以退出码 3 退出，此前不监听（RO-PROBE-FAIL）
    let store = open_store(&cfg)?;
    store
        .probe_readonly()
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;

    amberlog_storage::fault::pause("开始监听之前");
    let listener = tokio::net::TcpListener::bind(cfg.http_addr)
        .await
        .map_err(|e| {
            fail(
                EXIT_LISTEN,
                format!("cannot listen on {}: {e}", cfg.http_addr),
            )
        })?;
    let local_addr = listener.local_addr().map_err(|e| {
        fail(
            EXIT_LISTEN,
            format!("cannot read the listening address: {e}"),
        )
    })?;

    let health = Arc::new(Health::catalog(cfg.object_store_url.clone()));
    let state = AppState {
        health: Arc::clone(&health),
        metrics: Arc::new(Metrics::catalog()),
        auth: Arc::new(auth),
    };
    let catalog = Catalog::new(store.clone(), cfg.storage_prefix.clone());
    health.set_listening(local_addr, inbound.is_some());
    health.spawn_probe(store);
    health.evaluate();

    lifecycle.serving();

    let app = gateway::catalog_app(state, catalog::router(catalog));
    // 配了证书时只以 TLS 提供服务，证书重读随之开始（与 `serve` 相同，REQ-AMB-ARCH-ENCRYPTION）
    let mut server = match &inbound {
        Some(tls) => {
            let listener = tls.listener(listener).map_err(|e| {
                fail(
                    EXIT_LISTEN,
                    format!("cannot listen on {}: {e}", cfg.http_addr),
                )
            })?;
            tls.spawn_reload();
            Http::spawn(listener, app)
        }
        None => Http::spawn(listener, app),
    };
    tokio::select! {
        failure = server.failed() => return Err(failure),
        () = lifecycle.stopping() => {}
    }

    // 优雅停机（RO-SHUTDOWN-EXIT）：新到的目录前缀下的请求 503，`/readyz` 报 shutting_down；在途请求至多等
    // `server.shutdown_timeout`，等它们的应答写完，然后以退出码 0 退出。没有要 flush 或提交的东西。
    let deadline = tokio::time::Instant::now() + cfg.shutdown_timeout;
    health.begin_shutdown();
    if !health.drained(cfg.shutdown_timeout).await {
        logging::log(
            Level::Warn,
            "in-flight requests did not finish within server.shutdown_timeout",
            &[],
        );
    }
    server.close(deadline).await;
    Ok(())
}
