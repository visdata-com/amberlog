//! 授权文件的重读（REQ-AMB-AUTH-AUTHZ-RELOAD、-RELOAD-EFFECT、-RELOAD-INVALID、-EVT-RELOAD-FAILED）与认证、授权的计数
//! （REQ-AMB-OBS-HEALTH-METRICS-V7 的 `amberlog_auth_denied_total`、`amberlog_authz_reload_failures_total`）。
//!
//! 重读照入站证书的重读做（`gateway/tls.rs`）：每 60 秒读一次，读到有效内容即原子换入，此后开始认证的请求按新内容判定；
//! 读到无效内容时沿用旧内容、计一次失败，按「读取结果与上一次输出时相同即为同一份」去重输出 `authz_reload_failed`。

use std::io;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use axum::http::StatusCode;
use serde_json::Value;
use tokio::task::JoinHandle;

use super::{Authz, Table, file};
use crate::logging::{self, Level};

/// 重读周期（块定，不可配置）。
const RELOAD_INTERVAL: Duration = Duration::from_secs(60);

/// 认证区内以 401、403 作答的请求数与授权文件重读读到无效内容的次数；启动即为 0，`/metrics` 读它们。
#[derive(Debug, Default)]
pub struct AuthCounters {
    unauthorized: AtomicU64,
    forbidden: AtomicU64,
    reload_failures: AtomicU64,
}

impl AuthCounters {
    /// 以状态码 `status`（401 或 403）作答的请求数；其余状态码恒为 0。
    pub fn denied(&self, status: u16) -> u64 {
        match status {
            401 => self.unauthorized.load(Ordering::Relaxed),
            403 => self.forbidden.load(Ordering::Relaxed),
            _ => 0,
        }
    }

    /// 重读读到无效内容的次数（每次都计，不按事件的去重合并；启动时的读取不计）。
    pub fn reload_failures(&self) -> u64 {
        self.reload_failures.load(Ordering::Relaxed)
    }

    /// 认证区内一个请求的最终状态码：401、403 各计一次，其余不计（按不存在作答的不计入）。
    pub(crate) fn record_final(&self, status: StatusCode) {
        match status {
            StatusCode::UNAUTHORIZED => self.unauthorized.fetch_add(1, Ordering::Relaxed),
            StatusCode::FORBIDDEN => self.forbidden.fetch_add(1, Ordering::Relaxed),
            _ => return,
        };
    }

    fn record_reload_failure(&self) {
        self.reload_failures.fetch_add(1, Ordering::Relaxed);
    }
}

/// 一次读取的结果：内容，或「不存在」「不可读」两种状态之一（同一份无效内容按它判定）。
#[derive(Debug, Clone, PartialEq, Eq)]
enum FileState {
    Content(Vec<u8>),
    Missing,
    Unreadable,
}

fn read_state(path: &Path) -> FileState {
    match std::fs::read(path) {
        Ok(bytes) => FileState::Content(bytes),
        Err(e) if e.kind() == io::ErrorKind::NotFound => FileState::Missing,
        Err(_) => FileState::Unreadable,
    }
}

fn parse(state: &FileState) -> Result<Table, String> {
    match state {
        FileState::Content(bytes) => std::str::from_utf8(bytes)
            .map_err(|_| "not a valid TOML document: the file is not UTF-8".to_owned())
            .and_then(|text| file::parse(text).map_err(|why| why.to_string())),
        FileState::Missing => Err("file does not exist".to_owned()),
        FileState::Unreadable => Err("file is not readable".to_owned()),
    }
}

impl Authz {
    /// 认证开启且配了授权文件时，起每 60 秒一次的重读任务；否则不起（返回 `None`）。不因重读失败退出，也不碰就绪状态。
    pub fn spawn_reload(self: &Arc<Self>) -> Option<JoinHandle<()>> {
        if !self.enabled {
            return None;
        }
        let src = self.file()?.clone();
        let authz = Arc::clone(self);
        Some(tokio::spawn(async move {
            let mut tick = tokio::time::interval(RELOAD_INTERVAL);
            tick.tick().await;
            // 上一次输出 `authz_reload_failed` 时的读取结果；读到有效内容即清空
            let mut reported: Option<FileState> = None;
            loop {
                tick.tick().await;
                let now = read_state(&src.path);
                match parse(&now) {
                    Ok(table) => {
                        authz.replace_table(table);
                        reported = None;
                    }
                    Err(why) => {
                        authz.counters.record_reload_failure();
                        if reported.as_ref() != Some(&now) {
                            logging::event(
                                Level::Error,
                                "authz_reload_failed",
                                &format!(
                                    "the authorization file is not valid; still using its previous content ({why})"
                                ),
                                &[("authz_file", Value::from(src.text.clone()))],
                            );
                            reported = Some(now);
                        }
                    }
                }
            }
        }))
    }
}
