//! 可同时打开的文件数（`RLIMIT_NOFILE`）：启动时把软上限调到硬上限，在日志里报出生效值；生效值低于 [`LOW`] 时另打一行
//! warn，提醒运维调高硬上限。系统默认的软上限常为 1024，每条连接、每个 WAL 段文件都占一个，
//! 同时有约 1000 个连接时就会打不开新的 WAL 段、也连不上对象存储。调软上限不需要特权；硬上限照旧由运维设定。
//! 只在 Linux 上调整；两行日志都是非稳定日志。

use serde_json::Value;

use crate::logging::{self, Level};

/// 生效的软上限低于这个数时打 warn。
pub const LOW: u64 = 65536;

/// 调整后的软上限与硬上限。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Limits {
    soft: u64,
    hard: u64,
}

/// 调高软上限并把结果写进日志（在 `starting` 之后调用）。
pub fn raise_and_report() {
    match raise() {
        Ok(Some(l)) => {
            let fields = [("soft", Value::from(l.soft)), ("hard", Value::from(l.hard))];
            logging::log(
                Level::Info,
                &format!("open file limit is {}", l.soft),
                &fields,
            );
            if l.soft < LOW {
                logging::log(
                    Level::Warn,
                    &format!(
                        "open file limit {} is below {LOW}: raise the hard limit (LimitNOFILE for systemd, --ulimit nofile for containers)",
                        l.soft
                    ),
                    &fields,
                );
            }
        }
        Ok(None) => {}
        Err(e) => logging::log(
            Level::Warn,
            &format!("cannot raise the open file limit: {e}"),
            &[("reason", Value::from(e.to_string()))],
        ),
    }
}

/// 把软上限调到硬上限；硬上限不限时取内核允许的最大值 `/proc/sys/fs/nr_open`。不调整的平台返回 `None`。
#[cfg(target_os = "linux")]
fn raise() -> std::io::Result<Option<Limits>> {
    let mut lim = libc::rlimit {
        rlim_cur: 0,
        rlim_max: 0,
    };
    // SAFETY: 传入的指针指向本函数里的有效 `rlimit`。
    if unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, &mut lim) } != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let target = if lim.rlim_max == libc::RLIM_INFINITY {
        std::fs::read_to_string("/proc/sys/fs/nr_open")
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(lim.rlim_cur)
    } else {
        lim.rlim_max
    };
    if target > lim.rlim_cur {
        let raised = libc::rlimit {
            rlim_cur: target,
            rlim_max: lim.rlim_max,
        };
        // SAFETY: 同上。
        if unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &raised) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        lim.rlim_cur = target;
    }
    Ok(Some(Limits {
        soft: lim.rlim_cur,
        hard: lim.rlim_max,
    }))
}

#[cfg(not(target_os = "linux"))]
fn raise() -> std::io::Result<Option<Limits>> {
    Ok(None)
}
