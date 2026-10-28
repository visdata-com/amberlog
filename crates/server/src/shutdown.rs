//! 生命周期编排的信号部分（REQ-AMB-LIFE-DURABILITY「优雅停机」、ADR-0016）。
//!
//! SIGTERM 与 SIGINT 同样处置，按进程所处阶段：
//! - 启动流程完成之前（从进程最开始算起，见 [`EarlyMask`]）：中止启动。受理请求的形态（`serve`、`catalog`）以退出码 0 退出
//!   （「开始监听之前」，已确认记录留在 WAL 中，下次启动时重放）；离线子命令（`retire`、`evidence`）以退出码 6 退出
//!   （REQ-AMB-CFG-STARTUP-V7、REQ-AMB-LIFE-DURABILITY-V4）；
//! - 启动流程完成之后第一次：受理请求的形态进入优雅停机，离线子命令置取消标志，都由 [`Lifecycle::stopping`]、
//!   [`Lifecycle::stop_requested`] 交给启动流程或执行体处置；
//! - 此后再收到：立即以退出码 1 退出（块只要求非 0，取 ADR-0016 的「未分类」）。

use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::signal::unix::{Signal, SignalKind, signal};
use tokio::sync::Notify;

use crate::form::Form;
use crate::logging::{self, Level};
use crate::startup::{EXIT_INCOMPLETE, EXIT_OK, EXIT_UNCLASSIFIED};

/// 进程最开始对 SIGTERM 与 SIGINT 的屏蔽：处置（[`Lifecycle::install`]）要在 tokio 运行时建好之后才能装上，
/// 这之前到达的信号若按系统默认处置，进程会直接被终止（退出码 143 或 130），而不是以 0 中止启动。
///
/// 做法：进程还只有主线程时屏蔽这两个信号（此后建的线程，包括运行时的工作线程，继承屏蔽），装好处置后只解除主线程的屏蔽；
/// 屏蔽期间到达的信号挂起，解除时立即交付给主线程上的处置。主线程在整个运行期间都停在运行时的 `block_on` 里，一直在。
pub struct EarlyMask(libc::sigset_t);

impl EarlyMask {
    /// 屏蔽本线程的 SIGTERM 与 SIGINT。须在建任何线程之前调用。
    pub fn block() -> EarlyMask {
        // SAFETY: `sigset_t` 是普通数据，全零是合法初值，随后由 sigemptyset 初始化；sigemptyset、sigaddset 只写这个本地变量，
        // pthread_sigmask 只改调用线程的信号屏蔽字，传入的指针都指向有效的本地变量或为空。
        unsafe {
            let mut set: libc::sigset_t = std::mem::zeroed();
            libc::sigemptyset(&mut set);
            libc::sigaddset(&mut set, libc::SIGTERM);
            libc::sigaddset(&mut set, libc::SIGINT);
            libc::pthread_sigmask(libc::SIG_BLOCK, &set, std::ptr::null_mut());
            EarlyMask(set)
        }
    }

    /// 处置已装上：解除本线程（主线程）的屏蔽，挂起的信号随即交付。
    pub fn unblock(self) {
        // SAFETY: 同 [`EarlyMask::block`]；`self.0` 是 block 时初始化好的信号集。
        unsafe {
            libc::pthread_sigmask(libc::SIG_UNBLOCK, &self.0, std::ptr::null_mut());
        }
    }
}

const STARTING: u8 = 0;
const SERVING: u8 = 1;
const STOPPING: u8 = 2;

/// 进程阶段，信号处置据此分支。
pub struct Lifecycle {
    form: Form,
    phase: AtomicU8,
    stop: Notify,
}

impl Lifecycle {
    /// 安装 SIGTERM 与 SIGINT 的处置（替换默认的「被信号终止」），并在后台等待信号。须在 tokio 运行时内调用。
    pub fn install(form: Form) -> std::io::Result<Arc<Lifecycle>> {
        let term = signal(SignalKind::terminate())?;
        let int = signal(SignalKind::interrupt())?;
        let lifecycle = Arc::new(Lifecycle {
            form,
            phase: AtomicU8::new(STARTING),
            stop: Notify::new(),
        });
        tokio::spawn(Arc::clone(&lifecycle).watch(term, int));
        Ok(lifecycle)
    }

    /// 启动流程已完成（受理请求的形态：监听已建立；离线子命令：启动流程到交给执行体为止）：此后的第一次信号进入停机。
    pub fn serving(&self) {
        let _ = self
            .phase
            .compare_exchange(STARTING, SERVING, Ordering::SeqCst, Ordering::SeqCst);
    }

    /// 等到第一次停机信号。
    pub async fn stopping(&self) {
        self.stop.notified().await;
    }

    /// 是否已收到停机信号（启动流程完成之后）：离线子命令的执行体在请求边界检查它。
    pub fn stop_requested(&self) -> bool {
        self.phase.load(Ordering::SeqCst) == STOPPING
    }

    /// 启动流程完成之前收到信号时的退出码。
    fn abort_code(&self) -> u8 {
        match self.form {
            Form::Serve | Form::Catalog => EXIT_OK,
            Form::Retire | Form::Evidence => EXIT_INCOMPLETE,
        }
    }

    async fn watch(self: Arc<Self>, mut term: Signal, mut int: Signal) {
        loop {
            let name = tokio::select! {
                _ = term.recv() => "SIGTERM",
                _ = int.recv() => "SIGINT",
            };
            match self.phase.load(Ordering::SeqCst) {
                STARTING => {
                    let when = match self.form {
                        Form::Serve | Form::Catalog => "before listening",
                        Form::Retire | Form::Evidence => "before startup completed",
                    };
                    logging::log(
                        Level::Info,
                        &format!("received {name} {when}; startup aborted"),
                        &[],
                    );
                    std::process::exit(self.abort_code().into());
                }
                SERVING => {
                    self.phase.store(STOPPING, Ordering::SeqCst);
                    logging::log(Level::Info, &format!("received {name}; shutting down"), &[]);
                    self.stop.notify_one();
                }
                _ => {
                    logging::log(
                        Level::Warn,
                        &format!("received {name} again while shutting down; exiting immediately"),
                        &[],
                    );
                    std::process::exit(EXIT_UNCLASSIFIED.into());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(form: Form, phase: u8) -> Lifecycle {
        Lifecycle {
            form,
            phase: AtomicU8::new(phase),
            stop: Notify::new(),
        }
    }

    #[test]
    fn abort_code_by_form() {
        assert_eq!(at(Form::Serve, STARTING).abort_code(), EXIT_OK);
        assert_eq!(at(Form::Catalog, STARTING).abort_code(), EXIT_OK);
        assert_eq!(at(Form::Retire, STARTING).abort_code(), EXIT_INCOMPLETE);
        assert_eq!(at(Form::Evidence, STARTING).abort_code(), EXIT_INCOMPLETE);
    }

    #[test]
    fn stop_requested_only_after_first_signal() {
        let l = at(Form::Retire, STARTING);
        l.serving();
        assert!(!l.stop_requested());
        l.phase.store(STOPPING, Ordering::SeqCst);
        assert!(l.stop_requested());
    }
}
