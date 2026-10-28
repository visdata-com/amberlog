//! 生命周期编排（Lifecycle）的启动部分：解析命令行，按子命令选定进程形态（[`Form`]），
//! 装载配置，再交给该形态的启动流程。各形态的启动流程分在子模块里，只构造本形态用得到的元素；写入实例在 [`serve`]，
//! 按「配置 → `wal.dir` → 对象存储 → WAL 重放 → 监听」的顺序校验，首个失败项决定退出码（REQ-AMB-CFG-STARTUP-V2 B 组，ADR-0016）。
//!
//! 退出码：0 正常、1 未分类、2 配置与命令行用法、3 `wal.dir` 与对象存储、4 监听；离线子命令另有 5、6（[`EXIT_FINDINGS`]、[`EXIT_INCOMPLETE`]）。
//!
//! 对象存储一步含已存在数据集的基线读取（REQ-AMB-DM-NAMESPACE、REQ-AMB-STO-MANIFEST-COMMIT），其后是 WAL 重放与解释
//! （REQ-AMB-LIFE-DURABILITY）。`ready` 事件由 [`crate::telemetry::Health::evaluate`] 在监听建立后首次就绪时输出。

mod catalog;
mod encryption;
mod evidence;
mod nofile;
mod retire;
mod serve;

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use amberlog_storage::store::Store;
use serde_json::Value;

use crate::cli::{self, Command};
use crate::config::{self, Auth, Config, ConfigError};
use crate::form::Form;
use crate::gateway::tls::Inbound;
use crate::logging::{self, Level};
use crate::shutdown::{EarlyMask, Lifecycle};
use crate::telemetry;
use crate::version;

pub const EXIT_OK: u8 = 0;
pub const EXIT_UNCLASSIFIED: u8 = 1;
pub const EXIT_CONFIG: u8 = 2;
pub const EXIT_STORAGE: u8 = 3;
pub const EXIT_LISTEN: u8 = 4;
/// 离线子命令：做完了但发现问题（REQ-AMB-CFG-STARTUP-V7，含义按定义该子命令的块）。
pub const EXIT_FINDINGS: u8 = 5;
/// 离线子命令：没做完（含被信号中止，REQ-AMB-CFG-STARTUP-V7、REQ-AMB-LIFE-DURABILITY-V4）。
pub const EXIT_INCOMPLETE: u8 = 6;

/// 启动失败：退出码与写进日志的原因。
struct Failure {
    code: u8,
    message: String,
}

fn fail(code: u8, message: impl Into<String>) -> Failure {
    Failure {
        code,
        message: message.into(),
    }
}

/// 可执行文件入口。`args` 不含程序名。
pub fn main(args: impl IntoIterator<Item = OsString>) -> u8 {
    amberlog_storage::memory::fix_trim_threshold();
    match cli::parse(args) {
        Err(cli::UsageError(why)) => {
            eprintln!("amberlog: {why}\n{}", cli::USAGE);
            EXIT_CONFIG
        }
        Ok(Command::Version) => {
            println!("{}", version::line());
            EXIT_OK
        }
        Ok(Command::Serve { config }) => launch(Launch::Serve { config }),
        Ok(Command::Catalog { config }) => launch(Launch::Catalog { config }),
        Ok(Command::Retire { config }) => launch(Launch::Retire { config }),
        Ok(Command::EvidenceExport(args)) => launch(Launch::Evidence(evidence::Args::Export(args))),
        Ok(Command::EvidenceVerify(args)) => launch(Launch::Evidence(evidence::Args::Verify(args))),
    }
}

/// 子命令选定的形态及其在配置之外的参数（由 [`main`] 按命令行构造）。
pub enum Launch {
    Serve {
        config: Option<PathBuf>,
    },
    Catalog {
        config: Option<PathBuf>,
    },
    Retire {
        config: Option<PathBuf>,
    },
    /// 证据子命令：`--config` 在参数里，其余参数的取值在配置一步校验。
    Evidence(evidence::Args),
}

impl Launch {
    fn form(&self) -> Form {
        match self {
            Launch::Serve { .. } => Form::Serve,
            Launch::Catalog { .. } => Form::Catalog,
            Launch::Retire { .. } => Form::Retire,
            Launch::Evidence(_) => Form::Evidence,
        }
    }

    fn config(&self) -> Option<&Path> {
        match self {
            Launch::Serve { config } | Launch::Catalog { config } | Launch::Retire { config } => {
                config.as_deref()
            }
            Launch::Evidence(args) => args.config(),
        }
    }
}

/// 配置一步之后各形态带进启动流程的东西（[`prepare`] 构造）：监听形态带入站 TLS 设置（配了证书时），证据带校验过的子命令参数。
enum Prepared {
    Serve { inbound: Option<Inbound> },
    Catalog { inbound: Option<Inbound> },
    Retire,
    Evidence(evidence::Job),
}

/// 各形态共用的前段：信号处置、运行时、配置装载（必填项按形态）与 `starting` 日志，然后交给 [`run`]。
fn launch(launch: Launch) -> u8 {
    let form = launch.form();
    // 停机信号从进程最开始就有处置（REQ-AMB-LIFE-DURABILITY-V2）：先屏蔽，建好运行时、装上处置后再解除，
    // 这期间到达的信号在解除时交付，按「开始监听之前」以退出码 0 中止启动
    let mask = EarlyMask::block();
    let runtime = match runtime() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("amberlog: cannot start the async runtime: {e}");
            return EXIT_UNCLASSIFIED;
        }
    };
    let lifecycle = match runtime.block_on(async { Lifecycle::install(form) }) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("amberlog: cannot install signal handlers: {e}");
            return EXIT_UNCLASSIFIED;
        }
    };
    mask.unblock();
    amberlog_storage::fault::pause("装载配置之前");

    let cwd = match std::env::current_dir() {
        Ok(d) => d,
        Err(e) => {
            eprintln!("amberlog: cannot determine the working directory: {e}");
            return EXIT_UNCLASSIFIED;
        }
    };
    let loaded = config::load(form, launch.config(), std::env::vars_os(), &cwd)
        .and_then(|mut cfg| prepare(launch, &mut cfg, &cwd).map(|prepared| (cfg, prepared)));
    let (cfg, prepared) = match loaded {
        Ok(loaded) => loaded,
        Err(errors) => {
            for e in errors {
                eprintln!("amberlog: configuration error: {e}");
            }
            return EXIT_CONFIG;
        }
    };

    logging::init(cfg.log_format, cfg.log_level);
    logging::event(
        Level::Info,
        "starting",
        "starting amberlog",
        &[("version", Value::from(version::SEMVER))],
    );
    nofile::raise_and_report();
    if cfg.auth == Some(Auth::Disabled) {
        logging::event(
            Level::Warn,
            "auth_disabled",
            "authentication is disabled: every endpoint accepts requests without credentials",
            &[],
        );
    }

    encryption::announce(form, &cfg);

    let result = runtime.block_on(run(cfg, prepared, lifecycle));
    // 停机时等连接以 `server.shutdown_timeout` 为限（[`Http::close`]），届满时仍未关闭的连接不再等
    runtime.shutdown_background();
    match result {
        Ok(code) => code,
        Err(f) => {
            logging::log(
                Level::Error,
                &f.message,
                &[("exit_code", Value::from(f.code))],
            );
            f.code
        }
    }
}

/// 多线程运行时。工作线程数显式取可用并行度（取不到时为 1），不受 tokio 自己读的环境变量 `TOKIO_WORKER_THREADS` 影响：
/// 配置用环境变量只认 `AMBERLOG_` 前缀（REQ-AMB-CFG-STARTUP-V4），该变量取值非法时 tokio 还会 panic。
fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let workers = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(workers)
        .enable_all()
        .build()
}

/// 配置一步里按形态做的校验，在 `starting` 之前；失败与其他配置错误一样以退出码 2 退出。加密的校验（证书、明文规则、
/// CA 文件）对全部形态都做（REQ-AMB-ARCH-ENCRYPTION-CONFIG-STEP）。证据子命令的参数取值同属这一步，相对路径按 `cwd` 解析
/// （REQ-AMB-STO-EVIDENCE-EXPORT-ARGS、REQ-AMB-STO-EVIDENCE-VERIFY-ARGS）。
fn prepare(launch: Launch, cfg: &mut Config, cwd: &Path) -> Result<Prepared, Vec<ConfigError>> {
    let inbound = encryption::prepare(launch.form(), cfg)?;
    Ok(match launch {
        Launch::Serve { .. } => Prepared::Serve { inbound },
        Launch::Catalog { .. } => Prepared::Catalog { inbound },
        Launch::Retire { .. } => Prepared::Retire,
        Launch::Evidence(args) => Prepared::Evidence(evidence::prepare(args, cfg, cwd)?),
    })
}

/// 按形态交给各自的启动流程，返回进程退出码。
async fn run(cfg: Config, prepared: Prepared, lifecycle: Arc<Lifecycle>) -> Result<u8, Failure> {
    match prepared {
        Prepared::Serve { inbound } => serve::run(cfg, inbound, lifecycle).await.map(|()| EXIT_OK),
        Prepared::Catalog { inbound } => catalog::run(cfg, inbound, lifecycle)
            .await
            .map(|()| EXIT_OK),
        Prepared::Retire => retire::run(cfg, lifecycle).await,
        Prepared::Evidence(job) => evidence::run(cfg, job, lifecycle).await,
    }
}

/// 受理请求的形态（`serve`、`catalog`）的 HTTP 服务。
///
/// 停机过程中监听照旧：新到的 `/api/` 与目录前缀下的请求照样得 503，`/readyz` 照样报 `shutting_down`。在途计数在响应得出时
/// 就结束，这时应答还没写进套接字；停机的最后一步因此要经 [`Http::close`] 等连接把应答写完，否则进程退出时运行时丢掉
/// 连接任务，客户端收不到完整的应答（REQ-AMB-LIFE-DURABILITY-V4「让在途请求在 `server.shutdown_timeout` 内完成」）。
struct Http {
    close: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<std::io::Result<()>>,
}

impl Http {
    fn spawn<L>(listener: L, app: axum::Router) -> Http
    where
        L: axum::serve::Listener,
        L::Addr: std::fmt::Debug,
    {
        let (close, closing) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = closing.await;
                })
                .await
        });
        Http { close, task }
    }

    /// 等到服务意外结束（未经 [`Http::close`] 不会正常结束）。
    async fn failed(&mut self) -> Failure {
        fail(
            EXIT_UNCLASSIFIED,
            match (&mut self.task).await {
                Ok(Ok(())) => "HTTP server stopped unexpectedly".to_owned(),
                Ok(Err(e)) => format!("HTTP server failed: {e}"),
                Err(e) => format!("HTTP server failed: {e}"),
            },
        )
    }

    /// 停机的最后一步：不再接受新连接，每个连接写完手上的应答后关闭（空闲连接立即关闭），等全部连接关闭，至多到
    /// `deadline`（停机开始时刻加 `server.shutdown_timeout`；已过则不等）。届满时仍未关闭的连接随进程退出被关闭
    /// （REQ-AMB-LIFE-DURABILITY-V4「届满时仍在途的请求不再等待」）。
    async fn close(self, deadline: tokio::time::Instant) {
        let _ = self.close.send(());
        let _ = tokio::time::timeout_at(deadline, self.task).await;
    }
}

/// 按配置构造对象存储客户端（全进程共用一个）；配置不能构成客户端归退出码 3。各形态的探测不在这里，由各自的启动流程做。
fn open_store(cfg: &Config) -> Result<Store, Failure> {
    Store::open(&cfg.location, &cfg.s3, &cfg.storage_prefix)
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))
}

/// 写入实例在对象存储校验之后、读取已存在数据集与 WAL 重放之前读退役记录：存在即已退役（内容能否解析不论），以退出码 2
/// 退出并在 stderr 指出该对象的键，不监听、不重放（REQ-AMB-CFG-STARTUP-V7、REQ-AMB-LIFE-RETIREMENT-SERVE-REFUSE）；读取遇
/// 对象存储不可访问归退出码 3。
async fn refuse_if_retired(cfg: &Config, store: &Store) -> Result<(), Failure> {
    let retired = store
        .exists(&store.system_path(retire::RECORD))
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    if retired {
        return Err(fail(
            EXIT_CONFIG,
            format!(
                "the storage prefix has been retired: {} exists (delete it only to revive the prefix on purpose)",
                retire::record_key(cfg)
            ),
        ));
    }
    Ok(())
}

/// `wal.dir` 不存在则创建；无法创建或不可写归退出码 3。
fn check_wal_dir(dir: &Path) -> Result<(), Failure> {
    let shown = dir.display();
    std::fs::create_dir_all(dir).map_err(|e| {
        fail(
            EXIT_STORAGE,
            format!("wal.dir {shown} cannot be created: {e}"),
        )
    })?;
    telemetry::check_writable(dir).map_err(|e| {
        fail(
            EXIT_STORAGE,
            format!("wal.dir {shown} is not writable: {e}"),
        )
    })
}
