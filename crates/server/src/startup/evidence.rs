//! 证据运行方式（[`crate::form::Form::Evidence`]）：离线子命令 `amberlog evidence export` 与 `amberlog evidence verify` 在
//! 共用前段（[`super::launch`]）之后的部分——配置一步里子命令参数的校验与 `--output` 的核对（[`prepare`]），以及启动流程的
//! 其余各步与执行体（[`run`]；REQ-AMB-CFG-STARTUP-V7、REQ-AMB-STO-EVIDENCE-EXPORT、REQ-AMB-STO-EVIDENCE-VERIFY）。
//!
//! 只构造对象存储客户端与证据（`amberlog_storage::evidence`）：不建 `wal.dir`、不监听、不构造摄取与提交，
//! 所以只读与不监听由「这些元素不存在」直接保证。
//!
//! 启动流程：配置（含子命令参数）→ 构造对象存储客户端 → 暂停点 `offline.before_probe` → 对象存储读取探测（列举一次该数据集的 `manifest/`，
//! 不可访问以退出码 3 退出）→ 交给执行体（[`Lifecycle::serving`]）。
//!
//! 信号：启动流程完成之前收到的由 [`Lifecycle`] 以退出码 6 退出（那时尚未创建临时文件）。之后收到的第一次信号使执行体停下：
//! 导出删除临时文件，导出与查验都以退出码 6 退出、stdout 为空；导出改名成功之后、查验开始输出报告之后不再中止
//! （REQ-AMB-STO-EVIDENCE-EXPORT-SIGNAL、REQ-AMB-STO-EVIDENCE-VERIFY-SIGNAL）。再次收到时由 [`Lifecycle`] 立即退出。

use std::ffi::{OsStr, OsString};
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use amberlog_common::dataset::Dataset;
use amberlog_storage::evidence::export::{self, Failed, Request, Sink};
use amberlog_storage::evidence::tar::TarWriter;
use amberlog_storage::evidence::{package, read, verify};
use amberlog_storage::layout::HourBucket;
use amberlog_storage::store::{Location, Store};
use serde_json::Value;

use super::{
    EXIT_CONFIG, EXIT_FINDINGS, EXIT_INCOMPLETE, EXIT_OK, EXIT_STORAGE, EXIT_UNCLASSIFIED, Failure,
    fail, open_store,
};
use crate::cli::{ExportArgs, VerifyArgs};
use crate::config::{self, Config, ConfigError};
use crate::logging::{self, Level};
use crate::shutdown::Lifecycle;
use crate::version;

/// 证据子命令的参数（取值未校验）。
pub enum Args {
    Export(ExportArgs),
    Verify(VerifyArgs),
}

impl Args {
    /// `--config` 所指的路径。
    pub fn config(&self) -> Option<&Path> {
        match self {
            Args::Export(a) => a.config.as_deref(),
            Args::Verify(a) => a.config.as_deref(),
        }
    }
}

/// 已校验的证据子命令。
pub enum Job {
    Export(ExportPlan),
    Verify(VerifyPlan),
}

/// 配置一步中子命令参数的校验（在配置文件与环境变量之后、`starting` 之前）；错误逐条给出，以退出码 2 退出。
pub fn prepare(args: Args, cfg: &Config, cwd: &Path) -> Result<Job, Vec<ConfigError>> {
    match args {
        Args::Export(a) => {
            plan_export(&a, &cfg.location, &cfg.storage_prefix, cwd).map(Job::Export)
        }
        Args::Verify(a) => plan_verify(&a, cwd).map(Job::Verify),
    }
    .map_err(|errors| errors.into_iter().map(ConfigError::argument).collect())
}

/// 启动流程的其余各步与执行体；返回退出码。
pub async fn run(cfg: Config, job: Job, lifecycle: Arc<Lifecycle>) -> Result<u8, Failure> {
    let store = open_store(&cfg)?;
    amberlog_storage::fault::gate("offline.before_probe").await;
    let dataset = match &job {
        Job::Export(p) => &p.dataset,
        Job::Verify(p) => &p.dataset,
    };
    // 对象存储读取探测：列举一次该数据集的 `manifest/`。导出以同一结果判定数据集是否存在（DATASET-MISSING）；
    // 查验时数据集是否还存在由查验按 CURRENT-UNREADABLE 判
    let exists = read::dataset_exists(&store, dataset)
        .await
        .map_err(|e| fail(EXIT_STORAGE, e.to_string()))?;
    if let Job::Export(p) = &job
        && !exists
    {
        return Err(fail(
            EXIT_CONFIG,
            format!("dataset {} does not exist in the object store", p.dataset),
        ));
    }
    lifecycle.serving();
    match job {
        Job::Export(p) => run_export(&store, p, &lifecycle).await,
        Job::Verify(p) => run_verify(&store, p, &lifecycle).await,
    }
}

/// 执行体因停机信号停下：以退出码 6 退出，stdout 为空。
fn stopped(what: &str) -> Result<u8, Failure> {
    logging::log(
        Level::Info,
        &format!("the {what} was stopped by a signal"),
        &[("exit_code", Value::from(EXIT_INCOMPLETE))],
    );
    Ok(EXIT_INCOMPLETE)
}

/// 已校验的导出参数。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExportPlan {
    pub dataset: Dataset,
    pub range: Option<(i64, i64)>,
    /// 绝对路径。
    pub output: PathBuf,
}

/// `YYYY-MM-DDTHH:00:00Z` 的 UTC 整点（REQ-AMB-STO-EVIDENCE-EXPORT-RANGE-FORMAT），返回 UTC 微秒。
pub fn parse_hour(value: &OsStr) -> Option<i64> {
    let v = value.to_str()?.as_bytes();
    if v.len() != 20 || &v[13..] != b":00:00Z" || v[4] != b'-' || v[7] != b'-' || v[10] != b'T' {
        return None;
    }
    let digits = |r: std::ops::Range<usize>| -> Option<&str> {
        let s = std::str::from_utf8(&v[r]).ok()?;
        s.bytes().all(|b| b.is_ascii_digit()).then_some(s)
    };
    let label = format!(
        "{}{}{}{}",
        digits(0..4)?,
        digits(5..7)?,
        digits(8..10)?,
        digits(11..13)?
    );
    HourBucket::parse(&label).map(HourBucket::start_us)
}

/// 配置一步中的导出参数校验与 `--output` 的核对（ARGS 之外的 DATASET-ARG、RANGE-FORMAT、RANGE-PAIR、OUTPUT-IN-PREFIX、
/// OUTPUT-EXISTS）。错误逐条给出，以退出码 2 退出。
pub fn plan_export(
    args: &ExportArgs,
    location: &Location,
    storage_prefix: &str,
    cwd: &Path,
) -> Result<ExportPlan, Vec<String>> {
    let mut errors = Vec::new();
    let dataset = args.dataset.to_str().and_then(package::parse_dataset);
    if dataset.is_none() {
        errors.push(format!(
            "--dataset: expected default/<stream> with <stream> matching ^[a-z0-9][a-z0-9_]{{0,63}}$, got {:?}",
            args.dataset.to_string_lossy()
        ));
    }
    let hour = |name: &str, v: &Option<OsString>, errors: &mut Vec<String>| match v {
        None => Some(None),
        Some(v) => match parse_hour(v) {
            Some(t) => Some(Some(t)),
            None => {
                errors.push(format!(
                    "{name}: expected a whole UTC hour written as YYYY-MM-DDTHH:00:00Z, got {:?}",
                    v.to_string_lossy()
                ));
                None
            }
        },
    };
    let from = hour("--from", &args.from, &mut errors);
    let to = hour("--to", &args.to, &mut errors);
    let range = match (from, to) {
        (Some(None), Some(None)) => Some(None),
        (Some(Some(f)), Some(Some(t))) if f < t => Some(Some((f, t))),
        (Some(_), Some(_)) => {
            errors.push(
                "--from and --to must be given together, with --from earlier than --to".into(),
            );
            None
        }
        _ => None,
    };
    let output = cwd.join(&args.output);
    let shown = args.output.display();
    if std::fs::symlink_metadata(&output).is_ok() {
        errors.push(format!("--output {shown} already exists"));
    } else {
        match output.parent().map(std::fs::metadata) {
            Some(Ok(m)) if m.is_dir() => {
                if !writable(output.parent().expect("有上级目录")) {
                    errors.push(format!("--output {shown}: the directory is not writable"));
                }
            }
            _ => errors.push(format!(
                "--output {shown}: the directory does not exist or is not a directory"
            )),
        }
    }
    if let Location::File { dir } = location
        && in_prefix(&output, &dir.join(storage_prefix))
    {
        errors.push(format!(
            "--output {shown} is inside the object store prefix; the evidence package must not be written into the bucket"
        ));
    }
    match (dataset, range) {
        (Some(dataset), Some(range)) if errors.is_empty() => Ok(ExportPlan {
            dataset,
            range,
            output,
        }),
        _ => Err(errors),
    }
}

fn writable(dir: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt;
    let Ok(c) = std::ffi::CString::new(dir.as_os_str().as_bytes()) else {
        return false;
    };
    // SAFETY: `c` 是以 NUL 结尾的有效路径字符串，access 只读它。
    unsafe { libc::access(c.as_ptr(), libc::W_OK | libc::X_OK) == 0 }
}

/// `output` 是否落在 `root` 所对应的目录之内：`output` 所在目录与 `root` 各自解析符号链接之后比较
/// （REQ-AMB-STO-EVIDENCE-EXPORT-OUTPUT-IN-PREFIX）。`root` 不存在时按其最长的存在的上级解析。
fn in_prefix(output: &Path, root: &Path) -> bool {
    let resolve = |p: &Path| -> PathBuf {
        let mut rest = Vec::new();
        let mut cur = p.to_path_buf();
        loop {
            if let Ok(c) = cur.canonicalize() {
                return rest.iter().rev().fold(c, |acc: PathBuf, x| acc.join(x));
            }
            match (cur.parent(), cur.file_name()) {
                (Some(parent), Some(name)) => {
                    rest.push(name.to_owned());
                    cur = parent.to_path_buf();
                }
                _ => return p.to_path_buf(),
            }
        }
    };
    let Some(parent) = output.parent() else {
        return false;
    };
    let dir = resolve(parent);
    let file = dir.join(output.file_name().unwrap_or_default());
    file.starts_with(resolve(root))
}

/// 证据包的临时文件（`--output` 同目录，以 `.` 开头、含 `--output` 的文件名）上的 tar 写者。
struct TempPackage {
    tar: Option<TarWriter<BufWriter<File>>>,
    path: PathBuf,
}

impl TempPackage {
    /// 以「不存在才创建」新建临时文件，名字冲突时换名；不删除、不覆盖目录中已有的任何文件（OUTPUT-CREATE）。
    fn create(output: &Path) -> io::Result<TempPackage> {
        let dir = output.parent().unwrap_or(Path::new("."));
        let name = output.file_name().unwrap_or_default().to_string_lossy();
        loop {
            let path = dir.join(format!(
                ".{name}.{}.tmp",
                amberlog_storage::layout::new_object_id()
            ));
            match File::options().write(true).create_new(true).open(&path) {
                Ok(f) => {
                    return Ok(TempPackage {
                        tar: Some(TarWriter::new(BufWriter::new(f))),
                        path,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(e) => return Err(e),
            }
        }
    }

    /// 删除临时文件（能删时）。
    fn discard(&mut self) {
        self.tar = None;
        let _ = std::fs::remove_file(&self.path);
    }

    /// 写完结尾块并 fsync。
    fn finish(&mut self) -> io::Result<()> {
        let tar = self.tar.take().expect("只结束一次");
        let file = tar
            .finish()?
            .into_inner()
            .map_err(io::IntoInnerError::into_error)?;
        file.sync_all()
    }
}

impl Sink for TempPackage {
    fn member(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        self.tar.as_mut().expect("未结束").member(name, data)
    }

    fn flush(&mut self) -> io::Result<()> {
        // 写进文件（不必落盘）：暂停点上临时文件里已有内容
        Ok(()).and_then(|()| match self.tar.as_mut() {
            Some(t) => t.flush(),
            None => Ok(()),
        })
    }
}

/// 改名的结果。
enum Renamed {
    Done,
    /// 目标已存在。
    Exists,
}

/// 以不覆盖的方式把 `from` 改名为 `to`：`renameat2(RENAME_NOREPLACE)`，文件系统不支持时退为「`link` 后删除原名」。
fn rename_noreplace(from: &Path, to: &Path) -> io::Result<Renamed> {
    use std::os::unix::ffi::OsStrExt;
    let c = |p: &Path| std::ffi::CString::new(p.as_os_str().as_bytes()).map_err(io::Error::other);
    let (f, t) = (c(from)?, c(to)?);
    // SAFETY: 两个参数都是以 NUL 结尾的有效路径字符串；AT_FDCWD 表示相对当前目录解析（两者都是绝对路径）。
    let r = unsafe {
        libc::renameat2(
            libc::AT_FDCWD,
            f.as_ptr(),
            libc::AT_FDCWD,
            t.as_ptr(),
            libc::RENAME_NOREPLACE,
        )
    };
    if r == 0 {
        return Ok(Renamed::Done);
    }
    let e = io::Error::last_os_error();
    match e.raw_os_error() {
        Some(libc::EEXIST) => Ok(Renamed::Exists),
        Some(libc::EINVAL | libc::ENOSYS | libc::EOPNOTSUPP) => {
            match std::fs::hard_link(from, to) {
                Ok(()) => {
                    std::fs::remove_file(from)?;
                    Ok(Renamed::Done)
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => Ok(Renamed::Exists),
                Err(e) => Err(e),
            }
        }
        _ => Err(e),
    }
}

/// 导出的执行体：写临时文件，改名为 `--output`，向 stdout 输出回执。
async fn run_export(store: &Store, plan: ExportPlan, lifecycle: &Lifecycle) -> Result<u8, Failure> {
    let mut package = TempPackage::create(&plan.output).map_err(|e| {
        fail(
            EXIT_UNCLASSIFIED,
            format!(
                "cannot create a temporary file next to {}: {e}",
                plan.output.display()
            ),
        )
    })?;
    let request = Request {
        dataset: plan.dataset.clone(),
        range: plan.range,
        amberlog_version: version::SEMVER.to_owned(),
    };
    let outcome = tokio::select! {
        r = export::export(store, &request, &mut package) => Some(r),
        () = lifecycle.stopping() => None,
    };
    let Some(result) = outcome else {
        package.discard();
        return stopped("export");
    };
    let result = result.and_then(|e| package.finish().map(|()| e).map_err(Failed::Write));
    let exported = match result {
        Ok(e) => e,
        Err(f) => {
            let (code, message) = describe(&f, &package.path);
            package.discard();
            return Err(fail(code, message));
        }
    };
    // 改名成功之前收到的信号仍要中止；改名成功之后的不再中止
    if lifecycle.stop_requested() {
        package.discard();
        return stopped("export");
    }
    match rename_noreplace(&package.path, &plan.output) {
        Ok(Renamed::Done) => {}
        Ok(Renamed::Exists) => {
            package.discard();
            return Err(fail(
                EXIT_CONFIG,
                format!("--output {} already exists", plan.output.display()),
            ));
        }
        Err(e) => {
            package.discard();
            return Err(fail(
                EXIT_UNCLASSIFIED,
                format!(
                    "cannot rename the evidence package to {}: {e}",
                    plan.output.display()
                ),
            ));
        }
    }
    let mut out = io::stdout().lock();
    out.write_all(&exported.receipt)
        .and_then(|()| out.write_all(b"\n"))
        .and_then(|()| out.flush())
        .map_err(|e| {
            fail(
                EXIT_UNCLASSIFIED,
                format!("cannot write the receipt to stdout: {e}"),
            )
        })?;
    if exported.problems_found {
        logging::log(
            Level::Warn,
            "the export found problems; see check.json in the evidence package",
            &[("exit_code", Value::from(EXIT_FINDINGS))],
        );
        Ok(EXIT_FINDINGS)
    } else {
        Ok(EXIT_OK)
    }
}

fn describe(f: &Failed, temp: &Path) -> (u8, String) {
    match f {
        Failed::Unavailable(m) => (EXIT_INCOMPLETE, m.clone()),
        Failed::HintUnusable(p) => (
            EXIT_INCOMPLETE,
            format!(
                "the snapshot hint {p} is missing or unparseable and snapshot 1 does not exist, but snapshot files are listed under manifest/"
            ),
        ),
        Failed::FormatUnsupported(p, v) => (
            EXIT_INCOMPLETE,
            format!(
                "the current snapshot {p} has format_version {v}; only format_version 2 can be exported"
            ),
        ),
        Failed::HeadUnreadable(p, why) => (
            EXIT_INCOMPLETE,
            format!("the chain head {p} cannot be read: {why}"),
        ),
        Failed::Advanced(p) => (
            EXIT_INCOMPLETE,
            format!(
                "导出期间链已前进，所钉住的对象已不存在，请重跑 (the chain advanced during the export and the pinned object no longer exists; please rerun): {p}"
            ),
        ),
        Failed::Outside(uri) => (
            EXIT_INCOMPLETE,
            format!("cold metadata references {uri} outside the table location"),
        ),
        Failed::Write(e) => (
            EXIT_UNCLASSIFIED,
            format!("cannot write the evidence package {}: {e}", temp.display()),
        ),
    }
}

/// 已校验的查验参数。
pub struct VerifyPlan {
    pub dataset: Dataset,
    pub evidence: package::Unpacked,
    pub previous: Option<package::Previous>,
    pub retention: Option<i64>,
}

/// 配置一步中的查验参数校验（FILE-ARGS、EVIDENCE-SHAPE、PREVIOUS-KIND、DATASET-MATCH、RETENTION-ARG），以退出码 2 退出。
pub fn plan_verify(args: &VerifyArgs, cwd: &Path) -> Result<VerifyPlan, Vec<String>> {
    let mut errors = Vec::new();
    let read = |name: &str, p: &Path, errors: &mut Vec<String>| match std::fs::read(cwd.join(p)) {
        Ok(b) => Some(b),
        Err(e) => {
            errors.push(format!("{name} {}: cannot read the file: {e}", p.display()));
            None
        }
    };
    let evidence = read("--evidence", &args.evidence, &mut errors).and_then(|b| {
        package::unpack(&b)
            .map_err(|why| {
                errors.push(format!(
                    "--evidence {}: not an evidence package: {why}",
                    args.evidence.display()
                ))
            })
            .ok()
    });
    let previous = match &args.previous {
        None => Some(None),
        Some(p) => read("--previous", p, &mut errors).and_then(|b| {
            package::previous(&b)
                .map(Some)
                .map_err(|why| errors.push(format!("--previous {}: {why}", p.display())))
                .ok()
        }),
    };
    if let (Some(e), Some(Some(p))) = (&evidence, &previous)
        && p.dataset() != e.dataset()
    {
        errors.push(format!(
            "--previous belongs to dataset {} but --evidence belongs to dataset {}",
            p.dataset(),
            e.dataset()
        ));
    }
    let retention = match &args.retention {
        None => Some(None),
        Some(r) => match r.to_str().and_then(config::parse_duration) {
            Some(d) if !d.is_zero() => Some(Some(i64::try_from(d.as_micros()).unwrap_or(i64::MAX))),
            _ => {
                errors.push(format!(
                    "--retention: expected a non-zero duration such as 72h, got {:?}",
                    r.to_string_lossy()
                ));
                None
            }
        },
    };
    match (evidence, previous, retention) {
        (Some(evidence), Some(previous), Some(retention)) if errors.is_empty() => Ok(VerifyPlan {
            dataset: package::parse_dataset(evidence.dataset()).expect("形状已核对"),
            evidence,
            previous,
            retention,
        }),
        _ => Err(errors),
    }
}

/// 查验的执行体：核对，向 stdout 输出一行报告；退出码按查验结果（REQ-AMB-STO-EVIDENCE-VERIFY-EXIT）。
async fn run_verify(store: &Store, plan: VerifyPlan, lifecycle: &Lifecycle) -> Result<u8, Failure> {
    let request = verify::Request {
        dataset: plan.dataset,
        evidence: plan.evidence,
        previous: plan.previous,
        retention: plan.retention,
    };
    let outcome = tokio::select! {
        r = verify::verify(store, &request) => Some(r),
        () = lifecycle.stopping() => None,
    };
    // 开始输出报告之前收到的信号仍要中止
    let Some(report) = outcome.filter(|_| !lifecycle.stop_requested()) else {
        return stopped("verification");
    };
    let code = match report.result() {
        "pass" => EXIT_OK,
        "inconsistent" => EXIT_FINDINGS,
        _ => EXIT_INCOMPLETE,
    };
    let mut line = serde_json::to_vec(&report.to_json()).expect("可序列化");
    line.push(b'\n');
    let mut out = io::stdout().lock();
    out.write_all(&line)
        .and_then(|()| out.flush())
        .map_err(|e| {
            fail(
                EXIT_UNCLASSIFIED,
                format!("cannot write the report to stdout: {e}"),
            )
        })?;
    Ok(code)
}
