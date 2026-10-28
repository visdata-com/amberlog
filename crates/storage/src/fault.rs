//! 测试专用的崩溃暂停点（`fault-injection` feature）。
//!
//! 约定：进程读环境变量
//! `TESTBED_FAULT_PAUSE=<步骤名>`，走到同名步骤前写出标记文件 `TESTBED_FAULT_MARKER` 并永久阻塞，
//! 测试见到标记后发 SIGKILL；`TESTBED_FAULT_PAUSE_NTH=<k>` 让它停在第 k 次走到的那一处。
//! 不开该 feature 的构建里 [`pause`] 是空函数，调用点无需加 `cfg`。
//! 步骤名由用到它的测试定下，调用点就地写字面量。
//!
//! 可放行的暂停点 [`gate`]：`TESTBED_FAULT_GATE=<步骤名>` 与 `TESTBED_FAULT_GATE_FILE=<控制文件>` 选中一个步骤；要同时停住
//! 几个步骤时另设 `TESTBED_FAULT_GATES=<步骤名>=<控制文件>[;<步骤名>=<控制文件>…]`，各步骤各用自己的控制文件。
//! 两者都选中同一步骤时以 `TESTBED_FAULT_GATE` 为准。
//!
//! 对象存储请求的注入（某前缀下的写入回 403、忽略 `If-None-Match`、带该头的写入回 501 等）在 [`store`]。
//! 只记录对象存储请求（`TESTBED_FAULT_STORE_LOG`）也在那里。

#[cfg(feature = "fault-injection")]
pub mod store;

#[cfg(all(test, feature = "fault-injection"))]
mod tests;

/// 若本进程被要求停在 `step`，写出标记文件后阻塞当前线程直到被杀；否则立即返回。
///
/// 同一步骤在一个进程里会走到多次时（如冷层提交的每个写入请求之后），可另设 `TESTBED_FAULT_PAUSE_NTH=<k>`（k ≥ 1，
/// 缺省为 1）：本进程第 k 次走到该步骤才停，前 k − 1 次照常通过。测试逐次加大 k 并重启，即可依次停在每一处。
#[cfg(feature = "fault-injection")]
pub fn pause(step: &str) {
    use std::sync::atomic::{AtomicU64, Ordering};
    static HITS: AtomicU64 = AtomicU64::new(0);

    if std::env::var("TESTBED_FAULT_PAUSE").as_deref() != Ok(step) {
        return;
    }
    let nth = std::env::var("TESTBED_FAULT_PAUSE_NTH")
        .ok()
        .and_then(|k| k.parse::<u64>().ok())
        .unwrap_or(1);
    // 一个进程只选中一个步骤，计数只计该步骤
    if HITS.fetch_add(1, Ordering::SeqCst) + 1 != nth {
        return;
    }
    if let Ok(marker) = std::env::var("TESTBED_FAULT_MARKER") {
        let _ = std::fs::write(marker, step);
    }
    loop {
        std::thread::park();
    }
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn pause(_step: &str) {}

/// 可放行的暂停点：让一次提交确定地停在 `step`，测试在它停住期间观察并发行为，再放它继续（不杀进程）。
///
/// 环境变量 `TESTBED_FAULT_GATE=<步骤名>` 选中步骤，`TESTBED_FAULT_GATE_FILE` 给出控制文件的路径。控制文件的内容：
///
/// - `hold`：第一个走到该步骤的调用把它改写为 `held` 并等到内容变为 `go` 才返回，其后的调用不再停。
/// - `hold-all`：每个走到该步骤的调用都停住、不改写控制文件，直到内容变为 `go` 才一并返回（要同时停住几个查询时用，
///   如 REQ-AMB-QRY-RESOURCE-LIMITS 的测试）。此刻停住的调用数写在控制文件旁的 `<控制文件>.held`（十进制数，
///   整个文件一次换上）：调用停住时加一，返回或被丢弃（所在任务被取消）时减一；测试以它数出停住了几个。
/// - `hold-each`：逐次停住——每个走到该步骤的调用依次停住：把内容改写为 `held`，等到内容变为 `go`，再把内容改回
///   `hold-each` 后返回，下一个调用照停（同时走到的几个调用排队，一次只停一个）。测试见到 `held` 时观察，写 `go` 放行一个，
///   等内容不再是 `go`（已改回 `hold-each` 或下一个已停住）再写下一次；要结束逐次停住，在没有调用停住时写入别的内容
///   （REQ-AMB-ING-HOT-BACKPRESSURE-V5 的测试用：每一次写入提交都停住，测试不必与下一次提交赛跑）。
///
/// 要同时停住几个步骤（如 REQ-AMB-QRY-CROSS-TIER 的测试让入湖与一次查询各停一处）时，另以
/// `TESTBED_FAULT_GATES=<步骤名>=<控制文件>[;<步骤名>=<控制文件>…]` 逐个给出，每个步骤各用自己的控制文件，用法同上。
/// 停住的是异步任务、不占线程，调用方持有的锁照常持有。不开 `fault-injection` 的构建里是空函数，调用点无需加 `cfg`。
/// REQ-AMB-STO-MANIFEST-COMMIT 与 REQ-AMB-STO-HOT-FSCK 的测试用到它。
#[cfg(feature = "fault-injection")]
pub async fn gate(step: &str) {
    if let Some(control) = gate_file(step) {
        gate_at(std::path::Path::new(&control)).await;
    }
}

/// 按控制文件 `control` 的内容停住（见 [`gate`]）。
#[cfg(feature = "fault-injection")]
async fn gate_at(control: &std::path::Path) {
    // 进入过逐次停住的控制文件，此后的调用都先排队再读内容：停住期间内容为 `held`、放行与改回之间为 `go`，
    // 不排队就读会把这两种内容当作不停
    if let Some(queue) = each_queue(control, false) {
        return hold_each(control, queue).await;
    }
    match std::fs::read_to_string(control) {
        Ok(s) if s.trim() == "hold" => {
            if std::fs::write(control, "held").is_err() {
                return;
            }
            wait_for_go(control).await;
        }
        Ok(s) if s.trim() == "hold-all" => {
            let _held = HeldCount::enter(control);
            wait_for_go(control).await;
        }
        Ok(s) if s.trim() == "hold-each" => {
            if let Some(queue) = each_queue(control, true) {
                hold_each(control, queue).await;
            }
        }
        _ => {}
    }
}

#[cfg(feature = "fault-injection")]
async fn wait_for_go(control: &std::path::Path) {
    while !std::fs::read_to_string(control).is_ok_and(|s| s.trim() == "go") {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

/// 逐次停住的控制文件各自的排队锁；`register` 为真时没有就登记一把。
#[cfg(feature = "fault-injection")]
fn each_queue(
    control: &std::path::Path,
    register: bool,
) -> Option<std::sync::Arc<tokio::sync::Mutex<()>>> {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex, OnceLock};
    type Queues = Mutex<HashMap<std::path::PathBuf, Arc<tokio::sync::Mutex<()>>>>;
    static QUEUES: OnceLock<Queues> = OnceLock::new();

    let mut queues = QUEUES
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(|e| e.into_inner());
    if register {
        return Some(Arc::clone(queues.entry(control.to_owned()).or_default()));
    }
    queues.get(control).map(Arc::clone)
}

/// `hold-each`：同一控制文件的调用经同一把异步锁排队，取得锁后内容为 `hold-each` 才停；放行后先改回 `hold-each` 再交出锁。
#[cfg(feature = "fault-injection")]
async fn hold_each(control: &std::path::Path, queue: std::sync::Arc<tokio::sync::Mutex<()>>) {
    let _turn = queue.lock().await;
    if !std::fs::read_to_string(control).is_ok_and(|s| s.trim() == "hold-each") {
        return;
    }
    if std::fs::write(control, "held").is_err() {
        return;
    }
    wait_for_go(control).await;
    let _ = std::fs::write(control, "hold-each");
}

/// `hold-all` 下一个停住的调用：存续期间计入 `<控制文件>.held`，丢弃时（返回或所在任务被取消）减去。
#[cfg(feature = "fault-injection")]
struct HeldCount {
    control: std::path::PathBuf,
}

#[cfg(feature = "fault-injection")]
impl HeldCount {
    fn counts() -> &'static std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>> {
        static COUNTS: std::sync::OnceLock<
            std::sync::Mutex<std::collections::HashMap<std::path::PathBuf, u64>>,
        > = std::sync::OnceLock::new();
        COUNTS.get_or_init(Default::default)
    }

    fn enter(control: &std::path::Path) -> HeldCount {
        HeldCount::adjust(control, |n| n + 1);
        HeldCount {
            control: control.to_owned(),
        }
    }

    /// 持锁改计数并换上计数文件，使文件里的数与改动的先后一致。
    fn adjust(control: &std::path::Path, f: impl FnOnce(u64) -> u64) {
        let mut counts = HeldCount::counts()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let n = counts.entry(control.to_owned()).or_default();
        *n = f(*n);
        let mut held = control.as_os_str().to_owned();
        held.push(".held");
        let mut tmp = held.clone();
        tmp.push(".tmp");
        // 先写临时文件再改名，测试读到的总是完整的数
        if std::fs::write(&tmp, n.to_string()).is_ok() {
            let _ = std::fs::rename(&tmp, &held);
        }
    }
}

#[cfg(feature = "fault-injection")]
impl Drop for HeldCount {
    fn drop(&mut self) {
        HeldCount::adjust(&self.control, |n| n.saturating_sub(1));
    }
}

/// 步骤 `step` 的闸门控制文件：`TESTBED_FAULT_GATE` 选中它时为 `TESTBED_FAULT_GATE_FILE`，否则在 `TESTBED_FAULT_GATES` 中找。
#[cfg(feature = "fault-injection")]
fn gate_file(step: &str) -> Option<std::ffi::OsString> {
    if std::env::var("TESTBED_FAULT_GATE").as_deref() == Ok(step) {
        return std::env::var_os("TESTBED_FAULT_GATE_FILE");
    }
    let gates = std::env::var("TESTBED_FAULT_GATES").ok()?;
    gates.split(';').find_map(|pair| {
        let (name, file) = pair.split_once('=')?;
        (name.trim() == step).then(|| file.trim().into())
    })
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub async fn gate(_step: &str) {}

/// 只失败一次的注入点（REQ-AMB-STO-LAKE-ENTRY-V2 的测试用）：环境变量 `TESTBED_FAULT_FAIL=<步骤名>` 选中步骤时，
/// 本进程第一次走到该步骤返回 `true`（调用方按该步骤的失败处置），此后都返回 `false`。不开 `fault-injection` 的构建里
/// 恒为 `false`。
#[cfg(feature = "fault-injection")]
pub fn fail_once(step: &str) -> bool {
    use std::sync::atomic::{AtomicBool, Ordering};
    static FIRED: AtomicBool = AtomicBool::new(false);

    std::env::var("TESTBED_FAULT_FAIL").as_deref() == Ok(step)
        && !FIRED.swap(true, Ordering::SeqCst)
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn fail_once(_step: &str) -> bool {
    false
}

/// WAL 文件写入调用的注入点（REQ-AMB-ING-ACK-RESPONSE、REQ-AMB-LIFE-DURABILITY 的测试用）。
///
/// 环境变量 `TESTBED_FAULT_WAL` 给出一个控制文件的路径：该文件存在、且其内容（以空白分隔的操作名）含 `op` 时，
/// 这次调用以 I/O 错误失败。`op` 为 `write`（写入与新建段）、`fsync`、`truncate`（失败后回截）或 `delete`（删除段）。
/// 测试在运行中创建或删除控制文件即可施加或撤除注入，不必重启进程，也不靠改 `wal.dir` 的权限。
#[cfg(feature = "fault-injection")]
pub fn wal_io(op: &str) -> std::io::Result<()> {
    let Some(control) = std::env::var_os("TESTBED_FAULT_WAL") else {
        return Ok(());
    };
    match std::fs::read_to_string(control) {
        Ok(ops) if ops.split_whitespace().any(|o| o == op) => {
            Err(std::io::Error::other(format!("injected WAL {op} failure")))
        }
        _ => Ok(()),
    }
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn wal_io(_op: &str) -> std::io::Result<()> {
    Ok(())
}

/// 摄取批次在登记目标列之后、追加 WAL 之前的挂起点（REQ-AMB-DM-SCHEMA-DRIFT 的测试用）。
///
/// 环境变量 `TESTBED_FAULT_HOLD` 给出一个控制文件的路径：其内容为 `hold` 时，第一个到达挂起点的批次把它改写为 `held`
/// 并返回 `true`（由调用方放开该数据集的判定锁后以 [`ingest_hold_released`] 等待），其后的批次不再挂起；
/// 测试把内容改写为 `go` 即放行。不开 `fault-injection` 的构建里恒为 `false`。
#[cfg(feature = "fault-injection")]
pub fn claim_ingest_hold() -> bool {
    let Some(control) = std::env::var_os("TESTBED_FAULT_HOLD") else {
        return false;
    };
    match std::fs::read_to_string(&control) {
        Ok(s) if s.trim() == "hold" => std::fs::write(&control, "held").is_ok(),
        _ => false,
    }
}

#[cfg(not(feature = "fault-injection"))]
#[inline(always)]
pub fn claim_ingest_hold() -> bool {
    false
}

/// 等到 `TESTBED_FAULT_HOLD` 控制文件的内容变为 `go`（见 [`claim_ingest_hold`]）。
#[cfg(feature = "fault-injection")]
pub async fn ingest_hold_released() {
    let Some(control) = std::env::var_os("TESTBED_FAULT_HOLD") else {
        return;
    };
    while !std::fs::read_to_string(&control).is_ok_and(|s| s.trim() == "go") {
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
}

#[cfg(not(feature = "fault-injection"))]
pub async fn ingest_hold_released() {}
