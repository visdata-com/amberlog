//! 可执行文件 `amberlog`（REQ-AMB-CFG-STARTUP-V2）。

use std::process::ExitCode;

fn main() -> ExitCode {
    // 未预期的 panic 属未分类的失败，退出码 1（Rust 默认为 101）。
    let code =
        std::panic::catch_unwind(|| amberlog_server::startup::main(std::env::args_os().skip(1)))
            .unwrap_or(amberlog_server::startup::EXIT_UNCLASSIFIED);
    ExitCode::from(code)
}
