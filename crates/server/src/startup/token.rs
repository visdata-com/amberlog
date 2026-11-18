//! 工具子命令 `amberlog token new`（REQ-AMB-CFG-STARTUP-V10）：生成一个服务令牌（REQ-AMB-AUTH-AUTHZ-TOKEN-FORMAT），向 stdout
//! 写两行——令牌与它的哈希行（REQ-AMB-AUTH-AUTHZ-TOKEN-MATCH）——然后退出。
//!
//! 不读配置文件与 `AMBERLOG_*` 环境变量，不建运行时，不访问对象存储，不监听，不输出运行日志。退出码只有 0 与 2：取随机量或
//! 写 stdout 失败时 stderr 一行原因、以 2 退出。

use std::io::{self, Write};

use super::{EXIT_CONFIG, EXIT_OK};
use crate::authz::token;

pub(super) fn run() -> u8 {
    let mut random = [0u8; 32];
    if let Err(e) = fill_random(&mut random) {
        eprintln!("amberlog: cannot read random bytes from the operating system: {e}");
        return EXIT_CONFIG;
    }
    let token = token::from_random(&random);
    let line = token::hash_line(&token::digest(token.as_bytes()));
    let mut out = io::stdout().lock();
    match writeln!(out, "{token}\n{line}").and_then(|()| out.flush()) {
        Ok(()) => EXIT_OK,
        Err(e) => {
            eprintln!("amberlog: cannot write to stdout: {e}");
            EXIT_CONFIG
        }
    }
}

/// 取操作系统随机源（`getrandom(2)`，阻塞到熵池就绪），直到填满 `buf`。
fn fill_random(buf: &mut [u8]) -> io::Result<()> {
    let mut filled = 0;
    while filled < buf.len() {
        let rest = &mut buf[filled..];
        // SAFETY: 指针与长度取自同一个可写切片
        let n = unsafe { libc::getrandom(rest.as_mut_ptr().cast(), rest.len(), 0) };
        if n < 0 {
            let e = io::Error::last_os_error();
            if e.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            return Err(e);
        }
        filled += n.cast_unsigned();
    }
    Ok(())
}
