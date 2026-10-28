//! 证据（Evidence）：离线子命令 `amberlog evidence export` 与 `amberlog evidence verify` 的全部工作
//! （REQ-AMB-STO-EVIDENCE-EXPORT、REQ-AMB-STO-EVIDENCE-VERIFY）。
//!
//! 证据只读：不调用快照提交与冷层写入，不对 `<storage.prefix>/` 写或删任何对象，链头与链上的对象按块定义的读法
//! 直接读出（[`read`]）。子命令的参数、临时文件、信号与退出码在 `amberlog-server` 的离线子命令编排里。

pub mod export;
pub mod package;
pub mod read;
pub mod tar;
pub mod verify;
