//! 版本字符串：`--version` 输出、`starting` 与 `ready` 日志的 `version` 字段共用（REQ-AMB-CFG-STARTUP-V2）。

/// Cargo 包版本（semver）。
pub const SEMVER: &str = env!("CARGO_PKG_VERSION");

/// 构建时注入的提交号，见 `build.rs`。
pub const COMMIT: &str = env!("BUILD_GIT_COMMIT");

/// `amberlog <semver> (<commit>)`
pub fn line() -> String {
    format!("amberlog {SEMVER} ({COMMIT})")
}
