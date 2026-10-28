//! 进程形态：同一可执行文件 `amberlog` 的几种运行方式，由生命周期编排按子命令选定。
//!
//! 每种形态只启动它用得到的元素，「不监听」「不碰 `wal.dir`」「不写对象存储」这类承诺靠「对应元素根本不存在」直接保证：
//! 各形态的启动流程分在 [`crate::startup`] 下各自的子模块里，只构造本形态的元素。配置的必填项也按形态分开
//! （REQ-AMB-CFG-STARTUP-V7），由 [`crate::config::load`] 按本类型判定。

/// 进程形态。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Form {
    /// 写入实例（`amberlog serve`）：全部元素。
    Serve,
    /// 只读目录模式（`amberlog catalog`，REQ-AMB-STO-COLD-CATALOG）：生命周期编排、接入面、目录投影、健康与指标。
    Catalog,
    /// 退役运行方式（`amberlog retire`，REQ-AMB-LIFE-RETIREMENT）：生命周期编排、桶生命周期（只做退役收尾）、
    /// 物化、治理计数、冷层写入、快照提交、可见集、耐久缓冲、健康与指标；不启动接入面，不监听。
    Retire,
    /// 证据运行方式（`amberlog evidence export|verify`，REQ-AMB-STO-EVIDENCE-EXPORT、-VERIFY）：生命周期编排与证据；
    /// 不监听、不碰 `wal.dir`、不写对象存储。
    Evidence,
}

impl Form {
    /// 是否要求配置 `wal.dir`：要重放 WAL 的形态（写入实例与退役）。
    pub fn requires_wal_dir(self) -> bool {
        matches!(self, Form::Serve | Form::Retire)
    }

    /// `object_store.url` 为 `s3://` 时是否要求 `object_store.s3.conditional_put`：要写快照的形态（写入实例与退役）。
    pub fn requires_conditional_put(self) -> bool {
        matches!(self, Form::Serve | Form::Retire)
    }

    /// `auth.enabled` 为 true 时是否要求 `auth.token`：受理请求的形态（写入实例与只读目录模式）。
    pub fn requires_auth_token(self) -> bool {
        self.listens()
    }

    /// 是否监听并受理请求（有接入面）：写入实例与只读目录模式。入站 TLS 的设置与明文规则只对它们起作用
    /// （REQ-AMB-ARCH-ENCRYPTION-SCOPE-*）。
    pub fn listens(self) -> bool {
        matches!(self, Form::Serve | Form::Catalog)
    }

    /// 服务端加密是否起作用：写对象存储的形态（写入实例与退役，REQ-AMB-ARCH-ENCRYPTION-SCOPE-*）。
    pub fn writes_objects(self) -> bool {
        matches!(self, Form::Serve | Form::Retire)
    }
}

#[cfg(test)]
mod tests {
    use super::Form;

    #[test]
    fn required_keys_by_form() {
        let row = |f: Form| {
            (
                f.requires_wal_dir(),
                f.requires_conditional_put(),
                f.requires_auth_token(),
            )
        };
        assert_eq!(row(Form::Serve), (true, true, true));
        assert_eq!(row(Form::Catalog), (false, false, true));
        assert_eq!(row(Form::Retire), (true, true, false));
        assert_eq!(row(Form::Evidence), (false, false, false));
    }
}
