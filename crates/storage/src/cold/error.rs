//! 冷层实现内部的失败分类；对外按 [`super`] 的接口错误类型给出。

use std::fmt;

use crate::gate::DatasetState;
use crate::store::Unavailable;

/// 冷层操作的失败，按块的处置分类。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ColdError {
    /// 对象存储不可访问（REQ-AMB-STO-HOT-LAYOUT）：不停用数据集（REQ-AMB-STO-COLD-TABLE-READ-INACCESSIBLE）。
    Unavailable(Unavailable),
    /// 所需的冷层对象不存在或内容无法解析（REQ-AMB-STO-COLD-TABLE-UNPARSEABLE）。是否因此停用由读取的来源定
    /// （REQ-AMB-STO-COLD-TABLE-READ-BROKEN），由调用方决定。
    Broken(String),
    /// 要读的冷层数据文件、清单列表或清单对象不存在（元数据文件不存在归为 [`ColdError::Broken`]）。对外与
    /// [`ColdError::Broken`] 同类，只有跨层查询要分开它，以重判「数据已移动」（REQ-AMB-QRY-CROSS-TIER-V2）。
    Missing(String),
    /// 冷层提交重试耗尽（REQ-AMB-STO-COLD-TABLE-EXHAUST）：数据集已进入 manifest 不可读情形。
    Exhausted,
    /// 数据集处于停用状态：不写也不删冷层表目录之下的对象（REQ-AMB-STO-COLD-TABLE-DISABLED）。
    Disabled(DatasetState),
    /// 冷层删除提交所列文件都已不在冷层存活文件集中。
    Stale,
    /// 调用方给出的输入不合本块的约定（调用方的缺陷），或上游库报错。
    Invalid(String),
    /// 入湖把入湖输入重写为冷层数据文件时失败（解码、排序、编码等，不含冷层对象的读取）：
    /// REQ-AMB-STO-LAKE-ENTRY-V2-REWRITE-FAIL 所称重写失败。
    Rewrite(String),
}

impl fmt::Display for ColdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColdError::Unavailable(e) => write!(f, "{e}"),
            ColdError::Broken(m) => write!(f, "cold table object is missing or unparseable: {m}"),
            ColdError::Missing(m) => write!(f, "cold object does not exist: {m}"),
            ColdError::Exhausted => f.write_str("cold commit retries exhausted"),
            ColdError::Disabled(state) => write!(f, "dataset is disabled: {state:?}"),
            ColdError::Stale => f.write_str("none of the files to delete is live"),
            ColdError::Invalid(m) => write!(f, "{m}"),
            ColdError::Rewrite(m) => write!(f, "{m}"),
        }
    }
}

impl std::error::Error for ColdError {}

impl From<Unavailable> for ColdError {
    fn from(e: Unavailable) -> Self {
        ColdError::Unavailable(e)
    }
}

pub(crate) fn invalid(what: impl fmt::Display) -> ColdError {
    ColdError::Invalid(what.to_string())
}
