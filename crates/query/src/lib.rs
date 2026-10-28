//! 查询规划与执行（Query）。
//!
//! `_search`（REQ-AMB-QRY-SEARCH-ENDPOINT-V5）按报告顺序分几步：[`request`] 解析请求体，[`sql`] 判定能否解析与子集，
//! [`search`] 解析目标 stream、读取存储并规划（REQ-AMB-QRY-PLAN-VISIBILITY；冷层一侧见 [`cold`]，REQ-AMB-QRY-CROSS-TIER），[`bind`] 绑定列与类型检查，最后执行
//! （聚合查询见 [`aggregate`]，`histogram()` 见 [`histogram`]，`str_match()` 与 `match_all()` 见 [`text_match`]）。
//! `FROM` 引用系统表时由 [`system`]（治理视图 Governance）生成整张表，再走同一执行步骤。
//! 执行名额与每查询一份的内存池见 [`limits`]（REQ-AMB-QRY-RESOURCE-LIMITS）。字段列表端点（REQ-AMB-QRY-FIELDS）的作答见 [`fields`]。

pub mod aggregate;
pub mod bind;
pub mod bloom;
pub mod cold;
pub mod error;
pub mod fields;
pub mod files;
pub mod histogram;
mod input;
pub mod limits;
pub mod prune;
pub mod request;
pub mod search;
pub mod sql;
pub mod system;
#[cfg(any(test, feature = "testing"))]
pub mod testing;
pub mod text_match;
