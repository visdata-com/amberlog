//! 检索界面（REQ-AMB-UI-CONSOLE）：`GET /ui` 返回一个自包含的单页（脚本、样式与图标内嵌，编译时嵌入二进制）。
//!
//! 页面是仓根 `ui/` 前端工程的构建产物（ADR-0055）：改 `ui/src/` 后在 `ui/` 下执行 `pnpm build` 重新生成
//! 本文件旁的 `ui/index.html` 并一起提交，Rust 构建不调 Node。页面是浏览器里的 `_search` 客户端：检索、台账、对账三屏的数据只经
//! `POST /api/default/_search` 与 `GET /api/default/<stream>/_fields` 取得（台账与对账是系统表 `_ledger`、`_reconciliation`，
//! 后者给检索屏的字段栏与取值提示），访问口令由用户在页面上填写、
//! 只存当前标签页的会话存储。`/ui` 不在认证区内（REQ-AMB-ERR-ENVELOPE），服务端不往页面里嵌入任何口令或数据。

use axum::Router;
use axum::http::header;
use axum::response::IntoResponse;

use crate::gateway::Endpoint;

/// 页面全文（`ui/` 的构建产物）。
pub const PAGE: &str = include_str!("ui/index.html");

/// 注册 `GET /ui`（精确路径；查询串不影响结果，`/ui/` 等其他路径由接入面按未注册路径作答）。
pub fn router() -> Router {
    Router::new().route("/ui", Endpoint::new().get(page).into())
}

async fn page() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], PAGE)
}
