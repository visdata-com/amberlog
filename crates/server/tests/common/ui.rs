//! 检索界面（REQ-AMB-UI-CONSOLE、REQ-AMB-UI-SEARCH、REQ-AMB-UI-GOVERNANCE）三屏的系统测试共用部件：起实例、以无头浏览器
//! 打开 `/ui`、按元素 id 操作三屏、区分页面发出的各种数据请求、造数据。
//!
//! 元素 id（用户看不到，只给测试用）：顶栏 `token`、`tz`、`theme`，屏切换为 `nav button[data-screen=search|ledger|recon]`；
//! 三屏各以 `s-`、`l-`、`r-` 为前缀：`#<p>-start`、`#<p>-end`（起止框）、`#<p>-quick`（快捷项）、`#<p>-query`（「查询」）、
//! `#<p>-result`（结果区）、`#<p>-cancel`（「取消」）；检索屏另有 `#s-stream`、`#s-mode-cond`、`#s-mode-sql`、`#s-sql`，
//! 对账屏另有 `#r-dataset`。其余 id 由各屏的测试文件在文件头写明。

use std::time::Duration;

use serde_json::{Value, json};

use super::browser::{Browser, Request};
use super::{Fixture, Kv, Server, TOKEN};

pub const WAIT: Duration = Duration::from_secs(20);

/// 以给定配置起一个实例（`auth.enabled` 为缺省的 true，对象存储为 `file://` 临时目录）。
pub fn start(overrides: &[Kv]) -> (Fixture, Server) {
    let fx = Fixture::new();
    let s = fx.start(overrides);
    (fx, s)
}

pub fn url(s: &Server) -> String {
    format!("http://{}/ui", s.addr)
}

/// 起浏览器、先做 `setup`（改时区、外观、改答规则等），再打开 `/ui`。
pub fn open_with(s: &Server, setup: impl FnOnce(&mut Browser)) -> Browser {
    let mut b = Browser::launch();
    setup(&mut b);
    b.open(&url(s));
    b
}

/// 打开 `/ui` 的浏览器。
pub fn open(s: &Server) -> Browser {
    open_with(s, |_| {})
}

/// 打开 `/ui` 并填入访问口令。
pub fn open_with_token(s: &Server) -> Browser {
    let mut b = open(s);
    fill_token(&mut b, TOKEN);
    b
}

/// 填口令并确认（失去焦点）。
pub fn fill_token(b: &mut Browser, token: &str) {
    b.fill("token", token);
    b.blur("token");
}

// ---- 数据请求 ----

/// 请求体的 `query.sql`；没有请求体或请求体不是 JSON 时为空串（改答规则对 `GET /ui` 也会调判断函数）。
pub fn sql_of(r: &Request) -> String {
    r.body
        .as_deref()
        .and_then(|b| serde_json::from_str::<Value>(b).ok())
        .and_then(|v| v["query"]["sql"].as_str().map(str::to_owned))
        .unwrap_or_default()
}

/// 数据集候选查询（REQ-AMB-UI-SEARCH-DATASET-QUERY）。
pub fn is_candidates(r: &Request) -> bool {
    sql_of(r) == "SELECT dataset FROM _ledger"
}

/// 分布请求（REQ-AMB-UI-SEARCH-HIST-REQUEST）。
pub fn is_histogram(r: &Request) -> bool {
    sql_of(r).starts_with("SELECT histogram(")
}

/// 计数请求（REQ-AMB-UI-SEARCH-TOTAL）。
pub fn is_count(r: &Request) -> bool {
    sql_of(r).starts_with("SELECT count(*) AS n")
}

/// 附属请求：数据集候选查询、分布请求与计数请求（REQ-AMB-UI-CONSOLE-TERM）。
pub fn is_ancillary(r: &Request) -> bool {
    is_candidates(r) || is_histogram(r) || is_count(r)
}

/// 台账请求（REQ-AMB-UI-GOVERNANCE-LEDGER-QUERY）。
pub fn is_ledger(r: &Request) -> bool {
    sql_of(r) == "SELECT * FROM _ledger"
}

/// 对账请求（REQ-AMB-UI-GOVERNANCE-RECON-QUERY）。
pub fn is_recon(r: &Request) -> bool {
    sql_of(r).starts_with("SELECT * FROM _reconciliation WHERE dataset = ")
}

/// 检索请求：附属请求、台账请求与对账请求以外的数据请求。
pub fn is_search(r: &Request) -> bool {
    r.method == "POST"
        && r.path() == "/api/default/_search"
        && !is_ancillary(r)
        && !is_ledger(r)
        && !is_recon(r)
}

/// 到此刻为止页面发出的数据请求（`POST /api/default/_search`）。
pub fn data_requests(b: &mut Browser) -> Vec<Request> {
    b.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path() == "/api/default/_search")
        .collect()
}

/// 到此刻为止满足 `pred` 的数据请求。
pub fn requests_where(b: &mut Browser, pred: impl Fn(&Request) -> bool) -> Vec<Request> {
    data_requests(b).into_iter().filter(|r| pred(r)).collect()
}

/// 等满足 `pred` 的数据请求累计达到 `n` 个（至多 [`WAIT`]），返回全部。
pub fn wait_requests(b: &mut Browser, n: usize, pred: impl Fn(&Request) -> bool) -> Vec<Request> {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        let rs = requests_where(b, &pred);
        if rs.len() >= n {
            return rs;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{WAIT:?} 内只发出 {} 个，要 {n} 个：{rs:?}",
            rs.len()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// 等打开页面时的数据集候选查询发出并收到应答（之后再数请求，不受它干扰）。
pub fn wait_candidates(b: &mut Browser) {
    let r = wait_requests(b, 1, is_candidates).remove(0);
    assert!(
        b.wait_finished(&r, WAIT),
        "数据集候选查询 {WAIT:?} 内未收到应答"
    );
}

// ---- 三屏 ----

/// 切到某屏（`search`、`ledger`、`recon`）。
pub fn screen(b: &mut Browser, id: &str) {
    b.click_sel(&format!("nav button[data-screen={id}]"));
}

/// 当前显示的屏。
pub fn shown_screen(b: &mut Browser) -> String {
    b.eval("[...document.querySelectorAll('main > section')].filter(s => !s.hidden).map(s => s.id).join(',')")
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// 某屏（前缀 `p`）的结果区不在等应答（显示了结果、出错或提示）。
pub fn settled_js(p: &str) -> String {
    format!(
        "(() => {{ const r = document.getElementById('{p}-result'); \
         return r !== null && r.querySelector('.stale') === null && r.innerText.trim() !== ''; }})()"
    )
}

/// 等某屏的结果区出结果或提示（至多 [`WAIT`]）。
pub fn wait_settled(b: &mut Browser, p: &str) {
    let ok = b.wait_until(&settled_js(p), WAIT);
    assert!(ok, "{p}-result：{}", b.text(&format!("{p}-result")));
}

/// 检索屏以条件模式查询 stream（不加条件），等结果区出结果。
pub fn search_stream(b: &mut Browser, stream: &str) {
    screen(b, "search");
    b.fill("s-stream", stream);
    b.click("s-query");
    wait_settled(b, "s");
}

/// 检索屏切到 SQL 模式、以 `sql` 查询，等结果区出结果。
pub fn search_sql(b: &mut Browser, sql: &str) {
    screen(b, "search");
    b.click("s-mode-sql");
    b.fill("s-sql", sql);
    b.click("s-query");
    wait_settled(b, "s");
}

/// 台账屏点「查询」，等结果区出结果。
pub fn query_ledger(b: &mut Browser) {
    screen(b, "ledger");
    b.click("l-query");
    wait_settled(b, "l");
}

/// 结果区里表格的表头（各格文字）：表头有几行时取最后一行（台账屏的判定汇总在上一行另有组名），它与数据行逐格对应。
pub fn header(b: &mut Browser, p: &str) -> Vec<String> {
    let v = b.eval(&format!(
        "(() => {{ const t = document.querySelector('#{p}-result table'); if (t === null) return []; \
         const rows = t.tHead !== null && t.tHead.rows.length > 0 ? t.tHead.rows : t.rows; \
         const last = rows[rows.length - 1]; \
         return last === undefined ? [] : [...last.children].map(c => c.innerText); }})()"
    ));
    serde_json::from_value(v).unwrap()
}

/// 结果区表格中第一格为 `first` 的一行（各格文字）。
pub fn row(b: &mut Browser, p: &str, first: &str) -> Vec<String> {
    b.table(&format!("{p}-result"))
        .into_iter()
        .find(|r| r.first().map(String::as_str) == Some(first))
        .unwrap_or_else(|| panic!("{p}-result 没有 {first} 一行"))
}

/// 列头为 `name`（显示时刻的列另带「（<时区名>）」）的列的下标。整名比较：「条数」不会对上「首条数据时间」。
pub fn col(b: &mut Browser, p: &str, name: &str) -> usize {
    let h = header(b, p);
    let zoned = format!("{name}（");
    h.iter()
        .position(|c| c == name || c.starts_with(&zoned))
        .unwrap_or_else(|| panic!("{p}-result 没有「{name}」列：{h:?}"))
}

/// 三屏全部可见文字（含当前隐藏的屏）。
pub fn all_text(b: &mut Browser) -> String {
    b.eval("document.body.textContent")
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

// ---- 造数据 ----

/// 摄取一条 `_timestamp` 为 `us` 的记录（以 RFC 3339 字符串写入，不受数值单位推断影响）。
pub fn ingest_at(s: &Server, stream: &str, us: i64, fields: Value) {
    let mut rec = fields;
    rec["_timestamp"] = json!(super::gov::rfc3339(us));
    super::api::ingest_ok(s, stream, &json!([rec]).to_string());
}

/// 等 `sql` 在 `[start, end)` 中查到至少 `n` 行（摄取后到可见可能有延迟）。
pub fn wait_visible(s: &Server, sql: &str, range: (i64, i64), n: usize) {
    let (hits, ok) = super::api::poll(
        WAIT,
        || super::gov::query_ok(s, sql, range),
        |h| h.len() >= n,
    );
    assert!(ok, "{sql}：{hits:?}");
}

// ---- 停住查询 ----

/// 可放行暂停点的 `hold-all` 闸门（测试专用 feature `fault-injection`）：每个走到该步骤的查询都停住，
/// [`Gate::release`] 后一并放行。
pub struct Gate {
    step: &'static str,
    file: std::path::PathBuf,
}

impl Gate {
    pub fn new(fx: &Fixture, step: &'static str) -> Gate {
        let file = fx.root().join(format!("gate-{step}"));
        std::fs::write(&file, "hold-all").unwrap();
        Gate { step, file }
    }

    pub fn release(&self) {
        std::fs::write(&self.file, "go").unwrap();
    }

    /// 再次停住此后走到该步骤的查询（以 [`Gate::release`] 放开的闸门，场景中途「随后启用注入点」时用）。
    pub fn hold(&self) {
        std::fs::write(&self.file, "hold-all").unwrap();
    }

    /// 此刻停在该步骤的调用数。
    pub fn held(&self) -> usize {
        let mut held = self.file.clone().into_os_string();
        held.push(".held");
        std::fs::read_to_string(held)
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    /// 等停住的调用数达到 `n`（至多 [`WAIT`]）。
    pub fn wait_held(&self, n: usize) {
        let deadline = std::time::Instant::now() + WAIT;
        while self.held() < n {
            assert!(
                std::time::Instant::now() < deadline,
                "{WAIT:?} 内停在 {} 的调用只有 {} 个",
                self.step,
                self.held()
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    pub fn env(&self) -> (String, String) {
        (
            "TESTBED_FAULT_GATES".into(),
            format!("{}={}", self.step, self.file.display()),
        )
    }
}

/// 以配置 `overrides` 与闸门 `gate` 起实例。
pub fn start_gated(fx: &Fixture, overrides: &[Kv], gate: &Gate) -> Server {
    let mut cmd = fx.serve(overrides);
    let (k, v) = gate.env();
    cmd.env(k, v);
    Server::start(cmd)
}

/// 页面中选择器 `selector` 选中的各元素的计算样式：实际可见的背景（自元素向上第一个不透明的背景色）、元素自身的前景色，
/// 以及元素全部后代的前景色与背景色。
pub fn cell_styles(b: &mut Browser, selector: &str) -> Vec<Value> {
    let v = b.eval(&format!(
        "[...document.querySelectorAll({})].map(c => {{ \
           const clear = (x) => x === 'transparent' || x === 'rgba(0, 0, 0, 0)'; \
           let bg = null; \
           for (let e = c; e && !bg; e = e.parentElement) {{ \
             const x = getComputedStyle(e).backgroundColor; if (!clear(x)) bg = x; }} \
           const inner = [...c.querySelectorAll('*')].map(d => {{ const s = getComputedStyle(d); \
             return s.color + ' ' + s.backgroundColor; }}); \
           return [getComputedStyle(c).color, bg, inner]; }})",
        json!(selector)
    ));
    v.as_array().unwrap().clone()
}

/// UTC 微秒在 UTC 中的界面写法（`YYYY-MM-DD HH:MM:SS.ffffff`）。
pub fn utc_text(us: i64) -> String {
    super::gov::rfc3339(us)
        .replace('T', " ")
        .trim_end_matches('Z')
        .to_owned()
}
