//! REQ-AMB-UI-CONSOLE-V4 的系统测试：以子进程启动 `amberlog`（`auth.enabled` 为缺省的
//! true），以无头浏览器（`common::browser`，chrome-headless-shell，经 `tools/chrome.sh` 取得）打开 `/ui` 操作。
//! 对象存储用 `file://` 临时目录。三屏的元素 id 见 `common::ui` 的文件头；本文件另用顶栏的 `#tz-hint`（时区不认识的提示）、
//! `#token-note`（口令框旁的说明）、`#token-hint`（口令无效的提示）。
//! 停住查询的场景用可放行暂停点 `query.after_admission`（测试专用 feature `fault-injection`）。

mod common;

use std::time::{Duration, Instant};

use common::api::{HOUR_US, MINUTE_US, ingest_ok, now_us, poll, search_resp, sent, sent_element};
use common::authz::{self, ServiceToken};
use common::browser::Browser;
use common::gov::{query_ok, write_counters};
use common::missed::{GRACE_1M, Instance, minute_ago, ts_entry};
use common::oidc;
use common::ui::*;
use common::{BEARER, Fixture, Server, TOKEN};
use serde_json::{Value, json};
use sha2::Digest;

/// 2026-10-16T08:00:00Z。
const T: i64 = 1_792_137_600_000_000;

const CANCELLED: &str = "已取消。服务端可能仍在执行这次查询，马上重查可能提示服务繁忙";

fn around_now() -> (i64, i64) {
    let now = now_us();
    (now - HOUR_US, now + 1_000_000)
}

/// 检索屏填 stream 名与起止框。
fn fill_search(b: &mut Browser, stream: &str, start: &str, end: &str) {
    screen(b, "search");
    b.fill("s-stream", stream);
    search_range(b, start, end);
}

/// 检索请求的 `(start_time, end_time)`。
fn range_of(r: &common::browser::Request) -> (i64, i64) {
    let q = r.json();
    (
        q["query"]["start_time"].as_i64().unwrap(),
        q["query"]["end_time"].as_i64().unwrap(),
    )
}

/// 检索屏结果表 `_timestamp` 列第一行的格（元素 `.time`）的文字与悬停文字。
fn first_time_cell(b: &mut Browser) -> (String, String) {
    let v = b.eval(
        "(() => { const e = document.querySelector('#s-result tbody tr td .time'); \
         return e === null ? null : [e.innerText, e.title]; })()",
    );
    let a = v.as_array().expect("结果表有时刻格");
    (
        a[0].as_str().unwrap().to_owned(),
        a[1].as_str().unwrap().to_owned(),
    )
}

/// 页面背景（body）的亮度是否低于文字的亮度（相对亮度，WCAG 2 的算法）。
fn is_dark(b: &mut Browser) -> bool {
    b.eval(
        "(() => { const lum = (c) => { const [r, g, bl] = c.match(/\\d+(\\.\\d+)?/g).slice(0, 3).map(Number) \
           .map(v => { v /= 255; return v <= 0.03928 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4; }); \
           return 0.2126 * r + 0.7152 * g + 0.0722 * bl; }; \
         const s = getComputedStyle(document.body); return lum(s.backgroundColor) < lum(s.color); })()",
    ) == json!(true)
}

// ---------------------------------------------------------------------------
// page — 页面与入口
// ---------------------------------------------------------------------------

#[test]
fn ui_console_page_requests() {
    let (_fx, s) = start(&[]);
    let r = s.get("/ui");
    assert_eq!(r.status, 200);
    assert_eq!(r.header("content-type"), Some("text/html; charset=utf-8"));
    assert_page_headers(&r);
    // 请求记录记不到 Worker 发出的请求：页面全文不得用到这几种发请求的手段
    let page = r.text();
    for word in ["WebSocket", "Worker", "sendBeacon", "EventSource"] {
        assert!(!page.contains(word), "页面含 {word}");
    }
    let mut b = open(&s);
    std::thread::sleep(Duration::from_secs(5));
    let others: Vec<String> = b
        .requests()
        .iter()
        .filter(|r| {
            !(r.method == "GET" && r.path() == "/ui")
                && !(r.method == "POST" && r.path() == "/api/default/_search")
                && !is_oidc_config(r)
        })
        .map(|r| format!("{} {}", r.method, r.url))
        .collect();
    assert_eq!(others, Vec::<String>::new());
    // 另有恰一次 `GET /ui/oidc/config`（REQ-AMB-UI-CONSOLE-V4-LOGIN-CONFIG-REQUEST）
    assert_eq!(oidc_config_requests(&mut b).len(), 1);
}

/// 页面路径的响应头（REQ-AMB-UI-CONSOLE-V4-PAGE）。
fn assert_page_headers(r: &common::Resp) {
    assert_eq!(r.header("x-frame-options"), Some("DENY"));
    assert_eq!(r.header("referrer-policy"), Some("no-referrer"));
    let csp = r.header("content-security-policy").unwrap_or_default();
    assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
    assert_eq!(r.header("cache-control"), Some("no-cache"));
}

/// 界面取登录配置的请求 `GET /ui/oidc/config`。
fn is_oidc_config(r: &common::browser::Request) -> bool {
    r.method == "GET" && r.path() == "/ui/oidc/config"
}

/// 到此刻为止页面发出的 `GET /ui/oidc/config`。
fn oidc_config_requests(b: &mut Browser) -> Vec<common::browser::Request> {
    b.requests().into_iter().filter(is_oidc_config).collect()
}

/// 四个页面路径（查询串不影响结果）返回同一页面；`/ui/` 与其他 `/ui` 之下的路径按 REQ-AMB-ERR-ENVELOPE 为未注册路径
/// （REQ-AMB-UI-CONSOLE-V4-PAGE；没配界面登录时登录路径也是，REQ-AMB-AUTH-OIDC-UI-OFF-404）。
#[test]
fn ui_console_page_path_exact() {
    let (_fx, s) = start(&[]);
    let page = s.get("/ui").text();
    for path in [
        "/ui",
        "/ui?x=1",
        "/ui/search",
        "/ui/ledger",
        "/ui/reconciliation?tab=1",
    ] {
        let r = s.get(path);
        assert_eq!(r.status, 200, "{path}");
        assert_eq!(
            r.header("content-type"),
            Some("text/html; charset=utf-8"),
            "{path}"
        );
        assert_page_headers(&r);
        assert_eq!(r.text(), page, "{path}");
    }
    for path in [
        "/ui/",
        "/ui/settings",
        "/ui/search/x",
        "/ui/index.html",
        "/ui/assets/app.js",
        "/ui/callback",
    ] {
        let r = s.get(path);
        assert_eq!(r.status, 404, "{path}");
        assert_eq!(r.json()["reason"], "not_found", "{path}");
    }
}

#[test]
fn ui_console_routes_open_and_reload() {
    let (_fx, s) = start(&[]);
    let mut b = Browser::launch();
    for (path, screen) in [
        ("/ui/ledger", "ledger"),
        ("/ui/reconciliation", "recon"),
        ("/ui/search", "search"),
        ("/ui", "search"),
    ] {
        b.open(&format!("http://{}{path}", s.addr));
        let visible = format!(
            "document.getElementById('{screen}') !== null && !document.getElementById('{screen}').hidden"
        );
        assert!(b.wait_until(&visible, WAIT), "直接打开 {path}");
        assert_eq!(shown_screen(&mut b), screen, "直接打开 {path}");
        assert_eq!(b.eval("location.pathname"), json!(path), "直接打开 {path}");
        b.eval("window.__beforeReload = true");
        b.call("Page.reload", json!({"ignoreCache": true}));
        assert!(
            b.wait_until(
                &format!(
                    "document.readyState === 'complete' && window.__beforeReload !== true && ({visible})"
                ),
                WAIT
            ),
            "刷新 {path}"
        );
        assert_eq!(shown_screen(&mut b), screen, "刷新 {path}");
        assert_eq!(b.eval("location.pathname"), json!(path), "刷新 {path}");
    }
}

/// 检索屏的条件、时间范围与结果（比对切换前后）。
fn search_snapshot(b: &mut Browser) -> Value {
    b.eval(
        "[document.getElementById('s-stream').value, document.getElementById('s-conds').innerText, \
          document.getElementById('s-time-summary').textContent, document.getElementById('s-time-summary').title, \
          document.getElementById('s-result').innerHTML]",
    )
}

#[test]
fn ui_console_route_navigation_updates_history_without_queries() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    wait_candidates(&mut b);
    search_stream(&mut b, "app");
    let before = search_snapshot(&mut b);
    let n = b.requests().len();
    b.eval("window.__noReload = true");
    screen(&mut b, "ledger");
    assert!(b.wait_until("location.pathname === '/ui/ledger'", WAIT));
    screen(&mut b, "recon");
    assert!(b.wait_until("location.pathname === '/ui/reconciliation'", WAIT));
    b.eval("history.back()");
    assert!(b.wait_until("location.pathname === '/ui/ledger'", WAIT));
    assert_eq!(shown_screen(&mut b), "ledger");
    b.eval("history.back()");
    assert!(b.wait_until("location.pathname === '/ui'", WAIT));
    assert_eq!(shown_screen(&mut b), "search");
    assert_eq!(search_snapshot(&mut b), before);
    b.eval("history.forward()");
    assert!(b.wait_until("location.pathname === '/ui/ledger'", WAIT));
    assert_eq!(shown_screen(&mut b), "ledger");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(b.requests().len(), n, "切屏或前进后退发出了请求");
    assert_eq!(b.eval("window.__noReload"), json!(true), "页面重新加载了");
}

#[test]
fn ui_console_no_external_requests() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"},{"msg":"world"}]"#);
    let mut b = open_with_token(&s);
    search_stream(&mut b, "app");
    // 打开一行的详情
    b.click_sel("#s-result tbody tr");
    assert!(
        b.wait_until("document.getElementById('s-detail') !== null", WAIT),
        "详情未打开"
    );
    query_ledger(&mut b);
    b.eval(
        "[...document.querySelectorAll('#l-result tbody tr')].find(tr => tr.children[0].innerText === 'default/app')\
         .querySelectorAll('button').forEach(x => { if (x.textContent === '对账') x.click(); })",
    );
    wait_requests(&mut b, 1, is_recon);
    wait_settled(&mut b, "r");
    let others: Vec<String> = b
        .requests()
        .iter()
        .filter(|r| {
            !(r.method == "GET" && r.path() == "/ui")
                && !(r.method == "POST" && r.path() == "/api/default/_search")
                && !(r.method == "GET" && r.path() == "/api/default/app/_fields")
                && !is_oidc_config(r)
        })
        .map(|r| format!("{} {}", r.method, r.url))
        .collect();
    assert_eq!(others, Vec::<String>::new());
    // 另有恰一次 `GET /ui/oidc/config`
    assert_eq!(oidc_config_requests(&mut b).len(), 1);
}

#[test]
fn ui_console_three_screens() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    assert_eq!(shown_screen(&mut b), "search");
    for id in ["ledger", "recon", "search"] {
        screen(&mut b, id);
        assert_eq!(shown_screen(&mut b), id);
    }
}

#[test]
fn ui_console_switch_keeps_state() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    wait_candidates(&mut b);
    search_stream(&mut b, "app");
    let before = search_snapshot(&mut b);
    let n = data_requests(&mut b).len();
    b.eval("window.__noReload = true");
    screen(&mut b, "ledger");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "切到台账屏发了请求");
    assert_eq!(b.eval("location.pathname"), json!("/ui/ledger"));
    screen(&mut b, "search");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "切回检索屏发了请求");
    assert_eq!(b.eval("location.pathname"), json!("/ui/search"));
    assert_eq!(search_snapshot(&mut b), before);
    assert_eq!(b.eval("window.__noReload"), json!(true), "页面重新加载了");
}

#[test]
fn ui_console_requests_carry_timezone() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with(&s, |b| b.set_timezone("Asia/Shanghai"));
    fill_token(&mut b, TOKEN);
    assert_eq!(b.value("tz"), "Asia/Shanghai");
    search_stream(&mut b, "app");
    query_ledger(&mut b);
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.click("r-query");
    wait_settled(&mut b, "r");
    let rs = data_requests(&mut b);
    for p in [is_search, is_ledger, is_recon] {
        assert!(rs.iter().any(p), "三屏各有一个结果请求：{rs:?}");
    }
    for r in &rs {
        assert_eq!(r.json()["query"]["timezone"], "Asia/Shanghai", "{r:?}");
    }
}

// ---------------------------------------------------------------------------
// token — 口令
// ---------------------------------------------------------------------------

#[test]
fn ui_console_token_note() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    assert_eq!(b.text("token-note"), "只保存在本标签页，关闭即清除");
    // 说明紧挨着口令框（同在顶栏）
    assert!(b.exists("header #token") && b.exists("header #token-note"));
}

#[test]
fn ui_console_token_header() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open(&s);
    fill_token(&mut b, "s3cret");
    search_stream(&mut b, "app");
    fill_token(&mut b, "");
    b.click("s-query");
    let rs = wait_requests(&mut b, 2, is_search);
    assert_eq!(rs[0].header("Authorization"), Some("Bearer s3cret"));
    assert_eq!(rs[1].header("Authorization"), None);
}

#[test]
fn ui_console_token_not_persisted() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open(&s);
    fill_token(&mut b, "s3cret");
    search_stream(&mut b, "app");
    let stored = b.eval(
        "[location.href, document.cookie, \
          JSON.stringify(Object.entries(localStorage))]",
    );
    for v in stored.as_array().unwrap() {
        assert!(!v.as_str().unwrap().contains("s3cret"), "{stored}");
    }
    b.new_tab(&url(&s));
    assert_eq!(b.value("token"), "");
}

#[test]
fn ui_console_token_401_marked() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    fill_token(&mut b, "wrong-token");
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "请求失败", WAIT),
        "{}",
        b.text("s-result")
    );
    let text = b.text("s-result");
    assert!(
        text.contains("401") && text.contains("unauthorized"),
        "{text}"
    );
    assert_eq!(
        b.eval("document.getElementById('token').getAttribute('aria-invalid')"),
        json!("true")
    );
    assert_eq!(b.text("token-hint"), "口令无效或未填写");
    // 口令被修改（失去焦点时内容不同）后提示消失
    fill_token(&mut b, TOKEN);
    assert!(!b.exists("#token-hint"));
}

/// 口令框旁的提示（`#token-hint`，任一时刻至多一种）；没有时为 None。页面的脚本内联在 body 里，不能按全文找提示文字。
fn token_hint(b: &mut Browser) -> Option<String> {
    b.exists("#token-hint").then(|| b.text("token-hint"))
}

#[test]
fn ui_console_token_403_hint() {
    let fx = Fixture::new();
    let g = ServiceToken::generate();
    let entries = [authz::token_entry(
        "g",
        &g,
        r#"[{ role = "governance_viewer", streams = ["*"] }]"#,
    )];
    let s = authz::start(&fx, &entries, &[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open(&s);
    fill_token(&mut b, &g.token);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "请求失败", WAIT),
        "{}",
        b.text("s-result")
    );
    let text = b.text("s-result");
    assert!(text.contains("403") && text.contains("forbidden"), "{text}");
    assert_eq!(b.text("token-hint"), "当前身份没有这项操作的权限");
    assert_ne!(
        b.eval("document.getElementById('token').getAttribute('aria-invalid')"),
        json!("true"),
        "口令框被标出"
    );
    assert!(b.value("token") == g.token, "口令框的内容不再是令牌 G");
    // 口令被修改（失去焦点时内容不同）后提示消失
    fill_token(&mut b, TOKEN);
    assert_eq!(token_hint(&mut b), None);
}

// ---------------------------------------------------------------------------
// timezone — 时区
// ---------------------------------------------------------------------------

#[test]
fn ui_console_timezone_choices() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    b.click("tz");
    let v = b.eval(
        "[...document.querySelectorAll('#tz-listbox [role=\"option\"]')].map(o => o.dataset.value)",
    );
    let choices: Vec<String> = serde_json::from_value(v).unwrap();
    for z in ["UTC", "Asia/Shanghai", "America/New_York"] {
        assert!(
            choices.iter().any(|c| c == z),
            "{z}：{} 个候选",
            choices.len()
        );
    }
}

#[test]
fn ui_console_timezone_initial_browser() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with(&s, |b| b.set_timezone("America/New_York"));
    fill_token(&mut b, TOKEN);
    assert_eq!(b.value("tz"), "America/New_York");
    search_stream(&mut b, "app");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(r.json()["query"]["timezone"], "America/New_York");
}

#[test]
fn ui_console_timezone_remembered() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("Asia/Shanghai"));
    assert_eq!(b.value("tz"), "Asia/Shanghai");
    b.select("tz", "UTC");
    b.reopen(&url(&s));
    assert_eq!(b.value("tz"), "UTC");
}

#[test]
fn ui_console_timezone_redraw_no_request() {
    let (_fx, s) = start(&[]);
    ingest_at(&s, "app", T, json!({"msg": "x"}));
    let mut b = open_with(&s, |b| b.set_timezone("UTC"));
    fill_token(&mut b, TOKEN);
    fill_search(&mut b, "app", "2026-10-16 07:00", "2026-10-16 09:00");
    search_sql(&mut b, "SELECT * FROM \"app\"");
    assert_eq!(first_time_cell(&mut b).0, "2026-10-16 08:00:00.000000");
    let n = data_requests(&mut b).len();
    b.select("tz", "Asia/Shanghai");
    assert_eq!(first_time_cell(&mut b).0, "2026-10-16 16:00:00.000000");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "改时区后发了请求");
}

#[test]
fn ui_console_timezone_unknown_hint() {
    let (_fx, s) = start(&[]);
    let envelope = json!({"code": 400, "reason": "invalid_request", "message": "query.timezone: unknown time zone"});
    // 浏览器发出的 _search 请求一律改答：所选时区为 Asia/Shanghai 期间，每个请求都带它。只改答带它的请求，是为了
    // 改选 UTC 之后（检索屏会按新时区重发分布请求）请求照常到达服务端，提示不因改答而重现
    let mut b = open_with(&s, |b| {
        b.set_timezone("Asia/Shanghai");
        b.answer_where(
            |r| {
                r.path() == "/api/default/_search"
                    && r.body
                        .as_deref()
                        .is_some_and(|x| x.contains("\"timezone\":\"Asia/Shanghai\""))
            },
            400,
            &[("Content-Type", "application/json")],
            &envelope.to_string(),
        );
    });
    fill_token(&mut b, TOKEN);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "查询写法有误", WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(b.text("s-result").contains("invalid_request"));
    assert_eq!(b.text("tz-hint"), "服务端不认识所选时区，请改选其他时区");
    // 所选时区改变后提示消失
    b.select("tz", "UTC");
    assert!(!b.exists("#tz-hint"));
    std::thread::sleep(Duration::from_millis(500));
    assert!(!b.exists("#tz-hint"), "改选 UTC 后提示又出现");
}

// ---------------------------------------------------------------------------
// time — 时刻
// ---------------------------------------------------------------------------

/// 场景「时刻按所选时区显示」：`app` 含一条 T 的记录，所选时区 `Asia/Shanghai`，以覆盖它的范围查询。
fn time_format_local() -> (Fixture, Server, Browser) {
    let (fx, s) = start(&[]);
    ingest_at(&s, "app", T, json!({"msg": "x"}));
    let mut b = open_with(&s, |b| b.set_timezone("Asia/Shanghai"));
    fill_token(&mut b, TOKEN);
    fill_search(&mut b, "app", "2026-10-16 15:00", "2026-10-16 17:00");
    b.click("s-query");
    wait_settled(&mut b, "s");
    (fx, s, b)
}

#[test]
fn ui_console_time_format_local() {
    let (_fx, _s, mut b) = time_format_local();
    assert_eq!(first_time_cell(&mut b).0, "2026-10-16 16:00:00.000000");
}

#[test]
fn ui_console_time_label_hover() {
    let (_fx, _s, mut b) = time_format_local();
    let h = header(&mut b, "s");
    assert!(h[0].contains("Asia/Shanghai"), "{h:?}");
    let (_, title) = first_time_cell(&mut b);
    assert!(
        title.contains("+08:00") && title.contains("2026-10-16T08:00:00.000000Z"),
        "{title}"
    );
}

#[test]
fn ui_console_time_input_zone() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("Asia/Shanghai"));
    fill_token(&mut b, TOKEN);
    // 止点在检索屏弹层「绝对时间」的止点框里直接填带 `Z` 的 RFC 3339 时刻（REQ-AMB-UI-CONSOLE-V4-TIME-INPUT）
    fill_search(&mut b, "app", "2026-10-16 16:00", "2026-10-16T09:00:00Z");
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(range_of(&r), (1_792_137_600_000_000, 1_792_141_200_000_000));
}

#[test]
fn ui_console_time_input_dst() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("America/New_York"));
    fill_token(&mut b, TOKEN);
    fill_search(&mut b, "app", "2026-03-08 02:30", "2026-11-01 01:30");
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(range_of(&r), (1_772_953_200_000_000, 1_793_511_000_000_000));
}

#[test]
fn ui_console_time_input_invalid() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    // 每次在新开的标签页里填写，结果区原本没有「时刻格式无效」，它是这一次显示的
    for (i, (start, end)) in [
        ("2026/10/16 8点", ""),
        ("2026-10-16 08:00", ""),
        ("2026-02-30 10:00", "2026-03-01 10:00"),
        // 起点晚于止点、起止相等（REQ-AMB-UI-CONSOLE-V4-TIME-INPUT-INVALID）
        ("2026-10-16 09:00", "2026-10-16 08:00"),
        ("2026-10-16 08:00", "2026-10-16 08:00"),
    ]
    .into_iter()
    .enumerate()
    {
        if i > 0 {
            b.reopen(&url(&s));
        }
        screen(&mut b, "search");
        b.fill("s-stream", "app");
        assert!(!b.text("s-result").contains("时刻格式无效"));
        search_range(&mut b, start, end);
        b.click("s-query");
        assert!(
            b.wait_text("s-result", "时刻格式无效", WAIT),
            "{start} / {end}：{}",
            b.text("s-result")
        );
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_search).len(), 0);
}

#[test]
fn ui_console_quick_range() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("UTC"));
    fill_token(&mut b, TOKEN);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    search_quick(&mut b, 900_000_000);
    std::thread::sleep(Duration::from_millis(1100));
    let t0 = now_us();
    b.click("s-query");
    let t1 = now_us();
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    let (start, end) = range_of(&r);
    // 浏览器取的时刻精确到毫秒
    assert!(end >= t0 - 1000 && end <= t1, "{t0} ≤ {end} ≤ {t1}");
    assert_eq!(end - start, 900_000_000);
    // 检索屏时间范围的摘要显示这两个时刻（时刻摘要可到秒），悬停可见完整写法（REQ-AMB-UI-CONSOLE-V4-QUICK-RANGE、TIME-SUMMARY）
    let (a, z) = (utc_text(start), utc_text(end));
    assert!(
        b.wait_until(
            &format!(
                "document.getElementById('s-time-summary').title === {}",
                json!(format!("{a} → {z}"))
            ),
            WAIT
        ),
        "悬停文字：{}",
        b.eval("document.getElementById('s-time-summary').title")
    );
    let text = b.text("s-time-summary");
    assert!(
        text.contains(&a[..19]) && text.contains(&z[11..19]),
        "{text}"
    );
}

#[test]
fn ui_console_time_range_kept_on_tz_change() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("Asia/Shanghai"));
    fill_token(&mut b, TOKEN);
    fill_search(&mut b, "app", "2026-10-16 16:00", "2026-10-16 17:00");
    b.click("s-query");
    wait_requests(&mut b, 1, is_search);
    b.select("tz", "UTC");
    // 检索屏的起止框在时间范围弹层「绝对时间」页签里
    open_search_absolute(&mut b);
    assert_eq!(b.value("s-time-start"), "2026-10-16 08:00:00.000000");
    assert_eq!(b.value("s-time-end"), "2026-10-16 09:00:00.000000");
    close_search_range(&mut b);
    b.click("s-query");
    let rs = wait_requests(&mut b, 2, is_search);
    for r in &rs {
        assert_eq!(range_of(r), (1_792_137_600_000_000, 1_792_141_200_000_000));
    }
}

#[test]
fn ui_console_time_summary_hover() {
    let (_fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_timezone("UTC"));
    fill_token(&mut b, TOKEN);
    fill_search(&mut b, "app", "2026-10-16 08:00", "2026-10-16 09:00");
    b.click("s-query");
    wait_requests(&mut b, 1, is_search);
    let (a, z) = ("2026-10-16 08:00:00.000000", "2026-10-16 09:00:00.000000");
    // 摘要所示的起止是完整写法中连续的一段，悬停可见完整写法
    let text = b.text("s-time-summary");
    let (x, y) = text
        .split_once(" → ")
        .unwrap_or_else(|| panic!("摘要：{text}"));
    assert!(!x.is_empty() && a.contains(x.trim()), "{text}");
    assert!(!y.is_empty() && z.contains(y.trim()), "{text}");
    assert_eq!(
        b.eval("document.getElementById('s-time-summary').title"),
        json!(format!("{a} → {z}"))
    );
}

// ---------------------------------------------------------------------------
// values — 取值
// ---------------------------------------------------------------------------

/// 场景「停用数据集的空值显示为横线」的数据：`app`、`old` 各已提交一条记录，`old` 的链尾快照为格式版本 3
/// （REQ-AMB-STO-HOT-LAYOUT 的格式版本不兼容），由台账查询读到。热层巡检推迟，排除其他读取路径。
fn disabled_old(extra: &[common::Kv]) -> (Fixture, Server) {
    let fx = Fixture::new();
    let mut overrides = vec![
        ("flush.max_records", Some("1")),
        ("hot_fsck.interval", Some("\"24h\"")),
    ];
    overrides.extend_from_slice(extra);
    let s = fx.start(&overrides);
    for st in ["app", "old"] {
        ingest_ok(&s, st, r#"[{"a":1}]"#);
    }
    for st in ["app", "old"] {
        let ds = fx.dataset(st);
        let (_, done) = poll(
            Duration::from_secs(10),
            || {
                ds.current()
                    .map(|n| ds.snapshot(n)["summary"]["record_count"].clone())
            },
            |c| *c == Some(json!(1)),
        );
        assert!(done, "{st} 10 秒内未提交");
    }
    fx.dataset("old")
        .append_snapshot(|next| next["format_version"] = json!(3));
    (fx, s)
}

#[test]
fn ui_console_value_null_dash() {
    let (_fx, s) = disabled_old(&[]);
    let mut b = open_with_token(&s);
    query_ledger(&mut b);
    let r = row(&mut b, "l", "default/old");
    let at = |b: &mut Browser, name: &str| r[col(b, "l", name)].clone();
    assert_eq!(at(&mut b, "状态"), "已停用（格式版本不兼容）");
    for name in ["首条数据时间", "末条数据时间", "条数", "字节数", "所在层"] {
        assert_eq!(at(&mut b, name), "—", "{name}：{r:?}");
    }
}

#[test]
fn ui_console_value_raw() {
    let (_fx, s) = start(&[]);
    ingest_ok(
        &s,
        "app",
        r#"[{"big":12345678901234567,"f":0.1,"s":"a\"b"}]"#,
    );
    let mut b = open_with_token(&s);
    search_stream(&mut b, "app");
    let t = b.table("s-result");
    let h = header(&mut b, "s");
    let cell = |name: &str| {
        let i = h
            .iter()
            .position(|x| x == name)
            .unwrap_or_else(|| panic!("没有 {name} 列：{h:?}"));
        t[1][i].clone()
    };
    assert_eq!(cell("big"), "12345678901234567");
    assert_eq!(cell("f"), "0.1");
    assert_eq!(cell("s"), "a\"b");
}

#[test]
fn ui_console_empty_result() {
    let (_fx, s) = start(&[]);
    // 只有 3 小时前的一条，不在缺省的「最近 1 小时」里
    ingest_at(&s, "app", now_us() - 3 * HOUR_US, json!({"msg": "old"}));
    let mut b = open_with_token(&s);
    search_stream(&mut b, "app");
    // 「无结果」之下可另有说明文字（REQ-AMB-UI-CONSOLE-V4-EMPTY、FREEDOM）
    assert!(
        b.text("s-result").contains("无结果"),
        "{}",
        b.text("s-result")
    );
    assert!(!b.exists("#s-result table"));
}

/// `amberlog --version` 输出中的版本号（`amberlog <semver> (<commit>)` 的第二段）。
fn semver() -> String {
    let dir = common::TempDir::new();
    let out = common::amberlog(dir.path())
        .arg("--version")
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    text.split_whitespace().nth(1).unwrap().to_owned()
}

#[test]
fn ui_console_page_no_disclosure() {
    let fx = Fixture::new();
    let s = fx.start(&[("storage.prefix", Some("\"zqprefix7731\""))]);
    ingest_ok(&s, "zqds7731", r#"[{"msg":"hello"}]"#);
    let version = semver();
    assert!(!version.is_empty());
    let dir = fx.root().display().to_string();
    for path in ["/ui", "/ui/search", "/ui/ledger", "/ui/reconciliation"] {
        let r = s.get(path);
        assert_eq!(r.status, 200, "{path}");
        let page = r.text();
        for word in [version.as_str(), "zqds7731", "zqprefix7731", dir.as_str()] {
            assert!(!page.contains(word), "{path} 含 {word}");
        }
    }
}

#[test]
fn ui_console_page_limit() {
    let (_fx, s) = start(&[]);
    let records: Vec<Value> = (0..150).map(|i| json!({"i": i})).collect();
    ingest_ok(&s, "app", &Value::Array(records).to_string());
    let mut b = open_with_token(&s);
    search_stream(&mut b, "app");
    assert!(
        b.text("s-result").contains("结果已达本页上限 100 行"),
        "{}",
        b.text("s-result")
    );
    // 提示在结果表上方
    assert_eq!(
        b.eval(
            "(() => { const r = document.getElementById('s-result'); const p = [...r.querySelectorAll('*')]\
             .find(e => e.children.length === 0 && e.textContent === '结果已达本页上限 100 行'); \
             const t = r.querySelector('table'); \
             return !!(p && t && (p.compareDocumentPosition(t) & Node.DOCUMENT_POSITION_FOLLOWING)); })()"
        ),
        json!(true)
    );
}

// ---------------------------------------------------------------------------
// slow — 慢查询与取消
// ---------------------------------------------------------------------------

/// 场景「超过 3 秒显示已用时间与取消」：查询停在 `query.after_admission`，检索屏查询 `app` 并等 5 秒。
fn slow_query() -> (Fixture, Server, Gate, Browser) {
    let fx = Fixture::new();
    let gate = Gate::new(&fx, "query.after_admission");
    let s = start_gated(&fx, &[], &gate);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    let sent_at = Instant::now();
    b.click("s-query");
    while sent_at.elapsed() < Duration::from_millis(2700) {
        assert!(
            !b.exists("#s-cancel"),
            "发出后 {:?} 就显示了「取消」",
            sent_at.elapsed()
        );
        std::thread::sleep(Duration::from_millis(100));
    }
    let until = sent_at + Duration::from_secs(5);
    std::thread::sleep(until.saturating_duration_since(Instant::now()));
    (fx, s, gate, b)
}

#[test]
fn ui_console_slow_shows_cancel() {
    let (_fx, _s, gate, mut b) = slow_query();
    assert!(b.exists("#s-cancel"));
    let text = b.text("s-result");
    let secs: Option<u64> = text
        .split("已用 ")
        .nth(1)
        .and_then(|t| t.split(' ').next())
        .and_then(|n| n.parse().ok());
    assert!(secs.is_some_and(|n| (4..=6).contains(&n)), "{text}");
    gate.release();
}

#[test]
fn ui_console_cancel_message() {
    let (_fx, _s, gate, mut b) = slow_query();
    let n = data_requests(&mut b).len();
    b.click("s-cancel");
    assert!(
        b.wait_text("s-result", CANCELLED, WAIT),
        "{}",
        b.text("s-result")
    );
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(data_requests(&mut b).len(), n, "点「取消」后发了请求");
    gate.release();
}

#[test]
fn ui_console_no_browser_timeout() {
    let fx = Fixture::new();
    let gate = Gate::new(&fx, "query.after_admission");
    let s = start_gated(&fx, &[], &gate);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    let sent_at = Instant::now();
    while sent_at.elapsed() < Duration::from_secs(40) {
        let text = b.text("s-result");
        for word in [
            "无法连接服务",
            "请求失败",
            "查询写法有误",
            "服务繁忙",
            "暂时无法读取数据",
            "已取消",
        ] {
            assert!(
                !text.contains(word),
                "{:?} 时显示了出错文字：{text}",
                sent_at.elapsed()
            );
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    gate.release();
    assert!(
        b.wait_until("document.querySelector('#s-result table') !== null", WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(b.text("s-result").contains("hello"));
}

// ---------------------------------------------------------------------------
// errors — 出错时
// ---------------------------------------------------------------------------

#[test]
fn ui_console_429_countdown() {
    let fx = Fixture::new();
    let gate = Gate::new(&fx, "query.after_admission");
    let s = start_gated(&fx, &[("query.max_concurrency", Some("1"))], &gate);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    // 另一个客户端的查询占着唯一的执行名额
    let addr = s.addr;
    let other = std::thread::spawn(move || {
        let body = common::api::search_body("SELECT * FROM \"app\"");
        common::http(
            addr,
            "POST",
            "/api/default/_search",
            &[("Authorization", BEARER)],
            body.as_bytes(),
        )
    });
    gate.wait_held(1);
    let mut b = open_with_token(&s);
    wait_candidates(&mut b);
    screen(&mut b, "search");
    b.click("s-mode-sql");
    b.fill("s-sql", "SELECT * FROM \"app\"");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "服务繁忙：同时执行的查询已满", WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(
        b.text("s-result").contains(" 秒"),
        "倒计时：{}",
        b.text("s-result")
    );
    let disabled =
        |b: &mut Browser| b.eval("document.getElementById('s-query').disabled") == json!(true);
    assert!(disabled(&mut b), "倒计时中「查询」可用");
    let shown = Instant::now();
    assert!(b.wait_until(
        "document.getElementById('s-query').disabled === false",
        Duration::from_secs(120)
    ));
    assert!(
        shown.elapsed() >= Duration::from_millis(500),
        "倒计时太短：{:?}",
        shown.elapsed()
    );
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(
        requests_where(&mut b, is_search).len(),
        1,
        "倒计时前后又发了检索请求"
    );
    gate.release();
    assert_eq!(other.join().unwrap().status, 200);
}

#[test]
fn ui_console_memory_exceeded() {
    let fx = Fixture::new();
    let records: Vec<Value> = (0..1000)
        .map(|i| json!({"host": format!("h{i}")}))
        .collect();
    common::gov::committed(&fx, &[("app", &Value::Array(records).to_string())]);
    let s = fx.start(&[("query.max_memory_bytes", Some("1"))]);
    let sql = "SELECT host, count(*) FROM \"app\" GROUP BY host";
    let r = common::gov::query(&s, sql, around_now());
    assert_eq!(
        r.json()["reason"],
        "query_memory_exceeded",
        "前提：这条 SQL 越限：{}",
        r.text()
    );
    let mut b = open_with_token(&s);
    search_sql(&mut b, sql);
    // 出错文字之后另有一行请求编号（REQ-AMB-UI-CONSOLE-V4-ERR-REQUEST-ID）
    let text = b.text("s-result");
    assert_eq!(
        text.trim().lines().next(),
        Some("这次查询需要的内存超过上限。请缩小时间范围，或去掉全文条件、改用字段条件"),
        "{text}"
    );
}

#[test]
fn ui_console_invalid_sql() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let sql = "SELEC * FROM \"app\"";
    let expected = search_resp(&s, sql).json();
    assert_eq!(expected["reason"], "invalid_sql");
    let mut b = open_with_token(&s);
    search_sql(&mut b, sql);
    let text = b.text("s-result");
    for part in [
        "查询写法有误",
        "invalid_sql",
        expected["message"].as_str().unwrap(),
    ] {
        assert!(text.contains(part), "{part}：{text}");
    }
}

#[test]
fn ui_console_unavailable() {
    let (fx, s) = start(&[("flush.max_records", Some("1"))]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    let (_, done) = poll(
        Duration::from_secs(10),
        || fx.dataset("app").current(),
        Option::is_some,
    );
    assert!(done, "app 10 秒内未提交");
    let mut b = open_with_token(&s);
    wait_candidates(&mut b);
    // 对象存储停止服务：file:// 存储的目录拒绝访问
    common::set_mode(&fx.store_dir(), 0o000);
    let expected = common::gov::query(&s, "SELECT * FROM _ledger", around_now());
    query_ledger(&mut b);
    common::set_mode(&fx.store_dir(), 0o755);
    assert_eq!(
        expected.json()["reason"],
        "unavailable",
        "前提：{}",
        expected.text()
    );
    let text = b.text("l-result");
    for part in [
        "暂时无法读取数据（对象存储不可访问或服务正在停止）",
        expected.json()["message"].as_str().unwrap(),
    ] {
        assert!(text.contains(part), "{part}：{text}");
    }
}

#[test]
fn ui_console_other_failure() {
    let (_fx, s) = start(&[]);
    let (start, end) = around_now();
    let body = json!({"query": {"sql": "SELECT * FROM _ledger", "start_time": start, "end_time": end, "size": 1000}});
    let expected = s.request(
        "POST",
        "/api/default/_search",
        &[],
        body.to_string().as_bytes(),
    );
    assert_eq!(expected.status, 401, "前提：{}", expected.text());
    let mut b = open(&s);
    query_ledger(&mut b);
    let text = b.text("l-result");
    for part in [
        "请求失败",
        "401",
        "unauthorized",
        expected.json()["message"].as_str().unwrap(),
    ] {
        assert!(text.contains(part), "{part}：{text}");
    }
}

/// 改答全部 `_search` 请求为带 `x-request-id` 的错误信封后打开 `/ui` 并填入口令。
fn open_answering(s: &Server, status: u16, request_id: &str, envelope: &Value) -> Browser {
    let body = envelope.to_string();
    let mut b = open_with(s, |b| {
        b.answer_all(
            "/api/default/_search",
            status,
            &[
                ("Content-Type", "application/json"),
                ("x-request-id", request_id),
            ],
            &body,
        );
    });
    fill_token(&mut b, TOKEN);
    b
}

/// 结果区 `#<p>-result` 内文字恰为 `text` 的元素的计算样式 `user-select`；没有这样的元素时为 null。
fn user_select_of(b: &mut Browser, p: &str, text: &str) -> Value {
    b.eval(&format!(
        "(() => {{ const e = [...document.querySelectorAll('#{p}-result *')].find(x => x.children.length === 0 \
         && x.textContent.trim() === {text:?}); return e === undefined ? null : getComputedStyle(e).userSelect; }})()"
    ))
}

#[test]
fn ui_console_error_request_id_search() {
    let (_fx, s) = start(&[]);
    let envelope =
        json!({"code": 503, "reason": "unavailable", "message": "object store unreachable"});
    let mut b = open_answering(&s, 503, "u-503", &envelope);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "u-503", WAIT),
        "{}",
        b.text("s-result")
    );
    let text = b.text("s-result");
    for part in [
        "暂时无法读取数据（对象存储不可访问或服务正在停止）",
        "object store unreachable",
        "请求编号",
        "u-503",
    ] {
        assert!(text.contains(part), "{part}：{text}");
    }
    let us = user_select_of(&mut b, "s", "u-503");
    assert!(us.is_string(), "结果区没有文字恰为 u-503 的元素：{text}");
    assert_ne!(us, json!("none"));
}

#[test]
fn ui_console_error_request_id_ledger() {
    let (_fx, s) = start(&[]);
    let envelope = json!({"code": 400, "reason": "invalid_sql", "message": "near SELEC"});
    let mut b = open_answering(&s, 400, "u-400", &envelope);
    screen(&mut b, "ledger");
    b.click("l-query");
    assert!(
        b.wait_text("l-result", "u-400", WAIT),
        "{}",
        b.text("l-result")
    );
    let text = b.text("l-result");
    for part in [
        "查询写法有误",
        "invalid_sql",
        "near SELEC",
        "请求编号",
        "u-400",
    ] {
        assert!(text.contains(part), "{part}：{text}");
    }
}

#[test]
fn ui_console_backend_down() {
    let (_fx, mut s) = start(&[]);
    let mut b = open_with_token(&s);
    wait_candidates(&mut b);
    s.sigkill();
    screen(&mut b, "ledger");
    b.click("l-query");
    assert!(
        b.wait_text("l-result", "无法连接服务", Duration::from_secs(30)),
        "{}",
        b.text("l-result")
    );
}

// ---------------------------------------------------------------------------
// theme — 外观
// ---------------------------------------------------------------------------

/// 场景「缺省跟随系统、可改为浅色」：系统外观为深色、本地存储为空，打开后改为浅色。
fn theme_default_system() -> (Fixture, Server, Browser) {
    let (fx, s) = start(&[]);
    let mut b = open_with(&s, |b| b.set_color_scheme("dark"));
    assert!(is_dark(&mut b), "打开时不是深色外观");
    wait_candidates(&mut b);
    let n = b.requests().len();
    b.select("theme", "light");
    assert!(!is_dark(&mut b), "改为浅色后仍是深色外观");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(b.requests().len(), n, "改外观时发了请求");
    (fx, s, b)
}

#[test]
fn ui_console_theme_default_system() {
    theme_default_system();
}

#[test]
fn ui_console_theme_remembered() {
    let (_fx, s, mut b) = theme_default_system();
    b.reopen(&url(&s));
    assert!(!is_dark(&mut b), "重新打开后不是浅色外观");
}

// ---------------------------------------------------------------------------
// red-line — 红线
// ---------------------------------------------------------------------------

/// 三屏显示的文字，去掉检索屏中由响应得来的列名与取值、用户输入或加入的条件与 SQL 文本
/// （REQ-AMB-UI-CONSOLE-V2-REDLINE-CLAIMS、REDLINE-NUMBERS）：结果表格，以及页面标为 `data-verbatim`
/// 的元素（字段栏的字段名与取值、条件框中的条件、查询历史中的条件与 SQL 文本）。
fn text_besides_data(b: &mut Browser) -> String {
    b.eval(
        "(() => { const c = document.body.cloneNode(true); \
         c.querySelectorAll('#s-result table, [data-verbatim]').forEach(t => t.remove()); return c.textContent; })()",
    )
    .as_str()
    .unwrap_or_default()
    .to_owned()
}

#[test]
fn ui_console_no_integrity_claims() {
    // 某分钟窗口判为 missing：按时间戳上报 10 条、按时间戳确认 7 条（同 REQ-AMB-GOV-MISSED-COLLECTION 的造法）
    let i = Instance::start(&[GRACE_1M]);
    ingest_ok(&i.server, "app", r#"[{"msg":"hello"}]"#);
    let w = minute_ago(20);
    let e = ts_entry(
        w,
        0,
        0,
        json!({}),
        Some(json!({"a": 10})),
        Some((7, json!({"a": 7}))),
    );
    write_counters(&i.fx, "app", w - w.rem_euclid(HOUR_US), &[e]);
    i.wait_accepting(Duration::from_secs(60));
    let (hits, ok) = poll(
        WAIT,
        || {
            query_ok(
                &i.server,
                "SELECT verdict FROM _reconciliation WHERE dataset = 'default/app'",
                around_now(),
            )
        },
        |h| h.iter().any(|r| r["verdict"] == "missing"),
    );
    assert!(ok, "前提：有判为 missing 的窗口：{hits:?}");
    let mut b = open_with_token(&i.server);
    search_stream(&mut b, "app");
    query_ledger(&mut b);
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.click("r-query");
    wait_settled(&mut b, "r");
    let text = text_besides_data(&mut b);
    for word in [
        "自动发现漏采",
        "漏采告警",
        "数据完整性保障",
        "无漏采",
        "健康度",
        "完整率",
    ] {
        assert!(!text.contains(word), "{word}");
    }
}

#[test]
fn ui_console_no_unbuilt_entries() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    query_ledger(&mut b);
    screen(&mut b, "recon");
    search_stream(&mut b, "app");
    b.click_sel("#s-result tbody tr");
    assert!(
        b.wait_until("document.getElementById('s-detail') !== null", WAIT),
        "详情未打开"
    );
    let text = all_text(&mut b);
    for word in ["保存查询", "导出", "告警规则", "即将推出"] {
        assert!(!text.contains(word), "{word}");
    }
}

#[test]
fn ui_console_no_capacity_or_speed_claims() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"msg":"hello"}]"#);
    let mut b = open_with_token(&s);
    screen(&mut b, "recon");
    search_stream(&mut b, "app");
    query_ledger(&mut b);
    let text = text_besides_data(&mut b);
    for word in ["全文索引", "秒级", "冷层查询优化", "/秒", "TB", "PB"] {
        assert!(!text.contains(word), "{word}");
    }
}

#[test]
fn ui_console_ledger_not_colored() {
    let (_fx, s) = disabled_old(&[]);
    // app 有来源上报的发送数，web 没有
    let now = now_us();
    let w = now - now.rem_euclid(MINUTE_US);
    let r = sent(&s, "app", &format!("[{}]", sent_element("vector-1", w, 1)));
    assert_eq!(r.status, 200, "{}", r.text());
    ingest_ok(&s, "web", r#"[{"a":1}]"#);
    let mut b = open_with_token(&s);
    query_ledger(&mut b);
    for d in ["default/app", "default/web", "default/old"] {
        row(&mut b, "l", d);
    }
    let rows = cell_styles(&mut b, "#l-result tbody tr");
    assert_eq!(rows.len(), 3, "{rows:?}");
    let bg: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
    assert!(bg.iter().all(|x| *x == bg[0]), "三行背景色：{bg:?}");
    let mut cols = vec![col(&mut b, "l", "状态"), col(&mut b, "l", "对账状态")];
    for v in ["一致", "缺失", "重复", "上报中断", "待定", "不可判定"] {
        cols.push(col(&mut b, "l", v));
    }
    for c in cols {
        let cells = cell_styles(
            &mut b,
            &format!("#l-result tbody tr td:nth-child({})", c + 1),
        );
        assert_eq!(cells.len(), 3, "{cells:?}");
        let fg: Vec<&Value> = cells.iter().map(|x| &x[0]).collect();
        assert!(fg.iter().all(|x| *x == fg[0]), "第 {c} 列文字颜色：{fg:?}");
        assert!(
            cells.iter().all(|x| x[2] == cells[0][2]),
            "第 {c} 列内层样式：{cells:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// login — 界面登录
// ---------------------------------------------------------------------------

#[test]
fn ui_console_login_hidden_without_oidc() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    std::thread::sleep(Duration::from_secs(5));
    let config = oidc_config_requests(&mut b);
    assert_eq!(config.len(), 1, "GET /ui/oidc/config 不是恰一次");
    assert_eq!(b.status(&config[0]), Some(404));
    assert!(!b.exists("#login"), "没配界面登录时显示了「登录」入口");
    assert!(b.exists("#token"));
    assert_eq!(b.text("token-note"), "只保存在本标签页，关闭即清除");
    // 结果区只有查询前的占位（REQ-AMB-UI-CONSOLE-V4-FREEDOM），没有出错文字
    assert!(
        !b.exists("#s-result .message"),
        "检索屏结果区有出错文字：{}",
        b.text("s-result")
    );
}

/// 场景约定的「配了界面登录的实例」：`auth.token` 为 `s3cret`，`auth.oidc.*` 指向台架身份平台（含界面登录的
/// 三项），授权文件的 `idp_grants` 把 `alice` 映射为 `querier`、范围 [`*`]；等到说明文档已取得（`GET /ui/oidc/config` 得 200）。
fn login_instance() -> (Fixture, oidc::Idp, Server) {
    let fx = Fixture::new();
    let idp = oidc::Idp::start(&fx);
    let cfg = idp.login_config();
    let grant = oidc::idp_grant(
        &format!("subject = \"{}\"", oidc::ALICE_SUB),
        r#"[{ role = "querier", streams = ["*"] }]"#,
    );
    let s = authz::start(&fx, &[grant], &oidc::overrides(&cfg, &[]));
    let (_, ok) = poll(
        Duration::from_secs(20),
        || s.get("/ui/oidc/config").status,
        |st| *st == 200,
    );
    assert!(ok, "20 秒内 GET /ui/oidc/config 未得 200");
    (fx, idp, s)
}

/// 等「登录」入口出现。
fn wait_login_entry(b: &mut Browser) {
    assert!(
        b.wait_until("document.getElementById('login') !== null", WAIT),
        "没有「登录」入口"
    );
}

/// 让当前标签页导航到 `url`，等页面的路径为 `path` 且加载完成（回调页会抹掉查询串，不能按完整地址等）。
fn goto(b: &mut Browser, url: &str, path: &str) {
    b.call("Page.navigate", json!({"url": url}));
    assert!(
        b.wait_until(
            &format!(
                "location.pathname === {} && document.readyState === 'complete'",
                json!(path)
            ),
            Duration::from_secs(30)
        ),
        "{url}：{}",
        b.eval("location.href + ' ' + document.readyState")
    );
}

/// 当前页面会话存储、本地存储中的全部取值与 Cookie。
fn storage_values(b: &mut Browser) -> (Vec<String>, Vec<String>, String) {
    let v = b.eval("[Object.values(sessionStorage), Object.values(localStorage), document.cookie]");
    let a = v.as_array().unwrap();
    (
        serde_json::from_value(a[0].clone()).unwrap(),
        serde_json::from_value(a[1].clone()).unwrap(),
        a[2].as_str().unwrap_or_default().to_owned(),
    )
}

/// 查询串的各参数（百分号解码后）。
fn query_params(url: &str) -> std::collections::HashMap<String, String> {
    let q = url.split_once('?').map(|(_, q)| q).unwrap_or_default();
    let decode = |s: &str| {
        let bytes = s.replace('+', " ").into_bytes();
        let mut out = Vec::new();
        let mut i = 0;
        while i < bytes.len() {
            if bytes[i] == b'%' && i + 2 < bytes.len() {
                let h = std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap();
                out.push(u8::from_str_radix(h, 16).unwrap());
                i += 3;
            } else {
                out.push(bytes[i]);
                i += 1;
            }
        }
        String::from_utf8(out).unwrap()
    };
    q.split('&')
        .filter(|p| !p.is_empty())
        .map(|p| {
            let (k, v) = p.split_once('=').unwrap_or((p, ""));
            (decode(k), decode(v))
        })
        .collect()
}

/// 浏览器发出的、地址在台架身份平台授权地址上的请求（整页导航）。
fn authorize_navigations(b: &mut Browser, idp: &oidc::Idp) -> Vec<common::browser::Request> {
    let prefix = idp.url("/authorize");
    b.requests()
        .into_iter()
        .filter(|r| r.url.starts_with(&prefix))
        .collect()
}

/// 「测试台架拦截浏览器对台架身份平台授权地址的导航，不予放行」：对它的导航一律以 204 改答，不到达身份平台，
/// 页面也不离开（导航得 204 时浏览器留在原页面）。
fn block_authorize(b: &mut Browser, endpoint: &str) {
    let prefix = endpoint.to_owned();
    b.answer_where(move |r| r.url.starts_with(&prefix), 204, &[], "");
}

/// 等对授权地址 `endpoint` 的导航累计达到 `n` 个（至多 [`WAIT`]），返回全部。
fn wait_authorize(b: &mut Browser, endpoint: &str, n: usize) -> Vec<common::browser::Request> {
    let deadline = Instant::now() + WAIT;
    loop {
        let rs: Vec<_> = b
            .requests()
            .into_iter()
            .filter(|r| r.url.starts_with(endpoint))
            .collect();
        if rs.len() >= n {
            return rs;
        }
        assert!(
            Instant::now() < deadline,
            "{WAIT:?} 内对授权地址的导航只有 {} 个",
            rs.len()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// 浏览器发出的 `POST /ui/oidc/token`。
fn token_posts(b: &mut Browser) -> Vec<common::browser::Request> {
    b.requests()
        .into_iter()
        .filter(|r| r.method == "POST" && r.path() == "/ui/oidc/token")
        .collect()
}

/// 当前标签页的访问口令（会话存储中口令框的内容）。
fn session_token(b: &mut Browser) -> String {
    b.eval("sessionStorage.getItem('amberlog.token') ?? ''")
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

/// 点「登录」，台架身份平台通过 `alice` 的登录后，等页面回到 `/ui` 且口令框已有登录换得的令牌；返回该令牌。
fn login_as_alice(b: &mut Browser, s: &Server) -> String {
    wait_login_entry(b);
    b.click("login");
    let target = json!(url(s));
    assert!(
        b.wait_until(
            &format!(
                "location.href === {target} && document.readyState === 'complete' \
                 && document.getElementById('token') !== null && document.getElementById('token').value !== ''"
            ),
            Duration::from_secs(30)
        ),
        "登录后没有回到 /ui：{}",
        b.eval("location.href + ' ' + document.body.innerText")
    );
    session_token(b)
}

#[test]
fn ui_console_login_entry_with_token_box() {
    let (_fx, idp, s) = login_instance();
    let mut b = open(&s);
    wait_login_entry(&mut b);
    fill_token(&mut b, "wrong-token");
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "请求失败", WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(b.exists("header #login"), "「登录」入口不在口令框旁");
    assert_eq!(
        b.eval("document.getElementById('token').getAttribute('aria-invalid')"),
        json!("true")
    );
    assert_eq!(b.text("token-hint"), "口令无效或未填写");
    assert_eq!(b.eval("location.pathname"), json!("/ui"));
    assert!(authorize_navigations(&mut b, &idp).is_empty());
    assert!(idp.requests_to("/authorize").is_empty());
}

#[test]
fn ui_console_login_redirect_state_pkce() {
    let (_fx, idp, s) = login_instance();
    let config = s.get("/ui/oidc/config").json();
    let endpoint = config["authorization_endpoint"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut b = open_with(&s, |b| block_authorize(b, &endpoint));
    wait_login_entry(&mut b);
    b.click("login");
    let nav = &wait_authorize(&mut b, &endpoint, 1)[0];
    assert_eq!(nav.url.split('?').next(), Some(endpoint.as_str()));
    let p = query_params(&nav.url);
    let origin = format!("http://{}", s.addr);
    assert_eq!(p.get("response_type").map(String::as_str), Some("code"));
    assert_eq!(
        p.get("client_id").map(String::as_str),
        Some(oidc::CLIENT_ID)
    );
    assert_eq!(
        p.get("redirect_uri"),
        Some(&format!("{origin}/ui/callback"))
    );
    assert!(p.get("scope").is_some_and(|x| !x.is_empty()), "{p:?}");
    assert_eq!(
        p.get("code_challenge_method").map(String::as_str),
        Some("S256")
    );
    let state = p.get("state").cloned().unwrap_or_default();
    assert!(!state.is_empty(), "{p:?}");
    let challenge = p.get("code_challenge").cloned().unwrap_or_default();
    let (session, local, cookie) = storage_values(&mut b);
    assert!(session.contains(&state), "会话存储中没有 state");
    let verifier = session
        .iter()
        .find(|v| authz::base64url(&sha2::Sha256::digest(v.as_bytes())) == challenge)
        .cloned();
    assert!(
        verifier.is_some(),
        "会话存储中没有摘要等于 code_challenge 的值"
    );
    let verifier = verifier.unwrap();
    for x in [&state, &verifier] {
        assert!(
            !local.iter().any(|v| v.contains(x.as_str())),
            "本地存储含 state 或校验值"
        );
        assert!(!cookie.contains(x.as_str()), "Cookie 含 state 或校验值");
    }
    assert!(
        idp.requests_to("/authorize").is_empty(),
        "授权地址的导航被放行了"
    );
}

#[test]
fn ui_console_login_token_becomes_password() {
    let (_fx, idp, s) = login_instance();
    ingest_ok(&s, "app", r#"[{"msg":"after-login"}]"#);
    let mut b = open(&s);
    let t = login_as_alice(&mut b, &s);
    assert!(!t.is_empty() && t != TOKEN);
    let posts = token_posts(&mut b);
    assert_eq!(posts.len(), 1, "POST /ui/oidc/token 不是恰一次");
    let body = posts[0].json();
    let keys: Vec<&String> = body.as_object().unwrap().keys().collect();
    assert_eq!(keys, ["code", "code_verifier", "redirect_uri"]);
    // 令牌端点恰被交换一次：口令框里的令牌就是这次 POST /ui/oidc/token 换得的那一枚
    assert_eq!(idp.requests_to("/token").len(), 1);
    assert_eq!(b.eval("location.href"), json!(url(&s)));
    let history = b.call("Page.getNavigationHistory", json!({}));
    for e in history["entries"].as_array().unwrap() {
        let u = e["url"].as_str().unwrap_or_default();
        assert!(!u.contains("code="), "浏览历史含授权码：{u}");
    }
    let state = query_params(&authorize_navigations(&mut b, &idp)[0].url)["state"].clone();
    let verifier = body["code_verifier"].as_str().unwrap().to_owned();
    let (session, local, cookie) = storage_values(&mut b);
    assert!(!local.iter().any(|v| v.contains(&t)) && !cookie.contains(&t));
    assert!(!session.contains(&state) && !session.contains(&verifier));
    search_stream(&mut b, "app");
    let r = wait_requests(&mut b, 1, is_search).pop().unwrap();
    assert_eq!(
        r.header("authorization").map(str::to_owned),
        Some(format!("Bearer {t}"))
    );
    assert!(
        b.text("s-result").contains("after-login"),
        "{}",
        b.text("s-result")
    );
}

#[test]
fn ui_console_login_callback_check_fails() {
    let (_fx, idp, s) = login_instance();
    let endpoint = idp.url("/authorize");
    let mut b = open_with(&s, |b| block_authorize(b, &endpoint));
    let origin = format!("http://{}", s.addr);
    for (i, make) in [
        (|_: &str| "/ui/callback?code=c1&state=wrong".to_owned()) as fn(&str) -> String,
        |_| "/ui/callback?code=c1".to_owned(),
        |st| format!("/ui/callback?error=access_denied&state={st}"),
    ]
    .into_iter()
    .enumerate()
    {
        if i > 0 {
            b.open(&url(&s));
        }
        wait_login_entry(&mut b);
        b.click("login");
        let navs = wait_authorize(&mut b, &endpoint, i + 1);
        let state = query_params(&navs[i].url)["state"].clone();
        let target = make(&state);
        goto(&mut b, &format!("{origin}{target}"), "/ui/callback");
        assert!(
            b.wait_until(
                "document.body !== null && document.body.innerText.includes('登录失败')",
                WAIT
            ),
            "{target}：{}",
            b.eval("document.body.innerText")
        );
        assert_eq!(b.eval("location.search"), json!(""), "{target}");
        assert_eq!(session_token(&mut b), "", "{target}");
    }
    std::thread::sleep(Duration::from_millis(500));
    assert!(token_posts(&mut b).is_empty(), "发出了 POST /ui/oidc/token");
}

#[test]
fn ui_console_login_exchange_rejected() {
    let (_fx, idp, s) = login_instance();
    let envelope = json!({"code": 400, "reason": "invalid_request", "message": "rejected"});
    let mut b = open_with(&s, |b| {
        b.answer_all(
            "/ui/oidc/token",
            400,
            &[("Content-Type", "application/json")],
            &envelope.to_string(),
        );
    });
    wait_login_entry(&mut b);
    b.click("login");
    assert!(
        b.wait_until(
            "location.pathname === '/ui/callback' && document.body !== null && document.body.innerText.includes('登录失败')",
            Duration::from_secs(30)
        ),
        "{}",
        b.eval("location.href + ' ' + document.body.innerText")
    );
    let text = b
        .eval("document.body.innerText")
        .as_str()
        .unwrap_or_default()
        .to_owned();
    assert!(text.contains("400"), "{text}");
    assert_eq!(session_token(&mut b), "");
    let state = query_params(&authorize_navigations(&mut b, &idp)[0].url)["state"].clone();
    let verifier = token_posts(&mut b)[0].json()["code_verifier"]
        .as_str()
        .unwrap()
        .to_owned();
    let (session, _, _) = storage_values(&mut b);
    assert!(
        !session.contains(&state) && !session.contains(&verifier),
        "{session:?}"
    );
}

#[test]
fn ui_console_login_callback_static() {
    let (_fx, _idp, s) = login_instance();
    let plain = s.get("/ui/callback");
    let with_query = s.get("/ui/callback?code=abc&state=xyz");
    for r in [&plain, &with_query] {
        assert_eq!(r.status, 200);
        assert_eq!(r.header("content-type"), Some("text/html; charset=utf-8"));
        assert_eq!(r.header("x-frame-options"), Some("DENY"));
        assert_eq!(r.header("referrer-policy"), Some("no-referrer"));
        let csp = r.header("content-security-policy").unwrap_or_default();
        assert!(csp.contains("frame-ancestors 'none'"), "{csp}");
    }
    assert_eq!(plain.body, with_query.body);
    let target = format!("http://{}/ui/callback?code=abc&state=xyz", s.addr);
    let mut b = Browser::launch();
    goto(&mut b, &target, "/ui/callback");
    std::thread::sleep(Duration::from_secs(5));
    let urls: Vec<String> = b.requests().into_iter().map(|r| r.url).collect();
    assert_eq!(urls, [target]);
    assert!(
        b.eval("document.body.innerText")
            .as_str()
            .unwrap_or_default()
            .contains("登录失败")
    );
}

#[test]
fn ui_console_login_reauth_on_401() {
    let (_fx, idp, s) = login_instance();
    ingest_ok(&s, "app", r#"[{"msg":"after-login"}]"#);
    let answered = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let armed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let envelope = json!({"code": 401, "reason": "unauthorized", "message": "expired"});
    let (a, m) = (answered.clone(), armed.clone());
    let mut b = open_with(&s, move |b| {
        // 下一个 _search 请求（armed 之后的第一个）改答为 401
        b.answer_where(
            move |r| {
                use std::sync::atomic::Ordering::SeqCst;
                r.path() == "/api/default/_search"
                    && m.load(SeqCst)
                    && a.compare_exchange(false, true, SeqCst, SeqCst).is_ok()
            },
            401,
            &[("Content-Type", "application/json")],
            &envelope.to_string(),
        );
    });
    let first = login_as_alice(&mut b, &s);
    search_stream(&mut b, "app");
    assert!(b.text("s-result").contains("after-login"));
    armed.store(true, std::sync::atomic::Ordering::SeqCst);
    b.click("s-query");
    // 整页跳转到授权地址，通过登录后回到 /ui，口令框里是新换得的令牌
    assert!(
        b.wait_until(
            &format!(
                "location.href === {} && document.readyState === 'complete' && document.getElementById('token') !== null \
                 && document.getElementById('token').value !== '' && document.getElementById('token').value !== {}",
                json!(url(&s)),
                json!(first)
            ),
            Duration::from_secs(30)
        ),
        "没有重新登录回到 /ui：{}",
        b.eval("location.href")
    );
    assert!(answered.load(std::sync::atomic::Ordering::SeqCst));
    assert_eq!(authorize_navigations(&mut b, &idp).len(), 2);
    assert_eq!(idp.requests_to("/token").len(), 2);
    assert_ne!(
        b.eval("document.getElementById('token').getAttribute('aria-invalid')"),
        json!("true")
    );
    assert_ne!(token_hint(&mut b).as_deref(), Some("口令无效或未填写"));
    let second = session_token(&mut b);
    let n = requests_where(&mut b, is_search).len();
    search_stream(&mut b, "app");
    let r = wait_requests(&mut b, n + 1, is_search).pop().unwrap();
    assert_eq!(
        r.header("authorization").map(str::to_owned),
        Some(format!("Bearer {second}"))
    );
    assert!(
        b.text("s-result").contains("after-login"),
        "{}",
        b.text("s-result")
    );
}

#[test]
fn ui_console_login_reauth_stops_after_return() {
    let (_fx, idp, s) = login_instance();
    let envelope = json!({"code": 401, "reason": "unauthorized", "message": "not accepted"});
    let mut b = open_with(&s, |b| {
        b.answer_all(
            "/api/default/_search",
            401,
            &[("Content-Type", "application/json")],
            &envelope.to_string(),
        );
    });
    login_as_alice(&mut b, &s);
    let back = Instant::now();
    while back.elapsed() < Duration::from_secs(10) {
        b.eval("0");
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(b.eval("location.href"), json!(url(&s)));
    assert_eq!(
        authorize_navigations(&mut b, &idp).len(),
        1,
        "回到 /ui 后又导航到了身份平台"
    );
    assert_eq!(
        b.text("token-hint"),
        "登录得到的身份未通过服务端认证，已停止自动登录"
    );
    // REQ-AMB-UI-CONSOLE-V4-LOGIN-REAUTH 的现行读法：停止以后直到口令被修改或再点「登录」都不跳转——再点一次「查询」、
    // 页面重新载入后都照旧停止，提示留着
    let n = requests_where(&mut b, is_search).len();
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    b.click("s-query");
    wait_requests(&mut b, n + 1, is_search);
    std::thread::sleep(Duration::from_secs(2));
    b.open(&url(&s));
    std::thread::sleep(Duration::from_secs(3));
    assert_eq!(b.eval("location.href"), json!(url(&s)));
    assert_eq!(
        authorize_navigations(&mut b, &idp).len(),
        1,
        "停止以后又导航到了身份平台"
    );
    assert_eq!(
        token_hint(&mut b).as_deref(),
        Some("登录得到的身份未通过服务端认证，已停止自动登录")
    );
}
