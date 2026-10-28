//! REQ-AMB-UI-CONSOLE 的系统测试：以子进程启动 `amberlog`（`auth.enabled` 为缺省的
//! true），以无头浏览器（`common::browser`，chrome-headless-shell，经 `tools/chrome.sh` 取得）打开 `/ui` 操作。
//! 对象存储用 `file://` 临时目录。三屏的元素 id 见 `common::ui` 的文件头；本文件另用顶栏的 `#tz-hint`（时区不认识的提示）、
//! `#token-note`（口令框旁的说明）、`#token-hint`（口令无效的提示）。
//! 停住查询的场景用可放行暂停点 `query.after_admission`（测试专用 feature `fault-injection`）。

mod common;

use std::time::{Duration, Instant};

use common::api::{HOUR_US, MINUTE_US, ingest_ok, now_us, poll, search_resp, sent, sent_element};
use common::browser::Browser;
use common::gov::{query_ok, write_counters};
use common::missed::{GRACE_1M, Instance, minute_ago, ts_entry};
use common::ui::*;
use common::{BEARER, Fixture, Server, TOKEN};
use serde_json::{Value, json};

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
    b.fill("s-start", start);
    b.fill("s-end", end);
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
            r.path() != "/ui" && !(r.method == "POST" && r.path() == "/api/default/_search")
        })
        .map(|r| format!("{} {}", r.method, r.url))
        .collect();
    assert_eq!(others, Vec::<String>::new());
}

/// 查询串不影响结果，`/ui/` 等其他路径为未注册路径（REQ-AMB-UI-CONSOLE-PAGE；后者按 REQ-AMB-ERR-ENVELOPE，不对应块场景）。
#[test]
fn ui_console_page_path_exact() {
    let (_fx, s) = start(&[]);
    let page = s.get("/ui").text();
    let q = s.get("/ui?x=1#y");
    assert_eq!(q.status, 200);
    assert_eq!(q.text(), page);
    for p in ["/ui/", "/ui/index.html", "/ui/assets/app.js"] {
        assert_eq!(s.get(p).status, 404, "{p}");
    }
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
            r.path() != "/ui"
                && !(r.method == "POST" && r.path() == "/api/default/_search")
                && !(r.method == "GET" && r.path() == "/api/default/app/_fields")
        })
        .map(|r| format!("{} {}", r.method, r.url))
        .collect();
    assert_eq!(others, Vec::<String>::new());
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
    let snapshot = |b: &mut Browser| {
        b.eval(
            "[document.getElementById('s-stream').value, document.getElementById('s-start').value, \
              document.getElementById('s-end').value, document.getElementById('s-quick').value, \
              document.getElementById('s-result').innerHTML]",
        )
    };
    let before = snapshot(&mut b);
    let n = data_requests(&mut b).len();
    screen(&mut b, "ledger");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "切到台账屏发了请求");
    screen(&mut b, "search");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "切回检索屏发了请求");
    assert_eq!(snapshot(&mut b), before);
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

// ---------------------------------------------------------------------------
// timezone — 时区
// ---------------------------------------------------------------------------

#[test]
fn ui_console_timezone_choices() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    let v = b.eval("[...document.getElementById('tz').options].map(o => o.value)");
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
    // 每次在新开的标签页里填写，结果区原本为空，「时刻格式无效」是这一次显示的
    for (i, (start, end)) in [
        ("2026/10/16 8点", ""),
        ("2026-10-16 08:00", ""),
        ("2026-02-30 10:00", "2026-03-01 10:00"),
    ]
    .into_iter()
    .enumerate()
    {
        if i > 0 {
            b.reopen(&url(&s));
        }
        screen(&mut b, "search");
        b.fill("s-stream", "app");
        assert_eq!(b.text("s-result").trim(), "");
        b.fill("s-start", start);
        b.fill("s-end", end);
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
    b.select("s-quick", "900000000");
    std::thread::sleep(Duration::from_millis(1100));
    let t0 = now_us();
    b.click("s-query");
    let t1 = now_us();
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    let (start, end) = range_of(&r);
    // 浏览器取的时刻精确到毫秒
    assert!(end >= t0 - 1000 && end <= t1, "{t0} ≤ {end} ≤ {t1}");
    assert_eq!(end - start, 900_000_000);
    assert_eq!(b.value("s-start"), utc_text(start));
    assert_eq!(b.value("s-end"), utc_text(end));
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
    assert_eq!(b.value("s-start"), "2026-10-16 08:00:00.000000");
    assert_eq!(b.value("s-end"), "2026-10-16 09:00:00.000000");
    b.click("s-query");
    let rs = wait_requests(&mut b, 2, is_search);
    for r in &rs {
        assert_eq!(range_of(r), (1_792_137_600_000_000, 1_792_141_200_000_000));
    }
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
    let cell = |name: &str| {
        let i = t[0]
            .iter()
            .position(|h| h == name)
            .unwrap_or_else(|| panic!("没有 {name} 列：{t:?}"));
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
    assert_eq!(b.text("s-result").trim(), "无结果");
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
    assert_eq!(
        b.text("s-result").trim(),
        "这次查询需要的内存超过上限。请缩小时间范围，或去掉全文条件、改用字段条件"
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
