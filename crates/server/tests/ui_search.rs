//! REQ-AMB-UI-SEARCH-V2 的系统测试：以子进程启动 `amberlog`（`auth.enabled` 为缺省的
//! true），以无头浏览器（`common::browser`）打开 `/ui` 操作检索屏。对象存储用 `file://` 临时目录。
//! 三屏共用的元素 id 见 `common::ui` 的文件头；检索屏另用：`#s-cond-input`（条件框的输入）、`#s-conds .chip`（胶囊，
//! `data-staged` 为未生效）、`#s-hints li`（输入提示）、`#s-confirm-discard` 与 `#s-confirm-stay`（切回条件的确认）、
//! `#s-staged`（「查询」旁的未生效标记）、`#s-limit`（全文范围超限的说明）、`#s-sql-note`（SQL 模式长范围全文的提示）、
//! `#s-total`（总条数）、`#s-info`（本页行范围与扫描量）、`#s-hist-toggle`（分布图开关）、`#s-hist`（分布图区）、
//! `#s-hist .hbar`（柱，`data-start` 为分桶起点）、`#s-per-bar`（「每柱 <间隔>」）、`#s-sort`（`_timestamp` 列头）、
//! `th[data-col] .hide`（隐藏列）、`#s-prev`、`#s-next`、`#s-streams option`（数据集候选）。
//! 分布请求、计数请求按请求体 `sql` 的开头区分（`common::ui::is_histogram`、`is_count`），由台架以 `Fetch.fulfillRequest`
//! 改答（`answer_where`）。场景用到晚于接收时刻的记录时以 `ingest.max_future` 放宽摄取的时间窗。
//! 字段栏与取值提示一组在「fields」分区，其后依次为单条详情「detail」、上下文浏览「context」与本地查询历史「history」分区。

mod common;

use std::time::Duration;

use common::api::{HOUR_US, MINUTE_US, ingest_ok, now_us};
use common::browser::{Browser, Request};
use common::gov::query_ok;
use common::ui::*;
use common::{Kv, Server, TOKEN};
use serde_json::{Value, json};

/// 摄取时间窗放到很远，2026-10-30 等未来时刻的记录也能写入。
const FUTURE: Kv = ("ingest.max_future", Some("\"3000000d\""));

/// 2026-10-30T08:00:00Z。
const T0830: i64 = 1_793_347_200_000_000;

const LIMIT_TEXT: &str = "含全文检索时，时间范围最多 7 天。可以先用字段条件缩小，或分段查询";
const SQL_NOTE: &str = "含全文检索的长范围查询会扫描范围内的全部文本字段，可能很慢";
const BUSY: &str = "服务繁忙：同时执行的查询已满";

// ---------------------------------------------------------------------------
// 部件
// ---------------------------------------------------------------------------

/// 以时区 `tz` 打开 `/ui`、填口令，等数据集候选查询（填口令后重发的那次）收到应答。
fn open_in(s: &Server, tz: &str, setup: impl FnOnce(&mut Browser)) -> Browser {
    let mut b = open_with(s, |b| {
        b.set_timezone(tz);
        setup(b);
    });
    // 先等打开页面时的候选查询（不带口令）收到应答，再填口令：页面加载完时组件可能还没发出它
    let first = wait_requests(&mut b, 1, is_candidates).remove(0);
    assert!(b.wait_finished(&first, WAIT), "数据集候选查询未收到应答");
    fill_token(&mut b, TOKEN);
    let second = wait_requests(&mut b, 2, is_candidates).remove(1);
    assert!(b.wait_finished(&second, WAIT), "数据集候选查询未收到应答");
    b
}

/// 在条件框输入一段文字并回车。
fn add(b: &mut Browser, text: &str) {
    b.fill("s-cond-input", text);
    b.press("s-cond-input", "Enter");
    // 等输入框清空：条件已加入
    assert!(
        b.wait_until("document.getElementById('s-cond-input').value === ''", WAIT),
        "条件 {text} 未加入"
    );
}

/// 切到 SQL 模式，返回 SQL 文本。
fn to_sql(b: &mut Browser) -> String {
    b.click("s-mode-sql");
    assert!(
        b.wait_until("document.getElementById('s-sql') !== null", WAIT),
        "未切到 SQL 模式"
    );
    b.value("s-sql")
}

/// 胶囊的文字与是否未生效。
fn chips(b: &mut Browser) -> Vec<(String, bool)> {
    let v = b.eval(
        "[...document.querySelectorAll('#s-conds .chip')].map(c => \
         [c.querySelector('.chip-text').innerText, c.dataset.staged === 'true'])",
    );
    serde_json::from_value(v).unwrap()
}

/// 点胶囊 `text` 的删除。
fn remove_chip(b: &mut Browser, text: &str) {
    b.eval(&format!(
        "[...document.querySelectorAll('#s-conds .chip')].find(c => c.querySelector('.chip-text').innerText === {})\
         .querySelector('.chip-remove').click()",
        json!(text)
    ));
}

/// 数据集候选。
fn candidates(b: &mut Browser) -> Vec<String> {
    let v = b.eval("[...document.querySelectorAll('#s-streams option')].map(o => o.value)");
    serde_json::from_value(v).unwrap()
}

fn query_of(r: &Request) -> Value {
    r.json()["query"].clone()
}

fn range_of(r: &Request) -> (i64, i64) {
    let q = query_of(r);
    (
        q["start_time"].as_i64().unwrap(),
        q["end_time"].as_i64().unwrap(),
    )
}

/// 填数据集与起止框（按所选时区的本地时刻）。
fn fill_range(b: &mut Browser, stream: &str, start: &str, end: &str) {
    screen(b, "search");
    b.fill("s-stream", stream);
    b.fill("s-start", start);
    b.fill("s-end", end);
}

/// 点「查询」，等结果区出结果。
fn query(b: &mut Browser) {
    b.click("s-query");
    wait_settled(b, "s");
}

fn disabled(b: &mut Browser, id: &str) -> bool {
    b.eval(&format!("document.getElementById({}).disabled", json!(id))) == json!(true)
}

/// 分布图的柱：分桶起点与悬停文字。
fn bars(b: &mut Browser) -> Vec<(String, String)> {
    let v = b.eval(
        "[...document.querySelectorAll('#s-hist .hbar')].map(e => [e.dataset.start, e.title])",
    );
    serde_json::from_value(v).unwrap()
}

/// 等分布图画出 `n` 根柱。
fn wait_bars(b: &mut Browser, n: usize) {
    let ok = b.wait_until(
        &format!("document.querySelectorAll('#s-hist .hbar').length === {n}"),
        WAIT,
    );
    assert!(ok, "分布图：{:?}；图区：{}", bars(b), b.text("search"));
}

/// 结果表的列名（`th` 的 `data-col`）。
fn columns(b: &mut Browser) -> Vec<String> {
    let v = b.eval("[...document.querySelectorAll('#s-result th')].map(t => t.dataset.col)");
    serde_json::from_value(v).unwrap()
}

/// 结果表的行数。
fn row_count(b: &mut Browser) -> usize {
    b.eval("document.querySelectorAll('#s-result tbody tr').length")
        .as_u64()
        .unwrap() as usize
}

/// 以 HTTP `status` 与 `reason` 的错误信封改答满足 `pred` 的请求。
fn answer_error(b: &mut Browser, pred: fn(&Request) -> bool, status: u16, reason: &str) {
    let envelope =
        json!({"code": status, "reason": reason, "message": format!("injected {reason}")});
    b.answer_where(
        // 打开页面的 GET /ui 没有请求体，只判断数据请求
        move |r| r.method == "POST" && pred(r),
        status,
        &[("Content-Type", "application/json")],
        &envelope.to_string(),
    );
}

/// 摄取一条记录到 `stream`（接收时刻），等它可查。
fn ingest_now(s: &Server, stream: &str, record: Value) {
    ingest_ok(s, stream, &json!([record]).to_string());
    let now = now_us();
    wait_visible(
        s,
        &format!("SELECT * FROM \"{stream}\""),
        (now - HOUR_US, now + MINUTE_US),
        1,
    );
}

/// 「分布图画出全部分桶」的数据：stream `app` 中 08:15:10、08:15:20、08:15:30 与 08:40:00（2026-10-30，UTC）四条。
fn draw_data(s: &Server) {
    for us in [
        T0830 + 15 * MINUTE_US + 10_000_000,
        T0830 + 15 * MINUTE_US + 20_000_000,
        T0830 + 15 * MINUTE_US + 30_000_000,
        T0830 + 40 * MINUTE_US,
    ] {
        ingest_at(s, "app", us, json!({"k": "v"}));
    }
    wait_visible(s, "SELECT * FROM \"app\"", (T0830, T0830 + HOUR_US), 4);
}

/// 场景「分布图画出全部分桶」：UTC，`app` 在 2026-10-30 08:00～09:00 查询，分布图画完。
fn drawn(s: &Server) -> Browser {
    draw_data(s);
    let mut b = open_in(s, "UTC", |_| {});
    fill_range(&mut b, "app", "2026-10-30 08:00", "2026-10-30 09:00");
    query(&mut b);
    wait_bars(&mut b, 60);
    b
}

// ---------------------------------------------------------------------------
// dataset-range — 数据集与时间范围
// ---------------------------------------------------------------------------

#[test]
fn ui_search_dataset_query_on_open() {
    let (_fx, s) = start(&[("auth.enabled", Some("false"))]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    ingest_ok(&s, "web", r#"[{"a":1}]"#);
    let now = now_us();
    wait_visible(
        &s,
        "SELECT dataset FROM _ledger",
        (now - MINUTE_US, now + MINUTE_US),
        2,
    );
    let mut b = open(&s);
    std::thread::sleep(Duration::from_secs(5));
    let rs = data_requests(&mut b);
    assert_eq!(rs.len(), 1, "{rs:?}");
    let q = query_of(&rs[0]);
    assert_eq!(q["sql"], "SELECT dataset FROM _ledger");
    assert_eq!(q["size"], 10000);
    let (start, end) = range_of(&rs[0]);
    assert_eq!(end - start, 60_000_000);
    let c = candidates(&mut b);
    assert!(
        c.contains(&"app".into()) && c.contains(&"web".into()),
        "{c:?}"
    );
}

#[test]
fn ui_search_dataset_manual() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    assert!(!candidates(&mut b).contains(&"late".into()));
    screen(&mut b, "search");
    b.fill("s-stream", "late");
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(sql_of(&r), "SELECT * FROM \"late\"");
}

#[test]
fn ui_search_dataset_401_retry() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open(&s);
    let first = wait_requests(&mut b, 1, is_candidates).remove(0);
    assert!(b.wait_finished(&first, WAIT));
    assert!(
        b.wait_until(
            "document.getElementById('token').classList.contains('invalid')",
            WAIT
        ),
        "口令框未标出"
    );
    assert_eq!(b.text("s-result").trim(), "", "结果区有出错文字");
    assert!(!disabled(&mut b, "s-query"));
    fill_token(&mut b, TOKEN);
    wait_requests(&mut b, 2, is_candidates);
    assert!(
        b.wait_until(
            "[...document.querySelectorAll('#s-streams option')].some(o => o.value === 'app')",
            WAIT
        ),
        "候选：{:?}",
        candidates(&mut b)
    );
}

#[test]
fn ui_search_dataset_from_ledger() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |_| {});
    assert!(!candidates(&mut b).contains(&"fresh".into()));
    ingest_now(&s, "fresh", json!({"a": 1}));
    let now = now_us();
    let (_, ok) = common::api::poll(
        WAIT,
        || {
            query_ok(
                &s,
                "SELECT dataset FROM _ledger WHERE dataset = 'default/fresh'",
                (now - MINUTE_US, now + MINUTE_US),
            )
        },
        |h| !h.is_empty(),
    );
    assert!(ok, "台账没有 default/fresh");
    query_ledger(&mut b);
    screen(&mut b, "search");
    let c = candidates(&mut b);
    assert!(c.contains(&"fresh".into()), "{c:?}");
}

#[test]
fn ui_search_default_range() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    let before = now_us();
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    let after = now_us();
    let (start, end) = range_of(&r);
    assert!(
        before - 1000 <= end && end <= after + 1000,
        "{before} ≤ {end} ≤ {after}"
    );
    assert_eq!(end - start, 3_600_000_000);
}

// ---------------------------------------------------------------------------
// conditions — 条件框
// ---------------------------------------------------------------------------

#[test]
fn ui_search_modes() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    assert_eq!(
        b.eval("document.getElementById('s-mode-cond').getAttribute('aria-pressed')"),
        json!("true")
    );
    assert!(b.exists("#s-cond-input"));
    to_sql(&mut b);
    assert_eq!(
        b.eval("document.getElementById('s-mode-sql').getAttribute('aria-pressed')"),
        json!("true")
    );
}

#[test]
fn ui_search_field_condition_split() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    for t in [
        "status>=500",
        "host.name = a b",
        "msg like %err%",
        "user Is Not Null",
    ] {
        add(&mut b, t);
    }
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE "status" >= 500 AND "host.name" = 'a b' AND "msg" LIKE '%err%' AND "user" IS NOT NULL"#
    );
}

#[test]
fn ui_search_text_condition() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    for t in ["timeout", "it's", "\"user=alice\"", "= 5"] {
        add(&mut b, t);
    }
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE match_all('timeout') AND match_all('it''s') AND match_all('user=alice') AND match_all('= 5')"#
    );
}

#[test]
fn ui_search_condition_remove() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "status >= 500");
    add(&mut b, "timeout");
    remove_chip(&mut b, "status >= 500");
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(
        sql_of(&r),
        r#"SELECT * FROM "app" WHERE match_all('timeout')"#
    );
}

#[test]
fn ui_search_staged_marker() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"status": 200}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "level = info");
    query(&mut b);
    assert!(!b.exists("#s-staged"), "查询后仍有标记");
    add(&mut b, "status >= 500");
    assert!(b.exists("#s-staged"), "加入条件后没有标记");
    assert_eq!(
        chips(&mut b),
        vec![
            ("level = info".into(), false),
            ("status >= 500".into(), true)
        ]
    );
    query(&mut b);
    assert!(!b.exists("#s-staged"), "查询后标记未消失");
    assert!(chips(&mut b).iter().all(|(_, staged)| !staged));
}

#[test]
fn ui_search_input_hints() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"level": "info"}));
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    b.fill("s-cond-input", "lev");
    assert!(b.wait_until("document.querySelector('#s-hints') !== null", WAIT));
    let v = b.eval("[...document.querySelectorAll('#s-hints li')].map(l => l.innerText)");
    let hints: Vec<String> = serde_json::from_value(v).unwrap();
    assert!(hints.contains(&"level".into()), "{hints:?}");
    assert_eq!(
        hints.last().map(String::as_str),
        Some("按全文检索"),
        "{hints:?}"
    );
}

// ---------------------------------------------------------------------------
// condition-sql — 条件与 SQL
// ---------------------------------------------------------------------------

#[test]
fn ui_search_sql_gen_quoting() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "a\"b = 1");
    add(&mut b, "x");
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE "a""b" = 1 AND match_all('x')"#
    );
}

#[test]
fn ui_search_literal_rules() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    for t in [
        "code = \"404\"",
        "n = -3.5",
        "ok = true",
        "name = O'Brien",
        "p LIKE \"a%\"",
    ] {
        add(&mut b, t);
    }
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE "code" = '404' AND "n" = -3.5 AND "ok" = true AND "name" = 'O''Brien' AND "p" LIKE '"a%"'"#
    );
}

#[test]
fn ui_search_to_sql_includes_staged() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"status": 500}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "status >= 500");
    query(&mut b);
    add(&mut b, "level = error");
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE "status" >= 500 AND "level" = 'error'"#
    );
}

#[test]
fn ui_search_to_condition_confirm() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "status >= 500");
    to_sql(&mut b);
    b.fill("s-sql", "SELECT host FROM \"app\"");
    b.click("s-mode-cond");
    assert!(b.wait_until("document.getElementById('s-confirm') !== null", WAIT));
    assert!(b.text("s-confirm").contains("切回条件会放弃对 SQL 的修改"));
    b.click("s-confirm-stay");
    assert!(b.wait_until("document.getElementById('s-confirm') === null", WAIT));
    assert_eq!(b.value("s-sql"), "SELECT host FROM \"app\"");
    b.click("s-mode-cond");
    assert!(b.wait_until("document.getElementById('s-confirm') !== null", WAIT));
    b.click("s-confirm-discard");
    assert!(b.wait_until("document.getElementById('s-cond-input') !== null", WAIT));
    assert_eq!(chips(&mut b), vec![("status >= 500".into(), true)]);
}

// ---------------------------------------------------------------------------
// query — 查询
// ---------------------------------------------------------------------------

#[test]
fn ui_search_sql_mode_query() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"host": "h1"}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    to_sql(&mut b);
    let sql = "SELECT host, count(*) AS n FROM \"app\" GROUP BY host";
    b.fill("s-sql", sql);
    b.click("s-query");
    let q = query_of(&wait_requests(&mut b, 1, is_search).remove(0));
    assert_eq!(q["sql"], sql);
    assert_eq!(q["size"], 100);
    assert_eq!(q["from"], 0);
}

#[test]
fn ui_search_fulltext_narrow_24h() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    fill_range(&mut b, "app", "2026-10-10 00:00", "2026-10-16 00:00");
    add(&mut b, "timeout");
    assert_eq!(b.value("s-end"), "2026-10-16 00:00:00.000000");
    assert_eq!(b.value("s-start"), "2026-10-15 00:00:00.000000");
}

#[test]
fn ui_search_fulltext_limit_7d() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "timeout");
    b.select("s-quick", &(30 * 24 * HOUR_US).to_string());
    assert!(b.wait_until("document.getElementById('s-query').disabled === true", WAIT));
    assert!(
        b.wait_text("search", LIMIT_TEXT, WAIT),
        "{}",
        b.text("search")
    );
}

#[test]
fn ui_search_sql_fulltext_note() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"m": "timeout"}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.select("s-quick", &(30 * 24 * HOUR_US).to_string());
    to_sql(&mut b);
    let sql = "SELECT * FROM \"app\" WHERE MATCH_ALL('timeout')";
    b.fill("s-sql", sql);
    b.click("s-query");
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(sql_of(&r), sql);
    assert!(
        b.wait_text("search", SQL_NOTE, WAIT),
        "{}",
        b.text("search")
    );
}

#[test]
fn ui_search_query_not_started() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    query_done(&mut b);
    assert_eq!(history_items(&mut b).len(), 1, "前提：查询历史中只有一项");
    let n = b.requests_besides_ui().len();
    b.fill("s-start", "2026/10/16 8点");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "时刻格式无效", WAIT),
        "{}",
        b.text("s-result")
    );
    std::thread::sleep(Duration::from_millis(500));
    let after = b.requests_besides_ui();
    assert_eq!(after.len(), n, "发出了请求：{:?}", &after[n..]);
    assert_eq!(history_items(&mut b).len(), 1);
}

// ---------------------------------------------------------------------------
// histogram — 分布图
// ---------------------------------------------------------------------------

#[test]
fn ui_search_histogram_request() {
    let (_fx, s) = start(&[FUTURE]);
    draw_data(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    fill_range(&mut b, "app", "2026-10-30 08:00", "2026-10-30 09:00");
    add(&mut b, "status >= 500");
    b.click("s-query");
    let h = wait_requests(&mut b, 1, is_histogram).remove(0);
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    let q = query_of(&h);
    assert_eq!(
        q["sql"],
        r#"SELECT histogram(_timestamp, '1m') AS t, count(*) AS n FROM "app" WHERE "status" >= 500 GROUP BY histogram(_timestamp, '1m') ORDER BY t"#
    );
    assert_eq!(range_of(&h), range_of(&r));
    assert_eq!(q["size"], 1000);
    assert_eq!(q["timezone"], "UTC");
}

#[test]
fn ui_search_histogram_interval_timezone() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "Asia/Kolkata", |_| {});
    fill_range(&mut b, "app", "2026-10-16 00:00", "2026-10-20 04:00");
    b.click("s-query");
    let h = wait_requests(&mut b, 1, is_histogram).remove(0);
    assert!(
        sql_of(&h).starts_with("SELECT histogram(_timestamp, '1h')"),
        "{}",
        sql_of(&h)
    );
}

#[test]
fn ui_search_histogram_interval_dst_fall_back() {
    let (_fx, s) = start(&[FUTURE]);
    ingest_at(&s, "app", 1_793_160_000_000_000 + HOUR_US, json!({"a": 1}));
    wait_visible(&s, "SELECT * FROM \"app\"", (0, common::api::FAR_END_US), 1);
    let mut b = open_in(&s, "America/New_York", |_| {});
    fill_range(&mut b, "app", "2026-10-28 00:00", "2026-11-01 04:00");
    b.click("s-query");
    let h = wait_requests(&mut b, 1, is_histogram).remove(0);
    assert!(
        sql_of(&h).starts_with("SELECT histogram(_timestamp, '3h')"),
        "{}",
        sql_of(&h)
    );
    wait_bars(&mut b, 34);
}

#[test]
fn ui_search_histogram_draw() {
    let (_fx, s) = start(&[FUTURE]);
    let mut b = drawn(&s);
    assert_eq!(b.text("s-per-bar"), "每柱 1m");
    assert!(
        b.wait_text("s-total", "共 4 条", WAIT),
        "{}",
        b.text("s-total")
    );
}

#[test]
fn ui_search_histogram_tooltip_timezone() {
    let (_fx, s) = start(&[FUTURE]);
    draw_data(&s);
    let mut b = open_in(&s, "Asia/Shanghai", |_| {});
    fill_range(&mut b, "app", "2026-10-30 16:00", "2026-10-30 17:00");
    query(&mut b);
    wait_bars(&mut b, 60);
    for (_, title) in bars(&mut b) {
        let want = if title.starts_with("2026-10-30 16:15:00.000000") {
            "3"
        } else if title.starts_with("2026-10-30 16:40:00.000000") {
            "1"
        } else {
            "0"
        };
        let n = title.split_whitespace().nth(2).unwrap_or_default();
        assert_eq!(n, want, "{title}");
    }
}

#[test]
fn ui_search_histogram_zoom() {
    let (_fx, s) = start(&[FUTURE]);
    let mut b = drawn(&s);
    b.click_sel("#s-hist .hbar[data-start='1793348100000000']");
    let r = wait_requests(&mut b, 2, is_search).remove(1);
    let h = wait_requests(&mut b, 2, is_histogram).remove(1);
    let want = (1_793_348_100_000_000, 1_793_348_160_000_000);
    assert_eq!(range_of(&r), want);
    assert_eq!(range_of(&h), want);
}

#[test]
fn ui_search_histogram_collapse() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.click("s-hist-toggle");
    search_stream(&mut b, "app");
    wait_requests(&mut b, 1, is_count);
    b.reopen(&url(&s));
    fill_token(&mut b, TOKEN);
    assert_eq!(b.text("s-hist-toggle"), "显示分布图");
    search_stream(&mut b, "app");
    let counts = wait_requests(&mut b, 2, is_count);
    for c in &counts {
        assert_eq!(sql_of(c), "SELECT count(*) AS n FROM \"app\"");
    }
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_histogram).len(), 0);
    assert!(!b.exists("#s-hist"), "收起后分布图区仍占位");
}

#[test]
fn ui_search_histogram_429() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |b| {
        answer_error(b, is_histogram, 429, "too_many_queries")
    });
    search_stream(&mut b, "app");
    let h = wait_requests(&mut b, 1, is_histogram).remove(0);
    assert!(b.wait_finished(&h, WAIT));
    assert!(
        b.wait_text("s-hist-toggle", "重画分布图", WAIT),
        "{}",
        b.text("s-hist-toggle")
    );
    assert!(!b.exists("#s-hist"), "分布图区仍占位");
    assert_eq!(row_count(&mut b), 1);
    assert!(!b.text("s-result").contains(BUSY));
    assert!(!disabled(&mut b, "s-query"));
}

#[test]
fn ui_search_histogram_sql_mode() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"host": "h1"}));
    let mut b = open_in(&s, "UTC", |_| {});
    let before = data_requests(&mut b).len();
    search_sql(&mut b, "SELECT host FROM \"app\"");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), before + 1);
    assert_eq!(b.text("s-hist-toggle"), "手写 SQL 不画分布图");
    assert!(disabled(&mut b, "s-hist-toggle"));
    assert_eq!(b.text("s-total"), "共 — 条");
}

#[test]
fn ui_search_histogram_timezone_change() {
    let (_fx, s) = start(&[FUTURE]);
    let mut b = drawn(&s);
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    b.select("tz", "Asia/Shanghai");
    let h = wait_requests(&mut b, 2, is_histogram).remove(1);
    assert_eq!(query_of(&h)["timezone"], "Asia/Shanghai");
    assert_eq!(range_of(&h), range_of(&r));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_search).len(), 1);
}

#[test]
fn ui_search_histogram_cleared_on_invalid_time() {
    let (_fx, s) = start(&[FUTURE]);
    let mut b = drawn(&s);
    b.fill("s-start", "2026/10/30 8点");
    b.click("s-query");
    assert!(
        b.wait_text("s-result", "时刻格式无效", WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(bars(&mut b).is_empty());
    assert_eq!(b.text("s-total"), "共 — 条");
}

// ---------------------------------------------------------------------------
// total — 总条数与结果信息
// ---------------------------------------------------------------------------

#[test]
fn ui_search_total_unknown_on_failure() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |b| {
        answer_error(b, is_histogram, 500, "internal")
    });
    search_stream(&mut b, "app");
    let h = wait_requests(&mut b, 1, is_histogram).remove(0);
    assert!(b.wait_finished(&h, WAIT));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(b.text("s-total"), "共 — 条");
}

#[test]
fn ui_search_count_429_total_only() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |b| {
        answer_error(b, is_count, 429, "too_many_queries")
    });
    screen(&mut b, "search");
    b.click("s-hist-toggle");
    search_stream(&mut b, "app");
    let c = wait_requests(&mut b, 1, is_count).remove(0);
    assert!(b.wait_finished(&c, WAIT));
    std::thread::sleep(Duration::from_millis(300));
    assert_eq!(b.text("s-total"), "共 — 条");
    assert_eq!(row_count(&mut b), 1);
    assert!(!b.text("s-result").contains(BUSY));
    assert!(!disabled(&mut b, "s-query"));
}

/// 摄取 150 条 `_timestamp` 在最近一小时内的记录，等它们都可查。
fn ingest_150(s: &Server) {
    let now = now_us();
    let recs: Vec<Value> = (0..150)
        .map(|i| json!({"_timestamp": common::gov::rfc3339(now - (i + 1) * 1_000_000), "i": i}))
        .collect();
    ingest_ok(s, "app", &Value::Array(recs).to_string());
    wait_visible(
        s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        150,
    );
}

#[test]
fn ui_search_info_line() {
    let (_fx, s) = start(&[]);
    ingest_150(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    b.click("s-next");
    let r = wait_requests(&mut b, 2, is_search).remove(1);
    assert!(b.wait_finished(&r, WAIT));
    assert!(
        b.wait_text("s-info", "第 101～150 行", WAIT),
        "{}",
        b.text("s-info")
    );
    // 同一请求由测试直接再发一次：扫描量相同
    let resp = s.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", common::BEARER)],
        r.body.as_deref().unwrap().as_bytes(),
    );
    let v = resp.json();
    let info = b.text("s-info");
    assert!(info.contains("耗时 "), "{info}");
    for (k, unit) in [
        ("scan_files", " 个文件"),
        ("scan_bytes", " 字节"),
        ("scan_records", " 条记录"),
    ] {
        assert!(
            info.contains(&format!("{}{unit}", v[k])),
            "{k}={}：{info}",
            v[k]
        );
    }
    assert!(info.contains(" ms"), "{info}");
}

// ---------------------------------------------------------------------------
// table — 结果表
// ---------------------------------------------------------------------------

#[test]
fn ui_search_table_columns() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1},{"b":2}]"#);
    let now = now_us();
    wait_visible(
        &s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        2,
    );
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    let cols = columns(&mut b);
    assert_eq!(cols[0], "_timestamp", "{cols:?}");
    let mut rest = cols[1..].to_vec();
    rest.sort();
    assert_eq!(rest, vec!["a", "b"]);
    let ia = cols.iter().position(|c| c == "a").unwrap();
    let ib = cols.iter().position(|c| c == "b").unwrap();
    let rows = b.table("s-result");
    let cells: Vec<(String, String)> = rows[1..]
        .iter()
        .map(|r| (r[ia].clone(), r[ib].clone()))
        .collect();
    assert!(cells.contains(&("1".into(), "".into())), "{cells:?}");
    assert!(cells.contains(&("".into(), "2".into())), "{cells:?}");
}

/// 结果表第一行中列 `col` 的格的文字。
fn first_cell(b: &mut Browser, col: &str) -> String {
    let i = columns(b)
        .iter()
        .position(|c| c == col)
        .unwrap_or_else(|| panic!("没有 {col} 列"));
    b.table("s-result")[1][i].clone()
}

#[test]
fn ui_search_system_time_columns() {
    let fx = common::Fixture::new();
    let rx = common::webhook::Receiver::http();
    let clock = common::alerting::Clock::before_minute(Duration::from_secs(20));
    let rules = common::alerting::rules(&[common::alerting::RuleToml::new("r1", &rx.url("/hook"))]);
    let mut cmd = fx.serve(&[("alerts.rules", Some(rules.as_str()))]);
    clock.apply(&mut cmd);
    let s = Server::start(cmd);
    common::alerting::has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    common::alerting::wait_tick(&s, clock, "r1", t);
    let now = now_us();
    let wide = (now - 2 * HOUR_US, now + 2 * HOUR_US);
    let (_, ok) = common::api::poll(
        WAIT,
        || {
            query_ok(
                &s,
                "SELECT window_end FROM _reconciliation WHERE dataset = 'default/app'",
                wide,
            )
        },
        |h| !h.is_empty(),
    );
    assert!(ok, "对账表没有 default/app 的行");
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-start", &utc_text(wide.0));
    b.fill("s-end", &utc_text(wide.1));
    let time = |v: &str| {
        v.len() == 26
            && v.bytes().enumerate().all(|(i, c)| match i {
                4 | 7 => c == b'-',
                10 => c == b' ',
                13 | 16 => c == b':',
                19 => c == b'.',
                _ => c.is_ascii_digit(),
            })
    };
    search_sql(
        &mut b,
        "SELECT scheduled_at, attempts FROM _alert_evaluations",
    );
    let at = first_cell(&mut b, "scheduled_at");
    assert!(time(&at), "scheduled_at：{at}");
    let attempts = first_cell(&mut b, "attempts");
    assert!(
        !attempts.is_empty() && attempts.bytes().all(|c| c.is_ascii_digit()),
        "attempts：{attempts}"
    );
    search_sql(
        &mut b,
        "SELECT window_end FROM _reconciliation WHERE dataset = 'default/app'",
    );
    let end = first_cell(&mut b, "window_end");
    assert!(time(&end), "window_end：{end}");
}

#[test]
fn ui_search_sort_ascending() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"a": 1}));
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    let before = requests_where(&mut b, is_ancillary).len();
    b.click("s-sort");
    let r = wait_requests(&mut b, 2, is_search).remove(1);
    assert_eq!(sql_of(&r), "SELECT * FROM \"app\" ORDER BY _timestamp ASC");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_ancillary).len(), before);
}

#[test]
fn ui_search_highlight_ascii_fold() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"m": "TimeOut at É and é"}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "timeout");
    add(&mut b, "é");
    query(&mut b);
    let v = b.eval("[...document.querySelectorAll('#s-result td mark')].map(m => m.innerText)");
    let marks: Vec<String> = serde_json::from_value(v).unwrap();
    assert_eq!(marks, vec!["TimeOut", "é"]);
}

#[test]
fn ui_search_hide_column() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"host": "h1"}));
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    assert!(columns(&mut b).contains(&"host".into()));
    b.click_sel("#s-result th[data-col='host'] .hide");
    assert!(b.wait_until(
        "document.querySelector(\"#s-result th[data-col='host']\") === null",
        WAIT
    ));
    b.reopen(&url(&s));
    fill_token(&mut b, TOKEN);
    search_stream(&mut b, "app");
    assert!(columns(&mut b).contains(&"host".into()));
}

#[test]
fn ui_search_paging() {
    let (_fx, s) = start(&[]);
    ingest_150(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    wait_requests(&mut b, 1, is_histogram);
    let before = data_requests(&mut b).len();
    b.click("s-next");
    let r = wait_requests(&mut b, 2, is_search).remove(1);
    assert_eq!(query_of(&r)["from"], 100);
    assert!(b.wait_finished(&r, WAIT));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), before + 1);
    assert!(b.wait_until("document.getElementById('s-next').disabled === true", WAIT));
    assert!(!disabled(&mut b, "s-prev"));
}

// ---------------------------------------------------------------------------
// fields — 字段栏与取值提示（V2 I 组；元素 id：`#s-fields` 字段栏、`#s-fields-filter` 筛选框、`#s-fields-toggle` 收起、
// `#s-fields-show` 显示、`#s-fields-error` 字段列表请求失败；`.fgroup[data-group]` 组（「记录字段」组为空串）、
// `.fgroup-head`（`aria-expanded`）、`.fcount` 字段数、`.field[data-field]` 字段、`.ftype` 类型标记、`.fname` 字段名（点它
// 展开取值，不能展开时为 disabled）、`.fvalue` 取值行（`.fv-text`、`.fv-n`、`.fv-eq`「=」、`.fv-ne`「≠」）、
// `.fvalues-error` 取值请求失败）。字段列表请求按方法 `GET` 与路径以 `/_fields` 结尾区分。
// ---------------------------------------------------------------------------

/// 字段列表请求（REQ-AMB-UI-SEARCH-V2-FIELDS-REQUEST）。
fn is_fields(r: &Request) -> bool {
    r.method == "GET" && r.path().ends_with("/_fields")
}

/// 对字段 `f` 的取值请求（REQ-AMB-UI-SEARCH-V2-VALUES-REQUEST）。
fn is_values_of(r: &Request, f: &str) -> bool {
    sql_of(r).starts_with(&format!("SELECT \"{f}\", count(*)"))
}

fn is_values(r: &Request) -> bool {
    sql_of(r).contains(", count(*) AS n__ FROM ")
}

/// 到此刻为止的字段列表请求。
fn fields_requests(b: &mut Browser) -> Vec<Request> {
    b.requests().into_iter().filter(is_fields).collect()
}

/// 等字段列表请求累计达到 `n` 个，返回全部。
fn wait_fields_requests(b: &mut Browser, n: usize) -> Vec<Request> {
    let deadline = std::time::Instant::now() + WAIT;
    loop {
        let rs = fields_requests(b);
        if rs.len() >= n {
            return rs;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "{WAIT:?} 内字段列表请求只有 {} 个，要 {n} 个",
            rs.len()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// 在数据集输入框选定 `stream`（填入即从候选中选定）。
fn choose(b: &mut Browser, stream: &str) {
    screen(b, "search");
    b.fill("s-stream", stream);
}

/// 等字段栏列出字段 `f`。
fn wait_field(b: &mut Browser, f: &str) {
    let ok = b.wait_until(
        &format!("document.querySelector({}) !== null", json!(field_sel(f))),
        WAIT,
    );
    assert!(ok, "字段栏未列出 {f}：{}", b.text("s-fields"));
}

fn field_sel(f: &str) -> String {
    format!("#s-fields .field[data-field={}]", json!(f))
}

/// 字段栏列出的字段名（按显示的先后）。
fn field_names(b: &mut Browser) -> Vec<String> {
    let v = b.eval("[...document.querySelectorAll('#s-fields .field')].map(e => e.dataset.field)");
    serde_json::from_value(v).unwrap()
}

/// 字段 `f` 的类型标记。
fn type_mark(b: &mut Browser, f: &str) -> String {
    b.eval(&format!(
        "document.querySelector({}).querySelector('.ftype').innerText",
        json!(field_sel(f))
    ))
    .as_str()
    .unwrap()
    .to_owned()
}

/// 各组：组名、显示的字段数、是否展开、列出的字段名。
fn field_groups(b: &mut Browser) -> Vec<(String, String, bool, Vec<String>)> {
    let v = b.eval(
        "[...document.querySelectorAll('#s-fields .fgroup')].map(g => [g.dataset.group, \
         g.querySelector('.fcount').innerText, g.querySelector('.fgroup-head').getAttribute('aria-expanded') === 'true', \
         [...g.querySelectorAll('.field')].map(e => e.dataset.field)])",
    );
    serde_json::from_value(v).unwrap()
}

/// 字段 `f` 此刻能不能展开取值。
fn expandable(b: &mut Browser, f: &str) -> bool {
    b.eval(&format!(
        "document.querySelector({}).querySelector('.fname').disabled === false",
        json!(field_sel(f))
    )) == json!(true)
}

/// 点字段 `f` 的名字（展开或收起取值）。
fn toggle_field(b: &mut Browser, f: &str) {
    b.eval(&format!(
        "document.querySelector({}).querySelector('.fname').click()",
        json!(field_sel(f))
    ));
}

/// 字段 `f` 下列出的取值与条数。
fn values_of(b: &mut Browser, f: &str) -> Vec<(String, String)> {
    let v = b.eval(&format!(
        "[...document.querySelector({}).querySelectorAll('.fvalue')].map(e => \
         [e.querySelector('.fv-text').innerText, e.querySelector('.fv-n').innerText])",
        json!(field_sel(f))
    ));
    serde_json::from_value(v).unwrap()
}

/// 等字段 `f` 下列出取值。
fn wait_values(b: &mut Browser, f: &str) {
    let ok = b.wait_until(
        &format!(
            "document.querySelector({}).querySelector('.fvalue') !== null",
            json!(field_sel(f))
        ),
        WAIT,
    );
    assert!(ok, "{f} 下未列出取值：{}", b.text("s-fields"));
}

/// 输入提示的各项。
fn hints(b: &mut Browser) -> Vec<String> {
    assert!(b.wait_until("document.querySelector('#s-hints') !== null", WAIT));
    let v = b.eval("[...document.querySelectorAll('#s-hints li')].map(l => l.innerText)");
    serde_json::from_value(v).unwrap()
}

/// 以 HTTP `status` 与 `reason` 的错误信封改答满足 `pred` 的字段列表请求。
fn answer_fields_error(b: &mut Browser, pred: fn(&Request) -> bool, status: u16, reason: &str) {
    let envelope =
        json!({"code": status, "reason": reason, "message": format!("injected {reason}")});
    b.answer_where(
        move |r| is_fields(r) && pred(r),
        status,
        &[("Content-Type", "application/json")],
        &envelope.to_string(),
    );
}

/// 场景「展开字段时按已生效的条件统计取值」的数据：`level`／`status` 为 error／502 的 3 条、warn／503 的 1 条、
/// info／200 的 2 条（接收时刻）；`host` 为真时每条另有 `host`。
fn level_data(s: &Server, host: bool) {
    let mut records = Vec::new();
    for (level, status, n) in [("error", 502, 3), ("warn", 503, 1), ("info", 200, 2)] {
        for i in 0..n {
            let mut r = json!({"level": level, "status": status});
            if host {
                r["host"] = json!(format!("h{i}"));
            }
            records.push(r);
        }
    }
    ingest_ok(s, "app", &Value::Array(records).to_string());
    let now = now_us();
    wait_visible(
        s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        6,
    );
}

/// 场景「展开字段时按已生效的条件统计取值」：以 `app` 与条件 `status >= 500` 查询，展开 `level`，等它列出取值。
/// 返回检索请求与取值请求。
fn values_shown(s: &Server) -> (Browser, Request, Request) {
    level_data(s, false);
    let mut b = open_in(s, "UTC", |_| {});
    choose(&mut b, "app");
    add(&mut b, "status >= 500");
    query(&mut b);
    wait_field(&mut b, "level");
    toggle_field(&mut b, "level");
    let v = wait_requests(&mut b, 1, |r| is_values_of(r, "level")).remove(0);
    wait_values(&mut b, "level");
    let r = requests_where(&mut b, is_search).pop().unwrap();
    (b, r, v)
}

#[test]
fn ui_search_fields_request_triggers() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"level": "error"}));
    ingest_now(&s, "web", json!({"path": "/x"}));
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "app");
    wait_field(&mut b, "level");
    choose(&mut b, "web");
    wait_field(&mut b, "path");
    choose(&mut b, "web");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(fields_requests(&mut b).len(), 2, "再选定 web 时发出了请求");
    b.click("s-query");
    wait_settled(&mut b, "s");
    let rs = wait_fields_requests(&mut b, 3);
    let paths: Vec<&str> = rs.iter().map(Request::path).collect();
    assert_eq!(
        paths,
        [
            "/api/default/app/_fields",
            "/api/default/web/_fields",
            "/api/default/web/_fields"
        ]
    );
    for r in &rs {
        assert_eq!(r.header("Authorization"), Some("Bearer s3cret"));
    }
    // 第三个在点「查询」之后发出：在检索请求之后
    let all = b.requests();
    let at = |id: &str| all.iter().position(|r| r.id == id).unwrap();
    let search = requests_where(&mut b, is_search).remove(0);
    assert!(at(&rs[1].id) < at(&search.id) && at(&search.id) < at(&rs[2].id));
}

#[test]
fn ui_search_fields_retry_after_token() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"level": "error"}));
    let mut b = open(&s);
    wait_candidates(&mut b);
    choose(&mut b, "app");
    let shown = b.wait_until(
        "document.getElementById('s-fields-error')?.innerText.includes('401') === true",
        WAIT,
    );
    assert!(shown, "{}", b.text("s-fields"));
    assert!(b.text("s-fields-error").contains("无法读取字段列表"));
    assert_eq!(fields_requests(&mut b).len(), 1);
    fill_token(&mut b, TOKEN);
    let r = wait_fields_requests(&mut b, 2).remove(1);
    assert_eq!(r.path(), "/api/default/app/_fields");
    wait_field(&mut b, "level");
}

#[test]
fn ui_search_fields_table_dataset_change() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"code": "404"}));
    ingest_now(&s, "web", json!({"code": "x"}));
    let mut b = open_in(&s, "UTC", |b| {
        answer_fields_error(
            b,
            |r| r.path() == "/api/default/web/_fields",
            500,
            "internal",
        )
    });
    choose(&mut b, "app");
    wait_field(&mut b, "code");
    choose(&mut b, "web");
    let r = wait_fields_requests(&mut b, 2).remove(1);
    assert!(b.wait_finished(&r, WAIT));
    add(&mut b, "code = 404");
    assert_eq!(to_sql(&mut b), r#"SELECT * FROM "web" WHERE "code" = 404"#);
}

#[test]
fn ui_search_fields_table_kept_on_query() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "typed", json!({"code": "404"}));
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "typed");
    wait_field(&mut b, "code");
    add(&mut b, "code = 404");
    query(&mut b);
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert_eq!(
        query_of(&r)["sql"],
        json!(r#"SELECT * FROM "typed" WHERE "code" = '404'"#)
    );
    assert_eq!(row_count(&mut b), 1, "{}", b.text("s-result"));
}

#[test]
fn ui_search_fields_bar_types() {
    let (_fx, s) = start(&[]);
    ingest_ok(
        &s,
        "app",
        &json!([{"level": "error", "status": 502, "lat": 0.5, "ok": true}]).to_string(),
    );
    ingest_ok(&s, "app", &json!([{"status": "bad"}]).to_string());
    let now = now_us();
    wait_visible(
        &s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        2,
    );
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "app");
    wait_field(&mut b, "status__utf8");
    assert_eq!(
        field_names(&mut b),
        ["_timestamp", "lat", "level", "ok", "status", "status__utf8"]
    );
    let marks: Vec<String> = ["_timestamp", "status", "lat", "ok", "level"]
        .iter()
        .map(|f| type_mark(&mut b, f))
        .collect();
    let distinct: std::collections::BTreeSet<&String> = marks.iter().collect();
    assert_eq!(distinct.len(), 5, "{marks:?}");
    assert_eq!(
        type_mark(&mut b, "status__utf8"),
        type_mark(&mut b, "level")
    );
}

/// 场景「字段栏按前缀分组」：选定 `app`，等字段栏列出字段。
fn grouped_fields(s: &Server) -> Browser {
    ingest_now(
        s,
        "app",
        json!({"message": "m", "level": "error", "b": 1, "k8s": {"pod": "p1"},
               "http": {"status": 502, "path": "/x"}}),
    );
    let mut b = open_in(s, "UTC", |_| {});
    choose(&mut b, "app");
    wait_field(&mut b, "level");
    b
}

#[test]
fn ui_search_fields_groups() {
    let (_fx, s) = start(&[]);
    let mut b = grouped_fields(&s);
    let groups: Vec<(String, String, bool)> = field_groups(&mut b)
        .into_iter()
        .map(|(g, n, open, _)| (g, n, open))
        .collect();
    assert_eq!(
        groups,
        [
            ("".into(), "4".into(), true),
            ("http".into(), "2".into(), false),
            ("k8s".into(), "1".into(), false)
        ]
    );
    // 收起的组不列出字段：先展开 http 再看组内顺序
    let record = field_groups(&mut b).remove(0).3;
    assert_eq!(record, ["_timestamp", "message", "b", "level"]);
    b.click_sel("#s-fields .fgroup[data-group=http] .fgroup-head");
    assert!(b.wait_until(
        "document.querySelector('#s-fields .fgroup[data-group=http] .field') !== null",
        WAIT
    ));
    assert_eq!(field_groups(&mut b)[1].3, ["http.path", "http.status"]);
}

#[test]
fn ui_search_fields_filter() {
    let (_fx, s) = start(&[]);
    let mut b = grouped_fields(&s);
    b.fill("s-fields-filter", "STAT");
    assert!(b.wait_until(
        "document.querySelectorAll('#s-fields .fgroup').length === 1",
        WAIT
    ));
    assert_eq!(
        field_groups(&mut b),
        [("http".into(), "1".into(), true, vec!["http.status".into()])]
    );
}

#[test]
fn ui_search_fields_collapse_remembered() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    assert!(b.exists("#s-fields"));
    b.click("s-fields-toggle");
    assert!(b.wait_until("document.getElementById('s-fields') === null", WAIT));
    b.reopen(&url(&s));
    screen(&mut b, "search");
    assert!(b.wait_until("document.getElementById('s-fields-show') !== null", WAIT));
    assert!(!b.exists("#s-fields"));
}

#[test]
fn ui_search_fields_fail_bar_only() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"level": "error"}));
    let mut b = open_in(&s, "UTC", |b| {
        answer_fields_error(b, |_| true, 503, "unavailable")
    });
    search_stream(&mut b, "app");
    let shown = b.wait_until(
        "document.getElementById('s-fields-error')?.innerText.includes('unavailable') === true",
        WAIT,
    );
    assert!(shown, "{}", b.text("s-fields"));
    let r = wait_requests(&mut b, 1, is_search).remove(0);
    assert!(b.wait_finished(&r, WAIT));
    let err = b.text("s-fields-error");
    assert!(
        err.contains("无法读取字段列表") && err.contains("503"),
        "{err}"
    );
    assert_eq!(row_count(&mut b), 1);
    let result = b.text("s-result");
    assert!(
        !result.contains("无法读取") && !result.contains("503"),
        "{result}"
    );
    assert!(!disabled(&mut b, "s-query"));
}

#[test]
fn ui_search_values_request_sql() {
    let (_fx, s) = start(&[]);
    let (_b, r, v) = values_shown(&s);
    assert_eq!(
        query_of(&v)["sql"],
        json!(
            r#"SELECT "level", count(*) AS n__ FROM "app" WHERE "status" >= 500 AND "level" IS NOT NULL GROUP BY "level" ORDER BY n__ DESC LIMIT 10"#
        )
    );
    assert_eq!(range_of(&v), range_of(&r));
    assert_eq!(query_of(&v)["size"], 10);
}

#[test]
fn ui_search_values_request_no_conditions() {
    let (_fx, s) = start(&[]);
    level_data(&s, false);
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    wait_field(&mut b, "level");
    toggle_field(&mut b, "level");
    let v = wait_requests(&mut b, 1, |r| is_values_of(r, "level")).remove(0);
    assert_eq!(
        query_of(&v)["sql"],
        json!(
            r#"SELECT "level", count(*) AS n__ FROM "app" WHERE "level" IS NOT NULL GROUP BY "level" ORDER BY n__ DESC LIMIT 10"#
        )
    );
}

#[test]
fn ui_search_values_sql_mode_disabled() {
    let (_fx, s) = start(&[]);
    level_data(&s, false);
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "app");
    wait_field(&mut b, "level");
    search_sql(&mut b, "SELECT * FROM \"app\"");
    assert!(!expandable(&mut b, "level"));
    toggle_field(&mut b, "level");
    std::thread::sleep(Duration::from_millis(500));
    assert!(requests_where(&mut b, |r| is_values_of(r, "level")).is_empty());
}

#[test]
fn ui_search_values_one_at_a_time() {
    let fx = common::Fixture::new();
    let gate = Gate::new(&fx, "query.after_admission");
    gate.release();
    let s = start_gated(&fx, &[], &gate);
    level_data(&s, true);
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    wait_field(&mut b, "host");
    gate.hold();
    toggle_field(&mut b, "level");
    wait_requests(&mut b, 1, |r| is_values_of(r, "level"));
    gate.wait_held(1);
    assert!(!expandable(&mut b, "host"));
    toggle_field(&mut b, "host");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_values).len(), 1);
    gate.release();
    wait_values(&mut b, "level");
    assert!(expandable(&mut b, "host"));
}

#[test]
fn ui_search_values_show() {
    let (_fx, s) = start(&[]);
    let (mut b, _, _) = values_shown(&s);
    assert_eq!(
        values_of(&mut b, "level"),
        [("error".into(), "3".into()), ("warn".into(), "1".into())]
    );
}

#[test]
fn ui_search_values_add_no_request() {
    let (_fx, s) = start(&[]);
    let (mut b, _, _) = values_shown(&s);
    let before = b.requests_besides_ui().len();
    b.eval(&format!(
        "[...document.querySelector({}).querySelectorAll('.fvalue')]\
         .find(e => e.querySelector('.fv-text').innerText === 'error').querySelector('.fv-ne').click()",
        json!(field_sel("level"))
    ));
    assert!(b.wait_until(
        "document.querySelectorAll('#s-conds .chip').length === 2",
        WAIT
    ));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(b.requests_besides_ui().len(), before, "点「≠」时发出了请求");
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "app" WHERE "status" >= 500 AND "level" != 'error'"#
    );
}

#[test]
fn ui_search_values_memory_exceeded() {
    // 每条记录的 msg 各不相同：按 msg 分组要留住全部取值，检索请求与分布请求只留少量状态
    let fx = common::Fixture::new();
    // 40 万条分 20 批摄取：分组的计量用量随不同取值的个数增长，检索请求（前 100 行）的用量不随条数增长。
    // 实测：检索请求要 4～8 MiB，取值请求 20 万条时要 16～32 MiB、40 万条时要 32～64 MiB
    let bodies: Vec<String> = (0..20)
        .map(|k| {
            let records: Vec<Value> = (k * 20_000..(k + 1) * 20_000)
                .map(|i| json!({"msg": format!("m{i:07}-{:016x}", (i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15))}))
                .collect();
            Value::Array(records).to_string()
        })
        .collect();
    let batches: Vec<(&str, &str)> = bodies.iter().map(|b| ("app", b.as_str())).collect();
    common::gov::committed(&fx, &batches);
    let now = now_us();
    let range = (now - HOUR_US, now + MINUTE_US);
    // 检索屏发出的检索请求、分布请求与计数请求，`size` 与界面相同
    let screen = [
        ("SELECT * FROM \"app\"", 100),
        (
            "SELECT histogram(_timestamp, '1m') AS t, count(*) AS n FROM \"app\" \
             GROUP BY histogram(_timestamp, '1m') ORDER BY t",
            1000,
        ),
        ("SELECT count(*) AS n FROM \"app\"", 1),
    ];
    let values = "SELECT \"msg\", count(*) AS n__ FROM \"app\" WHERE \"msg\" IS NOT NULL \
                  GROUP BY \"msg\" ORDER BY n__ DESC LIMIT 10";
    // 上限 X：2 的整数次幂中、使上面三种请求各单独作答 5 次都回 200 的最小者（逐次加倍求得）
    let mut x: u64 = 1 << 16;
    let s = loop {
        let limit = x.to_string();
        let s = fx.start(&[("query.max_memory_bytes", Some(&limit))]);
        let ok = (0..5).all(|_| {
            screen.iter().all(|&(q, size)| {
                let body = json!({"query": {"sql": q, "start_time": range.0,
                                            "end_time": range.1, "size": size}});
                let r = s.request(
                    "POST",
                    "/api/default/_search",
                    &[("Authorization", common::BEARER)],
                    body.to_string().as_bytes(),
                );
                assert!(
                    r.status == 200 || r.json()["reason"] == "query_memory_exceeded",
                    "{q}：{}",
                    r.text()
                );
                r.status == 200
            })
        });
        if ok {
            break s;
        }
        drop(s);
        x *= 2;
        assert!(x <= 1 << 30, "上限加到 1 GiB 检索请求或分布请求仍越限");
    };
    let r = common::gov::query(&s, values, range);
    assert_eq!(
        r.json()["reason"],
        "query_memory_exceeded",
        "前提：上限 {x} 下取值请求越限：{}",
        r.text()
    );
    let mut b = open_in(&s, "UTC", |_| {});
    search_stream(&mut b, "app");
    wait_field(&mut b, "msg");
    toggle_field(&mut b, "msg");
    let ok = b.wait_until(
        &format!(
            "document.querySelector({}).querySelector('.fvalues-error') !== null",
            json!(field_sel("msg"))
        ),
        WAIT,
    );
    assert!(ok, "{}", b.text("s-fields"));
    assert_eq!(
        b.eval(&format!(
            "document.querySelector({}).querySelector('.fvalues-error').innerText",
            json!(field_sel("msg"))
        )),
        json!("取值太多，无法统计")
    );
    let result = b.text("s-result");
    assert!(
        !result.contains("内存") && !result.contains("取值太多"),
        "{result}"
    );
    assert_eq!(row_count(&mut b), 100);
    assert!(!disabled(&mut b, "s-query"));
}

#[test]
fn ui_search_values_reset_on_query() {
    let (_fx, s) = start(&[]);
    let (mut b, _, _) = values_shown(&s);
    b.click("s-query");
    wait_requests(&mut b, 2, is_search);
    wait_settled(&mut b, "s");
    assert!(!b.exists("#s-fields .fvalue"), "{}", b.text("s-fields"));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(requests_where(&mut b, is_values).len(), 1);
}

#[test]
fn ui_search_input_hints_field_table() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"level": "error"}));
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "app");
    wait_field(&mut b, "level");
    b.fill("s-cond-input", "lev");
    let h = hints(&mut b);
    assert!(h.contains(&"level".into()), "{h:?}");
    assert_eq!(h.last().map(String::as_str), Some("按全文检索"), "{h:?}");
}

#[test]
fn ui_search_input_hints_values() {
    let (_fx, s) = start(&[]);
    let (mut b, _, _) = values_shown(&s);
    b.fill("s-cond-input", "level = ");
    let h = hints(&mut b);
    assert!(
        h.contains(&"error".into()) && h.contains(&"warn".into()),
        "{h:?}"
    );
    assert_eq!(h.last().map(String::as_str), Some("按全文检索"), "{h:?}");
}

#[test]
fn ui_search_literal_typed() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "typed", json!({"code": "404", "n": 7}));
    let mut b = open_in(&s, "UTC", |_| {});
    choose(&mut b, "typed");
    wait_field(&mut b, "code");
    for t in ["code = 404", "n = 7", "code != \"5\""] {
        add(&mut b, t);
    }
    assert_eq!(
        to_sql(&mut b),
        r#"SELECT * FROM "typed" WHERE "code" = '404' AND "n" = 7 AND "code" != '5'"#
    );
}

// ---------------------------------------------------------------------------
// detail — 单条详情（元素 id：`#s-detail` 浮层、`#s-detail-pos`「第 i / n 行」、`#s-detail-time` 与 `#s-detail-utc`
// 两个时刻、`#s-detail-edge` 到头的提示、`#s-detail-tab-fields` 与 `#s-detail-tab-json` 两页、`#s-detail-filter` 筛选框、
// `#s-detail-fields tbody[data-group]` 各组（`message` 一项的组名为空）与其中的 `tr[data-key]`、`.add-eq`「= 加入」、
// `.add-ne`「≠ 排除」、`#s-detail-json` 原始 JSON、`#s-detail-close`「关闭」、`#s-detail-close-query`「关闭并查询」）
// ---------------------------------------------------------------------------

/// 查询 `stream` 并等分布请求收到应答，返回此时的数据请求数（之后再数请求，不受它们干扰）。
fn searched(b: &mut Browser, stream: &str) -> usize {
    search_stream(b, stream);
    let h = wait_requests(b, 1, is_histogram).remove(0);
    assert!(b.wait_finished(&h, WAIT), "分布请求未收到应答");
    data_requests(b).len()
}

/// 点结果表第 `n` 行（从 1 起）打开详情。
fn open_detail(b: &mut Browser, n: usize) {
    b.click_sel(&format!("#s-result tbody tr:nth-child({n})"));
    assert!(
        b.wait_until("document.getElementById('s-detail') !== null", WAIT),
        "详情未打开"
    );
}

/// 详情字段页的各组：组名与组内的键。
fn detail_groups(b: &mut Browser) -> Vec<(String, Vec<String>)> {
    let v = b.eval(
        "[...document.querySelectorAll('#s-detail-fields tbody')].map(g => \
         [g.dataset.group, [...g.querySelectorAll('tr[data-key]')].map(t => t.dataset.key)])",
    );
    serde_json::from_value(v).unwrap()
}

/// 点详情中字段 `key` 的按钮（`add-eq` 或 `add-ne`）。
fn detail_add(b: &mut Browser, key: &str, class: &str) {
    b.click_sel(&format!("#s-detail tr[data-key={}] .{class}", json!(key)));
}

/// `app` 中三条记录（`n` 为 1、2、3）。
fn three_records(s: &Server) {
    let now = now_us();
    for n in 1..=3 {
        ingest_ok(s, "app", &json!([{"n": n}]).to_string());
    }
    wait_visible(
        s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        3,
    );
}

/// 场景「字段按前缀分组并可筛选」的记录；`_timestamp` 取整分钟，界面上的时刻里不会出现筛选词 `502`。
fn grouped_record(s: &Server) {
    let now = now_us();
    let t = now - now.rem_euclid(MINUTE_US) - MINUTE_US;
    ingest_at(
        s,
        "app",
        t,
        json!({"message":"m","level":"error","http.status":502,"http.path":"/x","k8s.pod":"p1"}),
    );
    wait_visible(
        s,
        "SELECT * FROM \"app\"",
        (now - HOUR_US, now + MINUTE_US),
        1,
    );
}

#[test]
fn ui_search_detail_open() {
    let (_fx, s) = start(&[]);
    three_records(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    let n = searched(&mut b, "app");
    assert_eq!(row_count(&mut b), 3);
    open_detail(&mut b, 2);
    assert_eq!(b.text("s-detail-pos"), "第 2 / 3 行");
    std::thread::sleep(Duration::from_secs(1));
    assert_eq!(data_requests(&mut b).len(), n, "打开详情发出了请求");
}

#[test]
fn ui_search_detail_header() {
    let (_fx, s) = start(&[]);
    let t = 1_792_137_600_000_000;
    ingest_at(&s, "app", t, json!({"k": "v"}));
    wait_visible(&s, "SELECT * FROM \"app\"", (t, t + MINUTE_US), 1);
    let mut b = open_in(&s, "Asia/Shanghai", |_| {});
    fill_range(&mut b, "app", "2026-10-16 15:00", "2026-10-16 17:00");
    query(&mut b);
    open_detail(&mut b, 1);
    assert_eq!(b.text("s-detail-time"), "2026-10-16 16:00:00.000000");
    assert_eq!(b.text("s-detail-utc"), "2026-10-16T08:00:00.000000Z");
}

#[test]
fn ui_search_detail_groups() {
    let (_fx, s) = start(&[]);
    grouped_record(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    searched(&mut b, "app");
    open_detail(&mut b, 1);
    let groups = detail_groups(&mut b);
    let names: Vec<&str> = groups.iter().map(|(g, _)| g.as_str()).collect();
    assert_eq!(names, ["", "记录字段", "http", "k8s"], "{groups:?}");
    assert_eq!(groups[0].1, ["message"]);
    assert!(groups[1].1.contains(&"level".to_owned()), "{groups:?}");
    assert_eq!(groups[2].1, ["http.status", "http.path"]);
    assert_eq!(groups[3].1, ["k8s.pod"]);
    b.fill("s-detail-filter", "502");
    let shown: Vec<String> = detail_groups(&mut b)
        .into_iter()
        .flat_map(|(_, keys)| keys)
        .collect();
    assert_eq!(shown, ["http.status"]);
}

#[test]
fn ui_search_detail_json() {
    let (_fx, s) = start(&[]);
    grouped_record(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    searched(&mut b, "app");
    open_detail(&mut b, 1);
    b.click("s-detail-tab-json");
    assert!(b.wait_until("document.getElementById('s-detail-json') !== null", WAIT));
    let shown: Value = serde_json::from_str(&b.text("s-detail-json")).expect("原始 JSON 页是 JSON");
    let r = requests_where(&mut b, is_search).pop().unwrap();
    let hits = query_ok(&s, query_of(&r)["sql"].as_str().unwrap(), range_of(&r));
    assert_eq!(shown, hits[0]);
}

#[test]
fn ui_search_detail_add() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"code": "404", "status": 502}));
    let mut b = open_in(&s, "UTC", |_| {});
    let n = searched(&mut b, "app");
    open_detail(&mut b, 1);
    detail_add(&mut b, "code", "add-eq");
    detail_add(&mut b, "status", "add-ne");
    assert_eq!(chips(&mut b).len(), 2);
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "加入时发出了请求");
    assert_eq!(
        to_sql(&mut b),
        "SELECT * FROM \"app\" WHERE \"code\" = '404' AND \"status\" != 502"
    );
}

#[test]
fn ui_search_detail_add_sql_cursor() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"host": "h1"}));
    let mut b = open_in(&s, "UTC", |_| {});
    search_sql(&mut b, "SELECT * FROM \"app\"");
    let text = "SELECT * FROM \"app\" WHERE  LIMIT 5";
    b.fill("s-sql", text);
    let at = text.find("WHERE ").unwrap() + "WHERE ".len();
    b.eval(&format!(
        "(() => {{ const e = document.getElementById('s-sql'); e.focus(); e.setSelectionRange({at}, {at}); \
         e.dispatchEvent(new Event('select', {{bubbles: true}})); }})()"
    ));
    open_detail(&mut b, 1);
    detail_add(&mut b, "host", "add-eq");
    assert_eq!(
        b.value("s-sql"),
        "SELECT * FROM \"app\" WHERE \"host\" = 'h1' LIMIT 5"
    );
}

#[test]
fn ui_search_detail_close_and_query() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"code": "404", "status": 502}));
    let mut b = open_in(&s, "UTC", |_| {});
    searched(&mut b, "app");
    open_detail(&mut b, 1);
    detail_add(&mut b, "code", "add-eq");
    detail_add(&mut b, "status", "add-ne");
    assert_eq!(chips(&mut b).len(), 2);
    b.click("s-detail-close-query");
    assert!(b.wait_until("document.getElementById('s-detail') === null", WAIT));
    let r = wait_requests(&mut b, 2, is_search).remove(1);
    assert_eq!(
        query_of(&r)["sql"],
        json!("SELECT * FROM \"app\" WHERE \"code\" = '404' AND \"status\" != 502")
    );
}

#[test]
fn ui_search_detail_nav_stops() {
    let (_fx, s) = start(&[]);
    three_records(&s);
    let mut b = open_in(&s, "UTC", |_| {});
    let n = searched(&mut b, "app");
    assert_eq!(row_count(&mut b), 3);
    open_detail(&mut b, 3);
    b.press("s-detail", "ArrowDown");
    assert!(b.wait_until("document.getElementById('s-detail-edge') !== null", WAIT));
    assert_eq!(b.text("s-detail-pos"), "第 3 / 3 行");
    assert_eq!(
        b.text("s-detail-edge"),
        "已是本页最后一条，下一页请关闭后翻页"
    );
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "按 ↓ 发出了请求");
    b.press("s-detail", "Escape");
    assert!(b.wait_until("document.getElementById('s-detail') === null", WAIT));
}

// ---------------------------------------------------------------------------
// context — 上下文浏览（REQ-AMB-UI-SEARCH-V2 J 组；元素 id：详情里的 `#s-detail-context`「看前后记录」与 `#s-detail-keep`
// 「保留当前条件」，结果表之外的 `#s-context`（显示 t）、`#s-context-back`「返回检索结果」、`#s-context-few`（前后不足 20 条）、
// `#s-context-center-missing`（标不出中心行），中心行为结果表中带 `center` 类的 `tr`）
// ---------------------------------------------------------------------------

/// 2026-10-16T08:00:00Z。
const CTX_T0: i64 = 1_792_137_600_000_000;

/// stream `stream` 中 `n` 条记录：第 k 条为 `{"seq":k}`、`_timestamp` 为 2026-10-16T08:00:00Z 之后第 k 秒。
fn seq_records(s: &Server, stream: &str, n: i64) {
    let records: Vec<Value> = (0..n)
        .map(|k| json!({"seq": k, "_timestamp": common::gov::rfc3339(CTX_T0 + k * 1_000_000)}))
        .collect();
    ingest_ok(s, stream, &json!(records).to_string());
    wait_visible(
        s,
        &format!("SELECT * FROM \"{stream}\""),
        (CTX_T0, CTX_T0 + MINUTE_US),
        n as usize,
    );
}

/// 以数据集 `stream`、起止 `2026-10-16 08:00` 至 `08:01` 在条件模式下查询（`cond` 为要加的条件），等分布请求收到应答。
fn ctx_searched(b: &mut Browser, stream: &str, cond: Option<&str>) {
    fill_range(b, stream, "2026-10-16 08:00", "2026-10-16 08:01");
    if let Some(c) = cond {
        add(b, c);
    }
    query(b);
    let h = wait_requests(b, 1, is_histogram).pop().unwrap();
    assert!(b.wait_finished(&h, WAIT), "分布请求未收到应答");
}

/// 结果表各行 `seq` 列的文字。
fn seqs(b: &mut Browser) -> Vec<String> {
    let i = columns(b)
        .iter()
        .position(|c| c == "seq")
        .expect("有 seq 列");
    let v = b.eval(&format!(
        "[...document.querySelectorAll('#s-result tbody tr')].map(tr => tr.children[{i}].innerText)"
    ));
    serde_json::from_value(v).unwrap()
}

/// `seq` 依次为 `from` 到 `to`（含两端，可降可升）的文字。
fn seq_range(from: i64, to: i64) -> Vec<String> {
    if from >= to {
        (to..=from).rev().map(|k| k.to_string()).collect()
    } else {
        (from..=to).map(|k| k.to_string()).collect()
    }
}

/// 打开 `seq` 为 `k` 的行的详情。
fn open_seq(b: &mut Browser, k: i64) {
    let at = seqs(b)
        .iter()
        .position(|x| *x == k.to_string())
        .unwrap_or_else(|| panic!("结果表没有 seq 为 {k} 的行"));
    open_detail(b, at + 1);
}

/// 前段请求与后段请求（按请求体 `size` 认：前段 20、后段 21）。
fn is_before(r: &Request) -> bool {
    r.method == "POST"
        && r.body
            .as_deref()
            .and_then(|b| serde_json::from_str::<Value>(b).ok())
            .is_some_and(|v| v["query"]["size"] == 20)
}

fn is_after(r: &Request) -> bool {
    r.method == "POST"
        && r.body
            .as_deref()
            .and_then(|b| serde_json::from_str::<Value>(b).ok())
            .is_some_and(|v| v["query"]["size"] == 21)
}

/// 点「看前后记录」，等上下文结果显示出来。
fn browse_context(b: &mut Browser) {
    b.click("s-detail-context");
    assert!(
        b.wait_until(
            "document.getElementById('s-context') !== null && document.querySelector('#s-result .stale') === null",
            WAIT
        ),
        "上下文结果未显示：{}",
        b.text("s-result")
    );
}

/// 场景「上下文结果按时间排列并标出中心行」：`ctx` 50 条，UTC，条件模式查询后对 `seq` 为 25 的行看前后记录。
fn center_view(s: &Server) -> Browser {
    seq_records(s, "ctx", 50);
    let mut b = open_in(s, "UTC", |_| {});
    ctx_searched(&mut b, "ctx", None);
    open_seq(&mut b, 25);
    browse_context(&mut b);
    b
}

#[test]
fn ui_search_context_entry_condition_mode() {
    let (_fx, s) = start(&[]);
    seq_records(&s, "ctx", 50);
    let mut b = open_in(&s, "UTC", |_| {});
    ctx_searched(&mut b, "ctx", None);
    open_seq(&mut b, 25);
    assert!(
        b.exists("#s-detail-context"),
        "条件模式的详情没有「看前后记录」"
    );
    assert_eq!(
        b.eval("document.getElementById('s-detail-keep').checked"),
        json!(false)
    );
    b.click("s-detail-close");
    assert!(b.wait_until("document.getElementById('s-detail') === null", WAIT));
    to_sql(&mut b);
    b.fill("s-sql", "SELECT * FROM \"ctx\"");
    query(&mut b);
    open_seq(&mut b, 25);
    assert!(
        !b.exists("#s-detail-context"),
        "SQL 模式的详情有「看前后记录」"
    );
}

#[test]
fn ui_search_context_requests_keep_conditions() {
    let (_fx, s) = start(&[]);
    seq_records(&s, "ctx", 50);
    let mut b = open_in(&s, "UTC", |_| {});
    ctx_searched(&mut b, "ctx", Some("seq >= 0"));
    let ancillary = requests_where(&mut b, |r| is_histogram(r) || is_count(r)).len();
    open_seq(&mut b, 25);
    b.click("s-detail-keep");
    b.click("s-detail-context");
    assert!(b.wait_until("document.getElementById('s-detail') === null", WAIT));
    let before = wait_requests(&mut b, 1, is_before).remove(0);
    let after = wait_requests(&mut b, 1, is_after).remove(0);
    let t = CTX_T0 + 25_000_000;
    assert_eq!(
        query_of(&before),
        json!({"sql": "SELECT * FROM \"ctx\" WHERE \"seq\" >= 0", "start_time": t - HOUR_US,
               "end_time": t, "size": 20, "from": 0, "timezone": "UTC"})
    );
    assert_eq!(
        query_of(&after),
        json!({"sql": "SELECT * FROM \"ctx\" WHERE \"seq\" >= 0 ORDER BY _timestamp ASC", "start_time": t,
               "end_time": t + HOUR_US, "size": 21, "from": 0, "timezone": "UTC"})
    );
    assert!(b.wait_finished(&before, WAIT) && b.wait_finished(&after, WAIT));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        requests_where(&mut b, |r| is_histogram(r) || is_count(r)).len(),
        ancillary,
        "看前后记录发出了分布请求或计数请求"
    );
}

#[test]
fn ui_search_context_requests_default_no_conditions() {
    let (_fx, s) = start(&[]);
    seq_records(&s, "ctx", 50);
    let mut b = open_in(&s, "UTC", |_| {});
    ctx_searched(&mut b, "ctx", Some("seq >= 30"));
    open_seq(&mut b, 30);
    browse_context(&mut b);
    let before = requests_where(&mut b, is_before).pop().unwrap();
    let after = requests_where(&mut b, is_after).pop().unwrap();
    assert_eq!(query_of(&before)["sql"], json!("SELECT * FROM \"ctx\""));
    assert_eq!(
        query_of(&after)["sql"],
        json!("SELECT * FROM \"ctx\" ORDER BY _timestamp ASC")
    );
    assert_eq!(seqs(&mut b), seq_range(49, 10));
    assert_eq!(
        b.text("s-context-few"),
        "这条记录前后 1 小时内的记录不足 20 条"
    );
}

#[test]
fn ui_search_context_429_as_result() {
    let (_fx, s) = start(&[]);
    seq_records(&s, "ctx", 50);
    let mut b = open_in(&s, "UTC", |b| {
        answer_error(b, is_before, 429, "too_many_queries")
    });
    ctx_searched(&mut b, "ctx", None);
    open_seq(&mut b, 25);
    b.click("s-detail-context");
    assert!(
        b.wait_text("s-result", BUSY, WAIT),
        "{}",
        b.text("s-result")
    );
    assert!(b.text("s-result").contains(" 秒"), "{}", b.text("s-result"));
    assert!(disabled(&mut b, "s-query"));
}

#[test]
fn ui_search_context_cancel_keeps_results() {
    let fx = common::Fixture::new();
    let gate = Gate::new(&fx, "query.after_admission");
    gate.release();
    let s = start_gated(&fx, &[], &gate);
    seq_records(&s, "ctx", 50);
    let mut b = open_in(&s, "UTC", |_| {});
    ctx_searched(&mut b, "ctx", None);
    assert!(b.wait_until(
        "document.querySelectorAll('#s-hist .hbar').length > 0",
        WAIT
    ));
    let bars_before = bars(&mut b);
    assert_eq!(b.text("s-total"), "共 50 条");
    gate.hold();
    open_seq(&mut b, 25);
    let clicked = std::time::Instant::now();
    b.click("s-detail-context");
    gate.wait_held(2);
    std::thread::sleep(
        (clicked + Duration::from_secs(5)).saturating_duration_since(std::time::Instant::now()),
    );
    b.click("s-cancel");
    assert!(
        b.wait_text(
            "s-result",
            "已取消。服务端可能仍在执行这次查询，马上重查可能提示服务繁忙",
            WAIT
        ),
        "{}",
        b.text("s-result")
    );
    assert_eq!(row_count(&mut b), 50);
    assert_eq!(seqs(&mut b), seq_range(49, 0));
    assert_eq!(bars(&mut b), bars_before);
    assert_eq!(b.text("s-total"), "共 50 条");
    gate.release();
}

#[test]
fn ui_search_context_view_center() {
    let (_fx, s) = start(&[]);
    let mut b = center_view(&s);
    assert_eq!(seqs(&mut b), seq_range(45, 5));
    let center = b.eval(
        "[...document.querySelectorAll('#s-result tbody tr')].flatMap((tr, i) => tr.classList.contains('center') ? [i] : [])",
    );
    let at = seqs(&mut b).iter().position(|x| x == "25").unwrap();
    assert_eq!(center, json!([at]));
    assert!(
        b.text("s-context").contains("2026-10-16 08:00:25.000000"),
        "{}",
        b.text("s-context")
    );
    assert!(!b.exists("#s-context-few"));
    assert!(
        !b.text("search")
            .contains("这条记录前后 1 小时内的记录不足 20 条")
    );
}

#[test]
fn ui_search_context_view_insufficient() {
    let (_fx, s) = start(&[]);
    seq_records(&s, "few", 3);
    let mut b = open_in(&s, "UTC", |_| {});
    ctx_searched(&mut b, "few", None);
    open_seq(&mut b, 1);
    browse_context(&mut b);
    assert_eq!(seqs(&mut b), seq_range(2, 0));
    assert_eq!(
        b.text("s-context-few"),
        "这条记录前后 1 小时内的记录不足 20 条"
    );
}

#[test]
fn ui_search_context_fixed_no_paging() {
    let (_fx, s) = start(&[]);
    let mut b = center_view(&s);
    let n = data_requests(&mut b).len();
    b.click_sel("#s-result th[data-col='_timestamp']");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(data_requests(&mut b).len(), n, "点列头发出了请求");
    assert!(disabled(&mut b, "s-prev") && disabled(&mut b, "s-next"));
    assert_eq!(b.text("s-total"), "共 50 条");
    let last = row_count(&mut b);
    open_detail(&mut b, last);
    b.press("s-detail", "ArrowDown");
    assert!(b.wait_until("document.getElementById('s-detail-edge') !== null", WAIT));
    assert_eq!(b.text("s-detail-edge"), "已是最后一条");
    assert_eq!(b.text("s-detail-pos"), "第 41 / 41 行");
}

#[test]
fn ui_search_context_back() {
    let (_fx, s) = start(&[]);
    let mut b = center_view(&s);
    let n = data_requests(&mut b).len();
    b.click("s-context-back");
    assert!(b.wait_until("document.getElementById('s-context') === null", WAIT));
    assert_eq!(seqs(&mut b), seq_range(49, 0));
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(
        data_requests(&mut b).len(),
        n,
        "点「返回检索结果」发出了请求"
    );
}

// ---------------------------------------------------------------------------
// history — 本地查询历史（REQ-AMB-UI-SEARCH-V2 K 组）
// 元素：`#s-history-open`（打开列表）、`#s-history-list .history-item`（各项，从新到旧；`.time` 发起的时刻、
// `.history-stream` 数据集、`.history-cond` 各条件、`.history-sql` SQL 文本）、`#s-history-clear`（清空）、
// `#s-history-confirm` 与 `#s-history-confirm-restore`、`#s-history-confirm-keep`（恢复前的确认）。
// ---------------------------------------------------------------------------

/// 点「查询」，等这次的检索请求与随它发出的分布或计数请求都收到应答（连续查询时不撞并发上限）。
fn query_done(b: &mut Browser) {
    let n = requests_where(b, is_search).len();
    b.click("s-query");
    wait_requests(b, n + 1, is_search);
    std::thread::sleep(Duration::from_millis(100));
    for r in data_requests(b) {
        assert!(b.wait_finished(&r, WAIT), "{r:?} 未收到应答");
    }
    wait_settled(b, "s");
}

/// 打开查询历史的列表。
fn open_history(b: &mut Browser) {
    if !b.exists("#s-history") {
        b.click("s-history-open");
    }
    assert!(
        b.wait_until("document.getElementById('s-history') !== null", WAIT),
        "查询历史的列表未打开"
    );
}

/// 列表各项：发起的时刻、数据集、各条件、SQL 文本（没有的为 null 或空）。
fn history_items(b: &mut Browser) -> Vec<Value> {
    open_history(b);
    b.eval(
        "[...document.querySelectorAll('#s-history-list .history-item')].map(e => ({ \
           time: e.querySelector('.time').innerText, \
           stream: e.querySelector('.history-stream')?.innerText ?? null, \
           conds: [...e.querySelectorAll('.history-cond')].map(c => c.innerText), \
           sql: e.querySelector('.history-sql')?.innerText ?? null }))",
    )
    .as_array()
    .unwrap()
    .clone()
}

/// 在列表中选第 `i` 项（从 0 起，最新的为 0）。
fn pick_history(b: &mut Browser, i: usize) {
    open_history(b);
    b.eval(&format!(
        "document.querySelectorAll('#s-history-list .history-item')[{i}].click()"
    ));
}

/// 发往 `_search` 的请求数。
fn search_posts(b: &mut Browser) -> usize {
    data_requests(b).len()
}

#[test]
fn ui_search_history_save_20() {
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({"n": 1}));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    for n in 1..=25 {
        if n > 1 {
            remove_chip(&mut b, &format!("n = {}", n - 1));
        }
        add(&mut b, &format!("n = {n}"));
        query_done(&mut b);
    }
    query_done(&mut b);
    b.reopen(&url(&s));
    let items = history_items(&mut b);
    assert_eq!(items.len(), 20, "{items:?}");
    assert_eq!(items[0]["conds"], json!(["n = 25"]));
    assert_eq!(items[19]["conds"], json!(["n = 6"]));
}

#[test]
fn ui_search_local_storage_no_secrets() {
    // 示例记录的取值拆开写，免得被凭据扫描当成密钥
    let secret = concat!("zq-result-", "7731");
    let (_fx, s) = start(&[]);
    ingest_now(&s, "app", json!({ "secretval": secret }));
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "secretval IS NOT NULL");
    query_done(&mut b);
    assert!(b.text("s-result").contains(secret), "结果表未显示该记录");
    wait_field(&mut b, "secretval");
    toggle_field(&mut b, "secretval");
    wait_values(&mut b, "secretval");
    assert_eq!(
        values_of(&mut b, "secretval"),
        [(secret.into(), "1".into())]
    );
    let stored = b
        .eval("JSON.stringify(Object.entries(localStorage))")
        .as_str()
        .unwrap()
        .to_owned();
    assert!(
        stored.contains("amberlog.search.history") && stored.contains("secretval"),
        "前提：查询历史已记入：{stored}"
    );
    assert!(!stored.contains(TOKEN), "{stored}");
    assert!(!stored.contains(secret), "{stored}");
}

#[test]
fn ui_search_history_list() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "status >= 500");
    query_done(&mut b);
    to_sql(&mut b);
    b.fill("s-sql", "SELECT host FROM \"web\"");
    let before = now_us();
    query_done(&mut b);
    let after = now_us();
    let items = history_items(&mut b);
    assert_eq!(items.len(), 2, "{items:?}");
    assert_eq!(items[0]["sql"], json!("SELECT host FROM \"web\""));
    let t = items[0]["time"].as_str().unwrap();
    // 浏览器取时刻精确到毫秒
    assert!(
        utc_text(before - 1000).as_str() <= t && t <= utc_text(after).as_str(),
        "{t} 不在 {} 与 {} 之间",
        utc_text(before),
        utc_text(after)
    );
    assert_eq!(items[1]["stream"], json!("app"));
    assert_eq!(items[1]["conds"], json!(["status >= 500"]));
}

#[test]
fn ui_search_history_restore_no_request() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "timeout");
    let week = (7 * 24 * HOUR_US).to_string();
    b.select("s-quick", &week);
    query_done(&mut b);
    b.fill("s-stream", "web");
    remove_chip(&mut b, "全文 timeout");
    b.select("s-quick", &HOUR_US.to_string());
    query_done(&mut b);
    let n = search_posts(&mut b);
    pick_history(&mut b, 1);
    assert!(b.wait_until("document.getElementById('s-stream').value === 'app'", WAIT));
    assert_eq!(b.value("s-quick"), week);
    assert_eq!(chips(&mut b), vec![("全文 timeout".into(), true)]);
    assert!(b.exists("#s-staged"), "恢复后没有未生效的改动的标记");
    std::thread::sleep(Duration::from_millis(500));
    assert_eq!(search_posts(&mut b), n, "恢复时发出了 _search 请求");
    b.click("s-query");
    let r = wait_requests(&mut b, 3, is_search).remove(2);
    assert_eq!(
        sql_of(&r),
        r#"SELECT * FROM "app" WHERE match_all('timeout')"#
    );
    let (start, end) = range_of(&r);
    assert_eq!(end - start, 604_800_000_000);
}

#[test]
fn ui_search_history_restore_sql() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    to_sql(&mut b);
    b.fill("s-sql", "SELECT host FROM \"web\"");
    query_done(&mut b);
    b.click("s-mode-cond");
    assert!(b.wait_until("document.getElementById('s-confirm') !== null", WAIT));
    b.click("s-confirm-discard");
    assert!(b.wait_until("document.getElementById('s-cond-input') !== null", WAIT));
    add(&mut b, "a = 1");
    pick_history(&mut b, 0);
    assert!(
        b.wait_until("document.getElementById('s-sql') !== null", WAIT),
        "选该项后不在 SQL 模式"
    );
    assert_eq!(b.value("s-sql"), "SELECT host FROM \"web\"");
    b.click("s-mode-cond");
    assert!(b.wait_until("document.getElementById('s-cond-input') !== null", WAIT));
    assert!(!b.exists("#s-confirm"), "切回条件模式时要求了确认");
    assert_eq!(chips(&mut b), Vec::<(String, bool)>::new());
}

#[test]
fn ui_search_history_restore_confirm() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    add(&mut b, "a = 1");
    query_done(&mut b);
    to_sql(&mut b);
    b.fill("s-sql", "SELECT host FROM \"app\"");
    pick_history(&mut b, 0);
    assert!(
        b.wait_until(
            "document.getElementById('s-history-confirm') !== null",
            WAIT
        ),
        "没有显示确认"
    );
    assert!(
        b.text("s-history-confirm")
            .contains("恢复会放弃对 SQL 的修改")
    );
    b.click("s-history-confirm-keep");
    assert!(b.wait_until(
        "document.getElementById('s-history-confirm') === null",
        WAIT
    ));
    assert!(b.exists("#s-sql"), "已不在 SQL 模式");
    assert_eq!(b.value("s-sql"), "SELECT host FROM \"app\"");
}

#[test]
fn ui_search_history_clear() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "search");
    b.fill("s-stream", "app");
    for n in 1..=3 {
        if n > 1 {
            remove_chip(&mut b, &format!("a = {}", n - 1));
        }
        add(&mut b, &format!("a = {n}"));
        query_done(&mut b);
    }
    assert_eq!(history_items(&mut b).len(), 3);
    b.click("s-history-clear");
    assert!(
        b.wait_until(
            "document.querySelectorAll('#s-history-list .history-item').length === 0",
            WAIT
        ),
        "点击后列表不为空"
    );
    b.reopen(&url(&s));
    assert_eq!(history_items(&mut b), Vec::<Value>::new());
}
