//! REQ-AMB-UI-GOVERNANCE 的系统测试：以子进程启动 `amberlog`（`auth.enabled` 为
//! 缺省的 true），以无头浏览器（`common::browser`）打开 `/ui` 操作台账屏与对账屏。对象存储用 `file://` 临时目录。
//! 三屏共用的元素 id 见 `common::ui` 的文件头；台账屏与对账屏另用：`#r-only-nonconsistent`（「只看非一致」）、
//! `#r-notice`（对账屏的说明文字）、`#r-datasets option`（对账屏的数据集候选）。台账表格的表头有两行：上一行只有
//! 判定汇总的组名「各判定的时间窗数」，下一行是各列的列头（`common::ui::header` 取它）；台账行的「对账」「检索」是
//! 该行最后一格里的按钮。
//!
//! 漏采判定的数据按 REQ-AMB-GOV-MISSED-COLLECTION 场景的共同前提造：`reconciliation.grace` 为 `1m`，实例开始受理
//! 请求超过 1 分钟后再查（此前全部判为待定），计数对象由测试台架直接写入此前第 3 个小时（被测进程不会计入）或摄取
//! 之后等计数写出。

mod common;

use std::time::Duration;

use common::api::{HOUR_US, MINUTE_US, create_stream, ingest_ok, ingest_with, now_us, poll, sent};
use common::browser::{Browser, Request};
use common::gov::{entry, query_ok, rfc3339, write_counters};
use common::missed::{GRACE_1M, Instance, SOURCE, hour_ago, minute_ago, ts_entry};
use common::ui::*;
use common::{Server, TOKEN};
use serde_json::{Value, json};

const NOTE: &str = "本地持久化确认数在记录写入本机预写日志并确认时计入，不反映此后的丢失（旧的本地目录挂回时被丢弃的记录、\
节点磁盘损坏时尚未提交的记录）。判定按宽限期计算：时间窗结束后要等宽限期过去才下结论，在此之前、或来源可能仍在补发时\
显示「待定」，结论以后可能改变。";

/// 2026-10-16T08:00:00Z。
const T1016_0800: i64 = 1_792_137_600_000_000;

/// 开始受理请求后要等的时长：宽限期 1 分钟，再留余量。
const PAST_GRACE: Duration = Duration::from_secs(65);

// ---------------------------------------------------------------------------
// 部件
// ---------------------------------------------------------------------------

/// 以时区 `tz` 打开 `/ui`（先做 `setup`）、填口令，等数据集候选查询（填口令后重发的那次）收到应答。
fn open_in(s: &Server, tz: &str, setup: impl FnOnce(&mut Browser)) -> Browser {
    let mut b = open_with(s, |b| {
        b.set_timezone(tz);
        setup(b);
    });
    let first = wait_requests(&mut b, 1, is_candidates).remove(0);
    assert!(b.wait_finished(&first, WAIT), "数据集候选查询未收到应答");
    fill_token(&mut b, TOKEN);
    let second = wait_requests(&mut b, 2, is_candidates).remove(1);
    assert!(b.wait_finished(&second, WAIT), "数据集候选查询未收到应答");
    b
}

/// 列头为 `name`（时刻列为 `name` 加「（时区名）」）的列的下标。按整名找：「首条数据时间」也含「条数」二字。
fn column(b: &mut Browser, p: &str, name: &str) -> usize {
    let h = header(b, p);
    let zoned = format!("{name}（");
    h.iter()
        .position(|c| c == name || c.starts_with(&zoned))
        .unwrap_or_else(|| panic!("{p}-result 没有「{name}」列：{h:?}"))
}

/// 台账行 `d` 中「`name`」列的一格。
fn ledger_cell(b: &mut Browser, d: &str, name: &str) -> String {
    let r = row(b, "l", d);
    let i = column(b, "l", name);
    r[i].clone()
}

/// 对账表格中窗口起点为 `first` 的一行里「`name`」列的一格。
fn recon_cell(b: &mut Browser, first: &str, name: &str) -> String {
    let r = row(b, "r", first);
    let i = column(b, "r", name);
    r[i].clone()
}

/// 等 `_reconciliation` 中数据集 `d`（以此前 1 小时至此刻之后 1 秒查）有行：计数对象已写出。
fn wait_counted(s: &Server, d: &str) {
    let sql = format!("SELECT * FROM _reconciliation WHERE dataset = '{d}'");
    let (hits, done) = poll(
        WAIT,
        || {
            let now = now_us();
            query_ok(s, &sql, (now - HOUR_US, now + 1_000_000))
        },
        |hits| !hits.is_empty(),
    );
    assert!(done, "{d} 的计数对象未写出：{hits:?}");
}

/// 点台账行 `d` 的跳转按钮（「对账」或「检索」）。
fn ledger_jump(b: &mut Browser, d: &str, label: &str) {
    let clicked = b.eval(&format!(
        "(() => {{ const tr = [...document.querySelectorAll('#l-result tbody tr')].find(tr => tr.children[0].innerText === {d}); \
         if (!tr) return false; const x = [...tr.querySelectorAll('button')].find(x => x.textContent === {label}); \
         if (!x) return false; x.click(); return true; }})()",
        d = json!(d),
        label = json!(label)
    ));
    assert_eq!(clicked, json!(true), "台账行 {d} 没有「{label}」");
}

/// 对账屏以数据集 `d` 与起止 `[start, end)`（RFC 3339）查询，等结果区出结果。
fn query_recon(b: &mut Browser, d: &str, start: i64, end: i64) {
    screen(b, "recon");
    b.fill("r-dataset", d);
    b.fill("r-start", &rfc3339(start));
    b.fill("r-end", &rfc3339(end));
    b.click("r-query");
    wait_settled(b, "r");
}

/// 请求体的 `query`。
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

/// 对账表格的数据行（各格文字）。
fn recon_rows(b: &mut Browser) -> Vec<Vec<String>> {
    let v = b.eval(
        "[...document.querySelectorAll('#r-result tbody tr')].map(tr => [...tr.children].map(c => c.innerText))",
    );
    serde_json::from_value(v).unwrap()
}

/// 以 `(start, end)` 查 `_ledger` 中数据集 `d` 的一行。
fn api_ledger(s: &Server, d: &str, range: (i64, i64)) -> Value {
    let hits = query_ok(
        s,
        &format!("SELECT * FROM _ledger WHERE dataset = '{d}'"),
        range,
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    hits[0].clone()
}

/// 等 `_ledger` 中数据集 `d` 的一行（以此前 1 小时至此刻之后 1 秒查）满足 `ok`。
fn wait_ledger(s: &Server, d: &str, ok: impl Fn(&Value) -> bool) {
    let sql = format!("SELECT * FROM _ledger WHERE dataset = '{d}'");
    let (hits, done) = poll(
        WAIT,
        || {
            let now = now_us();
            query_ok(s, &sql, (now - HOUR_US, now + 1_000_000))
        },
        |hits| hits.first().is_some_and(&ok),
    );
    assert!(done, "{d}：{hits:?}");
}

/// 按各窗口所在小时分别写入计数对象。
fn write_by_hour(i: &Instance, stream: &str, windows: &[Value]) {
    let mut hours: Vec<i64> = windows
        .iter()
        .map(|w| w["window_start"].as_i64().unwrap())
        .map(|w| w - w.rem_euclid(HOUR_US))
        .collect();
    hours.dedup();
    for h in hours {
        let ws: Vec<Value> = windows
            .iter()
            .filter(|w| {
                let s = w["window_start"].as_i64().unwrap();
                s - s.rem_euclid(HOUR_US) == h
            })
            .cloned()
            .collect();
        write_counters(&i.fx, stream, h, &ws);
    }
}

/// 场景「检索、台账、对账三屏取到真实数据」的数据：`app` 此前第 59 至第 3 分钟每分钟一条带来源 `vector-1` 的记录，
/// `vector-1` 按记录时间戳上报这些分钟各 1 条；`web` 摄取一条记录、没有来源上报。等计数写出、记录与上报都过了
/// 宽限期且不再补发（此后各窗口的判定与上报中断的来源数不随时间改变）。
///
/// `vector-1` 另按送达上报同样的发送数：场景要台账的对账状态为「三方」，而 REQ-AMB-GOV-LEDGER-V3 的 `sources` 与
/// `reconciliation` 只看按送达的上报，只按时间戳上报时为 `two_way`。
fn real_data() -> Instance {
    let i = Instance::start(&[GRACE_1M]);
    let minutes: Vec<i64> = (3..=59).map(minute_ago).collect();
    let records: Vec<Value> = minutes
        .iter()
        .map(|w| json!({"_timestamp": rfc3339(w + 1_000_000), "a": 1}))
        .collect();
    let r = ingest_with(
        &i.server,
        "app",
        &[(SOURCE, "vector-1")],
        json!(records).to_string().as_bytes(),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let reports: Vec<Value> = minutes
        .iter()
        .flat_map(|w| {
            [
                json!({"source": "vector-1", "window_start": w, "window_end": w + MINUTE_US, "sent": 1, "basis": "timestamp"}),
                json!({"source": "vector-1", "window_start": w, "window_end": w + MINUTE_US, "sent": 1}),
            ]
        })
        .collect();
    let r = sent(&i.server, "app", &json!(reports).to_string());
    assert_eq!(r.status, 200, "{}", r.text());
    ingest_ok(&i.server, "web", r#"[{"a":1}]"#);
    let n = minutes.len() as u64;
    wait_ledger(&i.server, "default/app", |h| {
        h["reconciliation"] == "three_way"
            && h["ts_sources"] == "vector-1"
            && h["record_count"] == n
    });
    wait_ledger(&i.server, "default/web", |h| {
        h["reconciliation"] == "two_way"
    });
    // 迟到的摄取与上报之后，来源补发中持续一个宽限期（REQ-AMB-GOV-MISSED-COLLECTION-TERM-BACKFILL）
    std::thread::sleep(PAST_GRACE);
    i
}

/// 场景「对账屏显示判定列」的数据：此刻之前第 10 分钟的窗口 W 判为 `missing`、第 9 分钟判为 `consistent`。返回 W。
fn missing_then_consistent() -> (Instance, i64) {
    let i = Instance::start(&[GRACE_1M]);
    create_stream(&i.server, "app");
    let (w, w9) = (minute_ago(10), minute_ago(9));
    write_by_hour(
        &i,
        "app",
        &[
            ts_entry(
                w,
                3,
                3,
                json!({}),
                Some(json!({"fw-1": 5})),
                Some((3, json!({"fw-1": 3}))),
            ),
            ts_entry(
                w9,
                2,
                2,
                json!({}),
                Some(json!({"fw-1": 2})),
                Some((2, json!({"fw-1": 2}))),
            ),
        ],
    );
    i.wait_accepting(PAST_GRACE);
    assert_eq!(common::missed::verdict_of(&i.server, "app", w), "missing");
    assert_eq!(
        common::missed::verdict_of(&i.server, "app", w9),
        "consistent"
    );
    (i, w)
}

/// 场景「六种判定的文字」的数据：此前第 3 个小时起的 6 个分钟窗口依次判为 `pending`、`consistent`、`missing`、
/// `duplicate`、`silent`、`undeterminable`，对账屏以覆盖它们的范围查询 `default/app`。返回各窗口起点。
///
/// - 第 1 个窗口：来源 `bf` 缺失，而 `bf` 刚做过一次迟到的发送数上报（补发中），按待定计。
/// - 第 2 个窗口：`bf`、`fw-1` 都一致。
/// - 第 3、4 个窗口：`fw-1` 缺失、重复。
/// - 第 5 个窗口：`fw-1` 此前有按时间戳的发送数、这个窗口没有，上报中断；`bf` 一致。
/// - 第 6 个窗口：`durable_ts_by_source` 已含 1000 个其他来源，`bf`、`fw-1` 都不是受判来源；两者都在 `sent_ts` 中，
///   也不算上报中断。
fn six_verdicts(b: &mut Browser, i: &Instance) -> Vec<i64> {
    let h = hour_ago(3);
    let w: Vec<i64> = (0..6).map(|k| h + k * MINUTE_US).collect();
    let cap: serde_json::Map<String, Value> =
        (1..=1000).map(|n| (format!("s{n:04}"), json!(1))).collect();
    write_counters(
        &i.fx,
        "app",
        h,
        &[
            ts_entry(
                w[0],
                3,
                3,
                json!({}),
                Some(json!({"bf": 5})),
                Some((3, json!({"bf": 3}))),
            ),
            ts_entry(
                w[1],
                4,
                4,
                json!({}),
                Some(json!({"bf": 2, "fw-1": 2})),
                Some((4, json!({"bf": 2, "fw-1": 2}))),
            ),
            ts_entry(
                w[2],
                4,
                4,
                json!({}),
                Some(json!({"bf": 1, "fw-1": 5})),
                Some((4, json!({"bf": 1, "fw-1": 3}))),
            ),
            ts_entry(
                w[3],
                4,
                4,
                json!({}),
                Some(json!({"bf": 1, "fw-1": 2})),
                Some((4, json!({"bf": 1, "fw-1": 3}))),
            ),
            ts_entry(
                w[4],
                1,
                1,
                json!({}),
                Some(json!({"bf": 1})),
                Some((1, json!({"bf": 1}))),
            ),
            ts_entry(
                w[5],
                1000,
                1000,
                json!({}),
                Some(json!({"bf": 1, "fw-1": 1})),
                Some((1000, Value::Object(cap))),
            ),
        ],
    );
    i.wait_accepting(PAST_GRACE);
    screen(b, "recon");
    b.fill("r-dataset", "default/app");
    b.fill("r-start", &rfc3339(w[0]));
    b.fill("r-end", &rfc3339(w[5] + MINUTE_US));
    // 迟到的发送数上报：`bf` 此后一个宽限期内补发中（窗口取此前第 5 个小时，不在查询范围内）
    common::missed::report(&i.server, "app", "bf", hour_ago(5), 1);
    b.click("r-query");
    wait_settled(b, "r");
    w
}

// ---------------------------------------------------------------------------
// ledger — 台账屏
// ---------------------------------------------------------------------------

#[test]
fn ui_gov_ledger_default_query() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "ledger");
    let before = now_us();
    b.click("l-query");
    let r = wait_requests(&mut b, 1, is_ledger).remove(0);
    let after = now_us();
    let q = query_of(&r);
    assert_eq!(
        (&q["sql"], &q["size"]),
        (&json!("SELECT * FROM _ledger"), &json!(1000))
    );
    let (start, end) = range_of(&r);
    // 浏览器的此刻取 Date.now()，只到毫秒：`end_time` 可比测试记下的微秒时刻早不到 1 毫秒
    assert!(
        before - 1000 <= end && end <= after,
        "{before} ≤ {end} ≤ {after}"
    );
    assert_eq!(end - start, 86_400_000_000);
}

#[test]
fn ui_gov_ledger_quick_range() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "ledger");
    b.select("l-quick", "604800000000");
    b.click("l-query");
    let r = wait_requests(&mut b, 1, is_ledger).remove(0);
    let (start, end) = range_of(&r);
    assert_eq!(end - start, 604_800_000_000);
}

#[test]
fn ui_gov_three_screens_real_data() {
    let i = real_data();
    let mut b = open_in(&i.server, "UTC", |_| {});
    query_ledger(&mut b);
    for name in [
        "数据集",
        "状态",
        "来源",
        "首条数据时间",
        "末条数据时间",
        "条数",
        "字节数",
        "所在层",
        "待入湖条数",
        "保留期",
        "到期日",
        "对账状态",
    ] {
        let v = ledger_cell(&mut b, "default/app", name);
        assert!(!v.is_empty(), "{name} 为空");
    }
    assert_eq!(ledger_cell(&mut b, "default/app", "对账状态"), "三方");
    assert_eq!(ledger_cell(&mut b, "default/app", "所在层"), "热层");

    ledger_jump(&mut b, "default/app", "检索");
    b.click("s-query");
    wait_settled(&mut b, "s");
    let t = b.table("s-result");
    let a = t[0].iter().position(|h| h == "a").expect("有 a 列");
    assert!(t.len() > 1, "{t:?}");
    assert!(t[1..].iter().all(|r| r[a] == "1"), "{t:?}");

    query_ledger(&mut b);
    ledger_jump(&mut b, "default/app", "对账");
    wait_settled(&mut b, "r");
    let h = header(&mut b, "r");
    for name in ["发送数", "接收数", "本地持久化确认数"] {
        assert!(h.iter().any(|c| c == name), "{h:?}");
    }
    let rows = recon_rows(&mut b);
    assert!(!rows.is_empty());
    let (sent_i, recv_i, durable_i) = (
        column(&mut b, "r", "发送数"),
        column(&mut b, "r", "接收数"),
        column(&mut b, "r", "本地持久化确认数"),
    );
    assert!(rows.iter().any(|r| r[sent_i] == "1"), "{rows:?}");
    assert!(
        rows.iter()
            .any(|r| r[recv_i] == "57" && r[durable_i] == "57"),
        "{rows:?}"
    );
    let last = requests_where(&mut b, is_recon).pop().expect("对账请求");
    assert_eq!(
        query_of(&last)["sql"],
        json!("SELECT * FROM _reconciliation WHERE dataset = 'default/app'")
    );
}

#[test]
fn ui_gov_ledger_labels() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "web", r#"[{"a":1}]"#);
    wait_ledger(&s, "default/web", |h| h["reconciliation"] == "two_way");
    let mut b = open_in(&s, "UTC", |_| {});
    query_ledger(&mut b);
    let at = |b: &mut Browser, name: &str| ledger_cell(b, "default/web", name);
    assert_eq!(at(&mut b, "状态"), "未停用");
    assert_eq!(at(&mut b, "来源"), "无上报");
    assert_eq!(at(&mut b, "保留期"), "未设定");
    assert_eq!(at(&mut b, "到期日"), "未设定");
    assert_eq!(at(&mut b, "对账状态"), "两方（发送计数缺失）");
}

#[test]
fn ui_gov_ledger_verdict_summary() {
    let i = real_data();
    write_counters(
        &i.fx,
        "app",
        hour_ago(3),
        &[ts_entry(
            hour_ago(3),
            3,
            3,
            json!({}),
            Some(json!({"vector-1": 5})),
            Some((3, json!({"vector-1": 3}))),
        )],
    );
    let mut b = open_in(&i.server, "UTC", |_| {});
    query_ledger(&mut b);
    let r = requests_where(&mut b, is_ledger).pop().unwrap();
    let api = api_ledger(&i.server, "default/app", range_of(&r));
    assert_eq!(api["windows_missing"], 1, "{api}");
    // 判定汇总以「各判定的时间窗数」为题：表头上一行是组名，跨下一行的六个判定列
    let title = b.eval(
        "(() => { const th = [...document.querySelectorAll('#l-result thead tr:first-child th')]\
         .find(t => t.innerText === '各判定的时间窗数'); return th ? th.colSpan : 0; })()",
    );
    assert_eq!(title, json!(6));
    for (label, key) in [
        ("一致", "windows_consistent"),
        ("缺失", "windows_missing"),
        ("重复", "windows_duplicate"),
        ("上报中断", "windows_silent"),
        ("待定", "windows_pending"),
        ("不可判定", "windows_undeterminable"),
        ("上报中断的来源数", "silent_sources"),
    ] {
        let h = header(&mut b, "l");
        let idx = h
            .iter()
            .position(|c| c == label)
            .unwrap_or_else(|| panic!("没有「{label}」列：{h:?}"));
        let cell = row(&mut b, "l", "default/app")[idx].clone();
        assert_eq!(cell, api[key].to_string(), "{label}");
    }
    assert_eq!(ledger_cell(&mut b, "default/app", "缺失"), "1");
}

#[test]
fn ui_gov_ledger_ts_sources() {
    let i = real_data();
    let mut b = open_in(&i.server, "UTC", |_| {});
    query_ledger(&mut b);
    assert_eq!(
        ledger_cell(&mut b, "default/app", "按时间戳上报的来源"),
        "vector-1"
    );
    assert_eq!(
        ledger_cell(&mut b, "default/web", "按时间戳上报的来源"),
        "无"
    );
}

#[test]
fn ui_gov_jump_to_reconciliation() {
    let (_fx, s) = start(&[]);
    create_stream(&s, "app");
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "ledger");
    b.fill("l-start", "2026-10-16 08:00");
    b.fill("l-end", "2026-10-16 09:00");
    b.click("l-query");
    wait_settled(&mut b, "l");
    let ledger = requests_where(&mut b, is_ledger).pop().unwrap();
    assert_eq!(range_of(&ledger), (T1016_0800, T1016_0800 + HOUR_US));
    ledger_jump(&mut b, "default/app", "对账");
    let recon = wait_requests(&mut b, 1, is_recon).remove(0);
    assert_eq!(shown_screen(&mut b), "recon");
    assert_eq!(
        query_of(&recon)["sql"],
        json!("SELECT * FROM _reconciliation WHERE dataset = 'default/app'")
    );
    assert_eq!(range_of(&recon), range_of(&ledger));
}

#[test]
fn ui_gov_jump_to_search() {
    let (_fx, s) = start(&[]);
    create_stream(&s, "app");
    let mut b = open_in(&s, "UTC", |_| {});
    query_ledger(&mut b);
    let n = data_requests(&mut b).len();
    ledger_jump(&mut b, "default/app", "检索");
    assert_eq!(shown_screen(&mut b), "search");
    assert_eq!(b.value("s-stream"), "app");
    std::thread::sleep(Duration::from_secs(2));
    assert_eq!(data_requests(&mut b).len(), n, "跳转发出了请求");
}

#[test]
fn ui_gov_time_columns_timezone() {
    let i = Instance::start(&[]);
    create_stream(&i.server, "app");
    write_counters(
        &i.fx,
        "app",
        T1016_0800,
        &[entry(T1016_0800, 1, 1, json!({}))],
    );
    let mut b = open_in(&i.server, "Asia/Shanghai", |_| {});
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.fill("r-start", "2026-10-16 16:00");
    b.fill("r-end", "2026-10-16 16:05");
    b.click("r-query");
    wait_settled(&mut b, "r");
    let rows = recon_rows(&mut b);
    assert_eq!(rows.len(), 1, "{rows:?}");
    assert_eq!(rows[0][0], "2026-10-16 16:00:00.000000");
    assert!(header(&mut b, "r")[0].contains("Asia/Shanghai"));
}

#[test]
fn ui_gov_ledger_no_edit() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "app", r#"[{"a":1}]"#);
    wait_ledger(&s, "default/app", |h| h["record_count"] == 1);
    let mut b = open_in(&s, "UTC", |_| {});
    query_ledger(&mut b);
    row(&mut b, "l", "default/app");
    let editable = b.eval(
        "document.querySelectorAll('#l-result input, #l-result textarea, #l-result select, \
         #l-result [contenteditable]:not([contenteditable=\"false\"])').length",
    );
    assert_eq!(editable, json!(0));
    let buttons = b.eval("[...new Set([...document.querySelectorAll('#l-result button')].map(x => x.textContent))].sort()");
    assert_eq!(buttons, json!(["对账", "检索"]));
}

// ---------------------------------------------------------------------------
// reconciliation — 对账屏
// ---------------------------------------------------------------------------

#[test]
fn ui_gov_recon_default_range() {
    let (_fx, s) = start(&[]);
    create_stream(&s, "app");
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "recon");
    let options = b.eval("[...document.querySelectorAll('#r-datasets option')].map(o => o.value)");
    assert!(
        options.as_array().unwrap().contains(&json!("default/app")),
        "{options}"
    );
    b.fill("r-dataset", "default/app");
    let before = now_us();
    b.click("r-query");
    let r = wait_requests(&mut b, 1, is_recon).remove(0);
    let after = now_us();
    let (start, end) = range_of(&r);
    // 浏览器的此刻取 Date.now()，只到毫秒：`end_time` 可比测试记下的微秒时刻早不到 1 毫秒
    assert!(
        before - 1000 <= end && end <= after,
        "{before} ≤ {end} ≤ {after}"
    );
    assert_eq!(end - start, 3_600_000_000);
}

#[test]
fn ui_gov_recon_quote_dataset() {
    let (_fx, s) = start(&[]);
    let mut b = open_in(&s, "UTC", |_| {});
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/o'x");
    b.click("r-query");
    let r = wait_requests(&mut b, 1, is_recon).remove(0);
    let q = query_of(&r);
    assert_eq!(
        (&q["sql"], &q["size"]),
        (
            &json!("SELECT * FROM _reconciliation WHERE dataset = 'default/o''x'"),
            &json!(10000)
        )
    );
}

#[test]
fn ui_gov_recon_rows_no_fill() {
    let (fx, s) = start(&[]);
    create_stream(&s, "app");
    let h = hour_ago(3);
    let (w1, w3) = (h, h + 2 * MINUTE_US);
    write_counters(
        &fx,
        "app",
        h,
        &[entry(w1, 1, 1, json!({})), entry(w3, 2, 2, json!({}))],
    );
    let mut b = open_in(&s, "UTC", |_| {});
    query_recon(&mut b, "default/app", w1, w3 + MINUTE_US);
    let rows = recon_rows(&mut b);
    let mut starts: Vec<&str> = rows.iter().map(|r| r[0].as_str()).collect();
    starts.sort_unstable();
    assert_eq!(starts, [utc_text(w1), utc_text(w3)]);
}

#[test]
fn ui_gov_recon_sent_missing_label() {
    let (_fx, s) = start(&[]);
    ingest_ok(&s, "web", r#"[{"a":1}]"#);
    wait_counted(&s, "default/web");
    let mut b = open_in(&s, "UTC", |_| {});
    query_ledger(&mut b);
    ledger_jump(&mut b, "default/web", "对账");
    wait_settled(&mut b, "r");
    let rows = recon_rows(&mut b);
    assert!(!rows.is_empty());
    let (sent_i, sources_i, gap_i) = (
        column(&mut b, "r", "发送数"),
        column(&mut b, "r", "发送来源"),
        column(&mut b, "r", "「采集器 → 接收」差值"),
    );
    for r in &rows {
        assert_eq!(
            (r[sent_i].as_str(), r[sources_i].as_str(), r[gap_i].as_str()),
            ("未上报", "—", "—"),
            "{rows:?}"
        );
    }
}

#[test]
fn ui_gov_recon_verdict_column() {
    let (i, w) = missing_then_consistent();
    let mut b = open_in(&i.server, "UTC", |_| {});
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.click("r-query");
    wait_settled(&mut b, "r");
    assert!(header(&mut b, "r").iter().any(|c| c == "判定"));
    assert_eq!(recon_cell(&mut b, &utc_text(w), "判定"), "缺失");
    assert_eq!(recon_cell(&mut b, &utc_text(w + MINUTE_US), "判定"), "一致");
}

#[test]
fn ui_gov_recon_ts_counts() {
    let (i, w) = missing_then_consistent();
    let mut b = open_in(&i.server, "UTC", |_| {});
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.click("r-query");
    wait_settled(&mut b, "r");
    let hits = query_ok(
        &i.server,
        "SELECT ts_sent, ts_durable FROM _reconciliation WHERE dataset = 'default/app'",
        (w, w + MINUTE_US),
    );
    assert_eq!(hits.len(), 1, "{hits:?}");
    assert_eq!(
        recon_cell(&mut b, &utc_text(w), "按时间戳的发送数"),
        hits[0]["ts_sent"].to_string()
    );
    assert_eq!(
        recon_cell(&mut b, &utc_text(w), "按时间戳的确认数"),
        hits[0]["ts_durable"].to_string()
    );
}

#[test]
fn ui_gov_recon_only_nonconsistent() {
    let (i, w) = missing_then_consistent();
    let mut b = open_in(&i.server, "UTC", |_| {});
    screen(&mut b, "recon");
    b.fill("r-dataset", "default/app");
    b.click("r-only-nonconsistent");
    b.click("r-query");
    wait_settled(&mut b, "r");
    let r = requests_where(&mut b, is_recon).pop().unwrap();
    assert_eq!(
        query_of(&r)["sql"],
        json!(
            "SELECT * FROM _reconciliation WHERE dataset = 'default/app' AND verdict != 'consistent'"
        )
    );
    let v = column(&mut b, "r", "判定");
    let rows = recon_rows(&mut b);
    assert!(rows.iter().any(|r| r[0] == utc_text(w)), "{rows:?}");
    assert!(rows.iter().all(|r| r[v] != "一致"), "{rows:?}");
}

#[test]
fn ui_gov_recon_note() {
    let (_fx, s) = start(&[]);
    let mut b = open(&s);
    screen(&mut b, "recon");
    assert_eq!(b.text("r-notice"), NOTE);
}

// ---------------------------------------------------------------------------
// presentation — 判定与数值的呈现
// ---------------------------------------------------------------------------

#[test]
fn ui_gov_verdict_labels() {
    let i = Instance::start(&[GRACE_1M]);
    create_stream(&i.server, "app");
    let mut b = open_in(&i.server, "UTC", |_| {});
    let w = six_verdicts(&mut b, &i);
    let rows = recon_rows(&mut b);
    let v = column(&mut b, "r", "判定");
    // 对账表的行按窗口起点从晚到早；按窗口起点排成从早到晚再比
    let mut got: Vec<(&str, &str)> = rows
        .iter()
        .map(|r| (r[0].as_str(), r[v].as_str()))
        .collect();
    got.sort_unstable();
    let texts: Vec<String> = w.iter().map(|x| utc_text(*x)).collect();
    let want: Vec<(&str, &str)> = texts
        .iter()
        .map(String::as_str)
        .zip(["待定", "一致", "缺失", "重复", "上报中断", "不可判定"])
        .collect();
    assert_eq!(got, want);
}

#[test]
fn ui_gov_verdict_not_colored() {
    let i = Instance::start(&[GRACE_1M]);
    create_stream(&i.server, "app");
    let mut b = open_in(&i.server, "UTC", |_| {});
    six_verdicts(&mut b, &i);
    let rows = cell_styles(&mut b, "#r-result tbody tr");
    assert_eq!(rows.len(), 6, "{rows:?}");
    let bg: Vec<&Value> = rows.iter().map(|r| &r[1]).collect();
    assert!(bg.iter().all(|x| *x == bg[0]), "各行背景色：{bg:?}");
    for name in ["判定", "「采集器 → 接收」差值", "「接收 → 本地持久化」差值"]
    {
        let c = column(&mut b, "r", name);
        let cells = cell_styles(
            &mut b,
            &format!("#r-result tbody tr td:nth-child({})", c + 1),
        );
        assert_eq!(cells.len(), 6, "{cells:?}");
        assert!(
            cells
                .iter()
                .all(|x| x[0] == cells[0][0] && x[1] == cells[0][1] && x[2] == cells[0][2]),
            "{name} 列：{cells:?}"
        );
    }
}

#[test]
fn ui_gov_number_format() {
    let (_fx, s) = start(&[]);
    let row_json = json!({
        "dataset": "default/app", "status": "active", "sources": null, "time_min": null, "time_max": null,
        "record_count": 1_234_567, "size_bytes": 1_288_490_189u64, "tier": "hot", "pending_lake_record_count": 0,
        "retention": null, "expires_at": null, "reconciliation": "two_way"
    });
    let body = json!({"took": 1, "hits": [row_json], "total": 1, "from": 0, "size": 1000,
                      "scan_files": 0, "scan_bytes": 0, "scan_records": 0})
    .to_string();
    let mut b = open_in(&s, "UTC", move |b| {
        b.answer_where(
            is_ledger,
            200,
            &[("Content-Type", "application/json")],
            &body,
        );
    });
    query_ledger(&mut b);
    assert_eq!(ledger_cell(&mut b, "default/app", "条数"), "1,234,567");
    assert_eq!(ledger_cell(&mut b, "default/app", "字节数"), "1.2 GiB");
    let (count, size) = (column(&mut b, "l", "条数"), column(&mut b, "l", "字节数"));
    let titles = b.eval(&format!(
        "(() => {{ const tr = [...document.querySelectorAll('#l-result tbody tr')].find(tr => tr.children[0].innerText === 'default/app'); \
         return [{count}, {size}].map(i => tr.children[i].querySelector('[title]').title); }})()"
    ));
    assert_eq!(titles, json!(["1234567", "1288490189"]));
}
