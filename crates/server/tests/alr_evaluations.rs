//! REQ-AMB-ALR-EVALUATIONS：评估记录对象的存放与格式、写出、保留期与系统表 `_alert_evaluations`
//! 的场景。
//!
//! 场景约定同 REQ-AMB-ALR-RULES（[`common::alerting`]）：规则 `interval` 为 `1m`、`window` 为 `7d`、`webhook_url` 指向
//! 测试接收端（缺省回 HTTP 200）、`condition` 为 `{ rows = "> 0" }`。「等评估记录写出」指等到评估结束后至多 90 秒
//! （[`common::alerting::RECORD_WAIT`]），轮询到条件成立即止。评估记录对象在 `file://` 临时目录下，测试直接读、写、删。
//!
//! 等整分的场景以测试时钟偏移把被测进程的钟推到整分之前（[`Clock`]）；要求几个计划时刻落在同一 UTC 小时的场景，再把钟
//! 推离整点（[`mid_hour`]）；跨整点的场景把钟推到整点之前（[`before_hour`]）。不开 `fault-injection` 构建时偏移不生效，
//! 场景照样成立，只是要等到真实的整分、整点。

mod common;

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use common::alerting::{
    Clock, EVAL_END_US, MINUTE_US, RECORD_WAIT, RuleToml, SECOND_US, evaluations, has_data,
    has_one, metric_or_zero, rules, search, wait_evaluation, wait_tick,
};
use common::gov::rfc3339;
use common::webhook::{Receiver, Request};
use common::{Fixture, Kv, Server};
use serde_json::{Value, json};

const HOUR_US: i64 = 60 * MINUTE_US;

/// 整分之前留出的时间：够启动实例并摄取一条数据。
const LEAD: Duration = Duration::from_secs(20);

// ---------------------------------------------------------------------------------------------------------------
// 搭建

/// 被测进程的钟落在某整分之前约 [`LEAD`]。
fn clock() -> Clock {
    Clock::before_minute(LEAD)
}

/// 同 [`clock`]，另保证此后 `minutes` 分钟内的计划时刻与下一个计划时刻同在一个 UTC 小时（离下一个整点不足时把钟再前推
/// 到整点之后）。
fn mid_hour(minutes: i64) -> Clock {
    let mut c = clock();
    if !cfg!(feature = "fault-injection") {
        return c;
    }
    let t = c.next_tick(MINUTE_US);
    let into = t.rem_euclid(HOUR_US) / MINUTE_US;
    if into == 0 || into + minutes >= 60 {
        c.offset_s += (60 - into + 1).rem_euclid(60) * 60;
    }
    c
}

/// 被测进程的钟落在某整点之前约 1 分钟加 [`LEAD`]：此后头两个计划时刻为整点前 1 分钟与整点。
fn before_hour() -> Clock {
    let mut c = clock();
    if !cfg!(feature = "fault-injection") {
        return c;
    }
    let t = c.next_tick(MINUTE_US);
    let into = t.rem_euclid(HOUR_US) / MINUTE_US;
    // 让下一个计划时刻落在整点前 1 分钟（分钟数 59）
    c.offset_s += (59 - into).rem_euclid(60) * 60;
    c
}

/// 投递到 `rx` 的场景约定规则。
fn hooked(name: &str, rx: &Receiver) -> RuleToml {
    RuleToml::new(name, &rx.url("/hook"))
}

/// 以 `clock` 的偏移、规则 `rs`、另加的配置 `extra` 与环境变量 `envs` 起 `amberlog serve` 并等到 `ready`。
fn serve(
    f: &Fixture,
    clock: Clock,
    rs: &[RuleToml],
    extra: &[Kv],
    envs: &[(&str, String)],
) -> Server {
    let r = rules(rs);
    let mut kv: Vec<Kv> = vec![("alerts.rules", Some(r.as_str()))];
    kv.extend_from_slice(extra);
    let mut cmd = f.serve(&kv);
    clock.apply(&mut cmd);
    for (k, v) in envs {
        cmd.env(k, v);
    }
    Server::start(cmd)
}

/// 规则 `rule` 在计划时刻 `t`（被测进程的钟）所在小时的评估记录对象的路径。
fn object_path(f: &Fixture, rule: &str, t: i64) -> PathBuf {
    f.store_dir().join(object_rel(rule, t))
}

/// 同 [`object_path`]，相对对象存储根目录：`amberlog/_system/alerts/<rule>/<YYYY>/<MM>/<DD>/<HH>.json`。
fn object_rel(rule: &str, t: i64) -> String {
    let s = rfc3339(t);
    format!(
        "amberlog/_system/alerts/{rule}/{}/{}/{}/{}.json",
        &s[0..4],
        &s[5..7],
        &s[8..10],
        &s[11..13]
    )
}

/// 在对象存储里直接写一个对象（相对根目录）。
fn put(f: &Fixture, rel: &str, body: &[u8]) {
    let p = f.store_dir().join(rel);
    std::fs::create_dir_all(p.parent().unwrap()).unwrap();
    std::fs::write(p, body).unwrap();
}

/// 读一个评估记录对象并解析为 JSON（不存在为 `None`）。
fn read_object(f: &Fixture, rule: &str, t: i64) -> Option<Value> {
    let bytes = std::fs::read(object_path(f, rule, t)).ok()?;
    Some(serde_json::from_slice(&bytes).expect("评估记录对象是 JSON"))
}

/// 对象中 `scheduled_at` 为 `t` 的元素。
fn element(obj: &Value, t: i64) -> Option<Value> {
    obj["evaluations"]
        .as_array()?
        .iter()
        .find(|e| e["scheduled_at"] == t)
        .cloned()
}

/// 对象各元素的 `scheduled_at`，按出现次序。
fn scheduled(obj: &Value) -> Vec<i64> {
    obj["evaluations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["scheduled_at"].as_i64().unwrap())
        .collect()
}

/// 一个合法评估记录对象的内容：`rule` 在计划时刻 `t` 有一个命中且送达的元素。
fn valid_object(rule: &str, t: i64) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "format_version": 1,
        "org": "default",
        "rule": rule,
        "evaluations": [{
            "scheduled_at": t, "started_at": t + 1, "finished_at": t + 2,
            "window_start": t - 7 * 24 * HOUR_US, "window_end": t,
            "outcome": "matched", "reason": null, "matched_rows": 1, "truncated": false,
            "delivery": "delivered", "attempts": 1, "last_http_status": 200,
        }],
    }))
    .unwrap()
}

/// 规则 `rule` 的评估记录，按 `scheduled_at` 升序。
fn rows_of(s: &Server, rule: &str) -> Vec<Value> {
    let mut rows = evaluations(s, &format!("rule = '{rule}'"));
    rows.sort_by_key(|r| r["scheduled_at"].as_i64());
    rows
}

/// 等接收端收到 `rule` 在 `t` 的请求（至多 `limit`），返回它。
fn wait_request(rx: &Receiver, rule: &str, t: i64, limit: Duration) -> Request {
    let want = rfc3339(t);
    let hit = |r: &Request| {
        r.json()["rule"] == rule && r.scheduled_at().as_deref() == Some(want.as_str())
    };
    rx.wait_until(limit, |reqs| reqs.iter().any(hit))
        .and_then(|reqs| reqs.into_iter().find(hit))
        .unwrap_or_else(|| panic!("{limit:?} 内没有收到 {rule} 在 {want} 的请求"))
}

/// 等到条件成立（至多 `limit`，每 500 毫秒查一次）；超时返回 `false`。
fn wait_for(limit: Duration, mut pred: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + limit;
    loop {
        if pred() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// SIGTERM 并等进程以 0 退出。
fn stop(s: &mut Server) {
    s.signal("TERM");
    assert_eq!(
        s.wait_exit(Duration::from_secs(60)),
        Some(Some(0)),
        "{:#?}",
        s.stderr_lines()
    );
}

/// `amberlog_alert_history_unwritten` 的值。
fn unwritten(s: &Server) -> f64 {
    metric_or_zero(s, "amberlog_alert_history_unwritten")
}

/// 对象存储请求的故障注入控制文件（`TESTBED_FAULT_STORE`，路径前缀相对 `<storage.prefix>/`）。
struct Faults(PathBuf);

impl Faults {
    fn new(f: &Fixture) -> Faults {
        let p = f.root().join("fault-store");
        std::fs::write(&p, "").unwrap();
        Faults(p)
    }

    fn env(&self) -> Vec<(&'static str, String)> {
        vec![("TESTBED_FAULT_STORE", self.0.to_str().unwrap().to_owned())]
    }

    fn set(&self, rules: &str) {
        std::fs::write(&self.0, rules).unwrap();
    }
}

/// 场景「命中且送达的评估元素」的前提：stream `app` 已提交 2 条记录，规则 `r1` 的 `window` 为 `3m`，接收端回 HTTP 200。
struct Matched {
    f: Fixture,
    rx: Receiver,
    clock: Clock,
    s: Server,
}

fn matched_setup(clock: Clock) -> Matched {
    let f = Fixture::new();
    let rx = Receiver::http();
    let s = serve(
        &f,
        clock,
        &[hooked("r1", &rx).str("window", "3m")],
        &[],
        &[],
    );
    has_data(&s, clock, "app", &[r#"{"k":1}"#, r#"{"k":2}"#]);
    Matched { f, rx, clock, s }
}

/// 场景「评估记录按规则与计划时刻的小时存放」：返回其搭建与计划时刻 T（T 的评估记录已写出）。
fn one_record() -> (Fixture, Receiver, Server, i64) {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t);
    (f, rx, s, t)
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: record-object — 评估记录对象

#[test]
fn alr_eval_object_path_by_hour() {
    let (f, _rx, _s, t) = one_record();
    assert!(
        object_path(&f, "r1", t).is_file(),
        "{}",
        object_rel("r1", t)
    );
}

#[test]
fn alr_eval_object_per_hour_split() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = before_hour();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    // 整点 H 之后两个计划时刻：H 前 1 分钟与 H
    let first = clock.next_tick(MINUTE_US);
    let h = if first.rem_euclid(HOUR_US) == 0 {
        first + HOUR_US
    } else {
        first - first.rem_euclid(HOUR_US) + HOUR_US
    };
    wait_tick(&s, clock, "r1", h - MINUTE_US);
    wait_tick(&s, clock, "r1", h);
    let before = read_object(&f, "r1", h - MINUTE_US).expect("H 前一小时的对象");
    assert!(element(&before, h - MINUTE_US).is_some(), "{before}");
    assert!(element(&before, h).is_none(), "{before}");
    let at = read_object(&f, "r1", h).expect("H 所在小时的对象");
    assert!(element(&at, h).is_some(), "{at}");
}

#[test]
fn alr_eval_object_shape() {
    let (f, _rx, _s, t) = one_record();
    let obj = read_object(&f, "r1", t).unwrap();
    let keys: BTreeSet<&str> = obj
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        keys,
        BTreeSet::from(["format_version", "org", "rule", "evaluations"])
    );
    assert_eq!(obj["format_version"], 1);
    assert_eq!(obj["org"], "default");
    assert_eq!(obj["rule"], "r1");
}

/// 可放行的暂停点（`fault::gate` 的 `hold` 模式）：第一个走到 `step` 的评估停住，[`Gate::go`] 放行。
struct Gate {
    step: &'static str,
    file: PathBuf,
}

impl Gate {
    fn new(f: &Fixture, step: &'static str) -> Gate {
        let file = f.root().join(format!("gate-{step}"));
        std::fs::write(&file, "hold").unwrap();
        Gate { step, file }
    }

    fn env(&self) -> Vec<(&'static str, String)> {
        vec![
            ("TESTBED_FAULT_GATE", self.step.to_owned()),
            (
                "TESTBED_FAULT_GATE_FILE",
                self.file.to_str().unwrap().to_owned(),
            ),
        ]
    }

    fn wait_held(&self, limit: Duration) {
        assert!(
            common::wait_file_content(&self.file, "held", limit),
            "{limit:?} 内没有停在暂停点 {}",
            self.step
        );
    }

    fn go(&self) {
        std::fs::write(&self.file, "go").unwrap();
    }
}

#[test]
fn alr_eval_entry_skipped_nulls() {
    // 场景「上一轮的查询跑过一个周期时下一轮记跳过」（REQ-AMB-ALR-RULES）
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_slots");
    let clock = mid_hour(3);
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &gate.env());
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(10));
    clock.sleep_until(t + MINUTE_US + 10 * SECOND_US);
    gate.go();
    wait_evaluation(&s, "r1", t + MINUTE_US);

    let obj = read_object(&f, "r1", t + MINUTE_US).unwrap();
    let e = element(&obj, t + MINUTE_US).unwrap();
    assert_eq!(e.as_object().unwrap().len(), 12, "{e}");
    for k in [
        "started_at",
        "matched_rows",
        "truncated",
        "last_http_status",
    ] {
        assert!(e[k].is_null(), "{k}: {e}");
    }
    assert_eq!(e["outcome"], "skipped", "{e}");
    assert_eq!(e["reason"], "previous_running", "{e}");
    assert_eq!(e["delivery"], "not_attempted", "{e}");
    assert_eq!(e["attempts"], 0, "{e}");
}

#[test]
fn alr_eval_entry_values_matched() {
    let m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    wait_tick(&m.s, m.clock, "r1", t);
    let obj = read_object(&m.f, "r1", t).unwrap();
    let e = element(&obj, t).unwrap();
    assert_eq!(e["scheduled_at"], t, "{e}");
    assert_eq!(e["window_start"], t - 3 * MINUTE_US, "{e}");
    assert_eq!(e["window_end"], t, "{e}");
    assert_eq!(e["outcome"], "matched", "{e}");
    assert!(e["reason"].is_null(), "{e}");
    assert_eq!(e["matched_rows"], 2, "{e}");
    assert_eq!(e["truncated"], false, "{e}");
    assert_eq!(e["delivery"], "delivered", "{e}");
    assert_eq!(e["attempts"], 1, "{e}");
    assert_eq!(e["last_http_status"], 200, "{e}");
    let started = e["started_at"].as_i64().unwrap();
    let finished = e["finished_at"].as_i64().unwrap();
    assert!(t <= started && started <= finished, "{e}");
    drop(m.rx);
}

#[test]
fn alr_eval_entries_sorted() {
    let m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    for k in 0..3 {
        wait_tick(&m.s, m.clock, "r1", t + k * MINUTE_US);
    }
    let hours: BTreeSet<String> = (0..3)
        .map(|k| object_rel("r1", t + k * MINUTE_US))
        .collect();
    for rel in hours {
        let obj: Value =
            serde_json::from_slice(&std::fs::read(m.f.store_dir().join(&rel)).unwrap()).unwrap();
        let times = scheduled(&obj);
        assert!(times.windows(2).all(|w| w[0] < w[1]), "{rel}: {times:?}");
    }
}

#[test]
fn alr_eval_no_secrets_in_object() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let rule = RuleToml::new("r1", &rx.url("/hook?key=abc123"))
        .str("window", "3m")
        .raw("webhook_headers", r#"{ "X-Token" = "tok-987" }"#);
    let s = serve(&f, clock, &[rule], &[], &[]);
    has_data(&s, clock, "app", &[r#"{"k":1}"#, r#"{"k":2}"#]);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t);
    let text = std::fs::read_to_string(object_path(&f, "r1", t)).unwrap();
    for secret in ["abc123", "tok-987", "/hook"] {
        assert!(!text.contains(secret), "对象含 {secret}：{text}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: write — 写出

#[test]
fn alr_eval_overwrite_appends() {
    let m = matched_setup(mid_hour(3));
    let t1 = m.clock.next_tick(MINUTE_US);
    let t2 = t1 + MINUTE_US;
    wait_tick(&m.s, m.clock, "r1", t1);
    assert_eq!(scheduled(&read_object(&m.f, "r1", t1).unwrap()), [t1]);
    wait_tick(&m.s, m.clock, "r1", t2);
    let obj = read_object(&m.f, "r1", t2).unwrap();
    assert!(
        element(&obj, t1).is_some() && element(&obj, t2).is_some(),
        "{obj}"
    );
}

#[test]
fn alr_eval_written_within_60s() {
    let m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    m.clock.sleep_until(t);
    let path = object_path(&m.f, "r1", t);
    let mut seen: Option<(i64, Value)> = None;
    assert!(
        wait_for(RECORD_WAIT, || {
            let e = std::fs::read(&path)
                .ok()
                .and_then(|b| serde_json::from_slice::<Value>(&b).ok())
                .and_then(|o| element(&o, t));
            if let Some(e) = e {
                seen = Some((m.clock.now(), e));
            }
            seen.is_some()
        }),
        "{RECORD_WAIT:?} 内对象中没有 {t} 的元素"
    );
    let (at, e) = seen.unwrap();
    let finished = e["finished_at"].as_i64().unwrap();
    assert!(
        at - finished <= 60 * SECOND_US,
        "评估结束 {} 秒后才写出",
        (at - finished) / SECOND_US
    );
}

#[test]
fn alr_eval_resume_after_restart() {
    let clock = mid_hour(4);
    let mut m = matched_setup(clock);
    let t1 = clock.next_tick(MINUTE_US);
    wait_tick(&m.s, clock, "r1", t1);
    stop(&mut m.s);
    let s = serve(
        &m.f,
        clock,
        &[hooked("r1", &m.rx).str("window", "3m")],
        &[],
        &[],
    );
    let t2 = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t2);
    assert_eq!(
        object_rel("r1", t1),
        object_rel("r1", t2),
        "T1、T2 应同在一小时"
    );
    let obj = read_object(&m.f, "r1", t2).unwrap();
    assert!(
        element(&obj, t1).is_some() && element(&obj, t2).is_some(),
        "{obj}"
    );
}

#[test]
fn alr_eval_higher_version_not_overwritten() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = mid_hour(2);
    // 启动之后第一个计划时刻 T：启动与准备数据在 LEAD 之内，T 即此刻的下一个计划时刻
    let t = clock.next_tick(MINUTE_US);
    let v2 = br#"{"format_version":2,"org":"default","rule":"r1","evaluations":[]}"#;
    put(&f, &object_rel("r1", t), v2);
    let s = serve(
        &f,
        clock,
        &[hooked("r1", &rx).str("window", "3m")],
        &[],
        &[],
    );
    has_data(&s, clock, "app", &[r#"{"k":1}"#, r#"{"k":2}"#]);
    assert_eq!(clock.next_tick(MINUTE_US), t, "准备用时超出了 LEAD");
    let req = wait_request(&rx, "r1", t, Duration::from_secs(90));
    std::thread::sleep(RECORD_WAIT.saturating_sub(Duration::from_micros(
        (common::api::now_us() - req.at_us()) as u64,
    )));
    assert_eq!(std::fs::read(object_path(&f, "r1", t)).unwrap(), v2);
    s.wait_readyz(200, Duration::from_secs(5));
}

/// 场景「写不出时留在内存，恢复后写出」：故障生效期间等过 T1、T2，断言之后解除故障。返回 (解除前的未写出条数, 解除后的未写出条数)。
fn write_fail_then_recover(check_hidden: bool) -> (f64, f64) {
    let f = Fixture::new();
    let rx = Receiver::http();
    let faults = Faults::new(&f);
    let clock = mid_hour(3);
    let s = serve(
        &f,
        clock,
        &[hooked("r1", &rx).str("window", "3m")],
        &[],
        &faults.env(),
    );
    has_data(&s, clock, "app", &[r#"{"k":1}"#, r#"{"k":2}"#]);
    faults.set("write _system/alerts/ 503\n");
    let t1 = clock.next_tick(MINUTE_US);
    let t2 = t1 + MINUTE_US;
    wait_request(&rx, "r1", t1, Duration::from_secs(120));
    wait_request(&rx, "r1", t2, Duration::from_secs(120));
    for t in [t1, t2] {
        assert_eq!(
            rx.requests()
                .iter()
                .filter(|r| r.scheduled_at().as_deref() == Some(rfc3339(t).as_str()))
                .count(),
            1,
            "{t} 的请求应恰为一次"
        );
    }
    s.wait_readyz(200, Duration::from_secs(5));
    // 等 T2 的评估结束（投递有了结果）再读未写出条数
    assert!(
        wait_for(Duration::from_secs(30), || unwritten(&s) >= 2.0),
        "amberlog_alert_history_unwritten 没有到 2：{}",
        unwritten(&s)
    );
    let before = unwritten(&s);
    if check_hidden {
        let times: Vec<i64> = rows_of(&s, "r1")
            .iter()
            .map(|r| r["scheduled_at"].as_i64().unwrap())
            .collect();
        assert!(!times.contains(&t1) && !times.contains(&t2), "{times:?}");
    }
    faults.set("");
    std::thread::sleep(Duration::from_secs(90));
    let times: Vec<i64> = rows_of(&s, "r1")
        .iter()
        .map(|r| r["scheduled_at"].as_i64().unwrap())
        .collect();
    assert!(times.contains(&t1) && times.contains(&t2), "{times:?}");
    let after = unwritten(&s);
    (before, after)
}

#[test]
fn alr_eval_write_fail_retained() {
    let (before, after) = write_fail_then_recover(false);
    assert!(before >= 2.0, "{before}");
    assert_eq!(after, 0.0);
}

#[test]
fn alr_eval_flush_on_shutdown() {
    let mut m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    m.clock.sleep_until(t);
    wait_request(&m.rx, "r1", t, Duration::from_secs(60));
    // 接收端回了 200，投递随即有结果、评估结束；1 秒内发 SIGTERM
    std::thread::sleep(Duration::from_millis(300));
    stop(&mut m.s);
    let obj = read_object(&m.f, "r1", t).expect("停机前应写出该小时的对象");
    assert!(element(&obj, t).is_some(), "{obj}");
}

#[test]
fn alr_eval_unwritten_metric() {
    let (before, after) = write_fail_then_recover(false);
    assert!(before >= 2.0, "解除故障注入之前：{before}");
    assert_eq!(after, 0.0, "解除故障注入之后");
}

#[test]
fn alr_eval_offline_does_not_write() {
    let f = Fixture::new();
    let rule = rules(&[RuleToml::new("r1", "https://127.0.0.1:9/hook")]);
    let kv: Vec<Kv> = vec![("alerts.rules", Some(rule.as_str()))];
    // stream `app` 已有已提交数据（停机时 flush 并提交），之后没有写入实例在运行
    let mut s = f.start(&kv);
    has_one(&s, Clock::REAL);
    stop(&mut s);
    let listing = || -> BTreeSet<PathBuf> {
        let root = f.store_dir().join("amberlog/_system/alerts");
        let mut out = BTreeSet::new();
        let mut dirs = vec![root];
        while let Some(d) = dirs.pop() {
            let Ok(rd) = std::fs::read_dir(&d) else {
                continue;
            };
            for e in rd.flatten() {
                let p = e.path();
                if p.is_dir() {
                    dirs.push(p);
                } else {
                    out.insert(p);
                }
            }
        }
        out
    };
    let before = listing();
    let out = f.root().join("evidence-out");
    let r = f.export(
        &kv,
        &[
            "--dataset",
            "default/app",
            "--output",
            out.to_str().unwrap(),
        ],
    );
    assert_eq!(r.code, Some(0), "{}", r.stderr);
    assert_eq!(listing(), before);
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: retention — 保留期

/// 保留期场景的实例：`alerts.history_retention` 为 `retention`、`hot_fsck.interval` 为 `1m`，规则 `r1`。
fn retention_serve(f: &Fixture, rx: &Receiver, retention: &str, clock: Clock) -> Server {
    let r = format!("\"{retention}\"");
    serve(
        f,
        clock,
        &[hooked("r1", rx)],
        &[
            ("alerts.history_retention", Some(r.as_str())),
            ("hot_fsck.interval", Some("\"1m\"")),
        ],
        &[],
    )
}

#[test]
fn alr_eval_retention_deletes_expired() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let old = common::api::now_us() - 3 * HOUR_US;
    put(
        &f,
        &object_rel("old_rule", old),
        &valid_object("old_rule", old),
    );
    put(&f, &object_rel("r1", old), &valid_object("r1", old));
    let _s = retention_serve(&f, &rx, "1h", Clock::REAL);
    assert!(
        wait_for(Duration::from_secs(15 * 60), || {
            !object_path(&f, "old_rule", old).exists() && !object_path(&f, "r1", old).exists()
        }),
        "15 分钟内两个过期对象没有都被删除"
    );
}

/// 设了 `TESTBED_SOAK_HOURS`（跑满时长的长时运行）：长时场景照场景原样等满。
fn soak_job() -> bool {
    std::env::var_os("TESTBED_SOAK_HOURS").is_some()
}

/// 场景「不删路径不合模板的对象」要等的 15 分钟：长时运行照场景等满；缺省跑缩短版——等到两个过期的合模板对象都已删除
/// （至少跑过一轮检查），再多等一个检查周期（`hot_fsck.interval` 为 `1m`）加余量，然后断言（只改执行方式，不改断言）。
#[test]
fn alr_eval_retention_only_template_paths() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let old = common::api::now_us() - 3 * HOUR_US;
    put(
        &f,
        &object_rel("old_rule", old),
        &valid_object("old_rule", old),
    );
    put(&f, &object_rel("r1", old), &valid_object("r1", old));
    put(&f, "amberlog/_system/alerts/r1/notes.txt", b"notes");
    let _s = retention_serve(&f, &rx, "1h", Clock::REAL);
    if soak_job() {
        std::thread::sleep(Duration::from_secs(15 * 60));
    } else {
        assert!(
            wait_for(Duration::from_secs(15 * 60), || {
                !object_path(&f, "old_rule", old).exists() && !object_path(&f, "r1", old).exists()
            }),
            "15 分钟内两个过期对象没有都被删除"
        );
        std::thread::sleep(Duration::from_secs(75));
    }
    assert!(
        f.store_dir()
            .join("amberlog/_system/alerts/r1/notes.txt")
            .is_file()
    );
}

/// 场景「保留期内的对象不删」要等的 70 分钟：设了 `TESTBED_SOAK_HOURS`（跑满时长的长时运行）时真等；缺省以测试时钟
/// 偏移把实例时钟前推 70 分钟重启，再等两个检查周期（只改执行方式，不改断言）。
fn soak() -> bool {
    soak_job() || !cfg!(feature = "fault-injection")
}

#[test]
fn alr_eval_retention_keeps_recent() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let mut s = retention_serve(&f, &rx, "2h", clock);
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t);
    if soak() {
        std::thread::sleep(Duration::from_secs(70 * 60));
    } else {
        stop(&mut s);
        let later = Clock {
            offset_s: clock.offset_s + 70 * 60,
        };
        let _s = retention_serve(&f, &rx, "2h", later);
        std::thread::sleep(Duration::from_secs(150));
    }
    assert!(
        object_path(&f, "r1", t).is_file(),
        "{}",
        object_rel("r1", t)
    );
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: table — 系统表 `_alert_evaluations`

#[test]
fn alr_eval_table_via_search() {
    let m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    wait_tick(&m.s, m.clock, "r1", t);
    let r = search(
        &m.s,
        "SELECT rule, outcome, delivery FROM _alert_evaluations",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let body = r.json();
    assert!(
        body["hits"]
            .as_array()
            .unwrap()
            .contains(&json!({"rule":"r1","outcome":"matched","delivery":"delivered"})),
        "{body}"
    );
    for k in ["scan_records", "scan_files", "scan_bytes"] {
        assert_eq!(body[k], 0, "{k}");
    }
}

#[test]
fn alr_eval_range_on_scheduled_at() {
    let m = matched_setup(clock());
    let t1 = m.clock.next_tick(MINUTE_US);
    let (t2, t3) = (t1 + MINUTE_US, t1 + 2 * MINUTE_US);
    for t in [t1, t2, t3] {
        wait_tick(&m.s, m.clock, "r1", t);
    }
    let r = search(
        &m.s,
        "SELECT scheduled_at FROM _alert_evaluations WHERE rule = 'r1'",
        t2,
        t3,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"], json!([{"scheduled_at": t2}]));
}

#[test]
fn alr_eval_thirteen_columns() {
    let m = matched_setup(clock());
    let t = m.clock.next_tick(MINUTE_US);
    wait_tick(&m.s, m.clock, "r1", t);
    let r = search(
        &m.s,
        "SELECT * FROM _alert_evaluations LIMIT 1",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let hit = r.json()["hits"][0].clone();
    let keys: BTreeSet<String> = hit.as_object().unwrap().keys().cloned().collect();
    let want: BTreeSet<String> = [
        "rule",
        "scheduled_at",
        "started_at",
        "finished_at",
        "window_start",
        "window_end",
        "outcome",
        "matched_rows",
        "truncated",
        "delivery",
        "attempts",
        "last_http_status",
    ]
    .map(String::from)
    .into();
    assert_eq!(keys, want, "{hit}");
    search(
        &m.s,
        "SELECT severity FROM _alert_evaluations",
        0,
        EVAL_END_US,
    )
    .assert_envelope(400, "unknown_column");
}

#[test]
fn alr_eval_unwritten_not_visible() {
    write_fail_then_recover(true);
}

#[test]
fn alr_eval_removed_rule_history() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let mut s = serve(&f, clock, &[hooked("gone", &rx)], &[], &[]);
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "gone", t);
    stop(&mut s);
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    let r = search(
        &s,
        "SELECT count(*) AS n FROM _alert_evaluations WHERE rule = 'gone'",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.json()["hits"][0]["n"].as_i64() > Some(0), "{}", r.text());
}

/// 场景「缺省按计划时刻从晚到早」的前提：`alerts.max_concurrency` 为 2，规则 `a1`、`b1` 已各有 T1 < T2 的评估记录写出。
/// 之后以不含规则的配置重启，免得 T2 之后的评估记录混进结果。返回实例与 (T1, T2)。
fn two_rules_two_ticks() -> (Fixture, Server, i64, i64) {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let mut s = serve(
        &f,
        clock,
        &[hooked("a1", &rx), hooked("b1", &rx)],
        &[("alerts.max_concurrency", Some("2"))],
        &[],
    );
    has_one(&s, clock);
    let t1 = clock.next_tick(MINUTE_US);
    let t2 = t1 + MINUTE_US;
    for t in [t1, t2] {
        wait_tick(&s, clock, "a1", t);
        wait_evaluation(&s, "b1", t);
    }
    stop(&mut s);
    let s = serve(
        &f,
        clock,
        &[],
        &[("alerts.max_concurrency", Some("2"))],
        &[],
    );
    (f, s, t1, t2)
}

fn pairs(hits: &Value) -> Vec<(String, i64)> {
    hits.as_array()
        .unwrap()
        .iter()
        .map(|h| {
            (
                h["rule"].as_str().unwrap().to_owned(),
                h["scheduled_at"].as_i64().unwrap(),
            )
        })
        .collect()
}

#[test]
fn alr_eval_default_order() {
    let (_f, s, t1, t2) = two_rules_two_ticks();
    let r = search(
        &s,
        "SELECT rule, scheduled_at FROM _alert_evaluations",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let p = |r: &str, t: i64| (r.to_owned(), t);
    assert_eq!(
        pairs(&r.json()["hits"]),
        [p("a1", t2), p("b1", t2), p("a1", t1), p("b1", t1)]
    );
}

#[test]
fn alr_eval_order_ties() {
    let (_f, s, _t1, _t2) = two_rules_two_ticks();
    let r = search(
        &s,
        "SELECT rule, scheduled_at, outcome FROM _alert_evaluations ORDER BY outcome",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    let hits = r.json()["hits"].as_array().unwrap().clone();
    for w in hits.windows(2) {
        if w[0]["outcome"] != w[1]["outcome"] {
            continue;
        }
        let key = |h: &Value| {
            (
                std::cmp::Reverse(h["scheduled_at"].as_i64().unwrap()),
                h["rule"].as_str().unwrap().to_owned(),
            )
        };
        assert!(key(&w[0]) < key(&w[1]), "{} 与 {} 的次序", w[0], w[1]);
    }
}

#[test]
fn alr_eval_named_rule_isolation() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("a1", &rx), hooked("b1", &rx)],
        &[("alerts.max_concurrency", Some("2"))],
        &[],
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "a1", t);
    wait_evaluation(&s, "b1", t);
    put(&f, &object_rel("b1", t), b"not json");
    let r = search(
        &s,
        "SELECT count(*) AS n FROM _alert_evaluations WHERE rule = 'a1'",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.json()["hits"][0]["n"].as_i64() > Some(0), "{}", r.text());
    search(
        &s,
        "SELECT count(*) AS n FROM _alert_evaluations",
        0,
        EVAL_END_US,
    )
    .assert_envelope(500, "internal");
}

#[test]
fn alr_eval_named_rule_missing_empty() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let r = search(
        &s,
        "SELECT * FROM _alert_evaluations WHERE rule = 'no_such' AND outcome = 'matched'",
        0,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"], json!([]));
}

#[test]
fn alr_eval_range_read_isolation() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = mid_hour(2);
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t);
    put(&f, &object_rel("r1", t - 2 * HOUR_US), b"not json");
    let hour_start = clock.now() - clock.now().rem_euclid(HOUR_US);
    let r = search(
        &s,
        "SELECT count(*) AS n FROM _alert_evaluations WHERE rule = 'r1'",
        hour_start,
        EVAL_END_US,
    );
    assert_eq!(r.status, 200, "{}", r.text());
    assert!(r.json()["hits"][0]["n"].as_i64() > Some(0), "{}", r.text());
}

#[test]
fn alr_eval_read_unavailable_503() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let faults = Faults::new(&f);
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &faults.env());
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t);
    faults.set("* * 503\n");
    search(&s, "SELECT * FROM _alert_evaluations", 0, EVAL_END_US)
        .assert_envelope(503, "unavailable");
}

#[test]
fn alr_eval_read_unparseable_500() {
    let f = Fixture::new();
    let s = f.start(&[]);
    let now = common::api::now_us();
    put(
        &f,
        &object_rel("r1", now),
        br#"{"format_version":2,"org":"default","rule":"r1","evaluations":[]}"#,
    );
    search(
        &s,
        "SELECT * FROM _alert_evaluations WHERE rule = 'r1'",
        0,
        EVAL_END_US,
    )
    .assert_envelope(500, "internal");
}

#[test]
fn alr_eval_read_failures_either() {
    let f = Fixture::new();
    let faults = Faults::new(&f);
    let now = common::api::now_us();
    put(&f, &object_rel("a1", now), b"not json");
    put(&f, &object_rel("b1", now), &valid_object("b1", now));
    let s = serve(&f, Clock::REAL, &[], &[], &faults.env());
    faults.set("get,head,list _system/alerts/b1/ 403\n");
    let r = search(&s, "SELECT * FROM _alert_evaluations", 0, EVAL_END_US);
    match r.status {
        500 => r.assert_envelope(500, "internal"),
        503 => r.assert_envelope(503, "unavailable"),
        other => panic!("HTTP {other}：{}", r.text()),
    };
}

#[test]
fn alr_eval_not_governance() {
    let f = Fixture::new();
    let mut s = f.start(&[]);
    // stream `app` 已有已提交数据（停机时 flush 并提交）
    has_one(&s, Clock::REAL);
    stop(&mut s);
    let s = f.start(&[]);
    let now = common::api::now_us();
    put(&f, &object_rel("r1", now), b"not json");
    for sql in [
        "SELECT count(*) AS n FROM app",
        "SELECT dataset FROM _ledger",
        "SELECT * FROM _reconciliation WHERE dataset = 'default/app'",
    ] {
        let r = search(&s, sql, 0, EVAL_END_US);
        assert_eq!(r.status, 200, "{sql}：{}", r.text());
    }
    common::api::ingest_ok(&s, "app", r#"[{"k":1}]"#);
    let metrics = s.get_auth("/metrics").text();
    assert!(
        !metrics
            .lines()
            .any(|l| l.starts_with("amberlog_dataset_disabled{")),
        "{metrics}"
    );
}
