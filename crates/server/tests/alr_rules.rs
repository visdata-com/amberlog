//! REQ-AMB-ALR-RULES：定时告警的规则写法、评估时刻与名额、查询与条件、停机与指标。
//!
//! 场景约定（块 Problem「场景约定」）：规则以 `interval` 为 `1m` 为主，`window` 为 `7d`、`webhook_url` 指向测试接收端、
//! `condition` 为 `{ rows = "> 0" }`（[`common::alerting::RuleToml`]）；「stream `app` 已有数据」见
//! [`common::alerting::has_data`]；「读取 `_alert_evaluations`」见 [`common::alerting::evaluations`]。
//! 等整分的场景以测试时钟偏移把被测进程的钟推到整分之前（[`common::alerting::Clock`]），这些测试要带 `fault-injection` 特性运行。

mod common;

use std::path::PathBuf;
use std::process::{Command, Output};
use std::time::{Duration, Instant};

use common::alerting::{
    Clock, EVAL_END_US, MINUTE_US, RECORD_WAIT, RuleToml, SECOND_US, evaluations,
    evaluations_total, has_data, has_one, metric_or_zero, rules, search, toml_str, wait_evaluation,
    wait_tick,
};
use common::gov::rfc3339;
use common::webhook::{Receiver, Reply, Request};
use common::{Fixture, Kv, Server};
use serde_json::{Value, json};

/// 校验类场景里规则的 `webhook_url`：不会真去连接（进程以退出码 2 退出，或只看 `/readyz`）。
const HOOK: &str = "https://127.0.0.1:9/hook";

/// 一次子命令运行的结果。
struct Run {
    code: i32,
    stderr: String,
}

fn run(cmd: &mut Command) -> Run {
    let Output { status, stderr, .. } = cmd.output().expect("启动 amberlog");
    Run {
        code: status.code().expect("进程应正常退出而不是被信号终止"),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    }
}

/// 进程以退出码 2 退出，stderr 含 `has` 中的每一项、不含 `starting` 事件。
fn assert_exit2(r: &Run, has: &[&str]) {
    assert_eq!(r.code, 2, "stderr:\n{}", r.stderr);
    for s in has {
        assert!(r.stderr.contains(s), "stderr 应含 {s:?}:\n{}", r.stderr);
    }
    assert!(
        !r.stderr.contains("\"event\":\"starting\""),
        "配置一步失败不应输出 starting：\n{}",
        r.stderr
    );
}

/// 场景 A 组的规则：`name`、`interval` 为 `1m`、`sql` 为 `SELECT * FROM app`、`condition` 为 `{ rows = "> 0" }`、
/// `webhook_url` 为 [`HOOK`]（不写 `window`）。
fn rule(name: &str) -> RuleToml {
    RuleToml::new(name, HOOK).without("window")
}

/// 以只含 `rs` 的 `alerts.rules` 运行 `amberlog serve`，直到退出。
fn serve_with(f: &Fixture, rs: &[RuleToml]) -> Run {
    let r = rules(rs);
    run(&mut f.serve(&[("alerts.rules", Some(&r))]))
}

/// 以只含 `rs` 的 `alerts.rules` 启动 `amberlog serve`，断言 `GET /readyz` 在 60 秒内返回 200。
fn assert_ready_with(f: &Fixture, rs: &[RuleToml]) {
    let r = rules(rs);
    let s = f.start(&[("alerts.rules", Some(&r))]);
    s.wait_readyz(200, Duration::from_secs(60));
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: rule-syntax — 规则的写法

#[test]
fn alr_rules_unknown_field_exit2() {
    let f = Fixture::new();
    let r = serve_with(&f, &[rule("r1").str("severity", "high")]);
    assert_exit2(&r, &["alerts.rules", "r1"]);
}

#[test]
fn alr_rules_missing_field_or_wrong_type_exit2() {
    let f = Fixture::new();
    for bad in [
        rule("r1").without("sql"),
        rule("r1").without("webhook_url"),
        rule("r1").raw("interval", "1"),
        rule("r1").raw("condition", "\"rows > 0\""),
        rule("r1").raw("enabled", "\"yes\""),
    ] {
        let r = serve_with(&f, std::slice::from_ref(&bad));
        assert_exit2(&r, &["alerts.rules", "r1"]);
    }
}

#[test]
fn alr_rules_bad_name_exit2() {
    let f = Fixture::new();
    let long = "a".repeat(65);
    for name in ["R1", "_r1", "a-b", "amberlog_x", long.as_str()] {
        let r = serve_with(&f, &[rule(name)]);
        assert_exit2(&r, &["alerts.rules"]);
    }
}

#[test]
fn alr_rules_duplicate_name_exit2() {
    let f = Fixture::new();
    let r = serve_with(&f, &[rule("r1"), rule("r1").raw("enabled", "false")]);
    assert_exit2(&r, &["alerts.rules", "r1"]);
}

#[test]
fn alr_rules_bad_interval_exit2() {
    let f = Fixture::new();
    for interval in ["30s", "90s", "0m", "25h", "1x"] {
        let r = serve_with(&f, &[rule("r1").str("interval", interval)]);
        assert_exit2(&r, &["alerts.rules"]);
    }
    assert_ready_with(&f, &[rule("r1").str("interval", "120s")]);
}

#[test]
fn alr_rules_bad_window_exit2() {
    let f = Fixture::new();
    for window in ["30s", "8d"] {
        let r = serve_with(
            &f,
            &[rule("r1").str("interval", "5m").str("window", window)],
        );
        assert_exit2(&r, &["alerts.rules"]);
    }
}

#[test]
fn alr_rules_org_default_only() {
    let f = Fixture::new();
    let r = serve_with(&f, &[rule("r1").str("org", "acme")]);
    assert_exit2(&r, &["alerts.rules"]);
}

#[test]
fn alr_rules_sql_static_check() {
    let f = Fixture::new();
    for sql in [
        "SELEC * FROM app",
        "SELECT * FROM a JOIN b ON a.x = b.x",
        "SELECT * FROM \"App\"",
        "SELECT severity FROM _alert_evaluations",
    ] {
        let r = serve_with(&f, &[rule("r1").str("sql", sql)]);
        assert_exit2(&r, &["alerts.rules"]);
    }
    assert_ready_with(&f, &[rule("r1").str("sql", "SELECT * FROM no_such_stream")]);
}

#[test]
fn alr_rules_bad_condition_exit2() {
    let f = Fixture::new();
    for condition in [
        r#"{ rows = "> -1" }"#,
        r#"{ rows = "~ 3" }"#,
        r#"{ rows = "> 1", column = "n" }"#,
        r#"{ column = "n", op = ">" }"#,
        r#"{ column = "n", op = "gt", threshold = 0 }"#,
        r#"{ column = "", op = ">", threshold = 0 }"#,
    ] {
        let r = serve_with(&f, &[rule("r1").raw("condition", condition)]);
        assert_exit2(&r, &["alerts.rules"]);
    }
}

#[test]
fn alr_rules_valid_condition_forms_accepted() {
    let f = Fixture::new();
    for condition in [
        r#"{ rows = ">=3" }"#,
        r#"{ rows = "!= 9223372036854775807" }"#,
        r#"{ column = "n", op = "<=", threshold = 1.5 }"#,
    ] {
        assert_ready_with(&f, &[rule("r1").raw("condition", condition)]);
    }
}

#[test]
fn alr_rules_max_100_rules() {
    let f = Fixture::new();
    let all: Vec<RuleToml> = (0..101).map(|i| rule(&format!("r{i}"))).collect();
    let r = serve_with(&f, &all);
    assert_exit2(&r, &["alerts.rules"]);
    assert_ready_with(&f, &all[..100]);
}

#[test]
fn alr_rules_missing_name_reports_index() {
    let f = Fixture::new();
    let r = serve_with(&f, &[rule("r1"), rule("r2"), rule("r3").without("name")]);
    assert_exit2(&r, &["alerts.rules", "3"]);
}

#[test]
fn alr_rules_validated_in_all_modes() {
    let f = Fixture::new();
    // 「一个已有的证据包」：先以合法配置导出一个
    {
        let mut w = f.start(&[]);
        common::api::ingest_ok(&w, "app", r#"[{"k":1}]"#);
        common::store::stop(&mut w);
    }
    let package = f.tmp("e.tar");
    let exported = f.export(
        &[],
        &[
            "--dataset",
            "default/app",
            "--output",
            package.to_str().unwrap(),
        ],
    );
    assert_eq!(exported.code, Some(0), "{}", exported.stderr);

    let bad = rules(&[rule("r1").str("interval", "30s")]);
    let overrides = [("alerts.rules", Some(bad.as_str()))];
    let r = run(&mut f.catalog(&overrides));
    assert_exit2(&r, &["alerts.rules"]);
    let r = run(&mut f.evidence_cmd(
        &overrides,
        "verify",
        &["--evidence", package.to_str().unwrap()],
    ));
    assert_exit2(&r, &["alerts.rules"]);
}

// ---------------------------------------------------------------------------------------------------------------
// 评估类场景的共用

/// 等整分时留出的提前量：被测进程启动、摄取并等可查要用掉几秒。
const LEAD: Duration = Duration::from_secs(25);

/// 被测进程的钟落在某整分之前约 [`LEAD`]。
fn clock() -> Clock {
    Clock::before_minute(LEAD)
}

/// 被测进程的钟落在某个偶数分钟的整分之前约 [`LEAD`]（周期为 `2m` 的规则在偶数分钟到期）。
fn clock_before_even_minute() -> Clock {
    let c = clock();
    if (c.next_tick(MINUTE_US) / MINUTE_US) % 2 == 0 {
        c
    } else {
        Clock {
            offset_s: c.offset_s + 60,
        }
    }
}

/// 场景约定的规则（`window` 为 `7d`），投递到 `rx`。
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

/// 可放行的暂停点（`fault::gate` 的 `hold` 模式）：第一个走到 `step` 的评估或查询停住，[`Gate::go`] 放行。
struct Gate {
    step: &'static str,
    file: PathBuf,
}

impl Gate {
    fn new(f: &Fixture, step: &'static str) -> Gate {
        let gate = Gate::idle(f, step);
        gate.arm();
        gate
    }

    /// 暂不拦截（控制文件为空）；[`Gate::arm`] 之后才停住第一个走到的。`query.after_admission` 也拦搭建数据时的查询，
    /// 要等数据可查之后再布置。
    fn idle(f: &Fixture, step: &'static str) -> Gate {
        let file = f.root().join(format!("gate-{step}"));
        std::fs::write(&file, "").unwrap();
        Gate { step, file }
    }

    fn arm(&self) {
        std::fs::write(&self.file, "hold").unwrap();
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

    /// 等有一个停在暂停点上（至多 `limit`）。
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

/// 接收端收到的请求中，`rule` 为 `rule` 者。
fn of_rule(rx: &Receiver, rule: &str) -> Vec<Request> {
    rx.requests()
        .into_iter()
        .filter(|r| r.json()["rule"] == rule)
        .collect()
}

/// 接收端是否收到 `scheduled_at` 为 `t` 的请求。
fn got_tick(rx: &Receiver, t: i64) -> bool {
    let want = rfc3339(t);
    rx.requests()
        .iter()
        .any(|r| r.scheduled_at().as_deref() == Some(want.as_str()))
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

/// 规则 `rule` 的评估记录，按 `scheduled_at` 升序。
fn rows_of(s: &Server, rule: &str) -> Vec<Value> {
    let mut rows = evaluations(s, &format!("rule = '{rule}'"));
    rows.sort_by_key(|r| r["scheduled_at"].as_i64());
    rows
}

/// 一行评估记录的整数列。
fn int(row: &Value, col: &str) -> Option<i64> {
    row[col].as_i64()
}

/// 一行评估记录的字符串列（NULL 为 `None`）。
fn text<'a>(row: &'a Value, col: &str) -> Option<&'a str> {
    row[col].as_str()
}

/// 等到被测进程的 `amberlog_search_running` 为 `n`（至多 10 秒）。
fn wait_running(s: &Server, n: f64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while metric_or_zero(s, "amberlog_search_running") != n {
        assert!(
            Instant::now() < deadline,
            "amberlog_search_running 没有到 {n}"
        );
        std::thread::sleep(Duration::from_millis(100));
    }
}

/// 一个会停在暂停点上的 `_search`（`SELECT * FROM app`，读超时放宽到 3 分钟）。
fn held_search(s: &Server) -> u16 {
    let body =
        json!({"query": {"sql": "SELECT * FROM app", "start_time": 0, "end_time": EVAL_END_US}})
            .to_string();
    common::http_within(
        s.addr,
        "POST",
        "/api/default/_search",
        &[("Authorization", common::BEARER)],
        body.as_bytes(),
        Duration::from_secs(180),
    )
    .status
}

/// 启用内置规则的一组配置项（值为 TOML 原文）：`webhook_url` 指向 `rx`，`interval` 为 `None` 时不写（缺省 `5m`）。
struct Builtin(Vec<(&'static str, String)>);

impl Builtin {
    fn kv(&self) -> Vec<Kv<'_>> {
        self.0.iter().map(|(k, v)| (*k, Some(v.as_str()))).collect()
    }
}

fn builtin_on(rx: &Receiver, interval: Option<&str>) -> Builtin {
    let mut kv = vec![
        ("alerts.missed_collection.enabled", "true".to_owned()),
        (
            "alerts.missed_collection.webhook_url",
            toml_str(&rx.url("/hook")),
        ),
    ];
    if let Some(i) = interval {
        kv.push(("alerts.missed_collection.interval", toml_str(i)));
    }
    Builtin(kv)
}

/// 下一个计划时刻离此刻（被测进程的钟）不足 `margin` 时，取再下一个。
fn tick_at_least(clock: Clock, margin: Duration) -> i64 {
    let t = clock.next_tick(MINUTE_US);
    if t - clock.now() < margin.as_micros() as i64 {
        t + MINUTE_US
    } else {
        t
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: rule-syntax（续）— 需要评估的几个

#[test]
fn alr_rules_window_defaults_to_interval() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock_before_even_minute();
    let started = clock.now();
    let s = serve(
        &f,
        clock,
        &[hooked("r2", &rx).str("interval", "2m").without("window")],
        &[],
        &[],
    );
    has_one(&s, clock);
    let reqs = rx.wait_for(1, Duration::from_secs(150));
    let body = reqs[0].json();
    let end = body["window_end"].as_str().unwrap();
    let first = common::alerting::next_tick_after(started - 1, 2 * MINUTE_US);
    let t = (0..3)
        .map(|k| first + k * 2 * MINUTE_US)
        .find(|t| rfc3339(*t) == end)
        .unwrap_or_else(|| panic!("window_end 不是启动后的计划时刻：{body}"));
    assert_eq!(
        body["window_start"],
        json!(rfc3339(t - 2 * MINUTE_US)),
        "{body}"
    );
}

#[test]
fn alr_rules_disabled_rule_not_evaluated() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("off", &rx).raw("enabled", "false")],
        &[],
        &[],
    );
    has_one(&s, clock);
    let t2 = clock.next_tick(MINUTE_US) + MINUTE_US;
    clock.sleep_until(t2 + 10 * SECOND_US);
    assert!(rx.requests().is_empty(), "{:?}", rx.requests());
    assert!(rows_of(&s, "off").is_empty());
    let text = s.get_auth("/metrics").text();
    assert!(!text.contains("rule=\"off\""), "{text}");
    drop(s);

    let r = rules(&[hooked("off", &rx)
        .raw("enabled", "false")
        .str("interval", "30s")]);
    let r = run(&mut f.serve(&[("alerts.rules", Some(&r))]));
    assert_eq!(r.code, 2, "{}", r.stderr);
}

#[test]
fn alr_rules_no_hot_reload() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t1 = clock.next_tick(MINUTE_US);
    wait_request(&rx, "r1", t1, Duration::from_secs(90));

    // 改写配置文件中该规则的条件为永不命中
    let changed = rules(&[hooked("r1", &rx).raw("condition", r#"{ rows = "< 0" }"#)]);
    f.write_config(&[("alerts.rules", Some(&changed))]);
    for t in [t1 + MINUTE_US, t1 + 2 * MINUTE_US] {
        wait_request(&rx, "r1", t, Duration::from_secs(150));
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: schedule-and-slots — 评估时刻与名额

#[test]
fn alr_rules_catalog_does_not_evaluate() {
    let f = Fixture::new();
    // stream `app` 已入湖
    {
        let mut w = f.start(&common::evidence::W);
        f.laked(&w);
        common::store::stop(&mut w);
    }
    let rx = Receiver::http();
    let clock = clock();
    let r = rules(&[hooked("r1", &rx)]);
    let mut cmd = f.catalog(&[("alerts.rules", Some(&r))]);
    clock.apply(&mut cmd);
    let c = Server::start(cmd);
    let t2 = clock.next_tick(MINUTE_US) + MINUTE_US;
    clock.sleep_until(t2 + 15 * SECOND_US);
    assert!(rx.requests().is_empty(), "{:?}", rx.requests());
    let alerts = f.store_dir().join("amberlog/_system/alerts");
    let objects = common::lake::keys(&alerts);
    assert!(objects.is_empty(), "{objects:?}");
    drop(c);
}

#[test]
fn alr_rules_first_tick_aligned_utc() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = Clock::at_second(20);
    let started = clock.now();
    let s = serve(
        &f,
        clock,
        &[hooked("r2", &rx).str("interval", "2m")],
        &[],
        &[],
    );
    has_one(&s, clock);
    let first = common::alerting::next_tick_after(started - 1, 2 * MINUTE_US);
    let ticks = [first, first + 2 * MINUTE_US, first + 4 * MINUTE_US];
    wait_tick(&s, clock, "r2", ticks[2]);
    let rows = rows_of(&s, "r2");
    let got: Vec<i64> = rows.iter().filter_map(|r| int(r, "scheduled_at")).collect();
    assert_eq!(&got[..3], &ticks, "{rows:?}");
    assert!(got.iter().all(|t| *t >= started), "{got:?}");
    for t in got {
        assert_eq!(t % (2 * MINUTE_US), 0, "偶数分钟的整分：{t}");
    }
}

#[test]
fn alr_rules_no_catchup_after_restart() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let rs = [hooked("r1", &rx)];
    let mut s = serve(&f, clock, &rs, &[], &[]);
    has_one(&s, clock);
    let t1 = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", t1);
    common::store::stop(&mut s);
    let stopped = clock.now();
    std::thread::sleep(Duration::from_secs(180));
    let s = serve(&f, clock, &rs, &[], &[]);
    let reopened = clock.now();
    let next = clock.next_tick(MINUTE_US);
    wait_tick(&s, clock, "r1", next);
    let rows = rows_of(&s, "r1");
    assert!(
        rows.iter()
            .filter_map(|r| int(r, "scheduled_at"))
            .all(|t| !(stopped..reopened).contains(&t)),
        "停机期间的计划时刻不补评估：stopped {stopped} reopened {reopened} {rows:?}"
    );
}

#[test]
fn alr_rules_one_evaluation_per_tick() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t5 = clock.next_tick(MINUTE_US) + 4 * MINUTE_US;
    wait_tick(&s, clock, "r1", t5);
    let got: Vec<i64> = rows_of(&s, "r1")
        .iter()
        .filter_map(|r| int(r, "scheduled_at"))
        .collect();
    let distinct: std::collections::BTreeSet<i64> = got.iter().copied().collect();
    assert_eq!(distinct.len(), got.len(), "{got:?}");
    assert!(got.len() >= 5, "{got:?}");
}

#[test]
fn alr_rules_deadline_does_not_skip_next() {
    let f = Fixture::new();
    let rx = Receiver::http();
    rx.always(Reply::status(503).delay(Duration::from_secs(9)).into());
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t3 = clock.next_tick(MINUTE_US) + 2 * MINUTE_US;
    // 第三轮的投递拖到它的截止时刻（再一个周期）才结束
    wait_tick(&s, clock, "r1", t3);
    let rows = rows_of(&s, "r1");
    for r in &rows {
        assert_ne!(
            (text(r, "outcome"), text(r, "reason")),
            (Some("skipped"), Some("previous_running")),
            "{rows:?}"
        );
        if text(r, "outcome") == Some("matched") {
            assert_eq!(text(r, "delivery"), Some("abandoned"), "{rows:?}");
        }
    }
}

#[test]
fn alr_rules_previous_running_skipped() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_slots");
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &gate.env());
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(10));
    clock.sleep_until(t + MINUTE_US + 10 * SECOND_US);
    gate.go();
    let next = wait_evaluation(&s, "r1", t + MINUTE_US);
    assert_eq!(text(&next, "outcome"), Some("skipped"), "{next}");
    assert_eq!(text(&next, "reason"), Some("previous_running"), "{next}");
    assert_eq!(int(&next, "started_at"), None, "{next}");
    let first = wait_evaluation(&s, "r1", t);
    assert_eq!(text(&first, "outcome"), Some("matched"), "{first}");
}

#[test]
fn alr_rules_tick_latency() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t3 = clock.next_tick(MINUTE_US) + 2 * MINUTE_US;
    wait_tick(&s, clock, "r1", t3);
    for r in rows_of(&s, "r1") {
        let lag = int(&r, "started_at").unwrap() - int(&r, "scheduled_at").unwrap();
        assert!(lag <= 2_000_000, "{r}");
    }
}

#[test]
fn alr_rules_alert_slot_queue_in_order() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("a1", &rx), hooked("a2", &rx)], &[], &[]);
    has_one(&s, clock);
    let t1 = clock.next_tick(MINUTE_US);
    for t in [t1, t1 + MINUTE_US] {
        let a1 = wait_request(&rx, "a1", t, Duration::from_secs(150));
        let a2 = wait_request(&rx, "a2", t, Duration::from_secs(30));
        assert!(a1.at <= a2.at, "a1 的请求先到");
        let want = rfc3339(t);
        for rule in ["a1", "a2"] {
            let n = of_rule(&rx, rule)
                .iter()
                .filter(|r| r.scheduled_at().as_deref() == Some(want.as_str()))
                .count();
            assert_eq!(n, 1, "{rule} 在 {want} 恰一次");
        }
    }
    wait_evaluation(&s, "a2", t1 + MINUTE_US);
    let full = evaluations(&s, "reason = 'alert_slots_full'");
    assert!(full.is_empty(), "{full:?}");
}

#[test]
fn alr_rules_alert_slot_wait_until_next_tick() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_slots");
    let clock = clock_before_even_minute();
    let s = serve(
        &f,
        clock,
        &[hooked("a1", &rx), hooked("b1", &rx).str("interval", "2m")],
        &[],
        &gate.env(),
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    assert_eq!(t % (2 * MINUTE_US), 0);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(10));
    clock.sleep_until(t + 2 * MINUTE_US + 10 * SECOND_US);
    gate.go();
    let b1 = wait_evaluation(&s, "b1", t);
    assert_eq!(text(&b1, "outcome"), Some("skipped"), "{b1}");
    assert_eq!(text(&b1, "reason"), Some("alert_slots_full"), "{b1}");
    let b1_next = wait_evaluation(&s, "b1", t + 2 * MINUTE_US);
    assert_ne!(
        text(&b1_next, "reason"),
        Some("previous_running"),
        "{b1_next}"
    );
}

#[test]
fn alr_rules_start_latency() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_slots");
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &gate.env());
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(10));
    let released = clock.now();
    gate.go();
    let row = wait_evaluation(&s, "r1", t);
    let started = int(&row, "started_at").unwrap();
    assert!(
        (released..=released + SECOND_US).contains(&started),
        "R {released} started_at {started}"
    );
}

/// 场景「查询执行名额已满时评估记跳过」的搭建与步骤：`query.max_concurrency` 为 1，一个 `_search` 在计划时刻 T 之前
/// 10 秒占住唯一的名额、停在 `query.after_admission`，到 T 加 30 秒才放行。返回实例、接收端与 T。
fn query_slots_taken(
    f: &Fixture,
    rs: impl Fn(&Receiver) -> Vec<RuleToml>,
    builtin: bool,
) -> (Server, Receiver, i64, Clock) {
    let rx = Receiver::http();
    let gate = Gate::idle(f, "query.after_admission");
    let clock = Clock::before_minute(Duration::from_secs(40));
    let builtin = builtin.then(|| builtin_on(&rx, Some("1m")));
    let mut extra: Vec<Kv> = vec![("query.max_concurrency", Some("1"))];
    extra.extend(builtin.iter().flat_map(|b| b.kv()));
    let s = serve(f, clock, &rs(&rx), &extra, &gate.env());
    has_one(&s, clock);
    gate.arm();
    let t = tick_at_least(clock, Duration::from_secs(15));
    clock.sleep_until(t - 10 * SECOND_US);
    std::thread::scope(|sc| {
        let held = sc.spawn(|| held_search(&s));
        wait_running(&s, 1.0);
        clock.sleep_until(t + 30 * SECOND_US);
        gate.go();
        assert_eq!(held.join().unwrap(), 200);
    });
    (s, rx, t, clock)
}

#[test]
fn alr_rules_query_slots_full_skipped() {
    let f = Fixture::new();
    let (s, rx, t, _) = query_slots_taken(&f, |rx| vec![hooked("r1", rx)], false);
    let row = wait_evaluation(&s, "r1", t);
    assert_eq!(text(&row, "outcome"), Some("skipped"), "{row}");
    assert_eq!(text(&row, "reason"), Some("query_slots_full"), "{row}");
    assert!(int(&row, "started_at").is_some(), "{row}");
    assert!(!got_tick(&rx, t));
    assert_eq!(evaluations_total(&s, "r1", "skipped"), 1.0);
}

#[test]
fn alr_rules_skipped_has_record() {
    let f = Fixture::new();
    let (s, _rx, t, clock) = query_slots_taken(&f, |rx| vec![hooked("r1", rx)], false);
    wait_evaluation(&s, "r1", t);
    let rows = rows_of(&s, "r1");
    let got: Vec<i64> = rows.iter().filter_map(|r| int(r, "scheduled_at")).collect();
    let first = got[0];
    let want: Vec<i64> = (0..got.len() as i64)
        .map(|k| first + k * MINUTE_US)
        .collect();
    assert_eq!(
        got, want,
        "自第一个计划时刻起每个计划时刻恰有一行：{rows:?}"
    );
    assert!(got.contains(&t));
    assert!(
        rows.iter().any(|r| text(r, "outcome") == Some("skipped")),
        "{rows:?}"
    );
    let _ = clock;
}

#[test]
fn alr_rules_pre_admission_error_no_slot() {
    let f = Fixture::new();
    let (s, _rx, t, _) = query_slots_taken(
        &f,
        |rx| vec![hooked("x", rx).str("sql", "SELECT * FROM no_such")],
        false,
    );
    let row = wait_evaluation(&s, "x", t);
    assert_eq!(text(&row, "outcome"), Some("error"), "{row}");
    assert_eq!(text(&row, "reason"), Some("stream_not_found"), "{row}");
}

#[test]
fn alr_rules_latency_10s() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t3 = clock.next_tick(MINUTE_US) + 2 * MINUTE_US;
    wait_tick(&s, clock, "r1", t3);
    let reqs = of_rule(&rx, "r1");
    assert!(!reqs.is_empty());
    for req in reqs {
        let sched = req.scheduled_at().unwrap();
        let row = rows_of(&s, "r1")
            .into_iter()
            .find(|r| rfc3339(int(r, "scheduled_at").unwrap()) == sched)
            .unwrap();
        let lag = clock.to_process(req.at_us()) - int(&row, "scheduled_at").unwrap();
        assert!(lag <= 11 * SECOND_US, "{sched}: {lag}");
    }
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: query-and-condition — 查询与条件

#[test]
fn alr_rules_query_range_window() {
    let f = Fixture::new();
    let rx = Receiver::http();
    // 三条记录都要在 T 之前 30 秒以上摄取
    let clock = Clock::before_minute(Duration::from_secs(55));
    let s = serve(
        &f,
        clock,
        &[hooked("r5", &rx)
            .str("window", "5m")
            .str("sql", "SELECT seq FROM app")],
        &[],
        &[],
    );
    let t = clock.next_tick(MINUTE_US);
    let recs = [
        json!({"seq": 1, "_timestamp": t - 6 * MINUTE_US}),
        json!({"seq": 2, "_timestamp": t - SECOND_US}),
        json!({"seq": 3, "_timestamp": t + SECOND_US}),
    ];
    common::api::ingest_ok(&s, "app", &Value::Array(recs.to_vec()).to_string());
    assert!(t - clock.now() >= 30 * SECOND_US, "摄取晚了");
    let req = wait_request(&rx, "r5", t, Duration::from_secs(120));
    let body = req.json();
    assert_eq!(
        body["window_start"],
        json!(rfc3339(t - 5 * MINUTE_US)),
        "{body}"
    );
    assert_eq!(body["window_end"], json!(rfc3339(t)), "{body}");
    assert_eq!(body["rows"], json!([{"seq": 2}]), "{body}");
}

#[test]
fn alr_rules_result_matches_search() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let started = clock.now();
    let sql = "SELECT count(*) AS n FROM app WHERE level = 'error'";
    let s = serve(
        &f,
        clock,
        &[hooked("e", &rx)
            .str("sql", sql)
            .raw("condition", r#"{ column = "n", op = ">=", threshold = 2 }"#)],
        &[],
        &[],
    );
    has_data(
        &s,
        clock,
        "app",
        &[
            r#"{"level":"error"}"#,
            r#"{"level":"info"}"#,
            r#"{"level":"error"}"#,
        ],
    );
    let req = rx.wait_for(1, Duration::from_secs(120)).remove(0);
    let body = req.json();
    assert_eq!(body["rows"], json!([{"n": 2}]), "{body}");
    assert_eq!(body["matched_rows"], json!(1), "{body}");
    // 该请求的计划时刻 T：启动之后的头几个整分之一；评估的查询范围为 [T − 7d, T)
    let first = common::alerting::next_tick_after(started - 1, MINUTE_US);
    let t = (0..3)
        .map(|k| first + k * MINUTE_US)
        .find(|t| body["scheduled_at"] == json!(rfc3339(*t)))
        .unwrap_or_else(|| panic!("scheduled_at 不是启动后的计划时刻：{body}"));
    let r = search(&s, sql, t - 7 * 24 * 60 * MINUTE_US, t);
    assert_eq!(r.status, 200, "{}", r.text());
    assert_eq!(r.json()["hits"], json!([{"n": 2}]));
}

#[test]
fn alr_rules_not_counted_as_search() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let m = hooked("m", &rx).str("sql", "SELECT a FROM app ORDER BY a");
    let mut s = serve(
        &f,
        clock,
        std::slice::from_ref(&m),
        &[("query.max_memory_bytes", Some("1"))],
        &[],
    );
    // 内存上限为 1 时 count(*) 也会越限，这里不经 has_data 等可查：已确认的记录随即在可见集里
    let recs: Vec<Value> = (0..1000).map(|i| json!({"a": i})).collect();
    common::api::ingest_ok(&s, "app", &Value::Array(recs).to_string());
    let series = "amberlog_search_rejected_total{reason=\"query_memory_exceeded\"}";
    let before = metric_or_zero(&s, series);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    // 同一实例上读 `_alert_evaluations` 本身受内存上限约束（REQ-AMB-QRY-RESOURCE-LIMITS-V2），以评估指标判定这一轮结束
    let deadline = Instant::now() + RECORD_WAIT;
    while evaluations_total(&s, "m", "error") < 1.0 {
        assert!(
            Instant::now() < deadline,
            "{RECORD_WAIT:?} 内 m 没有以 error 结束"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    assert_eq!(metric_or_zero(&s, series), before);
    // 以缺省的内存上限、同一对象存储重启后读评估记录（停机时写出，REQ-AMB-ALR-EVALUATIONS）
    common::store::stop(&mut s);
    drop(s);
    let s = serve(&f, clock, &[m], &[], &[]);
    let row = wait_evaluation(&s, "m", t);
    assert_eq!(text(&row, "outcome"), Some("error"), "{row}");
    assert_eq!(text(&row, "reason"), Some("query_memory_exceeded"), "{row}");
}

#[test]
fn alr_rules_truncated_result() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("t", &rx).raw("condition", r#"{ rows = "> 12" }"#)],
        &[
            ("query.max_size", Some("10")),
            ("query.default_size", Some("10")),
        ],
        &[],
    );
    let recs: Vec<String> = (0..15).map(|i| format!(r#"{{"k":{i}}}"#)).collect();
    let recs: Vec<&str> = recs.iter().map(String::as_str).collect();
    has_data_small(&s, clock, &recs);
    let row = wait_tick(&s, clock, "t", clock.next_tick(MINUTE_US));
    assert_eq!(text(&row, "outcome"), Some("not_matched"), "{row}");
    assert_eq!(row["truncated"], json!(true), "{row}");
}

/// `query.max_size` 很小时的「已有数据」：摄取后以 `count(*)` 等可查（聚合只出一行，不受 `size` 影响）。
fn has_data_small(s: &Server, clock: Clock, recs: &[&str]) {
    let now = clock.now();
    let body: Vec<Value> = recs
        .iter()
        .map(|r| {
            let mut v: Value = serde_json::from_str(r).unwrap();
            v["_timestamp"] = json!(now);
            v
        })
        .collect();
    common::api::ingest_ok(s, "app", &Value::Array(body).to_string());
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let body = json!({"query": {"sql": "SELECT count(*) AS n FROM app", "start_time": 0, "end_time": EVAL_END_US, "size": 1}}).to_string();
        let r = s.request(
            "POST",
            "/api/default/_search",
            &[("Authorization", common::BEARER)],
            body.as_bytes(),
        );
        if r.status == 200 && r.json()["hits"][0]["n"].as_u64() >= Some(recs.len() as u64) {
            return;
        }
        assert!(Instant::now() < deadline, "{}", r.text());
        std::thread::sleep(Duration::from_millis(250));
    }
}

#[test]
fn alr_rules_rows_condition() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[
            hooked("r_gt", &rx).raw("condition", r#"{ rows = "> 2" }"#),
            hooked("r_eq", &rx).raw("condition", r#"{ rows = "= 3" }"#),
            hooked("r_lt", &rx).raw("condition", r#"{ rows = "< 3" }"#),
        ],
        &[("alerts.max_concurrency", Some("3"))],
        &[],
    );
    has_data(
        &s,
        clock,
        "app",
        &[r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":3}"#],
    );
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    for (rule, want) in [
        ("r_gt", "matched"),
        ("r_eq", "matched"),
        ("r_lt", "not_matched"),
    ] {
        let row = wait_evaluation(&s, rule, t);
        assert_eq!(text(&row, "outcome"), Some(want), "{rule}: {row}");
    }
}

#[test]
fn alr_rules_column_condition_any_row() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let sql = "SELECT host, lat FROM app";
    let s = serve(
        &f,
        clock,
        &[
            hooked("lat", &rx).str("sql", sql).raw(
                "condition",
                r#"{ column = "lat", op = ">", threshold = 200 }"#,
            ),
            hooked("hi", &rx).str("sql", sql).raw(
                "condition",
                r#"{ column = "lat", op = ">", threshold = 300 }"#,
            ),
            hooked("z", &rx)
                .str("sql", "SELECT host, lat FROM app WHERE lat > 1000")
                .raw(
                    "condition",
                    r#"{ column = "lat", op = ">", threshold = 0 }"#,
                ),
        ],
        &[("alerts.max_concurrency", Some("3"))],
        &[],
    );
    has_data(
        &s,
        clock,
        "app",
        &[
            r#"{"host":"a","lat":10}"#,
            r#"{"host":"b","lat":250}"#,
            r#"{"host":"c"}"#,
        ],
    );
    let t = clock.next_tick(MINUTE_US);
    let req = wait_request(&rx, "lat", t, Duration::from_secs(120));
    assert_eq!(req.json()["rows"], json!([{"host": "b", "lat": 250}]));
    for rule in ["hi", "z"] {
        let row = wait_evaluation(&s, rule, t);
        assert_eq!(text(&row, "outcome"), Some("not_matched"), "{rule}: {row}");
    }
}

#[test]
fn alr_rules_condition_column_missing() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let cond = r#"{ column = "lat", op = ">", threshold = 0 }"#;
    let s = serve(
        &f,
        clock,
        &[
            hooked("cm", &rx)
                .str("sql", "SELECT host FROM app")
                .raw("condition", cond),
            hooked("cm0", &rx)
                .str("sql", "SELECT host FROM app WHERE host = 'zzz'")
                .raw("condition", cond),
        ],
        &[("alerts.max_concurrency", Some("2"))],
        &[],
    );
    has_data(&s, clock, "app", &[r#"{"host":"a","lat":10}"#]);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    for rule in ["cm", "cm0"] {
        let row = wait_evaluation(&s, rule, t);
        assert_eq!(text(&row, "outcome"), Some("error"), "{rule}: {row}");
        assert_eq!(
            text(&row, "reason"),
            Some("condition_column_missing"),
            "{rule}: {row}"
        );
    }
}

#[test]
fn alr_rules_condition_column_not_numeric() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("cn", &rx).str("sql", "SELECT host FROM app").raw(
            "condition",
            r#"{ column = "host", op = ">", threshold = 0 }"#,
        )],
        &[],
        &[],
    );
    has_data(&s, clock, "app", &[r#"{"host":"a","lat":10}"#]);
    let row = wait_tick(&s, clock, "cn", clock.next_tick(MINUTE_US));
    assert_eq!(text(&row, "outcome"), Some("error"), "{row}");
    assert_eq!(
        text(&row, "reason"),
        Some("condition_column_not_numeric"),
        "{row}"
    );
}

#[test]
fn alr_rules_outcomes_and_metric() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[
            hooked("m", &rx),
            hooked("n", &rx).raw("condition", r#"{ rows = "> 5" }"#),
            hooked("x", &rx).str("sql", "SELECT * FROM no_such"),
        ],
        &[("alerts.max_concurrency", Some("3"))],
        &[],
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    let m = wait_evaluation(&s, "m", t);
    assert_eq!(
        (text(&m, "outcome"), m.get("reason")),
        (Some("matched"), None),
        "{m}"
    );
    let n = wait_evaluation(&s, "n", t);
    assert_eq!(
        (text(&n, "outcome"), n.get("reason")),
        (Some("not_matched"), None),
        "{n}"
    );
    let x = wait_evaluation(&s, "x", t);
    assert_eq!(text(&x, "outcome"), Some("error"), "{x}");
    assert_eq!(evaluations_total(&s, "m", "matched"), 1.0);
    assert_eq!(evaluations_total(&s, "n", "not_matched"), 1.0);
    assert_eq!(evaluations_total(&s, "x", "error"), 1.0);
}

#[test]
fn alr_rules_query_error_reason() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("x", &rx).str("sql", "SELECT * FROM no_such")],
        &[],
        &[],
    );
    let row = wait_tick(&s, clock, "x", clock.next_tick(MINUTE_US));
    assert_eq!(text(&row, "outcome"), Some("error"), "{row}");
    assert_eq!(text(&row, "reason"), Some("stream_not_found"), "{row}");
    assert!(rx.requests().is_empty());
}

#[test]
fn alr_rules_object_store_unavailable_error() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let control = f.root().join("store-faults");
    let clock = Clock::before_minute(Duration::from_secs(40));
    let s = serve(
        &f,
        clock,
        &[hooked("u", &rx)],
        &[("flush.interval", Some("\"1s\""))],
        &[("TESTBED_FAULT_STORE", control.to_str().unwrap().to_owned())],
    );
    has_one(&s, clock);
    // 已提交进快照，查询要读对象存储
    std::thread::sleep(Duration::from_secs(3));
    let t = tick_at_least(clock, Duration::from_secs(10));
    clock.sleep_until(t - 5 * SECOND_US);
    std::fs::write(&control, "* * 503\n").unwrap();
    clock.sleep_until(t + 10 * SECOND_US);
    std::fs::write(&control, "").unwrap();
    let row = wait_evaluation(&s, "u", t);
    assert_eq!(text(&row, "outcome"), Some("error"), "{row}");
    assert_eq!(text(&row, "reason"), Some("unavailable"), "{row}");
}

#[test]
fn alr_rules_deliver_only_on_match() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[
            hooked("yes", &rx),
            hooked("no", &rx).raw("condition", r#"{ rows = "> 5" }"#),
        ],
        &[],
        &[],
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    wait_request(&rx, "yes", t, Duration::from_secs(120));
    wait_evaluation(&s, "no", t);
    assert_eq!(of_rule(&rx, "yes").len(), 1);
    assert!(of_rule(&rx, "no").is_empty());
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: shutdown-ready-ingest — 停机，与就绪、摄取的关系

#[test]
fn alr_rules_no_new_after_shutdown() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::idle(&f, "query.after_admission");
    let clock = Clock::before_minute(Duration::from_secs(40));
    let rs = [hooked("r1", &rx)];
    let mut s = serve(
        &f,
        clock,
        &rs,
        &[("server.shutdown_timeout", Some("\"90s\""))],
        &gate.env(),
    );
    has_one(&s, clock);
    gate.arm();
    let t = tick_at_least(clock, Duration::from_secs(10));
    clock.sleep_until(t - 5 * SECOND_US);
    std::thread::scope(|sc| {
        let s_ref = &s;
        let held = sc.spawn(move || held_search(s_ref));
        wait_running(&s, 1.0);
        s.signal("TERM");
        clock.sleep_until(t + 10 * SECOND_US);
        gate.go();
        let _ = held.join();
    });
    assert_eq!(s.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    assert!(!got_tick(&rx, t));
    let s = serve(&f, clock, &rs, &[], &[]);
    let rows = evaluations(&s, &format!("scheduled_at = {t}"));
    assert!(rows.is_empty(), "{rows:?}");
}

#[test]
fn alr_rules_shutdown_abandons_delivery() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.before_delivery");
    let clock = clock();
    let rs = [hooked("r1", &rx)];
    let mut s = serve(
        &f,
        clock,
        &rs,
        &[("server.shutdown_timeout", Some("\"10s\""))],
        &gate.env(),
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(15));
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(20)), Some(Some(0)));
    assert!(!got_tick(&rx, t));
    let s = serve(&f, clock, &rs, &[], &[]);
    let row = wait_evaluation(&s, "r1", t);
    assert_eq!(text(&row, "outcome"), Some("matched"), "{row}");
    assert_eq!(text(&row, "delivery"), Some("abandoned"), "{row}");
    assert_eq!(int(&row, "attempts"), Some(0), "{row}");
}

#[test]
fn alr_rules_shutdown_skipped_vs_error() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_admission");
    let clock = clock();
    let rs = [hooked("a1", &rx), hooked("a2", &rx)];
    let mut s = serve(
        &f,
        clock,
        &rs,
        &[("alerts.max_concurrency", Some("1"))],
        &gate.env(),
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(15));
    s.signal("TERM");
    assert_eq!(s.wait_exit(Duration::from_secs(60)), Some(Some(0)));
    let s = serve(&f, clock, &rs, &[], &[]);
    let a1 = wait_evaluation(&s, "a1", t);
    assert_eq!(text(&a1, "outcome"), Some("error"), "{a1}");
    assert_eq!(text(&a1, "reason"), Some("shutting_down"), "{a1}");
    let a2 = wait_evaluation(&s, "a2", t);
    assert_eq!(text(&a2, "outcome"), Some("skipped"), "{a2}");
    assert_eq!(text(&a2, "reason"), Some("shutting_down"), "{a2}");
    assert_eq!(int(&a2, "started_at"), None, "{a2}");
    assert!(!got_tick(&rx, t));
}

#[test]
fn alr_rules_readyz_unaffected() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[
            RuleToml::new("r1", "http://127.0.0.1:9/hook"),
            hooked("x", &rx).str("sql", "SELECT * FROM no_such"),
        ],
        &[],
        &[],
    );
    has_one(&s, clock);
    let t2 = clock.next_tick(MINUTE_US) + MINUTE_US;
    while clock.now() < t2 + 15 * SECOND_US {
        let r = s.get("/readyz");
        assert_eq!(r.status, 200, "{}", r.text());
        assert_eq!(metric_or_zero(&s, "amberlog_ready"), 1.0);
        std::thread::sleep(Duration::from_secs(10));
    }
}

#[test]
fn alr_rules_ingest_unaffected() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let gate = Gate::new(&f, "alert.after_admission");
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[hooked("r1", &rx)],
        &[("query.max_concurrency", Some("1"))],
        &gate.env(),
    );
    has_one(&s, clock);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    gate.wait_held(Duration::from_secs(15));
    let r = common::api::ingest(&s, "app", r#"[{"k":"busy"}]"#);
    assert_eq!(r.status, 200, "{}", r.text());
    let w = common::api::minute_of(clock.now()) - MINUTE_US;
    let r = common::api::sent(
        &s,
        "app",
        &format!("[{}]", common::api::sent_element("fw-1", w, 1)),
    );
    assert_eq!(r.status, 200, "{}", r.text());
    gate.go();
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: metrics — 指标

#[test]
fn alr_rules_evaluation_metric() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(&f, clock, &[hooked("r1", &rx)], &[], &[]);
    has_one(&s, clock);
    let t3 = clock.next_tick(MINUTE_US) + 2 * MINUTE_US;
    wait_tick(&s, clock, "r1", t3);
    // 第三轮之后、下一轮之前读：两边计的是同一批评估
    let matched = rows_of(&s, "r1")
        .iter()
        .filter(|r| text(r, "outcome") == Some("matched"))
        .count();
    assert_eq!(evaluations_total(&s, "r1", "matched"), matched as f64);
}

#[test]
fn alr_rules_metric_series_init() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let s = serve(
        &f,
        clock,
        &[
            hooked("r1", &rx),
            hooked("off", &rx).raw("enabled", "false"),
        ],
        &[],
        &[],
    );
    let text = s.get_auth("/metrics").text();
    assert!(
        clock.now() < clock.next_tick(MINUTE_US),
        "在第一个计划时刻之前读"
    );
    for outcome in ["matched", "not_matched", "skipped", "error"] {
        let line =
            format!("amberlog_alert_evaluations_total{{rule=\"r1\",outcome=\"{outcome}\"}} 0\n");
        assert!(text.contains(&line), "缺 {line}：{text}");
    }
    assert!(!text.contains("rule=\"off\""), "{text}");
}

// ---------------------------------------------------------------------------------------------------------------
// 规则: builtin-rule — 内置规则

const BUILTIN: &str = "missed_collection";

#[test]
fn alr_rules_builtin_validation_exit2() {
    let f = Fixture::new();
    let on = ("alerts.missed_collection.enabled", Some("true"));
    let r = run(&mut f.serve(&[on]));
    assert_exit2(&r, &["alerts.missed_collection"]);
    let ftp = toml_str("ftp://127.0.0.1/hook");
    let r = run(&mut f.serve(&[("alerts.missed_collection.webhook_url", Some(&ftp))]));
    assert_exit2(&r, &["alerts.missed_collection"]);
    let r = serve_with(&f, &[rule(BUILTIN)]);
    assert_exit2(&r, &["alerts.rules"]);
}

#[test]
fn alr_rules_builtin_not_counted_in_max_rules() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let all: Vec<RuleToml> = (0..100).map(|i| rule(&format!("r{i}"))).collect();
    let r = rules(&all);
    let b = builtin_on(&rx, None);
    let mut kv: Vec<Kv> = vec![("alerts.rules", Some(r.as_str()))];
    kv.extend(b.kv());
    let s = f.start(&kv);
    s.wait_readyz(200, Duration::from_secs(60));
}

#[test]
fn alr_rules_builtin_validated_in_catalog() {
    let f = Fixture::new();
    let r = run(&mut f.catalog(&[("alerts.missed_collection.enabled", Some("true"))]));
    assert_exit2(&r, &["alerts.missed_collection"]);
}

#[test]
fn alr_rules_builtin_no_query_slot() {
    let f = Fixture::new();
    let (s, _rx, t, _) = query_slots_taken(&f, |_| Vec::new(), true);
    let row = wait_evaluation(&s, BUILTIN, t);
    assert_eq!(text(&row, "outcome"), Some("not_matched"), "{row}");
}

#[test]
fn alr_rules_builtin_record_and_metrics() {
    let f = Fixture::new();
    let rx = Receiver::http();
    let clock = clock();
    let b = builtin_on(&rx, Some("1m"));
    let s = serve(&f, clock, &[], &b.kv(), &[]);
    let t1 = clock.next_tick(MINUTE_US);
    let first = s.get_auth("/metrics").text();
    assert!(clock.now() < t1, "第一次请求应在第一个计划时刻之前");
    for series in [
        "amberlog_alert_evaluations_total{rule=\"missed_collection\",outcome=\"matched\"} 0",
        "amberlog_alert_deliveries_total{rule=\"missed_collection\",result=\"delivered\"} 0",
    ] {
        assert!(
            first.lines().any(|l| l == series),
            "应含 {series}：\n{first}"
        );
    }
    clock.sleep_until(t1 + MINUTE_US + 90 * SECOND_US);
    let rows = rows_of(&s, BUILTIN);
    for t in [t1, t1 + MINUTE_US] {
        let row = rows
            .iter()
            .find(|r| int(r, "scheduled_at") == Some(t))
            .unwrap_or_else(|| panic!("缺 scheduled_at 为 {t} 的行：{rows:?}"));
        let span = int(row, "window_end").unwrap() - int(row, "window_start").unwrap();
        assert_eq!(span, MINUTE_US, "{row}");
    }
}

/// 「转为缺失」要实例先运行的时长与窗口 W（此前第几分钟），同 REQ-AMB-GOV-MISSED-COLLECTION 的长时场景：设了
/// `TESTBED_SOAK_HOURS`（跑满时长的长时运行）时照场景（运行约 22 分钟、W 为此前第 20 分钟）；缺省跑缩短版——运行 2 分钟、W 取此前第 1 分钟，同样在
/// 后台判定的基线之后到期（REQ-AMB-GOV-MISSED-COLLECTION-TERM-TURNED-MISSING）。
fn turned_missing_setup() -> (Duration, i64) {
    if std::env::var_os("TESTBED_SOAK_HOURS").is_some() {
        (Duration::from_secs(22 * 60), 20)
    } else {
        (Duration::from_secs(120), 1)
    }
}

/// 向 `app` 摄取 3 条 `_timestamp` 落在 W 的记录（带 `X-Amberlog-Source: fw-1`），再以 `basis` 为 `timestamp` 上报
/// `fw-1` 在 W 为 5：W 的按时间戳的发送数大于确认数。
fn short_of_window(s: &Server, w: i64) {
    common::missed::ingest_at(s, "app", Some("fw-1"), &common::missed::in_window(w, 3));
    common::missed::report(s, "app", "fw-1", w, 5);
}

#[test]
fn alr_rules_builtin_disabled_by_default() {
    let f = Fixture::new();
    let s = f.start(&[common::missed::GRACE_1M]);
    short_of_window(&s, common::missed::minute_ago(20));
    std::thread::sleep(Duration::from_secs(6 * 60));
    assert!(rows_of(&s, BUILTIN).is_empty());
    let text = s.get_auth("/metrics").text();
    assert!(!text.contains("rule=\"missed_collection\""), "{text}");
}

#[test]
fn alr_rules_builtin_notifies_on_turned_missing() {
    let (warm, k) = turned_missing_setup();
    let f = Fixture::new();
    let rx = Receiver::http();
    let b = builtin_on(&rx, Some("1m"));
    let mut kv = b.kv();
    kv.push(common::missed::GRACE_1M);
    let s = f.start(&kv);
    let accepting = Instant::now();
    common::api::create_stream(&s, "app");
    std::thread::sleep(
        (accepting + warm + Duration::from_secs(2)).saturating_duration_since(Instant::now()),
    );
    let w = common::missed::minute_ago(k);
    short_of_window(&s, w);
    std::thread::sleep(Duration::from_secs(6 * 60));

    let got = of_rule(&rx, BUILTIN);
    assert_eq!(got.len(), 1, "{got:?}");
    let body = got[0].json();
    assert_eq!(body["sql"], "builtin:missed_collection", "{body}");
    assert_eq!(body["matched_rows"], 1, "{body}");
    assert_eq!(
        body["rows"],
        json!([{"dataset": "default/app", "window_start": w, "window_end": w + MINUTE_US,
                "ts_sent": 5, "ts_durable": 3, "missing_sources": "fw-1"}]),
        "{body}"
    );
    let notified = got[0].scheduled_at().unwrap();
    let later: Vec<Value> = rows_of(&s, BUILTIN)
        .into_iter()
        .filter(|r| rfc3339(int(r, "scheduled_at").unwrap()) > notified)
        .collect();
    assert!(!later.is_empty(), "通知之后应还有评估记录");
    for row in &later {
        assert_eq!(text(row, "outcome"), Some("not_matched"), "{row}");
    }
}
