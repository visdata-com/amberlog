//! REQ-AMB-ALR-WEBHOOK 的系统测试：以子进程运行 `amberlog`，
//! 经测试接收端（`common::webhook`）、`_alert_evaluations`、`/metrics` 与 stderr 观察。
//!
//! - 场景按 REQ-AMB-ALR-RULES 的场景约定（`common::alerting`）：被测进程的时钟以 `TESTBED_CLOCK_OFFSET` 推到整分前
//!   若干秒，等计划时刻不必空等真实的整分；这些测试都要带 `fault-injection` 特性运行。
//! - 「某计划时刻 T 的请求」按请求体的 `evaluation_id`（规则名加 `-` 加 T 的微秒数）认；「等某计划时刻 T 的投递结束」
//!   按 `_alert_evaluations` 出现 T 的行判定（评估在投递有结果时才结束，REQ-AMB-ALR-RULES-TERM-ENDED）。
//! - 文件末尾另有测试接收端与共用搭建自身的检查。

mod common;

use std::time::Duration;

use common::alerting::{
    Clock, MINUTE_US, RuleToml, deliveries, has_data, has_one, rules, start, wait_evaluation,
};
use common::store::FLUSH_1S;
use common::tls::{KeyKind, Pki, TlsClient, Versions};
use common::webhook::{Receiver, Refusing, Reply, Request, Step};
use common::{Fixture, Kv, Server, TempDir, http};
use serde_json::Value;

/// 启动到整分的提前量：留出启动与「stream `app` 已有数据」的用时。
const LEAD: Duration = Duration::from_secs(20);

/// 一个运行中的场景：被测进程（先于夹具析构）、夹具与时钟。
struct Case {
    s: Server,
    f: Fixture,
    clock: Clock,
}

/// 以 `alerts.rules` 为 `rules` 与另加的配置项启动（`flush.interval` 为 `1s`），向 `app` 摄取 `records` 并等可查。
fn case(rule_list: &[RuleToml], extra: &[Kv], records: &[&str]) -> Case {
    let f = Fixture::new();
    let clock = Clock::before_minute(LEAD);
    let value = rules(rule_list);
    let mut overrides: Vec<Kv> = vec![FLUSH_1S, ("alerts.rules", Some(value.as_str()))];
    overrides.extend_from_slice(extra);
    let s = start(&f, clock, &overrides);
    if !records.is_empty() {
        has_data(&s, clock, "app", records);
    }
    Case { s, f, clock }
}

/// 只有一条场景约定的规则 `r1`（`webhook_url` 为接收端的 `/hook`），stream `app` 已有数据。
fn case_r1(rx: &Receiver) -> Case {
    let c = case(&[RuleToml::new("r1", &rx.url("/hook"))], &[], &[]);
    has_one(&c.s, c.clock);
    c
}

impl Case {
    /// 下一个计划时刻（`interval` 为 `1m`）。
    fn next_tick(&self) -> i64 {
        self.clock.next_tick(MINUTE_US)
    }
}

/// 计划时刻为 `t` 的、规则 `rule` 的请求。
fn for_tick(reqs: &[Request], rule: &str, t: i64) -> Vec<Request> {
    let id = format!("{rule}-{t}");
    reqs.iter()
        .filter(|r| {
            serde_json::from_slice::<Value>(&r.body)
                .is_ok_and(|v| v["evaluation_id"].as_str() == Some(id.as_str()))
        })
        .cloned()
        .collect()
}

/// 等接收端收到规则 `rule` 的第一个请求（至多 3 分钟），返回它。
fn first_request(rx: &Receiver, rule: &str) -> Request {
    let prefix = format!("\"evaluation_id\":\"{rule}-");
    let got = rx
        .wait_until(Duration::from_secs(180), |r| {
            r.iter()
                .any(|q| String::from_utf8_lossy(&q.body).contains(&prefix))
        })
        .unwrap_or_else(|| panic!("3 分钟内没有收到 {rule} 的请求：{:#?}", rx.requests()));
    got.into_iter()
        .find(|q| String::from_utf8_lossy(&q.body).contains(&prefix))
        .unwrap()
}

/// 请求体 `evaluation_id` 中的计划时刻（微秒）。
fn tick_of(req: &Request, rule: &str) -> i64 {
    req.json()["evaluation_id"]
        .as_str()
        .and_then(|id| id.strip_prefix(&format!("{rule}-")))
        .and_then(|t| t.parse().ok())
        .unwrap_or_else(|| panic!("evaluation_id 不是 {rule}-<微秒>：{}", req.json()))
}

/// UNIX 微秒数的 RFC 3339 UTC 写法，恒为 6 位小数（与块 BODY-TIMES 的例子同形）。
fn rfc3339(us: i64) -> String {
    let (secs, frac) = (us.div_euclid(1_000_000), us.rem_euclid(1_000_000));
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    // H. Hinnant 的 civil_from_days
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// `amberlog --version` 输出中的 `<semver>`（`amberlog <semver> (<commit>)`）。
fn semver() -> String {
    let dir = TempDir::new();
    let out = common::amberlog(dir.path())
        .arg("--version")
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap();
    text.split_whitespace().nth(1).unwrap().to_owned()
}

/// 以 `overrides` 运行一次 `amberlog serve` 直到退出，返回退出码与 stderr。
fn exit_of(f: &Fixture, overrides: &[Kv]) -> (Option<i32>, String) {
    let out = f.serve(overrides).output().expect("运行 amberlog serve");
    (
        out.status.code(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
    )
}

// ---------------------------------------------------------------------------
// A 组：地址与请求头的写法
// ---------------------------------------------------------------------------

#[test]
fn alr_webhook_bad_url_exit2() {
    let f = Fixture::new();
    let urls = [
        "ftp://127.0.0.1/hook",
        "HTTPS://127.0.0.1/hook",
        "/hook",
        "https://user:pass@127.0.0.1/hook",
        "https://127.0.0.1/hook#x",
    ];
    for (i, url) in urls.iter().enumerate() {
        let value = rules(&[RuleToml::new("r1", url)]);
        let (code, stderr) = exit_of(&f, &[("alerts.rules", Some(value.as_str()))]);
        assert_eq!(code, Some(2), "{url}：{stderr}");
        assert!(stderr.contains("alerts.rules"), "{url}：{stderr}");
        if i == 3 {
            assert!(stderr.contains("webhook_headers"), "{stderr}");
            assert!(!stderr.contains("pass"), "{stderr}");
        }
    }
}

#[test]
fn alr_webhook_bad_headers_exit2() {
    let f = Fixture::new();
    for headers in [
        r#"{ "Content-Type" = "text/plain" }"#,
        r#"{ "X Token" = "a" }"#,
        r#"{ "X-Token" = "" }"#,
        r#"{ "X-Token" = "a", "x-token" = "b" }"#,
        r#"{ "X-Token" = "a\nb" }"#,
    ] {
        let value =
            rules(
                &[RuleToml::new("r1", "http://127.0.0.1:9/hook").raw("webhook_headers", headers)],
            );
        let (code, stderr) = exit_of(&f, &[("alerts.rules", Some(value.as_str()))]);
        assert_eq!(code, Some(2), "{headers}：{stderr}");
        assert!(stderr.contains("alerts.rules"), "{headers}：{stderr}");
    }
}

// ---------------------------------------------------------------------------
// B 组：请求的格式
// ---------------------------------------------------------------------------

#[test]
fn alr_webhook_post_to_configured_url() {
    let rx = Receiver::http();
    let c = case(
        &[RuleToml::new("r1", &rx.url("/hook/a?team=ops"))],
        &[],
        &[],
    );
    has_one(&c.s, c.clock);
    let req = first_request(&rx, "r1");
    assert_eq!(req.method, "POST");
    assert_eq!(req.target, "/hook/a?team=ops");
}

#[test]
fn alr_webhook_request_headers() {
    let rx = Receiver::http();
    let rule = RuleToml::new("r1", &rx.url("/hook/a?team=ops")).raw(
        "webhook_headers",
        r#"{ "Authorization" = "Bearer s3cr3t", "X-Team" = "ops" }"#,
    );
    let c = case(&[rule], &[], &[]);
    has_one(&c.s, c.clock);
    let req = first_request(&rx, "r1");
    assert_eq!(req.header("Content-Type"), Some("application/json"));
    assert_eq!(
        req.header("User-Agent"),
        Some(format!("amberlog/{}", semver()).as_str())
    );
    assert_eq!(req.header("Authorization"), Some("Bearer s3cr3t"));
    assert_eq!(req.header("X-Team"), Some("ops"));
}

/// 场景「请求体恰含版本 1 的十个字段」与「三个时刻按 RFC 3339 写到微秒」的数据：`k` 为 1、2 的两条记录。
fn two_records(rx: &Receiver, rule: RuleToml) -> (Case, Request) {
    let c = case(&[rule], &[], &[r#"{"k":1}"#, r#"{"k":2}"#]);
    let req = first_request(rx, "r1");
    (c, req)
}

#[test]
fn alr_webhook_body_ten_fields() {
    let rx = Receiver::http();
    let (_c, req) = two_records(
        &rx,
        RuleToml::new("r1", &rx.url("/hook")).str("sql", "SELECT k FROM app ORDER BY k"),
    );
    let v = req.json();
    let obj = v.as_object().expect("请求体是 JSON 对象");
    let mut keys: Vec<&str> = obj.keys().map(String::as_str).collect();
    keys.sort_unstable();
    assert_eq!(
        keys,
        [
            "evaluation_id",
            "matched_rows",
            "rows",
            "rule",
            "scheduled_at",
            "sql",
            "truncated",
            "version",
            "window_end",
            "window_start"
        ]
    );
    assert_eq!(v["version"], 1);
    assert_eq!(v["rule"], "r1");
    assert_eq!(v["matched_rows"], 2);
    assert_eq!(v["truncated"], false);
    assert_eq!(v["rows"], serde_json::json!([{"k": 1}, {"k": 2}]));
    assert_eq!(v["sql"], "SELECT k FROM app ORDER BY k");
}

#[test]
fn alr_webhook_body_times_rfc3339() {
    let rx = Receiver::http();
    let (_c, req) = two_records(
        &rx,
        RuleToml::new("r1", &rx.url("/hook"))
            .str("sql", "SELECT k FROM app ORDER BY k")
            .str("window", "5m"),
    );
    let t = tick_of(&req, "r1");
    assert_eq!(t % MINUTE_US, 0, "计划时刻是整分");
    let v = req.json();
    assert_eq!(v["scheduled_at"], rfc3339(t));
    assert!(v["scheduled_at"].as_str().unwrap().ends_with(":00.000000Z"));
    assert_eq!(v["window_start"], rfc3339(t - 5 * MINUTE_US));
    assert_eq!(v["window_end"], v["scheduled_at"]);
}

#[test]
fn alr_webhook_evaluation_id_stable() {
    let rx = Receiver::http();
    rx.script(
        vec![Step::status(503), Step::status(503)],
        Step::status(200),
    );
    let _c = case_r1(&rx);
    let got = rx.wait_for(3, Duration::from_secs(180));
    let first = &got[0];
    let t = tick_of(first, "r1");
    let three = for_tick(&got, "r1", t);
    assert_eq!(three.len(), 3, "{got:#?}");
    for r in &three {
        let v = r.json();
        assert_eq!(v["evaluation_id"], format!("r1-{t}"));
        assert_eq!(v["scheduled_at"], rfc3339(t));
    }
}

#[test]
fn alr_webhook_rows_first_ten() {
    let rx = Receiver::http();
    let recs: Vec<String> = (1..=12).map(|k| format!(r#"{{"k":{k}}}"#)).collect();
    let refs: Vec<&str> = recs.iter().map(String::as_str).collect();
    let _c = case(
        &[RuleToml::new("r1", &rx.url("/hook")).str("sql", "SELECT k FROM app ORDER BY k")],
        &[],
        &refs,
    );
    let v = first_request(&rx, "r1").json();
    let want: Vec<Value> = (1..=10).map(|k| serde_json::json!({ "k": k })).collect();
    assert_eq!(v["rows"], Value::Array(want));
    assert_eq!(v["matched_rows"], 12);
}

#[test]
fn alr_webhook_rows_column_condition() {
    let rx = Receiver::http();
    let _c = case(
        &[RuleToml::new("lat", &rx.url("/hook"))
            .str("sql", "SELECT host, lat FROM app ORDER BY host")
            .raw(
                "condition",
                r#"{ column = "lat", op = ">", threshold = 200 }"#,
            )],
        &[],
        &[
            r#"{"host":"a","lat":10}"#,
            r#"{"host":"b","lat":250}"#,
            r#"{"host":"c","lat":300}"#,
        ],
    );
    let v = first_request(&rx, "lat").json();
    assert_eq!(
        v["rows"],
        serde_json::json!([{"host":"b","lat":250},{"host":"c","lat":300}])
    );
    assert_eq!(v["matched_rows"], 2);
}

/// 场景「结果被截断时 matched_rows 为 query.max_size」与「截断标志与评估一致」：`query.max_size` 为 10，15 条记录，
/// 条件 `rows > 5`。
fn truncated_request() -> (Receiver, Case, Value) {
    let rx = Receiver::http();
    let recs: Vec<String> = (1..=15).map(|k| format!(r#"{{"k":{k}}}"#)).collect();
    let refs: Vec<&str> = recs.iter().map(String::as_str).collect();
    let c = case(
        &[RuleToml::new("t", &rx.url("/hook")).raw("condition", r#"{ rows = "> 5" }"#)],
        // query.default_size 缺省 100，不得大于 query.max_size：取场景外的合法值 10
        &[
            ("query.max_size", Some("10")),
            ("query.default_size", Some("10")),
        ],
        &refs,
    );
    let v = first_request(&rx, "t").json();
    (rx, c, v)
}

#[test]
fn alr_webhook_matched_rows_truncated() {
    let (_rx, _c, v) = truncated_request();
    assert_eq!(v["matched_rows"], 10);
}

#[test]
fn alr_webhook_truncated_flag() {
    let (_rx, _c, v) = truncated_request();
    assert_eq!(v["truncated"], true);
    assert_eq!(v["rows"].as_array().unwrap().len(), 10);
}

#[test]
fn alr_webhook_same_body_on_retry() {
    let rx = Receiver::http();
    rx.script(
        vec![Step::status(500), Step::status(500)],
        Step::status(204),
    );
    let _c = case_r1(&rx);
    let got = rx.wait_for(3, Duration::from_secs(180));
    let t = tick_of(&got[0], "r1");
    let three = for_tick(&got, "r1", t);
    assert_eq!(three.len(), 3, "{got:#?}");
    assert!(three.iter().all(|r| r.body == three[0].body));
}

// ---------------------------------------------------------------------------
// C 组：送达、超时与重试
// ---------------------------------------------------------------------------

#[test]
fn alr_webhook_first_attempt_latency() {
    let rx = Receiver::http();
    let f = Fixture::new();
    let clock = Clock::before_minute(LEAD);
    let control = f.root().join("before_delivery.gate");
    std::fs::write(&control, "hold").unwrap();
    let value = rules(&[RuleToml::new("r1", &rx.url("/hook"))]);
    let mut cmd = f.serve(&[FLUSH_1S, ("alerts.rules", Some(value.as_str()))]);
    clock.apply(&mut cmd);
    cmd.env("TESTBED_FAULT_GATE", "alert.before_delivery")
        .env("TESTBED_FAULT_GATE_FILE", &control);
    let s = Server::start(cmd);
    has_one(&s, clock);
    assert!(
        common::wait_file_content(&control, "held", Duration::from_secs(180)),
        "评估没有停在 alert.before_delivery：{:#?}",
        s.stderr_lines()
    );
    assert!(rx.requests().is_empty(), "暂停点之前已发出请求");
    let r = common::api::now_us();
    std::fs::write(&control, "go").unwrap();
    let first = first_request(&rx, "r1");
    assert!(
        first.at_us() <= r + 1_000_000,
        "第一个请求在解除暂停点后 {} 微秒才到",
        first.at_us() - r
    );
}

/// 接收端对每个请求回 HTTP 202、响应体 `not json`，等过两个计划时刻。
#[test]
fn alr_webhook_2xx_delivered() {
    let rx = Receiver::http();
    rx.always(Reply::status(202).body(b"not json").into());
    let c = case_r1(&rx);
    let t1 = c.next_tick();
    let t2 = t1 + MINUTE_US;
    let rows = [
        wait_evaluation(&c.s, "r1", t1),
        wait_evaluation(&c.s, "r1", t2),
    ];
    let got = rx.requests();
    for (t, row) in [t1, t2].iter().zip(&rows) {
        assert_eq!(for_tick(&got, "r1", *t).len(), 1, "{t}：{got:#?}");
        assert_eq!(row["delivery"], "delivered", "{row}");
        assert_eq!(row["attempts"], 1, "{row}");
        assert_eq!(row["last_http_status"], 202, "{row}");
    }
}

/// 场景「10 秒内无响应的尝试按超时重试」：第一个请求延迟 15 秒才回 200，其后立即回 200。返回 T 的请求与行。
fn timeout_case() -> (Vec<Request>, Value) {
    let rx = Receiver::http();
    rx.script(
        vec![Reply::status(200).delay(Duration::from_secs(15)).into()],
        Step::status(200),
    );
    let c = case_r1(&rx);
    let t = c.next_tick();
    let row = wait_evaluation(&c.s, "r1", t);
    (for_tick(&rx.requests(), "r1", t), row)
}

#[test]
fn alr_webhook_attempt_timeout() {
    let (reqs, row) = timeout_case();
    assert_eq!(reqs.len(), 2, "{reqs:#?}");
    let gap = reqs[1].after(&reqs[0]);
    assert!(
        gap >= Duration::from_secs(10) && gap <= Duration::from_secs(13),
        "{gap:?}"
    );
    assert_eq!(row["delivery"], "delivered_after_retry", "{row}");
    assert_eq!(row["attempts"], 2, "{row}");
}

#[test]
fn alr_webhook_deadline_next_tick() {
    let rx = Receiver::http();
    rx.always(Reply::status(503).delay(Duration::from_secs(9)).into());
    let c = case_r1(&rx);
    let t = c.next_tick();
    let row = wait_evaluation(&c.s, "r1", t);
    let reqs = for_tick(&rx.requests(), "r1", t);
    assert!(!reqs.is_empty());
    for r in &reqs {
        assert!(c.clock.to_process(r.at_us()) < t + MINUTE_US, "{r:?}");
    }
    assert_eq!(row["delivery"], "abandoned", "{row}");
    assert_eq!(row["last_http_status"], 503, "{row}");
    // 第 5 次尝试在截止时刻被中止，仍计入（REQ-AMB-ALR-WEBHOOK-ATTEMPTS、-DEADLINE）
    assert_eq!(row["attempts"], 5, "{row}");
}

/// 场景「5xx、408、429 与连接失败都重试」：某次投递的前四个连接依次回 500、408、429、不接受连接，第五个回 200。
fn retryable_case() -> (Receiver, Case, i64, Value) {
    let rx = Receiver::http();
    rx.script(
        vec![
            Step::status(500),
            Step::status(408),
            Step::status(429),
            Step::Refuse,
        ],
        Step::status(200),
    );
    let c = case_r1(&rx);
    let t = c.next_tick();
    let row = wait_evaluation(&c.s, "r1", t);
    (rx, c, t, row)
}

#[test]
fn alr_webhook_retryable_responses() {
    let (rx, _c, t, row) = retryable_case();
    assert_eq!(
        for_tick(&rx.requests(), "r1", t).len(),
        4,
        "{:#?}",
        rx.requests()
    );
    assert_eq!(row["delivery"], "delivered_after_retry", "{row}");
    assert_eq!(row["attempts"], 5, "{row}");
    assert_eq!(row["last_http_status"], 200, "{row}");
}

#[test]
fn alr_webhook_backoff_five_attempts() {
    let rx = Receiver::http();
    rx.always(Step::status(503));
    let c = case_r1(&rx);
    let t = c.next_tick();
    let row = wait_evaluation(&c.s, "r1", t);
    let reqs = for_tick(&rx.requests(), "r1", t);
    assert_eq!(reqs.len(), 5, "{reqs:#?}");
    for (i, want) in [1u64, 2, 4, 8].iter().enumerate() {
        let gap = reqs[i + 1].after(&reqs[i]);
        assert!(
            gap >= Duration::from_secs(*want) && gap <= Duration::from_secs(want + 1),
            "第 {} 与第 {} 次之间 {gap:?}",
            i + 1,
            i + 2
        );
    }
    assert_eq!(row["delivery"], "abandoned", "{row}");
    assert_eq!(row["attempts"], 5, "{row}");
}

/// 场景「其他 4xx 与重定向不重试」：两条规则 `r4`（回 401）与 `r3`（回 302 与指向另一路径的 `Location`），
/// `alerts.max_concurrency` 为 2。返回接收端、场景、计划时刻与两行。
fn not_retryable_case() -> (Receiver, Case, i64, Value, Value) {
    let rx = Receiver::http();
    let other = rx.url("/other");
    rx.always(Step::by(move |req| match req.target.as_str() {
        "/r4" => Reply::status(401),
        "/r3" => Reply::status(302).location(&other),
        _ => Reply::status(200),
    }));
    let c = case(
        &[
            RuleToml::new("r4", &rx.url("/r4")),
            RuleToml::new("r3", &rx.url("/r3")),
        ],
        &[("alerts.max_concurrency", Some("2"))],
        &[],
    );
    has_one(&c.s, c.clock);
    let t = c.next_tick();
    let r4 = wait_evaluation(&c.s, "r4", t);
    let r3 = wait_evaluation(&c.s, "r3", t);
    (rx, c, t, r4, r3)
}

#[test]
fn alr_webhook_not_retryable() {
    let (rx, _c, t, r4, r3) = not_retryable_case();
    let got = rx.requests();
    assert_eq!(for_tick(&got, "r4", t).len(), 1, "{got:#?}");
    assert_eq!(for_tick(&got, "r3", t).len(), 1, "{got:#?}");
    assert!(got.iter().all(|r| r.target != "/other"), "{got:#?}");
    for (row, status) in [(&r4, 401), (&r3, 302)] {
        assert_eq!(row["delivery"], "abandoned", "{row}");
        assert_eq!(row["attempts"], 1, "{row}");
        assert_eq!(row["last_http_status"], status, "{row}");
    }
}

#[test]
fn alr_webhook_abandoned_metric() {
    let (_rx, c, _t, _r4, _r3) = not_retryable_case();
    // 指标在投递有结果时递增，评估记录在其后写出：读到两行时两个序列都已递增；其后的计划时刻会再加，故读到行后立即读
    assert_eq!(deliveries(&c.s, "r4", "abandoned"), 1.0);
    assert_eq!(deliveries(&c.s, "r3", "abandoned"), 1.0);
}

#[test]
fn alr_webhook_no_replay_after_restart() {
    let rx = Receiver::http();
    rx.always(Step::Refuse);
    let Case { mut s, f, clock } = case_r1(&rx);
    let t = clock.next_tick(MINUTE_US);
    clock.sleep_until(t);
    // 投递开始重试：已有第二个连接（第一次尝试连接失败后 1 秒）
    let started = std::time::Instant::now();
    while rx.connections() < 2 {
        assert!(
            started.elapsed() < Duration::from_secs(60),
            "投递没有开始重试"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    s.sigkill();
    rx.always(Step::status(200));
    let value = rules(&[RuleToml::new("r1", &rx.url("/hook"))]);
    let s = start(
        &f,
        clock,
        &[FLUSH_1S, ("alerts.rules", Some(value.as_str()))],
    );
    let t2 = clock.next_tick(MINUTE_US);
    wait_evaluation(&s, "r1", t2);
    assert!(
        for_tick(&rx.requests(), "r1", t).is_empty(),
        "{:#?}",
        rx.requests()
    );
}

// ---------------------------------------------------------------------------
// D 组：投递结果与指标
// ---------------------------------------------------------------------------

#[test]
fn alr_webhook_not_attempted() {
    let rx = Receiver::http();
    let c = case(
        &[RuleToml::new("n", &rx.url("/hook")).raw("condition", r#"{ rows = "< 0" }"#)],
        &[],
        &[],
    );
    has_one(&c.s, c.clock);
    let row = wait_evaluation(&c.s, "n", c.next_tick());
    assert_eq!(row["delivery"], "not_attempted", "{row}");
    assert_eq!(row["attempts"], 0, "{row}");
    // NULL 列在 hit 中不写键
    assert!(row.get("last_http_status").is_none(), "{row}");
}

#[test]
fn alr_webhook_attempts_count_timeouts() {
    let (_reqs, row) = timeout_case();
    assert_eq!(row["attempts"], 2, "{row}");
}

#[test]
fn alr_webhook_last_status_null() {
    let refusing = Refusing::new();
    let c = case(&[RuleToml::new("r1", &refusing.url("/hook"))], &[], &[]);
    has_one(&c.s, c.clock);
    let row = wait_evaluation(&c.s, "r1", c.next_tick());
    assert_eq!(row["delivery"], "abandoned", "{row}");
    assert_eq!(row["attempts"], 5, "{row}");
    assert!(row.get("last_http_status").is_none(), "{row}");
}

/// `r1` 的 `amberlog_alert_deliveries_total{result="delivered"}` 与 `_alert_evaluations` 中 `delivery` 为
/// `delivered` 或 `delivered_after_retry` 的行数，轮询到两者相等（评估记录晚于指标写出，至多 3 分钟）。
fn delivered_metric_matches_rows(s: &Server) {
    let deadline = std::time::Instant::now() + Duration::from_secs(180);
    loop {
        let m = deliveries(s, "r1", "delivered");
        let rows = common::alerting::evaluations(
            s,
            "rule = 'r1' AND delivery IN ('delivered', 'delivered_after_retry')",
        )
        .len() as f64;
        if m == rows && m >= 1.0 {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "指标 {m} 与行数 {rows} 3 分钟内没有相等"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

#[test]
fn alr_webhook_delivered_metric() {
    let (_rx, c, _t, _row) = retryable_case();
    delivered_metric_matches_rows(&c.s);
    let rx = Receiver::http();
    rx.always(Reply::status(202).body(b"not json").into());
    let c2 = case_r1(&rx);
    wait_evaluation(&c2.s, "r1", c2.next_tick());
    delivered_metric_matches_rows(&c2.s);
}

#[test]
fn alr_webhook_metric_series_init() {
    let f = Fixture::new();
    let clock = Clock::before_minute(Duration::from_secs(40));
    let value = rules(&[RuleToml::new("r1", "http://127.0.0.1:9/hook")]);
    let s = start(&f, clock, &[("alerts.rules", Some(value.as_str()))]);
    let first = clock.next_tick(MINUTE_US);
    let text = s.get_auth("/metrics").text();
    assert!(clock.now() < first, "读 /metrics 时已过第一个计划时刻");
    for series in [
        "amberlog_alert_deliveries_total{rule=\"r1\",result=\"delivered\"} 0",
        "amberlog_alert_deliveries_total{rule=\"r1\",result=\"abandoned\"} 0",
    ] {
        assert!(text.lines().any(|l| l == series), "缺 {series}：{text}");
    }
}

/// `<storage.prefix>/_system/alerts/` 之下全部对象的内容（`file://` 存储）。
fn alert_objects(f: &Fixture) -> Vec<(std::path::PathBuf, Vec<u8>)> {
    fn walk(dir: &std::path::Path, out: &mut Vec<(std::path::PathBuf, Vec<u8>)>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, out);
            } else if let Ok(b) = std::fs::read(&p) {
                out.push((p, b));
            }
        }
    }
    let mut out = Vec::new();
    walk(&f.store_dir().join("amberlog/_system/alerts"), &mut out);
    out
}

#[test]
fn alr_webhook_header_values_hidden() {
    let rx = Receiver::http();
    rx.always(Step::Refuse);
    let rule = RuleToml::new("r1", &rx.url("/hook")).raw(
        "webhook_headers",
        r#"{ "Authorization" = "Bearer s3cr3t-token" }"#,
    );
    let c = case(&[rule], &[("log.level", Some("\"trace\""))], &[]);
    has_one(&c.s, c.clock);
    // wait_evaluation 读到行即评估记录已写出
    wait_evaluation(&c.s, "r1", c.next_tick());
    let stderr = c.s.stderr_lines().join("\n");
    assert!(!stderr.contains("s3cr3t-token"), "{stderr}");
    let objects = alert_objects(&c.f);
    assert!(!objects.is_empty(), "_system/alerts/ 之下没有对象");
    for (p, b) in &objects {
        assert!(
            !String::from_utf8_lossy(b).contains("s3cr3t-token"),
            "{p:?}"
        );
    }
    assert!(!c.s.get_auth("/metrics").text().contains("s3cr3t-token"));
}

#[test]
fn alr_webhook_log_url_strips_query() {
    let refusing = Refusing::new();
    let c = case(
        &[RuleToml::new("r1", &refusing.url("/hook?key=k-secret-42"))],
        &[("log.level", Some("\"trace\""))],
        &[],
    );
    has_one(&c.s, c.clock);
    wait_evaluation(&c.s, "r1", c.next_tick());
    let stderr = c.s.stderr_lines().join("\n");
    assert!(!stderr.contains("k-secret-42"), "{stderr}");
    assert!(!c.s.get_auth("/metrics").text().contains("k-secret-42"));
}

// ---------------------------------------------------------------------------
// 补充：配置不合法退出时不泄露请求头的值与查询串（HEADER-VALUES-HIDDEN、LOG-URL 的括注）
// ---------------------------------------------------------------------------

#[test]
fn alr_webhook_exit2_hides_secrets() {
    let f = Fixture::new();
    let value = rules(&[
        RuleToml::new("r1", "http://127.0.0.1:9/hook?key=k-secret-42")
            .str("interval", "90s")
            .raw(
                "webhook_headers",
                r#"{ "Authorization" = "Bearer s3cr3t-token" }"#,
            ),
    ]);
    let (code, stderr) = exit_of(&f, &[("alerts.rules", Some(value.as_str()))]);
    assert_eq!(code, Some(2), "{stderr}");
    assert!(stderr.contains("alerts.rules"), "{stderr}");
    assert!(!stderr.contains("s3cr3t-token"), "{stderr}");
    assert!(!stderr.contains("k-secret-42"), "{stderr}");
}

// ---------------------------------------------------------------------------
// 测试设施自身的检查
// ---------------------------------------------------------------------------

/// 测试接收端：按连接的先后作答，不接受的连接不记为请求，请求头名的大小写原样记下。
#[test]
fn webhook_receiver_records_and_replies() {
    let rx = Receiver::http();
    rx.script(
        vec![
            Step::status(500),
            Step::Refuse,
            Reply::status(302).location("/elsewhere").into(),
        ],
        Step::by(|req| Reply::status(if req.target == "/a?x=1" { 202 } else { 404 })),
    );
    let addr = rx.addr();
    let r = http(
        addr,
        "POST",
        "/hook",
        &[("X-Token", "Bearer t")],
        b"{\"k\":1}",
    );
    assert_eq!(r.status, 500);
    // 不接受的连接：对端读到的是连接关闭
    let mut s = std::net::TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut buf = Vec::new();
    let _ = std::io::Read::read_to_end(&mut s, &mut buf);
    assert!(buf.is_empty());
    let r = http(addr, "POST", "/hook", &[], b"");
    assert_eq!((r.status, r.header("location")), (302, Some("/elsewhere")));
    assert_eq!(http(addr, "POST", "/a?x=1", &[], b"").status, 202);
    assert_eq!(http(addr, "POST", "/b", &[], b"").status, 404);

    let got = rx.wait_for(4, Duration::from_secs(5));
    assert_eq!(got.len(), 4);
    assert_eq!(rx.connections(), 5);
    assert_eq!(
        (got[0].method.as_str(), got[0].target.as_str()),
        ("POST", "/hook")
    );
    assert!(
        got[0]
            .headers
            .iter()
            .any(|(k, v)| k == "X-Token" && v == "Bearer t")
    );
    assert_eq!(got[0].header("x-token"), Some("Bearer t"));
    assert_eq!(got[0].body, b"{\"k\":1}");
    assert!(!got[0].tls);
    assert_eq!(got[2].target, "/a?x=1");
    assert!(got[1].after(&got[0]) < Duration::from_secs(5));
}

/// 测试接收端的回应延迟：读完请求后等足时长再作答。
#[test]
fn webhook_receiver_delays_reply() {
    let rx = Receiver::http();
    rx.always(Reply::status(200).delay(Duration::from_millis(1500)).into());
    let t = std::time::Instant::now();
    assert_eq!(http(rx.addr(), "POST", "/", &[], b"x").status, 200);
    assert!(t.elapsed() >= Duration::from_millis(1500));
    assert_eq!(rx.requests().len(), 1);
}

/// HTTPS 接收端：信任其 CA 的客户端握手成功、请求被记下；不信任的客户端握手失败、不记请求。
#[test]
fn webhook_receiver_https() {
    let dir = TempDir::new();
    let pki = Pki::new(dir.path());
    let ca_w = pki.ca("CA-W");
    let rx = Receiver::https(&pki.leaf(&ca_w, "receiver", KeyKind::P256));
    assert!(rx.url("/hook").starts_with("https://127.0.0.1:"));

    let trusted = TlsClient::new(&ca_w.cert, Versions::Both);
    let mut conn = trusted.connect(rx.addr()).unwrap();
    let r = conn.request("POST", "/hook", &[], b"{}");
    assert_eq!(r.status, 200);

    let other = pki.ca("CA-other");
    let untrusted = TlsClient::new(&other.cert, Versions::Both);
    assert!(untrusted.connect(rx.addr()).is_err());

    std::thread::sleep(Duration::from_millis(200));
    let got = rx.requests();
    assert_eq!(got.len(), 1);
    assert!(got[0].tls);
    assert_eq!(got[0].body, b"{}");
    assert_eq!(rx.connections(), 2);
}

/// 共用搭建：规则的 TOML 写法可解析且取场景约定的缺省；时钟偏移与计划时刻的换算。
#[test]
fn alerting_helpers_rule_toml_and_clock() {
    use common::alerting::{Clock, RuleToml, kth_tick, next_tick_after, rules};
    let r = RuleToml::new("r1", "http://127.0.0.1:9/hook?team=ops")
        .str("sql", "SELECT k FROM app WHERE s = 'a\"b'")
        .raw(
            "webhook_headers",
            r#"{ "X-Team" = "ops", Authorization = "Bearer t" }"#,
        )
        .without("window");
    let doc: toml::Value = toml::from_str(&format!("[alerts]\nrules = {}\n", rules(&[r]))).unwrap();
    let rule = &doc["alerts"]["rules"][0];
    assert_eq!(rule["name"].as_str(), Some("r1"));
    assert_eq!(rule["interval"].as_str(), Some("1m"));
    assert!(rule.get("window").is_none());
    assert_eq!(
        rule["sql"].as_str(),
        Some("SELECT k FROM app WHERE s = 'a\"b'")
    );
    assert_eq!(rule["condition"]["rows"].as_str(), Some("> 0"));
    assert_eq!(rule["webhook_headers"]["X-Team"].as_str(), Some("ops"));

    let c = Clock { offset_s: 7 };
    assert_eq!(c.to_test(c.to_process(123)), 123);
    assert_eq!(c.to_process(0), 7_000_000);
    assert_eq!(next_tick_after(60_000_000, 60_000_000), 120_000_000);
    assert_eq!(next_tick_after(59_999_999, 60_000_000), 60_000_000);
    assert_eq!(kth_tick(61_000_000, 60_000_000, 2), 180_000_000);
    if cfg!(feature = "fault-injection") {
        let c = Clock::before_minute(Duration::from_secs(10));
        let left = next_tick_after(c.now(), 60_000_000) - c.now();
        assert!((8_000_000..=11_000_000).contains(&left), "{left}");
    }
}
