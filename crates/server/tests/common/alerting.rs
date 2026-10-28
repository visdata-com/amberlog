//! 定时告警场景的共用搭建（REQ-AMB-ALR-RULES 的场景约定；REQ-AMB-ALR-WEBHOOK、REQ-AMB-ALR-EVALUATIONS 与
//! REQ-AMB-ARCH-ENCRYPTION-V2 I 组的系统测试共用）。
//!
//! - 时钟：评估在被测进程时钟的整分（`interval` 的整数倍）到期。为免每个场景空等到下一个真实整分，以 `fault-injection`
//!   下的 `TESTBED_CLOCK_OFFSET`（整数秒）把被测进程的时钟推到某整分之前若干秒（[`Clock`]）；用到它的测试要带
//!   `--features fault-injection`。偏移在进程启动时定下，此后进程时钟与测试进程时钟恒差同一偏移。
//! - 规则：[`RuleToml`] 按场景约定给出缺省（`interval` 为 `1m`、`window` 为 `7d`、`sql` 为 `SELECT * FROM app`、
//!   `condition` 为 `{ rows = "> 0" }`，`org` 与 `enabled` 省略），逐字段覆盖；[`rules`] 拼成 `alerts.rules` 的取值。
//! - 「stream `app` 已有数据」：[`has_data`]；「读取 `_alert_evaluations`」：[`evaluations`]、[`wait_evaluation`]；
//!   读指标：[`metric_or_zero`]（此前不存在的序列按 0）。

use std::process::Command;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::api::{ingest_ok, now_us};
use super::{BEARER, Fixture, Kv, Server};

pub const SECOND_US: i64 = 1_000_000;
pub const MINUTE_US: i64 = 60 * SECOND_US;

/// 场景约定的「读取 `_alert_evaluations`」的查询范围上界。
pub const EVAL_END_US: i64 = 4_102_444_800_000_000;

/// 评估记录的写出时限 60 秒加余量（REQ-AMB-ALR-EVALUATIONS「等评估记录写出」）。
pub const RECORD_WAIT: Duration = Duration::from_secs(90);

/// 被测进程的时钟偏移（`TESTBED_CLOCK_OFFSET`，整数秒，可为 0）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clock {
    pub offset_s: i64,
}

impl Clock {
    /// 不偏移：被测进程用系统时钟。
    pub const REAL: Clock = Clock { offset_s: 0 };

    /// 偏移取 0～59 秒，使此刻被测进程的时钟落在某整分之前约 `lead` 秒（按整秒取，误差不足 1 秒；`lead` 不超过 60 秒）。
    /// 进程启动要花时间，`lead` 宜留出启动与准备数据的用时。
    ///
    /// 不开 `fault-injection` 构建时被测进程不读偏移，取 [`Clock::REAL`]（场景照样成立，只是要等到真实的整分）。
    pub fn before_minute(lead: Duration) -> Clock {
        if !cfg!(feature = "fault-injection") {
            return Clock::REAL;
        }
        let lead = lead.as_secs() as i64;
        assert!((0..=60).contains(&lead), "lead 须在 0～60 秒");
        let sec = now_us().div_euclid(SECOND_US).rem_euclid(60);
        Clock {
            offset_s: (60 - lead - sec).rem_euclid(60),
        }
    }

    /// 偏移取 0～59 秒，使此刻被测进程的时钟落在某分钟的第 `second` 秒（按整秒取）。
    pub fn at_second(second: u32) -> Clock {
        assert!(second < 60);
        if !cfg!(feature = "fault-injection") {
            return Clock::REAL;
        }
        let sec = now_us().div_euclid(SECOND_US).rem_euclid(60);
        Clock {
            offset_s: (i64::from(second) - sec).rem_euclid(60),
        }
    }

    /// 偏移的微秒数。
    pub fn offset_us(&self) -> i64 {
        self.offset_s * SECOND_US
    }

    /// 给命令设上偏移（偏移为 0 时也设，便于读 stderr 时核对）。
    pub fn apply(&self, cmd: &mut Command) {
        cmd.env("TESTBED_CLOCK_OFFSET", self.offset_s.to_string());
    }

    /// 被测进程此刻的时钟（UTC 微秒）。
    pub fn now(&self) -> i64 {
        self.to_process(now_us())
    }

    /// 测试进程时钟的时刻（如 [`super::webhook::Request::at_us`]）换算到被测进程的时钟。
    pub fn to_process(self, test_us: i64) -> i64 {
        test_us + self.offset_us()
    }

    /// 被测进程时钟的时刻换算到测试进程的时钟。
    pub fn to_test(self, process_us: i64) -> i64 {
        process_us - self.offset_us()
    }

    /// 被测进程时钟上严格晚于此刻的下一个计划时刻（`interval_us` 的整数倍，REQ-AMB-ALR-RULES-TERM-SCHEDULE）。
    pub fn next_tick(&self, interval_us: i64) -> i64 {
        next_tick_after(self.now(), interval_us)
    }

    /// 等到被测进程的时钟到达 `process_us`（已过则立即返回）。
    pub fn sleep_until(&self, process_us: i64) {
        let left = process_us - self.now();
        if left > 0 {
            std::thread::sleep(Duration::from_micros(left as u64));
        }
    }
}

/// 以 `clock` 的偏移启动 `amberlog serve` 并等到 `ready`。
pub fn start(fx: &Fixture, clock: Clock, overrides: &[Kv]) -> Server {
    let mut cmd = fx.serve(overrides);
    clock.apply(&mut cmd);
    Server::start(cmd)
}

/// 严格晚于 `us` 的下一个 `interval_us` 的整数倍。
pub fn next_tick_after(us: i64, interval_us: i64) -> i64 {
    (us.div_euclid(interval_us) + 1) * interval_us
}

/// `alerts.rules` 的一个元素（TOML 内联表），字段按写入次序输出。
#[derive(Debug, Clone)]
pub struct RuleToml {
    fields: Vec<(String, String)>,
}

impl RuleToml {
    /// 场景约定的规则：`name`、`webhook_url` 由调用方给出，`interval` 为 `1m`、`window` 为 `7d`、`sql` 为
    /// `SELECT * FROM app`、`condition` 为 `{ rows = "> 0" }`。
    pub fn new(name: &str, webhook_url: &str) -> RuleToml {
        RuleToml {
            fields: vec![
                ("name".into(), toml_str(name)),
                ("interval".into(), toml_str("1m")),
                ("window".into(), toml_str("7d")),
                ("sql".into(), toml_str("SELECT * FROM app")),
                ("condition".into(), r#"{ rows = "> 0" }"#.into()),
                ("webhook_url".into(), toml_str(webhook_url)),
            ],
        }
    }

    /// 设字段为 TOML 取值原文（如 `"\"5m\""`、`"false"`、`"{ X-Team = \"ops\" }"`）；已有则替换，没有则追加。
    pub fn raw(mut self, key: &str, toml: &str) -> RuleToml {
        match self.fields.iter_mut().find(|(k, _)| k == key) {
            Some(f) => f.1 = toml.to_owned(),
            None => self.fields.push((key.to_owned(), toml.to_owned())),
        }
        self
    }

    /// 设字段为字符串。
    pub fn str(self, key: &str, value: &str) -> RuleToml {
        self.raw(key, &toml_str(value))
    }

    /// 删去字段（取缺省，或构造缺字段的非法规则）。
    pub fn without(mut self, key: &str) -> RuleToml {
        self.fields.retain(|(k, _)| k != key);
        self
    }

    /// TOML 内联表的写法。
    pub fn to_toml(&self) -> String {
        let body: Vec<String> = self
            .fields
            .iter()
            .map(|(k, v)| format!("{} = {v}", toml_key(k)))
            .collect();
        format!("{{ {} }}", body.join(", "))
    }
}

/// `alerts.rules` 的取值（TOML 数组），用作 [`super::Fixture`] 的覆盖项 `("alerts.rules", Some(&rules(..)))`。
pub fn rules(rules: &[RuleToml]) -> String {
    let items: Vec<String> = rules.iter().map(RuleToml::to_toml).collect();
    format!("[{}]", items.join(", "))
}

/// TOML 基本字符串（含转义）。
pub fn toml_str(s: &str) -> String {
    // TOML 基本字符串的转义与 JSON 字符串的写法相容（\" \\ \n \t \uXXXX）
    serde_json::to_string(s).unwrap()
}

/// TOML 键：裸键可用时照写，否则写成带引号的键。
fn toml_key(k: &str) -> String {
    if !k.is_empty()
        && k.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        k.to_owned()
    } else {
        toml_str(k)
    }
}

/// 「stream `app` 已有数据」：向 `stream` 摄取 `records`（每条为 JSON 对象原文，不含 `_timestamp` 的补上被测进程此刻的
/// `_timestamp`），并等到它们可查（至多 60 秒）。
pub fn has_data(server: &Server, clock: Clock, stream: &str, records: &[&str]) {
    let now = clock.now();
    let recs: Vec<Value> = records
        .iter()
        .map(|r| {
            let mut v: Value = serde_json::from_str(r).expect("记录是 JSON 对象");
            let obj = v.as_object_mut().expect("记录是 JSON 对象");
            obj.entry("_timestamp").or_insert(json!(now));
            v
        })
        .collect();
    ingest_ok(server, stream, &Value::Array(recs).to_string());
    let want = records.len();
    let sql = format!("SELECT count(*) AS n FROM {stream}");
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let r = search(server, &sql, 0, EVAL_END_US);
        if r.status == 200 && r.json()["hits"][0]["n"].as_u64() >= Some(want as u64) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "{stream} 的数据 60 秒内不可查：{}",
            r.text()
        );
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// 「stream `app` 已有数据」的最简形：一条 `{"k":1}`。
pub fn has_one(server: &Server, clock: Clock) {
    has_data(server, clock, "app", &[r#"{"k":1}"#]);
}

/// 带凭据的 `_search`。不写 `size`，取实例的 `query.default_size`（场景可能把 `query.max_size` 设得很小）。
pub fn search(server: &Server, sql: &str, start: i64, end: i64) -> super::Resp {
    let body = json!({"query": {"sql": sql, "start_time": start, "end_time": end}}).to_string();
    server.request(
        "POST",
        "/api/default/_search",
        &[("Authorization", BEARER)],
        body.as_bytes(),
    )
}

/// 「读取 `_alert_evaluations`」一次：`SELECT * FROM _alert_evaluations`（`filter` 非空时加 `WHERE <filter>`），
/// 范围 `[0, 4102444800000000)`。断言 200，返回各行。
pub fn evaluations(server: &Server, filter: &str) -> Vec<Value> {
    let mut sql = "SELECT * FROM _alert_evaluations".to_owned();
    if !filter.is_empty() {
        sql.push_str(" WHERE ");
        sql.push_str(filter);
    }
    let r = search(server, &sql, 0, EVAL_END_US);
    assert_eq!(r.status, 200, "{sql}：{}", r.text());
    r.json()["hits"].as_array().unwrap().clone()
}

/// 反复读取 `_alert_evaluations`（点名 `rule`），直到出现 `scheduled_at` 为 `t` 的行（至多 [`RECORD_WAIT`]），返回该行。
pub fn wait_evaluation(server: &Server, rule: &str, t: i64) -> Value {
    let filter = format!("rule = '{rule}' AND scheduled_at = {t}");
    let deadline = Instant::now() + RECORD_WAIT;
    loop {
        if let Some(row) = evaluations(server, &filter).into_iter().next() {
            return row;
        }
        assert!(
            Instant::now() < deadline,
            "{RECORD_WAIT:?} 内没有读到 {rule} 在 {t} 的评估记录"
        );
        std::thread::sleep(Duration::from_millis(500));
    }
}

/// 「等过第 k 个计划时刻」的计划时刻：自 `from`（被测进程时钟）起第 `k` 个（`k` 从 1 起）严格晚于它的计划时刻。
pub fn kth_tick(from: i64, interval_us: i64, k: i64) -> i64 {
    next_tick_after(from, interval_us) + (k - 1) * interval_us
}

/// 等过计划时刻 `t`：等被测进程的时钟到 `t`，再等该轮评估的记录写出（经 [`wait_evaluation`]），返回该行。
pub fn wait_tick(server: &Server, clock: Clock, rule: &str, t: i64) -> Value {
    clock.sleep_until(t);
    wait_evaluation(server, rule, t)
}

/// `/metrics` 中样本 `series`（指标名连同标签）的值；此前不存在的序列按 0（场景约定）。
pub fn metric_or_zero(server: &Server, series: &str) -> f64 {
    super::store::metric(server, series).unwrap_or(0.0)
}

/// `amberlog_alert_deliveries_total{rule,result}` 的值（不存在按 0）。
pub fn deliveries(server: &Server, rule: &str, result: &str) -> f64 {
    metric_or_zero(
        server,
        &format!("amberlog_alert_deliveries_total{{rule=\"{rule}\",result=\"{result}\"}}"),
    )
}

/// `amberlog_alert_evaluations_total{rule,outcome}` 的值（不存在按 0）。
pub fn evaluations_total(server: &Server, rule: &str, outcome: &str) -> f64 {
    metric_or_zero(
        server,
        &format!("amberlog_alert_evaluations_total{{rule=\"{rule}\",outcome=\"{outcome}\"}}"),
    )
}
