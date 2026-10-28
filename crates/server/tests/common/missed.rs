//! 漏采判定（REQ-AMB-GOV-MISSED-COLLECTION）与对账表、台账判定列的系统测试共用部件：按块的「场景共同前提」起实例、
//! 摄取带来源标识与 `_timestamp` 的记录、按时间戳上报发送数、查某窗口的判定、读稳定事件。
//!
//! 「已开始受理请求超过 k 分钟」以 [`Instance::start`] 返回（`ready` 事件之后）起计。

use std::time::{Duration, Instant};

use serde_json::{Value, json};

use super::api::{HOUR_US, MINUTE_US, ingest_with, minute_of, now_us, sent};
use super::gov::{query, query_ok, rfc3339};
use super::{Fixture, Kv, Server};

/// 来源标识请求头。
pub const SOURCE: &str = "X-Amberlog-Source";
/// 场景共同前提的宽限期。
pub const GRACE_1M: Kv = ("reconciliation.grace", Some("\"1m\""));
/// 场景共同前提的「迟到的摄取或上报之后」要等的时长：宽限期 1 分钟、后台判定周期，再留余量。
pub const SETTLE: Duration = Duration::from_secs(150);

/// 一个运行中的实例与它开始受理请求的时刻。
pub struct Instance {
    pub fx: Fixture,
    pub server: Server,
    pub accepting: Instant,
    pub overrides: Vec<Kv<'static>>,
}

impl Instance {
    /// 以 `overrides` 启动（`log.format` 为缺省的 json）。
    pub fn start(overrides: &[Kv<'static>]) -> Instance {
        let fx = Fixture::new();
        let server = fx.start(overrides);
        Instance {
            fx,
            server,
            accepting: Instant::now(),
            overrides: overrides.to_vec(),
        }
    }

    /// 以 `reconciliation.grace` 为 `1m` 启动，等到开始受理请求超过 `warm`（场景共同前提为 2 分钟）。
    pub fn warmed(warm: Duration) -> Instance {
        let i = Instance::start(&[GRACE_1M]);
        i.wait_accepting(warm);
        i
    }

    /// 等到开始受理请求超过 `d`。
    pub fn wait_accepting(&self, d: Duration) {
        let until = self.accepting + d + Duration::from_secs(2);
        if let Some(left) = until.checked_duration_since(Instant::now()) {
            std::thread::sleep(left);
        }
    }

    /// 以同一配置与 `wal.dir` 重启（先由调用方停掉旧进程）。
    pub fn restart(&mut self) {
        self.server = self.fx.start(&self.overrides);
        self.accepting = Instant::now();
    }
}

/// 此刻所在整分钟之前第 `k` 个整分钟的起点。
pub fn minute_ago(k: i64) -> i64 {
    minute_of(now_us()) - k * MINUTE_US
}

/// 此刻所在小时之前第 `k` 个小时的起点。
pub fn hour_ago(k: i64) -> i64 {
    let now = now_us();
    now - now.rem_euclid(HOUR_US) - k * HOUR_US
}

/// 向 `stream` 摄取 `_timestamp` 依次为 `timestamps` 的记录（带来源标识时加请求头），断言 200。
pub fn ingest_at(server: &Server, stream: &str, source: Option<&str>, timestamps: &[i64]) {
    let body: Vec<Value> = timestamps
        .iter()
        .map(|t| json!({"_timestamp": rfc3339(*t), "a": 1}))
        .collect();
    let headers: Vec<(&str, &str)> = source.map(|s| (SOURCE, s)).into_iter().collect();
    let r = ingest_with(server, stream, &headers, json!(body).to_string().as_bytes());
    assert_eq!(r.status, 200, "{}", r.text());
}

/// `n` 个落在窗口 `w` 内的时刻（`w` 起点之后第 1、2……秒）。
pub fn in_window(w: i64, n: usize) -> Vec<i64> {
    (1..=n as i64).map(|i| w + i * 1_000_000).collect()
}

/// 以 `basis` 为 `timestamp` 上报 `source` 在窗口 `w` 的发送数 `n`，断言 200。
pub fn report(server: &Server, stream: &str, source: &str, w: i64, n: u64) {
    let body = json!([{"source": source, "window_start": w, "window_end": w + MINUTE_US,
                       "sent": n, "basis": "timestamp"}]);
    let r = sent(server, stream, &body.to_string());
    assert_eq!(r.status, 200, "{}", r.text());
}

/// 查窗口 `w` 的判定：以恰覆盖 `w` 的时间范围查询 `stream` 的 `_reconciliation`；没有行时为 `None`。
pub fn verdict(server: &Server, stream: &str, w: i64) -> Option<String> {
    let sql = format!("SELECT verdict FROM _reconciliation WHERE dataset = 'default/{stream}'");
    let r = query(server, &sql, (w, w + MINUTE_US));
    assert_eq!(r.status, 200, "{}", r.text());
    r.json()["hits"]
        .as_array()
        .unwrap()
        .first()
        .map(|h| h["verdict"].as_str().unwrap().to_owned())
}

/// 同 [`verdict`]，断言有一行。
pub fn verdict_of(server: &Server, stream: &str, w: i64) -> String {
    verdict(server, stream, w).unwrap_or_else(|| panic!("{stream} 窗口 {w} 没有对账行"))
}

/// 以时间范围 `range` 查询 `stream` 的 `_ledger`，返回唯一的一行。
pub fn ledger_row(server: &Server, stream: &str, range: (i64, i64)) -> Value {
    let sql = format!("SELECT * FROM _ledger WHERE dataset = 'default/{stream}'");
    let hits = query_ok(server, &sql, range);
    assert_eq!(hits.len(), 1, "{hits:?}");
    hits[0].clone()
}

/// stderr 中 `event` 为 `name` 的稳定事件。
pub fn events(server: &Server, name: &str) -> Vec<Value> {
    server
        .json_lines()
        .into_iter()
        .filter(|l| l["event"] == name)
        .collect()
}

/// 计数对象的一个 `windows` 元素，含按时间戳的字段。
pub fn ts_entry(
    w: i64,
    received: u64,
    durable: u64,
    sent: Value,
    sent_ts: Option<Value>,
    durable_ts: Option<(u64, Value)>,
) -> Value {
    let mut e = super::gov::entry(w, received, durable, sent);
    if let Some(s) = sent_ts {
        e["sent_ts"] = s;
    }
    if let Some((n, by_source)) = durable_ts {
        e["durable_ts"] = json!(n);
        if !by_source.is_null() {
            e["durable_ts_by_source"] = by_source;
        }
    }
    e
}
