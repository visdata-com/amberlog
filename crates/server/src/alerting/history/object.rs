//! 评估记录对象的路径与格式（REQ-AMB-ALR-EVALUATIONS A 组）。
//!
//! 写出与读取用同一个解析与校验函数 [`parse`]：字段恰好齐全、类型对、`format_version` 为 1 才可解析；
//! 其余（含 `format_version` 大于 1）一律按 REQ-AMB-ALR-EVALUATIONS-UNPARSEABLE 处置。

use amberlog_common::dataset::is_valid_stream_name;
use amberlog_query::system::EvaluationRow;
use amberlog_storage::layout::HourBucket;
use serde_json::{Map, Value, json};

use crate::alerting::outcome::EvaluationRecord;

pub(super) const HOUR_US: i64 = 3_600_000_000;

/// 本块的 `format_version`。
const FORMAT_VERSION: i64 = 1;

/// 评估记录对象相对 `<storage.prefix>/_system/` 的前缀。
pub(super) const ROOT: &str = "alerts";

/// 一个评估记录对象：规则名与计划时刻所在 UTC 小时的起点（REQ-AMB-ALR-EVALUATIONS-TERM-RECORD）。
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct ObjectKey {
    pub rule: String,
    pub hour: i64,
}

impl ObjectKey {
    /// 规则 `rule` 在计划时刻 `scheduled_at` 的评估记录所属的对象（REQ-AMB-ALR-EVALUATIONS-OBJECT-PLACE）。
    pub fn of(rule: &str, scheduled_at: i64) -> ObjectKey {
        ObjectKey {
            rule: rule.to_owned(),
            hour: HourBucket::of(scheduled_at).start_us(),
        }
    }

    /// 相对 `<storage.prefix>/_system/` 的路径 `alerts/<rule>/<YYYY>/<MM>/<DD>/<HH>.json`。
    pub fn path(&self) -> String {
        let label = HourBucket::of(self.hour).to_string();
        format!(
            "{}/{}/{}/{}/{}.json",
            rule_prefix(&self.rule),
            &label[0..4],
            &label[4..6],
            &label[6..8],
            &label[8..10]
        )
    }

    /// 列举所得的路径（相对 `_system/`）按模板 `alerts/<rule>/<YYYY>/<MM>/<DD>/<HH>.json` 严格解析：`<rule>` 合规则名的
    /// 正则 `^[a-z0-9][a-z0-9_]{0,63}$`（REQ-AMB-ALR-RULES-NAME，含保留名），年月日时是合法的 UTC 小时，四位年、其余两位。
    /// 不合模板为 `None`。
    pub fn parse(path: &str) -> Option<ObjectKey> {
        let parts: Vec<&str> = path.split('/').collect();
        let [root, rule, y, m, d, file] = parts.as_slice() else {
            return None;
        };
        let h = file.strip_suffix(".json")?;
        if *root != ROOT
            || !rule_name_shape(rule)
            || y.len() != 4
            || m.len() != 2
            || d.len() != 2
            || h.len() != 2
        {
            return None;
        }
        let hour = HourBucket::parse(&format!("{y}{m}{d}{h}"))?;
        Some(ObjectKey {
            rule: (*rule).to_owned(),
            hour: hour.start_us(),
        })
    }

    /// 该小时与 `[start, end)` 有交集（REQ-AMB-ALR-EVALUATIONS-RANGE-READ）。
    pub fn overlaps(&self, start: i64, end: i64) -> bool {
        self.hour < end && self.hour_end() > start
    }

    /// 该小时的结束时刻（下一小时的起点）。
    pub fn hour_end(&self) -> i64 {
        self.hour.saturating_add(HOUR_US)
    }
}

/// 规则 `rule` 的对象所在的前缀 `alerts/<rule>`（相对 `_system/`）；`rule` 须先经 [`rule_name_shape`] 核对。
pub(super) fn rule_prefix(rule: &str) -> String {
    format!("{ROOT}/{rule}")
}

/// 规则名的正则 `^[a-z0-9][a-z0-9_]{0,63}$`（REQ-AMB-ALR-RULES-NAME）：只有合它的名字才可能是评估记录对象的 `<rule>`。
pub(super) fn rule_name_shape(name: &str) -> bool {
    is_valid_stream_name(name)
}

/// `evaluations` 的一个元素（REQ-AMB-ALR-EVALUATIONS-ENTRY-SHAPE 的十二个字段）。没有地址与请求头的字段
/// （REQ-AMB-ALR-EVALUATIONS-NO-SECRETS）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Entry {
    pub scheduled_at: i64,
    pub started_at: Option<i64>,
    pub finished_at: i64,
    pub window_start: i64,
    pub window_end: i64,
    pub outcome: String,
    pub reason: Option<String>,
    pub matched_rows: Option<i64>,
    pub truncated: Option<bool>,
    pub delivery: String,
    pub attempts: i64,
    pub last_http_status: Option<i64>,
}

impl Entry {
    /// 一次已结束的评估的元素（REQ-AMB-ALR-EVALUATIONS-ENTRY-VALUES）。
    pub fn of(r: &EvaluationRecord) -> Entry {
        Entry {
            scheduled_at: r.scheduled_at,
            started_at: r.started_at,
            finished_at: r.finished_at,
            window_start: r.window_start,
            window_end: r.window_end,
            outcome: r.outcome.label().to_owned(),
            reason: r.outcome.reason().map(str::to_owned),
            matched_rows: r.matched_rows.map(|n| i64::try_from(n).unwrap_or(i64::MAX)),
            truncated: r.truncated,
            delivery: r.delivery.label().to_owned(),
            attempts: i64::from(r.attempts),
            last_http_status: r.last_http_status.map(i64::from),
        }
    }

    /// `_alert_evaluations` 的一行（`rule` 取对象的 `rule`）。
    pub fn row(&self, rule: &str) -> EvaluationRow {
        EvaluationRow {
            rule: rule.to_owned(),
            scheduled_at: self.scheduled_at,
            started_at: self.started_at,
            finished_at: self.finished_at,
            window_start: self.window_start,
            window_end: self.window_end,
            outcome: self.outcome.clone(),
            reason: self.reason.clone(),
            matched_rows: self.matched_rows,
            truncated: self.truncated,
            delivery: self.delivery.clone(),
            attempts: self.attempts,
            last_http_status: self.last_http_status,
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "scheduled_at": self.scheduled_at,
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "window_start": self.window_start,
            "window_end": self.window_end,
            "outcome": self.outcome,
            "reason": self.reason,
            "matched_rows": self.matched_rows,
            "truncated": self.truncated,
            "delivery": self.delivery,
            "attempts": self.attempts,
            "last_http_status": self.last_http_status,
        })
    }
}

/// 一个可解析的评估记录对象的内容。
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct Document {
    pub org: String,
    pub rule: String,
    pub evaluations: Vec<Entry>,
}

/// 对象的内容（REQ-AMB-ALR-EVALUATIONS-OBJECT-SHAPE）；`evaluations` 须已按 `scheduled_at` 升序、无重复。
pub(super) fn encode(org: &str, rule: &str, evaluations: &[Entry]) -> Vec<u8> {
    let doc = json!({
        "format_version": FORMAT_VERSION,
        "org": org,
        "rule": rule,
        "evaluations": evaluations.iter().map(Entry::to_json).collect::<Vec<_>>(),
    });
    serde_json::to_vec(&doc).expect("JSON 值总能序列化")
}

/// 把 `new` 并入 `base`（REQ-AMB-ALR-EVALUATIONS-ENTRY-ORDER）：按 `scheduled_at` 升序；同一 `scheduled_at` 已有元素时
/// 保留已有的，不再加第二个。
pub(super) fn merge(base: &[Entry], new: &[Entry]) -> Vec<Entry> {
    let mut out: Vec<Entry> = base.to_vec();
    for e in new {
        if let Err(i) = out.binary_search_by_key(&e.scheduled_at, |x| x.scheduled_at) {
            out.insert(i, e.clone());
        }
    }
    out
}

/// 解析与校验（REQ-AMB-ALR-EVALUATIONS-UNPARSEABLE）：不是合法 JSON、`format_version` 缺失或不是整数或不为 1、
/// 字段多出或缺少、类型不对，都为 `Err`（说明不含对象内容）。读到的元素按 `scheduled_at` 升序整理，同一计划时刻只留先出现的。
pub(super) fn parse(bytes: &[u8]) -> Result<Document, String> {
    let value: Value = serde_json::from_slice(bytes).map_err(|e| format!("not valid JSON: {e}"))?;
    let obj = value
        .as_object()
        .ok_or_else(|| "not a JSON object".to_owned())?;
    exact_keys(
        obj,
        &["format_version", "org", "rule", "evaluations"],
        "the object",
    )?;
    match obj["format_version"].as_i64() {
        Some(FORMAT_VERSION) => {}
        Some(v) if v > FORMAT_VERSION => {
            return Err(format!("format_version {v} is newer than supported (1)"));
        }
        _ => return Err("format_version is not a positive integer".to_owned()),
    }
    let org = string(obj, "org")?;
    let rule = string(obj, "rule")?;
    let items = obj["evaluations"]
        .as_array()
        .ok_or_else(|| "evaluations is not an array".to_owned())?;
    let mut entries = Vec::with_capacity(items.len());
    for (i, item) in items.iter().enumerate() {
        entries.push(entry(item).map_err(|e| format!("evaluations[{i}]: {e}"))?);
    }
    Ok(Document {
        org,
        rule,
        evaluations: merge(&[], &entries),
    })
}

const ENTRY_FIELDS: [&str; 12] = [
    "scheduled_at",
    "started_at",
    "finished_at",
    "window_start",
    "window_end",
    "outcome",
    "reason",
    "matched_rows",
    "truncated",
    "delivery",
    "attempts",
    "last_http_status",
];

fn entry(item: &Value) -> Result<Entry, String> {
    let obj = item
        .as_object()
        .ok_or_else(|| "not a JSON object".to_owned())?;
    exact_keys(obj, &ENTRY_FIELDS, "the element")?;
    Ok(Entry {
        scheduled_at: int(obj, "scheduled_at")?,
        started_at: nullable(obj, "started_at", Value::as_i64, "an integer")?,
        finished_at: int(obj, "finished_at")?,
        window_start: int(obj, "window_start")?,
        window_end: int(obj, "window_end")?,
        outcome: string(obj, "outcome")?,
        reason: nullable(obj, "reason", |v| v.as_str().map(str::to_owned), "a string")?,
        matched_rows: nullable(obj, "matched_rows", Value::as_i64, "an integer")?,
        truncated: nullable(obj, "truncated", Value::as_bool, "a boolean")?,
        delivery: string(obj, "delivery")?,
        attempts: int(obj, "attempts")?,
        last_http_status: nullable(obj, "last_http_status", Value::as_i64, "an integer")?,
    })
}

fn exact_keys(obj: &Map<String, Value>, want: &[&str], what: &str) -> Result<(), String> {
    if let Some(k) = obj.keys().find(|k| !want.contains(&k.as_str())) {
        return Err(format!("{what} has an unknown field `{k}`"));
    }
    if let Some(k) = want.iter().find(|k| !obj.contains_key(**k)) {
        return Err(format!("{what} lacks the field `{k}`"));
    }
    Ok(())
}

fn int(obj: &Map<String, Value>, key: &str) -> Result<i64, String> {
    obj[key]
        .as_i64()
        .ok_or_else(|| format!("`{key}` is not an integer"))
}

fn string(obj: &Map<String, Value>, key: &str) -> Result<String, String> {
    obj[key]
        .as_str()
        .map(str::to_owned)
        .ok_or_else(|| format!("`{key}` is not a string"))
}

fn nullable<T>(
    obj: &Map<String, Value>,
    key: &str,
    get: impl Fn(&Value) -> Option<T>,
    kind: &str,
) -> Result<Option<T>, String> {
    let v = &obj[key];
    if v.is_null() {
        return Ok(None);
    }
    get(v)
        .map(Some)
        .ok_or_else(|| format!("`{key}` is neither null nor {kind}"))
}
