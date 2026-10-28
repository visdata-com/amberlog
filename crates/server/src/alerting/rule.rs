//! 告警规则与条件（REQ-AMB-ALR-RULES A 组、C 组「条件」；内置规则见 F 组）。
//!
//! 规则在配置一步由 `[[alerts.rules]]` 与 `[alerts.missed_collection]` 解析而来，只在启动时读（-NO-RELOAD）；
//! 解析与校验之后交给调度的只有启用的规则。

use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use crate::alerting::webhook::{self, Target};
use crate::config::parse_duration;

/// 内置规则的名字（REQ-AMB-ALR-RULES-BUILTIN-TERM）。
pub const MISSED_COLLECTION: &str = "missed_collection";

/// 内置规则在请求体中的 `sql`（REQ-AMB-ALR-RULES-BUILTIN-AS-RULE）。
pub const MISSED_COLLECTION_SQL: &str = "builtin:missed_collection";

/// 一条启用的规则（含启用的内置规则）。
#[derive(Debug, Clone)]
pub struct Rule {
    pub name: String,
    /// 当前版本恒为 `default`（REQ-AMB-ALR-RULES-ORG）。
    pub org: String,
    /// 整分钟，1 分钟至 24 小时。
    pub interval: Duration,
    /// 整分钟，1 分钟至 7 天；未配置时等于 `interval`。
    pub window: Duration,
    pub source: Source,
    pub condition: Condition,
    pub webhook: Target,
}

/// 求结果的方式。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// 普通规则：执行规则的 `sql`（REQ-AMB-ALR-RULES-QUERY-RESULT）。
    Sql(String),
    /// 内置规则：向治理视图取转为缺失的窗口（REQ-AMB-ALR-RULES-BUILTIN-RESULT），不执行查询。
    MissedCollection,
}

impl Rule {
    /// 请求体的 `sql`：普通规则为其 `sql` 原文，内置规则为 [`MISSED_COLLECTION_SQL`]。
    pub fn sql_text(&self) -> &str {
        match &self.source {
            Source::Sql(sql) => sql,
            Source::MissedCollection => MISSED_COLLECTION_SQL,
        }
    }

    /// `interval` 的微秒数。
    pub fn interval_us(&self) -> i64 {
        micros(self.interval)
    }

    /// `window` 的微秒数。
    pub fn window_us(&self) -> i64 {
        micros(self.window)
    }
}

fn micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

/// 规则的条件（REQ-AMB-ALR-RULES-CONDITION）。
#[derive(Debug, Clone, PartialEq)]
pub enum Condition {
    /// 行数条件 `{ rows = "<比较符> <n>" }`。
    Rows { op: CmpOp, n: u64 },
    /// 列条件 `{ column, op, threshold }`。
    Column {
        column: String,
        op: CmpOp,
        threshold: Threshold,
    },
}

/// 比较符。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CmpOp {
    Gt,
    Ge,
    Lt,
    Le,
    Eq,
    Ne,
}

impl CmpOp {
    /// 条件写法中的比较符：`>`、`>=`、`<`、`<=`、`=`、`!=`。
    pub fn parse(s: &str) -> Option<CmpOp> {
        Some(match s {
            ">" => CmpOp::Gt,
            ">=" => CmpOp::Ge,
            "<" => CmpOp::Lt,
            "<=" => CmpOp::Le,
            "=" => CmpOp::Eq,
            "!=" => CmpOp::Ne,
            _ => return None,
        })
    }

    /// 左操作数与右操作数的大小关系是否使比较成立。
    pub fn holds(self, ord: Ordering) -> bool {
        match self {
            CmpOp::Gt => ord == Ordering::Greater,
            CmpOp::Ge => ord != Ordering::Less,
            CmpOp::Lt => ord == Ordering::Less,
            CmpOp::Le => ord != Ordering::Greater,
            CmpOp::Eq => ord == Ordering::Equal,
            CmpOp::Ne => ord != Ordering::Equal,
        }
    }
}

/// 列条件的阈值：TOML 整数或有限的 TOML 浮点数（按其双精度值）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Threshold {
    Int(i64),
    Float(f64),
}

/// 结果中一个数值列的非 NULL 取值。
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Number {
    Int64(i64),
    Float64(f64),
}

impl Threshold {
    /// 列取值 `value` 与阈值按 `op` 比较是否成立（REQ-AMB-ALR-RULES-MATCH-COLUMN）：比较口径同 REQ-AMB-QRY-SEARCH-ENDPOINT
    /// 对列与数值字面量的规定——Int64 列按精确值比较（浮点阈值不先近似为整数），Float64 列与阈值就近舍入所得的双精度数比较。
    pub fn satisfied_by(self, value: Number, op: CmpOp) -> bool {
        let ord = match (value, self) {
            (Number::Int64(v), Threshold::Int(t)) => v.cmp(&t),
            (Number::Int64(v), Threshold::Float(t)) => cmp_i64_f64(v, t),
            // i64 → f64 的 `as` 转换就近舍入（同距取偶）
            (Number::Float64(v), Threshold::Int(t)) => cmp_f64(v, t as f64),
            (Number::Float64(v), Threshold::Float(t)) => cmp_f64(v, t),
        };
        op.holds(ord)
    }
}

/// 两个有限的双精度数的大小（`-0.0` 与 `0.0` 相等）。
fn cmp_f64(a: f64, b: f64) -> Ordering {
    a.partial_cmp(&b).unwrap_or(Ordering::Equal)
}

/// 整数 `v` 与有限的双精度数 `t` 按精确值比较。
fn cmp_i64_f64(v: i64, t: f64) -> Ordering {
    // 2^63：i64 的取值都小于它，都不小于其相反数
    const TWO_63: f64 = 9_223_372_036_854_775_808.0;
    if t >= TWO_63 {
        return Ordering::Less;
    }
    if t < -TWO_63 {
        return Ordering::Greater;
    }
    // |t| < 2^63 时其整数部分可精确表示为 i64
    let whole = t.trunc();
    match v.cmp(&(whole as i64)) {
        Ordering::Equal => cmp_f64(0.0, t - whole),
        ord => ord,
    }
}

/// 规则数上限（REQ-AMB-ALR-RULES-MAX-RULES，含停用的规则）。
pub const MAX_RULES: usize = 100;

/// 规则的字段（REQ-AMB-ALR-RULES-FIELDS）。
const FIELDS: &[&str] = &[
    "name",
    "interval",
    "org",
    "sql",
    "window",
    "condition",
    "webhook_url",
    "webhook_headers",
    "enabled",
];

const MINUTE: Duration = Duration::from_secs(60);

/// 解析并校验配置中 `alerts.rules` 的取值（REQ-AMB-ALR-RULES A 组与 REQ-AMB-ALR-WEBHOOK A 组），返回启用的规则（按配置中的
/// 先后）。不合者逐条给出说明：以 `rule `<name>``（名字缺失或不合法时为 `rule #<序号>`，自 1 起）开头，只写字段名与原因，
/// 不回显任何字段的取值（REQ-AMB-ALR-WEBHOOK-HEADER-VALUES-HIDDEN、-LOG-URL）。
pub fn parse_rules(value: &toml::Value) -> Result<Vec<Arc<Rule>>, Vec<String>> {
    let Some(items) = value.as_array() else {
        return Err(vec![
            "expected a TOML array of tables ([[alerts.rules]])".into(),
        ]);
    };
    let mut errors = Vec::new();
    if items.len() > MAX_RULES {
        errors.push(format!(
            "at most {MAX_RULES} rules are allowed (found {})",
            items.len()
        ));
    }
    let mut seen = BTreeSet::new();
    let mut rules = Vec::new();
    for (i, item) in items.iter().enumerate() {
        let name = item.get("name").and_then(toml::Value::as_str);
        let shown = match name {
            Some(n) if valid_name(n) => format!("rule `{n}`"),
            _ => format!("rule #{}", i + 1),
        };
        match parse_rule(item) {
            Ok((rule, enabled)) => {
                if !seen.insert(rule.name.clone()) {
                    errors.push(format!("{shown}: another rule has the same name"));
                } else if enabled {
                    rules.push(Arc::new(rule));
                }
            }
            Err(why) => errors.push(format!("{shown}: {why}")),
        }
    }
    if errors.is_empty() {
        Ok(rules)
    } else {
        Err(errors)
    }
}

/// 规则名的写法（REQ-AMB-ALR-RULES-NAME）：`^[a-z0-9][a-z0-9_]{0,63}$`，不以 `amberlog_` 开头，不为内置规则的名字。
fn valid_name(name: &str) -> bool {
    amberlog_common::dataset::is_valid_stream_name(name)
        && !name.starts_with("amberlog_")
        && name != MISSED_COLLECTION
}

/// 一条规则；返回规则与其 `enabled`。说明不含字段的取值。
fn parse_rule(item: &toml::Value) -> Result<(Rule, bool), String> {
    let Some(table) = item.as_table() else {
        return Err("expected a TOML table".into());
    };
    if let Some(unknown) = table.keys().find(|k| !FIELDS.contains(&k.as_str())) {
        return Err(format!("unknown field `{unknown}`"));
    }
    let name = required_str(table, "name")?;
    if !valid_name(name) {
        return Err(
            "field `name` must match ^[a-z0-9][a-z0-9_]{0,63}$, must not start with `amberlog_` and must not be `missed_collection`"
                .into(),
        );
    }
    let interval = minutes(
        required_str(table, "interval")?,
        "interval",
        MINUTE * 60 * 24,
    )?;
    let window = match optional_str(table, "window")? {
        None => interval,
        Some(w) => minutes(w, "window", MINUTE * 60 * 24 * 7)?,
    };
    let org = optional_str(table, "org")?.unwrap_or(amberlog_common::dataset::DEFAULT_ORG);
    if org != amberlog_common::dataset::DEFAULT_ORG {
        return Err("field `org` must be `default`".into());
    }
    let sql = required_str(table, "sql")?;
    amberlog_query::search::check_static(sql).map_err(|e| {
        format!(
            "field `sql` is not accepted ({}): {}",
            e.reason.as_str(),
            e.message
        )
    })?;
    let condition = match table.get("condition") {
        None => return Err("field `condition` is required".into()),
        Some(c) => parse_condition(c)?,
    };
    let url = webhook::parse_url(required_str(table, "webhook_url")?)?;
    let headers = match table.get("webhook_headers") {
        None => Vec::new(),
        Some(h) => webhook::parse_headers(h)?,
    };
    let enabled = match table.get("enabled") {
        None => true,
        Some(toml::Value::Boolean(b)) => *b,
        Some(_) => return Err("field `enabled` must be a TOML boolean".into()),
    };
    Ok((
        Rule {
            name: name.to_owned(),
            org: org.to_owned(),
            interval,
            window,
            source: Source::Sql(sql.to_owned()),
            condition,
            webhook: Target { url, headers },
        },
        enabled,
    ))
}

fn required_str<'a>(table: &'a toml::Table, field: &str) -> Result<&'a str, String> {
    optional_str(table, field)?.ok_or_else(|| format!("field `{field}` is required"))
}

fn optional_str<'a>(table: &'a toml::Table, field: &str) -> Result<Option<&'a str>, String> {
    match table.get(field) {
        None => Ok(None),
        Some(toml::Value::String(s)) => Ok(Some(s)),
        Some(_) => Err(format!("field `{field}` must be a TOML string")),
    }
}

/// 时长 `d` 换算后为整分钟，且在 1 分钟与 `max` 之间。
pub fn is_whole_minutes_within(d: Duration, max: Duration) -> bool {
    d >= MINUTE && d <= max && d.as_micros().is_multiple_of(MINUTE.as_micros())
}

/// 内置规则作为一条启用的规则（REQ-AMB-ALR-RULES-BUILTIN-AS-RULE）：`org` 为 `default`、`window` 等于 `interval`、
/// 条件为 `{ rows = "> 0" }`，不执行 SQL。
pub fn builtin(interval: Duration, webhook: Target) -> Rule {
    Rule {
        name: MISSED_COLLECTION.to_owned(),
        org: amberlog_common::dataset::DEFAULT_ORG.to_owned(),
        interval,
        window: interval,
        source: Source::MissedCollection,
        condition: Condition::Rows {
            op: CmpOp::Gt,
            n: 0,
        },
        webhook,
    }
}

/// 时长类取值，换算后为整分钟，在 1 分钟与 `max` 之间（REQ-AMB-ALR-RULES-INTERVAL、-WINDOW）。
fn minutes(text: &str, field: &str, max: Duration) -> Result<Duration, String> {
    match parse_duration(text) {
        Some(d) if is_whole_minutes_within(d, max) => Ok(d),
        _ => Err(format!(
            "field `{field}` must be a duration of whole minutes between 1m and {}",
            if max == MINUTE * 60 * 24 { "24h" } else { "7d" }
        )),
    }
}

/// 条件的两种写法（REQ-AMB-ALR-RULES-CONDITION）。
fn parse_condition(value: &toml::Value) -> Result<Condition, String> {
    const BAD: &str = "field `condition` must be { rows = \"<op> <n>\" } or { column = \"<name>\", op = \"<op>\", threshold = <number> }";
    let Some(table) = value.as_table() else {
        return Err(BAD.into());
    };
    let mut keys: Vec<&str> = table.keys().map(String::as_str).collect();
    keys.sort_unstable();
    match keys.as_slice() {
        ["rows"] => {
            let text = table["rows"].as_str().ok_or(BAD)?;
            parse_rows(text).ok_or_else(|| BAD.to_owned())
        }
        ["column", "op", "threshold"] => {
            let column = table["column"]
                .as_str()
                .filter(|c| !c.is_empty())
                .ok_or(BAD)?;
            let op = table["op"].as_str().and_then(CmpOp::parse).ok_or(BAD)?;
            let threshold = match &table["threshold"] {
                toml::Value::Integer(i) => Threshold::Int(*i),
                toml::Value::Float(f) if f.is_finite() => Threshold::Float(*f),
                _ => return Err(BAD.into()),
            };
            Ok(Condition::Column {
                column: column.to_owned(),
                op,
                threshold,
            })
        }
        _ => Err(BAD.into()),
    }
}

/// 行数条件 `<比较符> <n>`：比较符与整数之间可有空白，整数不带正负号、不大于 2^63 − 1。
fn parse_rows(text: &str) -> Option<Condition> {
    let (op, rest) = [">=", "<=", "!=", ">", "<", "="]
        .iter()
        .find_map(|op| text.strip_prefix(op).map(|rest| (*op, rest)))?;
    let digits = rest.trim_start_matches([' ', '\t']);
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let n: i64 = digits.parse().ok()?;
    Some(Condition::Rows {
        op: CmpOp::parse(op)?,
        n: n as u64,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 一条合法规则的 TOML：`extra` 以 `[[rules]]` 开头时在其后追加规则，否则是覆盖或追加的字段。
    fn rules(extra: &str) -> toml::Value {
        let base = "[[rules]]\nname = \"r1\"\ninterval = \"1m\"\nsql = \"SELECT * FROM app\"\ncondition = { rows = \"> 0\" }\nwebhook_url = \"https://127.0.0.1:9/hook\"\n";
        if extra.starts_with("[[rules]]") {
            let text = format!("{base}{extra}");
            return text.parse::<toml::Table>().unwrap()["rules"].clone();
        }
        let mut v = base.parse::<toml::Table>().unwrap()["rules"].clone();
        let fields = extra.parse::<toml::Table>().unwrap();
        let rule = v.as_array_mut().unwrap()[0].as_table_mut().unwrap();
        for (k, f) in fields {
            rule.insert(k, f);
        }
        v
    }

    fn one_error(v: &toml::Value) -> String {
        let errors = parse_rules(v).unwrap_err();
        assert_eq!(errors.len(), 1, "{errors:?}");
        errors.into_iter().next().unwrap()
    }

    #[test]
    fn valid_rule_with_defaults() {
        let r = parse_rules(&rules("")).unwrap();
        assert_eq!(r.len(), 1);
        let r = &r[0];
        assert_eq!((r.name.as_str(), r.org.as_str()), ("r1", "default"));
        assert_eq!(
            (r.interval, r.window),
            (MINUTE, MINUTE),
            "window 缺省等于 interval"
        );
        assert_eq!(
            r.condition,
            Condition::Rows {
                op: CmpOp::Gt,
                n: 0
            }
        );
        assert_eq!(r.sql_text(), "SELECT * FROM app");
    }

    #[test]
    fn disabled_rules_are_validated_but_not_returned() {
        assert!(parse_rules(&rules("enabled = false")).unwrap().is_empty());
        assert!(one_error(&rules("enabled = false\ninterval = \"30s\"")).contains("interval"));
    }

    #[test]
    fn errors_name_the_rule_and_field_without_values() {
        let e = one_error(&rules("severity = \"high\""));
        assert!(e.starts_with("rule `r1`") && e.contains("severity"), "{e}");
        let e = one_error(&rules("interval = 1"));
        assert!(e.contains("interval") && e.contains("TOML string"), "{e}");
        let e = one_error(&rules("condition = \"rows > 0\""));
        assert!(e.contains("condition"), "{e}");
        let e = one_error(&rules("enabled = \"yes\""));
        assert!(e.contains("enabled"), "{e}");
    }

    #[test]
    fn bad_names_are_reported_by_index() {
        for name in [
            "R1",
            "_r1",
            "a-b",
            "amberlog_x",
            &"a".repeat(65),
            "missed_collection",
        ] {
            let e = one_error(&rules(&format!("name = \"{name}\"")));
            assert!(e.starts_with("rule #1:"), "{name}: {e}");
            // 说明里本来就写着保留名，只核对其余几个不被回显
            assert!(
                name == MISSED_COLLECTION || !e.contains(name),
                "不回显不合法的名字：{e}"
            );
        }
    }

    #[test]
    fn interval_and_window_take_whole_minutes_in_range() {
        for bad in ["30s", "90s", "0m", "25h", "1x"] {
            assert!(one_error(&rules(&format!("interval = \"{bad}\""))).contains("interval"));
        }
        let ok = parse_rules(&rules("interval = \"120s\"\nwindow = \"7d\"")).unwrap();
        assert_eq!(ok[0].interval, MINUTE * 2);
        for bad in ["30s", "8d"] {
            assert!(one_error(&rules(&format!("window = \"{bad}\""))).contains("window"));
        }
    }

    #[test]
    fn org_must_be_default() {
        assert!(one_error(&rules("org = \"acme\"")).contains("org"));
        assert!(parse_rules(&rules("org = \"default\"")).is_ok());
    }

    #[test]
    fn sql_static_check() {
        for bad in [
            "SELEC * FROM app",
            "SELECT * FROM a JOIN b ON a.x = b.x",
            "SELECT * FROM \\\"App\\\"",
        ] {
            let e = one_error(&rules(&format!("sql = \"{bad}\"")));
            assert!(e.contains("sql"), "{bad}: {e}");
        }
        assert!(parse_rules(&rules("sql = \"SELECT * FROM no_such_stream\"")).is_ok());
        assert!(
            one_error(&rules("sql = \"SELECT severity FROM _ledger\"")).contains("unknown_column")
        );
    }

    #[test]
    fn condition_forms() {
        for bad in [
            r#"{ rows = "> -1" }"#,
            r#"{ rows = "~ 3" }"#,
            r#"{ rows = "> 1", column = "n" }"#,
            r#"{ column = "n", op = ">" }"#,
            r#"{ column = "n", op = "gt", threshold = 0 }"#,
            r#"{ column = "", op = ">", threshold = 0 }"#,
            r#"{ rows = "> 9223372036854775808" }"#,
            r#"{ rows = " > 1" }"#,
            r#"{ rows = "> 1 " }"#,
        ] {
            assert!(
                one_error(&rules(&format!("condition = {bad}"))).contains("condition"),
                "{bad}"
            );
        }
        let ok = |c: &str| {
            parse_rules(&rules(&format!("condition = {c}"))).unwrap()[0]
                .condition
                .clone()
        };
        assert_eq!(
            ok(r#"{ rows = ">=3" }"#),
            Condition::Rows {
                op: CmpOp::Ge,
                n: 3
            }
        );
        assert_eq!(
            ok(r#"{ rows = "!= 9223372036854775807" }"#),
            Condition::Rows {
                op: CmpOp::Ne,
                n: i64::MAX as u64
            }
        );
        assert_eq!(
            ok(r#"{ column = "n", op = "<=", threshold = 1.5 }"#),
            Condition::Column {
                column: "n".into(),
                op: CmpOp::Le,
                threshold: Threshold::Float(1.5)
            }
        );
    }

    #[test]
    fn duplicate_names_and_max_rules() {
        let two = rules(
            "[[rules]]\nname = \"r1\"\ninterval = \"1m\"\nsql = \"SELECT * FROM app\"\ncondition = { rows = \"> 0\" }\nwebhook_url = \"https://127.0.0.1:9/hook\"\nenabled = false",
        );
        assert!(one_error(&two).starts_with("rule `r1`"));
        let many = |n: usize| {
            let text: String = (0..n)
                .map(|i| format!("[[rules]]\nname = \"r{i}\"\ninterval = \"1m\"\nsql = \"SELECT * FROM app\"\ncondition = {{ rows = \"> 0\" }}\nwebhook_url = \"https://127.0.0.1:9/hook\"\n"))
                .collect();
            text.parse::<toml::Table>().unwrap()["rules"].clone()
        };
        assert_eq!(parse_rules(&many(100)).unwrap().len(), 100);
        assert!(one_error(&many(101)).contains("100"));
    }

    #[test]
    fn missing_name_reports_index() {
        let v = rules(
            "[[rules]]\nname = \"r2\"\ninterval = \"1m\"\nsql = \"SELECT * FROM app\"\ncondition = { rows = \"> 0\" }\nwebhook_url = \"https://127.0.0.1:9/hook\"\n[[rules]]\ninterval = \"1m\"\nsql = \"SELECT * FROM app\"\ncondition = { rows = \"> 0\" }\nwebhook_url = \"https://127.0.0.1:9/hook\"",
        );
        let e = one_error(&v);
        assert!(e.starts_with("rule #3:") && e.contains("name"), "{e}");
    }

    #[test]
    fn cmp_op_parse_and_holds() {
        for (s, op) in [
            (">", CmpOp::Gt),
            (">=", CmpOp::Ge),
            ("<", CmpOp::Lt),
            ("<=", CmpOp::Le),
            ("=", CmpOp::Eq),
            ("!=", CmpOp::Ne),
        ] {
            assert_eq!(CmpOp::parse(s), Some(op));
        }
        assert_eq!(CmpOp::parse("=="), None);
        assert!(CmpOp::Ge.holds(Ordering::Equal));
        assert!(!CmpOp::Gt.holds(Ordering::Equal));
        assert!(CmpOp::Ne.holds(Ordering::Less));
    }

    #[test]
    fn int64_column_compares_exactly() {
        let t = Threshold::Float(1.5);
        assert!(t.satisfied_by(Number::Int64(2), CmpOp::Gt));
        assert!(!t.satisfied_by(Number::Int64(1), CmpOp::Gt));
        assert!(!t.satisfied_by(Number::Int64(1), CmpOp::Eq));
        assert!(t.satisfied_by(Number::Int64(1), CmpOp::Le));
        // 2^53 + 1 不可表示为双精度：精确比较不把它当成 2^53
        let big = (1i64 << 53) + 1;
        assert!(Threshold::Float((1i64 << 53) as f64).satisfied_by(Number::Int64(big), CmpOp::Gt));
        assert!(Threshold::Float(-0.5).satisfied_by(Number::Int64(0), CmpOp::Gt));
        assert!(Threshold::Float(-0.5).satisfied_by(Number::Int64(-1), CmpOp::Lt));
        assert!(Threshold::Float(1e19).satisfied_by(Number::Int64(i64::MAX), CmpOp::Lt));
        assert!(Threshold::Float(-1e19).satisfied_by(Number::Int64(i64::MIN), CmpOp::Gt));
        assert!(Threshold::Int(i64::MAX).satisfied_by(Number::Int64(i64::MAX), CmpOp::Eq));
    }

    #[test]
    fn float64_column_compares_with_rounded_threshold() {
        // 2^53 + 1 就近舍入为 2^53
        let t = Threshold::Int((1i64 << 53) + 1);
        assert!(t.satisfied_by(Number::Float64((1i64 << 53) as f64), CmpOp::Eq));
        assert!(Threshold::Int(200).satisfied_by(Number::Float64(250.0), CmpOp::Gt));
        assert!(Threshold::Float(0.0).satisfied_by(Number::Float64(-0.0), CmpOp::Eq));
        assert!(!Threshold::Float(1.5).satisfied_by(Number::Float64(1.5), CmpOp::Lt));
    }
}
