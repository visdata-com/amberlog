//! `_search` 请求体的解析与校验（REQ-AMB-QRY-SEARCH-ENDPOINT-V9「解析请求体」）。
//!
//! 报告顺序的前两位在这里定：先把字段缺失、类型不符、`size`/`from` 越界与时区名不可认（`invalid_request`）全部查完，
//! 再查时间范围（`invalid_time_range`）。未定义的字段一律忽略。

use amberlog_common::error::Reason;
use amberlog_common::json::{parse_body, protocol_integer};
use jiff::tz::TimeZone;
use serde_json::{Map, Value};

use crate::error::QueryError;
use crate::histogram;

/// `query.default_size` 与 `query.max_size`（REQ-AMB-CFG-STARTUP-V2；启动时已校验前者不大于后者）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub default_size: u64,
    pub max_size: u64,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            default_size: 100,
            max_size: 10_000,
        }
    }
}

/// 通过校验的请求：`size` 与 `from` 已回填缺省值。时间范围为 UTC 微秒左闭右开 `[start_time, end_time)`。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SearchRequest {
    pub sql: String,
    pub start_time: i64,
    pub end_time: i64,
    pub size: u64,
    pub from: u64,
    /// 请求的时区（`query.timezone`，不带时为 UTC）；只用于 histogram 调用的取值。
    pub timezone: TimeZone,
}

/// 解析请求体字节。
pub fn parse(body: &[u8], limits: &Limits) -> Result<SearchRequest, QueryError> {
    let value = parse_body(body).map_err(|e| {
        QueryError::new(
            Reason::InvalidJson,
            format!("request body is not valid JSON: {e}"),
        )
    })?;
    let Value::Object(top) = value else {
        return Err(QueryError::invalid_request(
            "request body must be a JSON object",
        ));
    };
    let Some(Value::Object(query)) = top.get("query") else {
        return Err(QueryError::invalid_request(
            "field `query` must be a JSON object",
        ));
    };

    let sql = match query.get("sql") {
        Some(Value::String(s)) => s.clone(),
        _ => {
            return Err(QueryError::invalid_request(
                "field `query.sql` must be a string",
            ));
        }
    };
    let start_time = required_integer(query, "start_time")?;
    let end_time = required_integer(query, "end_time")?;
    let size = match optional_integer(query, "size")? {
        None => limits.default_size,
        Some(n) if n < 0 || n as u64 > limits.max_size => {
            return Err(QueryError::invalid_request(format!(
                "field `query.size` must be between 0 and {}",
                limits.max_size
            )));
        }
        Some(n) => n as u64,
    };
    let from = match optional_integer(query, "from")? {
        None => 0,
        Some(n) if n < 0 => {
            return Err(QueryError::invalid_request(
                "field `query.from` must not be negative",
            ));
        }
        Some(n) => n as u64,
    };
    let timezone = match query.get("timezone") {
        None => TimeZone::UTC,
        Some(Value::String(name)) => histogram::time_zone(name).ok_or_else(|| {
            QueryError::invalid_request(format!(
                "field `query.timezone` must be an IANA time zone name or `UTC`; `{name}` is not recognized"
            ))
        })?,
        Some(_) => {
            return Err(QueryError::invalid_request(
                "field `query.timezone` must be a string",
            ));
        }
    };

    if start_time < 0 || end_time < 0 || end_time <= start_time {
        return Err(QueryError::new(
            Reason::InvalidTimeRange,
            "time range must satisfy 0 <= start_time < end_time",
        ));
    }

    Ok(SearchRequest {
        sql,
        start_time,
        end_time,
        size,
        from,
        timezone,
    })
}

fn required_integer(query: &Map<String, Value>, key: &str) -> Result<i64, QueryError> {
    optional_integer(query, key)?
        .ok_or_else(|| QueryError::invalid_request(format!("field `query.{key}` is required")))
}

/// 键不存在为 `None`；存在而不是协议整数（REQ-AMB-ERR-ENVELOPE-V2）即类型不符。
fn optional_integer(query: &Map<String, Value>, key: &str) -> Result<Option<i64>, QueryError> {
    match query.get(key) {
        None => Ok(None),
        Some(v) => protocol_integer(v).map(Some).ok_or_else(|| {
            QueryError::invalid_request(format!("field `query.{key}` must be an integer"))
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const Q: &str = r#""sql":"SELECT * FROM app","start_time":0,"end_time":4102444800000000"#;

    fn reason(body: &str) -> Result<SearchRequest, Reason> {
        parse(body.as_bytes(), &Limits::default()).map_err(|e| e.reason)
    }

    /// 场景「请求体字段的缺失、类型与越界」的请求体表，在解析这一层逐行核对。
    #[test]
    fn request_field_table() {
        let with_q = |extra: &str| format!(r#"{{"query":{{{Q},{extra}}}}}"#);
        let bad = [
            "[]".to_owned(),
            "{}".to_owned(),
            r#"{"query":"SELECT * FROM app"}"#.to_owned(),
            r#"{"query":{"start_time":0,"end_time":10}}"#.to_owned(),
            r#"{"query":{"sql":1,"start_time":0,"end_time":10}}"#.to_owned(),
            r#"{"query":{"sql":"SELECT * FROM app","end_time":10}}"#.to_owned(),
            r#"{"query":{"sql":"SELECT * FROM app","start_time":"0","end_time":10}}"#.to_owned(),
            with_q(r#""size":-1"#),
            with_q(r#""size":10001"#),
            with_q(r#""from":-1"#),
            with_q(r#""from":"0""#),
            with_q(r#""size":"10""#),
            with_q(r#""size":1.0"#),
            with_q(r#""from":null"#),
        ];
        for body in &bad {
            assert_eq!(reason(body), Err(Reason::InvalidRequest), "{body}");
        }
        let ok = reason(&format!(r#"{{"query":{{{Q},"extra":1}},"x":2}}"#)).unwrap();
        assert_eq!((ok.size, ok.from), (100, 0), "缺省回填");
        let ok = reason(&with_q(r#""size":10000,"from":7"#)).unwrap();
        assert_eq!((ok.size, ok.from), (10_000, 7));
        assert_eq!(reason("{"), Err(Reason::InvalidJson));
    }

    #[test]
    fn time_range_is_non_negative_and_non_empty() {
        let body = |s: i64, e: i64| {
            format!(r#"{{"query":{{"sql":"SELECT * FROM app","start_time":{s},"end_time":{e}}}}}"#)
        };
        assert_eq!(reason(&body(5, 5)), Err(Reason::InvalidTimeRange));
        assert_eq!(reason(&body(-1, 10)), Err(Reason::InvalidTimeRange));
        assert_eq!(reason(&body(3000, 1000)), Err(Reason::InvalidTimeRange));
        assert!(reason(&body(0, 1)).is_ok());
    }

    /// `invalid_request` 先于 `invalid_time_range`：倒置的时间范围加上越界的 `size` 报前者。
    #[test]
    fn invalid_request_reported_before_time_range() {
        let body = r#"{"query":{"sql":"SELEC","size":-1,"start_time":3000,"end_time":1000}}"#;
        assert_eq!(reason(body), Err(Reason::InvalidRequest));
    }

    /// `query.timezone`：IANA 时区名（含别名、ASCII 字母不区分大小写）或 `UTC`；写错与固定偏移是 `invalid_request`，
    /// `message` 指明该字段，且先于 `invalid_time_range`；顶层的 `timezone` 照旧忽略。
    #[test]
    fn timezone_field() {
        let with_q = |extra: &str| format!(r#"{{"query":{{{Q},{extra}}}}}"#);
        for tz in [r#""Mars/Olympus""#, "8", r#""+08:00""#, "null", r#""""#] {
            let body = with_q(&format!(r#""timezone":{tz}"#));
            let e = parse(body.as_bytes(), &Limits::default()).unwrap_err();
            assert_eq!(e.reason, Reason::InvalidRequest, "{body}");
            assert!(e.message.contains("query.timezone"), "{}", e.message);
        }
        let ok = reason(&with_q(r#""timezone":"asia/shanghai""#)).unwrap();
        assert_eq!(ok.timezone.iana_name(), Some("Asia/Shanghai"));
        assert_eq!(
            reason(&with_q(r#""timezone":"UTC""#)).unwrap().timezone,
            TimeZone::UTC
        );
        assert_eq!(
            reason(&with_q(r#""size":1"#)).unwrap().timezone,
            TimeZone::UTC
        );
        let top = format!(r#"{{"query":{{{Q}}},"timezone":"Mars/Olympus"}}"#);
        assert!(reason(&top).is_ok());
        let both = r#"{"query":{"sql":"SELECT * FROM app","start_time":3000,"end_time":1000,"timezone":"x"}}"#;
        assert_eq!(reason(both), Err(Reason::InvalidRequest));
    }

    #[test]
    fn size_bound_follows_configured_max() {
        let limits = Limits {
            default_size: 5,
            max_size: 5,
        };
        let body = |size: &str| format!(r#"{{"query":{{{Q}{size}}}}}"#);
        assert_eq!(parse(body("").as_bytes(), &limits).unwrap().size, 5);
        assert!(parse(body(r#","size":6"#).as_bytes(), &limits).is_err());
    }
}
