//! 运行日志（REQ-AMB-CFG-STARTUP-V2）：输出到 stderr；`json` 格式下每行一个含 `ts`、`level`、`msg` 的 JSON 对象。
//!
//! 稳定事件（如 `starting`、`ready`、`wal_backlog_high`，全部事件及其字段列在用户文档的事件表里）经 [`event`] 输出，不受 `log.level` 过滤，
//! 字段名只增不删不改名。其余日志经 [`log`] 输出，文本不稳定。

use std::io::Write;
use std::sync::OnceLock;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Level {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Level {
    fn as_str(self) -> &'static str {
        match self {
            Level::Error => "error",
            Level::Warn => "warn",
            Level::Info => "info",
            Level::Debug => "debug",
            Level::Trace => "trace",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Format {
    Json,
    Text,
}

static SINK: OnceLock<(Format, Level)> = OnceLock::new();

/// 按已校验的配置设定日志格式与级别。配置装载成功之前不输出运行日志。
pub fn init(format: Format, level: Level) {
    let _ = SINK.set((format, level));
}

/// 普通运行日志，受 `log.level` 过滤。
pub fn log(level: Level, msg: &str, fields: &[(&str, Value)]) {
    if let Some(&(format, max)) = SINK.get()
        && level <= max
    {
        write(format, level, msg, None, fields);
    }
}

/// 稳定事件，不受 `log.level` 过滤。
pub fn event(level: Level, event: &str, msg: &str, fields: &[(&str, Value)]) {
    if let Some(&(format, _)) = SINK.get() {
        write(format, level, msg, Some(event), fields);
    }
}

fn write(format: Format, level: Level, msg: &str, event: Option<&str>, fields: &[(&str, Value)]) {
    let ts = rfc3339_utc_now();
    let mut line = match format {
        Format::Json => {
            let mut obj = Map::new();
            obj.insert("ts".into(), ts.into());
            obj.insert("level".into(), level.as_str().into());
            obj.insert("msg".into(), msg.into());
            if let Some(e) = event {
                obj.insert("event".into(), e.into());
            }
            for (k, v) in fields {
                obj.insert((*k).into(), v.clone());
            }
            Value::Object(obj).to_string()
        }
        Format::Text => {
            let mut s = format!("{ts} {} {msg}", level.as_str().to_ascii_uppercase());
            if let Some(e) = event {
                s.push_str(&format!(" event={e}"));
            }
            for (k, v) in fields {
                match v {
                    Value::String(t) => s.push_str(&format!(" {k}={t}")),
                    other => s.push_str(&format!(" {k}={other}")),
                }
            }
            s
        }
    };
    line.push('\n');
    // 整行一次写出，多线程下不交错
    let _ = std::io::stderr().lock().write_all(line.as_bytes());
}

/// 文本中的 URL 去除用户信息、查询串与片段：`scheme://user:pass@host/p?q#f` 写作 `scheme://host/p`（5xx 的 `error`
/// 日志写底层错误原文前用，REQ-AMB-ERR-ENVELOPE-V7）。URL 从 `://` 起，到空白、引号、尖括号或右括号为止。
pub(crate) fn redact_urls(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(i) = rest.find("://") {
        out.push_str(&rest[..i + 3]);
        let after = &rest[i + 3..];
        let end = after
            .find(|c: char| c.is_whitespace() || "\"'<>)]}`".contains(c))
            .unwrap_or(after.len());
        let (url, tail) = after.split_at(end);
        let (authority, path) = url.split_at(url.find(['/', '?', '#']).unwrap_or(url.len()));
        out.push_str(
            authority
                .rsplit_once('@')
                .map_or(authority, |(_, host)| host),
        );
        out.push_str(path.split(['?', '#']).next().unwrap_or(""));
        rest = tail;
    }
    out.push_str(rest);
    out
}

/// 当前时刻的 RFC 3339 UTC 表示，精确到微秒，如 `2026-09-28T02:00:00.123456Z`。
fn rfc3339_utc_now() -> String {
    let micros = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0);
    rfc3339_utc(micros)
}

/// UNIX 微秒数 `micros` 的 RFC 3339 UTC 表示，恒为 6 位小数（运行日志的 `ts`、webhook 请求体的三个时刻）。
pub(crate) fn rfc3339_utc(micros: i64) -> String {
    let secs = micros.div_euclid(1_000_000);
    let frac = micros.rem_euclid(1_000_000);
    let (days, sod) = (secs.div_euclid(86_400), secs.rem_euclid(86_400));
    let (y, m, d) = civil_from_days(days);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{frac:06}Z",
        sod / 3600,
        sod % 3600 / 60,
        sod % 60
    )
}

/// 自 1970-01-01 起的天数换算为公历年月日（H. Hinnant 的 civil_from_days）。
fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn urls_lose_userinfo_and_query() {
        for (raw, want) in [
            (
                "GET http://u:p@10.0.0.1:9000/bkt/a/b.parquet?X-Sig=1#f failed",
                "GET http://10.0.0.1:9000/bkt/a/b.parquet failed",
            ),
            (
                "error (\"https://host/p?q=1\") and s3://k@bkt/x",
                "error (\"https://host/p\") and s3://bkt/x",
            ),
            ("http://host?only=query", "http://host"),
            ("no url here: a@b?c", "no url here: a@b?c"),
            ("http://a/x@y/z", "http://a/x@y/z"),
            ("", ""),
        ] {
            assert_eq!(redact_urls(raw), want, "{raw}");
        }
    }

    #[test]
    fn rfc3339_formatting() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00.000000Z");
        assert_eq!(
            rfc3339_utc(1_787_364_000_123_456),
            "2026-08-22T02:00:00.123456Z"
        );
        assert_eq!(
            rfc3339_utc(951_782_400_000_000),
            "2000-02-29T00:00:00.000000Z"
        );
        assert_eq!(
            rfc3339_utc(1_483_228_799_999_999),
            "2016-12-31T23:59:59.999999Z"
        );
    }
}
