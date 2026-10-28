//! `_timestamp` 的归一：取值形态判定、单位换算、缺省填充与时间窗（REQ-AMB-TIME-TIMESTAMP-V2）。
//!
//! 输入是记录顶层 `_timestamp` 的 JSON 取值（或缺失）、本请求的服务端接收时刻（UTC 微秒，
//! 由调用方每个请求只取一次）与时间窗配置；输出是归一后的 UTC 微秒，或逐条记录错误的 `reason`。
//!
//! 实现要点：
//! - **精确十进制**：全仓 serde_json 开启 `arbitrary_precision`（见根 `Cargo.toml`），
//!   `Number::as_str` 给出数字的十进制原文。幅值判档、单位换算与截断都在原文的「有效数字串 ×
//!   10 的幂」表示上做整数运算，全程不经 f64（块条款「不得先转换为二进制浮点数」）。
//!   `_timestamp` 取 `1e400` 这类超出 binary64 的数字时不是取值缺陷（REQ-AMB-ING-RECORD-ISOLATION 缺陷③不含 `_timestamp`），
//!   按幅值判为超出时间窗。
//!   serde_json 记下的原文会规范化指数写法（`1E2` 记作 `1e+2`）、把 `-0` 记作 `0`，都不改变精确值，
//!   对这里的换算无影响；记录模型要逐字符照抄字面量，另用 `amberlog_common::json::Json`
//!   （记录处理经 `Json::to_value` 把 `_timestamp` 的取值交给这里）。
//! - **RFC 3339**：自行按 RFC 3339 §5.6 的文法逐字节校验（`T`/`Z` 大小写不敏感、必须带 `Z`
//!   或 `±HH:MM` 偏移、秒可为 `60`），不借日期库的宽松解析；日历换算用公历日序公式自算，
//!   不依赖进程 `TZ` 与本地时区，也无需新依赖。
//! - **截断**：「不足一微秒的部分向零截断」一律在最终的 UTC 微秒值上向零取整，数字与字符串同一规则。

use std::time::Duration;

use amberlog_common::error::RecordReason;
use serde_json::Value;

/// 归一后 `_timestamp` 的上界：9999-12-31T23:59:59.999999Z（REQ-AMB-TIME-TIMESTAMP-V2）。
/// 与 `ingest.max_future` 各自独立判定；小时桶标识 `YYYYMMDDHH` 只容四位年份。
pub const MAX_TIMESTAMP_US: i64 = 253_402_300_799_999_999;

/// `ingest.max_future` 的默认值：24 小时。
pub const DEFAULT_MAX_FUTURE: Duration = Duration::from_secs(24 * 60 * 60);

/// 时间窗配置：`ingest.max_future`（默认 24 小时）与 `ingest.max_past`（默认不设置，即不限制过去）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimestampWindow {
    pub max_future: Duration,
    pub max_past: Option<Duration>,
}

impl Default for TimestampWindow {
    fn default() -> Self {
        TimestampWindow {
            max_future: DEFAULT_MAX_FUTURE,
            max_past: None,
        }
    }
}

/// 归一一条记录的 `_timestamp`。
///
/// - `value`：记录顶层 `_timestamp` 的取值，缺失传 `None`。
/// - `received_at_us`：本请求的服务端接收时刻（UTC 微秒）；缺省填充与时间窗判定都用它。
///
/// 缺失、`null` 与精确值为零的数字返回 `received_at_us`；无法解析的字符串、布尔、对象、数组报
/// `timestamp_invalid`；归一后为负、晚于接收时刻加 `max_future`、晚于 [`MAX_TIMESTAMP_US`]、或（配置了时）
/// 早于接收时刻减 `max_past` 报 `timestamp_out_of_range`。
pub fn normalize_timestamp(
    value: Option<&Value>,
    received_at_us: i64,
    window: &TimestampWindow,
) -> Result<i64, RecordReason> {
    let micros = match value {
        None | Some(Value::Null) => return Ok(received_at_us),
        Some(Value::Number(n)) => match number_to_micros(n.as_str()) {
            Some(NumberMicros::Zero) => return Ok(received_at_us),
            Some(NumberMicros::Micros(us)) => us,
            Some(NumberMicros::OutOfRange) => return Err(RecordReason::TimestampOutOfRange),
            // serde_json 给出的原文总符合 JSON 数字文法，此分支不可达；保守按无法解析处理。
            None => return Err(RecordReason::TimestampInvalid),
        },
        Some(Value::String(s)) => rfc3339_to_micros(s).ok_or(RecordReason::TimestampInvalid)?,
        Some(Value::Bool(_) | Value::Array(_) | Value::Object(_)) => {
            return Err(RecordReason::TimestampInvalid);
        }
    };
    check_window(micros, received_at_us, window)
}

fn check_window(
    micros: i64,
    received_at_us: i64,
    window: &TimestampWindow,
) -> Result<i64, RecordReason> {
    if micros < 0 {
        return Err(RecordReason::TimestampOutOfRange);
    }
    let future_limit = received_at_us.saturating_add(duration_micros(window.max_future));
    if micros > future_limit || micros > MAX_TIMESTAMP_US {
        return Err(RecordReason::TimestampOutOfRange);
    }
    if let Some(max_past) = window.max_past {
        let past_limit = received_at_us.saturating_sub(duration_micros(max_past));
        if micros < past_limit {
            return Err(RecordReason::TimestampOutOfRange);
        }
    }
    Ok(micros)
}

fn duration_micros(d: Duration) -> i64 {
    i64::try_from(d.as_micros()).unwrap_or(i64::MAX)
}

// ---------------------------------------------------------------------------
// 数字：按字面量的精确十进制值判档与换算
// ---------------------------------------------------------------------------

#[derive(Debug, PartialEq, Eq)]
enum NumberMicros {
    /// 精确值为零（`0`、`-0`、`0.0`、`0e5`、`0.000` 等）：以接收时刻填充。
    Zero,
    /// 归一并向零截断后的 UTC 微秒。
    Micros(i64),
    /// 归一后为负，或超出 Int64 可表示范围——前者按块直接越界；后者远晚于任何可存储的时刻，
    /// 同样只能以 `timestamp_out_of_range` 拒绝。
    OutOfRange,
}

/// 幅值分界：绝对值 ≥ 1e17 纳秒、[1e14, 1e17) 微秒、[1e11, 1e14) 毫秒、< 1e11 秒。
/// 返回把原值换成微秒要乘的 10 的幂。
fn unit_shift(magnitude: i64) -> i64 {
    if magnitude >= 17 {
        -3 // 纳秒：÷1000
    } else if magnitude >= 14 {
        0 // 微秒
    } else if magnitude >= 11 {
        3 // 毫秒：×1000
    } else {
        6 // 秒：×1000000
    }
}

/// 把 JSON 数字原文（`-?int(.frac)?([eE][+-]?exp)?`）换算为微秒；原文不合文法时返回 `None`。
fn number_to_micros(literal: &str) -> Option<NumberMicros> {
    let (negative, body) = match literal.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, literal),
    };
    let (mantissa, exp_text) = match body.find(['e', 'E']) {
        Some(i) => (&body[..i], Some(&body[i + 1..])),
        None => (body, None),
    };
    let (int_part, frac_part) = match mantissa.split_once('.') {
        Some((i, f)) => (i, f),
        None => (mantissa, ""),
    };
    if int_part.is_empty() || !all_digits(int_part) || !all_digits(frac_part) {
        return None;
    }
    if mantissa.contains('.') && frac_part.is_empty() {
        return None;
    }
    let exponent = match exp_text {
        Some(t) => parse_exponent(t)?,
        None => 0,
    };

    // 值 = 有效数字串 × 10^scale，有效数字串去掉了前导零、首位非零。
    let all: String = [int_part, frac_part].concat();
    let significant = all.trim_start_matches('0');
    if significant.is_empty() {
        return Some(NumberMicros::Zero);
    }
    let sig_len = i64::try_from(significant.len()).unwrap_or(i64::MAX);
    let frac_len = i64::try_from(frac_part.len()).unwrap_or(i64::MAX);
    let scale = exponent.saturating_sub(frac_len);

    // 绝对值的十进制数量级 floor(log10|v|)：|v| ≥ 10^k 当且仅当 magnitude ≥ k。
    let magnitude = sig_len.saturating_sub(1).saturating_add(scale);
    let us_scale = scale.saturating_add(unit_shift(magnitude));

    // 微秒值的整数部分位数；≤ 0 表示不足一微秒，向零截断得 0。
    let int_len = sig_len.saturating_add(us_scale);
    if int_len <= 0 {
        return Some(NumberMicros::Micros(0));
    }
    if negative {
        return Some(NumberMicros::OutOfRange);
    }
    // i64::MAX 有 19 位；整数部分超过 19 位必然溢出。
    if int_len > 19 {
        return Some(NumberMicros::OutOfRange);
    }
    // 取整数部分对应的有效数字（多出的即不足一微秒的部分，直接舍去 = 向零截断），不够的补零。
    let taken = int_len.min(sig_len) as usize;
    let head: i128 = significant[..taken].parse().ok()?;
    let value = head * 10i128.pow((int_len as usize - taken) as u32);
    Some(match i64::try_from(value) {
        Ok(us) => NumberMicros::Micros(us),
        Err(_) => NumberMicros::OutOfRange,
    })
}

/// 解析指数（`[+-]?digits`），绝对值饱和在 1e15——再大也只决定「截断为 0」或「溢出」，结果不变。
fn parse_exponent(text: &str) -> Option<i64> {
    let (negative, digits) = match text.as_bytes().first() {
        Some(b'+') => (false, &text[1..]),
        Some(b'-') => (true, &text[1..]),
        _ => (false, text),
    };
    if digits.is_empty() || !all_digits(digits) {
        return None;
    }
    const CAP: i64 = 1_000_000_000_000_000;
    let mut value: i64 = 0;
    for b in digits.bytes() {
        value = (value * 10 + i64::from(b - b'0')).min(CAP);
    }
    Some(if negative { -value } else { value })
}

fn all_digits(s: &str) -> bool {
    s.bytes().all(|b| b.is_ascii_digit())
}

// ---------------------------------------------------------------------------
// RFC 3339 字符串
// ---------------------------------------------------------------------------

/// 按 RFC 3339 §5.6 的 `date-time` 文法解析并换算为 UTC 微秒；不合文法或日期不存在时返回 `None`。
///
/// ```text
/// date-time = full-date "T" full-time              ; T 大小写不敏感
/// full-date = 4DIGIT "-" 2DIGIT "-" 2DIGIT
/// full-time = 2DIGIT ":" 2DIGIT ":" 2DIGIT ["." 1*DIGIT] time-offset
/// time-offset = "Z" / ("+" / "-") 2DIGIT ":" 2DIGIT   ; Z 大小写不敏感
/// ```
///
/// 秒为 `60` 时按该分钟之后一分钟的第 0 秒（保留小数秒）换算，不校验该时刻是否确有闰秒——
/// 直接把 60 秒计入累加即得此结果，跨时、跨日、跨年同样成立。
fn rfc3339_to_micros(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 {
        return None;
    }
    let year = fixed_digits(b, 0, 4)?;
    expect(b, 4, b'-')?;
    let month = fixed_digits(b, 5, 2)?;
    expect(b, 7, b'-')?;
    let day = fixed_digits(b, 8, 2)?;
    if !matches!(b[10], b'T' | b't') {
        return None;
    }
    let hour = fixed_digits(b, 11, 2)?;
    expect(b, 13, b':')?;
    let minute = fixed_digits(b, 14, 2)?;
    expect(b, 16, b':')?;
    let second = fixed_digits(b, 17, 2)?;

    if !(1..=12).contains(&month) || day < 1 || day > days_in_month(year, month) {
        return None;
    }
    if hour > 23 || minute > 59 || second > 60 {
        return None;
    }

    // 小数秒：取前 6 位为微秒，其后只记是否有非零位（决定向零截断的方向修正）。
    let mut pos = 19;
    let mut frac_us: i64 = 0;
    let mut dropped_nonzero = false;
    if b[pos] == b'.' {
        pos += 1;
        let start = pos;
        while pos < b.len() && b[pos].is_ascii_digit() {
            let digit = i64::from(b[pos] - b'0');
            let place = pos - start;
            if place < 6 {
                frac_us = frac_us * 10 + digit;
            } else if digit != 0 {
                dropped_nonzero = true;
            }
            pos += 1;
        }
        let count = pos - start;
        if count == 0 {
            return None;
        }
        for _ in count..6 {
            frac_us *= 10;
        }
    }

    // 时区偏移：必须恰好是 `Z` 或 `±HH:MM`，其后不得再有任何字符。
    let offset_secs = match b.get(pos)? {
        b'Z' | b'z' if pos + 1 == b.len() => 0,
        sign @ (b'+' | b'-') if pos + 6 == b.len() => {
            let oh = fixed_digits(b, pos + 1, 2)?;
            expect(b, pos + 3, b':')?;
            let om = fixed_digits(b, pos + 4, 2)?;
            if oh > 23 || om > 59 {
                return None;
            }
            let secs = oh * 3600 + om * 60;
            if *sign == b'-' { -secs } else { secs }
        }
        _ => return None,
    };

    let local_secs =
        days_from_civil(year, month, day) * 86_400 + hour * 3600 + minute * 60 + second;
    let utc_secs = local_secs - offset_secs;
    let mut micros = utc_secs * 1_000_000 + frac_us;
    // 上面相当于向负无穷取整；精确值为负且有舍去的非零位时，向零截断要再加 1。
    if dropped_nonzero && micros < 0 {
        micros += 1;
    }
    Some(micros)
}

/// 读 `b[at..at+len]` 的定长十进制数字；越界或含非数字字节返回 `None`。
fn fixed_digits(b: &[u8], at: usize, len: usize) -> Option<i64> {
    let slice = b.get(at..at + len)?;
    let mut v: i64 = 0;
    for &c in slice {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v * 10 + i64::from(c - b'0');
    }
    Some(v)
}

fn expect(b: &[u8], at: usize, want: u8) -> Option<()> {
    (b.get(at) == Some(&want)).then_some(())
}

fn is_leap_year(year: i64) -> bool {
    year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)
}

fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if is_leap_year(year) => 29,
        2 => 28,
        _ => 0,
    }
}

/// 公历（前推格里高利历）日期到 1970-01-01 起的天数（Howard Hinnant 的 days_from_civil）。
fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (month + 9) % 12; // 3 月为 0 … 2 月为 11
    let doy = (153 * mp + 2) / 5 + day - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[cfg(test)]
mod tests;
