//! 时间分桶函数 `histogram()`（REQ-AMB-QRY-HISTOGRAM-V2）：间隔的写法与换算、查询时区的查找、桶起点的计算。
//!
//! 调用的形态与位置在 [`crate::sql`] 核对，间隔写法与范围在 [`crate::bind`] 核对（报告顺序在 `unknown_column` 之后的
//! `invalid_sql` 一位）。查询时区（REQ-AMB-QRY-SEARCH-ENDPOINT-V9 的 `query.timezone`）在解析请求体时查好，随绑定交给
//! 桶起点的表达式。桶起点按查询时区的本地挂钟对齐（[`bucket_start_in`]）；时区为 `UTC` 时即 ⌊t / I⌋ × I。

use std::hash::{Hash, Hasher};
use std::sync::{Arc, LazyLock};

use arrow::array::{ArrayRef, AsArray, Int64Array};
use arrow::datatypes::{DataType, Int64Type};
use datafusion::common::{Result, ScalarValue, exec_err};
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use datafusion::prelude::{ident, lit};
use jiff::civil::DateTime;
use jiff::tz::{AmbiguousOffset, Offset, TimeZone, TimeZoneDatabase};
use jiff::{SignedDuration, Timestamp};

/// 间隔 `<整数><ms|s|m|h|d>` 换算所得的微秒数 I；写法不合、为 0 或超出 2^63−1 为 `None`。
///
/// 写法与配置项的时长写法相同（REQ-AMB-CFG-STARTUP）：`<整数>` 为一个或多个十进制数字（可有前导零），单位区分大小写。
pub fn interval_micros(text: &str) -> Option<i64> {
    let split = text.find(|c: char| !c.is_ascii_digit())?;
    let (digits, unit) = text.split_at(split);
    if digits.is_empty() {
        return None;
    }
    let per_unit: i64 = match unit {
        "ms" => 1_000,
        "s" => 1_000_000,
        "m" => 60_000_000,
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        _ => return None,
    };
    let n: i64 = digits.parse().ok()?;
    n.checked_mul(per_unit).filter(|&i| i > 0)
}

/// 按名字在编进可执行文件的 IANA 时区数据库中找时区（含别名，ASCII 字母不区分大小写；`UTC` 也在其中）；
/// 找不到为 `None`。不读运行环境的时区文件与 `TZ`（REQ-AMB-QRY-HISTOGRAM-V2）。
pub fn time_zone(name: &str) -> Option<TimeZone> {
    static DB: LazyLock<TimeZoneDatabase> = LazyLock::new(TimeZoneDatabase::bundled);
    // jiff 另把 `Etc/Unknown` 认作「未知时区」，它不是时区数据库里的名字
    if name.eq_ignore_ascii_case("Etc/Unknown") {
        return None;
    }
    DB.get(name).ok()
}

/// 桶起点：不大于 `t` 的最大的 `interval` 的整数倍（向负无穷取整）。
pub fn bucket_start(t: i64, interval: i64) -> Option<i64> {
    t.checked_sub(t.rem_euclid(interval))
}

/// 查询时区 `zone` 下 `t` 的桶起点（REQ-AMB-QRY-HISTOGRAM-V2「取值」）：本地挂钟值 L = t + o(t) 取整得本地桶起点 B，
/// 再换回 UTC——B 只对应一个时刻时取它；对应两个时刻（回拨）时取不晚于 `t` 的较晚者；没有对应时刻（拨快）时取
/// 跳变时刻。`zone` 为 UTC 时即 [`bucket_start`]。越出 i64 为 `None`。
pub fn bucket_start_in(t: i64, interval: i64, zone: &TimeZone) -> Option<i64> {
    if *zone == TimeZone::UTC {
        return bucket_start(t, interval);
    }
    let offset = micros(zone.to_offset(clamped(t)));
    let local = bucket_start(t.checked_add(offset)?, interval)?;
    // 本地桶起点超出 jiff 的日期范围（9999 年末前后）时，按 t 的偏移换回；那几十个小时里没有时区跳变
    let Ok(civil) =
        DateTime::constant(1970, 1, 1, 0, 0, 0, 0).checked_add(SignedDuration::from_micros(local))
    else {
        return local.checked_sub(offset);
    };
    Some(match zone.to_ambiguous_timestamp(civil).offset() {
        AmbiguousOffset::Unambiguous { offset } => local - micros(offset),
        AmbiguousOffset::Fold { before, after } => {
            // 两个候选时刻：按跳变前的偏移得较早者，按跳变后的偏移得较晚者
            let later = local - micros(after);
            if later <= t {
                later
            } else {
                local - micros(before)
            }
        }
        AmbiguousOffset::Gap { before, after } => {
            // 跳变时刻 T 满足 T + before ≤ B < T + after，即 (B − after, B − before] 中的那次跳变
            let lower = local - micros(after);
            zone.following(clamped(lower))
                .next()
                .map_or(local - micros(before), |tr| tr.timestamp().as_microsecond())
        }
    })
}

fn micros(offset: Offset) -> i64 {
    i64::from(offset.seconds()) * 1_000_000
}

/// 微秒 `t` 收进 jiff 能表示的时刻范围（约 ±9999 年）。
fn clamped(t: i64) -> Timestamp {
    let t = t.clamp(
        Timestamp::MIN.as_second() * 1_000_000,
        Timestamp::MAX.as_second() * 1_000_000,
    );
    Timestamp::from_microsecond(t).expect("已收进范围")
}

/// `_timestamp` 按间隔 `interval`（微秒）与查询时区 `zone` 取桶起点的表达式。
pub fn expr(interval: i64, zone: &TimeZone) -> Expr {
    let args = vec![
        ident(amberlog_common::schema::TIMESTAMP_COLUMN),
        lit(interval),
    ];
    if *zone == TimeZone::UTC {
        UDF.call(args)
    } else {
        ScalarUDF::new_from_impl(Histogram::new(zone.clone())).call(args)
    }
}

static UDF: LazyLock<ScalarUDF> =
    LazyLock::new(|| ScalarUDF::new_from_impl(Histogram::new(TimeZone::UTC)));

/// 桶起点的标量函数：参数为 Int64 的 `_timestamp` 与 Int64 的间隔（常量），结果为 Int64；查询时区随函数本身。
#[derive(Debug, PartialEq, Eq)]
struct Histogram {
    signature: Signature,
    zone: TimeZone,
}

impl Hash for Histogram {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.signature.hash(state);
        self.zone.iana_name().hash(state);
    }
}

impl Histogram {
    fn new(zone: TimeZone) -> Self {
        Histogram {
            signature: Signature::exact(
                vec![DataType::Int64, DataType::Int64],
                Volatility::Immutable,
            ),
            zone,
        }
    }
}

impl ScalarUDFImpl for Histogram {
    fn name(&self) -> &str {
        "amberlog_histogram"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [t, interval] = args.args.as_slice() else {
            return exec_err!("histogram takes two arguments");
        };
        let ColumnarValue::Scalar(ScalarValue::Int64(Some(interval))) = interval else {
            return exec_err!("histogram interval must be a constant");
        };
        let interval = *interval;
        if interval <= 0 {
            return exec_err!("histogram interval must be positive");
        }
        let bucket = |t: i64| match bucket_start_in(t, interval, &self.zone) {
            Some(b) => Ok(b),
            None => exec_err!("histogram bucket of {t} is out of range"),
        };
        match t {
            ColumnarValue::Scalar(ScalarValue::Int64(v)) => Ok(ColumnarValue::Scalar(
                ScalarValue::Int64(v.map(bucket).transpose()?),
            )),
            ColumnarValue::Array(a) => {
                let a = a.as_primitive::<Int64Type>();
                let out: Int64Array = a
                    .iter()
                    .map(|v| v.map(bucket).transpose())
                    .collect::<Result<_>>()?;
                Ok(ColumnarValue::Array(Arc::new(out) as ArrayRef))
            }
            other => exec_err!("histogram expects Int64, got {}", other.data_type()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 场景「间隔的写法与换算」。
    #[test]
    fn histogram_interval_parse_table() {
        let table: [(&str, Option<i64>); 19] = [
            ("1ms", Some(1_000)),
            ("30s", Some(30_000_000)),
            ("5m", Some(300_000_000)),
            ("1h", Some(3_600_000_000)),
            ("1d", Some(86_400_000_000)),
            ("05m", Some(300_000_000)),
            ("9223372036854775ms", Some(9_223_372_036_854_775_000)),
            ("9223372036854776ms", None),
            ("0ms", None),
            ("5", None),
            ("m", None),
            ("5M", None),
            ("5us", None),
            ("+5m", None),
            ("-5m", None),
            ("1.5h", None),
            ("1_000s", None),
            ("5 m", None),
            ("", None),
        ];
        for (text, want) in table {
            assert_eq!(interval_micros(text), want, "{text:?}");
        }
    }

    const MIN: i64 = 60_000_000;
    const HOUR: i64 = 3_600_000_000;
    const DAY: i64 = 86_400_000_000;

    fn zone(name: &str) -> TimeZone {
        time_zone(name).expect(name)
    }

    #[test]
    fn time_zone_names_ignore_ascii_case_and_reject_offsets() {
        assert_eq!(
            time_zone("asia/SHANGHAI").and_then(|z| z.iana_name().map(str::to_owned)),
            Some("Asia/Shanghai".to_owned())
        );
        assert!(time_zone("UTC").is_some_and(|z| z == TimeZone::UTC));
        assert!(time_zone("utc").is_some_and(|z| z == TimeZone::UTC));
        // 别名
        assert!(time_zone("Asia/Calcutta").is_some());
        assert!(time_zone("US/Eastern").is_some());
        assert!(time_zone("Etc/UTC").is_some());
        for bad in [
            "Mars/Olympus",
            "+08:00",
            "UTC+8",
            "",
            "Etc/Unknown",
            "etc/unknown",
            " Asia/Shanghai",
        ] {
            assert!(time_zone(bad).is_none(), "{bad:?}");
        }
    }

    /// HISTOGRAM-V2 各场景的数值（检索屏的分桶计算用同一组数值核对）：(时区, 间隔, t, 桶起点)。
    const BLOCK_SCENARIOS: [(&str, i64, i64, i64); 12] = [
        // 按 5 分钟分桶计数
        ("UTC", 5 * MIN, 0, 0),
        ("UTC", 5 * MIN, 299_999_999, 0),
        ("UTC", 5 * MIN, 300_000_000, 300_000_000),
        ("UTC", 5 * MIN, 900_000_001, 900_000_000),
        // 按天分桶对齐查询时区的本地零点
        (
            "Asia/Shanghai",
            DAY,
            1_792_166_399_999_999,
            1_792_080_000_000_000,
        ),
        (
            "Asia/Shanghai",
            DAY,
            1_792_166_400_000_000,
            1_792_166_400_000_000,
        ),
        // 夏令时回拨那天的桶长 25 小时
        (
            "America/New_York",
            DAY,
            1_793_505_600_000_000,
            1_793_505_600_000_000,
        ),
        (
            "America/New_York",
            DAY,
            1_793_595_599_999_999,
            1_793_505_600_000_000,
        ),
        (
            "America/New_York",
            DAY,
            1_793_595_600_000_000,
            1_793_595_600_000_000,
        ),
        // 重复的一小时分成两个桶
        (
            "America/New_York",
            HOUR,
            1_793_511_000_000_000,
            1_793_509_200_000_000,
        ),
        (
            "America/New_York",
            HOUR,
            1_793_514_600_000_000,
            1_793_512_800_000_000,
        ),
        // 被跳过的本地桶起点取跳变之后的第一个时刻
        (
            "America/New_York",
            2 * HOUR,
            1_772_955_000_000_000,
            1_772_953_200_000_000,
        ),
    ];

    #[test]
    fn bucket_start_in_matches_block_scenarios() {
        for (name, interval, t, want) in BLOCK_SCENARIOS {
            assert_eq!(
                bucket_start_in(t, interval, &zone(name)),
                Some(want),
                "{name} {interval} {t}"
            );
        }
        // 回拨那一小时按分钟分桶时桶起点不早于记录一小时
        assert_eq!(
            bucket_start_in(1_793_514_600_000_000, MIN, &zone("America/New_York")),
            Some(1_793_514_600_000_000)
        );
    }

    #[test]
    fn bucket_start_in_fixed_offset_zones() {
        // Asia/Kolkata 为 UTC+5:30：2026-10-20T00:00Z 是本地 05:30
        let kolkata = zone("Asia/Kolkata");
        let t = 1_792_454_400_000_000;
        assert_eq!(
            bucket_start_in(t, HOUR, &kolkata),
            Some(1_792_452_600_000_000)
        );
        assert_eq!(
            bucket_start_in(t, DAY, &kolkata),
            Some(1_792_434_600_000_000)
        );
        assert_eq!(bucket_start_in(t, 30 * MIN, &kolkata), Some(t));
        // Asia/Shanghai 为 UTC+8：按天的桶从 UTC 16:00 开始
        let shanghai = zone("Asia/Shanghai");
        assert_eq!(
            bucket_start_in(1_792_108_800_000_000, DAY, &shanghai),
            Some(1_792_080_000_000_000)
        );
        assert_eq!(
            bucket_start_in(1_792_108_800_000_000, HOUR, &shanghai),
            Some(1_792_108_800_000_000)
        );
    }

    #[test]
    fn bucket_start_in_new_york_2026_transitions() {
        let ny = zone("America/New_York");
        // 2026-03-08 拨快（本地 02:00～03:00 被跳过）：那天的桶长 23 小时
        assert_eq!(
            bucket_start_in(1_772_971_200_000_000, DAY, &ny),
            Some(1_772_946_000_000_000)
        );
        // 本地桶起点 02:30 落在被跳过的一段里，取跳变时刻 07:00Z（不是 02:30 按跳变前偏移的 07:30Z）
        assert_eq!(
            bucket_start_in(1_772_953_800_000_000, 150 * MIN, &ny),
            Some(1_772_953_200_000_000)
        );
        // 2026-11-01 回拨：本地 01:30 出现两次，按 30 分钟分桶各自成桶
        assert_eq!(
            bucket_start_in(1_793_511_900_000_000, 30 * MIN, &ny),
            Some(1_793_511_000_000_000)
        );
        assert_eq!(
            bucket_start_in(1_793_515_500_000_000, 30 * MIN, &ny),
            Some(1_793_514_600_000_000)
        );
    }

    /// 按块的定义逐分钟穷举：u + o(u) = B 的 u 中取不晚于 t 的最晚者，没有时取 u + o(u) > B 的最早者。
    fn by_definition(t: i64, interval: i64, zone: &TimeZone) -> i64 {
        let local = |u: i64| u + micros(zone.to_offset(clamped(u)));
        let b = bucket_start(local(t), interval).unwrap();
        let mut exact = None;
        let mut after = None;
        // America/New_York 的偏移相差不过 1 小时，所求时刻在 [t − I − 2h, t + 2h] 之内
        let mut u = t - t.rem_euclid(MIN) - interval - 2 * HOUR;
        while u <= t + 2 * HOUR {
            if local(u) == b && u <= t {
                exact = Some(u);
            }
            if local(u) > b && after.is_none() {
                after = Some(u);
            }
            u += MIN;
        }
        exact.or(after).unwrap()
    }

    #[test]
    fn bucket_start_in_agrees_with_definition_around_transitions() {
        let ny = zone("America/New_York");
        // 2026 年两次跳变（07:00Z、06:00Z）前后各一天，每 20 分钟取一个 t
        for transition in [1_772_953_200_000_000_i64, 1_793_512_800_000_000] {
            let mut t = transition - DAY;
            while t <= transition + DAY {
                for interval in [MIN, 30 * MIN, HOUR, 150 * MIN, DAY] {
                    let got = bucket_start_in(t, interval, &ny).unwrap();
                    assert_eq!(got, by_definition(t, interval, &ny), "{t} {interval}");
                    assert!(got <= t, "{t} {interval}");
                }
                t += 20 * MIN;
            }
        }
    }

    #[test]
    fn bucket_start_in_utc_is_epoch_aligned() {
        for (t, i) in [(0, 300), (299, 300), (1_793_514_600_000_123, DAY)] {
            assert_eq!(bucket_start_in(t, i, &TimeZone::UTC), bucket_start(t, i));
        }
        // 9999 年末超出 jiff 的时刻范围时仍按该时区的偏移给出不晚于 t 的桶起点
        let t = 253_402_300_799_999_999;
        let got = bucket_start_in(t, MIN, &zone("Asia/Shanghai")).unwrap();
        assert_eq!(got, t - t.rem_euclid(MIN));
    }

    #[test]
    fn bucket_start_floors_toward_negative_infinity() {
        assert_eq!(bucket_start(0, 300), Some(0));
        assert_eq!(bucket_start(299, 300), Some(0));
        assert_eq!(bucket_start(300, 300), Some(300));
        assert_eq!(bucket_start(-1, 300), Some(-300));
        assert_eq!(bucket_start(i64::MAX, 2), Some(i64::MAX - 1));
        assert_eq!(bucket_start(i64::MIN, 3), None);
    }
}
