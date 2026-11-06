import { DAY_US, MINUTE_US, formatLocal, formatOffset } from "../../lib/time.ts";

/** 图表使用秒级时间；查询边界仍保留原始微秒精度。 */
export function histogramTime(us: bigint, timezone: string): string {
  return formatLocal(us, timezone).slice(0, 19);
}

/** 短范围显示时分秒，跨日补日期，跨年补年份。 */
export function histogramTick(us: bigint, range: readonly [bigint, bigint], timezone: string): string {
  const text = histogramTime(us, timezone);
  const first = histogramTime(range[0], timezone);
  const last = histogramTime(range[1], timezone);
  const long = range[1] - range[0] >= 7n * DAY_US;
  if (first.slice(0, 4) !== last.slice(0, 4)) return text.slice(0, 10) + (long ? "" : "\n" + text.slice(11, 16));
  if (first.slice(0, 10) !== last.slice(0, 10)) {
    return text.slice(5, 10) + (long ? "" : "\n" + text.slice(11, 16));
  }
  return range[1] - range[0] <= 2n * MINUTE_US ? text.slice(11) : text.slice(11, 16);
}

/** 回拨期间重复的本地时间，通过偏移区分。 */
export function histogramZone(start: bigint, end: bigint, timezone: string): string {
  const first = formatOffset(start, timezone);
  const last = formatOffset(end, timezone);
  return timezone + " · UTC" + first + (first === last ? "" : " → UTC" + last);
}
