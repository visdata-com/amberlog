// 分布图的分桶：按 REQ-AMB-QRY-HISTOGRAM-V2 的取值规则在浏览器里自算桶起点，
// 与服务端一致（REQ-AMB-UI-SEARCH-HIST-INTERVAL、HIST-DRAW、HIST-ZOOM）。

import { type Zone, UTC_ZONE, floorDiv } from "./zone.ts";

const US_PER_S = 1000000n;
const US_PER_DAY = 86400n * US_PER_S;
export const MAX_BARS = 100;

/** 十三档间隔（REQ-AMB-UI-SEARCH-HIST-INTERVAL），都不满足时取整数天数。 */
export const INTERVALS: readonly (readonly [string, bigint])[] = ([
  ["1s", 1n], ["5s", 5n], ["10s", 10n], ["30s", 30n], ["1m", 60n], ["5m", 300n], ["10m", 600n],
  ["30m", 1800n], ["1h", 3600n], ["3h", 10800n], ["6h", 21600n], ["12h", 43200n], ["1d", 86400n],
] as const).map(([name, s]) => [name, s * US_PER_S] as const);

/** 偏移的绝对值不超过 26 小时：往回找本地桶起点时最多看到 t − I − 这么远。 */
const OFFSET_SPAN = 2n * US_PER_DAY;

/** `_timestamp` 为 t 的行在时区 zone、间隔 i 下的 histogram 取值（UTC 微秒）。 */
export function bucketOf(t: bigint, i: bigint, zone: Zone): bigint {
  if (zone === UTC_ZONE) return floorDiv(t, i) * i;
  let off = zone.offset(t);
  const b = floorDiv(t + off, i) * i;
  const floor = t - i - OFFSET_SPAN;
  // 从 t 所在的偏移段往前逐段找本地挂钟值为 b 的时刻：最先找到的是不晚于 t 的最晚者
  let end = t;
  let later: bigint | null = null;
  for (;;) {
    const start = zone.prevTransition(end, floor);
    const localEnd = end + off;
    if (b > localEnd && later !== null) return later; // b 落在拨快跳过的那段里：取跳变之后的第一个时刻
    if (start === null || b >= start + off) return b - off;
    later = start;
    end = start - 1n;
    off = zone.offset(end);
  }
}

/** t 之后第一个 histogram 取值不同于 k（t 的取值）的时刻。 */
function nextChange(t: bigint, k: bigint, i: bigint, zone: Zone): bigint {
  if (zone === UTC_ZONE) return floorDiv(t, i) * i + i;
  for (let x = t; ;) {
    const off = zone.offset(x);
    const reach = floorDiv(x + off, i) * i + i - off; // 本段内本地挂钟到达下一个 i 的整数倍的时刻
    const c = zone.nextTransition(x, reach) ?? reach;
    if (bucketOf(c, i, zone) !== k) return c;
    x = c;
  }
}

/** 取值 k 的下一个分桶起点：大于 k 的最小取值（REQ-AMB-QRY-HISTOGRAM-V2）。 */
export function nextBucketStart(k: bigint, i: bigint, zone: Zone): bigint {
  return bucketOf(nextChange(k, bucketOf(k, i, zone), i, zone), i, zone);
}

/** [start, end) 的分桶（各时刻不同的取值，从小到大）；多于 cap 个时只返回前 cap + 1 个。 */
export function buckets(start: bigint, end: bigint, i: bigint, zone: Zone, cap = Infinity): bigint[] {
  const out: bigint[] = [];
  if (end <= start) return out;
  let t = start;
  let k = bucketOf(t, i, zone);
  out.push(k);
  while (out.length <= cap) {
    t = nextChange(t, k, i, zone);
    if (t >= end) break;
    k = bucketOf(t, i, zone);
    out.push(k);
  }
  return out;
}

/** 使分桶个数不超过 100 的最小间隔：名字（如 `1m`、`2d`）与微秒数。 */
export function chooseInterval(start: bigint, end: bigint, zone: Zone): { name: string; us: bigint } {
  for (const [name, us] of INTERVALS) {
    if (buckets(start, end, us, zone, MAX_BARS).length <= MAX_BARS) return { name, us };
  }
  // 每个按 k 天的桶在 UTC 上不长于 k + 1 天，所以个数不超过 100 时 k 不小于 span / 101 天 − 1
  const span = end - start;
  let k = span / (101n * US_PER_DAY) - 1n;
  if (k < 2n) k = 2n;
  for (; ; k++) {
    const us = k * US_PER_DAY;
    if (buckets(start, end, us, zone, MAX_BARS).length <= MAX_BARS) return { name: k + "d", us };
  }
}

/**
 * 点柱或拖选的缩放范围（REQ-AMB-UI-SEARCH-HIST-ZOOM）：所选首桶起点至末桶的下一个分桶起点，
 * 与原范围取交集。
 */
export function zoomRange(first: bigint, last: bigint, i: bigint, zone: Zone, range: readonly [bigint, bigint]): [bigint, bigint] {
  const lo = first < last ? first : last;
  const hi = nextBucketStart(first < last ? last : first, i, zone);
  return [lo > range[0] ? lo : range[0], hi < range[1] ? hi : range[1]];
}
