// 时区的 UTC 偏移与偏移变化的时刻，供分桶计算用（REQ-AMB-QRY-HISTOGRAM-V2 的取值步骤）。
// 偏移取自浏览器的 Intl（lib/time.ts）；浏览器不给出变化时刻，按固定步长探测、再按秒二分找出，按块缓存。

import { offsetOf } from "../../lib/time.ts";

export interface Zone {
  readonly name: string;
  /** 时刻 t（UTC 微秒）的 UTC 偏移（微秒，东正西负）。 */
  offset(t: bigint): bigint;
  /** (after, limit] 中第一个偏移变化的时刻 x（offset(x) ≠ offset(x − 1)）；没有时为 null。 */
  nextTransition(after: bigint, limit: bigint): bigint | null;
  /** (floor, atOrBefore] 中最后一个偏移变化的时刻；没有时为 null。 */
  prevTransition(atOrBefore: bigint, floor: bigint): bigint | null;
}

export const UTC_ZONE: Zone = {
  name: "UTC",
  offset: () => 0n,
  nextTransition: () => null,
  prevTransition: () => null,
};

const US_PER_S = 1000000n;
/** 探测步长：相邻两次偏移变化相隔不足一个步长而又变回原值时探测不到。 */
const PROBE_STEP = 6n * 3600n * US_PER_S;
/** 偏移变化时刻按块缓存，块长 28 天。 */
const CHUNK = 28n * 86400n * US_PER_S;

export const floorDiv = (a: bigint, b: bigint): bigint => {
  const q = a / b;
  return q * b > a ? q - 1n : q;
};

const zones = new Map<string, Zone>();

/** 所选时区（`UTC` 或浏览器认得的 IANA 时区名）；偏移取自 lib/time.ts 的 offsetOf。 */
export function zoneOf(name: string): Zone {
  if (name === "UTC") return UTC_ZONE;
  const cached = zones.get(name);
  if (cached !== undefined) return cached;
  const z = makeZone(name);
  zones.set(name, z);
  return z;
}

function makeZone(name: string): Zone {
  const offset = (t: bigint): bigint => BigInt(offsetOf(t, name)) * US_PER_S;
  /** lo 与 hi 偏移不同：二分到秒，返回变化时刻（变化都发生在整秒）。 */
  const bisect = (lo: bigint, hi: bigint): bigint => {
    const o = offset(lo);
    let a = floorDiv(lo, US_PER_S), b = floorDiv(hi - 1n, US_PER_S) + 1n;
    while (b - a > 1n) {
      const m = (a + b) / 2n;
      if (offset(m * US_PER_S) === o) a = m; else b = m;
    }
    return b * US_PER_S;
  };
  /** 各块（CHUNK 长、按纪元对齐）中的偏移变化时刻，升序；探测一次后缓存。 */
  const chunks = new Map<bigint, bigint[]>();
  const transitionsOf = (idx: bigint): bigint[] => {
    let list = chunks.get(idx);
    if (list !== undefined) return list;
    list = [];
    const from = idx * CHUNK - 1n, to = (idx + 1n) * CHUNK - 1n;
    for (let x = from; x < to;) {
      const y = x + PROBE_STEP < to ? x + PROBE_STEP : to;
      if (offset(y) !== offset(x)) list.push(bisect(x, y));
      x = y;
    }
    chunks.set(idx, list);
    return list;
  };
  return {
    name,
    offset,
    nextTransition(after, limit) {
      for (let idx = floorDiv(after, CHUNK); idx * CHUNK <= limit; idx++) {
        for (const x of transitionsOf(idx)) {
          if (x > limit) return null;
          if (x > after) return x;
        }
      }
      return null;
    },
    prevTransition(atOrBefore, floor) {
      for (let idx = floorDiv(atOrBefore, CHUNK); (idx + 1n) * CHUNK > floor; idx--) {
        const list = transitionsOf(idx);
        for (let j = list.length - 1; j >= 0; j--) {
          const x = list[j]!;
          if (x <= floor) return null;
          if (x <= atOrBefore) return x;
        }
      }
      return null;
    },
  };
}
