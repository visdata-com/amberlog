// 分布图的柱与总条数（REQ-AMB-UI-SEARCH-HIST-DRAW、HIST-TOOLTIP、TOTAL）。
// 输入取分布请求 hits 中 `t`、`n` 的原始 JSON 文本，不经浮点换算。

export interface Bar {
  /** 分桶起点（UTC 微秒）。 */
  start: bigint;
  /** 记录数：该分桶 `n` 的原始 JSON 文本，响应中没有该分桶时为 `0`。 */
  n: string;
}

const INTEGER = /^-?\d+$/;

/** 分布请求的应答（各 hit 的 `t`、`n` 原文）按分桶起点归总；`t` 不是整数的忽略。 */
export function countsByStart(rows: readonly { t: string | null; n: string | null }[]): Map<bigint, string> {
  const m = new Map<bigint, string>();
  for (const { t, n } of rows) if (t !== null && INTEGER.test(t) && n !== null) m.set(BigInt(t), n);
  return m;
}

/** 每个分桶一根柱，按起点从小到大。 */
export function bars(starts: readonly bigint[], counts: ReadonlyMap<bigint, string>): Bar[] {
  return starts.map((start) => ({ start, n: counts.get(start) ?? "0" }));
}

/** 柱高占最高一根的比例（0～1）；全部为 0 时都为 0。 */
export function heights(list: readonly Bar[]): number[] {
  const ns = list.map((b) => (INTEGER.test(b.n) ? Number(b.n) : 0));
  const max = Math.max(0, ...ns);
  return ns.map((n) => (max > 0 ? n / max : 0));
}

/** 总条数：各 `n` 之和；有不是整数的 `n` 时为 null（显示「共 — 条」）。 */
export function total(rows: readonly { n: string | null }[]): bigint | null {
  let sum = 0n;
  for (const { n } of rows) {
    if (n === null || !INTEGER.test(n)) return null;
    sum += BigInt(n);
  }
  return sum;
}

/** 本页的行范围（REQ-AMB-UI-SEARCH-INFO）：第 from + 1 行至第 from + 本页行数行；本页没有行时为 null。 */
export function pageRows(from: number, count: number): string | null {
  return count === 0 ? null : `第 ${from + 1}～${from + count} 行`;
}
