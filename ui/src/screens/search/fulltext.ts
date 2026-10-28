// 全文条件：结果表与详情里的标出（REQ-AMB-UI-SEARCH-HIGHLIGHT）与时间范围的限制
// （FULLTEXT-NARROW、FULLTEXT-LIMIT、SQL-FULLTEXT-NOTE）。

import type { Condition } from "./sql.ts";

const HOUR_US = 3600000000n;
/** 加入第一个全文条件时，长于它的时间范围收到它。 */
export const FULLTEXT_NARROW_US = 24n * HOUR_US;
/** 条件模式下含全文条件时，时间范围最长为它。 */
export const FULLTEXT_LIMIT_US = 7n * 24n * HOUR_US;
export const FULLTEXT_LIMIT_TEXT = "含全文检索时，时间范围最多 7 天。可以先用字段条件缩小，或分段查询";
export const SQL_FULLTEXT_NOTE = "含全文检索的长范围查询会扫描范围内的全部文本字段，可能很慢";

export interface Segment { text: string; hit: boolean }

/** 只把 ASCII 大写字母视同小写（与 REQ-AMB-QRY-TEXT-MATCH 的 match_all 相同），长度不变。 */
const foldAscii = (s: string): string => s.replace(/[A-Z]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 32));

/** 把 text 切成片段，与 terms 中任一文本相匹配的部分（可重叠，合并）为 hit。 */
export function highlight(text: string, terms: readonly string[]): Segment[] {
  const folded = foldAscii(text);
  const marked = new Uint8Array(text.length);
  for (const term of terms) {
    if (term === "") continue;
    const t = foldAscii(term);
    for (let at = folded.indexOf(t); at >= 0; at = folded.indexOf(t, at + 1)) marked.fill(1, at, at + t.length);
  }
  const out: Segment[] = [];
  let from = 0;
  for (let j = 1; j <= text.length; j++) {
    if (j === text.length || marked[j] !== marked[from]) {
      out.push({ text: text.slice(from, j), hit: marked[from] === 1 });
      from = j;
    }
  }
  return out;
}

export const hasText = (conds: readonly Condition[]): boolean => conds.some((c) => c.kind === "text");

/** 条件模式下加入 added 时是否要把时间范围收到 24 小时（此前没有全文条件且范围长于 24 小时）。 */
export function narrowOnAdd(before: readonly Condition[], added: Condition, length: bigint | null): boolean {
  return added.kind === "text" && !hasText(before) && length !== null && length > FULLTEXT_NARROW_US;
}

/** 条件模式下「查询」因全文条件不可用：条件中有全文条件且时间范围长于 7 天。 */
export function fulltextBlocked(conds: readonly Condition[], length: bigint | null): boolean {
  return hasText(conds) && length !== null && length > FULLTEXT_LIMIT_US;
}
