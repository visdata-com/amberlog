// 检索屏的结果表（REQ-AMB-UI-SEARCH-TABLE、TIME-COLUMNS、TABLE-SORT、HIGHLIGHT、TABLE-HIDE）。

import { intOf, rawText, type Hit, type Json } from "../../lib/json.ts";
import { Time, TimeHeader } from "../../shell/Time.tsx";
import { highlight } from "./fulltext.ts";

/** 各 hit 的键按首次出现先后排成的并集，有 `_timestamp` 时它在首列。 */
export function columnsOf(hits: readonly Hit[]): string[] {
  const seen = new Set<string>();
  const keys: string[] = [];
  for (const h of hits) {
    for (const k of h.keys()) {
      if (!seen.has(k)) {
        seen.add(k);
        keys.push(k);
      }
    }
  }
  const ts = keys.indexOf("_timestamp");
  if (ts > 0) keys.unshift(...keys.splice(ts, 1));
  return keys;
}

/** 一格的内容：时刻列的整数按时刻显示，字符串标出全文词，其余按原始 JSON 文本。 */
export function Value({ v, time, terms }: { v: Json | undefined; time: boolean; terms: readonly string[] }) {
  const us = time ? intOf(v) : null;
  if (us !== null) return <Time us={us} />;
  if (typeof v === "string" && terms.length > 0) {
    return <>{highlight(v, terms).map((s) => (s.hit ? <mark>{s.text}</mark> : s.text))}</>;
  }
  return <>{rawText(v)}</>;
}

export interface TableProps {
  hits: readonly Hit[];
  timeKeys: ReadonlySet<string>;
  terms: readonly string[];
  hidden: ReadonlySet<string>;
  /** 条件模式下 `_timestamp` 列头可排序；null 为不可排序（SQL 模式）。 */
  ascending: boolean | null;
  /** 刚看过详情的一行（DETAIL-CLOSE），没有时为 -1。 */
  viewed: number;
  /** 上下文结果的中心行（REQ-AMB-UI-SEARCH-V2-CONTEXT-VIEW），没有时为 -1 或不给。 */
  center?: number;
  onSort: () => void;
  onHide: (col: string) => void;
  onOpen: (row: number) => void;
}

export function Table(p: TableProps) {
  const cols = columnsOf(p.hits).filter((c) => !p.hidden.has(c));
  return (
    <table class="hits">
      <thead>
        <tr>
          {cols.map((c) => (
            <th data-col={c}>
              {c === "_timestamp" && p.ascending !== null ? (
                <button type="button" class="sort" id="s-sort" onClick={p.onSort}>
                  <TimeHeader label={c} />
                  {p.ascending ? " ↑" : " ↓"}
                </button>
              ) : p.timeKeys.has(c) ? (
                <TimeHeader label={c} />
              ) : (
                c
              )}
              <button type="button" class="hide" aria-label="隐藏列" onClick={() => p.onHide(c)}>
                ×
              </button>
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {p.hits.map((h, i) => (
          <tr
            tabIndex={0}
            class={[i === p.viewed ? "viewed" : "", i === p.center ? "center" : ""].join(" ").trim() || undefined}
            onClick={() => p.onOpen(i)}
            onKeyDown={(e) => {
              if (e.key === "Enter") p.onOpen(i);
            }}
          >
            {cols.map((c) =>
              h.has(c) ? (
                // 时刻格的悬停文字由 <Time> 给出；其余格截断时经悬停看全文
                <td title={p.timeKeys.has(c) && intOf(h.get(c)) !== null ? undefined : rawText(h.get(c))}>
                  <Value v={h.get(c)} time={p.timeKeys.has(c)} terms={p.terms} />
                </td>
              ) : (
                <td />
              ),
            )}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
