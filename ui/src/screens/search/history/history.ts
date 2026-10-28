// 本地查询历史（REQ-AMB-UI-SEARCH-V2-HISTORY-SAVE、LOCAL-STORAGE）：每项的内容、记入与去重、存为 JSON 与读回。

import type { Condition, FieldOp } from "../sql.ts";

/** 时间范围：快捷项记其时长，否则记起止（UTC 微秒）。 */
export type HistoryRange = { quick: bigint } | { start: bigint; end: bigint };

export type HistoryItem =
  | { mode: "cond"; stream: string; range: HistoryRange; conds: Condition[]; at: bigint }
  | { mode: "sql"; range: HistoryRange; sql: string; at: bigint };

export const HISTORY_MAX = 20;

const sameRange = (a: HistoryRange, b: HistoryRange): boolean =>
  "quick" in a ? "quick" in b && a.quick === b.quick : !("quick" in b) && a.start === b.start && a.end === b.end;

export const sameCondition = (a: Condition, b: Condition): boolean =>
  a.kind === "text"
    ? b.kind === "text" && a.text === b.text
    : b.kind === "field" && a.field === b.field && a.op === b.op && a.value === b.value;

/** 两项除发起的时刻外相同。 */
export function sameQuery(a: HistoryItem, b: HistoryItem): boolean {
  if (!sameRange(a.range, b.range)) return false;
  if (a.mode === "sql") return b.mode === "sql" && a.sql === b.sql;
  return (
    b.mode === "cond" &&
    a.stream === b.stream &&
    a.conds.length === b.conds.length &&
    a.conds.every((c, i) => sameCondition(c, b.conds[i]!))
  );
}

/** 记入一项（`list` 从新到旧）：与最新一项相同时不另记，只留最新的 20 项。 */
export function record(list: readonly HistoryItem[], item: HistoryItem): HistoryItem[] {
  if (list[0] !== undefined && sameQuery(list[0], item)) return [...list];
  return [item, ...list].slice(0, HISTORY_MAX);
}

// 存储形状：微秒写成十进制字符串（JSON 数不保微秒精度）
type Stored = Record<string, unknown>;

export function encode(list: readonly HistoryItem[]): string {
  return JSON.stringify(
    list.map((it): Stored => {
      const range = "quick" in it.range ? { quick: String(it.range.quick) } : { start: String(it.range.start), end: String(it.range.end) };
      return it.mode === "sql"
        ? { mode: "sql", range, sql: it.sql, at: String(it.at) }
        : { mode: "cond", stream: it.stream, range, conds: it.conds, at: String(it.at) };
    }),
  );
}

const OPS: readonly string[] = ["=", "!=", "<", "<=", ">", ">=", "LIKE", "IS NULL", "IS NOT NULL"];

class Shape extends Error {}

const obj = (v: unknown): Stored => {
  if (typeof v !== "object" || v === null || Array.isArray(v)) throw new Shape();
  return v as Stored;
};
const str = (v: unknown): string => {
  if (typeof v !== "string") throw new Shape();
  return v;
};
const int = (v: unknown): bigint => {
  const s = str(v);
  if (!/^-?\d+$/.test(s)) throw new Shape();
  return BigInt(s);
};

function condition(v: unknown): Condition {
  const o = obj(v);
  if (o["kind"] === "text") return { kind: "text", text: str(o["text"]) };
  const op = str(o["op"]);
  if (o["kind"] !== "field" || !OPS.includes(op)) throw new Shape();
  return { kind: "field", field: str(o["field"]), op: op as FieldOp, value: str(o["value"]) };
}

function range(v: unknown): HistoryRange {
  const o = obj(v);
  return "quick" in o ? { quick: int(o["quick"]) } : { start: int(o["start"]), end: int(o["end"]) };
}

function item(v: unknown): HistoryItem {
  const o = obj(v);
  if (o["mode"] === "sql") return { mode: "sql", range: range(o["range"]), sql: str(o["sql"]), at: int(o["at"]) };
  const conds = o["conds"];
  if (o["mode"] !== "cond" || !Array.isArray(conds)) throw new Shape();
  return { mode: "cond", stream: str(o["stream"]), range: range(o["range"]), conds: conds.map(condition), at: int(o["at"]) };
}

/** 读回存储的文字；没有、读出失败或不是预期形状时为空历史。 */
export function decode(text: string | null): HistoryItem[] {
  if (text === null) return [];
  try {
    const v: unknown = JSON.parse(text);
    if (!Array.isArray(v)) return [];
    return v.map(item).slice(0, HISTORY_MAX);
  } catch {
    return [];
  }
}
