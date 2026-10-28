// 字段栏（REQ-AMB-UI-SEARCH-V2-FIELDS-BAR、FIELDS-GROUPS、FIELDS-FILTER）与输入提示中的取值（INPUT-HINTS）。

import type { Field } from "../../shell/api.ts";

export type { Field };

export interface FieldGroup {
  /** 组名：字段名中第一个 `.` 之前的部分；没有 `.` 的字段为 null（「记录字段」组）。 */
  name: string | null;
  fields: Field[];
}

/** REQ-AMB-DM-NAMESPACE-V2 所定 `{stream}` 的合规写法。 */
export const isStreamName = (s: string): boolean => /^[a-z0-9][a-z0-9_]{0,63}$/.test(s);

/** 只把 ASCII 字母视为不区分大小写（同 match_all）。 */
const foldAscii = (s: string): string => s.replace(/[A-Z]/g, (c) => String.fromCharCode(c.charCodeAt(0) + 32));

/** 按码点比较（不按 UTF-16 码元）。 */
export function byCodePoint(a: string, b: string): number {
  const x = [...a], y = [...b];
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i]!.codePointAt(0)! - y[i]!.codePointAt(0)!;
    if (d !== 0) return d;
  }
  return x.length - y.length;
}

const groupOf = (name: string): string | null => {
  const dot = name.indexOf(".");
  return dot < 0 ? null : name.slice(0, dot);
};

/**
 * 分组：「记录字段」组在前（`_timestamp` 最前、`message` 次之，其余按码点升序），其余各组按组名的码点升序、组内按名字的
 * 码点升序。`filter` 不为空时只留名字含它的字段，没有字段的组不列出。
 */
export function groupFields(fields: readonly Field[], filter = ""): FieldGroup[] {
  const f = foldAscii(filter);
  const groups = new Map<string | null, Field[]>();
  for (const x of fields) {
    if (f !== "" && !foldAscii(x.name).includes(f)) continue;
    const g = groupOf(x.name);
    const list = groups.get(g) ?? [];
    list.push(x);
    groups.set(g, list);
  }
  const rank = (n: string): number => (n === "_timestamp" ? 0 : n === "message" ? 1 : 2);
  const out: FieldGroup[] = [];
  const record = groups.get(null);
  if (record !== undefined) {
    record.sort((a, b) => rank(a.name) - rank(b.name) || byCodePoint(a.name, b.name));
    out.push({ name: null, fields: record });
  }
  const names = [...groups.keys()].filter((g): g is string => g !== null).sort(byCodePoint);
  for (const g of names) out.push({ name: g, fields: groups.get(g)!.sort((a, b) => byCodePoint(a.name, b.name)) });
  return out;
}

/** 输入的文字为「字段名 f 加一个运算」时返回 f（取 `names` 中能这样读的最长者），否则为 null。 */
export function fieldBeforeOp(input: string, names: readonly string[]): string | null {
  let best: string | null = null;
  for (const n of names) {
    if (!input.trimStart().startsWith(n)) continue;
    const rest = input.trimStart().slice(n.length);
    if (!/^\s*(=|!=|<=|>=|<|>|\sLIKE)\s*$/i.test(rest)) continue;
    if (best === null || n.length > best.length) best = n;
  }
  return best;
}
