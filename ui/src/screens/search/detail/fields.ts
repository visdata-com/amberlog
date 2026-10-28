// 单条详情的字段页与原始 JSON 页的纯函数（REQ-AMB-UI-SEARCH-DETAIL-GROUPS、DETAIL-JSON）。

import { Num, toJsonText, type Json } from "../../../lib/json.ts";

/** 不含 `.` 的键所在的组。 */
export const RECORD_GROUP = "记录字段";

export interface Group {
  /** 组名；`message` 一项的组为 null（它在最前，不属于任何组）。 */
  name: string | null;
  keys: string[];
}

/**
 * 字段页的分组：`message` 在前；其余的键按第一个 `.` 之前的部分分组，没有 `.` 的为「记录字段」组；
 * 「记录字段」组在前、其余各组按组名的码点升序，组内按键在 hit 中出现的先后。
 */
export function groupFields(keys: Iterable<string>): Group[] {
  const groups = new Map<string, string[]>();
  let message = false;
  for (const k of keys) {
    if (k === "message") {
      message = true;
      continue;
    }
    const dot = k.indexOf(".");
    const name = dot < 0 ? RECORD_GROUP : k.slice(0, dot);
    const g = groups.get(name);
    if (g === undefined) groups.set(name, [k]);
    else g.push(k);
  }
  const names = [...groups.keys()].filter((n) => n !== RECORD_GROUP).sort(byCodePoint);
  if (groups.has(RECORD_GROUP)) names.unshift(RECORD_GROUP);
  const out: Group[] = message ? [{ name: null, keys: ["message"] }] : [];
  for (const n of names) out.push({ name: n, keys: groups.get(n) as string[] });
  return out;
}

/** 按码点比较（`sort()` 的缺省比较按 UTF-16 码元，增补平面的字符会排错）。 */
function byCodePoint(a: string, b: string): number {
  const x = [...a], y = [...b];
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = (x[i] as string).codePointAt(0)! - (y[i] as string).codePointAt(0)!;
    if (d !== 0) return d;
  }
  return x.length - y.length;
}

/** 筛选：字段名或显示的值含 `filter`（ASCII 字母不区分大小写）；`filter` 去掉首尾空白后为空时都留。 */
export function matches(filter: string, key: string, shown: string): boolean {
  const f = filter.trim().toLowerCase();
  return f === "" || key.toLowerCase().includes(f) || shown.toLowerCase().includes(f);
}

/** hit 的 JSON 写法，两格缩进；数字照原文、键照原先后。 */
export function prettyJson(v: Json, indent = ""): string {
  const inner = indent + "  ";
  if (v instanceof Map) {
    if (v.size === 0) return "{}";
    const items = [...v].map(([k, x]) => inner + JSON.stringify(k) + ": " + prettyJson(x, inner));
    return "{\n" + items.join(",\n") + "\n" + indent + "}";
  }
  if (Array.isArray(v)) {
    if (v.length === 0) return "[]";
    return "[\n" + v.map((x) => inner + prettyJson(x, inner)).join(",\n") + "\n" + indent + "]";
  }
  if (v instanceof Num) return v.text;
  return JSON.stringify(v);
}

/** 字段的取值写成「加入」所需的形状（`detailCondition` 的参数）。 */
export function addValue(v: Json): { json: string; isString: boolean; text: string } {
  return { json: toJsonText(v), isString: typeof v === "string", text: typeof v === "string" ? v : "" };
}
