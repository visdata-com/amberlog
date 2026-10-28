// 上下文浏览的纯函数（REQ-AMB-UI-SEARCH-V2 上下文浏览一组）：两个上下文请求的写法（CONTEXT-REQUEST）与上下文结果的合成、
// 中心行与不足的判断（CONTEXT-VIEW）。

import { toJsonText, type Hit } from "../../../lib/json.ts";
import { HOUR_US } from "../../../lib/time.ts";
import type { Query } from "../../../shell/api.ts";
import { quoteIdent } from "../sql.ts";

/** 前段请求取 t 之前最近的 20 条，后段请求取 t 起最早的 21 条（含中心行）。 */
export const BEFORE_SIZE = 20;
export const AFTER_SIZE = 21;

const ASC = " ORDER BY _timestamp ASC";

/**
 * 检索请求的 SQL 中 `FROM "<stream>"` 之后的 ` WHERE ` 部分（不含末尾的 ` ORDER BY _timestamp ASC`）；没有条件时为空。
 * 取自实际发出的检索请求，与它的字面量写法一致。
 */
export function whereOf(searchSql: string, stream: string): string {
  const head = "SELECT * FROM " + quoteIdent(stream);
  if (!searchSql.startsWith(head)) return "";
  const rest = searchSql.slice(head.length);
  const body = rest.endsWith(ASC) ? rest.slice(0, -ASC.length) : rest;
  return body.startsWith(" WHERE ") ? body : "";
}

/** 前段与后段两个上下文请求；`where` 为空串时不带条件。 */
export function contextQueries(stream: string, t: bigint, where: string): [Query, Query] {
  const from = "SELECT * FROM " + quoteIdent(stream) + where;
  const start = t - HOUR_US;
  return [
    { sql: from, start_time: start < 0n ? 0n : start, end_time: t, size: BEFORE_SIZE, from: 0 },
    { sql: from + ASC, start_time: t, end_time: t + HOUR_US, size: AFTER_SIZE, from: 0 },
  ];
}

/** 两个 JSON 对象相同：键的集合相同、各键的值相同（数字按原文）。 */
export function sameObject(a: Hit, b: Hit): boolean {
  if (a.size !== b.size) return false;
  for (const [k, v] of a) {
    const w = b.get(k);
    if (w === undefined || toJsonText(v) !== toJsonText(w)) return false;
  }
  return true;
}

export interface ContextResult {
  /** 后段逆序在前、前段原序在后。 */
  rows: Hit[];
  /** 中心行在 `rows` 中的下标；后段中没有与打开的那一行相同者时为 null。 */
  center: number | null;
  /** 前段少于 20 条或后段少于 21 条。 */
  few: boolean;
}

/** 合成上下文结果（CONTEXT-VIEW）。 */
export function combine(before: readonly Hit[], after: readonly Hit[], opened: Hit): ContextResult {
  const at = after.findIndex((h) => sameObject(h, opened));
  return {
    rows: [...after].reverse().concat(before),
    center: at < 0 ? null : after.length - 1 - at,
    few: before.length < BEFORE_SIZE || after.length < AFTER_SIZE,
  };
}
