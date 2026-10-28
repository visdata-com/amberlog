// 检索屏的条件与 SQL：胶囊切分（REQ-AMB-UI-SEARCH-V2-FIELD-COND、TEXT-COND）、
// 字面量（LITERAL）、条件写成 SQL（SQL-GEN、HIST-REQUEST、TOTAL、VALUES-REQUEST）与系统表时刻列（TIME-COLUMNS）。

export type FieldOp = "=" | "!=" | "<" | "<=" | ">" | ">=" | "LIKE" | "IS NULL" | "IS NOT NULL";

export type Condition =
  | { kind: "field"; field: string; op: FieldOp; value: string }
  | { kind: "text"; text: string };

/** 同一位置较长者优先：`<=` 先于 `<`，`!=` 先于 `=`。 */
const SYMBOL_OPS: readonly FieldOp[] = ["!=", "<=", ">=", "=", "<", ">"];
const NULL_SUFFIX = / (IS NULL|IS NOT NULL)$/i;
/** 前后有空白的 `LIKE`（不区分大小写）。 */
const LIKE = /\sLIKE\s/i;

/** 去掉首尾空白后整段以 `"` 括起、中间不含 `"`。 */
function wholeQuoted(s: string): boolean {
  return s.length >= 2 && s.startsWith("\"") && s.endsWith("\"") && !s.slice(1, -1).includes("\"");
}

/** 条件模式下回车加入的条件；文字为空（或只剩一对 `"`）时为 null。 */
export function parseCondition(input: string): Condition | null {
  const s = input.trim();
  if (!wholeQuoted(s)) {
    const field = fieldCondition(s);
    if (field !== null) return field;
  }
  const text = wholeQuoted(s) ? s.slice(1, -1) : s;
  return text === "" ? null : { kind: "text", text };
}

function fieldCondition(s: string): Condition | null {
  const nul = NULL_SUFFIX.exec(s);
  if (nul !== null) {
    const field = s.slice(0, nul.index).trim();
    if (field === "") return null;
    const op: FieldOp = nul[1]!.length > 7 ? "IS NOT NULL" : "IS NULL";
    return { kind: "field", field, op, value: "" };
  }
  let best: { at: number; len: number; op: FieldOp } | null = null;
  for (const op of SYMBOL_OPS) {
    const at = s.indexOf(op);
    if (at < 0) continue;
    if (best === null || at < best.at || (at === best.at && op.length > best.len)) best = { at, len: op.length, op };
  }
  const like = LIKE.exec(s);
  if (like !== null) {
    // 「LIKE」本身从空白之后算起；与符号运算同一位置不会发生（前一个字符是空白）
    const at = like.index + 1;
    if (best === null || at < best.at) best = { at, len: 4, op: "LIKE" };
  }
  if (best === null) return null;
  const field = s.slice(0, best.at).trim();
  const value = s.slice(best.at + best.len).trim();
  if (field === "" || value === "") return null;
  return { kind: "field", field, op: best.op, value };
}

export const quoteIdent = (s: string): string => "\"" + s.replaceAll("\"", "\"\"") + "\"";
export const sqlString = (s: string): string => "'" + s.replaceAll("'", "''") + "'";

/** REQ-AMB-QRY-SEARCH-ENDPOINT-V9 所称数值字面量（十进制，可带正负号与指数）。 */
const NUMERIC = /^[+-]?(\d+\.?\d*|\.\d+)([eE][+-]?\d+)?$/;

/** 字段表中类型为 `Utf8` 的字段（条件模式下按字段表写字面量时用；没有字段表或 SQL 模式下不传）。 */
export type Utf8Fields = (field: string) => boolean;
const NO_TABLE: Utf8Fields = () => false;

/** `utf8` 为真时（字段表中该字段为 `Utf8`），未加双引号的取值也写成字符串字面量（REQ-AMB-UI-SEARCH-V2-LITERAL）。 */
export function literal(op: FieldOp, value: string, utf8 = false): string {
  if (op === "LIKE") return sqlString(value);
  if (value.length >= 2 && value.startsWith("\"") && value.endsWith("\"")) return sqlString(value.slice(1, -1));
  if (utf8) return sqlString(value);
  if (NUMERIC.test(value)) return value;
  if (value === "true" || value === "false") return value;
  return sqlString(value);
}

export function conditionSql(c: Condition, utf8: Utf8Fields = NO_TABLE): string {
  if (c.kind === "text") return "match_all(" + sqlString(c.text) + ")";
  if (c.op === "IS NULL" || c.op === "IS NOT NULL") return quoteIdent(c.field) + " " + c.op;
  return quoteIdent(c.field) + " " + c.op + " " + literal(c.op, c.value, utf8(c.field));
}

/** 有条件时为 ` WHERE …`（各条件按加入先后以 ` AND ` 连接），否则为空。 */
export function whereClause(conds: readonly Condition[], utf8: Utf8Fields = NO_TABLE): string {
  return conds.length === 0 ? "" : " WHERE " + conds.map((c) => conditionSql(c, utf8)).join(" AND ");
}

export function searchSql(stream: string, conds: readonly Condition[], ascending: boolean, utf8: Utf8Fields = NO_TABLE): string {
  return selectSql(stream, whereClause(conds, utf8), ascending);
}

/** 以已写出的 ` WHERE ` 部分写检索请求的 SQL（排序重发时沿用原请求的条件写法）。 */
export function selectSql(stream: string, where: string, ascending: boolean): string {
  return "SELECT * FROM " + quoteIdent(stream) + where + (ascending ? " ORDER BY _timestamp ASC" : "");
}

/** 分布请求与计数请求接检索请求的 ` WHERE ` 部分原文（`where` 为 `whereClause` 的结果）。 */
export function histogramSql(stream: string, where: string, interval: string): string {
  const h = "histogram(_timestamp, " + sqlString(interval) + ")";
  return "SELECT " + h + " AS t, count(*) AS n FROM " + quoteIdent(stream) + where + " GROUP BY " + h + " ORDER BY t";
}

export function countSql(stream: string, where: string): string {
  return "SELECT count(*) AS n FROM " + quoteIdent(stream) + where;
}

/** 取值请求（REQ-AMB-UI-SEARCH-V2-VALUES-REQUEST）：`where` 为最近一次检索请求的 ` WHERE ` 部分（不含 ` ORDER BY`）。 */
export function valuesSql(stream: string, where: string, field: string): string {
  const f = quoteIdent(field);
  const notNull = (where === "" ? " WHERE " : where + " AND ") + f + " IS NOT NULL";
  return "SELECT " + f + ", count(*) AS n__ FROM " + quoteIdent(stream) + notNull + " GROUP BY " + f + " ORDER BY n__ DESC LIMIT 10";
}

/** 取值在条件中的写法（VALUES-ADD、INPUT-HINTS、DETAIL-ADD）：JSON 字符串时以 `"` 括起，否则为原始 JSON 文本。 */
export const valueText = (v: { json: string; isString: boolean; text: string }): string =>
  v.isString ? "\"" + v.text + "\"" : v.json;

/** 从单条详情加入：字符串值以 `"` 括起，其余取原始 JSON 文本（REQ-AMB-UI-SEARCH-DETAIL-ADD）。 */
export function detailCondition(field: string, negate: boolean, value: { json: string; isString: boolean; text: string }): Condition {
  return { kind: "field", field, op: negate ? "!=" : "=", value: valueText(value) };
}

/**
 * SQL 模式下从详情插入条件：插到光标处，光标前一个字符存在且不是空白时先补一个空格。
 * 返回新文本与插入之后的光标位置。
 */
export function insertAtCursor(sql: string, cursor: number | null, snippet: string): { sql: string; cursor: number } {
  const at = cursor === null ? sql.length : Math.max(0, Math.min(cursor, sql.length));
  const before = sql.slice(0, at);
  const piece = before !== "" && !/\s$/.test(before) ? " " + snippet : snippet;
  return { sql: before + piece + sql.slice(at), cursor: at + piece.length };
}

/** SQL 文本含不区分大小写的 `match_all` 或 `str_match`（REQ-AMB-UI-SEARCH-SQL-FULLTEXT-NOTE）。 */
export const mentionsFulltext = (sql: string): boolean => /match_all|str_match/i.test(sql);

/** 系统表的时刻列（REQ-AMB-UI-SEARCH-TIME-COLUMNS）。 */
const SYSTEM_TIME_COLUMNS: Readonly<Record<string, readonly string[]>> = {
  _ledger: ["time_min", "time_max", "expires_at"],
  _reconciliation: ["window_start", "window_end"],
  _alert_evaluations: ["scheduled_at", "started_at", "finished_at", "window_start", "window_end"],
};

const TOKEN = /\s+|--[^\n]*|\/\*[\s\S]*?(?:\*\/|$)|'(?:[^']|'')*'?|"(?:[^"]|"")*"?|[A-Za-z_\u0080-￿][A-Za-z0-9_$\u0080-￿]*|[\s\S]/gy;

/** 去掉注释后第一个 `FROM` 之后的名字：不带引号时按小写，带双引号时按原样；没有时为 null。 */
export function fromTable(sql: string): string | null {
  const tokens: string[] = [];
  TOKEN.lastIndex = 0;
  for (let m: RegExpExecArray | null; (m = TOKEN.exec(sql)) !== null && m[0] !== "";) {
    const t = m[0];
    if (/^\s/.test(t) || t.startsWith("--") || t.startsWith("/*")) continue;
    tokens.push(t);
  }
  for (let i = 0; i + 1 < tokens.length; i++) {
    if (tokens[i]!.toLowerCase() !== "from") continue;
    const t = tokens[i + 1]!;
    if (t.startsWith("\"")) return t.endsWith("\"") && t.length >= 2 ? t.slice(1, -1).replaceAll("\"\"", "\"") : null;
    return /^[A-Za-z_\u0080-￿]/.test(t) ? t.toLowerCase() : null;
  }
  return null;
}

/** 结果表中按时刻显示的列名：`_timestamp`，SQL 引用系统表时另加该表的时刻列。 */
export function timeColumns(sql: string): ReadonlySet<string> {
  const table = fromTable(sql);
  const extra = table !== null && Object.hasOwn(SYSTEM_TIME_COLUMNS, table) ? SYSTEM_TIME_COLUMNS[table]! : [];
  return new Set(["_timestamp", ...extra]);
}
