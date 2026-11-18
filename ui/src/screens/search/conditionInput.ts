// 条件输入的提示（REQ-AMB-UI-SEARCH-V4-INPUT-HINTS）与草稿的判定（DRAFT）：不完整的输入不悄悄变成全文条件。
import type { ValueRow } from "./FieldsBar.tsx";
import { type Condition, type FieldOp, parseCondition, splitField, valueText } from "./sql.ts";

const OPS: readonly FieldOp[] = ["=", "!=", "<", "<=", ">", ">=", "LIKE", "IS NULL", "IS NOT NULL"];

export type ConditionHint =
  | { kind: "field" | "op"; text: string; fill: string }
  | { kind: "value" | "text"; text: string; condition: Condition };

export interface ConditionDraft {
  condition: Condition | null;
  error: string | null;
}

function fieldEntry(input: string, columns: readonly string[]) {
  const s = input.trimStart();
  const field = columns.filter((c) => s.startsWith(c) && /^[\s=!<>]/.test(s.slice(c.length)))
    .sort((a, b) => b.length - a.length)[0];
  if (field === undefined) return null;
  const rest = s.slice(field.length).trimStart();
  const match = /^(>=|<=|!=|=|<|>|LIKE(?=\s|$))([\s\S]*)$/i.exec(rest);
  // 如「message timeout」仍是普通关键词短语；只有空尾部或运算语法进入字段阶段。
  if (match === null && !OPS.some((op) => op.startsWith(rest.toUpperCase())) && !/^(?:[=!<>]|IS(?:\s|$)|LIKE(?:\s|$))/i.test(rest)) return null;
  return { field, rest, op: match === null ? null : match[1]!.toUpperCase() as FieldOp, value: match?.[2]?.trim() ?? "" };
}

const quoted = (s: string): boolean => s.length >= 2 && s.startsWith('"') && s.endsWith('"') && !s.slice(1, -1).includes('"');

/** DRAFT 四种不完整的输入的提示（REQ-AMB-UI-SEARCH-V4-DRAFT）。 */
export const INCOMPLETE = {
  quote: "请补全全文内容的双引号",
  op: "请选择完整的运算符",
  value: "请填写字段值后再添加条件",
  valueQuote: "请补全字段值的双引号",
} as const;

/** ② 以空白加 `IS`、`IS N`……`IS NOT NUL` 结尾（各词之间一段空白），其前有非空白字符。 */
const PARTIAL_IS = /\S\s+IS(?:\s+(?:N|NU|NUL|NOT(?:\s+(?:N|NU|NUL))?))?$/i;
/** ③ 后半：以空白加 `LIKE` 结尾，其前有非空白字符。 */
const TRAILING_LIKE = /\S\s+LIKE$/i;

/**
 * 草稿是不是不完整的输入（REQ-AMB-UI-SEARCH-V4-DRAFT），是时返回该种的提示。只看文字本身，不依赖字段表是否已取到。
 */
export function incompleteHint(input: string): string | null {
  const t = input.trim();
  if (t === "") return null;
  if (t === '"' || (t.startsWith('"') && !t.endsWith('"'))) return INCOMPLETE.quote;
  if (PARTIAL_IS.test(t)) return INCOMPLETE.op;
  if (quoted(t)) return null;
  const split = splitField(t);
  if ((split !== null && split.field !== "" && split.value === "" && split.op !== "IS NULL" && split.op !== "IS NOT NULL") || TRAILING_LIKE.test(t)) {
    return INCOMPLETE.value;
  }
  const c = parseCondition(t);
  if (c?.kind === "field" && c.value.startsWith('"') && (c.value.length === 1 || !c.value.endsWith('"'))) return INCOMPLETE.valueQuote;
  return null;
}

/**
 * 回车、点「查询」与切到 SQL 共用（DRAFT、FIELD-COND、TEXT-COND、QUERY、TO-SQL）：不完整的输入给出提示、不加入；
 * 否则按 FIELD-COND 或 TEXT-COND 得到要加入的条件（没有草稿时为 null）。
 */
export function conditionDraft(input: string): ConditionDraft {
  const error = incompleteHint(input);
  return error !== null ? { condition: null, error } : { condition: parseCondition(input), error: null };
}

/**
 * 输入提示（INPUT-HINTS）：空框不提示；字段名后提示运算、「字段名 运算」后提示已统计的取值；输入的文字会成为全文条件时
 * （不成为字段条件、也不是不完整的输入），最后一项为全文项，否则不提供全文项。
 */
export function conditionHints(input: string, columns: readonly string[], valuesOf: (field: string) => readonly ValueRow[] | null): ConditionHint[] {
  const s = input.trim();
  if (s === "") return [];
  const draft = conditionDraft(input);
  const fulltext: ConditionHint[] = draft.condition?.kind === "text"
    ? [{ kind: "text", text: "添加全文条件：" + draft.condition.text, condition: draft.condition }]
    : [];
  const entry = quoted(s) || s.startsWith('"') ? null : fieldEntry(input, columns);
  if (entry !== null) {
    if (entry.op === null) {
      return [...OPS.filter((op) => op.startsWith(entry.rest.toUpperCase())).map((op): ConditionHint => ({
        kind: "op", text: op, fill: entry.field + " " + op + (op.startsWith("IS") ? "" : " "),
      })), ...fulltext];
    }
    const prefix = (entry.value.startsWith('"') ? entry.value.slice(1).replace(/"$/, "") : entry.value).toLowerCase();
    return [...(valuesOf(entry.field) ?? []).filter((row) => row.text.toLowerCase().includes(prefix)).map((row): ConditionHint => ({
      kind: "value", text: row.text,
      condition: { kind: "field", field: entry.field, op: entry.op!, value: valueText(row) },
    })), ...fulltext];
  }
  const names: ConditionHint[] = columns.filter((c) => !s.startsWith('"') && c.toLowerCase().includes(s.toLowerCase()))
    .map((name) => ({ kind: "field", text: name, fill: name + " " }));
  return [...names, ...fulltext];
}
