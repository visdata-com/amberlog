// 条件输入的阶段、候选与提交校验；不完整的字段表达式不能回退为全文条件。
import type { ValueRow } from "./FieldsBar.tsx";
import { type Condition, type FieldOp, parseCondition, valueText } from "./sql.ts";

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

/** 回车、查询和切换 SQL 共用的校验，避免各入口解释同一段输入的方式不同。 */
export function conditionDraft(input: string, columns: readonly string[]): ConditionDraft {
  const s = input.trim();
  const fail = (error: string): ConditionDraft => ({ condition: null, error });
  if (s === "" || quoted(s)) return { condition: parseCondition(s), error: null };
  if (s.startsWith('"')) return fail("请补全全文内容的双引号");
  const entry = fieldEntry(input, columns);
  const condition = parseCondition(input);
  if (entry !== null && entry.op === null && condition?.kind !== "field") return fail("请选择完整的运算符");
  if (condition?.kind !== "field" && /\S\s+IS(?:\s+NOT)?(?:\s+N(?:U(?:L)?)?)?\s*$/i.test(s)) return fail("请选择完整的运算符");
  if ((entry !== null && entry.op !== null && entry.value === "") ||
      (condition?.kind !== "field" && /^.+?\s*(?:!=|<=|>=|=|<|>|\sLIKE)\s*$/i.test(s))) {
    return fail("请填写字段值后再添加条件");
  }
  if (condition?.kind === "field" && condition.value.startsWith('"') && (condition.value.length < 2 || !condition.value.endsWith('"'))) {
    return fail("请补全字段值的双引号");
  }
  return { condition, error: null };
}

/** 空框无候选；字段表达式仅提示运算或取值，普通关键词才提供全文入口。 */
export function conditionHints(input: string, columns: readonly string[], valuesOf: (field: string) => readonly ValueRow[] | null): ConditionHint[] {
  const s = input.trim();
  if (s === "") return [];
  const entry = quoted(s) || s.startsWith('"') ? null : fieldEntry(input, columns);
  if (entry !== null) {
    if (entry.op === null) {
      return OPS.filter((op) => op.startsWith(entry.rest.toUpperCase())).map((op) => ({
        kind: "op", text: op, fill: entry.field + " " + op + (op.startsWith("IS") ? "" : " "),
      }));
    }
    const prefix = (entry.value.startsWith('"') ? entry.value.slice(1).replace(/"$/, "") : entry.value).toLowerCase();
    return (valuesOf(entry.field) ?? []).filter((row) => row.text.toLowerCase().includes(prefix)).map((row) => ({
      kind: "value", text: row.text,
      condition: { kind: "field", field: entry.field, op: entry.op!, value: valueText(row) },
    }));
  }
  const draft = conditionDraft(input, columns);
  // 完整字段表达式可直接回车添加，不再混入全文入口。
  if (draft.condition?.kind === "field" || draft.error !== null) return [];
  const out: ConditionHint[] = columns.filter((c) => !s.startsWith('"') && c.toLowerCase().includes(s.toLowerCase()))
    .map((name) => ({ kind: "field", text: name, fill: name + " " }));
  if (draft.condition?.kind === "text") {
    out.push({ kind: "text", text: "添加全文条件：" + draft.condition.text, condition: draft.condition });
  }
  return out;
}
