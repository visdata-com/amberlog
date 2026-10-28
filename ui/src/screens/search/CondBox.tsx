// 检索屏的条件框（REQ-AMB-UI-SEARCH-V2-MODES、FIELD-COND、TEXT-COND、COND-REMOVE、STAGED、INPUT-HINTS、TO-SQL、TO-COND）：
// 条件模式下每个条件是一个胶囊，SQL 模式下换成 SQL 文本框。

import { useState } from "preact/hooks";
import { fieldBeforeOp } from "./fields.ts";
import type { ValueRow } from "./FieldsBar.tsx";
import { type Condition, type FieldOp, parseCondition, valueText } from "./sql.ts";

export interface Chip {
  id: number;
  cond: Condition;
}

const OPS: readonly FieldOp[] = ["=", "!=", "<", "<=", ">", ">=", "LIKE", "IS NULL", "IS NOT NULL"];
const FULLTEXT_HINT = "按全文检索";

export function chipText(c: Condition): string {
  if (c.kind === "text") return "全文 " + c.text;
  return c.op === "IS NULL" || c.op === "IS NOT NULL" ? c.field + " " + c.op : c.field + " " + c.op + " " + c.value;
}

type Hint = { kind: "field"; name: string } | { kind: "op"; op: FieldOp } | { kind: "value"; text: string; fill: string } | { kind: "text" };

/**
 * 输入提示：字段名（字段表的字段名，没有字段表时取当前结果表的列名）、运算；「字段名 运算」之后为字段栏已列出的取值；
 * 最后一项为「按全文检索」。
 */
function hintsFor(input: string, columns: readonly string[], valuesOf: (f: string) => readonly ValueRow[] | null): Hint[] {
  const s = input.trimStart();
  const out: Hint[] = [];
  const field = columns.find((c) => s.startsWith(c + " ") && s.slice(c.length + 1).trim() === "");
  const withOp = fieldBeforeOp(input, columns);
  const rows = withOp === null ? null : valuesOf(withOp);
  if (rows !== null) {
    for (const r of rows) out.push({ kind: "value", text: r.text, fill: valueText(r) });
  } else if (withOp !== null) {
    // 已写了运算、没有可提示的取值：只剩「按全文检索」
  } else if (field !== undefined) {
    for (const op of OPS) out.push({ kind: "op", op });
  } else if (s.trim() !== "") {
    const low = s.trim().toLowerCase();
    for (const c of columns) if (c.toLowerCase().includes(low)) out.push({ kind: "field", name: c });
  }
  out.push({ kind: "text" });
  return out;
}

const hintText = (h: Hint): string =>
  h.kind === "field" ? h.name : h.kind === "op" ? h.op : h.kind === "value" ? h.text : FULLTEXT_HINT;

export interface CondBoxProps {
  mode: "cond" | "sql";
  chips: readonly Chip[];
  /** 已生效的条件（上一次发起检索时条件框中的那些）。 */
  applied: ReadonlySet<number>;
  columns: readonly string[];
  /** 字段栏已为该字段列出的取值（取值尚未收起时），没有时为 null。 */
  valuesOf: (field: string) => readonly ValueRow[] | null;
  sql: string;
  confirming: boolean;
  onAdd: (c: Condition) => void;
  onRemove: (id: number) => void;
  onMode: (m: "cond" | "sql") => void;
  onSql: (text: string, cursor: number) => void;
  onCursor: (cursor: number) => void;
  onConfirm: (discard: boolean) => void;
}

export function CondBox(p: CondBoxProps) {
  const [input, setInput] = useState("");
  const [open, setOpen] = useState(false);
  const [pick, setPick] = useState(-1);
  const hints = open && p.mode === "cond" ? hintsFor(input, p.columns, p.valuesOf) : [];

  const commit = (c: Condition | null): void => {
    if (c === null) return;
    p.onAdd(c);
    setInput("");
    setPick(-1);
  };
  const choose = (h: Hint): void => {
    if (h.kind === "text") {
      // 选它即按 TEXT-COND 加入全文条件
      const s = input.trim();
      const quoted = s.length >= 2 && s.startsWith('"') && s.endsWith('"') && !s.slice(1, -1).includes('"');
      const text = quoted ? s.slice(1, -1) : s;
      commit(text === "" ? null : { kind: "text", text });
    } else if (h.kind === "value") {
      setInput(input.trimEnd() + " " + h.fill);
      setPick(-1);
    } else {
      setInput(h.kind === "field" ? h.name + " " : input.trimEnd() + " " + h.op + (h.op.startsWith("IS") ? "" : " "));
      setPick(-1);
    }
  };
  const onKey = (e: KeyboardEvent): void => {
    if (e.key === "ArrowDown" && hints.length > 0) {
      e.preventDefault();
      setPick((pick + 1) % hints.length);
    } else if (e.key === "ArrowUp" && hints.length > 0) {
      e.preventDefault();
      setPick(pick <= 0 ? hints.length - 1 : pick - 1);
    } else if (e.key === "Escape") {
      setOpen(false);
    } else if (e.key === "Enter") {
      e.preventDefault();
      const h = hints[pick];
      if (h !== undefined) choose(h);
      else commit(parseCondition(input));
    } else if (e.key === "Backspace" && input === "" && p.chips.length > 0) {
      p.onRemove(p.chips[p.chips.length - 1]!.id);
    }
  };
  const cursorOf = (e: Event): void => p.onCursor((e.currentTarget as HTMLTextAreaElement).selectionStart);

  return (
    <div class="condbox">
      <span class="modes">
        <button type="button" id="s-mode-cond" aria-pressed={p.mode === "cond"} onClick={() => p.onMode("cond")}>
          条件
        </button>
        <button type="button" id="s-mode-sql" aria-pressed={p.mode === "sql"} onClick={() => p.onMode("sql")}>
          SQL
        </button>
      </span>
      {p.mode === "cond" ? (
        <div class="chips" id="s-conds">
          {p.chips.map((c) => (
            <span class={p.applied.has(c.id) ? "chip" : "chip staged"} data-staged={!p.applied.has(c.id)}>
              <span class="chip-text" data-verbatim>{chipText(c.cond)}</span>
              <button type="button" class="chip-remove" aria-label="删除条件" onClick={() => p.onRemove(c.id)}>
                ×
              </button>
            </span>
          ))}
          <span class="cond-input">
            <input
              id="s-cond-input"
              autocomplete="off"
              spellcheck={false}
              aria-label="条件"
              value={input}
              onInput={(e) => {
                setInput(e.currentTarget.value);
                setOpen(true);
                setPick(-1);
              }}
              onFocus={() => setOpen(true)}
              onBlur={() => setOpen(false)}
              onKeyDown={onKey}
            />
            {hints.length > 0 && (
              <ul class="hints" id="s-hints" role="listbox">
                {hints.map((h, i) => (
                  <li
                    role="option"
                    aria-selected={i === pick}
                    class={h.kind}
                    data-verbatim={h.kind === "field" || h.kind === "value"}
                    // mousedown 先于输入框的 blur：在这里选，免得提示先被收起
                    onMouseDown={(e) => {
                      e.preventDefault();
                      choose(h);
                    }}
                  >
                    {hintText(h)}
                  </li>
                ))}
              </ul>
            )}
          </span>
        </div>
      ) : (
        <textarea
          id="s-sql"
          spellcheck={false}
          aria-label="SQL"
          value={p.sql}
          onInput={(e) => p.onSql(e.currentTarget.value, e.currentTarget.selectionStart)}
          onClick={cursorOf}
          onKeyUp={cursorOf}
          onSelect={cursorOf}
        />
      )}
      {p.confirming && (
        <div class="confirm" id="s-confirm">
          <span>切回条件会放弃对 SQL 的修改</span>
          <button type="button" id="s-confirm-discard" onClick={() => p.onConfirm(true)}>
            放弃修改并切回
          </button>
          <button type="button" id="s-confirm-stay" onClick={() => p.onConfirm(false)}>
            留在 SQL
          </button>
        </div>
      )}
    </div>
  );
}
