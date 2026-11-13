// 检索屏的条件框（REQ-AMB-UI-SEARCH-V2-MODES、FIELD-COND、TEXT-COND、COND-REMOVE、STAGED、INPUT-HINTS、TO-SQL、TO-COND）：
// 条件模式下每个条件是一个胶囊，SQL 模式下换成 SQL 文本框。

import { SqlEditor, type SqlEditorProps } from './SqlEditor.tsx';
import { Input } from "../../components/ui/Input.tsx";
import type { ComponentChildren as ReactNode, TargetedKeyboardEvent as KeyboardEvent } from "preact";
import { useState } from "preact/hooks";
import type { ValueRow } from "./FieldsBar.tsx";
import { conditionDraft, conditionHints, type ConditionHint } from "./conditionInput.ts";
import { equalitySql } from "./equality.ts";
import type { Condition } from "./sql.ts";

export interface Chip {
  id: number;
  cond: Condition;
}

export function chipText(c: Condition): string {
  if (c.kind === "set") return equalitySql(c);
  if (c.kind === "text") return "全文 " + c.text;
  return c.op === "IS NULL" || c.op === "IS NOT NULL" ? c.field + " " + c.op : c.field + " " + c.op + " " + c.value;
}

export interface CondBoxProps {
  toolbar?: ReactNode;
  sqlMetadata: Pick<SqlEditorProps, "datasets" | "fields" | "loading" | "errors" | "onRetry">;
  mode: "cond" | "sql";
  chips: readonly Chip[];
  /** 已生效的条件（上一次发起检索时条件框中的那些）。 */
  applied: ReadonlySet<number>;
  columns: readonly string[];
  /** 字段栏已为该字段列出的取值（取值尚未收起时），没有时为 null。 */
  valuesOf: (field: string) => readonly ValueRow[] | null;
  input: string;
  error: string | null;
  onInput: (text: string) => void;
  onError: (error: string) => void;
  sql: string;
  onAdd: (c: Condition) => void;
  onRemove: (id: number) => void;
  onMode: (m: "cond" | "sql") => void;
  onSql: (text: string, cursor: number, inputType?: string) => void;
  onCursor: (cursor: number) => void;
}

export function CondBox(p: CondBoxProps) {
  const input = p.input;
  const [open, setOpen] = useState(false);
  const [pick, setPick] = useState(-1);
  const hints = open && p.mode === "cond" ? conditionHints(input, p.columns, p.valuesOf) : [];
  const guidance = open && hints.length === 0 && p.error === null ? conditionDraft(input, p.columns).error : null;

  const commit = (c: Condition | null): void => {
    if (c === null) return;
    p.onAdd(c);
    p.onInput("");
    setOpen(false);
    setPick(-1);
  };
  const choose = (h: ConditionHint): void => {
    if ("condition" in h) commit(h.condition);
    else {
      // 空值运算不再需要额外输入，选择后直接添加。
      const draft = conditionDraft(h.fill, p.columns);
      if (h.kind === "op" && draft.condition?.kind === "field") commit(draft.condition);
      else p.onInput(h.fill);
      setPick(-1);
    }
  };
  const onKey = (e: KeyboardEvent<HTMLInputElement>): void => {
    if (e.isComposing || e.keyCode === 229) return;
    if (!open && (e.key === "ArrowDown" || e.key === "ArrowUp")) {
      const available = conditionHints(input, p.columns, p.valuesOf);
      if (available.length > 0) {
        e.preventDefault();
        setOpen(true);
        setPick(e.key === "ArrowDown" ? 0 : available.length - 1);
      }
      return;
    }
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
      else {
        const draft = conditionDraft(input, p.columns);
        if (draft.error !== null) p.onError(draft.error);
        else commit(draft.condition);
      }
    } else if (e.key === "Backspace" && input === "" && p.chips.length > 0) {
      p.onRemove(p.chips[p.chips.length - 1]!.id);
    }
  };

  return (
    <div className="condbox">
      <div className="query-header">
        <span className="modes">
          <button type="button" id="s-mode-cond" aria-pressed={p.mode === "cond"} onClick={() => p.onMode("cond")}>
            条件
          </button>
          <button type="button" id="s-mode-sql" aria-pressed={p.mode === "sql"} onClick={() => p.onMode("sql")}>
            SQL
          </button>
        </span>
        <div className="query-actions">{p.toolbar}</div>
      </div>
      {p.mode === "cond" ? (
        <div className="condition-editor">
          <div className={p.error === null ? "chips" : "chips invalid"} id="s-conds">
            {p.chips.map((c, index) => (
              <span key={c.id} className={p.applied.has(c.id) ? "chip" : "chip staged"} data-staged={!p.applied.has(c.id)}>
                <span className="chip-text" data-verbatim title={chipText(c.cond)}>{index > 0 ? "AND · " : ""}{chipText(c.cond)}</span>
                <button type="button" className="chip-remove" aria-label="删除条件" onClick={() => p.onRemove(c.id)}>
                  ×
                </button>
              </span>
            ))}
            <span className="cond-input">
              <Input
                variant="embedded"
                id="s-cond-input"
                autoComplete="off"
                spellcheck={false}
                aria-label="条件"
                role="combobox"
                aria-autocomplete="list"
                aria-expanded={hints.length > 0}
                aria-controls={hints.length > 0 ? "s-hints" : undefined}
                aria-activedescendant={hints[pick] !== undefined ? "s-hint-" + pick : undefined}
                aria-invalid={p.error !== null}
                aria-describedby={p.error !== null ? "s-cond-error" : guidance !== null ? "s-cond-guide" : undefined}
                placeholder="输入关键词或字段条件，回车添加或直接查询"
                value={input}
                onInput={(e) => {
                  p.onInput(e.currentTarget.value);
                  setOpen(true);
                  setPick(-1);
                }}
                onFocus={() => {
                  setOpen(true);
                  setPick(-1);
                }}
                onBlur={() => setOpen(false)}
                onKeyDown={onKey}
              />
              {hints.length > 0 && (
                <ul className="hints" id="s-hints" role="listbox">
                  {hints.map((h, i) => (
                    <li
                      key={h.kind + ":" + h.text}
                      id={"s-hint-" + i}
                      role="option"
                      aria-selected={i === pick}
                      className={h.kind}
                      data-verbatim={h.kind === "field" || h.kind === "value"}
                      // mousedown 先于输入框的 blur：在这里选，免得提示先被收起
                      onMouseDown={(e) => {
                        e.preventDefault();
                        choose(h);
                      }}
                    >
                      {h.text}
                    </li>
                  ))}
                </ul>
              )}
            </span>
          </div>
          {p.error !== null && <span className="cond-error" id="s-cond-error" role="alert">{p.error}</span>}
          {guidance !== null && <span className="cond-guide" id="s-cond-guide">{guidance}</span>}
        </div>
      ) : (
        <SqlEditor sql={p.sql} onSql={p.onSql} onCursor={p.onCursor} {...p.sqlMetadata} />
      )}
      {p.mode === "sql" && p.error !== null && <p className="cond-error" role="alert">{p.error}</p>}
    </div>
  );
}
