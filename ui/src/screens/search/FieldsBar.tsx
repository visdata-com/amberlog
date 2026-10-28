// 字段栏（REQ-AMB-UI-SEARCH-V2 字段栏与取值提示一组）：当前数据集的字段按前缀分组、带类型标记、可筛选与收起；条件模式下展开字段列出取值，
// 以「=」「≠」加入条件。状态与请求在 SearchScreen，这里只画。

import { useState } from "preact/hooks";
import { type Field, groupFields } from "./fields.ts";

/** 一个取值：原始 JSON 文本、是否 JSON 字符串、显示的文字，与条数原文。 */
export interface ValueRow {
  json: string;
  isString: boolean;
  text: string;
  n: string;
}

export type Values = { kind: "loading" } | { kind: "ok"; rows: ValueRow[] } | { kind: "error"; lines: string[] };

/** 类型标记：四种类型与 `_timestamp` 各不相同（FIELDS-BAR）。 */
const MARKS: Readonly<Record<string, string>> = { Int64: "123", Float64: "1.5", Boolean: "✓", Utf8: "Aa" };
const markOf = (f: Field): string => (f.name === "_timestamp" ? "⏱" : (MARKS[f.type] ?? "?"));
const kindOf = (f: Field): string => (f.name === "_timestamp" ? "time" : f.type);

/** 「记录字段」组的键（其余组以组名为键）。 */
export const RECORD_GROUP = "";

export interface FieldsBarProps {
  /** 当前数据集的字段表；没有时为 null（不列出字段）。 */
  fields: readonly Field[] | null;
  /** 字段列表请求失败时字段栏显示的各行（FIELDS-FAIL）。 */
  error: readonly string[] | null;
  /** 展开着的组（键为组名，「记录字段」组为 RECORD_GROUP）。 */
  open: ReadonlySet<string>;
  onToggleGroup: (key: string) => void;
  /** 展开了取值的字段与它们的取值。 */
  values: ReadonlyMap<string, Values>;
  /** 此刻能不能展开字段的取值（VALUES-REQUEST 的前提）。 */
  canExpand: boolean;
  onToggleField: (field: string) => void;
  onAdd: (field: string, negate: boolean, v: ValueRow) => void;
  onCollapse: () => void;
}

export function FieldsBar(p: FieldsBarProps) {
  const [filter, setFilter] = useState("");
  const groups = p.fields === null ? [] : groupFields(p.fields, filter);
  return (
    <aside class="fields" id="s-fields">
      <div class="fields-head">
        <input
          id="s-fields-filter"
          autocomplete="off"
          spellcheck={false}
          aria-label="筛选字段"
          value={filter}
          onInput={(e) => setFilter(e.currentTarget.value)}
        />
        <button type="button" id="s-fields-toggle" aria-label="收起字段栏" onClick={p.onCollapse}>
          «
        </button>
      </div>
      {p.error !== null && (
        <div class="fields-error" id="s-fields-error">
          {p.error.map((l) => (
            <p>{l}</p>
          ))}
        </div>
      )}
      {groups.map((g) => {
        const key = g.name ?? RECORD_GROUP;
        // 筛选时组内匹配的字段照样列出，不论组是否收起（FIELDS-FILTER）
        const open = filter !== "" || p.open.has(key);
        return (
          <section class="fgroup" data-group={key}>
            <button type="button" class="fgroup-head" aria-expanded={open} onClick={() => p.onToggleGroup(key)}>
              <span class="fgroup-name">{g.name ?? "记录字段"}</span>
              <span class="fcount">{g.fields.length}</span>
            </button>
            {open &&
              g.fields.map((f) => {
                const v = p.values.get(f.name);
                const expandable = f.name !== "_timestamp" && (v !== undefined || p.canExpand);
                return (
                  <div class="field" data-field={f.name}>
                    <div class="field-row">
                      <span class="ftype" data-type={kindOf(f)}>
                        {markOf(f)}
                      </span>
                      <button
                        type="button"
                        class="fname"
                        data-verbatim
                        disabled={!expandable}
                        aria-expanded={v !== undefined}
                        onClick={() => p.onToggleField(f.name)}
                      >
                        {f.name}
                      </button>
                    </div>
                    {v?.kind === "error" && (
                      <div class="fvalues-error">
                        {v.lines.map((l) => (
                          <p>{l}</p>
                        ))}
                      </div>
                    )}
                    {v?.kind === "ok" && (
                      <ul class="fvalues">
                        {v.rows.map((r) => (
                          <li class="fvalue">
                            <span class="fv-text" data-verbatim title={r.text}>
                              {r.text}
                            </span>
                            <span class="fv-n" data-verbatim>{r.n}</span>
                            <button type="button" class="fv-eq" onClick={() => p.onAdd(f.name, false, r)}>
                              =
                            </button>
                            <button type="button" class="fv-ne" onClick={() => p.onAdd(f.name, true, r)}>
                              ≠
                            </button>
                          </li>
                        ))}
                      </ul>
                    )}
                  </div>
                );
              })}
          </section>
        );
      })}
    </aside>
  );
}
