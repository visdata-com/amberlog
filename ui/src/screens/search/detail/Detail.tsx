// 单条详情（REQ-AMB-UI-SEARCH 单条详情一组）：只读检索屏已到手的本页结果，不发请求；字段页（分组、筛选、「加入」）与原始
// JSON 页；↑↓ 换行，到本页两端停住、不自动翻页；Esc 关闭。浮层的根元素为 `#s-detail`。

import { useLayoutEffect, useRef, useState } from "preact/hooks";
import { intOf, rawText, type Hit, type Json } from "../../../lib/json.ts";
import { useStore } from "../../../lib/store.ts";
import { formatLocal, formatUtc } from "../../../lib/time.ts";
import { settings } from "../../../shell/settings.ts";
import { type Condition, detailCondition } from "../sql.ts";
import { Value } from "../Table.tsx";
import { addValue, groupFields, matches, prettyJson } from "./fields.ts";
import "./detail.css";

const FIRST = "已是本页第一条";
const LAST = "已是本页最后一条，下一页请关闭后翻页";

export interface DetailProps {
  /** 本页结果与当前行的下标。 */
  hits: readonly Hit[];
  index: number;
  onIndex: (i: number) => void;
  /** 按时刻显示的键（同结果表）。 */
  timeKeys: ReadonlySet<string>;
  /** 已生效的全文条件的文本（标出用）。 */
  textTerms: readonly string[];
  /** 「= 加入」「≠ 排除」：条件模式下加入未生效的条件，SQL 模式下插到光标处，由检索屏按模式处理。 */
  onAdd: (c: Condition) => void;
  /** 有未生效的改动（显示「关闭并查询」）。 */
  pending: boolean;
  onClose: () => void;
  onCloseAndQuery: () => void;
  /** 提供「看前后记录」时的处理（参数为是否勾选「保留当前条件」）；不提供时为 undefined（REQ-AMB-UI-SEARCH-V2-CONTEXT-ENTRY）。 */
  onContext?: ((keepConditions: boolean) => void) | undefined;
  /** 到本页第一行、最后一行时的提示；缺省为 DETAIL-NAV 的两句（上下文结果另用 CONTEXT-FIXED 的两句）。 */
  edges?: readonly [string, string] | undefined;
}

/** 键盘事件发自输入框时，↑↓ 留给输入框。 */
const inInput = (e: KeyboardEvent): boolean => {
  const t = e.target as HTMLElement | null;
  return t !== null && (t.tagName === "INPUT" || t.tagName === "TEXTAREA");
};

export function Detail(p: DetailProps) {
  const { timezone } = useStore(settings);
  const [page, setPage] = useState<"fields" | "json">("fields");
  const [filter, setFilter] = useState("");
  const [edge, setEdge] = useState<string | null>(null);
  const [keepConditions, setKeepConditions] = useState(false);
  const [first, last] = p.edges ?? [FIRST, LAST];
  const n = p.hits.length;
  const hit = p.hits[p.index];

  const move = (d: -1 | 1): void => {
    const i = p.index + d;
    if (i < 0) setEdge(first);
    else if (i >= n) setEdge(last);
    else {
      setEdge(null);
      p.onIndex(i);
    }
  };

  // 换到别的行（含在结果表里点了另一行）时去掉到头的提示；只在行号真变了时清，打开时不清
  const shownIndex = useRef(p.index);
  useLayoutEffect(() => {
    if (shownIndex.current === p.index) return;
    shownIndex.current = p.index;
    setEdge(null);
  }, [p.index]);

  // 键盘：渲染提交后同步挂上（浮层一出现就能收到按键），只挂一次，经 ref 取此刻的处理
  const keys = useRef({ move, onClose: p.onClose });
  keys.current = { move, onClose: p.onClose };
  useLayoutEffect(() => {
    const onKey = (e: KeyboardEvent): void => {
      if (e.key === "Escape") {
        e.preventDefault();
        keys.current.onClose();
      } else if ((e.key === "ArrowUp" || e.key === "ArrowDown") && !inInput(e)) {
        e.preventDefault();
        keys.current.move(e.key === "ArrowUp" ? -1 : 1);
      }
    };
    document.addEventListener("keydown", onKey);
    return () => document.removeEventListener("keydown", onKey);
  }, []);

  if (hit === undefined) return null;
  const ts = intOf(hit.get("_timestamp"));
  const shown = (k: string, v: Json | undefined): string => {
    const us = p.timeKeys.has(k) ? intOf(v) : null;
    return us !== null ? formatLocal(us, timezone) : rawText(v);
  };

  return (
    <div id="s-detail" class="detail" role="dialog" aria-label="单条详情" tabIndex={-1}>
      <header class="detail-head">
        <span id="s-detail-pos">{"第 " + (p.index + 1) + " / " + n + " 行"}</span>
        {ts !== null && (
          <span class="detail-time">
            <span id="s-detail-time" class="time">{formatLocal(ts, timezone)}</span>
            <span id="s-detail-utc" class="time">{formatUtc(ts)}</span>
          </span>
        )}
        <span class="spacer" />
        <button type="button" id="s-detail-prev" aria-label="上一条" onClick={() => move(-1)}>
          ↑
        </button>
        <button type="button" id="s-detail-next" aria-label="下一条" onClick={() => move(1)}>
          ↓
        </button>
        {p.pending && (
          <button type="button" id="s-detail-close-query" class="primary" onClick={p.onCloseAndQuery}>
            关闭并查询
          </button>
        )}
        <button type="button" id="s-detail-close" onClick={p.onClose}>
          关闭
        </button>
      </header>
      {p.onContext !== undefined && (
        <div class="detail-context">
          <button type="button" id="s-detail-context" onClick={() => p.onContext?.(keepConditions)}>
            看前后记录
          </button>
          <label class="field">
            <input
              type="checkbox"
              id="s-detail-keep"
              checked={keepConditions}
              onChange={(e) => setKeepConditions(e.currentTarget.checked)}
            />
            保留当前条件
          </label>
        </div>
      )}
      {edge !== null && (
        <p class="hint" id="s-detail-edge" role="status">
          {edge}
        </p>
      )}
      <div class="detail-tabs" role="tablist">
        <button
          type="button"
          role="tab"
          id="s-detail-tab-fields"
          aria-selected={page === "fields"}
          onClick={() => setPage("fields")}
        >
          字段
        </button>
        <button
          type="button"
          role="tab"
          id="s-detail-tab-json"
          aria-selected={page === "json"}
          onClick={() => setPage("json")}
        >
          原始 JSON
        </button>
        {page === "fields" && (
          <input
            id="s-detail-filter"
            class="detail-filter"
            autocomplete="off"
            aria-label="筛选字段"
            placeholder="筛选"
            value={filter}
            onInput={(e) => setFilter(e.currentTarget.value)}
          />
        )}
      </div>
      {page === "fields" ? (
        <table class="detail-fields" id="s-detail-fields">
          {groupFields(hit.keys()).map((g) => {
            const keys = g.keys.filter((k) => matches(filter, k, shown(k, hit.get(k))));
            if (keys.length === 0) return null;
            return (
              <tbody class="group" data-group={g.name ?? ""}>
                {g.name !== null && (
                  <tr class="group-name">
                    <th colSpan={3}>{g.name}</th>
                  </tr>
                )}
                {keys.map((k) => {
                  const v = hit.get(k) as Json;
                  const add = (negate: boolean): void => p.onAdd(detailCondition(k, negate, addValue(v)));
                  return (
                    <tr data-key={k}>
                      <th class="key" data-verbatim>
                        {k}
                      </th>
                      <td class="value" title={shown(k, v)} data-verbatim>
                        <Value v={v} time={p.timeKeys.has(k)} terms={p.textTerms} />
                      </td>
                      <td class="adds">
                        <button type="button" class="add-eq" onClick={() => add(false)}>
                          = 加入
                        </button>
                        <button type="button" class="add-ne" onClick={() => add(true)}>
                          ≠ 排除
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            );
          })}
        </table>
      ) : (
        <pre class="detail-json" id="s-detail-json" data-verbatim>
          {prettyJson(hit)}
        </pre>
      )}
    </div>
  );
}
