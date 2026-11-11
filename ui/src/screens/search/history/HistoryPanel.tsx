// 查询历史的列表（REQ-AMB-UI-SEARCH-V2-HISTORY-LIST、HISTORY-RESTORE、HISTORY-CLEAR）：从新到旧列出，选一项交给检索屏恢复；
// 恢复会放弃 SQL 文本中未生效的修改时先请用户确认。

import { useRef, useState } from "preact/hooks";
import { useStore } from "../../../lib/store.ts";
import { Time } from "../../../shell/Time.tsx";
import { useVisibleViewport } from "../../../shell/useVisibleViewport.ts";
import { chipText } from "../CondBox.tsx";
import type { HistoryItem } from "./history.ts";
import { clearHistory, history } from "./store.ts";
import "./history.css";

export interface HistoryPanelProps {
  /** SQL 文本此刻有未生效的修改。 */
  sqlEdited: boolean;
  onRestore: (item: HistoryItem) => void;
}

export function HistoryPanel(p: HistoryPanelProps) {
  const { items } = useStore(history);
  const [open, setOpen] = useState(false);
  const [asking, setAsking] = useState<HistoryItem | null>(null);
  const trigger = useRef<HTMLButtonElement>(null);
  const viewportStyle = useVisibleViewport(open || asking !== null);

  const choose = (it: HistoryItem): void => {
    setOpen(false);
    if (p.sqlEdited) setAsking(it);
    else p.onRestore(it);
  };

  return (
    <span class="history">
      <button ref={trigger} type="button" id="s-history-open" aria-expanded={open} onClick={() => setOpen(!open)}>
        查询历史
      </button>
      {open && (
        <div class="history-panel" id="s-history" style={viewportStyle}>
          <div class="history-heading">
            <span>查询历史</span>
            <button type="button" aria-label="关闭查询历史" onClick={() => {
              setOpen(false);
              trigger.current?.focus({ preventScroll: true });
            }}>关闭</button>
          </div>
          <ol id="s-history-list">
            {items.map((it) => (
              <li>
                <button type="button" class="history-item" onClick={() => choose(it)}>
                  <Time us={it.at} />
                  {it.mode === "cond" ? (
                    <>
                      <span class="history-stream">{it.stream}</span>
                      {it.conds.map((c) => (
                        <span class="history-cond" data-verbatim>
                          {chipText(c)}
                        </span>
                      ))}
                    </>
                  ) : (
                    <code class="history-sql" data-verbatim>
                      {it.sql}
                    </code>
                  )}
                </button>
              </li>
            ))}
          </ol>
          <button type="button" id="s-history-clear" onClick={clearHistory}>
            清空查询历史
          </button>
        </div>
      )}
      {asking !== null && (
        <div class="confirm" id="s-history-confirm" style={viewportStyle}>
          <span>恢复会放弃对 SQL 的修改</span>
          <button
            type="button"
            id="s-history-confirm-restore"
            onClick={() => {
              setAsking(null);
              p.onRestore(asking);
            }}
          >
            放弃修改并恢复
          </button>
          <button type="button" id="s-history-confirm-keep" onClick={() => setAsking(null)}>
            不恢复
          </button>
        </div>
      )}
    </span>
  );
}
