// 某屏的结果区：慢查询的已用秒数与「取消」、出错时各条、取消提示、「时刻格式无效」、「无结果」与本页上限
// （REQ-AMB-UI-CONSOLE 取值、慢查询与取消、出错时三组）。屏只管画表格。

import type { ComponentChildren } from "preact";
import { useEffect, useState } from "preact/hooks";
import type { Hit, Json } from "../lib/json.ts";
import { BAD_TIME, CANCELLED, TOO_MANY } from "./errors.ts";
import { shownView, type ResultRequest, type ResultView } from "./requests.ts";
import { ResultState, type ResultStateContent } from "./ResultState.tsx";

const SLOW_MS = 3000;

/** `on` 为真时每 250 毫秒重画一次。 */
function useTicking(on: boolean): number {
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    if (!on) return undefined;
    setNow(Date.now());
    const t = setInterval(() => setNow(Date.now()), 250);
    return () => clearInterval(t);
  }, [on]);
  return now;
}

function Lines({ lines }: { lines: string[] }) {
  return (
    <div class="message" role="status">
      {lines.map((l) => (
        <p>{l}</p>
      ))}
    </div>
  );
}

export interface ResultAreaProps {
  req: ResultRequest;
  /** 屏的 id 前缀（`s`、`l`、`r`）：结果区为 `#<p>-result`，「取消」为 `#<p>-cancel`。 */
  prefix: string;
  initial: ResultStateContent;
  emptyDescription: string;
  render: (hits: Hit[], size: number, body: Map<string, Json>) => ComponentChildren;
  /** 为真时结果不按「无结果」与本页上限呈现（检索屏显示上下文结果时，REQ-AMB-UI-SEARCH-V2-CONTEXT-AS-RESULT）。 */
  plain?: boolean;
}

export function ResultArea({ req, prefix, initial, emptyDescription, render, plain = false }: ResultAreaProps) {
  const v = req.view;
  const now = useTicking(v.kind === "running" || v.kind === "busy");
  // 出错或取消的文字之下保留的原结果
  const withKept = (lines: ComponentChildren, keep: ResultView | undefined): ComponentChildren =>
    keep === undefined ? lines : (
      <>
        {lines}
        {body(keep)}
      </>
    );
  const body = (view: ResultView): ComponentChildren => {
    switch (view.kind) {
      case "idle":
        return <ResultState kind="idle" {...initial} />;
      case "running":
        return null;
      case "ok":
        if (plain) return render(view.hits, view.size, view.body);
        if (view.hits.length === 0) {
          return <ResultState kind="empty" title="未找到匹配结果" description={emptyDescription} />;
        }
        return (
          <>
            {view.hits.length === view.size && <p class="limit">{"结果已达本页上限 " + view.size + " 行"}</p>}
            {render(view.hits, view.size, view.body)}
          </>
        );
      case "error":
        return withKept(<Lines lines={view.lines} />, view.keep);
      case "busy": {
        const left = Math.max(0, Math.ceil((view.until - now) / 1000));
        return withKept(<Lines lines={left > 0 ? [TOO_MANY, left + " 秒"] : [TOO_MANY]} />, view.keep);
      }
      case "cancelled":
        return withKept(<Lines lines={[CANCELLED]} />, view.keep);
      case "invalidTime":
        return <Lines lines={[BAD_TIME]} />;
    }
  };
  const elapsed = v.kind === "running" ? now - v.since : 0;
  const prev = v.kind === "running" ? shownView(v.prev) : null;
  const loading = prev !== null && !(prev.kind === "ok" && (plain || prev.hits.length > 0));
  return (
    <div class="result" id={prefix + "-result"} aria-busy={v.kind === "running"}>
      {v.kind === "running" && elapsed >= SLOW_MS && (
        <div class="slow">
          <span>{"已用 " + Math.floor(elapsed / 1000) + " 秒"}</span>
          <button type="button" id={prefix + "-cancel"} onClick={() => req.cancel()}>
            取消
          </button>
        </div>
      )}
      {v.kind === "running" ? (
        loading ? (
          <ResultState kind="loading" title="正在查询" description="正在获取查询结果，请稍候。" />
        ) : (
          <div class="stale">{body(v.prev)}</div>
        )
      ) : (
        body(v)
      )}
    </div>
  );
}
