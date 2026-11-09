// 每屏一组数据请求（REQ-AMB-UI-CONSOLE-TERM：结果请求与附属请求）：慢查询与取消、429 倒计时、出错呈现只对结果请求；
// 「取消」中止本屏尚未得到响应的全部数据请求（含附属请求）。检索屏的上下文请求（REQ-AMB-UI-SEARCH-V2 上下文浏览一组）按结果请求
// 呈现，失败或被取消时结果区在出错或取消的文字之下保留原来的结果（`keep`）。

import { useEffect, useMemo, useState } from "preact/hooks";
import type { Hit, Json } from "../lib/json.ts";
import { getFields, postSearch, type FieldsOutcome, type Outcome, type Query } from "./api.ts";
import { errorLines, isTooMany, retrySeconds } from "./errors.ts";

export type ResultView =
  | { kind: "idle" }
  | { kind: "running"; since: number; prev: ResultView }
  | { kind: "ok"; hits: Hit[]; size: number; body: Map<string, Json> }
  | { kind: "error"; lines: string[]; keep?: ResultView }
  | { kind: "busy"; until: number; keep?: ResultView }
  | { kind: "cancelled"; keep?: ResultView }
  | { kind: "invalidTime" };

/** 视图中正在显示的结果：等应答时为上一次的视图，出错或取消而保留了原结果时为原结果。 */
export function shownView(v: ResultView): ResultView {
  if (v.kind === "running") return shownView(v.prev);
  if ((v.kind === "error" || v.kind === "busy" || v.kind === "cancelled") && v.keep !== undefined) return v.keep;
  return v;
}

/** 「取消」的回调参数：`context` 为真表示取消的是上下文请求。 */
export interface CancelEvent {
  context: boolean;
}

export class ResultRequest {
  view: ResultView = { kind: "idle" };
  private ctl = new AbortController();
  private resultSeq = 0;
  // 每个附属请求槽：最新请求的编号，与中止该槽在途请求用的控制器
  private slots = new Map<string, { seq: number; ctl: AbortController }>();
  private busyUntil = 0;
  private busyTimer: ReturnType<typeof setTimeout> | undefined;
  private readonly subs = new Set<() => void>();
  private readonly cancelSubs = new Set<(e: CancelEvent) => void>();
  /** 在途的上下文请求的中止器；没有时为 null。 */
  private ctxCtl: AbortController | null = null;
  /** 结果区正在等的是上下文请求。 */
  private runningContext = false;

  /** 结果请求已发出、尚未得到响应。 */
  get running(): boolean {
    return this.view.kind === "running";
  }

  /** ERR-429 倒计时未结束：该屏的「查询」不可用。 */
  get queryDisabled(): boolean {
    return Date.now() < this.busyUntil;
  }

  /** 发结果请求。返回 null 表示已被取消，或已被本屏更晚的结果请求取代（晚到的应答丢弃）。 */
  async result(q: Query): Promise<Outcome | null> {
    this.dropContext();
    const seq = ++this.resultSeq;
    const prev = this.view.kind === "running" ? this.view.prev : this.view;
    this.setView({ kind: "running", since: Date.now(), prev });
    const o = await postSearch(q, this.ctl.signal);
    if (seq !== this.resultSeq || o.kind === "aborted") return null;
    if (o.kind === "ok") this.setView({ kind: "ok", hits: o.hits, size: q.size, body: o.body });
    else this.fail(o, undefined);
    return o;
  }

  /** 结果请求失败的呈现：429 倒计时，其余按出错各条；`keep` 为出错文字之下保留的原结果。 */
  private fail(o: Outcome, keep: ResultView | undefined): void {
    if (isTooMany(o)) {
      this.busyUntil = Date.now() + retrySeconds(o) * 1000;
      clearTimeout(this.busyTimer);
      this.busyTimer = setTimeout(() => this.notify(), this.busyUntil - Date.now());
      this.setView(keep === undefined ? { kind: "busy", until: this.busyUntil } : { kind: "busy", until: this.busyUntil, keep });
    } else {
      const lines = errorLines(o);
      this.setView(keep === undefined ? { kind: "error", lines } : { kind: "error", lines, keep });
    }
  }

  /**
   * 同时发出几个上下文请求，按一个结果请求呈现（慢查询与取消、429 倒计时、出错）；先得到的失败即呈现，并中止其余在途的。
   * 都成功时结果区回到原来的结果、返回各应答（顺序同 `qs`）；失败、被取消，或已被本屏的结果请求、`dropContext` 取代时
   * 返回 null，晚到的应答丢弃。
   */
  async context(qs: readonly Query[]): Promise<Outcome[] | null> {
    this.dropContext();
    const seq = ++this.resultSeq;
    const ctl = new AbortController();
    this.ctxCtl = ctl;
    const keep = shownView(this.view);
    this.setView({ kind: "running", since: Date.now(), prev: keep });
    this.runningContext = true;
    let failed: Outcome | null = null;
    const outs = await Promise.all(
      qs.map((q) =>
        postSearch(q, ctl.signal).then((o) => {
          if (failed === null && o.kind !== "ok" && o.kind !== "aborted") {
            failed = o;
            ctl.abort();
          }
          return o;
        }),
      ),
    );
    if (this.ctxCtl === ctl) this.ctxCtl = null;
    if (seq !== this.resultSeq) return null;
    this.runningContext = false;
    if (failed !== null) {
      this.fail(failed, keep);
      return null;
    }
    if (outs.some((o) => o.kind !== "ok")) return null;
    this.setView(keep);
    return outs;
  }

  /** 中止在途的上下文请求，晚到的应答丢弃；结果区回到原来的结果（CONTEXT-STALE）。 */
  dropContext(): void {
    if (this.ctxCtl === null) return;
    this.ctxCtl.abort();
    this.ctxCtl = null;
    if (this.runningContext && this.view.kind === "running") {
      this.resultSeq++;
      this.runningContext = false;
      this.setView(this.view.prev);
    }
  }

  /**
   * 发附属请求：与结果请求同属本屏、一起被「取消」中止，但不改结果区、不倒计时、不使「查询」不可用。
   * 返回 null 表示已被取消，或已被同一 `slot` 更晚的附属请求取代。
   */
  ancillary(q: Query, slot: string): Promise<Outcome | null> {
    return this.send(slot, (signal) => postSearch(q, signal), false);
  }

  /** 发字段列表请求（附属请求）：同一 `slot` 更晚的调用中止并取代它；被中止或被取代时返回 null。 */
  fields(stream: string, slot: string): Promise<FieldsOutcome | null> {
    return this.inSlot(slot, (signal) => getFields(stream, signal));
  }

  /**
   * 在 `slot` 中发一个附属请求（`run` 用给它的 `signal` 发请求）：同一 `slot` 更晚的调用中止并取代它，
   * 「取消」与 `dropSlot` 也中止它；被中止或被取代时返回 null。
   */
  inSlot<T extends { kind: string }>(slot: string, run: (signal: AbortSignal) => Promise<T>): Promise<T | null> {
    return this.send(slot, run, true);
  }

  // `abortPrev` 为假时被取代的请求不中止，只丢弃其响应
  private async send<T extends { kind: string }>(
    slot: string,
    run: (signal: AbortSignal) => Promise<T>,
    abortPrev: boolean,
  ): Promise<T | null> {
    const prev = this.slots.get(slot);
    if (abortPrev) prev?.ctl.abort();
    const ctl = prev !== undefined && !abortPrev ? prev.ctl : new AbortController();
    const seq = (prev?.seq ?? 0) + 1;
    this.slots.set(slot, { seq, ctl });
    const o = await run(ctl.signal);
    if (this.slots.get(slot)?.seq !== seq || o.kind === "aborted") return null;
    return o;
  }

  /** 中止 `slot` 中尚未得到响应的附属请求，其后到的响应丢弃。 */
  dropSlot(slot: string): void {
    const cur = this.slots.get(slot);
    if (cur === undefined) return;
    cur.ctl.abort();
    this.slots.set(slot, { seq: cur.seq + 1, ctl: new AbortController() });
  }

  /** 起止框有一框为空或写法不合：结果区显示「时刻格式无效」，不发请求（TIME-INPUT-INVALID）。 */
  invalidTime(): void {
    this.resultSeq++;
    this.setView({ kind: "invalidTime" });
  }

  /** 用户点「取消」：中止本屏尚未得到响应的全部数据请求，结果区显示取消提示（CANCEL）。 */
  cancel(): void {
    const context = this.runningContext && this.view.kind === "running";
    const keep = shownView(this.view);
    this.ctl.abort();
    this.ctl = new AbortController();
    this.ctxCtl?.abort();
    this.ctxCtl = null;
    this.runningContext = false;
    this.resultSeq++;
    for (const k of [...this.slots.keys()]) this.dropSlot(k);
    // 取消上下文请求时结果表保持原来的结果（REQ-AMB-UI-SEARCH-V2-CONTEXT-FAIL）
    this.setView(context ? { kind: "cancelled", keep } : { kind: "cancelled" });
    for (const f of [...this.cancelSubs]) f({ context });
  }

  /** 用户点「取消」时调 `f`。 */
  onCancel(f: (e: CancelEvent) => void): () => void {
    this.cancelSubs.add(f);
    return () => this.cancelSubs.delete(f);
  }

  subscribe(f: () => void): () => void {
    this.subs.add(f);
    return () => this.subs.delete(f);
  }

  private setView(v: ResultView): void {
    this.view = v;
    this.notify();
  }

  private notify(): void {
    for (const f of [...this.subs]) f();
  }
}

/** 本屏的一组数据请求；视图变化时重画调用它的组件。 */
export function useResultRequest(): ResultRequest {
  const r = useMemo(() => new ResultRequest(), []);
  const [, tick] = useState(0);
  useEffect(() => r.subscribe(() => tick((n) => n + 1)), [r]);
  return r;
}
