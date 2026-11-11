import type { TargetedKeyboardEvent as ReactKeyboardEvent } from "preact";
import { useEffect, useLayoutEffect, useRef, useState } from "preact/hooks";
import { Button } from "../components/ui/Button.tsx";
import { Input } from "../components/ui/Input.tsx";
import { useStore } from "../lib/store.ts";
import { formatLocal, nowUs } from "../lib/time.ts";
import { settings } from "./settings.ts";
import type { TimeRange } from "./TimeRange.tsx";
import { calendarDays, draftValue, formatRangeTime, initDraft, moveDate, rangeSummary, relativePreview, selectDate, shiftMonth, weekDay, type RangeDraft } from "./time-range-picker.ts";
import "./time-range-picker.css";

const WEEKDAYS = ["日", "一", "二", "三", "四", "五", "六"];

/** 检索专用时间编辑器：只提交范围，不执行查询。其他两屏继续使用 TimeRangeInputs。 */
export function TimeRangePicker({ range, hidden, prefix }: { range: TimeRange; hidden: boolean; prefix: string }) {
  const model = useStore(range.store);
  const { timezone } = useStore(settings);
  const [draft, setDraft] = useState<RangeDraft | null>(null);
  const [expanded, setExpanded] = useState(false);
  const [month, setMonth] = useState("");
  const [focusDate, setFocusDate] = useState("");
  const [position, setPosition] = useState({ left: 16, top: 16, maxHeight: 600 });
  const trigger = useRef<HTMLButtonElement>(null);
  const panel = useRef<HTMLDivElement>(null);
  const grid = useRef<HTMLDivElement>(null);
  const relativeTab = useRef<HTMLButtonElement>(null);
  const absoluteTab = useRef<HTMLButtonElement>(null);
  const editing = useRef(false);
  const dateFocusRequested = useRef(false);
  const currentDraft = useRef<RangeDraft | null>(null);
  const writingRange = useRef(false);
  const snapshot = useRef({ model: range.model, timezone });
  const id = prefix + "-time";
  const open = expanded && draft !== null && !hidden;
  const summary = rangeSummary(model, range.choices, timezone);

  const close = (restoreFocus = false): void => {
    if (!editing.current) return;
    editing.current = false;
    setExpanded(false);
    if (restoreFocus && !hidden) trigger.current?.focus({ preventScroll: true });
  };
  const show = (): void => {
    if (editing.current) { close(true); return; }
    const next = currentDraft.current !== null && snapshot.current.model === range.model && snapshot.current.timezone === timezone
      ? currentDraft.current : initDraft(range.model, timezone, nowUs());
    const date = next.absolute.startDate ?? formatLocal(nowUs(), timezone).slice(0, 10);
    editing.current = true;
    setMonth(date);
    setFocusDate(date);
    currentDraft.current = next;
    snapshot.current = { model: range.model, timezone };
    setDraft(next);
    setExpanded(true);
  };
  const updateDraft = (change: (value: RangeDraft) => RangeDraft): void => {
    if (currentDraft.current === null) return;
    const next = change(currentDraft.current);
    currentDraft.current = next;
    setDraft(next);
    const value = draftValue(next, timezone);
    writingRange.current = true;
    try {
      if (value.kind === "quick") range.setQuick(value.us);
      else if (value.kind === "absolute") range.setRange(value.start, value.end);
      else range.store.set({ quick: null, start: null, end: null, startText: null, endText: null });
      snapshot.current = { model: range.model, timezone };
    } finally { writingRange.current = false; }
  };

  // 自身编辑同步更新范围但保留弹层；外部更新结束编辑，避免覆盖历史、缩放或跳转。
  useEffect(() => {
    const invalidate = (): void => { close(); currentDraft.current = null; setDraft(null); };
    const offRange = range.store.subscribe(() => { if (!writingRange.current) invalidate(); });
    let previousZone = settings.get().timezone;
    const offSettings = settings.subscribe(() => {
      const current = settings.get().timezone;
      if (current !== previousZone) { previousZone = current; invalidate(); }
    });
    return () => { offRange(); offSettings(); };
  }, [range, hidden]);
  useLayoutEffect(() => { if (hidden) close(); }, [hidden]);

  useLayoutEffect(() => {
    if (!open) return;
    (draft.mode === "relative" ? relativeTab : absoluteTab).current?.focus({ preventScroll: true });
  }, [open]);

  useLayoutEffect(() => {
    if (!open || !panel.current || !trigger.current) return;
    const update = (): void => {
      const p = panel.current, anchor = trigger.current;
      if (!p || !anchor) return;
      const v = window.visualViewport;
      const vx = v?.offsetLeft ?? 0, vy = v?.offsetTop ?? 0;
      const vw = v?.width ?? window.innerWidth, vh = v?.height ?? window.innerHeight;
      const rect = anchor.getBoundingClientRect();
      const maxHeight = Math.max(0, vh - 32);
      const height = Math.min(p.scrollHeight, maxHeight);
      const below = vy + vh - 16 - rect.bottom - 4;
      const above = rect.top - vy - 20;
      const proposedTop = below >= height || below >= above ? rect.bottom + 4 : rect.top - height - 4;
      const top = Math.max(vy + 16, Math.min(proposedTop, vy + vh - 16 - height));
      const left = Math.max(vx + 16, Math.min(rect.right - p.offsetWidth, vx + vw - 16 - p.offsetWidth));
      setPosition(old => old.left === left && old.top === top && old.maxHeight === maxHeight ? old : { left, top, maxHeight });
    };
    const outside = (event: PointerEvent): void => {
      if (event.target instanceof Node && !panel.current?.contains(event.target) && !trigger.current?.contains(event.target)) close();
    };
    const escape = (event: KeyboardEvent): void => {
      if (event.key === "Escape" && !event.isComposing) {
        event.preventDefault();
        event.stopImmediatePropagation();
        close(true);
      }
    };
    update();
    const observer = new ResizeObserver(update);
    observer.observe(panel.current);
    observer.observe(trigger.current);
    window.addEventListener("resize", update);
    window.addEventListener("scroll", update, true);
    window.visualViewport?.addEventListener("resize", update);
    window.visualViewport?.addEventListener("scroll", update);
    document.addEventListener("pointerdown", outside, true);
    document.addEventListener("keydown", escape, true);
    return () => {
      observer.disconnect();
      window.removeEventListener("resize", update);
      window.removeEventListener("scroll", update, true);
      window.visualViewport?.removeEventListener("resize", update);
      window.visualViewport?.removeEventListener("scroll", update);
      document.removeEventListener("pointerdown", outside, true);
      document.removeEventListener("keydown", escape, true);
    };
  }, [open]);

  useLayoutEffect(() => {
    if (open && draft.mode === "absolute" && dateFocusRequested.current) {
      dateFocusRequested.current = false;
      const day = grid.current?.querySelector<HTMLButtonElement>(`[data-date="${focusDate}"]`);
      day?.focus({ preventScroll: true });
      day?.scrollIntoView({ block: "nearest", inline: "nearest" });
    }
  }, [open, draft?.mode, draft?.absolute, month, focusDate]);

  const switchMode = (mode: RangeDraft["mode"]): void => {
    updateDraft(d => ({ ...d, mode }));
  };
  const tabKey = (event: ReactKeyboardEvent<HTMLButtonElement>): void => {
    if (!["ArrowLeft", "ArrowRight", "Home", "End"].includes(event.key)) return;
    event.preventDefault(); event.stopPropagation();
    const mode = event.key === "Home" ? "relative" : event.key === "End" ? "absolute" : draft?.mode === "relative" ? "absolute" : "relative";
    switchMode(mode);
    (mode === "relative" ? relativeTab : absoluteTab).current?.focus();
  };
  const changeMonth = (delta: number): void => {
    const next = shiftMonth(focusDate, delta);
    setFocusDate(next); setMonth(next);
  };
  const dateKey = (event: ReactKeyboardEvent<HTMLButtonElement>): void => {
    let next: string;
    switch (event.key) {
      case "ArrowLeft": next = moveDate(focusDate, -1); break;
      case "ArrowRight": next = moveDate(focusDate, 1); break;
      case "ArrowUp": next = moveDate(focusDate, -7); break;
      case "ArrowDown": next = moveDate(focusDate, 7); break;
      case "Home": next = moveDate(focusDate, -weekDay(focusDate)); break;
      case "End": next = moveDate(focusDate, 6 - weekDay(focusDate)); break;
      case "PageUp": next = shiftMonth(focusDate, -1); break;
      case "PageDown": next = shiftMonth(focusDate, 1); break;
      default: return;
    }
    event.preventDefault(); event.stopPropagation();
    dateFocusRequested.current = true;
    setFocusDate(next); setMonth(next);
  };
  const chooseDate = (date: string): void => {
    dateFocusRequested.current = true;
    setFocusDate(date); setMonth(date);
    updateDraft(d => ({ ...d, absolute: { ...d.absolute, ...selectDate(d.absolute, date) } }));
  };
  const value = draft === null ? null : draftValue(draft, timezone);
  const preview = draft === null || value === null || value.kind === "invalid" ? null :
    value.kind === "absolute" ? [value.start, value.end] : relativePreview(value.us, draft.previewNow);
  const dates = month === "" ? [] : calendarDays(month);
  const today = formatLocal(nowUs(), timezone).slice(0, 10);

  return (
    <span className="time-range-picker">
      <Button id={id + "-trigger"} ref={trigger} className="time-range-summary" aria-haspopup="dialog" aria-expanded={open}
        aria-controls={open ? id + "-panel" : undefined} aria-describedby={id + "-description"} onClick={show}>
        <svg aria-hidden="true" width="16" height="16" viewBox="0 0 20 20" fill="none" stroke="currentColor" strokeWidth="1.5"><circle cx="10" cy="10" r="8" /><path d="M10 5v5l3 2" /></svg>
        <span>{summary.text}</span><span aria-hidden="true">{open ? "▴" : "▾"}</span>
      </Button>
      <span className="time-range-description" id={id + "-description"}>{summary.description}</span>
      {open && (
        <div id={id + "-panel"} ref={panel} className="time-range-panel" role="dialog" aria-labelledby={id + "-title"}
          style={{ left: position.left + "px", top: position.top + "px", maxHeight: position.maxHeight + "px" }}>
          <div className="time-range-heading"><span id={id + "-title"}>时间范围</span><Button aria-label="关闭时间范围" onClick={() => close(true)}>×</Button></div>
          <div className="time-range-tabs" role="tablist" aria-label="时间类型">
            {(["relative", "absolute"] as const).map(mode => (
              <Button key={mode} id={id + "-tab-" + mode} ref={mode === "relative" ? relativeTab : absoluteTab} role="tab" tabIndex={draft.mode === mode ? 0 : -1}
                aria-selected={draft.mode === mode} aria-controls={id + "-" + mode} onClick={() => switchMode(mode)} onKeyDown={tabKey}>
                {mode === "relative" ? "相对时间" : "绝对时间"}
              </Button>
            ))}
          </div>
          <div id={id + "-relative"} role="tabpanel" aria-labelledby={id + "-tab-relative"} hidden={draft.mode !== "relative"}>
            <div className="time-range-presets">
              {range.choices.map(choice => (
                <Button key={String(choice.us)} value={String(choice.us)} aria-pressed={draft.quick === choice.us} onClick={() => updateDraft(d => ({ ...d, quick: choice.us, previewNow: nowUs() }))}>
                  <span>{choice.label}</span><span aria-hidden="true">{draft.quick === choice.us ? "✓" : ""}</span>
                </Button>
              ))}
            </div>
          </div>
          <div id={id + "-absolute"} role="tabpanel" aria-labelledby={id + "-tab-absolute"} hidden={draft.mode !== "absolute"}>
            <div className="time-range-month">
              <Button aria-label="上个月" disabled={month.startsWith("0000-01")} onClick={() => changeMonth(-1)}>‹</Button>
              <span id={id + "-month"} aria-live="polite">{month.slice(0, 4)} 年 {Number(month.slice(5, 7))} 月</span>
              <Button aria-label="下个月" disabled={month.startsWith("9999-12")} onClick={() => changeMonth(1)}>›</Button>
            </div>
            <div ref={grid} role="grid" aria-labelledby={id + "-month"} aria-describedby={id + "-date-help"} className="time-range-calendar">
              <div role="row">{WEEKDAYS.map(day => <span key={day} role="columnheader">{day}</span>)}</div>
              {Array.from({ length: 6 }, (_, row) => (
                <div key={row} role="row">
                  {dates.slice(row * 7, row * 7 + 7).map((day, column) => {
                    const date = day.date;
                    const start = date !== null && date === draft.absolute.startDate;
                    const end = date !== null && date === draft.absolute.endDate;
                    const selected = date !== null && draft.absolute.startDate !== null && draft.absolute.endDate !== null && date >= draft.absolute.startDate && date <= draft.absolute.endDate;
                    return <span key={date ?? column} role="gridcell" aria-selected={start || end || selected}>
                      {date !== null && <Button variant={start || end ? "primary" : "secondary"}
                        className={["time-range-day", day.inMonth ? "" : "is-other", selected ? "is-range" : "", start || end ? "is-endpoint" : ""].join(" ")}
                        data-date={date} tabIndex={date === focusDate ? 0 : -1} aria-current={date === today ? "date" : undefined}
                        aria-label={date + (start ? "，开始日期" : "") + (end ? "，结束日期" : "") + (selected && !start && !end ? "，范围内" : "")}
                        onFocus={() => setFocusDate(date)} onKeyDown={dateKey} onClick={() => chooseDate(date)}>{day.day}</Button>}
                    </span>;
                  })}
                </div>
              ))}
            </div>
            <p className="time-range-note" id={id + "-date-help"} aria-live="polite">{draft.absolute.endDate === null && draft.absolute.startDate !== null ? "请选择结束日期；更早日期将重新选择开始。" : "选择开始和结束日期，支持同日及跨月范围。"}</p>
            <div className="time-range-times">
              {(["start", "end"] as const).map(which => {
                const error = value?.kind === "invalid" ? (which === "start" ? value.startError : value.endError) : undefined;
                return <label key={which}>
                  <span>{which === "start" ? "开始时间" : "结束时间"}</span>
                  <span className="time-range-date">{which === "start" ? draft.absolute.startDate ?? "未选择日期" : draft.absolute.endDate ?? "未选择日期"}</span>
                  <Input id={id + "-" + which} value={which === "start" ? draft.absolute.startTime : draft.absolute.endTime}
                    autoComplete="off" spellcheck={false} aria-invalid={Boolean(error)} aria-describedby={error ? id + "-error" : undefined}
                    onInput={event => { const text = event.currentTarget.value; updateDraft(d => ({ ...d, absolute: { ...d.absolute, [which + "Time"]: text } })); }} />
                </label>;
              })}
            </div>
          </div>
          <div className="time-range-preview">
            <span>预览范围</span>
            {preview ? <span className="time-range-preview-value">{formatRangeTime(preview[0]!, timezone)} → {formatRangeTime(preview[1]!, timezone)}</span> : <span>待完成有效时间范围</span>}
            {draft.mode === "relative" && <span className="time-range-note">相对时间以查询时刻为准，此预览不是已展示结果的范围。</span>}
          </div>
          {value?.kind === "invalid" && <p className="time-range-error" id={id + "-error"} role="status">{value.error}</p>}
          <div className="time-range-footer"><span className="time-range-zone">时区 <span>{timezone}</span></span>
          </div>
        </div>
      )}
    </span>
  );
}
