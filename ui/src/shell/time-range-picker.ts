// 检索屏时间范围弹层的纯逻辑（REQ-AMB-UI-SEARCH-V4-RANGE、REQ-AMB-UI-CONSOLE-V4-TIME-INPUT、TIME-SUMMARY）。
// 「绝对时间」页签的起止两框接受 TIME-INPUT 的全部写法（含 `T`、`Z` 与 `±HH:MM`），日历只用来填写框中的日期部分。
// 日历按 civil 日期计算，时区转换继续交给 lib/time。
import { DAY_US, HOUR_US, formatLocal, parseTimeInput, usToWall, wallToUs } from "../lib/time.ts";
import type { QuickChoice, RangeModel } from "./TimeRange.tsx";

const midnight = (y: number, mo: number, d: number): bigint => wallToUs({ y, mo, d, h: 0, mi: 0, s: 0, us: 0 });
const MIN_DAY = midnight(0, 1, 1) / DAY_US;
const MAX_DAY = midnight(9999, 12, 31) / DAY_US;
const pad = (n: number, width = 2): string => String(n).padStart(width, "0");
const iso = (day: bigint): string => {
  const w = usToWall(day * DAY_US);
  return `${pad(w.y, 4)}-${pad(w.mo)}-${pad(w.d)}`;
};
const dayOf = (date: string): bigint => {
  const [y, mo, d] = date.split("-").map(Number);
  return midnight(y!, mo!, d!) / DAY_US;
};
const clampDay = (d: bigint): bigint => d < MIN_DAY ? MIN_DAY : d > MAX_DAY ? MAX_DAY : d;

export interface CalendarDay { date: string | null; day: number; inMonth: boolean }
/** 「绝对时间」页签：起止两框的文字，与日历下一次点选填哪一框。 */
export interface AbsoluteDraft { startText: string; endText: string; pick: "start" | "end" }
export interface RangeDraft {
  mode: "relative" | "absolute";
  quick: bigint;
  previewNow: bigint;
  absolute: AbsoluteDraft;
  original: { absolute: AbsoluteDraft; start: bigint | null; end: bigint | null };
}
export type DraftValue =
  | { kind: "quick"; us: bigint }
  | { kind: "absolute"; start: bigint; end: bigint }
  | { kind: "invalid"; error: string; startError?: string; endError?: string };

/** 固定六周网格；超出四位年份范围的格子不可选择。 */
export function calendarDays(date: string): CalendarDay[] {
  const month = date.slice(0, 7);
  const first = dayOf(month + "-01");
  const weekday = Number(((first + 4n) % 7n + 7n) % 7n);
  return Array.from({ length: 42 }, (_, i) => {
    const day = first - BigInt(weekday) + BigInt(i);
    if (day < MIN_DAY || day > MAX_DAY) return { date: null, day: 0, inMonth: false };
    const value = iso(day);
    return { date: value, day: Number(value.slice(8)), inMonth: value.startsWith(month) };
  });
}

/** 按月份移动并夹紧月末，不依赖浏览器本地时区或 Date 的 0～99 年特殊处理。 */
export function shiftMonth(date: string, delta: number): string {
  const w = usToWall(dayOf(date) * DAY_US);
  const index = Math.max(0, Math.min(9999 * 12 + 11, w.y * 12 + w.mo - 1 + delta));
  const y = Math.floor(index / 12), mo = index % 12 + 1;
  const next = mo === 12 ? midnight(y + 1, 1, 1) : midnight(y, mo + 1, 1);
  const last = usToWall(next - DAY_US).d;
  return iso(midnight(y, mo, Math.min(w.d, last)) / DAY_US);
}
export const moveDate = (date: string, days: number): string => iso(clampDay(dayOf(date) + BigInt(days)));
export const weekDay = (date: string): number => Number(((dayOf(date) + 4n) % 7n + 7n) % 7n);

const DATE_PREFIX = /^(\d{4}-\d{2}-\d{2})(?=$|[ T])/;

/** 框中文字开头的日期（日历上存在的才算）；没有时为 null。 */
export function dateOfText(text: string): string | null {
  const m = DATE_PREFIX.exec(text.trim());
  return m !== null && parseTimeInput(m[1] + " 00:00", "UTC") !== null ? m[1]! : null;
}

/**
 * 日历点选日期：填进下一框的日期部分，框中原有的时刻部分保留（没有时起点取当天 0 点、止点取当天最后一微秒）。
 * 先填起点再填止点；止点早于起点时改为重选起点。
 */
export function chooseDate(a: AbsoluteDraft, date: string): AbsoluteDraft {
  const rest = (text: string, fallback: string): string =>
    DATE_PREFIX.test(text.trim()) && text.trim().length > 10 ? text.trim().slice(10) : fallback;
  const startDate = dateOfText(a.startText);
  if (a.pick === "start" || startDate === null || date < startDate) {
    return { ...a, startText: date + rest(a.startText, " 00:00:00.000000"), pick: "end" };
  }
  return { ...a, endText: date + rest(a.endText, " 23:59:59.999999"), pick: "start" };
}
export const relativePreview = (us: bigint, now: bigint): [bigint, bigint] => [now - us, now];
/** 时刻摘要（TIME-SUMMARY）：到秒；完整写法见 formatLocal。 */
export const formatRangeTime = (us: bigint, timezone: string): string => formatLocal(us, timezone).slice(0, 19);

/** 初始化仅计算快照，绝不调用会写回模型的 resolve/edit。 */
export function initDraft(model: RangeModel, timezone: string, now: bigint): RangeDraft {
  const [start, end] = model.quick === null ? [model.start, model.end] : relativePreview(model.quick, now);
  const local = (t: bigint | null): string => t === null ? "" : formatLocal(t, timezone);
  const absolute: AbsoluteDraft = { startText: local(start), endText: local(end), pick: "start" };
  return {
    mode: model.quick === null ? "absolute" : "relative", quick: model.quick ?? HOUR_US, previewNow: now,
    absolute,
    original: { absolute: { ...absolute }, start, end },
  };
}

export function draftValue(draft: RangeDraft, timezone: string): DraftValue {
  if (draft.mode === "relative") return { kind: "quick", us: draft.quick };
  const a = draft.absolute;
  // 未编辑的框保留原 UTC：框里显示的写法在夏令时回拨时段有两个解释，重开后不应改成较早的那个。
  const old = draft.original;
  const endpoint = (which: "start" | "end"): bigint | null => {
    const text = which === "start" ? a.startText : a.endText;
    const oldText = which === "start" ? old.absolute.startText : old.absolute.endText;
    const oldValue = old[which];
    return text === oldText && oldValue !== null ? oldValue : parseTimeInput(text, timezone);
  };
  const start = endpoint("start"), end = endpoint("end");
  if (start === null || end === null) {
    return {
      kind: "invalid", error: "请填写有效的开始和结束时刻",
      ...(start === null ? { startError: a.startText.trim() === "" ? "请填写开始时刻" : "开始时刻格式无效" } : {}),
      ...(end === null ? { endError: a.endText.trim() === "" ? "请填写结束时刻" : "结束时刻格式无效" } : {}),
    };
  }
  // 起点不早于止点同样不发请求（REQ-AMB-UI-CONSOLE-V4-TIME-INPUT-INVALID）
  if (start >= end) return { kind: "invalid", error: "结束时间必须晚于开始时间", endError: "结束时间必须晚于开始时间" };
  return { kind: "absolute", start, end };
}

/**
 * 弹层之外的摘要（RANGE）：起止时刻时显示该起止；选了快捷项、且已以它发出过检索请求时显示最近一次所取的起止，
 * 尚未发出时只显示快捷项的名字。`text` 是时刻摘要（到秒），`title` 是悬停可见的完整时刻（TIME-SUMMARY）。
 */
export function rangeSummary(model: RangeModel, choices: readonly QuickChoice[], timezone: string): { text: string; title: string | undefined; description: string } {
  const full = (a: bigint, b: bigint): string => formatLocal(a, timezone) + " → " + formatLocal(b, timezone);
  const short = (a: bigint, b: bigint): string => {
    const x = formatRangeTime(a, timezone), y = formatRangeTime(b, timezone);
    return x + " → " + (x.slice(0, 10) === y.slice(0, 10) ? y.slice(11) : y);
  };
  if (model.quick !== null) {
    const label = choices.find(c => c.us === model.quick)?.label ?? "相对时间";
    if (model.queried === true && model.start !== null && model.end !== null) {
      return { text: label + " · " + short(model.start, model.end), title: full(model.start, model.end), description: `${label}，最近一次查询取 ${full(model.start, model.end)}；时区 ${timezone}` };
    }
    return { text: label, title: undefined, description: `${label}；时区 ${timezone}；相对时间以查询时刻为准` };
  }
  if (model.start === null || model.end === null) return { text: "请选择时间范围", title: undefined, description: `时间范围无效；时区 ${timezone}` };
  return { text: short(model.start, model.end), title: full(model.start, model.end), description: `${full(model.start, model.end)}；时区 ${timezone}` };
}
