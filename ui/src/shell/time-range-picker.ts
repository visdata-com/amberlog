// 检索时间选择的纯逻辑。日历按 civil 日期计算，时区转换继续交给 lib/time。
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
export interface DateSelection { startDate: string | null; endDate: string | null }
export interface AbsoluteDraft extends DateSelection { startTime: string; endTime: string }
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

export function selectDate(selection: DateSelection, date: string): DateSelection {
  if (selection.startDate === null || selection.endDate !== null || date < selection.startDate) {
    return { startDate: date, endDate: null };
  }
  return { startDate: selection.startDate, endDate: date };
}
export const relativePreview = (us: bigint, now: bigint): [bigint, bigint] => [now - us, now];
export const formatRangeTime = (us: bigint, timezone: string): string => formatLocal(us, timezone).slice(0, 19);

/** 初始化仅计算快照，绝不调用会写回模型的 resolve/edit。 */
export function initDraft(model: RangeModel, timezone: string, now: bigint): RangeDraft {
  const [start, end] = model.quick === null ? [model.start, model.end] : relativePreview(model.quick, now);
  const local = (t: bigint | null): string => t === null ? "" : formatRangeTime(t, timezone);
  const a = local(start), b = local(end);
  const absolute = { startDate: a === "" ? null : a.slice(0, 10), endDate: b === "" ? null : b.slice(0, 10), startTime: a.slice(11), endTime: b.slice(11) };
  return {
    mode: model.quick === null ? "absolute" : "relative", quick: model.quick ?? HOUR_US, previewNow: now,
    absolute,
    original: { absolute: { ...absolute }, start, end },
  };
}

export function draftValue(draft: RangeDraft, timezone: string): DraftValue {
  if (draft.mode === "relative") return { kind: "quick", us: draft.quick };
  const a = draft.absolute;
  const time = /^\d{2}:\d{2}(?::\d{2}(?:\.\d{1,6})?)?$/;
  const parse = (date: string | null, text: string): bigint | null =>
    date !== null && time.test(text.trim()) ? parseTimeInput(date + " " + text.trim(), timezone) : null;
  // 未编辑端点保留原 UTC，避免回拨时段的较晚时刻在重开后被重新解释为较早时刻。
  const old = draft.original;
  const endpoint = (which: "start" | "end"): bigint | null => {
    const date = which === "start" ? a.startDate : a.endDate;
    const text = which === "start" ? a.startTime : a.endTime;
    const oldDate = which === "start" ? old.absolute.startDate : old.absolute.endDate;
    const oldText = which === "start" ? old.absolute.startTime : old.absolute.endTime;
    const oldValue = old[which];
    if (text === oldText && oldValue !== null) {
      if (date === oldDate) return oldValue;
      // 只改日期时沿用未显示的小数秒，再交给既有 DST 转换规则解释。
      return parse(date, formatLocal(oldValue, timezone).slice(11));
    }
    return parse(date, text);
  };
  const start = endpoint("start"), end = endpoint("end");
  if (start === null || end === null) {
    return {
      kind: "invalid", error: "请选择完整日期并输入有效时间",
      ...(start === null ? { startError: a.startDate === null ? "请选择开始日期" : "开始时间格式无效" } : {}),
      ...(end === null ? { endError: a.endDate === null ? "请选择结束日期" : "结束时间格式无效" } : {}),
    };
  }
  if (start >= end) return { kind: "invalid", error: "结束时间必须晚于开始时间", endError: "结束时间必须晚于开始时间" };
  return { kind: "absolute", start, end };
}

export function rangeSummary(model: RangeModel, choices: readonly QuickChoice[], timezone: string): { text: string; description: string } {
  if (model.quick !== null) {
    const text = choices.find(c => c.us === model.quick)?.label ?? "相对时间";
    return { text, description: `${text}；时区 ${timezone}；相对时间以查询时刻为准` };
  }
  if (model.start === null || model.end === null) return { text: "请选择时间范围", description: `时间范围无效；时区 ${timezone}` };
  const a = formatRangeTime(model.start, timezone), b = formatRangeTime(model.end, timezone);
  return { text: a + " → " + (a.slice(0, 10) === b.slice(0, 10) ? b.slice(11) : b), description: `${a} → ${b}；时区 ${timezone}` };
}
