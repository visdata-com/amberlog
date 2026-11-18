import { test } from "node:test";
import assert from "node:assert/strict";
import { DAY_US, HOUR_US, parseTimeInput, formatLocal } from "../lib/time.ts";
import { calendarDays, shiftMonth, moveDate, chooseDate, dateOfText, initDraft, draftValue, relativePreview, rangeSummary, formatRangeTime } from "./time-range-picker.ts";

const start = 1793491200123456n;
const model = { quick: null, start, end: start + HOUR_US, startText: null, endText: null };

test("日历从周日开始且包含 42 格和邻月日期", () => {
  const days = calendarDays("2026-09-23");
  assert.equal(days.length, 42);
  assert.equal(days[0]?.date, "2026-08-30");
  assert.equal(days[41]?.date, "2026-10-10");
  assert.equal(days.filter(d => d.inMonth).length, 30);
});
test("闰月、跨年、月份末尾和四位年份边界", () => {
  assert.equal(calendarDays("2024-02-01").filter(d => d.inMonth).length, 29);
  assert.equal(shiftMonth("2024-01-31", 1), "2024-02-29");
  assert.equal(shiftMonth("2026-12-31", 1), "2027-01-31");
  assert.equal(shiftMonth("0000-01-01", -1), "0000-01-01");
  assert.equal(shiftMonth("9999-12-31", 1), "9999-12-31");
  assert.equal(moveDate("0000-01-01", -1), "0000-01-01");
  assert.equal(moveDate("9999-12-31", 1), "9999-12-31");
  assert.ok(calendarDays("9999-12-01").some(d => d.date === null));
});
test("日历点选先填起点再填止点，保留框中的时刻部分，更早的日期重选起点", () => {
  const empty = { startText: "", endText: "", pick: "start" as const };
  const a = chooseDate(empty, "2026-09-30");
  assert.deepEqual(a, { startText: "2026-09-30 00:00:00.000000", endText: "", pick: "end" });
  const b = chooseDate(a, "2026-10-02");
  assert.deepEqual(b, { startText: "2026-09-30 00:00:00.000000", endText: "2026-10-02 23:59:59.999999", pick: "start" });
  assert.equal(chooseDate(a, "2026-09-20").startText, "2026-09-20 00:00:00.000000");
  const kept = chooseDate({ startText: "2026-10-16T09:00:00Z", endText: "", pick: "start" }, "2026-10-17");
  assert.equal(kept.startText, "2026-10-17T09:00:00Z");
  assert.equal(dateOfText("2026-02-30 10:00"), null);
  assert.equal(dateOfText("2026-10-16T09:00:00+08:00"), "2026-10-16");
});
test("绝对草稿的起止框按指定时区显示到微秒且不修改源模型", () => {
  const before = { ...model };
  const d = initDraft(model, "Asia/Shanghai", start + DAY_US);
  assert.equal(d.mode, "absolute");
  assert.equal(d.absolute.startText, "2026-11-01 08:00:00.123456");
  assert.deepEqual(draftValue(d, "Asia/Shanghai"), { kind: "absolute", start, end: start + HOUR_US });
  assert.deepEqual(model, before);
});
test("相对草稿只保存时长，绝对初值为打开预览，模式往返不丢草稿", () => {
  const m = { ...model, quick: HOUR_US };
  const d = initDraft(m, "UTC", start + DAY_US);
  assert.equal(d.mode, "relative");
  assert.deepEqual(draftValue(d, "UTC"), { kind: "quick", us: HOUR_US });
  const absolute = { ...d, mode: "absolute" as const };
  assert.deepEqual(draftValue(absolute, "UTC"), { kind: "absolute", start: start + DAY_US - HOUR_US, end: start + DAY_US });
  assert.deepEqual(draftValue({ ...absolute, mode: "relative" }, "UTC"), { kind: "quick", us: HOUR_US });
  assert.deepEqual(relativePreview(HOUR_US, start), [start - HOUR_US, start]);
});
test("起止框接受 TIME-INPUT 的全部写法，含 T、Z 与偏移", () => {
  const d = initDraft(model, "Asia/Shanghai", start);
  for (const [text, want] of [
    ["2026-10-16 08:00", "2026-10-16 08:00:00.000000"],
    ["2026-10-16T08:00:00", "2026-10-16 08:00:00.000000"],
    ["2026-10-16 08:00:00.5", "2026-10-16 08:00:00.500000"],
    ["2026-10-16T00:00:00Z", "2026-10-16 08:00:00.000000"],
    ["2026-10-16T02:00:00-06:00", "2026-10-16 16:00:00.000000"],
  ] as const) {
    const v = draftValue({ ...d, absolute: { ...d.absolute, startText: text, endText: "2026-10-17 00:00" } }, "Asia/Shanghai");
    assert.equal(v.kind, "absolute", text);
    if (v.kind === "absolute") assert.equal(formatLocal(v.start, "Asia/Shanghai"), want, text);
  }
  for (const text of ["", "25:00", "2026-02-30 10:00", "2026/10/16 8点", "2026-10-16 08:00:00.1234567"]) {
    const v = draftValue({ ...d, absolute: { ...d.absolute, startText: text } }, "Asia/Shanghai");
    assert.equal(v.kind, "invalid", text);
    if (v.kind === "invalid") assert.ok(v.startError);
  }
});
test("起点不早于止点不可提交", () => {
  const d = initDraft(model, "UTC", start);
  for (const endText of ["2026-11-01 00:00:00.123456", "2026-10-31 23:00"]) {
    assert.equal(draftValue({ ...d, absolute: { ...d.absolute, endText } }, "UTC").kind, "invalid");
  }
});
test("夏令时重复/跳过规则沿用已有转换", () => {
  const d = initDraft(model, "UTC", start);
  d.absolute = { startText: "2026-11-01 01:30:00.000001", endText: "2026-11-01 03:00", pick: "start" };
  const repeated = draftValue(d, "America/New_York");
  assert.equal(repeated.kind, "absolute");
  if (repeated.kind === "absolute") assert.equal(repeated.start, 1793511000000001n);
  d.absolute = { startText: "2026-03-08 02:30", endText: "2026-03-08 04:00", pick: "start" };
  const skipped = draftValue(d, "America/New_York");assert.equal(skipped.kind, "absolute");
  if (skipped.kind === "absolute") assert.equal(formatLocal(skipped.start, "America/New_York"), "2026-03-08 03:00:00.000000");
});
test("摘要到秒、悬停为完整时刻；快捷项查询过以后显示实际取的起止", () => {
  const s = rangeSummary(model, [], "Asia/Shanghai");
  assert.doesNotMatch(s.text, /\.\d/);
  assert.equal(s.title, "2026-11-01 08:00:00.123456 → 2026-11-01 09:00:00.123456");
  assert.equal(formatRangeTime(start, "UTC"), "2026-11-01 00:00:00");
  const choices = [{ label: "最近 1 小时", us: HOUR_US }];
  const quick = rangeSummary({ ...model, quick: HOUR_US, queried: false }, choices, "UTC");
  assert.equal(quick.text, "最近 1 小时");
  assert.equal(quick.title, undefined);
  const done = rangeSummary({ ...model, quick: HOUR_US, queried: true }, choices, "UTC");
  assert.equal(done.text, "最近 1 小时 · 2026-11-01 00:00:00 → 01:00:00");
  assert.equal(done.title, "2026-11-01 00:00:00.123456 → 2026-11-01 01:00:00.123456");
  assert.equal(parseTimeInput("2026-11-01 00:00:00.123456", "UTC"), start);
});
test("重开夏令时回拨的较晚时刻不编辑再应用，保留原 UTC", () => {
  const later = 1793514600000001n;
  const d = initDraft({ ...model, start: later, end: later + HOUR_US }, "America/New_York", later);
  assert.deepEqual(draftValue(d, "America/New_York"), { kind: "absolute", start: later, end: later + HOUR_US });
  d.absolute.startText = "2026-11-01 01:31:00.000001";
  const changed = draftValue(d, "America/New_York");
  assert.equal(changed.kind, "absolute");
  if (changed.kind === "absolute") assert.equal(changed.start, 1793511060000001n);
});
