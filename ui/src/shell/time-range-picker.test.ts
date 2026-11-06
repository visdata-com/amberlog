import { test } from "node:test";
import assert from "node:assert/strict";
import { DAY_US, HOUR_US, parseTimeInput, formatLocal } from "../lib/time.ts";
import { calendarDays, shiftMonth, moveDate, selectDate, initDraft, draftValue, relativePreview, rangeSummary, formatRangeTime } from "./time-range-picker.ts";

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
test("日期范围支持同日、跨月、反向重启和完成后重选", () => {
  const anchor = selectDate({ startDate: null, endDate: null }, "2026-09-30");
  assert.deepEqual(selectDate(anchor, "2026-10-02"), { startDate: "2026-09-30", endDate: "2026-10-02" });
  assert.deepEqual(selectDate(anchor, "2026-09-30"), { startDate: "2026-09-30", endDate: "2026-09-30" });
  assert.deepEqual(selectDate(anchor, "2026-09-20"), { startDate: "2026-09-20", endDate: null });
  assert.deepEqual(selectDate({ startDate: "2026-09-01", endDate: "2026-09-03" }, "2026-09-05"), { startDate: "2026-09-05", endDate: null });
});
test("绝对草稿按指定时区初始化且不修改源模型", () => {
  const before = { ...model };
  const d = initDraft(model, "Asia/Shanghai", start + DAY_US);
  assert.equal(d.mode, "absolute");
  assert.equal(d.absolute.startDate, "2026-11-01");
  assert.equal(d.absolute.startTime, "08:00:00");
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
  assert.deepEqual(relativePreview(HOUR_US, start + DAY_US), [start + DAY_US - HOUR_US, start + DAY_US]);
});
test("只改日期保留时间微秒，查询终点不自动加一天", () => {
  const d = initDraft(model, "UTC", start);
  d.absolute = { ...d.absolute, startDate: "2026-11-02", endDate: "2026-11-03" };
  const v = draftValue(d, "UTC");
  assert.deepEqual(v, { kind: "absolute", start: start + DAY_US, end: start + 2n * DAY_US + HOUR_US });
});
test("允许分钟、秒和 1～6 位小数，拒绝无效时间/日期", () => {
  for (const time of ["00:00", "00:00:00", "00:00:00.1", "00:00:00.000001"]) {
    const d = initDraft(model, "UTC", start);d.absolute.startTime = time;
    assert.equal(draftValue(d, "UTC").kind, "absolute");
  }
  for (const time of ["", "24:00", "00:60", "12:00:00.1234567", "00:00Z"]) {
    const d = initDraft(model, "UTC", start);d.absolute.startTime = time;
    const v = draftValue(d, "UTC");assert.equal(v.kind, "invalid");
    if (v.kind === "invalid") assert.ok(v.startError);
  }
  const d = initDraft(model, "UTC", start);d.absolute.startDate = "2026-02-30";
  assert.equal(draftValue(d, "UTC").kind, "invalid");
});
test("未完成、空范围及逆序均不可提交", () => {
  const d = initDraft(model, "UTC", start);
  assert.equal(draftValue({ ...d, absolute: { ...d.absolute, endDate: null } }, "UTC").kind, "invalid");
  for (const endTime of ["00:00:00.123456", "00:00:00.000000"]) {
    assert.equal(draftValue({ ...d, absolute: { ...d.absolute, endTime } }, "UTC").kind, "invalid");
  }
});
test("夏令时重复/跳过规则沿用已有转换", () => {
  const d = initDraft(model, "UTC", start);
  d.absolute = { startDate: "2026-11-01", endDate: "2026-11-01", startTime: "01:30:00.000001", endTime: "03:00" };
  const repeated = draftValue(d, "America/New_York");
  assert.equal(repeated.kind, "absolute");
  if (repeated.kind === "absolute") assert.equal(repeated.start, 1793511000000001n);
  d.absolute = { startDate: "2026-03-08", endDate: "2026-03-08", startTime: "02:30", endTime: "04:00" };
  const skipped = draftValue(d, "America/New_York");assert.equal(skipped.kind, "absolute");
  if (skipped.kind === "absolute") assert.equal(formatLocal(skipped.start, "America/New_York"), "2026-03-08 03:00:00.000000");
});
test("摘要和预览显示到秒，可访问描述保留完整日期与时区", () => {
  const s = rangeSummary(model, [], "Asia/Shanghai");
  assert.doesNotMatch(s.text, /\.\d/);assert.match(s.description, /Asia\/Shanghai/);
  assert.match(s.description, /2026-11-01 09:00:00/);assert.doesNotMatch(s.description, /\.\d/);
  assert.equal(formatRangeTime(start, "UTC"), "2026-11-01 00:00:00");
  assert.equal(rangeSummary({ ...model, quick: HOUR_US }, [{ label: "最近 1 小时", us: HOUR_US }], "UTC").text, "最近 1 小时");
  assert.equal(parseTimeInput("2026-11-01 00:00:00.123456", "UTC"), start);
});

test("重开夏令时回拨的较晚时刻不编辑再应用，保留原 UTC", () => {
  const later = 1793514600000001n;
  const d = initDraft({ ...model, start: later, end: later + HOUR_US }, "America/New_York", later);
  assert.deepEqual(draftValue(d, "America/New_York"), { kind: "absolute", start: later, end: later + HOUR_US });
  d.absolute.startTime = "01:31:00.000001";
  const changed = draftValue(d, "America/New_York");
  assert.equal(changed.kind, "absolute");
  if (changed.kind === "absolute") assert.equal(changed.start, 1793511060000001n);
});


test("秒级显示不改变未编辑范围，编辑秒值不携带原隐藏小数", () => {
  const d = initDraft(model, "UTC", start);
  assert.equal(d.absolute.startTime, "00:00:00");
  assert.deepEqual(draftValue(d, "UTC"), { kind: "absolute", start, end: start + HOUR_US });
  d.absolute.startTime = "00:00:01";
  assert.deepEqual(draftValue(d, "UTC"), { kind: "absolute", start: 1793491201000000n, end: start + HOUR_US });
});
