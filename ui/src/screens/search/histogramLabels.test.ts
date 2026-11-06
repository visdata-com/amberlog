import { test } from "node:test";
import assert from "node:assert/strict";
import { histogramTick, histogramTime, histogramZone } from "./histogramLabels.ts";

const us = (text: string): bigint => BigInt(Date.parse(text)) * 1000n;

test("图表省略微秒，秒级范围的刻度保留秒", () => {
  const start = us("2026-11-02T08:00:30Z") + 123456n;
  const range = [start, start + 60_000_000n] as const;
  assert.equal(histogramTime(start, "Asia/Shanghai"), "2026-11-02 16:00:30");
  assert.equal(histogramTick(start, range, "Asia/Shanghai"), "16:00:30");
  assert.equal(histogramTick(start, [start, start + 3_600_000_000n], "Asia/Shanghai"), "16:00");
});

test("时间轴跨日显示日期、跨年显示年份，按所选时区判断", () => {
  const range = [us("2026-11-02T15:30:00Z"), us("2026-11-02T16:30:00Z")] as const;
  assert.equal(histogramTick(range[1], range, "Asia/Shanghai"), "11-03\n00:30");
  assert.equal(histogramTick(range[1], range, "UTC"), "16:30");
  const years = [us("2026-12-31T23:00:00Z"), us("2027-01-01T01:00:00Z")] as const;
  assert.equal(histogramTick(years[1], years, "UTC"), "2027-01-01\n01:00");
});

test("悬停时区说明能区分夏令时回拨时重复的本地时间", () => {
  const start = us("2026-11-01T05:30:00Z");
  const end = us("2026-11-01T06:30:00Z");
  assert.equal(histogramTime(start, "America/New_York"), histogramTime(end, "America/New_York"));
  assert.equal(histogramZone(start, end, "America/New_York"), "America/New_York · UTC-04:00 → UTC-05:00");
  assert.equal(histogramZone(start, end, "Asia/Shanghai"), "Asia/Shanghai · UTC+08:00");
});
