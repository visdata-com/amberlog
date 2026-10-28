import { test } from "node:test";
import assert from "node:assert/strict";
import {
  formatLocal,
  formatOffset,
  formatUtc,
  gapEnd,
  localToUtcEarliest,
  offsetOf,
  parseTimeInput,
  usToWall,
  utcCandidates,
  wallOf,
  wallToUs,
} from "./time.ts";

const T = 1792137600000000n; // 2026-10-16T08:00:00Z

test("按时区显示到微秒", () => {
  assert.equal(formatLocal(T, "UTC"), "2026-10-16 08:00:00.000000");
  assert.equal(formatLocal(T, "Asia/Shanghai"), "2026-10-16 16:00:00.000000");
  assert.equal(formatLocal(T + 123456n, "Asia/Kolkata"), "2026-10-16 13:30:00.123456");
  assert.equal(formatLocal(T, "America/New_York"), "2026-10-16 04:00:00.000000");
  assert.equal(formatLocal(-1n, "UTC"), "1969-12-31 23:59:59.999999");
});

test("偏移与 UTC 形式", () => {
  assert.equal(formatOffset(T, "Asia/Shanghai"), "+08:00");
  assert.equal(formatOffset(T, "Asia/Kolkata"), "+05:30");
  assert.equal(formatOffset(T, "America/New_York"), "-04:00");
  assert.equal(formatOffset(T, "UTC"), "+00:00");
  assert.equal(formatUtc(T), "2026-10-16T08:00:00.000000Z");
  assert.equal(formatUtc(T + 1n), "2026-10-16T08:00:00.000001Z");
  assert.equal(offsetOf(T, "Asia/Kolkata"), 19800);
});

test("挂钟与本地微秒互换", () => {
  const w = { y: 2024, mo: 2, d: 29, h: 23, mi: 59, s: 59, us: 999999 };
  assert.deepEqual(usToWall(wallToUs(w)), w);
  assert.deepEqual(wallOf(T, "Asia/Shanghai"), { y: 2026, mo: 10, d: 16, h: 16, mi: 0, s: 0, us: 0 });
});

test("输入按所选时区或所写偏移解释", () => {
  assert.equal(parseTimeInput("2026-10-16 16:00", "Asia/Shanghai"), T);
  assert.equal(parseTimeInput("2026-10-16T16:00:00", "Asia/Shanghai"), T);
  assert.equal(parseTimeInput("2026-10-16 16:00:00.5", "Asia/Shanghai"), T + 500000n);
  assert.equal(parseTimeInput("2026-10-16T09:00:00Z", "Asia/Shanghai"), 1792141200000000n);
  assert.equal(parseTimeInput("2026-10-16T17:00:00.000001+08:00", "UTC"), 1792141200000001n);
  assert.equal(parseTimeInput("2026-10-16T03:00:00-05:00", "UTC"), T);
});

test("不合写法与日历上不存在的日期", () => {
  for (const s of ["", "  ", "2026/10/16 8点", "2026-10-16", "2026-02-30 10:00", "2026-13-01 00:00",
    "2026-10-16 24:00", "2026-10-16 08:00:00.1234567", "2026-10-16T08:00Z", "2026-10-16T08:00:00+8:00"]) {
    assert.equal(parseTimeInput(s, "UTC"), null, s);
  }
  assert.equal(parseTimeInput("2024-02-29 00:00", "UTC"), 1709164800000000n);
  assert.equal(parseTimeInput("2100-02-29 00:00", "UTC"), null);
});

test("被跳过的本地时刻取跳变之后的第一个时刻、重复的取较早者", () => {
  // America/New_York：2026-03-08 02:00 拨到 03:00；2026-11-01 02:00 回拨到 01:00
  assert.equal(parseTimeInput("2026-03-08 02:30", "America/New_York"), 1772953200000000n);
  assert.equal(parseTimeInput("2026-11-01 01:30", "America/New_York"), 1793511000000000n);
  const local = wallToUs({ y: 2026, mo: 11, d: 1, h: 1, mi: 30, s: 0, us: 0 });
  assert.deepEqual(utcCandidates(local, "America/New_York"), [1793511000000000n, 1793514600000000n]);
  const skipped = wallToUs({ y: 2026, mo: 3, d: 8, h: 2, mi: 30, s: 0, us: 0 });
  assert.deepEqual(utcCandidates(skipped, "America/New_York"), []);
  assert.equal(gapEnd(skipped, "America/New_York"), 1772953200000000n);
  assert.equal(localToUtcEarliest(skipped, "America/New_York"), 1772953200000000n);
});

test("没有夏令时的时区每个本地时刻恰对应一个 UTC 时刻", () => {
  const local = wallToUs({ y: 2026, mo: 3, d: 8, h: 2, mi: 30, s: 0, us: 0 });
  assert.equal(utcCandidates(local, "Asia/Shanghai").length, 1);
  assert.equal(utcCandidates(local, "UTC")[0], local);
});
