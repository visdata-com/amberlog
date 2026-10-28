import { test } from "node:test";
import assert from "node:assert/strict";
import { UTC_ZONE, zoneOf } from "./zone.ts";
import { bucketOf, buckets, chooseInterval, nextBucketStart, zoomRange } from "./histogram.ts";

const S = 1000000n, M = 60n * S, H = 60n * M, D = 24n * H;
const NY = zoneOf("America/New_York");
const SH = zoneOf("Asia/Shanghai");
const KOL = zoneOf("Asia/Kolkata");

// 以下数值取自 REQ-AMB-QRY-HISTOGRAM-V2 的场景

test("按 5 分钟分桶（UTC）", () => {
  const vals = [0n, 299999999n, 300000000n, 900000001n].map((t) => bucketOf(t, 5n * M, UTC_ZONE));
  assert.deepEqual(vals, [0n, 0n, 300000000n, 900000000n]);
});

test("按天分桶对齐 UTC 零点", () => {
  assert.equal(bucketOf(1759449600000000n, D, UTC_ZONE), 1759449600000000n);
  assert.equal(bucketOf(1759535999999999n, D, UTC_ZONE), 1759449600000000n);
});

test("按天分桶对齐查询时区的本地零点", () => {
  assert.equal(bucketOf(1792166399999999n, D, SH), 1792080000000000n);
  assert.equal(bucketOf(1792166400000000n, D, SH), 1792166400000000n);
});

test("夏令时回拨那天的桶长 25 小时", () => {
  assert.equal(bucketOf(1793505600000000n, D, NY), 1793505600000000n);
  assert.equal(bucketOf(1793595599999999n, D, NY), 1793505600000000n);
  assert.equal(bucketOf(1793595600000000n, D, NY), 1793595600000000n);
  assert.equal(nextBucketStart(1793505600000000n, D, NY) - 1793505600000000n, 25n * H);
});

test("重复的一小时分成两个桶", () => {
  assert.equal(bucketOf(1793511000000000n, H, NY), 1793509200000000n);
  assert.equal(bucketOf(1793514600000000n, H, NY), 1793512800000000n);
});

test("回拨那一小时按分钟分桶时桶起点不早于记录一小时", () => {
  assert.equal(bucketOf(1793514600000000n, M, NY), 1793514600000000n);
});

test("被跳过的本地桶起点取跳变之后的第一个时刻", () => {
  assert.equal(bucketOf(1772955000000000n, 2n * H, NY), 1772953200000000n);
  // 跳变之前的一刻仍属本地 00:00 那一桶
  assert.equal(bucketOf(1772953200000000n - 1n, 2n * H, NY), 1772953200000000n - 2n * H);
});

// 以下数值取自 REQ-AMB-UI-SEARCH 的场景

test("按所选时区数分桶个数：Asia/Kolkata 本地 100 个小时取 1h", () => {
  // 2026-10-16 00:00 至 2026-10-20 04:00（+05:30）
  const start = 1792089000000000n, end = start + 100n * H;
  assert.equal(buckets(start, end, H, KOL).length, 100);
  assert.equal(buckets(start, end, H, UTC_ZONE).length, 101);
  assert.equal(chooseInterval(start, end, KOL).name, "1h");
  assert.equal(chooseInterval(start, end, UTC_ZONE).name, "3h");
});

test("跨夏令时回拨时按实际的分桶数：America/New_York 取 3h、34 根柱", () => {
  // 2026-10-28 00:00 EDT 至 2026-11-01 04:00 EST，实际 101 小时
  const start = 1793160000000000n, end = start + 101n * H;
  assert.equal(buckets(start, end, H, NY).length, 101);
  const iv = chooseInterval(start, end, NY);
  assert.equal(iv.name, "3h");
  assert.equal(buckets(start, end, iv.us, NY).length, 34);
});

test("分布图画出全部分桶：UTC 一小时 60 根柱、每柱 1m", () => {
  const start = 1793347200000000n, end = start + H; // 2026-10-30 08:00Z
  const iv = chooseInterval(start, end, UTC_ZONE);
  assert.equal(iv.name, "1m");
  assert.equal(buckets(start, end, iv.us, UTC_ZONE).length, 60);
});

test("点一根柱缩小时间范围", () => {
  const start = 1793347200000000n, end = start + H;
  const r = zoomRange(1793348100000000n, 1793348100000000n, M, UTC_ZONE, [start, end]);
  assert.deepEqual(r, [1793348100000000n, 1793348160000000n]);
});

test("缩放与原范围取交集", () => {
  const start = 1793347230000000n, end = start + H; // 08:00:30 起
  const r = zoomRange(1793347200000000n, 1793350800000000n, M, UTC_ZONE, [start, end]);
  assert.deepEqual(r, [start, end]);
});

test("回拨那一夜的分钟桶连续、个数等于实际分钟数", () => {
  // America/New_York 2026-11-01 本地 00:00 至 04:00 实际 300 分钟
  const start = 1793505600000000n, end = start + 300n * M;
  const bs = buckets(start, end, M, NY);
  assert.equal(bs.length, 300);
  for (let j = 1; j < bs.length; j++) assert.equal(bs[j]! - bs[j - 1]!, M);
});

test("拨快那天按小时数分桶、按天桶长 23 小时", () => {
  const start = 1772946000000000n; // 2026-03-08 00:00 EST = 05:00Z
  const bs = buckets(start, start + 23n * H, H, NY);
  assert.equal(bs.length, 23);
  assert.equal(nextBucketStart(start, D, NY) - start, 23n * H);
});

test("每个取值不晚于该时刻、且取值为 k 的时刻恰在 [k, 下一个分桶起点) 中", () => {
  for (const [zone, i] of [[NY, 30n * M], [NY, 3n * H], [zoneOf("Australia/Lord_Howe"), 30n * M], [KOL, D]] as const) {
    const start = 1793160000000000n;
    let prev = bucketOf(start, i, zone);
    for (let t = start; t < start + 8n * D; t += 7n * M + 13n * S) {
      const k = bucketOf(t, i, zone);
      assert.ok(k <= t, `${zone.name} ${t}`);
      assert.ok(k >= prev, `${zone.name} ${t} 不单调`);
      assert.ok(t < nextBucketStart(k, i, zone), `${zone.name} ${t}`);
      prev = k;
    }
  }
});

test("长范围按天数选间隔", () => {
  const start = 1767225600000000n; // 2026-01-01Z
  assert.equal(chooseInterval(start, start + 30n * D, UTC_ZONE).name, "12h");
  assert.equal(chooseInterval(start, start + 200n * D, UTC_ZONE).name, "2d");
  assert.equal(chooseInterval(start, start + 1000n * D, NY).name, "11d");
  assert.deepEqual(buckets(start, start, H, NY), []);
});

// 服务端 REQ-AMB-QRY-HISTOGRAM-V2 实现所用的同一组数值：时区、间隔、t、桶起点
test("与服务端同一组数值核对", () => {
  const rows: [string, bigint, bigint, bigint][] = [
    ["UTC", 5n * M, 0n, 0n],
    ["UTC", 5n * M, 299999999n, 0n],
    ["UTC", 5n * M, 300000000n, 300000000n],
    ["UTC", 5n * M, 900000001n, 900000000n],
    ["UTC", D, 1759449600000000n, 1759449600000000n],
    ["UTC", D, 1759535999999999n, 1759449600000000n],
    ["Asia/Shanghai", D, 1792166399999999n, 1792080000000000n],
    ["Asia/Shanghai", D, 1792166400000000n, 1792166400000000n],
    ["America/New_York", D, 1793505600000000n, 1793505600000000n],
    ["America/New_York", D, 1793595599999999n, 1793505600000000n],
    ["America/New_York", D, 1793595600000000n, 1793595600000000n],
    ["America/New_York", H, 1793511000000000n, 1793509200000000n],
    ["America/New_York", H, 1793514600000000n, 1793512800000000n],
    ["America/New_York", M, 1793514600000000n, 1793514600000000n],
    ["America/New_York", 2n * H, 1772955000000000n, 1772953200000000n],
    ["Asia/Kolkata", H, 1792454400000000n, 1792452600000000n],
    ["Asia/Kolkata", D, 1792454400000000n, 1792434600000000n],
    ["Asia/Kolkata", 30n * M, 1792454400000000n, 1792454400000000n],
    ["Asia/Shanghai", D, 1792108800000000n, 1792080000000000n],
    ["Asia/Shanghai", H, 1792108800000000n, 1792108800000000n],
    ["America/New_York", D, 1772971200000000n, 1772946000000000n],
    ["America/New_York", 150n * M, 1772953800000000n, 1772953200000000n],
    ["America/New_York", 30n * M, 1793511900000000n, 1793511000000000n],
    ["America/New_York", 30n * M, 1793515500000000n, 1793514600000000n],
  ];
  for (const [tz, i, t, want] of rows) assert.equal(bucketOf(t, i, zoneOf(tz)), want, `${tz} ${i} ${t}`);
  // 拨快那天按天的桶长 23 小时
  assert.equal(nextBucketStart(1772946000000000n, D, NY) - 1772946000000000n, 23n * H);
});
