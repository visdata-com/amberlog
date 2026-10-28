import { test } from "node:test";
import assert from "node:assert/strict";
import { countsByStart, bars, heights, total, pageRows } from "./chart.ts";

test("分布图：没有的分桶为 0，记录数取原文", () => {
  const counts = countsByStart([{ t: "1793348100000000", n: "3" }, { t: "1793349600000000", n: "1" }, { t: "1.5", n: "9" }]);
  const list = bars([1793348040000000n, 1793348100000000n, 1793349600000000n], counts);
  assert.deepEqual(list.map((b) => b.n), ["0", "3", "1"]);
  assert.deepEqual(heights(list), [0, 1, 1 / 3]);
  assert.deepEqual(heights(bars([1n, 2n], new Map())), [0, 0]);
});

test("总条数为各 n 之和，按整数相加不失真", () => {
  assert.equal(total([{ n: "3" }, { n: "1" }]), 4n);
  assert.equal(total([{ n: "9007199254740993" }, { n: "1" }]), 9007199254740994n);
  assert.equal(total([]), 0n);
  assert.equal(total([{ n: null }]), null);
});

test("本页的行范围", () => {
  assert.equal(pageRows(100, 50), "第 101～150 行");
  assert.equal(pageRows(0, 0), null);
});
