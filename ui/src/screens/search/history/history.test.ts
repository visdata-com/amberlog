import { test } from "node:test";
import assert from "node:assert/strict";
import { type HistoryItem, decode, encode, record } from "./history.ts";

const cond = (n: number, at = 0n): HistoryItem => ({
  mode: "cond",
  stream: "app",
  range: { quick: 3600000000n },
  conds: [{ kind: "field", field: "n", op: "=", value: String(n) }],
  at,
});

test("新的一项在最前，只留 20 项", () => {
  let list: HistoryItem[] = [];
  for (let n = 1; n <= 25; n++) list = record(list, cond(n));
  assert.equal(list.length, 20);
  assert.deepEqual(list[0], cond(25));
  assert.deepEqual(list[19], cond(6));
});

test("与最新一项只差发起的时刻时不另记", () => {
  const list = record([cond(1, 1n)], cond(1, 2n));
  assert.deepEqual(list, [cond(1, 1n)]);
  // 与较旧的一项相同照记
  assert.equal(record([cond(2), cond(1)], cond(1)).length, 3);
});

test("时间范围、数据集、条件或 SQL 有一处不同就另记", () => {
  const base = cond(1);
  const variants: HistoryItem[] = [
    { ...base, range: { quick: 86400000000n } } as HistoryItem,
    { ...base, range: { start: 1n, end: 2n } } as HistoryItem,
    { ...base, stream: "web" } as HistoryItem,
    { ...base, conds: [{ kind: "text", text: "n = 1" }] } as HistoryItem,
    { ...base, conds: [] } as HistoryItem,
    { mode: "sql", range: base.range, sql: 'SELECT * FROM "app"', at: 0n },
  ];
  for (const v of variants) assert.equal(record([base], v).length, 2, JSON.stringify(encode([v])));
  const sql: HistoryItem = { mode: "sql", range: { start: 1n, end: 2n }, sql: "SELECT 1", at: 5n };
  assert.equal(record([sql], { ...sql, at: 6n }).length, 1);
  assert.equal(record([sql], { ...sql, sql: "SELECT 2" }).length, 2);
});

test("存成 JSON 再读回不变，微秒不丢精度", () => {
  const list: HistoryItem[] = [
    { mode: "sql", range: { start: 1760000000123456n, end: 1760000000999999n }, sql: 'SELECT host FROM "web"', at: 1760000001000001n },
    {
      mode: "cond",
      stream: "app",
      range: { quick: 604800000000n },
      conds: [
        { kind: "text", text: "timeout" },
        { kind: "field", field: "status", op: ">=", value: "500" },
        { kind: "field", field: "x", op: "IS NULL", value: "" },
      ],
      at: 1n,
    },
  ];
  assert.deepEqual(decode(encode(list)), list);
});

test("读出失败或不是预期形状时为空历史", () => {
  for (const t of [null, "", "{", "{}", "null", "[1]", '[{"mode":"x"}]', '[{"mode":"sql","range":{"quick":"1"},"sql":"s","at":1}]',
    '[{"mode":"cond","stream":"a","range":{"quick":"1"},"conds":[{"kind":"field","field":"f","op":"~","value":""}],"at":"1"}]']) {
    assert.deepEqual(decode(t), [], String(t));
  }
});
