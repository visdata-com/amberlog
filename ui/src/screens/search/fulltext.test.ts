import { test } from "node:test";
import assert from "node:assert/strict";
import { highlight, narrowOnAdd, fulltextBlocked, FULLTEXT_NARROW_US, FULLTEXT_LIMIT_US } from "./fulltext.ts";
import { parseCondition, type Condition } from "./sql.ts";

const hits = (text: string, terms: string[]) => highlight(text, terms).filter((s) => s.hit).map((s) => s.text);

test("按 match_all 的规则标出全文词（块场景）", () => {
  assert.deepEqual(hits("TimeOut at É and é", ["timeout", "é"]), ["TimeOut", "é"]);
});

test("片段拼回原文；重叠与相邻的标出合并", () => {
  const segs = highlight("aaa-AB-ab", ["aa", "ab"]);
  assert.equal(segs.map((s) => s.text).join(""), "aaa-AB-ab");
  assert.deepEqual(hits("aaa-AB-ab", ["aa", "ab"]), ["aaa", "AB", "ab"]);
  assert.deepEqual(highlight("", ["x"]), []);
  assert.deepEqual(highlight("abc", []), [{ text: "abc", hit: false }]);
  assert.deepEqual(hits("abc", [""]), []);
});

test("非 ASCII 字母按原样、% 与 _ 按字面", () => {
  assert.deepEqual(hits("STRASSE straße", ["ß"]), ["ß"]);
  assert.deepEqual(hits("50% a_b", ["%", "_"]), ["%", "_"]);
  assert.deepEqual(hits("😀x😀", ["😀"]), ["😀", "😀"]);
});

const text = parseCondition("timeout")!;
const field = parseCondition("a = 1")!;
const H = 3600000000n;

test("加入第一个全文条件且范围长于 24 小时时收窄", () => {
  assert.ok(narrowOnAdd([], text, 6n * 24n * H));
  assert.ok(narrowOnAdd([field], text, FULLTEXT_NARROW_US + 1n));
  assert.ok(!narrowOnAdd([], text, FULLTEXT_NARROW_US));
  assert.ok(!narrowOnAdd([text], parseCondition("x")!, 6n * 24n * H));
  assert.ok(!narrowOnAdd([], field, 6n * 24n * H));
  assert.ok(!narrowOnAdd([], text, null));
});

test("含全文条件且范围长于 7 天时不可查询", () => {
  const conds: Condition[] = [field, text];
  assert.ok(fulltextBlocked(conds, 30n * 24n * H));
  assert.ok(!fulltextBlocked(conds, FULLTEXT_LIMIT_US));
  assert.ok(!fulltextBlocked([field], 30n * 24n * H));
});
