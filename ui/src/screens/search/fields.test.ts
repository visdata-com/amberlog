import { test } from "node:test";
import assert from "node:assert/strict";
import { byCodePoint, fieldBeforeOp, groupFields, isStreamName, type Field } from "./fields.ts";

const f = (name: string, type = "Utf8"): Field => ({ name, type });

test("字段栏按前缀分组（V2 块场景）", () => {
  const fields = ["_timestamp", "b", "http.path", "http.status", "k8s.pod", "level", "message"].map((n) => f(n));
  const groups = groupFields(fields);
  assert.deepEqual(groups.map((g) => [g.name, g.fields.map((x) => x.name)]), [
    [null, ["_timestamp", "message", "b", "level"]],
    ["http", ["http.path", "http.status"]],
    ["k8s", ["k8s.pod"]],
  ]);
});

test("按字段名筛选：只把 ASCII 字母视为不区分大小写（V2 块场景）", () => {
  const fields = ["_timestamp", "b", "http.path", "http.status", "k8s.pod", "level", "message"].map((n) => f(n));
  assert.deepEqual(groupFields(fields, "STAT").map((g) => [g.name, g.fields.map((x) => x.name)]), [["http", ["http.status"]]]);
  assert.deepEqual(groupFields([f("Éa"), f("éb")], "é").map((g) => g.fields.map((x) => x.name)), [["éb"]]);
  assert.deepEqual(groupFields(fields, "zzz"), []);
});

test("按码点排序，不按 UTF-16 码元", () => {
  // U+FF5E（一个码元）小于 U+1F600（两个码元，首码元 0xD83D）按码元会反过来
  assert.ok(byCodePoint("\u{FF5E}", "\u{1F600}") < 0);
  assert.ok(byCodePoint("a", "ab") < 0);
  assert.deepEqual(groupFields([f("b.x"), f("B.y"), f("a")]).map((g) => g.name), [null, "B", "b"]);
});

test("stream 名的合规写法", () => {
  for (const s of ["app", "a_1", "0"]) assert.ok(isStreamName(s), s);
  for (const s of ["", "_a", "App", "a-b", "a".repeat(65)]) assert.ok(!isStreamName(s), s);
});

test("输入提示：识别「字段名 运算」", () => {
  const names = ["level", "level.x", "n"];
  assert.equal(fieldBeforeOp("level = ", names), "level");
  assert.equal(fieldBeforeOp("level=", names), "level");
  assert.equal(fieldBeforeOp("  level != ", names), "level");
  assert.equal(fieldBeforeOp("level like ", names), "level");
  assert.equal(fieldBeforeOp("level.x >= ", names), "level.x");
  assert.equal(fieldBeforeOp("level = e", names), null);
  assert.equal(fieldBeforeOp("level", names), null);
  assert.equal(fieldBeforeOp("levelLIKE ", names), null);
});
