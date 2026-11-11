import { test } from "node:test";
import assert from "node:assert/strict";
import { conditionDraft, conditionHints } from "./conditionInput.ts";

const columns = ["level", "level.name", "status", "message"];
const noValues = () => null;
const values = () => [
  { text: "error", json: '"error"', isString: true, n: "3" },
  { text: "warn", json: '"warn"', isString: true, n: "2" },
];

test("空框无候选，关键词提供带内容的全文入口", () => {
  for (const input of ["", "  "]) assert.deepEqual(conditionHints(input, columns, noValues), []);
  assert.deepEqual(conditionHints("lev", columns, noValues).map((h) => h.text), ["level", "level.name", "添加全文条件：lev"]);
  assert.deepEqual(conditionHints("timeout", columns, noValues), [
    { kind: "text", text: "添加全文条件：timeout", condition: { kind: "text", text: "timeout" } },
  ]);
  assert.deepEqual(conditionDraft("message timeout", columns), { condition: { kind: "text", text: "message timeout" }, error: null });
  assert.deepEqual(conditionHints("message timeout", columns, noValues).map((h) => h.text), ["添加全文条件：message timeout"]);
});

test("字段后仅提示运算符，并支持补全 LIKE 与 IS NOT NULL", () => {
  const hints = conditionHints("level ", columns, noValues);
  assert.equal(hints.length, 9);
  assert.ok(hints.every((h) => h.kind === "op"));
  assert.deepEqual(conditionHints("level is n", columns, noValues), [
    { kind: "op", text: "IS NULL", fill: "level IS NULL" },
    { kind: "op", text: "IS NOT NULL", fill: "level IS NOT NULL" },
  ]);
  assert.deepEqual(conditionHints("level l", columns, noValues), [{ kind: "op", text: "LIKE", fill: "level LIKE " }]);
});

test("值候选按当前输入过滤，保留字段与运算符，不混入全文", () => {
  for (const input of ["level=er", 'level = "er', 'level = "error"']) {
    assert.deepEqual(conditionHints(input, columns, values), [
      { kind: "value", text: "error", condition: { kind: "field", field: "level", op: "=", value: '"error"' } },
    ]);
  }
  assert.equal(conditionHints("level != ", columns, values).length, 2);
  assert.deepEqual(conditionHints("level = ", columns, noValues), []);
  assert.deepEqual(conditionHints("level.name >= ", columns, values).map((h) => "condition" in h && h.condition.kind === "field" ? h.condition.field : null), ["level.name", "level.name"]);
});

test("不完整字段条件和引号不能被误添为全文，字段表尚未加载也能校验缺值", () => {
  for (const input of ["level ", "level !", "level IS NOT", "unknown IS NOT", "status >=", "unknown >=", "level LIKE", 'level = "er', '"timeout']) {
    const draft = conditionDraft(input, columns);
    assert.equal(draft.condition, null, input);
    assert.ok(draft.error, input);
  }
  assert.ok(conditionDraft("status >=", []).error);
  assert.deepEqual(conditionHints("unknown >=", columns, noValues), []);
});

test("有效字段、空值、含运算符的值和显式全文保持语义", () => {
  for (const input of ["status >= 500", "level IS NULL", "level IS NOT NULL", "other = true", "level = a>", 'level = ""']) {
    const draft = conditionDraft(input, columns);
    assert.equal(draft.error, null, input);
    assert.equal(draft.condition?.kind, "field", input);
  }
  assert.deepEqual(conditionDraft('"status >="', columns), { condition: { kind: "text", text: "status >=" }, error: null });
  assert.deepEqual(conditionHints('"status >="', columns, noValues).map((h) => h.text), ["添加全文条件：status >="]);
  assert.deepEqual(conditionDraft("level", columns), { condition: { kind: "text", text: "level" }, error: null });
  assert.deepEqual(conditionDraft('""', columns), { condition: null, error: null });
});
