import { test } from "node:test";
import assert from "node:assert/strict";
import { INCOMPLETE, conditionDraft, conditionHints } from "./conditionInput.ts";

const columns = ["level", "level.name", "status", "message"];
const noValues = () => null;
const values = () => [
  { text: "error", json: '"error"', isString: true, n: "3" },
  { text: "warn", json: '"warn"', isString: true, n: "2" },
];

test("空框无提示；会成为全文条件时最后一项为全文项", () => {
  for (const input of ["", "  "]) assert.deepEqual(conditionHints(input, columns, noValues), []);
  assert.deepEqual(conditionHints("lev", columns, noValues).map((h) => h.text), ["level", "level.name", "添加全文条件：lev"]);
  assert.deepEqual(conditionHints("timeout", columns, noValues), [
    { kind: "text", text: "添加全文条件：timeout", condition: { kind: "text", text: "timeout" } },
  ]);
  assert.deepEqual(conditionDraft("message timeout"), { condition: { kind: "text", text: "message timeout" }, error: null });
  assert.deepEqual(conditionHints("message timeout", columns, noValues).map((h) => h.text), ["添加全文条件：message timeout"]);
});

test("字段名后提示运算；只写了字段名时仍会成为全文条件，全文项在最后", () => {
  const hints = conditionHints("level ", columns, noValues);
  assert.equal(hints.length, 10);
  assert.ok(hints.slice(0, 9).every((h) => h.kind === "op"));
  assert.deepEqual(hints[9], { kind: "text", text: "添加全文条件：level", condition: { kind: "text", text: "level" } });
  assert.deepEqual(conditionHints("level is n", columns, noValues), [
    { kind: "op", text: "IS NULL", fill: "level IS NULL" },
    { kind: "op", text: "IS NOT NULL", fill: "level IS NOT NULL" },
  ]);
});

test("「字段名 运算」后提示已统计的取值，不提供全文项", () => {
  for (const input of ["level=er", 'level = "er', 'level = "error"']) {
    assert.deepEqual(conditionHints(input, columns, values), [
      { kind: "value", text: "error", condition: { kind: "field", field: "level", op: "=", value: '"error"' } },
    ]);
  }
  assert.deepEqual(conditionHints("level = ", columns, values).map((h) => h.text), ["error", "warn"]);
  assert.deepEqual(conditionHints("level = ", columns, noValues), []);
});

test("四种不完整的输入：只看文字本身，不依赖字段表", () => {
  const cases: [string, string][] = [
    ['"', INCOMPLETE.quote],
    ['"timeout', INCOMPLETE.quote],
    ["level IS", INCOMPLETE.op],
    ["unknown is not", INCOMPLETE.op],
    ["x IS NOT NU", INCOMPLETE.op],
    ["status >=", INCOMPLETE.value],
    ["unknown =", INCOMPLETE.value],
    ["level LIKE", INCOMPLETE.value],
    ['level = "er', INCOMPLETE.valueQuote],
    ['level = "', INCOMPLETE.valueQuote],
  ];
  for (const [input, error] of cases) {
    for (const cols of [columns, []]) {
      assert.deepEqual(conditionDraft(input), { condition: null, error }, input);
      assert.equal(conditionHints(input, cols, noValues).some((h) => h.kind === "text"), false, input);
    }
  }
});

test("不是不完整的输入的照 FIELD-COND 与 TEXT-COND", () => {
  for (const input of ["status >= 500", "level IS NULL", "level IS NOT NULL", "other = true", "level = a>", 'level = ""']) {
    const draft = conditionDraft(input);
    assert.equal(draft.error, null, input);
    assert.equal(draft.condition?.kind, "field", input);
  }
  // 字段名为空的切分（如 `= 5`）、只写了字段名、`IS` 前没有字符：都是全文条件
  for (const [input, text] of [["= 5", "= 5"], ["level", "level"], ["level ", "level"], ["IS", "IS"], ['"status >="', "status >="]]) {
    assert.deepEqual(conditionDraft(input), { condition: { kind: "text", text }, error: null }, input);
  }
  assert.deepEqual(conditionDraft('""'), { condition: null, error: null });
});
