import { test } from "node:test";
import assert from "node:assert/strict";
import { Num, intOf, parseJson, rawText, toJsonText } from "./json.ts";

test("数字保留十进制原文、对象保留键的先后", () => {
  const v = parseJson('{"big":12345678901234567,"f":0.1,"s":"a\\"b","z":1e3,"a":[true,null]}');
  assert.ok(v instanceof Map);
  assert.deepEqual([...v.keys()], ["big", "f", "s", "z", "a"]);
  assert.equal(rawText(v.get("big")), "12345678901234567");
  assert.equal(rawText(v.get("f")), "0.1");
  assert.equal(rawText(v.get("s")), 'a"b');
  assert.equal(rawText(v.get("z")), "1e3");
  assert.equal(rawText(v.get("a")), "[true,null]");
  assert.equal(toJsonText(v), '{"big":12345678901234567,"f":0.1,"s":"a\\"b","z":1e3,"a":[true,null]}');
});

test("NULL 与缺少的键显示为横线", () => {
  assert.equal(rawText(null), "—");
  assert.equal(rawText(undefined), "—");
  assert.equal(rawText(false), "false");
});

test("整数取值", () => {
  assert.equal(intOf(new Num("1792137600000000")), 1792137600000000n);
  assert.equal(intOf(new Num("1.5")), null);
  assert.equal(intOf("1"), null);
});

test("不合 JSON 时抛错", () => {
  for (const s of ["", "{", '{"a":}', "[1,]", "01", "1 2", '"\\x"']) {
    assert.throws(() => parseJson(s), SyntaxError, s);
  }
});
