import { test } from "node:test";
import assert from "node:assert/strict";
import { parseJson, type Json } from "../../../lib/json.ts";
import { addValue, groupFields, matches, prettyJson, RECORD_GROUP } from "./fields.ts";

test("message 在前，记录字段组在前，其余组按码点升序，组内按出现先后", () => {
  const keys = ["_timestamp", "level", "k8s.pod", "http.status", "message", "http.path", "Z.x", "a.b"];
  assert.deepEqual(groupFields(keys), [
    { name: null, keys: ["message"] },
    { name: RECORD_GROUP, keys: ["_timestamp", "level"] },
    { name: "Z", keys: ["Z.x"] },
    { name: "a", keys: ["a.b"] },
    { name: "http", keys: ["http.status", "http.path"] },
    { name: "k8s", keys: ["k8s.pod"] },
  ]);
});

test("没有 message、没有不带点的键时不出空组", () => {
  assert.deepEqual(groupFields(["b.x", "a.y"]), [
    { name: "a", keys: ["a.y"] },
    { name: "b", keys: ["b.x"] },
  ]);
});

test("组名按码点而不是 UTF-16 码元排序", () => {
  // U+FF5E 的码元大于 U+1F600 的高代理项，按码点则在前
  assert.deepEqual(
    groupFields(["\u{1F600}.a", "～.b"]).map((g) => g.name),
    ["～", "\u{1F600}"],
  );
});

test("筛选按字段名或显示的值", () => {
  assert.equal(matches("502", "http.status", "502"), true);
  assert.equal(matches("502", "http.path", "/x"), false);
  assert.equal(matches("HTTP", "http.path", "/x"), true);
  assert.equal(matches("  ", "a", "b"), true);
});

test("原始 JSON 照原文与键的先后", () => {
  const v = parseJson('{"b":1.50,"a":{"x":[1,"s"],"e":{}},"n":null,"t":true,"arr":[]}');
  const text = prettyJson(v);
  assert.equal(
    text,
    '{\n  "b": 1.50,\n  "a": {\n    "x": [\n      1,\n      "s"\n    ],\n    "e": {}\n  },\n  "n": null,\n  "t": true,\n  "arr": []\n}',
  );
  assert.deepEqual(JSON.parse(text), JSON.parse('{"b":1.50,"a":{"x":[1,"s"],"e":{}},"n":null,"t":true,"arr":[]}'));
});

test("加入的取值：字符串另记原文，其余为原始 JSON 文本", () => {
  const m = parseJson('{"code":"404","status":502,"o":{"k":1}}') as Map<string, Json>;
  assert.deepEqual(addValue(m.get("code")!), { json: '"404"', isString: true, text: "404" });
  assert.deepEqual(addValue(m.get("status")!), { json: "502", isString: false, text: "" });
  assert.deepEqual(addValue(m.get("o")!), { json: '{"k":1}', isString: false, text: "" });
});
