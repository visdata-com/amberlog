import { test } from "node:test";
import assert from "node:assert/strict";
import { parseJson, type Hit } from "../../../lib/json.ts";
import { combine, contextQueries, sameObject, whereOf } from "./context.ts";

const hit = (src: string): Hit => parseJson(src) as Hit;
const seqs = (rows: Hit[]): string[] => rows.map((h) => String((h.get("seq") as { text: string }).text));

test("从检索请求取 WHERE 部分，去掉末尾的升序", () => {
  assert.equal(whereOf('SELECT * FROM "ctx" WHERE "seq" >= 0', "ctx"), ' WHERE "seq" >= 0');
  assert.equal(whereOf('SELECT * FROM "ctx" WHERE "seq" >= 0 ORDER BY _timestamp ASC', "ctx"), ' WHERE "seq" >= 0');
  assert.equal(whereOf('SELECT * FROM "ctx"', "ctx"), "");
  assert.equal(whereOf('SELECT * FROM "ctx" ORDER BY _timestamp ASC', "ctx"), "");
  assert.equal(whereOf('SELECT * FROM "a""b" WHERE match_all(\'x\')', 'a"b'), " WHERE match_all('x')");
  assert.equal(whereOf('SELECT * FROM "other" WHERE "a" = 1', "ctx"), "");
});

test("两段请求的范围、大小与排序", () => {
  const t = 1792137625000000n;
  const [before, after] = contextQueries("ctx", t, ' WHERE "seq" >= 0');
  assert.deepEqual(before, {
    sql: 'SELECT * FROM "ctx" WHERE "seq" >= 0',
    start_time: 1792134025000000n,
    end_time: t,
    size: 20,
    from: 0,
  });
  assert.deepEqual(after, {
    sql: 'SELECT * FROM "ctx" WHERE "seq" >= 0 ORDER BY _timestamp ASC',
    start_time: t,
    end_time: 1792141225000000n,
    size: 21,
    from: 0,
  });
});

test("前段起点不小于 0", () => {
  const [before] = contextQueries("ctx", 5n, "");
  assert.equal(before.start_time, 0n);
  assert.equal(before.sql, 'SELECT * FROM "ctx"');
});

test("JSON 对象相同按键的集合与值，不论键的先后", () => {
  assert.equal(sameObject(hit('{"a":1,"b":"x"}'), hit('{"b":"x","a":1}')), true);
  assert.equal(sameObject(hit('{"a":1}'), hit('{"a":1,"b":null}')), false);
  assert.equal(sameObject(hit('{"a":1.0}'), hit('{"a":1}')), false);
  assert.equal(sameObject(hit('{"a":{"x":[1]}}'), hit('{"a":{"x":[1]}}')), true);
});

test("后段逆序在前、前段在后，标出中心行", () => {
  const before = [24, 23, 22].map((k) => hit(`{"seq":${k}}`));
  const after = [25, 26, 27].map((k) => hit(`{"seq":${k}}`));
  const r = combine(before, after, hit('{"seq":25}'));
  assert.deepEqual(seqs(r.rows), ["27", "26", "25", "24", "23", "22"]);
  assert.equal(r.center, 2);
  assert.equal(r.few, true);
});

test("后段中没有打开的那一行时不标中心行；两段都满时不算不足", () => {
  const before = Array.from({ length: 20 }, (_, i) => hit(`{"seq":${-i}}`));
  const after = Array.from({ length: 21 }, (_, i) => hit(`{"seq":${i + 1}}`));
  const r = combine(before, after, hit('{"seq":0,"x":1}'));
  assert.equal(r.center, null);
  assert.equal(r.few, false);
  assert.equal(r.rows.length, 41);
});
