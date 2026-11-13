import { test } from "node:test";
import assert from "node:assert/strict";
import type { Chip } from "./CondBox.tsx";
import { parseCondition, whereClause } from "./sql.ts";
import { updateChipsEquality } from "./fieldConditions.ts";
const chip = (id: number, text: string): Chip => ({ id, cond: parseCondition(text)! });
const update = (chips: Chip[], exclude: boolean, id: number) => updateChipsEquality(chips, "level", exclude, {type:"string",value:"error"}, () => id);

test("字段取值首次加入或排除时追加集合", () => {
  const existing = [chip(1, "host = server")];
  for (const exclude of [false,true]) {
    const next = update(existing,exclude,2);
    assert.equal(next[0],existing[0]);
    assert.equal(whereClause(next.map(c=>c.cond)), ` WHERE "host" = 'server' AND "level" ${exclude ? '!=' : '='} 'error'`);
  }
});
test("字段取值双向切换保留位置并仅生成一个 SQL 条件", () => {
  const existing = [chip(1,"host = server"),chip(2,'level = "error"'),chip(3,"duration > 10")];
  const excluded = update(existing,true,4);
  assert.deepEqual(excluded.map(c=>c.id),[1,4,3]);
  assert.equal(whereClause(excluded.map(c=>c.cond)), ` WHERE "host" = 'server' AND "level" != 'error' AND "duration" > 10`);
  const included = update(excluded,false,5);
  assert.deepEqual(included.map(c=>c.id),[1,5,3]);
  assert.equal(whereClause(included.map(c=>c.cond)),whereClause(existing.map(c=>c.cond)));
});
test("重复相同操作保留列表和条件 ID", () => {
  for (const exclude of [false,true]) {
    const existing = [chip(1,`level ${exclude ? '!=' : '='} "error"`)];
    assert.equal(update(existing,exclude,2),existing);
  }
});
test("独立 AND 组不再跨组去重或改写", () => {
  const existing = [chip(1,'level = "error"'),chip(2,'level = "warn"')];
  assert.throws(()=>update(existing,true,3),/SQL/);
  assert.equal(whereClause(existing.map(c=>c.cond)), ` WHERE "level" = 'error' AND "level" = 'warn'`);
});
test("保留其他字段、范围与全文项，混合类型不合并", () => {
  const existing = ['status = 500','level > "a"','level LIKE error','level IS NULL','timeout'].map((s,i)=>chip(i+1,s));
  const next = update(existing,true,6);
  existing.forEach((c,i)=>assert.equal(next[i],c));
  const typed = updateChipsEquality([chip(1,'status = 500')],'status',true,{type:'string',value:'500'},()=>2);
  assert.equal(whereClause(typed.map(c=>c.cond)), ` WHERE \"status\" = 500 AND \"status\" != '500'`);
});
test("切换使用新 ID 且不修改已执行快照", () => {
  const existing=[chip(1,'level = "error"'),chip(2,'duration > 10')];
  const snapshot=structuredClone(existing);
  for(const c of existing){Object.freeze(c.cond);Object.freeze(c);}Object.freeze(existing);
  const next=update(existing,true,3);
  assert.deepEqual(next.map(c=>c.id),[3,2]);
  assert.deepEqual(existing,snapshot);
  assert.equal(update(next,true,4),next);
});
