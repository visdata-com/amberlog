import {test} from 'node:test';
import assert from 'node:assert/strict';
import {decodeQuery, rewriteWhere} from './queryCodec.ts';
import {whereClause} from './sql.ts';
function parse(sql: string) { const q = decodeQuery(sql); if (!q.ok) assert.fail(q.error); return q; }
test('codec roundtrips equality, inequality, membership and pure same-field OR', () => {
  for (const input of ['A IN (1, 2) AND B != 4', '(A=1 OR A=2) AND B<>4', '((A IN (1,2))) AND (B NOT IN (4))']) {
    const q = parse('SELECT * FROM events WHERE '+input+';');
    assert.equal(whereClause(q.groups), ' WHERE "a" IN (1, 2) AND "b" != 4');
    assert.equal(q.dataset, 'events');
  }
  const q = parse('SELECT * FROM events WHERE A=1 AND A=2');
  assert.equal(whereClause(q.groups), ' WHERE "a" = 1 AND "a" = 2');
});
test('quotes, comments, exact numbers and retained non-WHERE clauses', () => {
  const sql = `SELECT count(*) /* projection */ FROM "ev" WHERE "A" = 'it''s; WHERE' /* keep */ GROUP BY "A" ORDER BY "A" DESC LIMIT 5; -- end`;
  const q = parse(sql);
  assert.equal(q.groups[0]?.values[0]?.value, "it's; WHERE");
  assert.equal(rewriteWhere(q, q.groups), sql);
  const changed = rewriteWhere(q, parse('SELECT * FROM ev WHERE A=9007199254740993').groups);
  assert.match(changed, /9007199254740993/);
  assert.match(changed, /\/\* projection \*\//);
  assert.match(changed, /\/\* keep \*\//);
  assert.match(changed, /GROUP BY "A" ORDER BY "A" DESC LIMIT 5; -- end$/);
  assert.equal(parse(changed).groups[0]?.values[0]?.type, 'number');
  assert.equal(parse("SELECT * FROM ev WHERE A='1'").groups[0]?.values[0]?.type, 'string');
  assert.equal(parse('SELECT * FROM ev WHERE A=true').groups[0]?.values[0]?.type, 'boolean');
});
test('bounded conversion rejects unsupported or incomplete SQL without modifying input', () => {
  for (const suffix of ['A=1 OR B=2','A>1','NOT A=1','A=NULL','A IN (1,\'2\')','A=1 AND','(A=1','A=1); SELECT * FROM ev',"A='oops",'A=1 /* unfinished','A=1 AND (A=2 OR B=3)']) {
    const sql = 'SELECT * FROM ev WHERE '+suffix;
    const q = decodeQuery(sql);
    assert.equal(q.ok, false, sql);
    assert.equal(q.sql, sql);
  }
  for (const sql of ['SELECT FROM ev','SELECT * FROM ev alias','SELECT * FROM ev WHERE','SELECT * FROM ev; SELECT * FROM ev','SELECT * FROM ev ORDER BY','SELECT * FROM ev LIMIT']) assert.equal(decodeQuery(sql).ok, false, sql);
});
test('adding and removing WHERE preserves clause boundaries and comments', () => {
  const q = parse('SELECT * FROM ev -- source\nORDER BY A LIMIT 5;');
  const groups = parse('SELECT * FROM ev WHERE A=1').groups;
  const changed = rewriteWhere(q, groups);
  assert.equal(parse(changed).groups.length, 1);
  assert.match(changed, /-- source\n/);
  assert.match(changed, /ORDER BY A LIMIT 5;/);
  assert.equal(parse(rewriteWhere(parse(changed), [])).groups.length, 0);
});

test('history persists full SQL for multi-values and keeps legacy flat AND semantics', async () => {
  const {encode,decode} = await import('./history/history.ts');
  const legacy = {mode:'cond' as const,stream:'ev',range:{quick:3600000000n},at:0n,conds:[1,2].map(value=>({kind:'field' as const,field:'A',op:'=' as const,value:String(value)}))};
  const modern = {mode:'sql' as const,range:legacy.range,at:1n,sql:'SELECT * FROM ev WHERE A IN (1,2) AND B != 4'};
  const restored = decode(encode([modern,legacy]));
  assert.deepEqual(restored,[modern,legacy]);
  const old = restored[1]!;
  assert.equal(old.mode === 'cond' ? whereClause(old.conds) : '', ' WHERE "A" = 1 AND "A" = 2');
  const current = restored[0]!;
  assert.equal(current.mode === 'sql' ? parse(current.sql).groups[0]?.values.length : 0,2);
});

test('record sorting after a mode roundtrip preserves the draft WHERE and comments', async () => {
  const {sortRecordSql} = await import('./queryCodec.ts');
  const source='SELECT * /* keep */ FROM ev WHERE A=1; -- tail';
  const ascending=sortRecordSql(source,true);
  assert.match(ascending,/ORDER BY _timestamp ASC/);
  assert.match(ascending,/A=1/);
  assert.match(ascending,/; -- tail$/);
  assert.ok(parse(ascending).normal);
  const descending=sortRecordSql(ascending,false);
  assert.match(descending,/ORDER BY _timestamp DESC/);
  assert.equal((descending.match(/ORDER BY/g) ?? []).length,1);
  assert.match(descending,/\/\* keep \*\//);
});
