import {test} from 'node:test';
import {strict as assert} from 'node:assert';
import {completionAt, fieldSource, completionEdit} from './sqlCompletion.ts';
const names=['status','service','host.name','a"b'];
const hint=(sql:string,caret=sql.length)=>completionAt(sql,caret,caret,['test_web','test_api'],names);

test('first character and contextual prefix matches with stable complete candidate list',()=>{
 assert.deepEqual(hint('s').items.map(x=>x.label),['SELECT']);
 assert.deepEqual(hint('Se').items.map(x=>x.label),['SELECT']);
 assert.deepEqual(hint('SELECT * FROM te').items.map(x=>x.label),['test_api','test_web']);
 assert.equal(hint('SELECT * FROM ').items.length,2);
 for(const s of ['SELECT st','SELECT * FROM app WHERE st','SELECT * FROM app GROUP BY st','SELECT * FROM app ORDER BY st'])assert.equal(hint(s).items[0]?.label,'status');
 assert.equal(completionAt('SELECT f',8,8,[],Array.from({length:12},(_,i)=>'field'+i)).items.length,12);
 for(const s of ['jo','wi','ha','off','SELECT * FROM app WHERE status = st',"SELECT 'st",'SELECT /* st','SELECT -- st','SELECT xyz'])assert.equal(hint(s).items.length,0,s);
 assert.equal(hint('SELECT "st').items[0]?.label,'status');
});
test('FROM fields use confirmed single source independently of filter parsing',()=>{
 assert.deepEqual(fieldSource('SELECT st','old'),{kind:'resolved',dataset:'old'});
 assert.deepEqual(fieldSource('SELECT st FROM app WHERE (x LIKE','old'),{kind:'resolved',dataset:'app'});
 assert.deepEqual(fieldSource('SELECT * FROM ap','old'),{kind:'pending'});
 assert.deepEqual(fieldSource('SELECT * FROM "app"','old'),{kind:'resolved',dataset:'app'});
 assert.deepEqual(fieldSource('SELECT * FROM app','old',true),{kind:'resolved',dataset:'app'});
 assert.deepEqual(fieldSource("SELECT 'FROM bogus' -- FROM wrong\n",'old'),{kind:'resolved',dataset:'old'});
 for(const sql of ['SELECT * FROM a,b','SELECT * FROM a JOIN b','SELECT * FROM a;SELECT * FROM b','SELECT * FROM (SELECT * FROM a)','SELECT * FROM _ledger','SELECT * FROM a AS b'])assert.equal(fieldSource(sql,'old',true).kind,'unsupported',sql);
});
test('edits replace the whole token and quote names while preserving suffix and stale guards',()=>{
 const sql='select * FROM app';const c=hint(sql,3);
 assert.deepEqual(completionEdit(sql,3,3,c,c.items[0]!),{text:'SELECT * FROM app',cursor:6});
 const d=hint('SELECT * FROM te');assert.deepEqual(completionEdit(d.sql,d.caret,d.caret,d,d.items[0]!),{text:'SELECT * FROM "test_api"',cursor:24});
 for(const [prefix,want] of [['"host','"host.name"'],['"a','"a""b"']] as const){const q=hint('SELECT '+prefix);assert.equal(completionEdit(q.sql,q.caret,q.caret,q,q.items[0]!)?.text,'SELECT '+want);}
 assert.equal(completionEdit(sql+' ',3,3,c,c.items[0]!),null);
 assert.equal(completionEdit(sql,2,3,c,c.items[0]!),null);
 assert.equal(hint('SELECT st',9).items[0]?.label,'status');
});
test('value lists and invalid sources do not leak field suggestions',()=>{
 assert.equal(hint('SELECT * FROM app WHERE status IN (st').items.length,0);
 assert.equal(fieldSource('SELECT * FROM "test_api" WHERE x = 1','old',false,19).kind,'pending');
 assert.equal(hint("SELECT * FROM app WHERE status = 'x' an").items[0]?.label,'AND');
});
test('replacing a FROM name before an existing WHERE does not request each prefix',()=>{
 for(const name of ['n','ne','new']){
  const sql='SELECT * FROM '+name+' WHERE x = 1';
  assert.equal(fieldSource(sql,'old',false,14+name.length).kind,'pending');
  assert.deepEqual(fieldSource(sql,'old',false,15+name.length),{kind:'resolved',dataset:name});
 }
});
test('unary NOT accepts fields and quoted prefixes retain escaped quotes',()=>{
 assert.equal(hint('SELECT * FROM app WHERE NOT st').items[0]?.label,'status');
 const c=completionAt('SELECT "a""',11,11,[],['abc','a"b']);
 assert.deepEqual(c.items.map(i=>i.label),['a"b']);
});
