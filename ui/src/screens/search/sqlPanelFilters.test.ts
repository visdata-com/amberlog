import {test} from 'node:test';
import assert from 'node:assert/strict';
import * as codec from './queryCodec.ts';
const v={json:'"error"',isString:true,text:'error'};
function update(sql,field='level',exclude=false,value=v){
 assert.equal(typeof codec.applySqlPanelFilter,'function');
 return codec.applySqlPanelFilter(sql,field,exclude,value);
}
function changed(sql,field='level',exclude=false,value=v){const r=update(sql,field,exclude,value);assert.equal(r.kind,'changed',JSON.stringify(r));return r.sql;}
test('WHERE update toggles and repeats without a caret argument',()=>{
 const base="SELECT * FROM logs WHERE service = 'api' ORDER BY _timestamp DESC LIMIT 100";
 const added=changed(base);const excluded=changed(added,'level',true);
 assert.match(excluded,/service = 'api'/);assert.match(excluded,/"level" != 'error'/);
 assert.equal(excluded.endsWith('ORDER BY _timestamp DESC LIMIT 100'),true);
 assert.equal(update(excluded,'level',true).kind,'unchanged');
 assert.equal(codec.decodeQuery(excluded).ok,true);
});
test('creates WHERE before tails and preserves projection, comments and semicolon',()=>{
 for(const tail of ['', 'GROUP BY service ORDER BY service LIMIT 10','ORDER BY _timestamp DESC LIMIT 100']){
 const sql='SELECT service FROM logs -- WHERE fake\n'+tail+'; -- end';
 const result=changed(sql);assert.ok(result.startsWith('SELECT service FROM logs -- WHERE fake\n'));
 assert.match(result,/WHERE "level" = 'error'/);assert.ok(result.endsWith(tail+'; -- end'));
 assert.equal(codec.decodeQuery(result,false).ok,true);
 }
});
test('mixed ranges, BETWEEN, functions, LIKE and NULL predicates survive',()=>{
 for(const expr of ['duration > 100','duration BETWEEN 1 AND 10',"message LIKE '%AND%'",'host IS NOT NULL',"str_match(message, 'AND')",'NOT (duration < 1 OR duration > 100)']){
 const sql=`SELECT * FROM logs WHERE ${expr} AND level = 'error'`;
 const result=changed(sql,'level',true);assert.ok(result.includes(expr));assert.match(result,/"level" != 'error'/);
 assert.equal(update(result,'level',true).kind,'unchanged');assert.equal(codec.decodeQuery(result).ok,false);
 }
});
test('OR precedence survives append and later toggle',()=>{
 const sql="SELECT * FROM logs WHERE service = 'api' OR host = 'web'";
 const result=changed(sql);assert.match(result,/\(service = 'api' OR host = 'web'\s*\)\s+AND/);
 const next=changed(result,'level',true);assert.match(next,/"level" != 'error'/);assert.equal(update(next,'level',true).kind,'unchanged');
});
test('multi-value movement preserves empty-group removal and other conditions',()=>{
 const sql='SELECT * FROM logs WHERE "A" IN (1, 2) AND host = \'api\'';
 const next=changed(sql,'A',true,{json:'1',isString:false,text:''});
 assert.match(next,/"A" = 2/);assert.match(next,/"A" != 1/);assert.match(next,/host = 'api'/);
 const moved=changed(next,'A',true,{json:'2',isString:false,text:''});assert.match(moved,/"A" NOT IN \(1, 2\)/);assert.equal(codec.decodeQuery(moved).ok,true);
});
test('ambiguous and incomplete SQL are unchanged errors',()=>{
 for(const expr of ["level = 'error' AND level = 'warn'","level = 'error' OR host = 'web'","NOT (level = 'error')",'level > 1','duration >','duration BETWEEN 1 AND','duration IN ()','duration = 1 AND',"message = 'oops",'duration = (1','duration = 1 OR','duration = 1 garbage']) {
 const sql='SELECT * FROM logs WHERE '+expr;assert.equal(update(sql).kind,'error',expr);
 }
 for(const sql of ['SELECT * FROM logs; SELECT * FROM other','SELECT * FROM logs JOIN other ON a=b','SELECT * FROM logs WHERE','SELECT * FROM logs ORDER BY','SELECT * FROM logs LIMIT'])assert.equal(update(sql).kind,'error',sql);
});
test('comments and quoted keywords are not boundaries',()=>{
 const sql="SELECT * FROM logs WHERE message = 'WHERE AND LIMIT' AND level /*keep*/ = 'error' -- tail\nORDER BY _timestamp DESC; -- end";
 const next=changed(sql,'level',true);for(const text of ["message = 'WHERE AND LIMIT'",'/*keep*/','-- tail','ORDER BY _timestamp DESC; -- end'])assert.ok(next.includes(text));
 assert.equal(codec.decodeQuery(next).ok,true);
});
test('a bare unfinished field name cannot become an opaque predicate',()=>{
 assert.equal(update('SELECT * FROM logs WHERE le').kind,'error');
});
test('flattened parentheses retain trailing inner comments',()=>{
 const sql='SELECT * FROM logs WHERE (a = 1 AND b = 2 /*keep*/)';
 const result=changed(sql);assert.ok(result.includes('/*keep*/'));assert.equal(codec.decodeQuery(result).ok,true);
});
test('incomplete projection and trailing clauses are rejected before editing',()=>{
 for(const sql of ['SELECT * FROM logs WHERE a=1 ORDER BY a,','SELECT * FROM logs GROUP BY a,','SELECT a, FROM logs','SELECT * FROM logs ORDER BY a DESC,','SELECT * FROM logs ORDER BY a +'])assert.equal(update(sql).kind,'error',sql);
});
test('complete aggregate projections and ordering survive filtering',()=>{
 const sql='SELECT service, count(*) AS n FROM logs GROUP BY service ORDER BY n DESC LIMIT 10';
 assert.ok(changed(sql).startsWith('SELECT service, count(*) AS n FROM logs'));
});
test('adding the first condition keeps single-line SQL on one line',()=>{
 assert.equal(changed('SELECT * FROM logs'),`SELECT * FROM logs WHERE "level" = 'error'`);
 assert.equal(changed('SELECT * FROM logs ORDER BY _timestamp DESC LIMIT 100'),`SELECT * FROM logs WHERE "level" = 'error' ORDER BY _timestamp DESC LIMIT 100`);
 assert.equal(changed('SELECT * FROM logs;'),`SELECT * FROM logs WHERE "level" = 'error';`);
});
test('adding WHERE retains existing line breaks and escapes an EOF line comment',()=>{
 assert.equal(changed('SELECT *\nFROM logs\n'),`SELECT *\nFROM logs\nWHERE "level" = 'error'`);
 const sql=changed('SELECT * FROM logs -- keep');
 assert.equal(sql,`SELECT * FROM logs -- keep\nWHERE "level" = 'error'`);
 assert.equal(codec.decodeQuery(sql).ok,true);
});
