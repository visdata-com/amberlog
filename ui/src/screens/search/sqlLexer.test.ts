import {test} from 'node:test';
import {strict as assert} from 'node:assert';
import {lexSql} from './sqlLexer.ts';
import {decodeQuery} from './queryCodec.ts';

test('lexer preserves UTF-16 source ranges, comments and escaped quoted names',()=>{
 const sql=`SELECT "a""b", '中😀''文', 1.2e-3 -- FROM fake\nFROM app /* x */`;
 const tokens=lexSql(sql);
 assert.equal(tokens.map(t=>t.text).join(''),sql);
 for(const t of tokens)assert.equal(sql.slice(t.start,t.end),t.text);
 assert.deepEqual(tokens.filter(t=>t.kind==='ident').map(t=>t.text),['"a""b"']);
 assert.equal(tokens.filter(t=>t.kind==='comment').length,2);
 assert.equal(tokens.find(t=>t.kind==='number')?.text,'1.2e-3');
});
test('unfinished tokens are retained while strict conversion still rejects them',()=>{
 for(const tail of [`'abc`, '"abc', '/* abc']){
 const sql='SELECT * FROM app WHERE x = '+tail;
 assert.equal(lexSql(sql).at(-1)?.closed,false);
 assert.equal(decodeQuery(sql).ok,false);
 }
 const sql=`SELECT * FROM "app" WHERE "a""b" = 'x''y' /*ok*/`;
 assert.equal(decodeQuery(sql).ok,true);
 assert.ok(lexSql(sql).every(t=>t.closed));
});
