import {test} from 'node:test';
import assert from 'node:assert/strict';
import * as filters from './fieldConditions.ts';
import {whereClause} from './sql.ts';
const value = (json: string, text = '') => ({json,isString:json.startsWith('"'),text});
test('panel scalar adapter rejects complex values and preserves numeric text',()=>{
 assert.equal(typeof filters.panelLiteral,'function');
 for(const json of ['null','[]','{}']) assert.equal(filters.panelLiteral(value(json)),null);
 assert.deepEqual(filters.panelLiteral(value('9007199254740993')),{type:'number',value:'9007199254740993'});
 assert.deepEqual(filters.panelLiteral(value('true')),{type:'boolean',value:'true'});
 assert.deepEqual(filters.panelLiteral(value('"1"','1')),{type:'string',value:'1'});
});
test('panel condition operation is pure, typed, idempotent and switches polarity',()=>{
 assert.equal(typeof filters.applyPanelCondition,'function');
 let chips=[];let id=0;
 const apply=(field,exclude,v)=>filters.applyPanelCondition(chips,field,exclude,v,()=>++id);
 let r=apply('A',false,value('1'));assert.equal(r.kind,'changed');chips=r.chips;
 assert.equal(apply('A',false,value('1')).kind,'unchanged');
 r=apply('A',false,value('2'));chips=r.chips;
 r=apply('A',true,value('1'));chips=r.chips;
 assert.equal(whereClause(chips.map(c=>c.cond)),' WHERE "A" = 2 AND "A" != 1');
 const saved=structuredClone(chips);assert.equal(apply('A',true,value('null')).kind,'error');assert.deepEqual(chips,saved);
});
