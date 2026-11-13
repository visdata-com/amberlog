import { test } from 'node:test';
import assert from 'node:assert/strict';
import { updateEquality, type Equality } from './equality.ts';
import { whereClause } from './sql.ts';
const n = (value: string) => ({ type: 'number' as const, value });
test('panel selections group inclusion, move polarity, deduplicate and remove empty groups', () => {
  let groups: Equality[] = [];
  groups = updateEquality(groups, 'A', false, n('1'));
  groups = updateEquality(groups, 'A', false, n('2'));
  groups = updateEquality(groups, 'B', true, n('4'));
  assert.equal(whereClause(groups), ' WHERE "A" IN (1, 2) AND "B" != 4');
  assert.equal(updateEquality(groups, 'A', false, n('1.0')), groups);
  groups = updateEquality(groups, 'A', true, n('1'));
  assert.equal(whereClause(groups), ' WHERE "A" = 2 AND "B" != 4 AND "A" != 1');
  groups = updateEquality(groups, 'A', true, n('2'));
  assert.equal(whereClause(groups), ' WHERE "B" != 4 AND "A" NOT IN (1, 2)');
});
test('values retain exact precision and distinguish strings and numbers', () => {
  let groups = updateEquality([], 'A', false, n('9007199254740993'));
  groups = updateEquality(groups, 'A', false, n('9007199254740992'));
  assert.match(whereClause(groups), /9007199254740993, 9007199254740992/);
  const typed = updateEquality(groups, 'A', false, {type:'string',value:'1'});
  assert.equal(typed.length,2);
  assert.match(whereClause(typed), /AND \"A\" = '1'$/);
});
test('independent conjunction groups must not be silently merged', () => {
  const groups: Equality[] = [1,2].map(value => ({kind:'set',field:'A',exclude:false,values:[n(String(value))]}));
  assert.equal(whereClause(groups), ' WHERE "A" = 1 AND "A" = 2');
  assert.throws(() => updateEquality(groups, 'A', false, n('3')), /SQL/);
});

test('updating an old flat condition keeps unrelated AND terms and stable IDs on repeat', async () => {
  const {updateChipsEquality} = await import('./fieldConditions.ts');
  const chips = [{id:1,cond:{kind:'field' as const,field:'A',op:'=' as const,value:'1'}}, {id:2,cond:{kind:'field' as const,field:'B',op:'>' as const,value:'0'}}];
  const next = updateChipsEquality(chips, 'A', false, n('2'), () => 3);
  assert.equal(whereClause(next.map(c=>c.cond)), ' WHERE "A" IN (1, 2) AND "B" > 0');
  assert.equal(updateChipsEquality(next, 'A', false, n('2'), () => 4), next);
});
test('single-value polarity replacement preserves its position among other fields', () => {
  let groups = updateEquality([], 'A', false, n('1'));
  groups = updateEquality(groups, 'B', false, n('2'));
  assert.equal(whereClause(updateEquality(groups,'A',true,n('1'))), ' WHERE "A" != 1 AND "B" = 2');
});
