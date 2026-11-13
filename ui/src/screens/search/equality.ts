import { quoteIdent, sqlString } from './sql.ts';

/** 数字保留原文，绝不经由 Number；每个集合是独立的 AND 项。 */
export interface LiteralValue { type: 'string' | 'number' | 'boolean'; value: string }
export interface Equality { kind: 'set'; field: string; exclude: boolean; values: LiteralValue[] }
export const valueSql = (v: LiteralValue): string => v.type === 'string' ? sqlString(v.value) : v.value;
export function equalitySql(c: Equality): string {
  const op = c.values.length === 1 ? (c.exclude ? '!=' : '=') : (c.exclude ? 'NOT IN' : 'IN');
  const values = c.values.map(valueSql).join(', ');
  return quoteIdent(c.field) + ' ' + op + ' ' + (c.values.length === 1 ? values : '(' + values + ')');
}
function key(v: LiteralValue): string {
  if (v.type !== 'number') return v.type + ':' + v.value;
  const [mantissa = '', exponent = '0'] = v.value.toLowerCase().split('e');
  const negative = mantissa.startsWith('-');
  const unsigned = mantissa.replace(/^[+-]/, '');
  const fraction = unsigned.split('.')[1]?.length ?? 0;
  let digits = unsigned.replace('.', '').replace(/^0+/, '');
  if (!digits) return 'number:0';
  const trailing = digits.length - digits.replace(/0+$/, '').length;
  digits = digits.slice(0, digits.length - trailing);
  return 'number:' + (negative ? '-' : '') + digits + 'e' + (BigInt(exponent) - BigInt(fraction) + BigInt(trailing));
}
export const sameValue = (a: LiteralValue, b: LiteralValue): boolean => key(a) === key(b);
export function updateEquality(groups: Equality[], field: string, exclude: boolean, value: LiteralValue): Equality[] {
  const candidates = groups.filter(g => g.field === field && g.values[0]?.type === value.type);
  for (const polarity of [false, true]) {
    if (candidates.filter(g => g.exclude === polarity).length > 1) throw new Error('该字段有多个独立 AND 条件，请在 SQL 中编辑目标条件');
  }
  const target = candidates.find(g => g.exclude === exclude);
  const opposite = candidates.find(g => g.exclude !== exclude);
  if (target?.values.some(v => sameValue(v, value)) && !opposite?.values.some(v => sameValue(v, value))) return groups;
  let inserted = false;
  const next = groups.flatMap(g => {
    if (!candidates.includes(g)) return [g];
    const values = g.exclude === exclude
      ? g.values.some(v => sameValue(v, value)) ? g.values : [...g.values, value]
      : g.values.filter(v => !sameValue(v, value));
    if (!values.length && !target) { inserted = true; return [{...g, exclude, values:[value]}]; }
    return values.length ? [{...g, values}] : [];
  });
  if (!target && !inserted) next.push({kind:'set',field,exclude,values:[value]});
  return next;
}
