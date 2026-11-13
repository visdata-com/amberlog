import type { Chip } from "./CondBox.tsx";

import { updateEquality, type Equality, type LiteralValue } from './equality.ts';
import { conditionSql, type Utf8Fields } from './sql.ts';
import { decodeQuery } from './queryCodec.ts';

/** 旧等值标签仍是独立 AND 项；只有唯一目标时才允许字段面板扩展集合。 */
export function updateChipsEquality(chips: Chip[], field: string, exclude: boolean, value: LiteralValue, nextId: () => number, utf8?: Utf8Fields): Chip[] {
  const groups = chips.flatMap(chip => {
    if (chip.cond.kind === 'set') return [chip.cond];
    if (chip.cond.kind !== 'field' || !['=', '!='].includes(chip.cond.op)) return [];
    const parsed = decodeQuery('SELECT * FROM t WHERE '+conditionSql(chip.cond,utf8));
    return parsed.ok ? parsed.groups : [];
  });
  const updated = updateEquality(groups,field,exclude,value);
  if (updated === groups) return chips;
  let index = 0;
  const used = new Set<Equality>();
  const result = chips.flatMap(chip => {
    const c = chip.cond;
    if (c.kind !== 'set' && !(c.kind === 'field' && ['=', '!='].includes(c.op))) return [chip];
    const old = groups[index++];
    const replacement = updated.find(g => g === old || g.field === old?.field && g.values[0]?.type === old?.values[0]?.type && g.exclude === old?.exclude)
      ?? updated.find(g => g.field === old?.field && g.values[0]?.type === old?.values[0]?.type && !groups.some(before => before.field === g.field && before.values[0]?.type === g.values[0]?.type && before.exclude === g.exclude));
    if (!replacement || used.has(replacement)) return [];
    used.add(replacement);
    return replacement === old ? [chip] : [{id:nextId(),cond:replacement}];
  });
  for (const cond of updated) if (!used.has(cond)) result.push({id:nextId(),cond});
  return result;
}

export interface PanelValue { json: string; isString: boolean; text: string }
export const UNSUPPORTED_PANEL_VALUE = '仅支持字符串、数字和布尔值；NULL、数组和对象请在 SQL 中编辑';
export function panelLiteral(value: PanelValue): LiteralValue | null {
  if (value.isString) return {type:'string',value:value.text};
  if (/^(true|false)$/.test(value.json)) return {type:'boolean',value:value.json};
  if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/.test(value.json)) return {type:'number',value:value.json};
  return null;
}
export function applyPanelCondition(chips: Chip[], field: string, exclude: boolean, value: PanelValue, nextId: () => number, utf8?: Utf8Fields):
  {kind:'changed';chips:Chip[]} | {kind:'unchanged'} | {kind:'error';error:string} {
  const literal = panelLiteral(value);
  if (!literal) return {kind:'error',error:UNSUPPORTED_PANEL_VALUE};
  try {
    const next = updateChipsEquality(chips,field,exclude,literal,nextId,utf8);
    return next === chips ? {kind:'unchanged'} : {kind:'changed',chips:next};
  } catch(error) { return {kind:'error',error:error instanceof Error ? error.message : '无法更新条件'}; }
}
