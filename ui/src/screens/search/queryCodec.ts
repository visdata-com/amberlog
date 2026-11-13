import { lexSql } from './sqlLexer.ts';
import { sameValue, type Equality, type LiteralValue } from './equality.ts';
import { whereClause, type Condition, type Utf8Fields } from './sql.ts';

type Token = { text: string; start: number; end: number; kind: 'word' | 'ident' | 'string' | 'number' | 'symbol' | 'comment' };
export interface DecodedQuery {
  ok: true; sql: string; dataset: string; groups: Equality[];
  /** 从 WHERE 关键字到尾部子句之间的源区间；无 WHERE 时是插入位置。 */
  start: number; end: number; comments: string[]; normal: boolean;
  tableStart: number; tableEnd: number; orderStart: number | null; tailEnd: number;
}
export type QueryDecode = DecodedQuery | {ok: false; sql: string; error: string};
function fail(message: string): never { throw new Error(message); }
const keyword = (t: Token | undefined, word: string): boolean => t?.kind === 'word' && t.text.toUpperCase() === word;
const ident = (t: Token | undefined): string => {
  if (t?.kind === 'ident') return t.text.slice(1,-1).replaceAll('""','"');
  if (t?.kind === 'word') return t.text.toLowerCase();
  return fail('需要字段名或数据集名');
};
function tokenize(sql: string): Token[] {
  return lexSql(sql).filter(t => t.kind !== 'space').map(t => {
    if (!t.closed) fail(t.kind === 'comment' ? 'SQL 注释未结束' : 'SQL 引号未闭合');
    return t as Token;
  });
}
function parseWhere(tokens: Token[]): Equality[] {
  let at = 0;
  const symbol = (s: string): boolean => tokens[at]?.text === s;
  const take = (s: string): boolean => { if (symbol(s) || keyword(tokens[at], s)) {at++;return true;} return false; };
  const expect = (s: string): void => { if (!take(s)) fail('条件不完整，需要 '+s); };
  const value = (): LiteralValue => {
    let sign = '';
    if (symbol('+') || symbol('-')) sign = tokens[at++]!.text;
    const t = tokens[at++];
    if (t?.kind === 'number') return {type:'number',value:sign+t.text};
    if (sign) return fail('正负号后需要数字');
    if (t?.kind === 'string') return {type:'string',value:t.text.slice(1,-1).replaceAll("''", "'")};
    if (keyword(t,'TRUE') || keyword(t,'FALSE')) return {type:'boolean',value:t!.text.toLowerCase()};
    return fail('仅支持字符串、数字和布尔值的等于/不等于条件（不含 NULL）');
  };
  const atom = (): Equality[] => {
    if (take('(')) { const result = or(); expect(')'); return result; }
    const field = ident(tokens[at++]);
    let exclude = false;
    let values: LiteralValue[];
    if (take('=') || (exclude = take('!=') || take('<>'))) values = [value()];
    else {
      exclude = take('NOT'); expect('IN'); expect('('); values = [value()];
      while (take(',')) values.push(value());
      expect(')');
    }
    if (values.some(v => v.type !== values[0]!.type)) fail('IN 集合必须使用相同类型');
    values = values.filter((v,i) => !values.slice(0,i).some(x => sameValue(x,v)));
    return [{kind:'set',field,exclude,values}];
  };
  const and = (): Equality[] => { let groups = atom(); while (take('AND')) groups = [...groups,...atom()]; return groups; };
  const or = (): Equality[] => {
    let groups = and();
    while (take('OR')) {
      const other = and(), a = groups[0], b = other[0];
      if (groups.length !== 1 || other.length !== 1 || !a || !b || a.exclude || b.exclude || a.field !== b.field || a.values[0]?.type !== b.values[0]?.type) fail('仅支持同字段等于条件组成的 OR；请继续在 SQL 中编辑');
      groups = [{...a,values:[...a.values,...b.values.filter(v => !a.values.some(x => sameValue(x,v)))]}];
    }
    return groups;
  };
  const result = or();
  if (at !== tokens.length) fail('仅支持等于、不等于、IN、NOT IN 和 AND / 同字段 OR');
  return result;
}
export function decodeQuery(sql: string, parseFilters = true): QueryDecode {
  try {
    const all = tokenize(sql), tokens = all.filter(t => t.kind !== 'comment');
    if (!keyword(tokens[0],'SELECT')) fail('仅支持单数据集 SELECT 查询');
    let depth = 0;
    const top: number[] = [];
    for (let i = 0; i < tokens.length; i++) {
      const t = tokens[i]!;
      if (t.text === ';') { if (depth || i !== tokens.length-1) fail('不能转换多条 SQL 语句'); continue; }
      if (depth === 0) top.push(i);
      if (t.text === '(') depth++;
      if (t.text === ')' && --depth < 0) fail('SQL 括号不匹配');
    }
    if (depth) fail('SQL 括号未闭合');
    const from = top.find(i => keyword(tokens[i],'FROM')) ?? -1;
    if (from < 2) fail('SELECT 或 FROM 不完整');
    const table = tokens[from+1];
    const dataset = ident(table);
    const endIndex = tokens.at(-1)?.text === ';' ? tokens.length-1 : tokens.length;
    const clauses = top.filter(i => i > from+1);
    const starts: {index:number; words:number; name:string}[] = [];
    for (const i of clauses) {
      const t = tokens[i];
      if (keyword(t,'WHERE') || keyword(t,'LIMIT')) starts.push({index:i,words:1,name:t!.text.toUpperCase()});
      if ((keyword(t,'GROUP') || keyword(t,'ORDER')) && keyword(tokens[i+1],'BY')) starts.push({index:i,words:2,name:t!.text.toUpperCase()});
      if (['UNION','JOIN','HAVING','OFFSET','EXCEPT','INTERSECT'].some(k => keyword(t,k))) fail('该查询结构暂不支持条件转换');
    }
    if ((starts[0]?.index ?? endIndex) !== from+2) fail('仅支持单数据集，不支持别名或连接');
    let previous = -1;
    for (let i=0;i<starts.length;i++) {
      const c = starts[i]!, rank = ['WHERE','GROUP','ORDER','LIMIT'].indexOf(c.name);
      if (rank <= previous) fail('SQL 子句顺序或数量不正确');
      previous = rank;
      if (c.index+c.words >= (starts[i+1]?.index ?? endIndex)) fail(c.name+' 子句不完整');
      if (c.name === 'LIMIT') {
        const tail = tokens.slice(c.index+1,endIndex);
        if (tail.length !== 1 || tail[0]?.kind !== 'number' || !/^\d+$/.test(tail[0].text)) fail('LIMIT 需要非负整数');
      }
    }
    const where = starts.find(c => c.name === 'WHERE');
    const after = starts.find(c => c.name !== 'WHERE');
    const end = after ? tokens[after.index]!.start : tokens[endIndex]?.start ?? sql.length;
    const start = where ? tokens[where.index]!.start : end;
    const groups = where && parseFilters ? parseWhere(tokens.slice(where.index+1, after?.index ?? endIndex)) : [];
    // 只有标准记录查询可以展示基础记录的分布/总数。
    const order = starts.find(c => c.name === 'ORDER');
    const orderTokens = order ? tokens.slice(order.index+2,endIndex) : [];
    const timeOrder = orderTokens.length >= 1 && orderTokens.length <= 2
      && (orderTokens[0]?.kind === 'word' || orderTokens[0]?.kind === 'ident') && ident(orderTokens[0]) === '_timestamp'
      && (orderTokens.length === 1 || keyword(orderTokens[1],'ASC') || keyword(orderTokens[1],'DESC'));
    const normal = tokens.slice(1,from).map(t=>t.text).join('') === '*'
      && starts.every(c=>c.name === 'WHERE' || c.name === 'ORDER' && timeOrder);
    return {ok:true,sql,dataset,groups,start,end,comments:all.filter(t=>t.kind==='comment' && t.start>=start && t.end<=end).map(t=>t.text),normal,tableStart:table!.start,tableEnd:table!.end,orderStart:order ? tokens[order.index]!.start : null,tailEnd:tokens[endIndex]?.start ?? sql.length};
  } catch (error) { return {ok:false,sql,error:error instanceof Error ? error.message : '无法转换 SQL'}; }
}
/** 只改 WHERE，其他源片段原样保留。注释移到新 WHERE 前，行注释后保留换行。 */
export function rewriteWhere(q: DecodedQuery, conditions: readonly Condition[], original: readonly Condition[] = q.groups, utf8?: Utf8Fields): string {
  if (JSON.stringify(conditions) === JSON.stringify(original)) return q.sql;
  const comments = q.comments.map(c=>c+'\n').join('');
  return q.sql.slice(0,q.start) + '\n' + comments + whereClause(conditions, utf8).trimStart() + '\n' + q.sql.slice(q.end);
}

/** 保留现有记录表的时间排序入口；只替换排序段，不重建 WHERE 或投影。 */
export function sortRecordSql(sql: string, ascending: boolean): string {
  const q = decodeQuery(sql, false);
  if (!q.ok || !q.normal) return sql;
  const start = q.orderStart ?? q.tailEnd;
  const comments = tokenize(sql).filter(t => t.kind === 'comment' && t.start >= start && t.end <= q.tailEnd).map(t => t.text + '\n').join('');
  return sql.slice(0,start) + '\n' + comments + 'ORDER BY _timestamp ' + (ascending ? 'ASC' : 'DESC') + '\n' + sql.slice(q.tailEnd);
}

export {applySqlPanelFilter} from './sqlPanelFilters.ts';
