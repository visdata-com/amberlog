import {lexSql, type SqlToken} from './sqlLexer.ts';
import {decodeQuery} from './queryCodec.ts';
import {equalitySql, updateEquality, type Equality} from './equality.ts';
import {panelLiteral, UNSUPPORTED_PANEL_VALUE, type PanelValue} from './fieldConditions.ts';

type Expr = {start:number;end:number;refs:Set<string>;kind:'atom'|'and'|'or'|'not';parts:Expr[]};
const name = (t:SqlToken):string => t.kind === 'ident' ? t.text.slice(1,-1).replaceAll('""','"') : t.text.toLowerCase();
const error = ():never => {throw new Error('SQL 条件未完成或超出安全更新范围，请在 SQL 中编辑');};
/** A bounded expression reader, not a general SQL validator. Every consumed token has a known role. */
function expression(tokens:SqlToken[]):Expr {
 let at=0;
 const word=(s:string):boolean=>tokens[at]?.kind==='word' && tokens[at]!.text.toUpperCase()===s;
 const take=(s:string):boolean=>{if(word(s)||tokens[at]?.text===s){at++;return true;}return false;};
 const expect=(s:string):void=>{if(!take(s))error();};
 const node=(start:number,kind:Expr['kind'],parts:Expr[],refs=new Set<string>()):Expr=>({start:tokens[start]!.start,end:tokens[at-1]!.end,kind,parts,refs:new Set([...refs,...parts.flatMap(p=>[...p.refs])])});
 const scalar=():Set<string>=>{
  const refs=new Set<string>();
  if(take('+')||take('-')) {if(tokens[at]?.kind!=='number')error();at++;return refs;}
  const t=tokens[at];if(!t) return error();
  if(t.kind==='number'||t.kind==='string'||word('TRUE')||word('FALSE')||word('NULL')) {at++;return refs;}
  if(t.kind!=='ident' && t.kind!=='word')return error();
  if(t.kind==='word' && /^(AND|OR|NOT|IN|BETWEEN|LIKE|IS|SELECT|FROM|WHERE|CASE|END|AS)$/.test(t.text.toUpperCase()))return error();
  at++;
  if(take('(')) {
   if(!take(')')){for(const ref of scalar())refs.add(ref);while(take(','))for(const ref of scalar())refs.add(ref);expect(')');}
  } else refs.add(name(t));
  return refs;
 };
 const atom=():Expr=>{
  const start=at;
  if(take('NOT'))return node(start,'not',[atom()]);
  if(take('(')){const p=or();expect(')');return {...p,start:tokens[start]!.start,end:tokens[at-1]!.end};}
  const refs=scalar();const add=()=>{for(const ref of scalar())refs.add(ref);};
  if(['=','!=','<>','<','>','<=','>='].includes(tokens[at]?.text??'')){at++;add();}
  else if(take('IS')){take('NOT');expect('NULL');}
  else {
   const negated=take('NOT');
   if(take('BETWEEN')){add();expect('AND');add();}
   else if(take('IN')){expect('(');add();while(take(','))add();expect(')');}
   else if(take('LIKE')){add();}
   else if(negated)error();
   // A bare name may be an unfinished edit; accept only explicit booleans or function calls.
   else if(tokens[start+1]?.text!=='(' && !/^(TRUE|FALSE)$/i.test(tokens[start]!.text))error();
  }
  return node(start,'atom',[],refs);
 };
 const and=():Expr=>{const start=at,parts=[atom()];while(take('AND'))parts.push(atom());return parts.length===1?parts[0]!:node(start,'and',parts);};
 const or=():Expr=>{const start=at,parts=[and()];while(take('OR'))parts.push(and());return parts.length===1?parts[0]!:node(start,'or',parts);};
 if(!tokens.length)return error();const result=or();if(at!==tokens.length)error();return result;
}
export type SqlPanelResult = {kind:'changed';sql:string;dataset:string}|{kind:'unchanged'}|{kind:'error';error:string};
export function applySqlPanelFilter(sql:string,field:string,exclude:boolean,value:PanelValue):SqlPanelResult {
 try {
  const literal=panelLiteral(value);if(!literal)return {kind:'error',error:UNSUPPORTED_PANEL_VALUE};
  const q=decodeQuery(sql,false);if(!q.ok)return {kind:'error',error:q.error};
  const all=lexSql(sql);const tokens=all.filter(t=>t.kind!=='space'&&t.kind!=='comment');
  // Structural decoding locates clauses but does not reject a dangling list/operator.
  // Preserve tail expressions verbatim; only require their lexical end to be complete.
  const completeEnd=(part:SqlToken[], wildcard=false):void=>{
   const last=part.at(-1);
   if(!last || last.kind==='symbol' && last.text!==')' && !(wildcard && part.length===1 && last.text==='*')
     || last.kind==='word' && /^(AS|AND|OR|NOT|BETWEEN|IN|LIKE|IS|BY)$/.test(last.text.toUpperCase()))error();
  };
  completeEnd(tokens.filter(t=>t.start>tokens[0]!.start && t.end<=q.tableStart).slice(0,-1),true);
  let depth=0;const tails:number[]=[];
  tokens.forEach((t,i)=>{
   if(depth===0 && t.start>=q.tableEnd && t.kind==='word' && ['GROUP','ORDER','LIMIT'].includes(t.text.toUpperCase()))tails.push(i);
   if(t.text==='(')depth++;if(t.text===')')depth--;
  });
  for(let i=0;i<tails.length;i++){
   const start=tails[i]!;const words=tokens[start]!.text.toUpperCase()==='LIMIT'?1:2;
   completeEnd(tokens.slice(start+words,tails[i+1]??tokens.length).filter(t=>t.text!==';'));
  }
  const where=tokens.find(t=>t.start===q.start && t.kind==='word'&&t.text.toUpperCase()==='WHERE');
  if(!where){
   const predicate=equalitySql({kind:'set',field,exclude,values:[literal]});
   const before=sql.slice(0,q.start), after=sql.slice(q.start);
   const last=all.filter(t=>t.end<=q.start && t.kind!=='space').at(-1);
   // Only an unterminated line comment needs a new line to keep WHERE executable.
   const inLineComment=last?.kind==='comment' && last.text.startsWith('--') && !before.slice(last.end).includes('\n');
   const separator=inLineComment?'\n':/\s$/.test(before)?'':' ';
   return {kind:'changed',dataset:q.dataset,sql:before+separator+'WHERE '+predicate+(after && !after.startsWith(';')?' ':'')+after};
  }
  const bodyTokens=tokens.filter(t=>t.start>=where.end && t.start<q.end);
  const root=expression(bodyTokens);
  const flatten=(e:Expr):Expr[]=>e.kind==='and'?e.parts.flatMap(flatten):[e];
  const terms=flatten(root);
  const entries=terms.map(term=>{
   const parsed=decodeQuery('SELECT * FROM t WHERE '+sql.slice(term.start,term.end));
   const group=parsed.ok && parsed.groups.length===1 ? parsed.groups[0]! : null;
   return {term,group};
  });
  for(const {term,group} of entries)if(!group && term.refs.has(field))throw new Error('该字段位于复杂表达式中，请在 SQL 中明确目标条件');
  const groups=entries.flatMap(e=>e.group?[e.group]:[]);
  const next=updateEquality(groups,field,exclude,literal);if(next===groups)return {kind:'unchanged'};
  const used=new Set<Equality>();
  const comments=(start:number,end:number)=>all.filter(t=>t.kind==='comment'&&t.start>=start&&t.end<=end).map(t=>t.text+'\n').join('');
  const pieces:string[]=[];
  for(let i=0;i<entries.length;i++){
   const {term,group}=entries[i]!;
   // Keep inter-term comments even when the term they preceded is removed.
   const gapStart=i===0?where.end:entries[i-1]!.term.end;
   const before=comments(gapStart,term.start);
   if(!group){pieces.push(before+sql.slice(term.start,term.end));continue;}
   const replacement=next.find(g=>g===group || g.field===group.field && g.values[0]?.type===group.values[0]?.type && g.exclude===group.exclude)
    ?? next.find(g=>!used.has(g) && g.field===group.field && g.values[0]?.type===group.values[0]?.type && !groups.some(old=>old.field===g.field&&old.values[0]?.type===g.values[0]?.type&&old.exclude===g.exclude));
   if(!replacement||used.has(replacement)){const kept=before+comments(term.start,term.end);if(kept)pieces.push(kept+'TRUE');continue;}
   used.add(replacement);
   pieces.push(before+(replacement===group ? sql.slice(term.start,term.end) : comments(term.start,term.end)+equalitySql(replacement)));
  }
  for(const g of next)if(!used.has(g))pieces.push(equalitySql(g));
  // Parenthesize retained OR terms, including nested groups flattened out of an AND.
  const safe=pieces.map(piece=>{
   const ts=lexSql(piece).filter(t=>t.kind!=='space'&&t.kind!=='comment');
   const parsed=expression(ts);return parsed.kind==='or'?'('+piece+'\n)':piece;
  });
  const trailing=comments(entries.at(-1)!.term.end,root.end)+sql.slice(root.end,q.end);
  return {kind:'changed',dataset:q.dataset,sql:sql.slice(0,where.end)+' '+safe.join(' AND ')+trailing+sql.slice(q.end)};
 }catch(e){return {kind:'error',error:e instanceof Error?e.message:'无法更新条件'};}
}
