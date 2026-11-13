import {lexSql, type SqlToken} from './sqlLexer.ts';
import {quoteIdent} from './sql.ts';
import {isStreamName,byCodePoint} from './fields.ts';

export const SQL_KEYWORDS = new Set(['SELECT','FROM','WHERE','GROUP','BY','ORDER','LIMIT','AS','AND','OR','NOT','IN','IS','NULL','LIKE','ASC','DESC','TRUE','FALSE']);
const fold = (s:string):string => s.replace(/[A-Z]/g,c=>c.toLowerCase());
const word = (t:SqlToken|undefined):string => t?.kind === 'word' ? t.text.toUpperCase() : '';
const name = (t:SqlToken):string => t.kind === 'ident' ? t.text.slice(1,t.closed ? -1 : undefined).replaceAll('""','"') : fold(t.text);
const meaningful = (sql:string):SqlToken[] => lexSql(sql).filter(t=>t.kind!=='space'&&t.kind!=='comment');
export type FieldSource = {kind:'none'|'pending'|'unsupported'}|{kind:'resolved';dataset:string};

/** confirmed is only for complete external/pasted text, never each typed prefix. */
export function fieldSource(sql:string,fallback:string|null,confirmed=false,caret?:number):FieldSource {
 const all=meaningful(sql),top:SqlToken[]=[];let depth=0;
 for(let i=0;i<all.length;i++){
  const t=all[i]!;
  if(t.text===';'&&i!==all.length-1)return {kind:'unsupported'};
  if(depth===0)top.push(t);
  if(t.text==='(')depth++;
  if(t.text===')')depth--;
 }
 if(top.some(t=>['JOIN','UNION','INTERSECT','EXCEPT','WITH'].includes(word(t))))return {kind:'unsupported'};
 const from=top.findIndex(t=>word(t)==='FROM');
 if(from<0)return fallback&&isStreamName(fallback)?{kind:'resolved',dataset:fallback}:{kind:'none'};
 if(top.filter(t=>word(t)==='FROM').length!==1)return {kind:'unsupported'};
 const table=top[from+1];
 if(!table)return {kind:'pending'};
 if(!['word','ident'].includes(table.kind))return {kind:'unsupported'};
 const after=top[from+2];
 if(after&&after.text!==';'&&!['WHERE','GROUP','ORDER','LIMIT'].includes(word(after)))return {kind:'unsupported'};
 if(!confirmed&&caret!==undefined&&caret>table.start&&(caret<table.end||caret===table.end&&table.kind==='word'))return {kind:'pending'};
 if(!table.closed||!confirmed&&table.kind==='word'&&table.end===sql.length)return {kind:'pending'};
 const dataset=name(table);
 return isStreamName(dataset)?{kind:'resolved',dataset}:{kind:'unsupported'};
}

export type SqlCandidate = {id:string;kind:'keyword'|'dataset'|'field';label:string;insertText:string};
export type CompletionContext = {sql:string;caret:number;start:number;end:number;items:SqlCandidate[]};
export function completionAt(sql:string,caret:number,end:number,datasets:readonly string[],fields:readonly string[]):CompletionContext {
 const result:CompletionContext={sql,caret,start:caret,end:caret,items:[]};
 if(caret!==end)return result;
 const tokens=lexSql(sql);
 const active=tokens.find(t=>t.start<caret&&caret<=t.end&&t.kind!=='space');
 if(active&&['comment','string','number'].includes(active.kind))return result;
 const token=active&&['word','ident'].includes(active.kind)?active:undefined;
 if(token){result.start=token.start;result.end=token.end;}
 const prefixEnd=token?.kind==='ident'&&token.closed&&caret===token.end?caret-1:caret;
 const prefix=token?sql.slice(token.start+(token.kind==='ident'?1:0),prefixEnd).replaceAll('""','"'):'';
 const before=meaningful(sql.slice(0,result.start));
 const last=before.at(-1);const prev=word(last);
 let clause='';let depth=0;
 const brackets:boolean[]=[];
 for(let i=0;i<before.length;i++){const t=before[i]!;if(t.text==='(')brackets.push(word(before[i-1])==='IN'||brackets.at(-1)===true);if(t.text===')')brackets.pop();}
 if(brackets.at(-1))return result;
 for(const t of before){if(t.text==='(')depth++;if(t.text===')')depth--;if(depth===0&&['SELECT','FROM','WHERE','GROUP','ORDER','LIMIT'].includes(word(t)))clause=word(t);}
 let keywords:string[]=[];let names:readonly string[]=[];let kind:'field'|'dataset'='field';
 if(!before.length)keywords=['SELECT'];
 else if(prev==='FROM'){names=datasets;kind='dataset';}
 else if(prev==='GROUP'||prev==='ORDER')keywords=['BY'];
 else if(clause==='FROM')keywords=['WHERE','GROUP','ORDER','LIMIT'];
 else if(clause==='LIMIT')keywords=[];
 else if(['=','!=','<>','<','>','<=','>='].includes(last?.text??'')||['LIKE','IN'].includes(prev))keywords=['NULL','TRUE','FALSE'];
 else if(prev==='IS')keywords=['NOT','NULL'];
 else if(prev==='NOT'){
  const preceding=before.at(-2),keyword=word(preceding);
  if(['WHERE','AND','OR','NOT'].includes(keyword)||preceding?.text==='('){names=fields;keywords=['NOT'];}
  else keywords=keyword==='IS'?['NULL']:['IN','LIKE'];
 }
 else if(clause==='SELECT'){
  const fieldPosition=prev==='SELECT'||last?.text===','||last?.text==='(';
  if(fieldPosition)names=fields;else keywords=['FROM','AS'];
 }else if(clause==='WHERE'){
  const fieldPosition=prev==='WHERE'||prev==='AND'||prev==='OR'||last?.text==='(';
  if(fieldPosition){names=fields;keywords=['NOT'];}
  else keywords=['AND','OR','IN','NOT','IS','LIKE','GROUP','ORDER','LIMIT'];
 }else if(clause==='GROUP'||clause==='ORDER'){
  if(prev==='BY'||last?.text===',')names=fields;
  else keywords=clause==='ORDER'?['ASC','DESC','LIMIT']:['ORDER','LIMIT'];
 }
 if(!prefix&&kind!=='dataset')return result;
 const candidates:SqlCandidate[]=[...new Set(names)].map(label=>({id:kind+':'+label,kind,label,insertText:quoteIdent(label)}));
 if(token?.kind!=='ident')for(const label of keywords)candidates.push({id:'keyword:'+label,kind:'keyword',label,insertText:label});
 result.items=candidates.filter(c=>fold(c.label).startsWith(fold(prefix))).sort((a,b)=>{
  const category=Number(a.kind==='keyword')-Number(b.kind==='keyword');
  return category||byCodePoint(a.label,b.label);
 });
 return result;
}
export function completionEdit(sql:string,start:number,end:number,context:CompletionContext,item:SqlCandidate):{text:string;cursor:number}|null {
 if(sql!==context.sql||start!==end||start!==context.caret||!context.items.some(c=>c.id===item.id))return null;
 const space=item.kind==='keyword'&&!/^\s/.test(sql.slice(context.end))?' ':'';
 const insert=item.insertText+space;
 return {text:sql.slice(0,context.start)+insert+sql.slice(context.end),cursor:context.start+insert.length};
}
