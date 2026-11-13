import {createPortal, type TargetedKeyboardEvent as KeyboardEvent, type TargetedEvent as SyntheticEvent} from 'preact';
import {useLayoutEffect,useRef,useState} from 'preact/hooks';
import {completionAt,completionEdit,SQL_KEYWORDS,type SqlCandidate} from './sqlCompletion.ts';
import {lexSql} from './sqlLexer.ts';
import {CompletionSelections,insertCompletion} from './sqlEditing.ts';
import './sqlEditor.css';

export interface SqlEditorProps {
 sql:string; datasets:readonly string[]; fields:readonly string[];
 onSql:(text:string,cursor:number,inputType?:string)=>void;onCursor:(cursor:number)=>void;
 loading?:boolean; errors?:readonly string[];onRetry?:()=>void;
}
const labels={keyword:'关键字',dataset:'数据集',field:'字段'};
export function SqlEditor(p:SqlEditorProps){
 const input=useRef<HTMLTextAreaElement>(null),highlight=useRef<HTMLPreElement>(null),list=useRef<HTMLUListElement>(null);
 const history=useRef(new CompletionSelections()),previous=useRef(p.sql),composing=useRef(false),newline=useRef(false);
 const [view,setView]=useState({text:p.sql,start:0,end:0,open:false});
 const [pick,setPick]=useState(0),[position,setPosition]=useState({left:0,top:0,width:280,maxHeight:264,ready:false});
 const [editError,setEditError]=useState(false);
 const context=completionAt(p.sql,view.start,view.end,p.datasets,p.fields);
 const items=view.open&&view.text===p.sql&&!composing.current?context.items:[];
 const selected=Math.min(pick,Math.max(0,items.length-1));
 const close=():void=>setView(v=>({...v,open:false}));
 const sync=():void=>{
  const t=input.current,h=highlight.current;if(!t||!h)return;
  const border=getComputedStyle(t);
  const inputWidth=t.clientWidth+parseFloat(border.borderLeftWidth)+parseFloat(border.borderRightWidth);
  h.style.width=inputWidth+'px';
  h.scrollTop=t.scrollTop;h.scrollLeft=t.scrollLeft;
  if(!items.length)return;
  // Keep the original editor's mirror measurement: it matches textarea wrapping
  // without depending on the highlighted token DOM structure.
  const mirror=document.createElement('div'),style=getComputedStyle(t);
  for(const key of ['font','letter-spacing','line-height','padding','border','box-sizing','tab-size','word-break','overflow-wrap','white-space'])mirror.style.setProperty(key,style.getPropertyValue(key));
  Object.assign(mirror.style,{position:'fixed',visibility:'hidden',pointerEvents:'none',width:inputWidth+'px',height:'auto',left:'0',top:'0'});
  mirror.textContent=t.value.slice(0,t.selectionStart);
  const marker=document.createElement('span');marker.textContent='\u200b';mirror.append(marker);document.body.append(mirror);
  const rect=t.getBoundingClientRect(),line=parseFloat(style.lineHeight)||21;
  const x=rect.left+marker.offsetLeft-t.scrollLeft,y=rect.top+marker.offsetTop-t.scrollTop+line;
  mirror.remove();
  const width=Math.min(320,innerWidth-16),height=Math.min(items.length,8)*32+8;
  const top=y+height<=innerHeight-8?y:Math.max(8,y-height);
  setPosition({left:Math.max(8,Math.min(x,innerWidth-width-8)),top:Math.max(8,Math.min(top,innerHeight-40)),width,maxHeight:Math.min(height,innerHeight-16),ready:true});
 };
 useLayoutEffect(()=>{
  if(previous.current!==p.sql){history.current.clear();previous.current=p.sql;setView({text:p.sql,start:input.current?.selectionStart??0,end:input.current?.selectionEnd??0,open:false});setEditError(false);}
 },[p.sql]);
 useLayoutEffect(()=>{sync();},[p.sql,view.start,items.length]);
 useLayoutEffect(()=>{
  const t=input.current;if(!t)return;
  const resize=new ResizeObserver(sync);resize.observe(t);
  window.addEventListener('resize',sync);window.addEventListener('scroll',sync,true);
  return()=>{resize.disconnect();window.removeEventListener('resize',sync);window.removeEventListener('scroll',sync,true);};
 },[items.length,view.start]);
 useLayoutEffect(()=>{list.current?.children[selected]?.scrollIntoView({block:'nearest'});},[selected,items.length]);
 const changed=(t:HTMLTextAreaElement,type:string):void=>{
  const restore=history.current.input(type,previous.current,t.value);
  if(restore)t.setSelectionRange(...restore);
  previous.current=t.value;
  setView({text:t.value,start:t.selectionStart,end:t.selectionEnd,open:!composing.current&&!newline.current&&type!=='historyUndo'&&type!=='historyRedo'});
  newline.current=false;setPick(0);setEditError(false);p.onSql(t.value,t.selectionStart,type);
 };
 const cursor=(e:SyntheticEvent<HTMLTextAreaElement>):void=>{
  const t=e.currentTarget;p.onCursor(t.selectionStart);
  setView(v=>({...v,start:t.selectionStart,end:t.selectionEnd,open:v.open&&t.selectionStart===t.selectionEnd}));
 };
 const choose=(item:SqlCandidate):void=>{
  const t=input.current;if(!t||composing.current)return;
  const edit=completionEdit(t.value,t.selectionStart,t.selectionEnd,context,item);if(!edit){close();return;}
  const before=t.value,start=[t.selectionStart,t.selectionEnd] as const;
  const inserted=edit.text.slice(context.start,edit.cursor);
  if(edit.text!==before){
   if(!insertCompletion(t,context.start,context.end,inserted)){setEditError(true);close();return;}
   history.current.record(before,t.value,start,[t.selectionStart,t.selectionEnd]);
  }
  t.setSelectionRange(edit.cursor,edit.cursor);p.onCursor(edit.cursor);close();
 };
 const key=(e:KeyboardEvent<HTMLTextAreaElement>):void=>{
  if(composing.current||e.isComposing||e.keyCode===229)return;
  if(e.key==='Enter'){
   if(items.length){e.preventDefault();choose(items[selected]!);return;}
   newline.current=true;close();return;
  }
  if(!items.length)return;
  if(e.key==='ArrowDown'||e.key==='ArrowUp'){e.preventDefault();setPick((selected+(e.key==='ArrowDown'?1:items.length-1))%items.length);}
  else if(e.key==='Tab'){e.preventDefault();choose(items[selected]!);}
  else if(e.key==='Escape'){e.preventDefault();e.stopPropagation();close();}
 };
 return <div className="sql-editor">
  <div className="sql-input-wrap">
   <pre ref={highlight} className="sql-highlight" aria-hidden="true">{lexSql(p.sql).map(t=><span key={t.start} className={'sql-'+(t.kind==='word'&&SQL_KEYWORDS.has(t.text.toUpperCase())?'keyword':t.kind)}>{t.text}</span>)}{'\n'}</pre>
   <textarea ref={input} id="s-sql" spellcheck={false} autoComplete="off" aria-label="SQL" role={"combobox" as "textbox"} aria-multiline="true" aria-autocomplete="list" aria-expanded={items.length>0} aria-controls={items.length?'sql-options':undefined} aria-activedescendant={items.length?'sql-option-'+selected:undefined}
    placeholder="输入 SQL，如 SELECT * FROM app" value={p.sql}
    onInput={e=>changed(e.currentTarget,(e as InputEvent).inputType)} onKeyDown={key} onSelect={cursor} onClick={cursor} onKeyUp={cursor} onScroll={sync} onBlur={close}
    onCompositionStart={()=>{composing.current=true;close();}}
    onCompositionEnd={e=>{composing.current=false;changed(e.currentTarget,'insertCompositionText');}}/>
  </div>
  {p.loading&&<span className="cond-guide" role="status">正在加载字段…</span>}
  {!!p.errors?.length&&<div role="status" className="cond-error">{p.errors.join('；')} <button type="button" onClick={p.onRetry}>重试加载字段</button></div>}
  {editError&&<div role="alert" className="cond-error">当前浏览器未能插入补全，请继续手动输入。</div>}
  {!!items.length&&position.ready&&createPortal(<ul ref={list} id="sql-options" role="listbox" aria-label="SQL 补全" className="sql-options" style={`left:${position.left}px;top:${position.top}px;width:${position.width}px;max-height:${position.maxHeight}px`}>
   {items.map((item,i)=><li key={item.id} id={'sql-option-'+i} role="option" aria-selected={i===selected} onMouseDown={e=>{e.preventDefault();choose(item);}}><span>{item.label}</span><small>{labels[item.kind]}</small></li>)}
  </ul>,document.body)}
 </div>;
}
