/** Lossless, tolerant scanner. Positions use textarea UTF-16 offsets. */
export type SqlToken = {kind: 'word'|'ident'|'string'|'number'|'symbol'|'comment'|'space';text:string;start:number;end:number;closed:boolean};
export function lexSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  while (i < sql.length) {
    const start = i;
    let kind: SqlToken['kind'] = 'symbol';
    let closed = true;
    if (/\s/.test(sql[i]!)) { kind = 'space'; while (i < sql.length && /\s/.test(sql[i]!)) i++; }
    else if (sql.startsWith('--', i)) { kind = 'comment'; i = sql.indexOf('\n', i); if (i < 0) i = sql.length; }
    else if (sql.startsWith('/*', i)) { kind = 'comment'; const end = sql.indexOf('*/',i+2); closed = end >= 0; i = closed ? end+2 : sql.length; }
    else if (sql[i] === "'" || sql[i] === '"') {
      const quote = sql[i++]!; kind = quote === "'" ? 'string' : 'ident';
      closed = false;
      while (i < sql.length) {
        if (sql[i++] !== quote) continue;
        if (sql[i] === quote) { i++; continue; }
        closed = true; break;
      }
    } else {
      const rest = sql.slice(i);
      const number = /^(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(rest);
      const word = /^[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*/.exec(rest);
      if (number) { kind = 'number'; i += number[0].length; }
      else if (word) { kind = 'word'; i += word[0].length; }
      else { i += /^(?:!=|<>|<=|>=)/.test(rest) ? 2 : 1; }
    }
    tokens.push({text:sql.slice(start,i),start,end:i,kind,closed});
  }
  return tokens;
}
