export type SqlToken = {
  kind: "word" | "ident" | "string" | "number" | "symbol" | "comment" | "space";
  text: string;
};

/** Lossless, tolerant SQL scanner used only for syntax highlighting. */
export function lexSql(sql: string): SqlToken[] {
  const tokens: SqlToken[] = [];
  let i = 0;
  while (i < sql.length) {
    const start = i;
    let kind: SqlToken["kind"] = "symbol";
    if (/\s/.test(sql[i]!)) {
      kind = "space";
      while (i < sql.length && /\s/.test(sql[i]!)) i++;
    } else if (sql.startsWith("--", i)) {
      kind = "comment";
      i = sql.indexOf("\n", i);
      if (i < 0) i = sql.length;
    } else if (sql.startsWith("/*", i)) {
      kind = "comment";
      const end = sql.indexOf("*/", i + 2);
      i = end < 0 ? sql.length : end + 2;
    } else if (sql[i] === "'" || sql[i] === '"') {
      const quote = sql[i++]!;
      kind = quote === "'" ? "string" : "ident";
      while (i < sql.length) {
        if (sql[i++] !== quote) continue;
        if (sql[i] === quote) i++;
        else break;
      }
    } else {
      const rest = sql.slice(i);
      const number = /^(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?/.exec(rest);
      const word = /^[A-Za-z_\u0080-\uffff][A-Za-z0-9_$\u0080-\uffff]*/.exec(rest);
      if (number) {
        kind = "number";
        i += number[0].length;
      } else if (word) {
        kind = "word";
        i += word[0].length;
      } else {
        i += /^(?:!=|<>|<=|>=)/.test(rest) ? 2 : 1;
      }
    }
    tokens.push({ kind, text: sql.slice(start, i) });
  }
  return tokens;
}
