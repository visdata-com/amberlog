import { useLayoutEffect, useRef } from "preact/hooks";
import { lexSql } from "./sqlLexer.ts";
import "./sqlEditor.css";

const KEYWORDS = new Set([
  "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "IS", "NULL", "LIKE",
  "AS", "ORDER", "BY", "ASC", "DESC", "LIMIT", "GROUP", "HAVING", "COUNT",
]);

export function SqlHighlight({ sql, onSql, onCursor }: { sql: string; onSql: (value: string, cursor: number) => void; onCursor: (cursor: number) => void }) {
  const textarea = useRef<HTMLTextAreaElement>(null);
  const highlight = useRef<HTMLPreElement>(null);
  const sync = (): void => {
    if (textarea.current === null || highlight.current === null) return;
    highlight.current.scrollTop = textarea.current.scrollTop;
    highlight.current.scrollLeft = textarea.current.scrollLeft;
  };
  useLayoutEffect(() => sync(), [sql]);
  const tokens = lexSql(sql);
  return (
    <div className="sql-highlight-wrap">
      <pre ref={highlight} className="sql-highlight" aria-hidden="true">
        {tokens.map((token, index) => (
          <span key={index} className={token.kind === "word" && KEYWORDS.has(token.text.toUpperCase()) ? "sql-keyword" : `sql-${token.kind}`}>
            {token.text}
          </span>
        ))}
        {"\n"}
      </pre>
      <textarea
        ref={textarea}
        id="s-sql"
        spellcheck={false}
        aria-label="SQL"
        placeholder="输入 SQL，如 SELECT * FROM app"
        value={sql}
        onInput={(event) => onSql(event.currentTarget.value, event.currentTarget.selectionStart)}
        onClick={(event) => onCursor(event.currentTarget.selectionStart)}
        onKeyUp={(event) => onCursor(event.currentTarget.selectionStart)}
        onSelect={(event) => onCursor(event.currentTarget.selectionStart)}
        onScroll={sync}
      />
    </div>
  );
}
