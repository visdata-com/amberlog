import assert from "node:assert/strict";
import test from "node:test";
import { lexSql } from "./sqlLexer.ts";

test("SQL 高亮扫描保留关键字、字符串、数字和注释", () => {
  assert.deepEqual(
    lexSql("SELECT 'app' -- note\nFROM app WHERE n >= 10"),
    [
      { kind: "word", text: "SELECT" },
      { kind: "space", text: " " },
      { kind: "string", text: "'app'" },
      { kind: "space", text: " " },
      { kind: "comment", text: "-- note" },
      { kind: "space", text: "\n" },
      { kind: "word", text: "FROM" },
      { kind: "space", text: " " },
      { kind: "word", text: "app" },
      { kind: "space", text: " " },
      { kind: "word", text: "WHERE" },
      { kind: "space", text: " " },
      { kind: "word", text: "n" },
      { kind: "space", text: " " },
      { kind: "symbol", text: ">=" },
      { kind: "space", text: " " },
      { kind: "number", text: "10" },
    ],
  );
});
