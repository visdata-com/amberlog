import { test } from "node:test";
import assert from "node:assert/strict";
import {
  type Condition, parseCondition, literal, searchSql, histogramSql, countSql, valuesSql, whereClause,
  detailCondition, insertAtCursor, mentionsFulltext, fromTable, timeColumns, conditionSql,
} from "./sql.ts";

const parseAll = (inputs: string[]): Condition[] => inputs.map((s) => {
  const c = parseCondition(s);
  assert.notEqual(c, null, s);
  return c!;
});

test("字段条件的切分（块场景）", () => {
  const conds = parseAll(["status>=500", "host.name = a b", "msg like %err%", "user Is Not Null"]);
  assert.equal(searchSql("app", conds, false),
    "SELECT * FROM \"app\" WHERE \"status\" >= 500 AND \"host.name\" = 'a b' AND \"msg\" LIKE '%err%' AND \"user\" IS NOT NULL");
});

test("不带运算的文字成为全文条件（块场景）", () => {
  const conds = parseAll(["timeout", "it's", "\"user=alice\"", "= 5"]);
  assert.equal(searchSql("app", conds, false),
    "SELECT * FROM \"app\" WHERE match_all('timeout') AND match_all('it''s') AND match_all('user=alice') AND match_all('= 5')");
});

test("条件写成 SQL：标识符中的双引号（块场景）", () => {
  const conds = parseAll(["a\"b = 1", "x"]);
  assert.equal(searchSql("app", conds, false), "SELECT * FROM \"app\" WHERE \"a\"\"b\" = 1 AND match_all('x')");
  assert.equal(searchSql("a\"pp", [], false), "SELECT * FROM \"a\"\"pp\"");
});

test("字面量的写法（块场景）", () => {
  const conds = parseAll(["code = \"404\"", "n = -3.5", "ok = true", "name = O'Brien", "p LIKE \"a%\""]);
  assert.equal(searchSql("app", conds, false),
    "SELECT * FROM \"app\" WHERE \"code\" = '404' AND \"n\" = -3.5 AND \"ok\" = true AND \"name\" = 'O''Brien' AND \"p\" LIKE '\"a%\"'");
});

test("运算：第一个出现、同一位置较长者优先", () => {
  assert.deepEqual(parseCondition("a<=1"), { kind: "field", field: "a", op: "<=", value: "1" });
  assert.deepEqual(parseCondition("a!=b=c"), { kind: "field", field: "a", op: "!=", value: "b=c" });
  assert.deepEqual(parseCondition("a=b!=c"), { kind: "field", field: "a", op: "=", value: "b!=c" });
  assert.deepEqual(parseCondition("a<>b"), { kind: "field", field: "a", op: "<", value: ">b" });
  assert.deepEqual(parseCondition("x like y = z"), { kind: "field", field: "x", op: "LIKE", value: "y = z" });
  assert.deepEqual(parseCondition("x = y like z"), { kind: "field", field: "x", op: "=", value: "y like z" });
});

test("LIKE 要前后有空白", () => {
  assert.deepEqual(parseCondition("unlikely"), { kind: "text", text: "unlikely" });
  assert.deepEqual(parseCondition("a LIKEb"), { kind: "text", text: "a LIKEb" });
  assert.deepEqual(parseCondition("a\tLIKE\tb"), { kind: "field", field: "a", op: "LIKE", value: "b" });
});

test("IS NULL 与 IS NOT NULL", () => {
  assert.deepEqual(parseCondition("user is null"), { kind: "field", field: "user", op: "IS NULL", value: "" });
  assert.deepEqual(parseCondition("  a=b IS NOT NULL "), { kind: "field", field: "a=b", op: "IS NOT NULL", value: "" });
  assert.equal(conditionSql(parseCondition("user is null")!), "\"user\" IS NULL");
  // 只一个词时不是字段条件
  assert.deepEqual(parseCondition("IS NULL"), { kind: "text", text: "IS NULL" });
});

test("字段名或取值为空时按全文条件", () => {
  assert.deepEqual(parseCondition("a ="), { kind: "text", text: "a =" });
  assert.deepEqual(parseCondition(">= 3"), { kind: "text", text: ">= 3" });
  assert.deepEqual(parseCondition("x LIKE  "), { kind: "text", text: "x LIKE" });
});

test("双引号括起的整段是全文条件；中间有双引号的不算", () => {
  assert.deepEqual(parseCondition("  \"a = b\"  "), { kind: "text", text: "a = b" });
  assert.deepEqual(parseCondition("\"a\" = \"b\""), { kind: "field", field: "\"a\"", op: "=", value: "\"b\"" });
  assert.equal(parseCondition("\"\""), null);
  assert.equal(parseCondition("   "), null);
});

test("数值字面量按十进制写法", () => {
  for (const v of ["42", "+1", "-0", "3.", ".5", "1e3", "-1.5E-7"]) assert.equal(literal("=", v), v);
  for (const v of ["0x10", "1_000", ".", "1e", "e3", "--1", "Infinity", "NaN"]) assert.equal(literal("=", v), "'" + v + "'");
  assert.equal(literal("=", "TRUE"), "'TRUE'");
  assert.equal(literal("LIKE", "5"), "'5'");
  assert.equal(literal("=", "\""), "'\"'");
  assert.equal(literal("=", "\"\""), "''");
});

test("排序、分布请求与计数请求的 SQL", () => {
  const conds = parseAll(["status >= 500"]);
  assert.equal(searchSql("app", [], true), "SELECT * FROM \"app\" ORDER BY _timestamp ASC");
  assert.equal(histogramSql("app", whereClause(conds), "1m"),
    "SELECT histogram(_timestamp, '1m') AS t, count(*) AS n FROM \"app\" WHERE \"status\" >= 500 GROUP BY histogram(_timestamp, '1m') ORDER BY t");
  assert.equal(countSql("app", ""), "SELECT count(*) AS n FROM \"app\"");
});

test("从详情加入：字符串带双引号，其余取原始 JSON 文本（块场景）", () => {
  const conds = [
    detailCondition("code", false, { json: "\"404\"", isString: true, text: "404" }),
    detailCondition("status", true, { json: "502", isString: false, text: "502" }),
  ];
  assert.equal(searchSql("app", conds, false), "SELECT * FROM \"app\" WHERE \"code\" = '404' AND \"status\" != 502");
});

test("SQL 模式下插到光标处（块场景）", () => {
  const sql = "SELECT * FROM \"app\" WHERE  LIMIT 5";
  const cursor = sql.indexOf("WHERE ") + "WHERE ".length;
  const r = insertAtCursor(sql, cursor, "\"host\" = 'h1'");
  assert.equal(r.sql, "SELECT * FROM \"app\" WHERE \"host\" = 'h1' LIMIT 5");
  assert.equal(insertAtCursor("SELECT * FROM t", null, "x").sql, "SELECT * FROM t x");
  assert.equal(insertAtCursor("", null, "x").sql, "x");
  assert.equal(insertAtCursor("ab", 1, "x").sql, "a xb");
});

test("SQL 模式的全文提示按不区分大小写的文字认", () => {
  assert.ok(mentionsFulltext("SELECT * FROM \"app\" WHERE MATCH_ALL('timeout')"));
  assert.ok(mentionsFulltext("select * from app where Str_Match(a, 'x')"));
  assert.ok(!mentionsFulltext("SELECT * FROM app"));
});

test("系统表名：去掉注释后第一个 FROM 之后的名字", () => {
  assert.equal(fromTable("SELECT scheduled_at, attempts FROM _alert_evaluations"), "_alert_evaluations");
  assert.equal(fromTable("SELECT window_end FROM _reconciliation WHERE dataset = 'default/app'"), "_reconciliation");
  assert.equal(fromTable("select * from _LEDGER"), "_ledger");
  assert.equal(fromTable("SELECT * FROM \"_LEDGER\""), "_LEDGER");
  assert.equal(fromTable("SELECT * /* FROM _ledger */ FROM app"), "app");
  assert.equal(fromTable("SELECT * -- FROM _ledger\nFROM app"), "app");
  assert.equal(fromTable("SELECT 'from _ledger' FROM app"), "app");
  assert.equal(fromTable("SELECT 1"), null);
});

test("系统表的时刻列，含 AS 改名所得的同名列", () => {
  assert.deepEqual([...timeColumns("SELECT record_count AS time_min FROM _ledger")].sort(),
    ["_timestamp", "expires_at", "time_max", "time_min"]);
  assert.ok(timeColumns("SELECT window_end FROM _reconciliation").has("window_end"));
  assert.ok(!timeColumns("SELECT * FROM \"_Ledger\"").has("time_min"));
  assert.deepEqual([...timeColumns("SELECT * FROM app")], ["_timestamp"]);
});

test("按字段表的类型写字面量（V2 块场景）", () => {
  const utf8 = (f: string): boolean => f === "code";
  const conds = parseAll(["code = 404", "n = 7", "code != \"5\""]);
  assert.equal(searchSql("typed", conds, false, utf8),
    "SELECT * FROM \"typed\" WHERE \"code\" = '404' AND \"n\" = 7 AND \"code\" != '5'");
  // Utf8 字段上 LIKE、IS NULL 照旧
  assert.equal(literal("LIKE", "4%", true), "'4%'");
  assert.equal(literal("=", "true", true), "'true'");
  assert.equal(searchSql("typed", parseAll(["code is null"]), false, utf8), "SELECT * FROM \"typed\" WHERE \"code\" IS NULL");
});

test("取值请求的 SQL（V2 块场景）", () => {
  assert.equal(valuesSql("app", whereClause(parseAll(["status >= 500"])), "level"),
    "SELECT \"level\", count(*) AS n__ FROM \"app\" WHERE \"status\" >= 500 AND \"level\" IS NOT NULL GROUP BY \"level\" ORDER BY n__ DESC LIMIT 10");
  assert.equal(valuesSql("app", "", "level"),
    "SELECT \"level\", count(*) AS n__ FROM \"app\" WHERE \"level\" IS NOT NULL GROUP BY \"level\" ORDER BY n__ DESC LIMIT 10");
  assert.equal(valuesSql("a\"p", "", "x\"y"),
    "SELECT \"x\"\"y\", count(*) AS n__ FROM \"a\"\"p\" WHERE \"x\"\"y\" IS NOT NULL GROUP BY \"x\"\"y\" ORDER BY n__ DESC LIMIT 10");
});
