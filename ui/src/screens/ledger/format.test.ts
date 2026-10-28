import { test } from "node:test";
import assert from "node:assert/strict";
import { Num } from "../../lib/json.ts";
import { bytes, grouped } from "./format.ts";

const n = (t: string): Num => new Num(t);

test("条数加千分位", () => {
  assert.equal(grouped(n("1234567")), "1,234,567");
  assert.equal(grouped(n("999")), "999");
  assert.equal(grouped(n("1000")), "1,000");
  assert.equal(grouped(n("0")), "0");
  assert.equal(grouped(n("-12345")), "-12,345");
  assert.equal(grouped(n("18446744073709551615")), "18,446,744,073,709,551,615");
});

test("不是 JSON 整数时不改写", () => {
  assert.equal(grouped(null), null);
  assert.equal(grouped(undefined), null);
  assert.equal(grouped(n("1.5")), null);
  assert.equal(grouped("12"), null);
  assert.equal(bytes(null), null);
  assert.equal(bytes(n("-1")), null);
});

test("字节数按 1024 进位、保留一位小数", () => {
  assert.equal(bytes(n("1288490189")), "1.2 GiB");
  assert.equal(bytes(n("0")), "0 B");
  assert.equal(bytes(n("1023")), "1023 B");
  assert.equal(bytes(n("1024")), "1.0 KiB");
  assert.equal(bytes(n("1536")), "1.5 KiB");
  assert.equal(bytes(n("1048575")), "1.0 MiB");
  assert.equal(bytes(n("1099511627776")), "1.0 TiB");
  assert.equal(bytes(n("2251799813685248")), "2048.0 TiB");
});
