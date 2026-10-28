import { test } from "node:test";
import assert from "node:assert/strict";
import type { Outcome } from "./api.ts";
import { errorLines, isTooMany, retrySeconds } from "./errors.ts";

const http = (status: number, reason: string | null, message: string | null, retryAfter: string | null = null): Outcome =>
  ({ kind: "http", status, reason, message, retryAfter });

test("出错时各条的文字", () => {
  assert.deepEqual(errorLines(http(429, "too_many_queries", "m")), ["服务繁忙：同时执行的查询已满"]);
  assert.deepEqual(errorLines(http(400, "query_memory_exceeded", "m")), [
    "这次查询需要的内存超过上限。请缩小时间范围，或去掉全文条件、改用字段条件",
  ]);
  assert.deepEqual(errorLines(http(400, "invalid_sql", "near SELEC")), ["查询写法有误", "invalid_sql", "near SELEC"]);
  assert.deepEqual(errorLines(http(503, "unavailable", "store down")), [
    "暂时无法读取数据（对象存储不可访问或服务正在停止）",
    "store down",
  ]);
  assert.deepEqual(errorLines(http(401, "unauthorized", "missing")), ["请求失败", "401", "unauthorized", "missing"]);
  assert.deepEqual(errorLines(http(502, null, null)), ["请求失败", "502"]);
  // 不是错误信封的 400、503 与 reason 不对的 429 都按「请求失败」
  assert.deepEqual(errorLines(http(400, null, null)), ["请求失败", "400"]);
  assert.deepEqual(errorLines(http(503, "other", "x")), ["请求失败", "503", "other", "x"]);
  assert.deepEqual(errorLines(http(429, "other", "x")), ["请求失败", "429", "other", "x"]);
  assert.deepEqual(errorLines({ kind: "connect" }), ["无法连接服务"]);
  assert.deepEqual(errorLines({ kind: "aborted" }), []);
});

test("429 的倒计时秒数", () => {
  assert.equal(isTooMany(http(429, "too_many_queries", "m")), true);
  assert.equal(retrySeconds(http(429, "too_many_queries", "m", "12")), 12);
  assert.equal(retrySeconds(http(429, "too_many_queries", "m", "0")), 0);
  for (const v of [null, "-1", "1.5", "Wed, 21 Oct 2026 07:28:00 GMT", ""]) {
    assert.equal(retrySeconds(http(429, "too_many_queries", "m", v)), 5, String(v));
  }
});
