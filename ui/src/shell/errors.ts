// 出错时各条的文字（REQ-AMB-UI-CONSOLE-ERR-*）。纯函数，结果请求由结果区显示，附属请求需要时（分布图区）也用它。

import type { Outcome } from "./api.ts";

export const TOO_MANY = "服务繁忙：同时执行的查询已满";
export const MEMORY = "这次查询需要的内存超过上限。请缩小时间范围，或去掉全文条件、改用字段条件";
export const INVALID = "查询写法有误";
export const UNAVAILABLE = "暂时无法读取数据（对象存储不可访问或服务正在停止）";
export const FAILED = "请求失败";
export const CONNECT = "无法连接服务";
export const CANCELLED = "已取消。服务端可能仍在执行这次查询，马上重查可能提示服务繁忙";
export const BAD_TIME = "时刻格式无效";
export const EMPTY = "无结果";
export const DEFAULT_RETRY_SECONDS = 5;

/** 429 且响应体为 `reason` 为 `too_many_queries` 的错误信封。 */
export function isTooMany(o: Outcome): boolean {
  return o.kind === "http" && o.status === 429 && o.reason === "too_many_queries";
}

/** ERR-429 的倒计时秒数：`Retry-After` 为非负十进制整数时取它，否则 5 秒。 */
export function retrySeconds(o: Outcome): number {
  if (o.kind === "http" && o.retryAfter !== null && /^[0-9]+$/.test(o.retryAfter)) return Number(o.retryAfter);
  return DEFAULT_RETRY_SECONDS;
}

/** 失败的数据请求在结果区（或分布图区）显示的各行文字；成功或被取消时为空。429 的倒计时另由调用方显示。 */
export function errorLines(o: Outcome): string[] {
  switch (o.kind) {
    case "ok":
    case "aborted":
      return [];
    case "connect":
      return [CONNECT];
    case "http": {
      const envelope = o.reason !== null && o.message !== null;
      if (isTooMany(o)) return [TOO_MANY];
      if (o.status === 400 && o.reason === "query_memory_exceeded") return [MEMORY];
      if (o.status === 400 && envelope) return [INVALID, o.reason as string, o.message as string];
      if (o.status === 503 && o.reason === "unavailable") return [UNAVAILABLE, o.message as string];
      return envelope ? [FAILED, String(o.status), o.reason as string, o.message as string] : [FAILED, String(o.status)];
    }
  }
}
