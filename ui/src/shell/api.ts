// 取数入口：POST /api/default/_search 与 GET /api/default/<stream>/_fields
// （REQ-AMB-UI-CONSOLE-V2-TERM、REQUESTS、REQUEST-TIMEZONE、TOKEN-HEADER）。

import { parseJson, type Hit, type Json } from "../lib/json.ts";
import { settings } from "./settings.ts";

const ENDPOINT = "/api/default/_search";

export interface Query {
  sql: string;
  start_time: bigint;
  end_time: bigint;
  size: number;
  from?: number;
}

export type Outcome =
  | { kind: "ok"; hits: Hit[]; body: Map<string, Json> }
  | { kind: "http"; status: number; reason: string | null; message: string | null; retryAfter: string | null }
  | { kind: "connect" }
  | { kind: "aborted" };

export function requestBody(q: Query, timezone: string): string {
  return (
    '{"query":{"sql":' + JSON.stringify(q.sql) + ',"start_time":' + q.start_time + ',"end_time":' + q.end_time +
    ',"size":' + q.size + ',"from":' + (q.from ?? 0) + ',"timezone":' + JSON.stringify(timezone) + "}}"
  );
}

export interface Field {
  name: string;
  type: string;
}

type Failure = Exclude<Outcome, { kind: "ok" }>;

export type FieldsOutcome = { kind: "ok"; fields: Field[] } | Failure;

/** 发一个检索请求。不设超时（NO-TIMEOUT）；`signal` 中止时为 `aborted`。 */
export async function postSearch(q: Query, signal: AbortSignal): Promise<Outcome> {
  const { timezone } = settings.get();
  const r = await send("POST", ENDPOINT, requestBody(q, timezone), signal);
  if (r.kind !== "ok") {
    // 时区在请求发出后已改的，提示不针对所选时区，不提示
    if (r.kind === "http" && r.status === 400 && r.reason === "invalid_request" && r.message?.includes("query.timezone") === true) {
      if (settings.get().timezone === timezone) settings.set({ tzUnknown: true });
    }
    return r;
  }
  const hits = r.body?.get("hits");
  if (r.body !== null && Array.isArray(hits) && hits.every((h) => h instanceof Map)) {
    return { kind: "ok", hits: hits as Hit[], body: r.body };
  }
  return { kind: "http", status: r.status, reason: null, message: null, retryAfter: null };
}

/** 发一个字段列表请求（REQ-AMB-QRY-FIELDS）：不带 `query.timezone`，其余同检索请求。 */
export async function getFields(stream: string, signal: AbortSignal): Promise<FieldsOutcome> {
  const r = await send("GET", "/api/default/" + encodeURIComponent(stream) + "/_fields", null, signal);
  if (r.kind !== "ok") return r;
  const list = r.body?.get("fields");
  const fields: Field[] = [];
  if (Array.isArray(list)) {
    for (const f of list) {
      const name = f instanceof Map ? f.get("name") : undefined;
      const type = f instanceof Map ? f.get("type") : undefined;
      if (typeof name !== "string" || typeof type !== "string") break;
      fields.push({ name, type });
    }
    if (fields.length === list.length) return { kind: "ok", fields };
  }
  return { kind: "http", status: r.status, reason: null, message: null, retryAfter: null };
}

// 2xx 时给出解析后的响应体（不是 JSON 对象时为 null），其余按 Outcome 的失败几种
async function send(
  method: string,
  url: string,
  body: string | null,
  signal: AbortSignal,
): Promise<{ kind: "ok"; status: number; body: Map<string, Json> | null } | Failure> {
  const { token } = settings.get();
  const headers: Record<string, string> = {};
  if (body !== null) headers["Content-Type"] = "application/json";
  if (token !== "") headers["Authorization"] = "Bearer " + token;
  let res: Response;
  let raw: string;
  try {
    res = await fetch(url, { method, headers, body, signal });
    raw = await res.text();
  } catch {
    return signal.aborted ? { kind: "aborted" } : { kind: "connect" };
  }
  let parsed: Json | undefined;
  try {
    parsed = parseJson(raw);
  } catch {
    parsed = undefined;
  }
  const obj = parsed instanceof Map ? parsed : null;
  if (res.status >= 200 && res.status <= 299) return { kind: "ok", status: res.status, body: obj };
  const reason = obj?.get("reason");
  const message = obj?.get("message");
  const envelope = typeof reason === "string" && typeof message === "string";
  // 对全部数据请求（结果请求与附属请求）生效；口令在请求发出后已改的，提示不针对当前的口令，不提示
  if (res.status === 401 && settings.get().token === token) settings.set({ tokenInvalid: true });
  return {
    kind: "http",
    status: res.status,
    reason: envelope ? reason : null,
    message: envelope ? message : null,
    retryAfter: res.headers.get("Retry-After"),
  };
}
