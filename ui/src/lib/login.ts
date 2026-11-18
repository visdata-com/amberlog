// 界面登录的共用部分（REQ-AMB-UI-CONSOLE-V3 J 组）：外壳与回调页都用。会话存储里的键、登录配置的形状、
// `state` 与 PKCE 校验值的生成、授权地址的拼法。只用会话存储（LOGIN-START、TOKEN-STORAGE），不碰 Cookie 与本地存储。

import { sha256 } from "./sha256.ts";

/** 访问口令（口令框的内容）。 */
export const TOKEN_KEY = "amberlog.token";
export const STATE_KEY = "amberlog.login.state";
export const VERIFIER_KEY = "amberlog.login.verifier";
/** 口令由登录换得、此后未被修改（LOGIN-REAUTH）。 */
export const FROM_LOGIN_KEY = "amberlog.login.from";
/** 登录刚回到 `/ui`，还没有发出数据请求（LOGIN-REAUTH）。 */
export const RETURNED_KEY = "amberlog.login.returned";
/** 已停止自动登录（LOGIN-REAUTH）：页面重新载入后也保留，直到口令被修改或用户再点「登录」。 */
export const STOPPED_KEY = "amberlog.login.stopped";

/** `GET /ui/oidc/config` 的应答（LOGIN-ENTRY 所定的形状）。 */
export interface LoginConfig {
  authorization_endpoint: string;
  client_id: string;
  scope: string;
}

/** 应答体合 LOGIN-ENTRY 的形状时给出登录配置，否则为 null。 */
export function parseLoginConfig(v: unknown): LoginConfig | null {
  if (typeof v !== "object" || v === null || Array.isArray(v)) return null;
  const o = v as Record<string, unknown>;
  const { authorization_endpoint, client_id, scope } = o;
  if (typeof authorization_endpoint !== "string" || typeof client_id !== "string" || typeof scope !== "string") return null;
  return { authorization_endpoint, client_id, scope };
}

/** 回调页的地址：页面的源后接 `/ui/callback`。 */
export function redirectUri(origin: string): string {
  return origin + "/ui/callback";
}

/** base64url，无填充（RFC 4648 §5）。 */
export function base64url(bytes: Uint8Array): string {
  let s = "";
  for (const b of bytes) s += String.fromCharCode(b);
  return btoa(s).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

/** `n` 字节的随机量，按 base64url 写出（字符都在 RFC 7636 所列的非保留字符内）。 */
export function randomString(n: number): string {
  return base64url(crypto.getRandomValues(new Uint8Array(n)));
}

/** `code_verifier` 的 SHA-256 摘要按 base64url 无填充编码（RFC 7636 §4.2 的 S256）。 */
export async function codeChallenge(verifier: string): Promise<string> {
  const data = new TextEncoder().encode(verifier);
  const digest =
    typeof crypto.subtle?.digest === "function"
      ? new Uint8Array(await crypto.subtle.digest("SHA-256", data))
      : sha256(data);
  return base64url(digest);
}

/** 授权地址：`authorization_endpoint` 原有的查询参数保留，另加 LOGIN-START 所列各参数。 */
export function authorizeUrl(c: LoginConfig, origin: string, state: string, challenge: string): string {
  const u = new URL(c.authorization_endpoint);
  u.searchParams.set("response_type", "code");
  u.searchParams.set("client_id", c.client_id);
  u.searchParams.set("redirect_uri", redirectUri(origin));
  u.searchParams.set("scope", c.scope);
  u.searchParams.set("state", state);
  u.searchParams.set("code_challenge", challenge);
  u.searchParams.set("code_challenge_method", "S256");
  return u.toString();
}

// 会话存储不可用（被禁用）时登录做不成，但界面其余部分照常
export function sessionGet(key: string): string | null {
  try {
    return sessionStorage.getItem(key);
  } catch {
    return null;
  }
}
export function sessionSet(key: string, value: string): void {
  try {
    sessionStorage.setItem(key, value);
  } catch {
    // 同上
  }
}
export function sessionRemove(key: string): void {
  try {
    sessionStorage.removeItem(key);
  } catch {
    // 同上
  }
}

/**
 * 点「登录」或重新登录（LOGIN-START）：新生成 `state`（128 位）与 `code_verifier`（256 位，43 个字符），存会话存储，
 * 再整页跳转到授权地址。
 */
export async function startLogin(c: LoginConfig): Promise<void> {
  const state = randomString(16);
  const verifier = randomString(32);
  sessionSet(STATE_KEY, state);
  sessionSet(VERIFIER_KEY, verifier);
  location.assign(authorizeUrl(c, location.origin, state, await codeChallenge(verifier)));
}
