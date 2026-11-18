// 外壳一侧的界面登录（REQ-AMB-UI-CONSOLE-V3 J 组）：页面加载后取一次登录配置、「登录」入口、
// 口令由登录换得时数据请求得 401 整页跳转重新登录，登录回来后的第一个数据请求仍得 401 就停止（LOGIN-REAUTH）。

import { createStore } from "../lib/store.ts";
import {
  FROM_LOGIN_KEY,
  RETURNED_KEY,
  STOPPED_KEY,
  parseLoginConfig,
  sessionGet,
  sessionRemove,
  sessionSet,
  startLogin,
  type LoginConfig,
} from "../lib/login.ts";
import { onTokenConfirmed, settings } from "./settings.ts";

export const login = createStore({
  /** `GET /ui/oidc/config` 合形状的应答；为 null 时不显示「登录」入口（LOGIN-ENTRY）。 */
  config: null as LoginConfig | null,
  /**
   * 已按 LOGIN-REAUTH 停止自动登录：直到口令被修改或用户再点「登录」，得 401 不再跳转。记在会话存储里，页面重新载入后
   * 照旧停止（重新载入不在「直到」所列的情形里）。
   */
  stopped: sessionGet(STOPPED_KEY) === "1",
});
// 重新载入时停止的状态还在，口令框旁的提示也照旧
if (login.get().stopped) settings.set({ tokenHint: "loginStopped" });

let redirecting = false;
// 登录配置的请求有了结果（成功与否）；数据请求得 401 时先等它，免得配置还没回来就按 TOKEN-401 处置
let configSettled: Promise<void> = Promise.resolve();

/** 页面加载后发恰一次 `GET /ui/oidc/config`（LOGIN-CONFIG-REQUEST）：不带 `Authorization`；失败不重发、不提示。 */
export function loadLoginConfig(): void {
  configSettled = fetchConfig();
}

async function fetchConfig(): Promise<void> {
  try {
    const res = await fetch("/ui/oidc/config");
    const raw = await res.text();
    if (res.status !== 200) return;
    const config = parseLoginConfig(JSON.parse(raw));
    if (config !== null) login.set({ config });
  } catch {
    // 连接失败或不是 JSON：不显示「登录」入口
  }
}

/** 用户点「登录」（LOGIN-START）。 */
export function clickLogin(): void {
  const { config } = login.get();
  if (config === null) return;
  resume();
  if (settings.get().tokenHint === "loginStopped") settings.set({ tokenHint: null });
  redirect(config);
}

function redirect(config: LoginConfig): void {
  if (redirecting) return;
  redirecting = true;
  void startLogin(config);
}

// 口令被修改（TOKEN-401 所称）：口令不再算由登录换得，停止的状态随之解除
onTokenConfirmed(() => {
  sessionRemove(FROM_LOGIN_KEY);
  resume();
});

function resume(): void {
  sessionRemove(STOPPED_KEY);
  login.set({ stopped: false });
}

// 登录回到 `/ui` 之后的第一个数据请求：页面加载时会话存储里有登录刚回来的标记，第一个数据请求发出时取走它
let returned = sessionGet(RETURNED_KEY) === "1";
// 第一个数据请求的结果（HTTP 状态码，连接失败为 0）；与它同时在途的请求得 401 时等它的结果再定
let firstDone: Promise<number> | null = null;

/** 一个数据请求发出前调用：它是登录回来后的第一个数据请求时，返回发出后要调的回调（参数为状态码，连接失败为 0）。 */
export function beginDataRequest(): ((status: number) => void) | null {
  if (!returned) return null;
  returned = false;
  sessionRemove(RETURNED_KEY);
  let resolve: (status: number) => void = () => {};
  firstDone = new Promise((r) => {
    resolve = r;
  });
  return resolve;
}

/**
 * 数据请求得到 401、且口令在此期间未改时调用。口令由登录换得、此后未被修改、且「登录」入口正在显示时按 LOGIN-REAUTH：
 * 返回 `redirect`（已整页跳转重新登录，界面不按 TOKEN-401 标出口令框，也不在结果区显示这次失败）或 `stopped`（这是登录
 * 回来后的第一个数据请求，口令框旁显示已停止自动登录）；其余情形返回 `none`，照 TOKEN-401。
 */
export async function onUnauthorized(first: boolean): Promise<"redirect" | "stopped" | "none"> {
  await configSettled;
  const { config } = login.get();
  if (config === null || sessionGet(FROM_LOGIN_KEY) !== "1") return "none";
  if (first) {
    stop();
    return "stopped";
  }
  if (!login.get().stopped && firstDone !== null && (await firstDone) === 401) stop();
  if (login.get().stopped) return "stopped";
  redirect(config);
  return "redirect";
}

function stop(): void {
  if (!login.get().stopped) {
    sessionSet(STOPPED_KEY, "1");
    login.set({ stopped: true });
    settings.set({ tokenHint: "loginStopped" });
  }
}
