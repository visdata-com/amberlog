// 三屏共用的设置：所选时区、外观、访问口令与它们的提示（REQ-AMB-UI-CONSOLE 口令、时区、外观三组）。

import { createStore } from "../lib/store.ts";
import { browserTimeZone, timeZoneChoices } from "../lib/time.ts";

export type Screen = "search" | "ledger" | "recon";
export type Theme = "system" | "light" | "dark";

const TZ_KEY = "amberlog.timezone";
const THEME_KEY = "amberlog.theme";
const TOKEN_KEY = "amberlog.token";

// 存储不可用（隐私模式、被禁用）时照常工作，只是不记
function load(storage: () => Storage, key: string): string | null {
  try {
    return storage().getItem(key);
  } catch {
    return null;
  }
}
function save(storage: () => Storage, key: string, value: string): void {
  try {
    storage().setItem(key, value);
  } catch {
    // 同上
  }
}
const local = (): Storage => localStorage;
const session = (): Storage => sessionStorage;

export const timeZones: readonly string[] = timeZoneChoices();

function initialTimeZone(): string {
  const saved = load(local, TZ_KEY);
  return saved !== null && timeZones.includes(saved) ? saved : browserTimeZone(timeZones);
}

function initialTheme(): Theme {
  const saved = load(local, THEME_KEY);
  return saved === "light" || saved === "dark" || saved === "system" ? saved : "system";
}

export const settings = createStore({
  screen: "search" as Screen,
  timezone: initialTimeZone(),
  theme: initialTheme(),
  /** 访问口令（口令框的当前内容）；只存当前标签页的会话存储（TOKEN-STORAGE）。 */
  token: load(session, TOKEN_KEY) ?? "",
  /** 数据请求得到 401 后为真，直到口令被修改（TOKEN-401）。 */
  tokenInvalid: false,
  /** 数据请求的 400 指明 `query.timezone` 后为真，直到所选时区改变（TZ-UNKNOWN）。 */
  tzUnknown: false,
});

export function setScreen(screen: Screen): void {
  settings.set({ screen });
}

export function setTimeZone(timezone: string): void {
  if (timezone === settings.get().timezone) return;
  save(local, TZ_KEY, timezone);
  settings.set({ timezone, tzUnknown: false });
}

export function setTheme(theme: Theme): void {
  save(local, THEME_KEY, theme);
  settings.set({ theme });
}

export function setToken(token: string): void {
  save(session, TOKEN_KEY, token);
  settings.set({ token });
}

// 「口令被修改」：口令框失去焦点或在其中回车时，内容与上一次如此确认时不同
let confirmedToken = settings.get().token;
const confirmed = new Set<() => void>();

export function confirmToken(): void {
  const { token } = settings.get();
  if (token === confirmedToken) return;
  confirmedToken = token;
  settings.set({ tokenInvalid: false });
  for (const f of [...confirmed]) f();
}

/** 口令被修改时调 `f`（检索屏的数据集候选查询在 401 后据此重发）。 */
export function onTokenConfirmed(f: () => void): () => void {
  confirmed.add(f);
  return () => confirmed.delete(f);
}
