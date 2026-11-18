// 回调页（REQ-AMB-UI-CONSOLE-V3-CALLBACK-STATE、CALLBACK-EXCHANGE、LOGIN-TOKEN）：核对地址栏的 `state`，
// 通过后向 `POST /ui/oidc/token` 换令牌，存为访问口令后回到 `/ui`。除这一个登录请求与回到 `/ui` 的整页跳转外不发请求
// （LOGIN-SELF-CONTAINED），不引入外壳的界面代码。

import {
  FROM_LOGIN_KEY,
  RETURNED_KEY,
  STATE_KEY,
  TOKEN_KEY,
  VERIFIER_KEY,
  redirectUri,
  sessionGet,
  sessionRemove,
  sessionSet,
} from "../lib/login.ts";
import "./style.css";

const FAILED = "登录失败";

function show(lines: string[]): void {
  const main = document.getElementById("callback") as HTMLElement;
  main.replaceChildren(
    ...lines.map((l) => {
      const p = document.createElement("p");
      p.textContent = l;
      return p;
    }),
  );
}

/** 抹掉地址栏中的查询串（授权码、`state` 与 `error`），不新增历史条目。 */
function clearQuery(): void {
  history.replaceState(null, "", location.pathname);
}

async function run(): Promise<void> {
  const q = new URLSearchParams(location.search);
  const code = q.get("code");
  const state = q.get("state");
  const saved = sessionGet(STATE_KEY);
  clearQuery();
  if (q.has("error") || code === null || state === null || saved === null || state !== saved) {
    show([FAILED]);
    return;
  }
  const verifier = sessionGet(VERIFIER_KEY) ?? "";
  const body = JSON.stringify({ code, code_verifier: verifier, redirect_uri: redirectUri(location.origin) });
  let res: Response;
  let raw: string;
  try {
    const p = fetch("/ui/oidc/token", { method: "POST", headers: { "Content-Type": "application/json" }, body });
    sessionRemove(STATE_KEY);
    sessionRemove(VERIFIER_KEY);
    res = await p;
    raw = await res.text();
  } catch {
    show([FAILED]);
    return;
  }
  if (res.status !== 200) {
    show([FAILED, String(res.status)]);
    return;
  }
  let parsed: unknown;
  try {
    parsed = JSON.parse(raw);
  } catch {
    parsed = null;
  }
  const token =
    typeof parsed === "object" && parsed !== null ? (parsed as Record<string, unknown>)["access_token"] : undefined;
  if (typeof token !== "string") {
    show([FAILED, String(res.status)]);
    return;
  }
  // 只存访问令牌（不存刷新令牌、ID 令牌），并记下它由登录换得、登录刚回来（LOGIN-REAUTH）
  sessionSet(TOKEN_KEY, token);
  sessionSet(FROM_LOGIN_KEY, "1");
  sessionSet(RETURNED_KEY, "1");
  location.replace("/ui");
}

void run();
