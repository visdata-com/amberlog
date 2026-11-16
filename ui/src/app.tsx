// 界面外壳（REQ-AMB-UI-CONSOLE-V3）：顶栏（三屏切换、时区、外观、口令）与三屏。三屏始终挂载、只显示当前屏，
// 所以切换不发请求、各屏已填的内容与结果都保留（SWITCH）。

import { useLayoutEffect, useState } from "preact/hooks";
import { useLocation } from "preact-iso";
import { Input } from "./components/ui/Input.tsx";
import { useStore } from "./lib/store.ts";
import { SCREEN_PATHS, screenFromPath } from "./shell/routes.ts";
import { LedgerScreen } from "./screens/ledger/LedgerScreen.tsx";
import { ReconScreen } from "./screens/recon/ReconScreen.tsx";
import { SearchScreen } from "./screens/search/SearchScreen.tsx";
import { SearchGuide } from "./screens/search/SearchGuide.tsx";
import { TimeZonePicker } from "./shell/TimeZonePicker.tsx";
import { Select } from "./shell/Select.tsx";
import { clickLogin, login } from "./shell/login.ts";
import {
  confirmToken,
  setScreen,
  setTheme,
  setToken,
  settings,
  type Screen,
  type Theme,
  type TokenHint,
} from "./shell/settings.ts";

const SCREENS: { id: Screen; label: string }[] = [
  { id: "search", label: "检索" },
  { id: "ledger", label: "台账" },
  { id: "recon", label: "对账" },
];

// 口令框旁的提示（TOKEN-401 标出口令框，TOKEN-403 不标出）
const TOKEN_HINTS: Record<TokenHint, string> = {
  invalid: "口令无效或未填写",
  forbidden: "当前身份没有这项操作的权限",
  loginStopped: "登录得到的身份未通过服务端认证，已停止自动登录",
};

const THEMES: { id: Theme; label: string }[] = [
  { id: "system", label: "跟随系统" },
  { id: "light", label: "浅色" },
  { id: "dark", label: "深色" },
];

/** 外观：`<html data-theme>` 为 light 或 dark 时照它，没有时按 prefers-color-scheme（style.css）。 */
export function applyTheme(theme: Theme): void {
  if (theme === "system") delete document.documentElement.dataset["theme"];
  else document.documentElement.dataset["theme"] = theme;
}

function TopBar({ screen, onGuide, guideActive }: { screen: Screen; onGuide: () => void; guideActive: boolean }) {
  const s = useStore(settings);
  const l = useStore(login);
  return (
    <header className="topbar">
      <span className="brand">Amberlog</span>
      <div className="topbar__primary">
        <nav>
          {SCREENS.map((x) => (
            <button key={x.id} type="button" data-screen={x.id} aria-selected={screen === x.id} onClick={() => setScreen(x.id)}>
              {x.label}
            </button>
          ))}
        </nav>
        {screen === "search" && (
          <button type="button" className="guide-entry" aria-label="打开检索指引" aria-expanded={guideActive} onClick={onGuide}>
            使用指引
          </button>
        )}
      </div>
      <span className="spacer" />
      <label className="field">
        <span>时区</span>
        <TimeZonePicker timezone={s.timezone} />
      </label>
      {s.tzUnknown && (
        <span className="hint" id="tz-hint">
          服务端不认识所选时区，请改选其他时区
        </span>
      )}
      <label className="field">
        <span>外观</span>
        <Select
          id="theme"
          aria-label="外观"
          placeholder="选择外观"
          value={s.theme}
          options={THEMES.map((theme) => ({ value: theme.id, label: theme.label }))}
          onChange={(value) => setTheme(value as Theme)}
        />
      </label>
      <label className="field">
        <span>口令</span>
        <Input
          id="token"
          type="password"
          autoComplete="off"
          placeholder="输入访问口令"
          className={s.tokenHint === "invalid" ? "invalid" : undefined}
          aria-invalid={s.tokenHint === "invalid"}
          value={s.token}
          onInput={(e) => setToken(e.currentTarget.value)}
          onBlur={confirmToken}
          onKeyDown={(e) => {
            if (e.key === "Enter") confirmToken();
          }}
        />
      </label>
      {l.config !== null && (
        <button type="button" id="login" onClick={clickLogin}>
          登录
        </button>
      )}
      <span className="note" id="token-note">
        只保存在本标签页，关闭即清除
      </span>
      {s.tokenHint !== null && (
        <span className="hint" id="token-hint">
          {TOKEN_HINTS[s.tokenHint]}
        </span>
      )}
    </header>
  );
}

export function App() {
  const { path, route } = useLocation();
  const screen = screenFromPath(path);
  const [guideActive, setGuideActive] = useState(false);
  const closeGuide = () => {
    setGuideActive(false);
  };
  // URL 驱动可见屏幕；现有跨屏入口仍通过 setScreen 触发导航。
  // 三屏实例始终保留，路由切换只改变 hidden，不触发查询或重置草稿与结果。
  useLayoutEffect(() => {
    if (settings.get().screen !== screen) settings.set({ screen });
    return settings.subscribe(() => {
      const next = settings.get().screen;
      if (next !== screen) route(`/ui${SCREEN_PATHS[next]}`);
    });
  }, [screen, route]);
  return (
    <div className={screen === "search" ? "app-shell" : "app-shell app-shell--fill"}>
      <TopBar screen={screen} onGuide={() => setGuideActive(true)} guideActive={guideActive} />
      <main>
        <SearchScreen hidden={screen !== "search"} />
        <LedgerScreen hidden={screen !== "ledger"} />
        <ReconScreen hidden={screen !== "recon"} />
      </main>
      {screen === "search" && guideActive && <SearchGuide onClose={closeGuide} />}
    </div>
  );
}
