// 界面外壳（REQ-AMB-UI-CONSOLE）：顶栏（三屏切换、时区、外观、口令）与三屏。三屏始终挂载、只显示当前屏，
// 所以切换不发请求、各屏已填的内容与结果都保留（SWITCH）。

import { useStore } from "./lib/store.ts";
import { LedgerScreen } from "./screens/ledger/LedgerScreen.tsx";
import { ReconScreen } from "./screens/recon/ReconScreen.tsx";
import { SearchScreen } from "./screens/search/SearchScreen.tsx";
import { TimeZonePicker } from "./shell/TimeZonePicker.tsx";
import {
  confirmToken,
  setScreen,
  setTheme,
  setToken,
  settings,
  type Screen,
  type Theme,
} from "./shell/settings.ts";

const SCREENS: { id: Screen; label: string }[] = [
  { id: "search", label: "检索" },
  { id: "ledger", label: "台账" },
  { id: "recon", label: "对账" },
];

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

function TopBar() {
  const s = useStore(settings);
  return (
    <header class="topbar">
      <span class="brand">Amberlog</span>
      <nav>
        {SCREENS.map((x) => (
          <button type="button" data-screen={x.id} aria-selected={s.screen === x.id} onClick={() => setScreen(x.id)}>
            {x.label}
          </button>
        ))}
      </nav>
      <span class="spacer" />
      <label class="field">
        <span>时区</span>
        <TimeZonePicker timezone={s.timezone} />
      </label>
      {s.tzUnknown && (
        <span class="hint" id="tz-hint">
          服务端不认识所选时区，请改选其他时区
        </span>
      )}
      <label class="field">
        <span>外观</span>
        <select id="theme" value={s.theme} onChange={(e) => setTheme(e.currentTarget.value as Theme)}>
          {THEMES.map((t) => (
            <option value={t.id}>{t.label}</option>
          ))}
        </select>
      </label>
      <label class="field">
        <span>口令</span>
        <input
          id="token"
          type="password"
          autocomplete="off"
          class={s.tokenInvalid ? "invalid" : undefined}
          aria-invalid={s.tokenInvalid}
          value={s.token}
          onInput={(e) => setToken(e.currentTarget.value)}
          onBlur={confirmToken}
          onKeyDown={(e) => {
            if (e.key === "Enter") confirmToken();
          }}
        />
      </label>
      <span class="note" id="token-note">
        只保存在本标签页，关闭即清除
      </span>
      {s.tokenInvalid && (
        <span class="hint" id="token-hint">
          口令无效或未填写
        </span>
      )}
    </header>
  );
}

export function App() {
  const { screen } = useStore(settings);
  return (
    <>
      <TopBar />
      <main>
        <SearchScreen hidden={screen !== "search"} />
        <LedgerScreen hidden={screen !== "ledger"} />
        <ReconScreen hidden={screen !== "recon"} />
      </main>
    </>
  );
}
