import { render } from "preact";
import { LocationProvider } from "preact-iso";
import { App, applyTheme } from "./app.tsx";
import { loadLoginConfig } from "./shell/login.ts";
import { settings } from "./shell/settings.ts";
import "./style.css";

// 外观随设置同步换上（不等组件重画）
let theme = settings.get().theme;
applyTheme(theme);
settings.subscribe(() => {
  if (settings.get().theme !== theme) {
    theme = settings.get().theme;
    applyTheme(theme);
  }
});
// 取一次登录配置（REQ-AMB-UI-CONSOLE-V3-LOGIN-CONFIG-REQUEST），先于各屏的数据请求发出
loadLoginConfig();
render(
  <LocationProvider scope="/ui">
    <App />
  </LocationProvider>,
  document.getElementById("app") as HTMLElement,
);
