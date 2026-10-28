import { render } from "preact";
import { App, applyTheme } from "./app.tsx";
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
render(<App />, document.getElementById("app") as HTMLElement);
