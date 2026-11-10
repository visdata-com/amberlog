import { render } from "preact";
import { LocationProvider, useLocation } from "preact-iso";
import { App, applyTheme } from "./app.tsx";
import { settings } from "./shell/settings.ts";
import { NotFoundPage } from "./shell/NotFoundPage.tsx";
import { isKnownPath } from "./shell/routes.ts";
import "./style.css";

function RoutedApp() {
  const { path } = useLocation();
  return isKnownPath(path) ? <App /> : <NotFoundPage />;
}

// 外观随设置同步换上（不等组件重画）
let theme = settings.get().theme;
applyTheme(theme);
settings.subscribe(() => {
  if (settings.get().theme !== theme) {
    theme = settings.get().theme;
    applyTheme(theme);
  }
});
render(
  <LocationProvider scope="/ui">
    <RoutedApp />
  </LocationProvider>,
  document.getElementById("app") as HTMLElement,
);
