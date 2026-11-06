import { createRoot } from "react-dom/client";
import { BrowserRouter, Route, Routes } from "react-router";
import { App, applyTheme } from "./app.tsx";
import { settings } from "./shell/settings.ts";
import { NotFoundPage } from "./shell/NotFoundPage.tsx";
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
createRoot(document.getElementById("app") as HTMLElement).render(
  <BrowserRouter basename="/ui">
    <Routes>
      <Route path="/" element={<App />}>
        <Route index element={null} />
        <Route path="search" element={null} />
        <Route path="ledger" element={null} />
        <Route path="reconciliation" element={null} />
      </Route>
      <Route path="*" element={<NotFoundPage />} />
    </Routes>
  </BrowserRouter>,
);
