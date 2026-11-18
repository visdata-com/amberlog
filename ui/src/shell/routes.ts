import type { Screen } from "./settings.ts";

// 三个屏地址（REQ-AMB-UI-CONSOLE-V4-SCREENS）。服务端只注册 /ui 与这三个地址（ADR-0060 第 5 条），
// 别的地址到不了前端，所以前端不设「页面不存在」页。

export const SCREEN_PATHS: Record<Screen, string> = {
  search: "/search",
  ledger: "/ledger",
  recon: "/reconciliation",
};

export function screenFromPath(path: string): Screen {
  if (path === `/ui${SCREEN_PATHS.ledger}`) return "ledger";
  if (path === `/ui${SCREEN_PATHS.recon}`) return "recon";
  return "search";
}
