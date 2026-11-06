import type { Screen } from "./settings.ts";

export const SCREEN_PATHS: Record<Screen, string> = {
  search: "/search",
  ledger: "/ledger",
  recon: "/reconciliation",
};

/** BrowserRouter 使用 /ui 基路径；这里接收的是去掉基路径后的路由路径。 */
export function screenFromPath(path: string): Screen {
  if (path === SCREEN_PATHS.ledger) return "ledger";
  if (path === SCREEN_PATHS.recon) return "recon";
  return "search";
}
