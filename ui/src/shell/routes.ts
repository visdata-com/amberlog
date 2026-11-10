import type { Screen } from "./settings.ts";

export const SCREEN_PATHS: Record<Screen, string> = {
  search: "/search",
  ledger: "/ledger",
  recon: "/reconciliation",
};

export function isKnownPath(path: string): boolean {
  return path === "/ui" || path === "/ui/" || Object.values(SCREEN_PATHS).some((screen) => path === `/ui${screen}`);
}

export function screenFromPath(path: string): Screen {
  if (path === `/ui${SCREEN_PATHS.ledger}`) return "ledger";
  if (path === `/ui${SCREEN_PATHS.recon}`) return "recon";
  return "search";
}
