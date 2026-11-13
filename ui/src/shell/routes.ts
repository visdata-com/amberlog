import type { Screen } from "./settings.ts";

export const SCREEN_PATHS: Record<Screen, string> = {
  search: "/search",
  ledger: "/ledger",
  recon: "/reconciliation",
};

function normalizedPath(path: string): string {
  const trimmed = path.replace(/\/+$/, "") || "/";
  return trimmed === "/ui" ? "/" : trimmed.startsWith("/ui/") ? trimmed.slice(3) : trimmed;
}

export function isKnownPath(path: string): boolean {
  const normalized = normalizedPath(path);
  return normalized === "/" || Object.values(SCREEN_PATHS).some((screen) => normalized === screen);
}

export function screenFromPath(path: string): Screen {
  const normalized = normalizedPath(path);
  if (normalized === SCREEN_PATHS.ledger) return "ledger";
  if (normalized === SCREEN_PATHS.recon) return "recon";
  return "search";
}
