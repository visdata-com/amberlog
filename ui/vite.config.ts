import { fileURLToPath } from "node:url";
import { defineConfig, loadEnv, type ConfigEnv, type Plugin } from "vite";
import preact from "@preact/preset-vite";

const root = fileURLToPath(new URL("./", import.meta.url));

export function backendTarget(value: string | undefined): string {
  if (value === undefined) return "http://127.0.0.1:8800";
  try {
    const url = new URL(value);
    if (!["http:", "https:"].includes(url.protocol) || url.username || url.password ||
      url.pathname !== "/" || value.includes("?") || value.includes("#") || value.trim() !== value) {
      throw new Error("not an origin");
    }
    return url.origin;
  } catch {
    throw new Error("UI_BACKEND_URL must be an HTTP(S) origin without credentials, path, query or fragment");
  }
}

function normalizeUiEntry(): Plugin {
  return {
    name: "amberlog-ui-entry-slash",
    apply: "serve" as const,
    configureServer(server) {
      server.middlewares.use((request, response, next) => {
        const url = request.url ?? "";
        if (url === "/ui" || url.startsWith("/ui?")) {
          response.statusCode = 302;
          response.setHeader("Location", `/ui/${url.slice("/ui".length)}`);
          response.end();
          return;
        }
        next();
      });
    },
  };
}

export default defineConfig(({ mode, envDir = root }: ConfigEnv & { envDir?: string }) => {
  const env = loadEnv(mode, envDir, "UI_BACKEND_URL");
  const target = backendTarget(process.env.UI_BACKEND_URL ?? env.UI_BACKEND_URL);
  return {
    root,
    base: "/ui/",
    envDir,
    // The proxy target is server configuration, not a browser environment variable.
    // No variable read by the UI tooling starts with AMBERLOG_: `amberlog serve` exits with code 2 on
    // unknown AMBERLOG_* variables, and developers often run both from the same shell (ADR-0060 item 9).
    envPrefix: "UI_PUBLIC_UNUSED_",
    publicDir: false,
    cacheDir: fileURLToPath(new URL("./node_modules/.vite", import.meta.url)),
    plugins: [preact({ reactAliasesEnabled: false }), normalizeUiEntry()],
    build: {
      // 产物由 build.mjs 按入口逐个构建、内联成单个自包含 HTML（ADR-0060 第 2 条）：不分块、不出资源文件。
      target: "es2022",
      sourcemap: false,
      cssCodeSplit: false,
      modulePreload: false,
      assetsInlineLimit: Number.MAX_SAFE_INTEGER,
    },
    server: {
      host: process.env.UI_DEV_HOST ?? "127.0.0.1",
      port: Number(process.env.UI_DEV_PORT ?? "5173"),
      strictPort: true,
      fs: { strict: true, allow: [root] },
      proxy: {
        "/api/": { target, changeOrigin: true },
        "^/healthz(?:\\?.*)?$": { target, changeOrigin: true },
        "^/readyz(?:\\?.*)?$": { target, changeOrigin: true },
      },
    },
  };
});
