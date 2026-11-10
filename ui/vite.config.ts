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
  const target = backendTarget(process.env.UI_BACKEND_URL ?? env.UI_BACKEND_URL ?? process.env.AMBERLOG_API_ORIGIN);
  return {
    root,
    base: "/ui/",
    envDir,
    // The proxy target is server configuration, not a browser environment variable.
    envPrefix: "AMBERLOG_UI_PUBLIC_UNUSED_",
    cacheDir: fileURLToPath(new URL("./node_modules/.vite", import.meta.url)),
    plugins: [preact({ reactAliasesEnabled: false }), normalizeUiEntry()],
    build: {
      target: "es2022",
      manifest: true,
      sourcemap: false,
    },
    server: {
      host: process.env.AMBERLOG_UI_DEV_HOST ?? "127.0.0.1",
      port: Number(process.env.AMBERLOG_UI_DEV_PORT ?? "5173"),
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
