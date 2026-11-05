import { defineConfig, type Plugin } from "vite";
import react from "@vitejs/plugin-react";

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

export default defineConfig({
  base: "/ui/",
  plugins: [react(), normalizeUiEntry()],
  build: {
    target: "es2022",
    manifest: true,
    sourcemap: false,
  },
  server: {
    host: process.env.AMBERLOG_UI_DEV_HOST ?? "127.0.0.1",
    port: Number(process.env.AMBERLOG_UI_DEV_PORT ?? "5173"),
    strictPort: true,
    proxy: {
      "/api": {
        target: process.env.AMBERLOG_API_ORIGIN ?? "http://127.0.0.1:8800",
        changeOrigin: true,
      },
    },
  },
});
