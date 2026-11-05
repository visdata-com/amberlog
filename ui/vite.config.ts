import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  base: "/ui/",
  plugins: [react()],
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
