import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { Agent } from "node:https";
import { resolve } from "node:path";
import { defineConfig } from "vite";

const UPSTREAM = "https://oaix.fugue.pro";

/**
 * 复用 TLS 连接。默认每个代理请求都要重新握手，实测握手本身就要 ~1.3s，
 * 而且请求会被串行化：8 个并发 /healthz 要 14.4s 才跑完（直连生产只要 3.1s）。
 * 这只影响本地开发体验，生产的 Go 网关本身是并发的。
 */
const upstreamAgent = new Agent({ keepAlive: true, maxSockets: 24 });

export default defineConfig(({ command }) => ({
  base: command === "serve" ? "/" : "/assets/",
  plugins: [react(), tailwindcss()],
  resolve: {
    dedupe: ["react", "react-dom"],
    alias: {
      "@": resolve(__dirname, "frontend/src"),
    },
  },
  root: "frontend",
  server: {
    proxy: Object.fromEntries(
      [
        "/admin",
        // Normal-user routes (/api/me, /api/tokens, /api/auth/login, ...) live
        // under /api; without this the dev server answers them with index.html.
        "/api",
        "/healthz",
      ].map((prefix) => [prefix, { target: UPSTREAM, changeOrigin: true, agent: upstreamAgent }]),
    ),
  },
  build: {
    outDir: "../oaix_gateway/web",
    emptyOutDir: true,
    rollupOptions: {
      output: {
        assetFileNames: (assetInfo) => {
          const name = assetInfo.names?.[0] ?? assetInfo.name ?? "";
          if (name.endsWith(".css")) {
            return "styles.css";
          }
          return "[name][extname]";
        },
        chunkFileNames: "src/[name]-[hash].js",
        entryFileNames: "src/main.js",
      },
    },
  },
}));
