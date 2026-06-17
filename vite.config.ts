import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { resolve } from "node:path";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/assets/",
  plugins: [react(), tailwindcss()],
  resolve: {
    alias: {
      "@": resolve(__dirname, "frontend/src"),
    },
  },
  root: "frontend",
  server: {
    proxy: {
      "/admin": "https://oaix.fugue.pro",
      "/healthz": "https://oaix.fugue.pro",
    },
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
});
