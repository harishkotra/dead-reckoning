// Vite configuration for the Dead Reckoning web UI. The dev server proxies
// WebSocket connections to the Express bridge so the browser can open a plain
// ws:// connection to the same origin (no CORS/port mismatch headaches).
//
// Ports come from the repo-root .env (BRIDGE_PORT, VITE_PORT) so a single
// config file drives the gRPC server, the bridge, and this proxy together.

import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig(({ mode }) => {
  // Load the repo-root .env (Vite normally only auto-loads ./web/.env).
  const env = loadEnv(mode, new URL("..", import.meta.url).pathname, "");
  const bridgePort = Number.parseInt(env.BRIDGE_PORT || "3001", 10);
  const vitePort = Number.parseInt(env.VITE_PORT || "5173", 10);

  return {
    plugins: [react()],
    server: {
      port: vitePort,
      strictPort: true,
      // Forward /ws to the bridge (Express + WebSocket) so the UI uses one origin.
      proxy: {
        "/ws": {
          target: `ws://localhost:${bridgePort}`,
          ws: true,
        },
      },
    },
  };
});
