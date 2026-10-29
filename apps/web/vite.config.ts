import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

const apiTarget = process.env.SIGNALOPS_API_URL ?? "http://127.0.0.1:8010";

export default defineConfig({
  plugins: [react()],
  server: {
    proxy: {
      "/health/": apiTarget,
      "/v1": apiTarget,
    },
  },
});
