import { defineConfig } from "@playwright/test";

// The R1 browser journey against a running Compose stack with the fake provider.
// See docs/development.md, "Browser journey check". Not part of `pnpm test`.
export default defineConfig({
  testDir: "./e2e",
  testMatch: "**/*.e2e.ts",
  // Ingesting a real-time fault and waiting for its incident takes minutes.
  timeout: 12 * 60_000,
  expect: { timeout: 20_000 },
  retries: 0,
  workers: 1,
  reporter: [["list"], ["html", { open: "never", outputFolder: "e2e-report" }]],
  outputDir: "e2e-results",
  use: {
    baseURL: process.env.E2E_BASE_URL ?? "http://127.0.0.1:8090",
    // `chrome` uses an installed Google Chrome instead of a downloaded Chromium.
    channel: process.env.E2E_BROWSER_CHANNEL ?? undefined,
    trace: "retain-on-failure",
  },
});
