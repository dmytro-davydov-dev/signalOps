// Captures the demonstration narrative (docs/demo.md) in a real browser against the
// Compose stack and the fake provider: a fault, its incident and evidence, the
// automatic diagnosis, a worker killed mid-investigation and the resumed run, and
// the baseline comparison of a completed evaluation run. Screenshots go to
// docs/demo/; the video goes to e2e-results/. No paid model is called.
//
// Skipped unless E2E_DEMO_CAPTURE=1. Needs E2E_OPERATOR_TOKEN, a completed
// evaluation run, and SIGNALOPS_FAKE_MODEL_DELAY_SECONDS=3 on the worker so the
// run is still going when the worker is killed. Kills and restarts the
// investigation-worker container of the Compose project in the repository root.

import { execSync, spawn } from "node:child_process";
import { resolve } from "node:path";
import { expect, request as apiRequest, test, type APIRequestContext } from "@playwright/test";

const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const SHOTS = resolve(REPO_ROOT, "docs/demo");
const INGEST =
  "docker compose --profile demo run --rm demo demo-post downstream-timeout " +
  "--realtime --duration-s 300 --api-url http://api:8000";

interface Incident {
  id: string;
  title: string;
  detected_at: string;
}

interface ProgressItem {
  event_type: string;
}

test.skip(process.env.E2E_DEMO_CAPTURE !== "1", "set E2E_DEMO_CAPTURE=1 to capture the demo");
test.use({ video: "on", viewport: { width: 1280, height: 900 } });
test.setTimeout(20 * 60_000);

async function json<T>(api: APIRequestContext, path: string): Promise<T> {
  const response = await api.get(path);
  expect(response.ok(), `${path} answered ${String(response.status())}`).toBe(true);
  return (await response.json()) as T;
}

test("demonstration capture", async ({ page, baseURL }) => {
  const token = process.env.E2E_OPERATOR_TOKEN ?? "";
  expect(token, "set E2E_OPERATOR_TOKEN").not.toBe("");
  const api = await apiRequest.newContext({
    baseURL,
    extraHTTPHeaders: { Authorization: `Bearer ${token}` },
  });
  const startedAt = Date.now();
  const ingest = spawn(INGEST, { cwd: REPO_ROOT, shell: true, stdio: "inherit" });
  const ingestDone = new Promise<number | null>((done) => ingest.on("exit", done));

  try {
    await page.goto("/incidents");
    await page.getByLabel("Operator token").fill(token);
    await page.getByRole("button", { name: "Continue" }).click();
    await expect(page.getByRole("heading", { name: "Incidents" })).toBeVisible();

    // 1. The fault's incident, once its windows close.
    let incident: Incident | undefined;
    await expect
      .poll(
        async () => {
          // `docker compose run` may recreate the API while the poster starts.
          const response = await api.get("/v1/incidents?status=open");
          if (!response.ok()) return false;
          const listing = (await response.json()) as { items: Incident[] };
          incident = listing.items.find((item) => Date.parse(item.detected_at) >= startedAt);
          return incident !== undefined;
        },
        { timeout: 8 * 60_000, intervals: [10_000] },
      )
      .toBe(true);
    if (incident === undefined) throw new Error("no incident");
    await page.reload();
    await page.screenshot({ path: `${SHOTS}/01-incidents.png` });
    await page.locator(`a[href="/incidents/${incident.id}"]`).click();
    await expect(page.getByRole("heading", { name: incident.title })).toBeVisible();
    await expect(page.locator("ol.timeline > li").first()).toBeVisible();
    await expect(page.getByRole("img", { name: /^Dependency neighborhood of / })).toBeVisible();
    await page.screenshot({ path: `${SHOTS}/02-incident.png`, fullPage: true });

    // 2. The detector's automatic investigation and its diagnosis.
    const runs = await json<{ items: { id: string }[] }>(
      api,
      `/v1/incidents/${incident.id}/investigations`,
    );
    const automatic = runs.items.at(-1);
    if (automatic === undefined) throw new Error("no automatic investigation");
    await page.goto(`/investigations/${automatic.id}`);
    await expect(page.getByText(/^(Diagnosed|Insufficient evidence)$/)).toBeVisible({
      timeout: 3 * 60_000,
    });
    await page.screenshot({ path: `${SHOTS}/03-diagnosis.png`, fullPage: true });

    // 3. A second investigation; kill the worker after two completed steps.
    await page.goto(`/incidents/${incident.id}`);
    await page.getByRole("button", { name: "Start investigation" }).click();
    await expect(page.getByRole("heading", { name: "Investigation", exact: true })).toBeVisible();
    const investigationId = new URL(page.url()).pathname.split("/").at(-1) ?? "";
    await expect
      .poll(
        async () => {
          const history = await json<{ items: ProgressItem[] }>(
            api,
            `/v1/investigations/${investigationId}/progress?limit=200`,
          );
          return history.items.filter((item) => item.event_type === "step.completed").length;
        },
        { timeout: 2 * 60_000, intervals: [500] },
      )
      .toBeGreaterThanOrEqual(2);
    execSync("docker compose kill investigation-worker", { cwd: REPO_ROOT, stdio: "inherit" });
    await page.waitForTimeout(3_000);
    await page.screenshot({ path: `${SHOTS}/04-worker-killed.png`, fullPage: true });
    execSync("docker compose start investigation-worker", { cwd: REPO_ROOT, stdio: "inherit" });

    // The lease expires, a worker reclaims the job, and the run finishes.
    await expect(page.getByText(/^(Diagnosed|Insufficient evidence)$/)).toBeVisible({
      timeout: 4 * 60_000,
    });
    await expect(page.getByText(/attempt 2/).first()).toBeVisible();
    await page.screenshot({ path: `${SHOTS}/05-resumed.png`, fullPage: true });

    // 4. The baseline comparison of the newest completed evaluation run.
    const evaluations = await json<{ items: { id: string; label: string; status: string }[] }>(
      api,
      "/v1/evaluation-runs",
    );
    const evaluation = evaluations.items.find((item) => item.status === "completed");
    expect(evaluation, "no completed evaluation run").toBeDefined();
    if (evaluation !== undefined) {
      await page.goto(`/evaluations/${evaluation.id}`);
      await expect(page.getByRole("heading", { name: evaluation.label })).toBeVisible();
      await expect(page.getByRole("note")).toContainText("fake provider");
      await page.screenshot({ path: `${SHOTS}/06-baselines.png`, fullPage: true });
    }

    expect(await ingestDone).toBe(0);
  } finally {
    if (ingest.exitCode === null) ingest.kill("SIGINT");
    await api.dispose();
  }
});
