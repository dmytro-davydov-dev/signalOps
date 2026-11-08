// The R1 journey in a real browser against the Compose stack and the fake provider:
// ingest a fault, find its incident, inspect evidence and the dependency graph,
// start an investigation, reconnect to its progress stream, inspect the result,
// and cancel another run. No paid model is called.
//
// Run it with the commands in docs/development.md, "Browser journey check".

import { spawn, type ChildProcess } from "node:child_process";
import { resolve } from "node:path";
import { expect, request as apiRequest, test, type APIRequestContext } from "@playwright/test";

const REPO_ROOT = resolve(import.meta.dirname, "../../..");
const DEFAULT_INGEST =
  "docker compose --profile demo run --rm demo demo-post downstream-timeout " +
  "--realtime --duration-s 150 --api-url http://api:8000";

interface Incident {
  id: string;
  title: string;
  detected_at: string;
}

interface ProgressItem {
  id: number;
  event_type: string;
  payload: { step_key?: string; attempt?: number };
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`Set ${name}; see docs/development.md, "Browser journey check".`);
  }
  return value;
}

function startIngest(): { process: ChildProcess; done: Promise<number | null> } {
  const command = process.env.E2E_INGEST_COMMAND ?? DEFAULT_INGEST;
  const child = spawn(command, { cwd: REPO_ROOT, shell: true, stdio: "inherit" });
  const done = new Promise<number | null>((resolveExit) => {
    child.on("exit", (code) => {
      resolveExit(code);
    });
  });
  return { process: child, done };
}

async function json<T>(api: APIRequestContext, path: string): Promise<T> {
  const response = await api.get(path);
  expect(response.ok(), `${path} answered ${String(response.status())}`).toBe(true);
  return (await response.json()) as T;
}

async function investigationCount(api: APIRequestContext, incidentId: string): Promise<number> {
  const body = await json<{ items: unknown[] }>(api, `/v1/incidents/${incidentId}/investigations`);
  return body.items.length;
}

/** Each step's latest attempt, as the page should show it. */
function latestSteps(items: ProgressItem[]): string[] {
  const steps = new Map<string, number>();
  for (const item of items) {
    const key = item.payload.step_key;
    if (key !== undefined && (steps.get(key) ?? 0) <= (item.payload.attempt ?? 1)) {
      steps.set(key, item.payload.attempt ?? 1);
    }
  }
  return [...steps.keys()];
}

test("an operator follows a fault from ingestion to a diagnosis", async ({ page, baseURL }) => {
  const token = requiredEnv("E2E_OPERATOR_TOKEN");
  const api = await apiRequest.newContext({
    baseURL,
    extraHTTPHeaders: { Authorization: `Bearer ${token}` },
  });
  const startedAt = Date.now();
  const ingest = startIngest();

  try {
    // Sign in.
    await page.goto("/incidents");
    await page.getByLabel("Operator token").fill(token);
    await page.getByRole("button", { name: "Continue" }).click();
    await expect(page.getByRole("heading", { name: "Incidents" })).toBeVisible();

    // Wait for the fault's incident: the detector fires once its windows close.
    let incident: Incident | undefined;
    await expect
      .poll(
        async () => {
          const listing = await json<{ items: Incident[] }>(api, "/v1/incidents?status=open");
          incident = listing.items.find((item) => Date.parse(item.detected_at) >= startedAt);
          return incident !== undefined;
        },
        { timeout: 8 * 60_000, intervals: [10_000] },
      )
      .toBe(true);
    if (incident === undefined) {
      throw new Error("no incident");
    }

    // Find it in the list and open it.
    await page.reload();
    await page.locator(`a[href="/incidents/${incident.id}"]`).click();
    await expect(page.getByRole("heading", { name: incident.title })).toBeVisible();

    // Evidence, its quality, and the dependency graph.
    await expect(page.getByRole("heading", { name: "Evidence timeline" })).toBeVisible();
    await expect(page.locator("ol.timeline > li").first()).toBeVisible();
    await expect(page.getByRole("heading", { name: "Data quality" })).toBeVisible();
    await expect(page.getByRole("img", { name: /^Dependency neighborhood of / })).toBeVisible();

    // Start an investigation; a double click must not start two.
    await expect(page.getByRole("heading", { name: "Investigations" })).toBeVisible();
    const before = await investigationCount(api, incident.id);
    const lastEventIds: string[] = [];
    page.on("request", (sent) => {
      if (sent.url().includes("/events")) {
        lastEventIds.push(sent.headers()["last-event-id"] ?? "");
      }
    });
    await page.getByRole("button", { name: "Start investigation" }).dblclick();
    await expect(page.getByRole("heading", { name: "Investigation", exact: true })).toBeVisible();
    const investigationId = new URL(page.url()).pathname.split("/").at(-1) ?? "";
    expect(await investigationCount(api, incident.id)).toBe(before + 1);

    // Progress arrives live; then reconnect mid-run.
    const steps = page.getByRole("list", { name: "Workflow steps" });
    await expect(steps.getByRole("listitem").first()).toBeVisible({ timeout: 60_000 });
    await page.reload();
    await expect(steps.getByRole("listitem").first()).toBeVisible();

    // The run ends: a diagnosis or an abstention, both valid outcomes.
    const outcome = page.getByText(/^(Diagnosed|Insufficient evidence)$/);
    await expect(outcome).toBeVisible({ timeout: 3 * 60_000 });
    // After the reload the stream resumed from the history it had read.
    expect(lastEventIds.some((value) => /^\d+$/.test(value))).toBe(true);

    // The page shows every step the durable history holds, once each.
    const history = await json<{ items: ProgressItem[] }>(
      api,
      `/v1/investigations/${investigationId}/progress?limit=200`,
    );
    expect(history.items.at(-1)?.event_type).toBe("investigation.ended");
    const shown = await steps.locator(".step-name").allTextContents();
    expect(shown).toEqual(latestSteps(history.items));

    // Inspect the result and its cost.
    if ((await outcome.textContent()) === "Diagnosed") {
      const cited = page.getByRole("list", { name: "Cited evidence" });
      await expect(cited.getByText("verified").first()).toBeVisible();
      await expect(page.getByText(/^\(the model's own estimate, not calibrated\)$/)).toBeVisible();
    } else {
      await expect(page.getByText(/valid outcome, not a failure/)).toBeVisible();
    }
    await expect(page.getByText(/estimated\)/)).toBeVisible();

    // Cancel another run.
    await page.getByRole("link", { name: "Incident", exact: true }).click();
    await page.getByRole("button", { name: "Start investigation" }).click();
    await expect(page.getByRole("heading", { name: "Investigation", exact: true })).toBeVisible();
    await page.getByRole("button", { name: "Cancel investigation" }).click();
    await expect(page.getByText("Cancelled", { exact: true })).toBeVisible({ timeout: 60_000 });
    await expect(page.getByText(/was cancelled\. No diagnosis was stored/)).toBeVisible();

    expect(await ingest.done).toBe(0);
  } finally {
    if (ingest.process.exitCode === null) {
      ingest.process.kill("SIGINT");
    }
    await api.dispose();
  }
});
