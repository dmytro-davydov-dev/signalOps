// The evaluation review in a real browser against the Compose stack: open a completed
// evaluation run, compare its variants, filter to incorrect cases, and drill into one
// case's cited evidence. No paid model is called; the run must already exist.
//
// Run it with the commands in docs/development.md, "Browser journey check", adding
// E2E_EVALUATION_RUN_ID (default: the newest completed run in the token's workspace).

import { expect, request as apiRequest, test } from "@playwright/test";

interface RunSummary {
  id: string;
  label: string;
  status: string;
}

interface CaseRow {
  id: string;
  scenario_name: string;
  variant: string;
  investigation_id: string | null;
  scores: { outcome_correct: boolean; cited: number; cited_supported: number } | null;
}

function requiredEnv(name: string): string {
  const value = process.env[name];
  if (value === undefined || value === "") {
    throw new Error(`Set ${name}; see docs/development.md, "Browser journey check".`);
  }
  return value;
}

test("a reviewer compares variants and drills into a failed case", async ({ page, baseURL }) => {
  const token = requiredEnv("E2E_OPERATOR_TOKEN");
  const api = await apiRequest.newContext({
    baseURL,
    extraHTTPHeaders: { Authorization: `Bearer ${token}` },
  });

  const list = await api.get("/v1/evaluation-runs");
  expect(list.ok(), `listing runs answered ${String(list.status())}`).toBe(true);
  const runs = ((await list.json()) as { items: RunSummary[] }).items;
  const wanted = process.env.E2E_EVALUATION_RUN_ID;
  const run =
    wanted !== undefined && wanted !== ""
      ? runs.find((item) => item.id === wanted)
      : runs.find((item) => item.status === "completed");
  expect(run, "no completed evaluation run; queue one first").toBeDefined();
  if (run === undefined) return;
  expect(run.status).toBe("completed");

  const detail = await api.get(`/v1/evaluation-runs/${run.id}`);
  const cases = ((await detail.json()) as { cases: CaseRow[] }).cases;
  // A failed case with an investigation, so the drill-in reaches cited evidence.
  const failed = cases.find(
    (item) =>
      item.scores !== null &&
      !item.scores.outcome_correct &&
      item.investigation_id !== null &&
      item.scores.cited > 0,
  );
  expect(failed, "the run has no incorrect case with citations").toBeDefined();
  if (failed === undefined) return;

  await page.goto("/evaluations");
  await page.getByLabel("Operator token").fill(token);
  await page.getByRole("button", { name: "Continue" }).click();
  await expect(page.getByRole("heading", { name: "Evaluation runs" })).toBeVisible();

  await page.getByRole("link", { name: run.label }).first().click();
  await expect(page.getByRole("heading", { name: run.label })).toBeVisible();
  await expect(page.getByRole("note")).toContainText("fake provider");
  const comparison = page.getByRole("table").first();
  for (const variant of [/Deterministic/, /Single agent/i, /Multi-agent/]) {
    await expect(comparison.getByRole("row", { name: variant })).toBeVisible();
  }
  await expect(page.getByRole("heading", { name: "Detection" })).toBeVisible();
  await page.screenshot({ path: "e2e-results/evaluation-run.png", fullPage: true });

  await page.getByLabel("Only incorrect and failed cases").check();
  const caseRows = page.locator("tbody tr").filter({ has: page.locator("th[scope=row] a") });
  await expect(caseRows.getByText("correct", { exact: true })).toHaveCount(0);

  await page.goto(`/evaluations/${run.id}/cases/${failed.id}`);
  await expect(page.getByRole("heading", { name: new RegExp(failed.scenario_name) })).toBeVisible();
  await expect(page.getByText(/^Incorrect/)).toBeVisible();
  await expect(
    page.getByText(
      `${String(failed.scores?.cited_supported)} of ${String(failed.scores?.cited)} citations are fault evidence`,
    ),
  ).toBeVisible();
  await expect(page.getByRole("heading", { name: "Cited evidence" })).toBeVisible();
  await expect(page.getByText(/^(not )?fault evidence$/).first()).toBeVisible();
  await page.screenshot({ path: "e2e-results/evaluation-case.png", fullPage: true });

  // The investigation opens in the case's replay scope with its evidence.
  await page.getByRole("link", { name: /Open the investigation/ }).click();
  await expect(page).toHaveURL(/replay_scope=replay%3A/);
  await expect(page.getByText(/diagnos/i).first()).toBeVisible();
  await page.screenshot({ path: "e2e-results/evaluation-investigation.png", fullPage: true });
  await api.dispose();
});
