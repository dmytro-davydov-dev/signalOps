import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { clearOperatorToken, setOperatorToken } from "../api/operatorToken";
import { fetchByPath } from "../test/incidentFixtures";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const RUN = "11111111-1111-4111-8111-111111111111";
const CASE = "22222222-2222-4222-8222-222222222222";
const GOOD_CASE = "33333333-3333-4333-8333-333333333333";
const INVESTIGATION = "44444444-4444-4444-8444-444444444444";
const SIGNAL = "55555555-5555-4555-8555-555555555555";
const SCOPE = "replay:66666666-6666-4666-8666-666666666666";

const expected = {
  incident_expected: true,
  diagnosable: true,
  root_cause_service: "bank-gateway",
  failure_type: "downstream_timeout",
  affected_services: ["bank-gateway", "checkout", "payment"],
  fault_window: null,
  supporting_event_ids: [],
};

const observed = (service: string) => ({
  incident_ids: ["77777777-7777-4777-8777-777777777777"],
  investigation_id: INVESTIGATION,
  investigation_status: "completed",
  diagnosis_status: "diagnosed",
  root_cause_service: service,
  failure_type: "downstream_timeout",
  cited_signal_ids: [SIGNAL],
  cited_event_ids: ["payment-log-1"],
  detected_at: null,
  started_at: null,
  completed_at: null,
  spent_microusd: 0,
  unknown_reserved_microusd: 0,
  model_calls: 0,
});

const scores = (correct: boolean) => ({
  outcome_correct: correct,
  root_cause_correct: correct,
  abstained: false,
  false_diagnosis: !correct,
  correct_abstention: null,
  cited: 1,
  cited_supported: correct ? 1 : 0,
  evidence_precision: correct ? 1 : 0,
  unsupported_diagnosis: !correct,
  latency_seconds: 1.5,
});

const caseRow = (id: string, variant: string, service: string, correct: boolean) => ({
  id,
  case_key: `downstream-timeout/held-out@1/${variant}/1`,
  scenario_name: "downstream-timeout/held-out",
  scenario_version: "1",
  family: "downstream_timeout",
  split: "held_out",
  synthetic: true,
  variant,
  trial: 1,
  status: "scored",
  replay_scope: SCOPE,
  investigation_id: INVESTIGATION,
  expected,
  observed: observed(service),
  scores: scores(correct),
  error: null,
});

const summary = {
  id: RUN,
  label: "held-out comparison",
  split: "held_out",
  status: "completed",
  variants: ["deterministic", "multi_agent"],
  trials: 1,
  dataset_hash: "a".repeat(64),
  created_at: "2026-11-08T12:00:00Z",
  started_at: "2026-11-08T12:00:01Z",
  completed_at: "2026-11-08T12:05:00Z",
  error: null,
  case_counts: { scored: 2 },
};

const variantMetrics = (accuracy: number) => ({
  samples: 1,
  trials: 1,
  outcome_accuracy: accuracy,
  accuracy_stdev: null,
  root_cause_accuracy: accuracy,
  evidence: { source: "automatic", cited: 1, supported: accuracy, precision: accuracy },
  unsupported_diagnoses: 1 - accuracy,
  unsupported_rate: 1 - accuracy,
  abstentions: 0,
  correct_abstentions: 0,
  abstention_opportunities: 0,
  latency_seconds_p50: 1.5,
  latency_seconds_p95: 1.5,
  cost_microusd_mean: 0,
  harness_failures: 0,
});

const detail = {
  ...summary,
  seed: 0,
  workflow_versions: { deterministic: "d1", multi_agent: "v3" },
  prompt_versions: {},
  model_settings: { provider: "fake", model: "fake-1", measures_model_quality: false },
  pricing_config: {},
  limits: { budget_microusd: 3000000, max_steps: 40, deadline_seconds: 600 },
  metrics: {
    variants: { deterministic: variantMetrics(0), multi_agent: variantMetrics(1) },
    detection: {
      replays_scored: 1,
      true_positives: 1,
      false_positives: 0,
      false_negatives: 0,
      precision: 1,
      recall: 1,
      delay_seconds_p50: 90,
      delay_seconds_p95: 90,
    },
  },
  cases: [
    caseRow(CASE, "deterministic", "payment", false),
    caseRow(GOOD_CASE, "multi_agent", "bank-gateway", true),
  ],
};

const caseDetail = {
  ...caseRow(CASE, "deterministic", "payment", false),
  replay_report: { status: "completed" },
  citations: [
    {
      signal_id: SIGNAL,
      source_event_id: "payment-log-1",
      service_name: "payment",
      kind: "log",
      severity: "error",
      occurred_at: "2026-10-16T10:35:00Z",
      message: "upstream bank-gateway call timed out after 2000 ms",
      fault_evidence: false,
    },
  ],
  audits: [],
};

describe("evaluation pages", () => {
  beforeEach(() => {
    clearOperatorToken({ rejected: false });
    setOperatorToken("operator-token");
  });

  it("lists runs and says when there are none", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(fetchByPath({ "/v1/evaluation-runs": () => jsonResponse({ items: [] }) })),
    );
    renderRoute("/evaluations");
    expect(await screen.findByText(/No evaluation runs yet/)).toBeInTheDocument();
  });

  it("compares variants under one budget and filters to failed cases", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn(fetchByPath({ [`/v1/evaluation-runs/${RUN}`]: () => jsonResponse(detail) })),
    );
    renderRoute(`/evaluations/${RUN}`);

    expect(await screen.findByRole("heading", { name: "held-out comparison" })).toBeInTheDocument();
    expect(screen.getByRole("note")).toHaveTextContent("fake provider");
    expect(screen.getByText(/the same for every variant/)).toBeInTheDocument();
    const [comparison] = screen.getAllByRole("table");
    if (comparison === undefined) throw new Error("no comparison table");
    expect(within(comparison).getByRole("row", { name: /Multi-agent/ })).toHaveTextContent("100%");
    expect(within(comparison).getByRole("row", { name: /Deterministic rule/ })).toHaveTextContent("0%");
    expect(screen.getByText(/Precision 100%, recall 100%/)).toBeInTheDocument();

    await userEvent.click(screen.getByLabelText("Only incorrect and failed cases"));
    const links = screen.getAllByRole("link", { name: "downstream-timeout/held-out" });
    expect(links).toHaveLength(1);
    expect(links[0]).toHaveAttribute("href", `/evaluations/${RUN}/cases/${CASE}`);
  });

  it("shows a failed case's evidence and records an audit", async () => {
    const audit = vi.fn(() =>
      jsonResponse(
        {
          signal_id: SIGNAL,
          supported: false,
          auditor: "reviewer",
          note: null,
          created_at: "2026-11-08T12:10:00Z",
        },
        201,
      ),
    );
    vi.stubGlobal(
      "fetch",
      vi.fn(
        fetchByPath({
          [`/v1/evaluation-runs/${RUN}/cases/${CASE}`]: () => jsonResponse(caseDetail),
          [`/v1/evaluation-runs/${RUN}/cases/${CASE}/audits`]: audit,
        }),
      ),
    );
    renderRoute(`/evaluations/${RUN}/cases/${CASE}`);

    expect(await screen.findByText("bank-gateway · downstream_timeout")).toBeInTheDocument();
    expect(screen.getByText("payment · downstream_timeout")).toBeInTheDocument();
    expect(screen.getByText(/no cited signal is fault evidence/)).toBeInTheDocument();
    expect(screen.getByText("not fault evidence")).toBeInTheDocument();
    expect(screen.getByRole("link", { name: /Open the investigation/ })).toHaveAttribute(
      "href",
      `/investigations/${INVESTIGATION}?replay_scope=${encodeURIComponent(SCOPE)}`,
    );

    const doesNot = screen.getByRole("button", { name: "Does not" });
    expect(doesNot).toBeDisabled();
    await userEvent.type(screen.getByLabelText("Auditor name"), "reviewer");
    await userEvent.click(doesNot);
    expect(audit).toHaveBeenCalledOnce();
  });
});
