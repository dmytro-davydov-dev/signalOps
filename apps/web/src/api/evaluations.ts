import {
  isBoolean,
  isListOf,
  isNullableNumber,
  isNullableString,
  isNumber,
  isRecord,
  isString,
  matches,
} from "./checks";
import { getJson, postJson } from "./client";

export const SYSTEM_VARIANTS = ["deterministic", "single_agent", "multi_agent"] as const;
export type SystemVariant = (typeof SYSTEM_VARIANTS)[number];

export const VARIANT_LABELS: Record<string, string> = {
  deterministic: "Deterministic rule",
  single_agent: "Single agent",
  multi_agent: "Multi-agent",
};

export interface EvaluationRunSummary {
  id: string;
  label: string;
  split: string;
  status: string;
  variants: string[];
  trials: number;
  dataset_hash: string;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  error: string | null;
  case_counts: Record<string, number>;
}

export interface ExpectedOutcome {
  incident_expected: boolean;
  diagnosable: boolean;
  root_cause_service: string | null;
  failure_type: string | null;
  affected_services: string[];
}

export interface ObservedOutcome {
  incident_ids: string[];
  investigation_id: string | null;
  investigation_status: string | null;
  diagnosis_status: string | null;
  root_cause_service: string | null;
  failure_type: string | null;
  cited_signal_ids: string[];
  spent_microusd: number;
  model_calls: number;
}

export interface CaseScores {
  outcome_correct: boolean;
  root_cause_correct: boolean | null;
  abstained: boolean;
  false_diagnosis: boolean;
  correct_abstention: boolean | null;
  cited: number;
  cited_supported: number;
  evidence_precision: number | null;
  unsupported_diagnosis: boolean;
  latency_seconds: number | null;
}

/** First failing stages (P13-T03), in the order they are checked. */
export const FAILURE_STAGES = [
  "harness_failure",
  "detection_miss",
  "no_lead",
  "lead_lost_in_synthesis",
  "verifier_false_reject",
  "verifier_false_accept",
  "conclude_dropped",
  "wrong_failure_type",
  "false_diagnosis_on_underdetermined",
  "rule_error",
  "unattributed",
] as const;

export const FAILURE_STAGE_LABELS: Record<string, string> = {
  harness_failure: "Harness failure",
  detection_miss: "Detection missed the fault",
  no_lead: "No specialist found the cause",
  lead_lost_in_synthesis: "Lead lost in synthesis",
  verifier_false_reject: "Verifier rejected the true cause",
  verifier_false_accept: "Verifier accepted a wrong cause",
  conclude_dropped: "Conclusion dropped an accepted cause",
  wrong_failure_type: "Right service, wrong failure type",
  false_diagnosis_on_underdetermined: "Diagnosed where abstaining was correct",
  rule_error: "Baseline rule was wrong",
  unattributed: "Unattributed (no stage scores)",
};

export interface EvaluationCaseRow {
  id: string;
  case_key: string;
  scenario_name: string;
  scenario_version: string;
  family: string;
  split: string;
  synthetic: boolean;
  variant: string;
  trial: number;
  status: string;
  replay_scope: string;
  investigation_id: string | null;
  expected: ExpectedOutcome;
  observed: ObservedOutcome | null;
  scores: CaseScores | null;
  /** Loosely typed; see docs/evaluation.md (stage scores). Absent on older servers. */
  stage_scores?: Record<string, unknown> | null;
  failure_stage?: string | null;
  failure_origin?: string | null;
  limit_hit?: boolean | null;
  error: string | null;
}

/** Stored as the server computed it; see docs/evaluation.md for every field. */
export type Metrics = Record<string, unknown>;

export interface EvaluationRunDetail extends EvaluationRunSummary {
  seed: number;
  workflow_versions: Record<string, unknown>;
  prompt_versions: Record<string, unknown>;
  model_settings: Record<string, unknown>;
  pricing_config: Record<string, unknown>;
  limits: Record<string, unknown>;
  metrics: Metrics | null;
  cases: EvaluationCaseRow[];
}

export interface CitedSignal {
  signal_id: string;
  source_event_id: string;
  service_name: string | null;
  kind: string;
  severity: string;
  occurred_at: string;
  message: string;
  fault_evidence: boolean;
}

export interface AuditView {
  signal_id: string;
  supported: boolean;
  auditor: string;
  note: string | null;
  created_at: string;
}

export interface StageClaim {
  claim: string;
  service_name: string | null;
  failure_type: string | null;
  confidence: number | null;
  evidence_signal_ids: string[];
  supporting_roles: string[];
}

export interface StageOutputView {
  step_key: string;
  role: string;
  ended_by: string | null;
  status: string | null;
  findings: StageClaim[];
  leading: StageClaim | null;
  verdict: string | null;
  citation_checks: { signal_id: string; supports: boolean }[];
  missing_evidence: string[];
  note: string | null;
}

export interface EvaluationCaseDetail extends EvaluationCaseRow {
  replay_report: Record<string, unknown> | null;
  citations: CitedSignal[];
  audits: AuditView[];
  stage_outputs?: StageOutputView[];
  stage_signals?: CitedSignal[];
}

export interface AuditInput {
  signal_id: string;
  supported: boolean;
  auditor: string;
  note?: string;
}

const isNullableBoolean = (value: unknown) => value === null || typeof value === "boolean";
const isCounts = (value: unknown) => isRecord(value) && Object.values(value).every(isNumber);
const isNullableRecord = (value: unknown) => value === null || isRecord(value);
const optional = (check: (value: unknown) => boolean) => (value: unknown) =>
  value === undefined || check(value);

const summaryChecks = {
  id: isString,
  label: isString,
  split: isString,
  status: isString,
  variants: isListOf(isString),
  trials: isNumber,
  dataset_hash: isString,
  created_at: isString,
  started_at: isNullableString,
  completed_at: isNullableString,
  error: isNullableString,
  case_counts: isCounts,
};
const isSummary = matches(summaryChecks);

const isExpected = matches({
  incident_expected: isBoolean,
  diagnosable: isBoolean,
  root_cause_service: isNullableString,
  failure_type: isNullableString,
  affected_services: isListOf(isString),
});

const isObserved = matches({
  incident_ids: isListOf(isString),
  investigation_id: isNullableString,
  investigation_status: isNullableString,
  diagnosis_status: isNullableString,
  root_cause_service: isNullableString,
  failure_type: isNullableString,
  cited_signal_ids: isListOf(isString),
  spent_microusd: isNumber,
  model_calls: isNumber,
});

const isScores = matches({
  outcome_correct: isBoolean,
  root_cause_correct: isNullableBoolean,
  abstained: isBoolean,
  false_diagnosis: isBoolean,
  correct_abstention: isNullableBoolean,
  cited: isNumber,
  cited_supported: isNumber,
  evidence_precision: isNullableNumber,
  unsupported_diagnosis: isBoolean,
  latency_seconds: isNullableNumber,
});

const caseChecks = {
  id: isString,
  case_key: isString,
  scenario_name: isString,
  scenario_version: isString,
  family: isString,
  split: isString,
  synthetic: isBoolean,
  variant: isString,
  trial: isNumber,
  status: isString,
  replay_scope: isString,
  investigation_id: isNullableString,
  expected: isExpected,
  observed: (value: unknown) => value === null || isObserved(value),
  scores: (value: unknown) => value === null || isScores(value),
  stage_scores: optional(isNullableRecord),
  failure_stage: optional(isNullableString),
  failure_origin: optional(isNullableString),
  limit_hit: optional(isNullableBoolean),
  error: isNullableString,
};
const isCaseRow = matches(caseChecks);

const isDetail = matches({
  ...summaryChecks,
  seed: isNumber,
  workflow_versions: isRecord,
  prompt_versions: isRecord,
  model_settings: isRecord,
  pricing_config: isRecord,
  limits: isRecord,
  metrics: isNullableRecord,
  cases: isListOf(isCaseRow),
});

const isCited = matches({
  signal_id: isString,
  source_event_id: isString,
  service_name: isNullableString,
  kind: isString,
  severity: isString,
  occurred_at: isString,
  message: isString,
  fault_evidence: isBoolean,
});

const isAudit = matches({
  signal_id: isString,
  supported: isBoolean,
  auditor: isString,
  note: isNullableString,
  created_at: isString,
});

const isClaim = matches({
  claim: isString,
  service_name: isNullableString,
  failure_type: isNullableString,
  confidence: isNullableNumber,
  evidence_signal_ids: isListOf(isString),
  supporting_roles: isListOf(isString),
});

const isStageOutput = matches({
  step_key: isString,
  role: isString,
  ended_by: isNullableString,
  status: isNullableString,
  findings: isListOf(isClaim),
  leading: (value: unknown) => value === null || isClaim(value),
  verdict: isNullableString,
  citation_checks: isListOf(matches({ signal_id: isString, supports: isBoolean })),
  missing_evidence: isListOf(isString),
  note: isNullableString,
});

const isCaseDetail = matches({
  ...caseChecks,
  replay_report: isNullableRecord,
  citations: isListOf(isCited),
  audits: isListOf(isAudit),
  stage_outputs: optional(isListOf(isStageOutput)),
  stage_signals: optional(isListOf(isCited)),
});

const isRunList = matches({ items: isListOf(isSummary) });

const runsPath = "/v1/evaluation-runs";
const runPath = (id: string) => `${runsPath}/${encodeURIComponent(id)}`;
const casePath = (runId: string, caseId: string) =>
  `${runPath(runId)}/cases/${encodeURIComponent(caseId)}`;

export function fetchEvaluationRuns(signal?: AbortSignal): Promise<EvaluationRunSummary[]> {
  return getJson(
    runsPath,
    (value) => (isRunList(value) ? (value as { items: EvaluationRunSummary[] }).items : undefined),
    { operator: true, signal },
  );
}

export function fetchEvaluationRun(id: string, signal?: AbortSignal): Promise<EvaluationRunDetail> {
  return getJson(
    runPath(id),
    (value) => (isDetail(value) ? (value as EvaluationRunDetail) : undefined),
    { operator: true, signal },
  );
}

export function fetchEvaluationCase(
  runId: string,
  caseId: string,
  signal?: AbortSignal,
): Promise<EvaluationCaseDetail> {
  return getJson(
    casePath(runId, caseId),
    (value) => (isCaseDetail(value) ? (value as EvaluationCaseDetail) : undefined),
    { operator: true, signal },
  );
}

export function recordAudit(
  runId: string,
  caseId: string,
  audit: AuditInput,
): Promise<AuditView> {
  return postJson(
    `${casePath(runId, caseId)}/audits`,
    audit,
    (value) => (isAudit(value) ? (value as AuditView) : undefined),
    { operator: true },
  );
}

/** A number from loosely typed metrics, or undefined when absent or not numeric. */
export function metric(source: unknown, ...path: string[]): number | undefined {
  let value: unknown = source;
  for (const key of path) {
    if (!isRecord(value)) return undefined;
    value = value[key];
  }
  return typeof value === "number" ? value : undefined;
}
