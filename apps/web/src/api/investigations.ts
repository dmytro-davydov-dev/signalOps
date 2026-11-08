import {
  isListOf,
  isNullableString,
  isNumber,
  isOneOf,
  isString,
  matches,
} from "./checks";
import { getJson, postJson } from "./client";
import { isEvidenceSignal, LIVE_SCOPE, type EvidenceSignal } from "./incidents";
import { isInvestigationStatus } from "./investigationStatus";
import { DIAGNOSIS_STATUSES, type DiagnosisStatus, type InvestigationStatus } from "./generated/investigationStatus";

export interface InvestigationUsage {
  budget_microusd: number;
  reserved_microusd: number;
  spent_microusd: number;
  model_calls: number;
  input_tokens: number;
  output_tokens: number;
  cache_read_tokens: number;
  cache_write_tokens: number;
  /** Provider-reported tokens at published prices. */
  actual_microusd: number;
  /** Spend estimated without provider usage. */
  estimated_microusd: number;
  /** Calls whose cost cannot be known; they may have been billed. */
  unknown_calls: number;
  unknown_reserved_microusd: number;
}

export interface RootCause {
  service_id: string;
  service_name: string;
  failure_type: string;
  confidence: number;
  evidence_signal_ids: string[];
}

export interface Diagnosis {
  status: DiagnosisStatus;
  summary: string;
  root_cause: RootCause | null;
  alternatives: { hypothesis: string; assessment: string }[];
  suggested_actions: { action: string; requires_approval: boolean }[];
  limitations: string[];
  missing_evidence: string[];
  workflow_version: string;
}

export interface Investigation {
  id: string;
  incident_id: string;
  replay_scope: string;
  status: InvestigationStatus;
  workflow_version: string;
  evidence_cutoff_at: string;
  created_at: string;
  deadline_at: string;
  completed_at: string | null;
  cancel_requested_at: string | null;
  max_steps: number;
  usage: InvestigationUsage;
  diagnosis: Diagnosis | null;
}

export interface InvestigationSummary {
  id: string;
  status: InvestigationStatus;
  created_at: string;
  completed_at: string | null;
  cancel_requested_at: string | null;
  diagnosis_status: DiagnosisStatus | null;
}

export interface IncidentInvestigations {
  incident_id: string;
  replay_scope: string;
  items: InvestigationSummary[];
  truncated: boolean;
}

export interface InvestigationEvidence {
  investigation_id: string;
  evidence_cutoff_at: string;
  /** Cited signals in scope and known by the cutoff, in citation order. */
  items: EvidenceSignal[];
  /** Citations that failed that check. */
  unverified_signal_ids: string[];
}

export interface InvestigationAccepted {
  id: string;
  incident_id: string;
  status: InvestigationStatus;
  created_at: string;
}

const isDiagnosisStatus = isOneOf(DIAGNOSIS_STATUSES);
const isNullableDiagnosisStatus = (value: unknown) => value === null || isDiagnosisStatus(value);

const isUsage = matches({
  budget_microusd: isNumber,
  reserved_microusd: isNumber,
  spent_microusd: isNumber,
  model_calls: isNumber,
  input_tokens: isNumber,
  output_tokens: isNumber,
  cache_read_tokens: isNumber,
  cache_write_tokens: isNumber,
  actual_microusd: isNumber,
  estimated_microusd: isNumber,
  unknown_calls: isNumber,
  unknown_reserved_microusd: isNumber,
});

const isDiagnosis = matches({
  status: isDiagnosisStatus,
  summary: isString,
  root_cause: (value: unknown) =>
    value === null ||
    matches({
      service_id: isString,
      service_name: isString,
      failure_type: isString,
      confidence: isNumber,
      evidence_signal_ids: isListOf(isString),
    })(value),
  alternatives: isListOf(matches({ hypothesis: isString, assessment: isString })),
  suggested_actions: isListOf(
    matches({ action: isString, requires_approval: (value: unknown) => value === true }),
  ),
  limitations: isListOf(isString),
  missing_evidence: isListOf(isString),
  workflow_version: isString,
});

const isInvestigation = matches({
  id: isString,
  incident_id: isString,
  replay_scope: isString,
  status: isInvestigationStatus,
  workflow_version: isString,
  evidence_cutoff_at: isString,
  created_at: isString,
  deadline_at: isString,
  completed_at: isNullableString,
  cancel_requested_at: isNullableString,
  max_steps: isNumber,
  usage: isUsage,
  diagnosis: (value: unknown) => value === null || isDiagnosis(value),
});

const isSummary = matches({
  id: isString,
  status: isInvestigationStatus,
  created_at: isString,
  completed_at: isNullableString,
  cancel_requested_at: isNullableString,
  diagnosis_status: isNullableDiagnosisStatus,
});

const isIncidentInvestigations = matches({
  incident_id: isString,
  replay_scope: isString,
  items: isListOf(isSummary),
  truncated: (value: unknown) => typeof value === "boolean",
});

const isEvidence = matches({
  investigation_id: isString,
  evidence_cutoff_at: isString,
  items: isListOf(isEvidenceSignal),
  unverified_signal_ids: isListOf(isString),
});

const isAccepted = matches({
  id: isString,
  incident_id: isString,
  status: isInvestigationStatus,
  created_at: isString,
});

const parseInvestigation = (value: unknown) =>
  isInvestigation(value) ? (value as Investigation) : undefined;
const parseEvidence = (value: unknown) =>
  isEvidence(value) ? (value as InvestigationEvidence) : undefined;
const parseIncidentInvestigations = (value: unknown) =>
  isIncidentInvestigations(value) ? (value as IncidentInvestigations) : undefined;
const parseAccepted = (value: unknown) =>
  isAccepted(value) ? (value as InvestigationAccepted) : undefined;

function scoped(replayScope: string): string {
  return replayScope === LIVE_SCOPE ? "" : `?replay_scope=${encodeURIComponent(replayScope)}`;
}

const investigationPath = (id: string) => `/v1/investigations/${encodeURIComponent(id)}`;

export function fetchInvestigation(
  id: string,
  replayScope: string,
  signal?: AbortSignal,
): Promise<Investigation> {
  return getJson(`${investigationPath(id)}${scoped(replayScope)}`, parseInvestigation, {
    operator: true,
    signal,
  });
}

export function fetchInvestigationEvidence(
  id: string,
  replayScope: string,
  signal?: AbortSignal,
): Promise<InvestigationEvidence> {
  return getJson(
    `${investigationPath(id)}/evidence${scoped(replayScope)}`,
    parseEvidence,
    { operator: true, signal },
  );
}

export function fetchIncidentInvestigations(
  incidentId: string,
  replayScope: string,
  signal?: AbortSignal,
): Promise<IncidentInvestigations> {
  return getJson(
    `/v1/incidents/${encodeURIComponent(incidentId)}/investigations${scoped(replayScope)}`,
    parseIncidentInvestigations,
    { operator: true, signal },
  );
}

/**
 * Request an investigation. The same key returns the same investigation, so a
 * repeated click or a retry after a lost response cannot start a second one.
 */
export function requestInvestigation(
  incidentId: string,
  idempotencyKey: string,
): Promise<InvestigationAccepted> {
  return postJson(
    `/v1/incidents/${encodeURIComponent(incidentId)}/investigations`,
    {},
    parseAccepted,
    { operator: true, headers: { "Idempotency-Key": idempotencyKey } },
  );
}

export function cancelInvestigation(id: string): Promise<Investigation> {
  return postJson(`${investigationPath(id)}/cancel`, {}, parseInvestigation, {
    operator: true,
  });
}
