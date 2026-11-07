import { getJson, postJson } from "./client";
import { LIVE_SCOPE } from "./incidents";
import { isInvestigationStatus } from "./investigationStatus";
import type { InvestigationStatus } from "./generated/investigationStatus";

export interface InvestigationUsage {
  budget_microusd: number;
  reserved_microusd: number;
  spent_microusd: number;
}

export interface InvestigationDiagnosis {
  status: string;
  summary: string;
}

export interface Investigation {
  id: string;
  incident_id: string;
  replay_scope: string;
  status: InvestigationStatus;
  created_at: string;
  deadline_at: string;
  completed_at: string | null;
  cancel_requested_at: string | null;
  usage: InvestigationUsage;
  diagnosis: InvestigationDiagnosis | null;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isInvestigation(value: unknown): value is Investigation {
  return (
    isRecord(value) &&
    typeof value.id === "string" &&
    typeof value.incident_id === "string" &&
    typeof value.replay_scope === "string" &&
    isInvestigationStatus(value.status) &&
    typeof value.created_at === "string" &&
    typeof value.deadline_at === "string" &&
    (value.completed_at === null || typeof value.completed_at === "string") &&
    (value.cancel_requested_at === null || typeof value.cancel_requested_at === "string") &&
    isRecord(value.usage) &&
    (value.diagnosis === null ||
      (isRecord(value.diagnosis) && typeof value.diagnosis.summary === "string"))
  );
}

const parseInvestigation = (value: unknown) => (isInvestigation(value) ? value : undefined);

export function fetchInvestigation(
  id: string,
  replayScope: string,
  signal?: AbortSignal,
): Promise<Investigation> {
  const search =
    replayScope === LIVE_SCOPE ? "" : `?replay_scope=${encodeURIComponent(replayScope)}`;
  return getJson(`/v1/investigations/${encodeURIComponent(id)}${search}`, parseInvestigation, {
    operator: true,
    signal,
  });
}

export function cancelInvestigation(id: string): Promise<Investigation> {
  return postJson(`/v1/investigations/${encodeURIComponent(id)}/cancel`, {}, parseInvestigation, {
    operator: true,
  });
}
