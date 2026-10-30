import {
  DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS,
  INVESTIGATION_STATUSES,
  TERMINAL_INVESTIGATION_STATUSES,
  type DiagnosisStatus,
  type InvestigationStatus,
} from "./generated/investigationStatus";

export type StatusTone = "pending" | "success" | "abstained" | "failure" | "neutral";

export interface InvestigationStatusView {
  label: string;
  tone: StatusTone;
  terminal: boolean;
  // The diagnosis status the API returns with this investigation status, if any.
  diagnosis: DiagnosisStatus | null;
}

// Insufficient evidence is a successful abstention, shown apart from failures.
const presentation: Record<InvestigationStatus, { label: string; tone: StatusTone }> = {
  queued: { label: "Queued", tone: "pending" },
  running: { label: "Running", tone: "pending" },
  completed: { label: "Diagnosed", tone: "success" },
  insufficient_evidence: { label: "Insufficient evidence", tone: "abstained" },
  failed: { label: "Failed", tone: "failure" },
  cancelled: { label: "Cancelled", tone: "neutral" },
  budget_exceeded: { label: "Budget exceeded", tone: "failure" },
};

export function isInvestigationStatus(value: unknown): value is InvestigationStatus {
  return (INVESTIGATION_STATUSES as readonly unknown[]).includes(value);
}

export function describeInvestigationStatus(status: InvestigationStatus): InvestigationStatusView {
  return {
    ...presentation[status],
    terminal: TERMINAL_INVESTIGATION_STATUSES.includes(status),
    diagnosis: DIAGNOSIS_STATUS_BY_INVESTIGATION_STATUS[status],
  };
}
