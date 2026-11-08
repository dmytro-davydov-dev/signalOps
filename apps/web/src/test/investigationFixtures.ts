import type { Investigation, InvestigationEvidence } from "../api/investigations";
import type { ProgressEvent } from "../api/progress";
import { CHECKOUT, GATEWAY, INCIDENT_ID } from "./incidentFixtures";
import { jsonResponse } from "./renderRoute";

export const INVESTIGATION_ID = "9b1f2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d";
export const CITED = "22222222-2222-4222-8222-222222222222";
export const MISSING = "44444444-4444-4444-8444-444444444444";

export const noUsage = {
  budget_microusd: 1_000_000,
  reserved_microusd: 0,
  spent_microusd: 0,
  model_calls: 0,
  input_tokens: 0,
  output_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
  actual_microusd: 0,
  estimated_microusd: 0,
  unknown_calls: 0,
  unknown_reserved_microusd: 0,
};

export const running: Investigation = {
  id: INVESTIGATION_ID,
  incident_id: INCIDENT_ID,
  replay_scope: "live",
  status: "running",
  workflow_version: "v1",
  evidence_cutoff_at: "2026-11-07T10:00:00Z",
  created_at: "2026-11-07T10:00:00Z",
  deadline_at: "2026-11-07T10:10:00Z",
  completed_at: null,
  cancel_requested_at: null,
  max_steps: 12,
  usage: noUsage,
  diagnosis: null,
};

export const diagnosed: Investigation = {
  ...running,
  status: "completed",
  completed_at: "2026-11-07T10:01:00Z",
  usage: {
    ...noUsage,
    spent_microusd: 78_076,
    actual_microusd: 41_250,
    estimated_microusd: 36_826,
    model_calls: 3,
    input_tokens: 8_200,
    output_tokens: 420,
  },
  diagnosis: {
    status: "diagnosed",
    summary: "Checkout failures follow bank-gateway timeouts.",
    root_cause: {
      service_id: GATEWAY,
      service_name: "bank-gateway",
      failure_type: "downstream_timeout",
      confidence: 0.82,
      evidence_signal_ids: [CITED],
    },
    alternatives: [{ hypothesis: "Database outage", assessment: "No database errors appear." }],
    suggested_actions: [{ action: "Review the gateway timeout", requires_approval: true }],
    limitations: ["No database connection metrics were available"],
    missing_evidence: [],
    workflow_version: "v1",
  },
};

export const abstained: Investigation = {
  ...running,
  status: "insufficient_evidence",
  completed_at: "2026-11-07T10:01:00Z",
  diagnosis: {
    status: "insufficient_evidence",
    summary: "No diagnosis for: checkout",
    root_cause: null,
    alternatives: [],
    suggested_actions: [],
    limitations: [],
    missing_evidence: ["Traces from the payment service"],
    workflow_version: "v1",
  },
};

export const evidence: InvestigationEvidence = {
  investigation_id: INVESTIGATION_ID,
  evidence_cutoff_at: running.evidence_cutoff_at,
  items: [
    {
      id: CITED,
      service_id: CHECKOUT,
      kind: "trace",
      occurred_at: "2026-11-07T09:59:05Z",
      severity: "error",
      message: "POST /charge timed out",
      trace_id: "trace-1",
      attributes: { api_key: "[REDACTED]" },
      quality_status: "accepted",
      quality_flags: [],
    },
  ],
  unverified_signal_ids: [],
};

export function step(
  id: number,
  eventType: "step.started" | "step.completed" | "step.failed",
  stepKey: string,
  attempt = 1,
  errorCategory?: string,
): ProgressEvent {
  return {
    id,
    event_type: eventType,
    payload: {
      step_key: stepKey,
      agent_role: "investigator",
      attempt,
      ...(errorCategory ? { error_category: errorCategory } : {}),
    },
    created_at: "2026-11-07T10:00:01Z",
  };
}

export function ended(id: number, status: string, diagnosisStatus: string | null): ProgressEvent {
  return {
    id,
    event_type: "investigation.ended",
    payload: { status, diagnosis_status: diagnosisStatus },
    created_at: "2026-11-07T10:01:00Z",
  };
}

export function progressPage(items: ProgressEvent[], after = 0) {
  return jsonResponse({
    investigation_id: INVESTIGATION_ID,
    replay_scope: "live",
    items: items.filter((event) => event.id > after),
    next_after: null,
  });
}

export interface SseStream {
  response: Response;
  send: (text: string) => void;
  close: () => void;
  fail: () => void;
}

/** A text/event-stream response the test writes to; an abort ends it as fetch would. */
export function sseStream(signal?: AbortSignal | null): SseStream {
  let controller!: ReadableStreamDefaultController<Uint8Array>;
  let open = true;
  const body = new ReadableStream<Uint8Array>({
    start(c) {
      controller = c;
    },
    cancel() {
      open = false;
    },
  });
  const end = (action: () => void) => {
    if (open) {
      open = false;
      action();
    }
  };
  signal?.addEventListener("abort", () => {
    end(() => {
      controller.error(new DOMException("aborted", "AbortError"));
    });
  });
  return {
    response: new Response(body, { headers: { "Content-Type": "text/event-stream" } }),
    send: (text) => {
      if (open) {
        controller.enqueue(new TextEncoder().encode(text));
      }
    },
    close: () => {
      end(() => {
        controller.close();
      });
    },
    fail: () => {
      end(() => {
        controller.error(new TypeError("network error"));
      });
    },
  };
}

export function sseFrame(event: ProgressEvent): string {
  return `id: ${String(event.id)}\nevent: progress\ndata: ${JSON.stringify(event)}\n\n`;
}
