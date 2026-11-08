import { isListOf, isNumber, isOneOf, isRecord, isString, matches } from "./checks";
import { getJson } from "./client";
import { LIVE_SCOPE } from "./incidents";

export const PROGRESS_EVENT_TYPES = [
  "step.started",
  "step.completed",
  "step.failed",
  "investigation.ended",
] as const;
export type ProgressEventType = (typeof PROGRESS_EVENT_TYPES)[number];

export interface StepPayload {
  step_key: string;
  agent_role: string;
  attempt: number;
  error_category?: string;
}

export interface EndedPayload {
  status: string;
  diagnosis_status: string | null;
}

export interface ProgressEvent {
  /** The durable cursor, ascending in commit order within an investigation. */
  id: number;
  event_type: ProgressEventType;
  payload: StepPayload | EndedPayload;
  created_at: string;
}

export interface ProgressPage {
  investigation_id: string;
  items: ProgressEvent[];
  next_after: number | null;
}

const isStepPayload = matches({ step_key: isString, agent_role: isString, attempt: isNumber });
const isEndedPayload = matches({ status: isString });

export const isProgressEvent = (value: unknown): boolean =>
  matches({ id: isNumber, event_type: isOneOf(PROGRESS_EVENT_TYPES), created_at: isString })(
    value,
  ) &&
  isRecord(value) &&
  (value.event_type === "investigation.ended"
    ? isEndedPayload(value.payload)
    : isStepPayload(value.payload));

const isPage = matches({
  investigation_id: isString,
  items: isListOf(isProgressEvent),
  next_after: (value: unknown) => value === null || typeof value === "number",
});

const parsePage = (value: unknown) => (isPage(value) ? (value as ProgressPage) : undefined);

export function fetchProgress(
  investigationId: string,
  replayScope: string,
  after: number,
  signal?: AbortSignal,
): Promise<ProgressPage> {
  const search = new URLSearchParams({ after: String(after) });
  if (replayScope !== LIVE_SCOPE) {
    search.set("replay_scope", replayScope);
  }
  return getJson(
    `/v1/investigations/${encodeURIComponent(investigationId)}/progress?${search.toString()}`,
    parsePage,
    { operator: true, signal },
  );
}

export type StepState = "running" | "completed" | "failed";

export interface StepView {
  key: string;
  role: string;
  attempt: number;
  state: StepState;
  errorCategory: string | undefined;
}

/** Each step's latest attempt, in the order steps first started. */
export function stepsFrom(events: readonly ProgressEvent[]): StepView[] {
  const steps = new Map<string, StepView>();
  for (const event of events) {
    if (event.event_type === "investigation.ended") {
      continue;
    }
    const payload = event.payload as StepPayload;
    const known = steps.get(payload.step_key);
    if (known !== undefined && known.attempt > payload.attempt) {
      continue;
    }
    const state: StepState =
      event.event_type === "step.started"
        ? "running"
        : event.event_type === "step.completed"
          ? "completed"
          : "failed";
    steps.set(payload.step_key, {
      key: payload.step_key,
      role: payload.agent_role,
      attempt: payload.attempt,
      state,
      errorCategory: payload.error_category,
    });
  }
  return [...steps.values()];
}
