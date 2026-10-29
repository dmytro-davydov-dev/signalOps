import { getJson } from "./client";

export interface DependencyStatus {
  status: "ok" | "error";
  latency_ms: number;
  error: string | null;
}

export interface Readiness {
  status: "ready" | "not_ready";
  checks: Record<string, DependencyStatus>;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isDependencyStatus(value: unknown): value is DependencyStatus {
  return (
    isRecord(value) &&
    (value.status === "ok" || value.status === "error") &&
    typeof value.latency_ms === "number" &&
    (value.error === null || typeof value.error === "string")
  );
}

export function parseReadiness(value: unknown): Readiness | undefined {
  if (!isRecord(value) || (value.status !== "ready" && value.status !== "not_ready")) {
    return undefined;
  }
  const { checks } = value;
  if (!isRecord(checks) || !Object.values(checks).every(isDependencyStatus)) {
    return undefined;
  }
  return { status: value.status, checks: checks as Record<string, DependencyStatus> };
}

export function fetchReadiness(signal?: AbortSignal): Promise<Readiness> {
  // 503 carries the same body and means "not ready", not "request failed".
  return getJson("/health/ready", parseReadiness, { acceptStatuses: [503], signal });
}
