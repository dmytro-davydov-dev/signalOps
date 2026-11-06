import type { IncidentSeverity, IncidentStatus } from "../api/incidents";

export function SeverityBadge({ severity }: { severity: IncidentSeverity }) {
  const urgent = severity === "high" || severity === "critical";
  return <span className={urgent ? "badge badge-error" : "badge badge-warn"}>{severity}</span>;
}

export function StatusBadge({ status }: { status: IncidentStatus }) {
  return (
    <span className={status === "resolved" ? "badge badge-ok" : "badge badge-error"}>{status}</span>
  );
}
