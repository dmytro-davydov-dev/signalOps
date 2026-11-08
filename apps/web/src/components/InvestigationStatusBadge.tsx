import type { InvestigationStatus } from "../api/generated/investigationStatus";
import { describeInvestigationStatus, type StatusTone } from "../api/investigationStatus";

const BADGE_BY_TONE: Record<StatusTone, string> = {
  pending: "badge badge-warn",
  success: "badge badge-ok",
  abstained: "badge badge-abstained",
  failure: "badge badge-error",
  neutral: "badge",
};

export function InvestigationStatusBadge({ status }: { status: InvestigationStatus }) {
  const view = describeInvestigationStatus(status);
  return <span className={BADGE_BY_TONE[view.tone]}>{view.label}</span>;
}
