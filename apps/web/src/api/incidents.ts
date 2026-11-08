import {
  isBoolean,
  isListOf,
  isNullableString,
  isNumber,
  isOneOf,
  isRecord,
  isString,
  matches,
  type Checks,
} from "./checks";
import { getJson, patchJson } from "./client";

export const INCIDENT_STATUSES = ["open", "resolved"] as const;
export type IncidentStatus = (typeof INCIDENT_STATUSES)[number];

export const INCIDENT_SEVERITIES = ["low", "medium", "high", "critical"] as const;
export type IncidentSeverity = (typeof INCIDENT_SEVERITIES)[number];

export const LIVE_SCOPE = "live";

export interface IncidentSummary {
  id: string;
  replay_scope: string;
  title: string;
  severity: IncidentSeverity;
  status: IncidentStatus;
  detected_at: string;
  first_seen_at: string;
  last_seen_at: string;
  resolved_at: string | null;
  version: number;
}

export interface IncidentListItem extends IncidentSummary {
  /** Primary services first. Older servers omit it, which reads as none. */
  services: IncidentService[];
}

export interface IncidentPage {
  items: IncidentListItem[];
  next_cursor: string | null;
}

export interface IncidentService {
  service_id: string;
  name: string;
  environment: string;
  role: "primary" | "affected";
}

export interface IncidentTrigger {
  trigger_id: string;
  service_id: string;
  rule_id: string;
  metric_name: string;
  window_start: string;
  window_end: string;
  observed: number;
  threshold: number;
  sample_count: number;
  error_count: number;
}

export interface EvidenceRef {
  signal_id: string;
  kind: string;
  occurred_at: string;
  severity: string;
  quality_status: string;
  relevance: number;
}

export interface IncidentDetail extends IncidentSummary {
  services: IncidentService[];
  /** Up to 200 references, oldest first. */
  evidence: EvidenceRef[];
  triggers: IncidentTrigger[];
  triggers_truncated: boolean;
}

export interface EvidenceSignal {
  id: string;
  service_id: string | null;
  kind: string;
  occurred_at: string;
  severity: string;
  message: string | null;
  trace_id: string | null;
  attributes: Record<string, unknown>;
  quality_status: string;
  quality_flags: string[];
}

export interface IncidentEvidence {
  signal: EvidenceSignal;
  relevance: number;
}

export interface EvidencePage {
  items: IncidentEvidence[];
  next_cursor: string | null;
}

export interface GraphEdge {
  from_service_id: string;
  to_service_id: string;
  relation: string;
  valid_from: string;
  hops: number;
}

export interface GraphDeployment {
  deployment_id: string;
  revision: string;
  deployed_at: string;
}

export interface GraphService {
  service_id: string;
  name: string;
  environment: string;
  deployments: GraphDeployment[];
}

export interface ServiceGraph {
  service_id: string;
  as_of: string;
  services: GraphService[];
  downstream: GraphEdge[];
  upstream: GraphEdge[];
  truncated: boolean;
}

export interface IncidentFilters {
  replayScope: string;
  status?: string;
  severity?: string;
  serviceId?: string;
}

const summaryChecks: Checks = {
  id: isString,
  replay_scope: isString,
  title: isString,
  severity: isOneOf(INCIDENT_SEVERITIES),
  status: isOneOf(INCIDENT_STATUSES),
  detected_at: isString,
  first_seen_at: isString,
  last_seen_at: isString,
  resolved_at: isNullableString,
  version: isNumber,
};

const isService = matches({
  service_id: isString,
  name: isString,
  environment: isString,
  role: isOneOf(["primary", "affected"]),
});

const isDetail = matches({
  ...summaryChecks,
  services: isListOf(isService),
  evidence: isListOf(
    matches({
      signal_id: isString,
      kind: isString,
      occurred_at: isString,
      severity: isString,
      quality_status: isString,
      relevance: isNumber,
    }),
  ),
  triggers: isListOf(
    matches({
      trigger_id: isString,
      service_id: isString,
      rule_id: isString,
      metric_name: isString,
      window_start: isString,
      window_end: isString,
      observed: isNumber,
      threshold: isNumber,
      sample_count: isNumber,
      error_count: isNumber,
    }),
  ),
  triggers_truncated: isBoolean,
});

const isPage = matches({
  items: isListOf(
    matches({
      ...summaryChecks,
      services: (value: unknown) => value === undefined || isListOf(isService)(value),
    }),
  ),
  next_cursor: isNullableString,
});

export const isEvidenceSignal = matches({
  id: isString,
  service_id: isNullableString,
  kind: isString,
  occurred_at: isString,
  severity: isString,
  message: isNullableString,
  trace_id: isNullableString,
  attributes: isRecord,
  quality_status: isString,
  quality_flags: isListOf(isString),
});

const isEvidencePage = matches({
  items: isListOf(
    matches({
      relevance: isNumber,
      signal: isEvidenceSignal,
    }),
  ),
  next_cursor: isNullableString,
});

const isEdge = matches({
  from_service_id: isString,
  to_service_id: isString,
  relation: isString,
  valid_from: isString,
  hops: isNumber,
});

const isGraph = matches({
  service_id: isString,
  as_of: isString,
  services: isListOf(
    matches({
      service_id: isString,
      name: isString,
      environment: isString,
      deployments: isListOf(
        matches({ deployment_id: isString, revision: isString, deployed_at: isString }),
      ),
    }),
  ),
  downstream: isListOf(isEdge),
  upstream: isListOf(isEdge),
  truncated: isBoolean,
});

const parseDetail = (value: unknown) => (isDetail(value) ? (value as IncidentDetail) : undefined);
const parsePage = (value: unknown): IncidentPage | undefined => {
  if (!isPage(value)) {
    return undefined;
  }
  const page = value as { items: (IncidentSummary & { services?: IncidentService[] })[] } & IncidentPage;
  return { ...page, items: page.items.map((item) => ({ ...item, services: item.services ?? [] })) };
};
const parseEvidencePage = (value: unknown) =>
  isEvidencePage(value) ? (value as EvidencePage) : undefined;
const parseGraph = (value: unknown) => (isGraph(value) ? (value as ServiceGraph) : undefined);

function query(params: Record<string, string | undefined>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value !== undefined && value !== "") {
      search.set(key, value);
    }
  }
  const text = search.toString();
  return text === "" ? "" : `?${text}`;
}

function scoped(replayScope: string): string | undefined {
  return replayScope === LIVE_SCOPE ? undefined : replayScope;
}

export function fetchIncidents(
  filters: IncidentFilters,
  cursor: string | undefined,
  signal?: AbortSignal,
): Promise<IncidentPage> {
  const search = query({
    replay_scope: scoped(filters.replayScope),
    status: filters.status,
    severity: filters.severity,
    service_id: filters.serviceId,
    cursor,
  });
  return getJson(`/v1/incidents${search}`, parsePage, { operator: true, signal });
}

export function fetchIncident(
  id: string,
  replayScope: string,
  signal?: AbortSignal,
): Promise<IncidentDetail> {
  const search = query({ replay_scope: scoped(replayScope) });
  return getJson(`/v1/incidents/${encodeURIComponent(id)}${search}`, parseDetail, {
    operator: true,
    signal,
  });
}

export function fetchIncidentEvidence(
  id: string,
  replayScope: string,
  cursor: string | undefined,
  signal?: AbortSignal,
): Promise<EvidencePage> {
  const search = query({ replay_scope: scoped(replayScope), cursor });
  return getJson(`/v1/incidents/${encodeURIComponent(id)}/evidence${search}`, parseEvidencePage, {
    operator: true,
    signal,
  });
}

export function fetchServiceGraph(
  serviceId: string,
  replayScope: string,
  asOf: string,
  signal?: AbortSignal,
): Promise<ServiceGraph> {
  const search = query({ replay_scope: scoped(replayScope), as_of: asOf });
  return getJson(`/v1/services/${encodeURIComponent(serviceId)}/graph${search}`, parseGraph, {
    operator: true,
    signal,
  });
}

export function updateIncidentStatus(
  id: string,
  status: IncidentStatus,
  version: number,
): Promise<IncidentDetail> {
  return patchJson(
    `/v1/incidents/${encodeURIComponent(id)}`,
    { status, version },
    parseDetail,
    { operator: true },
  );
}
