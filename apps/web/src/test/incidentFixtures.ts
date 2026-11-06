import type { IncidentDetail, IncidentSummary, ServiceGraph } from "../api/incidents";
import { jsonResponse } from "./renderRoute";

export const CHECKOUT = "dc2bf08d-6c3c-44ea-855a-d7d42eeefb36";
export const PAYMENT = "57988918-06d0-4d3a-8d0e-111111111111";
export const GATEWAY = "5f3e2d1c-0b9a-4876-9543-210fedcba987";
export const INCIDENT_ID = "6a7b8c9d-0e1f-4a2b-9c3d-4e5f6a7b8c9d";

export const summary: IncidentSummary = {
  id: INCIDENT_ID,
  replay_scope: "live",
  title: "High error rate on checkout (demo)",
  severity: "high",
  status: "open",
  detected_at: "2026-11-06T17:02:34Z",
  first_seen_at: "2026-11-06T17:01:00Z",
  last_seen_at: "2026-11-06T17:03:00Z",
  resolved_at: null,
  version: 3,
};

export const detail: IncidentDetail = {
  ...summary,
  services: [
    { service_id: CHECKOUT, name: "checkout", environment: "demo", role: "primary" },
    { service_id: PAYMENT, name: "payment", environment: "demo", role: "primary" },
  ],
  evidence: [],
  triggers: [
    {
      trigger_id: "11111111-1111-4111-8111-111111111111",
      service_id: CHECKOUT,
      rule_id: "trace_error_rate",
      metric_name: "trace.60s",
      window_start: "2026-11-06T17:01:00Z",
      window_end: "2026-11-06T17:02:00Z",
      observed: 0.61,
      threshold: 0.2,
      sample_count: 40,
      error_count: 24,
    },
  ],
  triggers_truncated: false,
};

export const evidencePage = {
  incident_id: INCIDENT_ID,
  replay_scope: "live",
  items: [
    {
      relevance: 1,
      signal: {
        id: "22222222-2222-4222-8222-222222222222",
        service_id: CHECKOUT,
        kind: "trace",
        occurred_at: "2026-11-06T17:01:05Z",
        severity: "error",
        message: "POST /charge timed out",
        trace_id: "trace-1",
        attributes: { token: "[REDACTED]" },
        quality_status: "degraded",
        quality_flags: ["clock_skew"],
      },
    },
  ],
  next_cursor: null,
};

export const graph: ServiceGraph = {
  service_id: CHECKOUT,
  as_of: summary.last_seen_at,
  services: [
    { service_id: GATEWAY, name: "bank-gateway", environment: "demo", deployments: [] },
    { service_id: CHECKOUT, name: "checkout", environment: "demo", deployments: [] },
    { service_id: PAYMENT, name: "payment", environment: "demo", deployments: [] },
  ],
  downstream: [
    {
      from_service_id: CHECKOUT,
      to_service_id: PAYMENT,
      relation: "DEPENDS_ON",
      valid_from: "2026-11-06T16:29:00Z",
      hops: 1,
    },
    {
      from_service_id: PAYMENT,
      to_service_id: GATEWAY,
      relation: "DEPENDS_ON",
      valid_from: "2026-11-06T16:29:00Z",
      hops: 2,
    },
  ],
  upstream: [],
  truncated: false,
};

export type Route = (url: URL, init: RequestInit | undefined) => Response | Promise<Response>;

/** A fetch stub answering by path; an unmatched request fails the test loudly. */
export function fetchByPath(routes: Record<string, Route>) {
  return (input: string, init?: RequestInit): Promise<Response> => {
    const url = new URL(input, "http://signalops.test");
    const route = routes[url.pathname];
    if (route === undefined) {
      throw new Error(`Unexpected request: ${url.pathname}`);
    }
    return Promise.resolve(route(url, init));
  };
}

export const incidentRoutes = (overrides: Record<string, Route> = {}): Record<string, Route> => ({
  [`/v1/incidents/${INCIDENT_ID}`]: () => jsonResponse(detail),
  [`/v1/incidents/${INCIDENT_ID}/evidence`]: () => jsonResponse(evidencePage),
  [`/v1/services/${CHECKOUT}/graph`]: () => jsonResponse(graph),
  [`/v1/services/${PAYMENT}/graph`]: () =>
    jsonResponse({ ...graph, service_id: PAYMENT, downstream: [], upstream: [] }),
  ...overrides,
});
