import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams, useSearchParams } from "react-router";
import { ApiError } from "../api/client";
import { formatTime } from "../api/format";
import {
  fetchIncident,
  fetchIncidentEvidence,
  fetchServiceGraph,
  LIVE_SCOPE,
  updateIncidentStatus,
  type IncidentDetail,
  type IncidentEvidence,
  type IncidentStatus,
} from "../api/incidents";
import { DependencyGraph } from "../components/DependencyGraph";
import { ErrorState } from "../components/ErrorState";
import { SeverityBadge, StatusBadge } from "../components/IncidentBadges";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";

const REFRESH_MS = 15_000;
// The detail response holds at most this many evidence references.
const MAX_EVIDENCE_REFS = 200;

export function IncidentDetailPage() {
  return (
    <OperatorGate>
      <IncidentView />
    </OperatorGate>
  );
}

function IncidentView() {
  const { incidentId = "" } = useParams();
  const [params] = useSearchParams();
  const replayScope = params.get("replay_scope") ?? LIVE_SCOPE;

  const query = useQuery({
    queryKey: ["incident", incidentId, replayScope],
    queryFn: ({ signal }) => fetchIncident(incidentId, replayScope, signal),
    refetchInterval: REFRESH_MS,
  });

  if (query.isPending) {
    return <LoadingState label="Loading incident…" />;
  }
  const incident = query.data;
  if (incident === undefined) {
    const missing = query.error instanceof ApiError && query.error.status === 404;
    return (
      <section>
        <ErrorState
          title={missing ? "Incident not found" : "Cannot load incident"}
          error={query.error}
          onRetry={missing ? undefined : () => void query.refetch()}
        />
        <p>
          <Link to="/incidents">Back to incidents</Link>
        </p>
      </section>
    );
  }

  return (
    <section>
      <p>
        <Link to="/incidents">Incidents</Link>
      </p>
      <h1>{incident.title}</h1>
      {query.isRefetchError && <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />}
      <p className="badges">
        <SeverityBadge severity={incident.severity} />
        <StatusBadge status={incident.status} />
      </p>
      <dl className="facts">
        <dt>Detected</dt>
        <dd>{formatTime(incident.detected_at)}</dd>
        <dt>Seen</dt>
        <dd>
          {formatTime(incident.first_seen_at)} to {formatTime(incident.last_seen_at)}
        </dd>
        {incident.resolved_at && (
          <>
            <dt>Resolved</dt>
            <dd>{formatTime(incident.resolved_at)}</dd>
          </>
        )}
        <dt>Version</dt>
        <dd>{incident.version}</dd>
        {incident.replay_scope !== LIVE_SCOPE && (
          <>
            <dt>Replay scope</dt>
            <dd>{incident.replay_scope}</dd>
          </>
        )}
      </dl>
      <StatusControl incident={incident} />
      <DataQuality incident={incident} />
      <Services incident={incident} />
      <Neighborhood incident={incident} />
      <Triggers incident={incident} />
      <Timeline incident={incident} />
    </section>
  );
}

function StatusControl({ incident }: { incident: IncidentDetail }) {
  const queryClient = useQueryClient();
  const key = ["incident", incident.id, incident.replay_scope];
  const mutation = useMutation({
    mutationFn: (status: IncidentStatus) =>
      updateIncidentStatus(incident.id, status, incident.version),
    onSuccess: (updated) => {
      queryClient.setQueryData(key, updated);
      void queryClient.invalidateQueries({ queryKey: ["incidents"] });
    },
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: key });
    },
  });

  if (incident.replay_scope !== LIVE_SCOPE) {
    return <p className="hint">Replay incidents are read-only.</p>;
  }

  const next: IncidentStatus = incident.status === "open" ? "resolved" : "open";
  const conflict = mutation.error instanceof ApiError && mutation.error.status === 409;
  return (
    <>
      <p>
        <button
          type="button"
          disabled={mutation.isPending}
          onClick={() => {
            mutation.mutate(next);
          }}
        >
          {next === "resolved" ? "Resolve incident" : "Reopen incident"}
        </button>
      </p>
      {conflict && (
        <p className="state state-stale" role="alert">
          This incident changed while you were viewing it. The latest version is loaded; review it
          and try again.
        </p>
      )}
      {mutation.isError && !conflict && (
        <ErrorState title="Status not changed" error={mutation.error} />
      )}
    </>
  );
}

/** Counts of the incident's evidence references by quality status. */
function DataQuality({ incident }: { incident: IncidentDetail }) {
  const counts = new Map<string, number>();
  incident.evidence.forEach((ref) => {
    counts.set(ref.quality_status, (counts.get(ref.quality_status) ?? 0) + 1);
  });
  const flagged = incident.evidence.length - (counts.get("accepted") ?? 0);
  const capped = incident.evidence.length >= MAX_EVIDENCE_REFS;
  return (
    <>
      <h2>Data quality</h2>
      {incident.evidence.length === 0 ? (
        <p className="state" role="status">
          No cited signals yet, so there is nothing to assess.
        </p>
      ) : (
        <p className={flagged > 0 ? "state state-stale" : "hint"} role="status">
          {flagged === 0
            ? `All ${String(incident.evidence.length)} cited signals were accepted.`
            : `${String(flagged)} of ${String(incident.evidence.length)} cited signals are not fully accepted (${[
                ...counts,
              ]
                .filter(([status]) => status !== "accepted")
                .map(([status, count]) => `${String(count)} ${status}`)
                .join(", ")}). Weigh them accordingly; the timeline below lists their flags.`}
          {capped && ` Counts cover the first ${String(MAX_EVIDENCE_REFS)} references.`}
        </p>
      )}
    </>
  );
}

function Services({ incident }: { incident: IncidentDetail }) {
  const search = incident.replay_scope === LIVE_SCOPE ? "" : `&replay_scope=${incident.replay_scope}`;
  return (
    <>
      <h2>Services</h2>
      <div className="scroll-x">
        <table>
          <thead>
            <tr>
              <th scope="col">Service</th>
              <th scope="col">Environment</th>
              <th scope="col">Role</th>
            </tr>
          </thead>
          <tbody>
            {incident.services.map((service) => (
              <tr key={service.service_id}>
                <th scope="row">
                  <Link to={`/incidents?service_id=${service.service_id}${search}`}>
                    {service.name}
                  </Link>
                </th>
                <td>{service.environment}</td>
                <td>{service.role}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  );
}

function Neighborhood({ incident }: { incident: IncidentDetail }) {
  const [selected, setSelected] = useState<string>();
  const serviceId = selected ?? incident.services[0]?.service_id;
  const asOf = incident.last_seen_at;
  const query = useQuery({
    queryKey: ["service-graph", serviceId, incident.replay_scope, asOf],
    queryFn: ({ signal }) =>
      fetchServiceGraph(serviceId ?? "", incident.replay_scope, asOf, signal),
    enabled: serviceId !== undefined,
  });

  const graph = query.data;
  return (
    <>
      <h2>Dependency neighborhood</h2>
      <p className="hint">
        As of {formatTime(asOf)}. Observed calls show correlation, not cause. Highlighted services
        belong to this incident.
      </p>
      {incident.services.length > 1 && (
        <label className="filters">
          Centered on
          <select
            value={serviceId}
            onChange={(event) => {
              setSelected(event.target.value);
            }}
          >
            {incident.services.map((service) => (
              <option key={service.service_id} value={service.service_id}>
                {service.name}
              </option>
            ))}
          </select>
        </label>
      )}
      {query.isPending && serviceId !== undefined && (
        <LoadingState label="Loading dependency neighborhood…" />
      )}
      {query.isError && (
        <ErrorState
          title="Cannot load the dependency neighborhood"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {graph?.truncated && (
        <p className="state state-stale" role="status">
          This neighborhood reached its size bound, so more dependencies may exist.
        </p>
      )}
      {graph && graph.downstream.length + graph.upstream.length === 0 && (
        <p className="state" role="status">
          No dependencies were observed for this service as of this time.
        </p>
      )}
      {graph && graph.downstream.length + graph.upstream.length > 0 && (
        <DependencyGraph
          graph={graph}
          affected={new Set(incident.services.map((service) => service.service_id))}
        />
      )}
    </>
  );
}

function Triggers({ incident }: { incident: IncidentDetail }) {
  const names = new Map(incident.services.map((service) => [service.service_id, service.name]));
  if (incident.triggers.length === 0) {
    return null;
  }
  return (
    <>
      <h2>Rule firings</h2>
      <div className="scroll-x">
        <table>
          <thead>
            <tr>
              <th scope="col">Window</th>
              <th scope="col">Service</th>
              <th scope="col">Rule</th>
              <th scope="col">Observed</th>
              <th scope="col">Threshold</th>
              <th scope="col">Samples</th>
            </tr>
          </thead>
          <tbody>
            {incident.triggers.map((trigger) => (
              <tr key={trigger.trigger_id}>
                <td>{formatTime(trigger.window_start)}</td>
                <td>{names.get(trigger.service_id) ?? trigger.service_id}</td>
                <td>{trigger.rule_id}</td>
                <td>{trigger.observed.toFixed(2)}</td>
                <td>{trigger.threshold.toFixed(2)}</td>
                <td>{trigger.sample_count}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {incident.triggers_truncated && (
        <p className="hint">Showing the earliest {incident.triggers.length} rule firings.</p>
      )}
    </>
  );
}

function Timeline({ incident }: { incident: IncidentDetail }) {
  const names = new Map(incident.services.map((service) => [service.service_id, service.name]));
  const query = useInfiniteQuery({
    queryKey: ["incident-evidence", incident.id, incident.replay_scope],
    queryFn: ({ pageParam, signal }) =>
      fetchIncidentEvidence(incident.id, incident.replay_scope, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    refetchInterval: REFRESH_MS,
  });
  const items = query.data?.pages.flatMap((page) => page.items);

  return (
    <>
      <h2>Evidence timeline</h2>
      {query.isPending && <LoadingState label="Loading evidence…" />}
      {items === undefined && query.isError && (
        <ErrorState
          title="Cannot load evidence"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {items !== undefined && query.isRefetchError && (
        <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />
      )}
      {items?.length === 0 && (
        <p className="state" role="status">
          Insufficient evidence: no signals are cited by this incident yet.
        </p>
      )}
      {items !== undefined && items.length > 0 && (
        <ol className="timeline">
          {items.map((item) => (
            <EvidenceRow key={item.signal.id} item={item} names={names} />
          ))}
        </ol>
      )}
      {query.isFetchNextPageError && (
        <p className="state state-error" role="alert">
          Could not load more evidence.
        </p>
      )}
      {query.hasNextPage && (
        <p>
          <button
            type="button"
            disabled={query.isFetchingNextPage}
            onClick={() => void query.fetchNextPage()}
          >
            {query.isFetchingNextPage ? "Loading…" : "Load more"}
          </button>
        </p>
      )}
    </>
  );
}

interface EvidenceRowProps {
  item: IncidentEvidence;
  names: ReadonlyMap<string, string>;
}

function EvidenceRow({ item: { signal }, names }: EvidenceRowProps) {
  const service = signal.service_id === null ? "unknown service" : names.get(signal.service_id);
  return (
    <li>
      <p className="timeline-head">
        <time dateTime={signal.occurred_at}>{formatTime(signal.occurred_at)}</time>
        <span>{service ?? signal.service_id}</span>
        <span>{signal.kind}</span>
        <span>{signal.severity}</span>
        {signal.quality_status !== "accepted" && (
          <span className="badge badge-warn">{signal.quality_status}</span>
        )}
        {signal.quality_flags.map((flag) => (
          <span key={flag} className="flag">
            {flag}
          </span>
        ))}
      </p>
      <p>{signal.message ?? "No message"}</p>
      <details>
        <summary>Details</summary>
        {signal.trace_id && <p>Trace: {signal.trace_id}</p>}
        <pre>{JSON.stringify(signal.attributes, null, 2)}</pre>
      </details>
    </li>
  );
}
