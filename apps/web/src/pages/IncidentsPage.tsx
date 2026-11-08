import { useInfiniteQuery } from "@tanstack/react-query";
import { Link, useSearchParams } from "react-router";
import { formatTime } from "../api/format";
import {
  fetchIncidents,
  INCIDENT_SEVERITIES,
  INCIDENT_STATUSES,
  LIVE_SCOPE,
  type IncidentFilters,
  type IncidentService,
} from "../api/incidents";
import { ErrorState } from "../components/ErrorState";
import { SeverityBadge, StatusBadge } from "../components/IncidentBadges";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";

const REFRESH_MS = 15_000;

export function IncidentsPage() {
  return (
    <OperatorGate>
      <IncidentList />
    </OperatorGate>
  );
}

function IncidentList() {
  const [params, setParams] = useSearchParams();
  const filters: IncidentFilters = {
    replayScope: params.get("replay_scope") ?? LIVE_SCOPE,
    status: params.get("status") ?? undefined,
    severity: params.get("severity") ?? undefined,
    serviceId: params.get("service_id") ?? undefined,
  };
  const replay = filters.replayScope !== LIVE_SCOPE;

  const query = useInfiniteQuery({
    queryKey: ["incidents", filters],
    queryFn: ({ pageParam, signal }) => fetchIncidents(filters, pageParam, signal),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (page) => page.next_cursor ?? undefined,
    refetchInterval: REFRESH_MS,
  });

  const setFilter = (name: string, value: string | undefined) => {
    setParams((current) => {
      const next = new URLSearchParams(current);
      if (value) {
        next.set(name, value);
      } else {
        next.delete(name);
      }
      return next;
    });
  };

  const items = query.data?.pages.flatMap((page) => page.items);
  const hasFilter = filters.status ?? filters.severity ?? filters.serviceId;
  const serviceName = items
    ?.flatMap((incident) => incident.services)
    .find((service) => service.service_id === filters.serviceId)?.name;

  return (
    <section>
      <h1>Incidents</h1>
      <form className="filters" aria-label="Incident filters">
        <label>
          Status
          <select
            value={filters.status ?? ""}
            onChange={(event) => {
              setFilter("status", event.target.value);
            }}
          >
            <option value="">Any</option>
            {INCIDENT_STATUSES.map((status) => (
              <option key={status}>{status}</option>
            ))}
          </select>
        </label>
        <label>
          Severity
          <select
            value={filters.severity ?? ""}
            onChange={(event) => {
              setFilter("severity", event.target.value);
            }}
          >
            <option value="">Any</option>
            {INCIDENT_SEVERITIES.map((severity) => (
              <option key={severity}>{severity}</option>
            ))}
          </select>
        </label>
        {filters.serviceId && (
          <p className="filter-chip">
            Service: {serviceName ?? "selected service"}{" "}
            <button
              type="button"
              className="link"
              onClick={() => {
                setFilter("service_id", undefined);
              }}
            >
              Clear service filter
            </button>
          </p>
        )}
      </form>
      {replay && <p className="hint">Replay scope: {filters.replayScope}</p>}

      {query.isPending && <LoadingState label="Loading incidents…" />}
      {items === undefined && query.isError && (
        <ErrorState
          title="Cannot load incidents"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {items !== undefined && query.isRefetchError && (
        <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />
      )}
      {items?.length === 0 && (
        <p className="state" role="status">
          {hasFilter
            ? "No incidents match these filters."
            : "No incidents yet. They appear here when a detection rule fires."}
        </p>
      )}
      {items !== undefined && items.length > 0 && (
        <div className="scroll-x">
          <table>
            <thead>
              <tr>
                <th scope="col">Incident</th>
                <th scope="col">Services</th>
                <th scope="col">Severity</th>
                <th scope="col">Status</th>
                <th scope="col">Detected</th>
              </tr>
            </thead>
            <tbody>
              {items.map((incident) => (
                <tr key={incident.id}>
                  <th scope="row">
                    <Link
                      to={{
                        pathname: `/incidents/${incident.id}`,
                        search: replay ? `?replay_scope=${filters.replayScope}` : "",
                      }}
                    >
                      {incident.title}
                    </Link>
                  </th>
                  <td>
                    <ServiceLinks
                      services={incident.services}
                      onSelect={(serviceId) => {
                        setFilter("service_id", serviceId);
                      }}
                    />
                  </td>
                  <td>
                    <SeverityBadge severity={incident.severity} />
                  </td>
                  <td>
                    <StatusBadge status={incident.status} />
                  </td>
                  <td>{formatTime(incident.detected_at)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {query.isFetchNextPageError && (
        <p className="state state-error" role="alert">
          Could not load more incidents.
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
    </section>
  );
}

interface ServiceLinksProps {
  services: IncidentService[];
  onSelect: (serviceId: string) => void;
}

/** Each service filters the list to its incidents; primary services are listed first. */
function ServiceLinks({ services, onSelect }: ServiceLinksProps) {
  if (services.length === 0) {
    return <span className="hint">None recorded</span>;
  }
  return (
    <ul className="inline-list">
      {services.map((service) => (
        <li key={service.service_id}>
          <button
            type="button"
            className="link"
            title={`Show incidents involving ${service.name}`}
            onClick={() => {
              onSelect(service.service_id);
            }}
          >
            {service.name}
          </button>
          {service.role === "affected" && <span className="hint"> (affected)</span>}
        </li>
      ))}
    </ul>
  );
}
