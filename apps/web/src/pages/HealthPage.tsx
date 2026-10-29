import { useQuery } from "@tanstack/react-query";
import { fetchReadiness } from "../api/health";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";

const REFRESH_MS = 10_000;

export function HealthPage() {
  const query = useQuery({
    queryKey: ["health", "ready"],
    queryFn: ({ signal }) => fetchReadiness(signal),
    refetchInterval: REFRESH_MS,
  });

  if (query.isPending) {
    return <LoadingState label="Checking dependencies…" />;
  }

  const readiness = query.data;
  if (readiness === undefined) {
    return (
      <ErrorState
        title="Cannot load system health"
        error={query.error}
        onRetry={() => void query.refetch()}
      />
    );
  }

  // A failed refetch keeps the previous data; show it as stale rather than hiding it.
  const stale = query.isError;
  const ready = readiness.status === "ready";
  const checkedAt = new Date(query.dataUpdatedAt).toLocaleTimeString();

  return (
    <section>
      <h1>System health</h1>
      <p className={ready ? "badge badge-ok" : "badge badge-error"}>
        {ready ? "All dependencies ready" : "Not ready"}
      </p>
      {stale && (
        <p className="state state-stale" role="status">
          Last refresh failed. Showing results from {checkedAt}.
        </p>
      )}
      <table>
        <caption>Checked at {checkedAt}</caption>
        <thead>
          <tr>
            <th scope="col">Dependency</th>
            <th scope="col">Status</th>
            <th scope="col">Latency</th>
            <th scope="col">Error</th>
          </tr>
        </thead>
        <tbody>
          {Object.entries(readiness.checks).map(([name, check]) => (
            <tr key={name}>
              <th scope="row">{name}</th>
              <td className={check.status === "ok" ? "ok" : "error"}>{check.status}</td>
              <td>{check.latency_ms.toFixed(1)} ms</td>
              <td>{check.error ?? "—"}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
