import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router";
import { ApiError } from "../api/client";
import { formatTime } from "../api/format";
import { LIVE_SCOPE } from "../api/incidents";
import {
  cancelInvestigation,
  fetchInvestigation,
  type Investigation,
} from "../api/investigations";
import { describeInvestigationStatus, type StatusTone } from "../api/investigationStatus";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";

// Polling stands in for the progress stream until P08-T03.
const REFRESH_MS = 3_000;

const BADGE_BY_TONE: Record<StatusTone, string> = {
  pending: "badge badge-warn",
  success: "badge badge-ok",
  abstained: "badge badge-warn",
  failure: "badge badge-error",
  neutral: "badge",
};

export function InvestigationPage() {
  return (
    <OperatorGate>
      <InvestigationView />
    </OperatorGate>
  );
}

function CancelControl({ investigation }: { investigation: Investigation }) {
  const queryClient = useQueryClient();
  const key = ["investigation", investigation.id, investigation.replay_scope];
  const mutation = useMutation({
    mutationFn: () => cancelInvestigation(investigation.id),
    onSuccess: (updated) => {
      queryClient.setQueryData(key, updated);
    },
    onError: () => {
      void queryClient.invalidateQueries({ queryKey: key });
    },
  });

  if (investigation.replay_scope !== LIVE_SCOPE) {
    return <p className="hint">Replay investigations are read-only.</p>;
  }
  if (investigation.cancel_requested_at !== null) {
    return (
      <p className="state" role="status">
        Cancellation requested. The status changes once the worker stops.
      </p>
    );
  }

  const conflict = mutation.error instanceof ApiError && mutation.error.status === 409;
  return (
    <>
      <p>
        <button
          type="button"
          disabled={mutation.isPending}
          onClick={() => {
            mutation.mutate();
          }}
        >
          Cancel investigation
        </button>
      </p>
      {mutation.isError && (
        <p className="state state-error" role="alert">
          {conflict
            ? "This investigation already ended, so it cannot be cancelled."
            : "Cancelling failed. Try again."}
        </p>
      )}
    </>
  );
}

function InvestigationView() {
  const { investigationId = "" } = useParams();
  const [params] = useSearchParams();
  const replayScope = params.get("replay_scope") ?? LIVE_SCOPE;

  const query = useQuery({
    queryKey: ["investigation", investigationId, replayScope],
    queryFn: ({ signal }) => fetchInvestigation(investigationId, replayScope, signal),
    // A terminal status is final, so polling stops there.
    refetchInterval: ({ state }) =>
      state.data !== undefined && describeInvestigationStatus(state.data.status).terminal
        ? false
        : REFRESH_MS,
  });

  if (query.isPending) {
    return <LoadingState label="Loading investigation…" />;
  }
  const investigation = query.data;
  if (investigation === undefined) {
    const missing = query.error instanceof ApiError && query.error.status === 404;
    return (
      <section>
        <ErrorState
          title={missing ? "Investigation not found" : "Cannot load investigation"}
          error={query.error}
          onRetry={missing ? undefined : () => void query.refetch()}
        />
        <p>
          <Link to="/incidents">Back to incidents</Link>
        </p>
      </section>
    );
  }

  const status = describeInvestigationStatus(investigation.status);
  return (
    <section>
      <p>
        <Link to={`/incidents/${investigation.incident_id}`}>Incident</Link>
      </p>
      <h1>Investigation</h1>
      {query.isRefetchError && <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />}
      <p className="badges">
        <span className={BADGE_BY_TONE[status.tone]}>{status.label}</span>
      </p>
      {investigation.diagnosis && <p>{investigation.diagnosis.summary}</p>}
      <dl className="facts">
        <dt>Requested</dt>
        <dd>{formatTime(investigation.created_at)}</dd>
        <dt>Deadline</dt>
        <dd>{formatTime(investigation.deadline_at)}</dd>
        {investigation.cancel_requested_at && (
          <>
            <dt>Cancellation requested</dt>
            <dd>{formatTime(investigation.cancel_requested_at)}</dd>
          </>
        )}
        {investigation.completed_at && (
          <>
            <dt>Ended</dt>
            <dd>{formatTime(investigation.completed_at)}</dd>
          </>
        )}
      </dl>
      {!status.terminal && <CancelControl investigation={investigation} />}
    </section>
  );
}
