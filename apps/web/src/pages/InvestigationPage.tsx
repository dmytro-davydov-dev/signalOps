import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { Link, useParams, useSearchParams } from "react-router";
import { ApiError } from "../api/client";
import { formatCount, formatMicroUsd, formatTime } from "../api/format";
import { LIVE_SCOPE, type EvidenceSignal } from "../api/incidents";
import {
  cancelInvestigation,
  fetchInvestigation,
  fetchInvestigationEvidence,
  type Diagnosis,
  type Investigation,
  type InvestigationUsage,
} from "../api/investigations";
import { describeInvestigationStatus } from "../api/investigationStatus";
import { stepsFrom, type StepView } from "../api/progress";
import { ErrorState } from "../components/ErrorState";
import { InvestigationStatusBadge } from "../components/InvestigationStatusBadge";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";
import type { StreamState } from "../api/progressStream";
import { useProgressHistory, useProgressStream } from "../hooks/useProgress";

// Progress events refresh the investigation as they arrive; this only catches
// changes no event announces, such as a reservation settling.
const REFRESH_MS = 15_000;

export function InvestigationPage() {
  return (
    <OperatorGate>
      <InvestigationView />
    </OperatorGate>
  );
}

function InvestigationView() {
  const { investigationId = "" } = useParams();
  const [params] = useSearchParams();
  const replayScope = params.get("replay_scope") ?? LIVE_SCOPE;

  const query = useQuery({
    queryKey: ["investigation", investigationId, replayScope],
    queryFn: ({ signal }) => fetchInvestigation(investigationId, replayScope, signal),
    // A terminal status is final, so refreshing stops there.
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
  const incidentSearch =
    investigation.replay_scope === LIVE_SCOPE ? "" : `?replay_scope=${investigation.replay_scope}`;
  return (
    <section>
      <p>
        <Link to={`/incidents/${investigation.incident_id}${incidentSearch}`}>Incident</Link>
      </p>
      <h1>Investigation</h1>
      {query.isRefetchError && <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />}
      <p className="badges">
        <InvestigationStatusBadge status={investigation.status} />
      </p>
      <Outcome investigation={investigation} />
      <dl className="facts">
        <dt>Requested</dt>
        <dd>{formatTime(investigation.created_at)}</dd>
        <dt>Evidence cutoff</dt>
        <dd>{formatTime(investigation.evidence_cutoff_at)}</dd>
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
        <dt>Workflow</dt>
        <dd>
          {investigation.workflow_version}, at most {investigation.max_steps} steps
        </dd>
      </dl>
      {!status.terminal && <CancelControl investigation={investigation} />}
      <Progress investigation={investigation} />
      {investigation.diagnosis && (
        <DiagnosisDetails investigation={investigation} diagnosis={investigation.diagnosis} />
      )}
      <Usage usage={investigation.usage} />
    </section>
  );
}

function Outcome({ investigation }: { investigation: Investigation }) {
  const { diagnosis } = investigation;
  switch (investigation.status) {
    case "queued":
      return <p className="hint">Waiting for a worker to pick this investigation up.</p>;
    case "running":
      return <p className="hint">A worker is investigating. Progress appears below.</p>;
    case "completed":
      return diagnosis && <p className="lead">{diagnosis.summary}</p>;
    case "insufficient_evidence":
      return (
        <div className="state state-abstained" role="status">
          <p>
            <strong>Insufficient evidence.</strong> The investigator did not find enough support
            for a root cause, so it did not name one. This is a valid outcome, not a failure.
          </p>
          {diagnosis && <p>{diagnosis.summary}</p>}
          {diagnosis && diagnosis.missing_evidence.length > 0 && (
            <>
              <p>Evidence that would have been needed:</p>
              <ul>
                {diagnosis.missing_evidence.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </>
          )}
        </div>
      );
    case "failed":
      return (
        <p className="state state-error" role="status">
          The investigation failed before reaching a conclusion, for example after repeated
          provider errors or a passed deadline. No diagnosis was stored.
        </p>
      );
    case "budget_exceeded":
      return (
        <p className="state state-error" role="status">
          The investigation stopped when its budget ran out. No diagnosis was stored.
        </p>
      );
    case "cancelled":
      return (
        <p className="state" role="status">
          The investigation was cancelled. No diagnosis was stored.
        </p>
      );
  }
}

function CancelControl({ investigation }: { investigation: Investigation }) {
  const queryClient = useQueryClient();
  const key = ["investigation", investigation.id, investigation.replay_scope];
  const mutation = useMutation({
    mutationFn: () => cancelInvestigation(investigation.id),
    onSuccess: (updated) => {
      queryClient.setQueryData(key, updated);
      void queryClient.invalidateQueries({ queryKey: ["incident-investigations"] });
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

const STEP_LABELS: Record<StepView["state"], string> = {
  running: "Running",
  completed: "Completed",
  failed: "Failed",
};

const STREAM_NOTICES: Partial<Record<StreamState, { text: string; className: string }>> = {
  connecting: { text: "Connecting to live updates…", className: "hint" },
  live: { text: "Live", className: "badge badge-ok" },
  reconnecting: {
    text: "Live updates interrupted. Reconnecting; missed steps will be replayed.",
    className: "state state-stale",
  },
  failed: {
    text: "Live updates stopped. Reload the page to try again.",
    className: "state state-error",
  },
};

function Progress({ investigation }: { investigation: Investigation }) {
  const terminal = describeInvestigationStatus(investigation.status).terminal;
  const query = useProgressHistory(investigation.id, investigation.replay_scope);
  // The stream starts after the history, so it only adds what is new.
  const stream = useProgressStream(
    investigation.id,
    investigation.replay_scope,
    !terminal && query.isSuccess,
  );
  const steps = query.data === undefined ? undefined : stepsFrom(query.data);
  const notice = stream === undefined ? undefined : STREAM_NOTICES[stream];

  return (
    <>
      <h2>Workflow progress</h2>
      {notice && (
        <p role="status" aria-label="Live updates">
          <span className={notice.className}>{notice.text}</span>
        </p>
      )}
      {query.isPending && <LoadingState label="Loading progress…" />}
      {steps === undefined && query.isError && (
        <ErrorState
          title="Cannot load progress"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {steps !== undefined && query.isRefetchError && (
        <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />
      )}
      {steps?.length === 0 && (
        <p className="state" role="status">
          {terminal ? "No workflow steps ran." : "No steps have started yet."}
        </p>
      )}
      {steps !== undefined && steps.length > 0 && (
        <ol className="steps" aria-label="Workflow steps">
          {steps.map((step) => (
            <li key={step.key} className={`step step-${step.state}`}>
              <span className="step-name">{step.key}</span>
              <span>{STEP_LABELS[step.state]}</span>
              {step.attempt > 1 && <span className="hint">attempt {step.attempt}</span>}
              {step.errorCategory && <span className="hint">{step.errorCategory}</span>}
            </li>
          ))}
        </ol>
      )}
    </>
  );
}

interface DiagnosisDetailsProps {
  investigation: Investigation;
  diagnosis: Diagnosis;
}

function DiagnosisDetails({ investigation, diagnosis }: DiagnosisDetailsProps) {
  const cause = diagnosis.root_cause;
  return (
    <>
      {cause && (
        <>
          <h2>Root cause hypothesis</h2>
          <dl className="facts">
            <dt>Service</dt>
            <dd>{cause.service_name}</dd>
            <dt>Failure type</dt>
            <dd>{cause.failure_type.replaceAll("_", " ")}</dd>
            <dt>Confidence</dt>
            <dd>
              {Math.round(cause.confidence * 100)}%{" "}
              <span className="hint">(the model&apos;s own estimate, not calibrated)</span>
            </dd>
          </dl>
          <CitedEvidence investigation={investigation} cited={cause.evidence_signal_ids.length} />
        </>
      )}
      {diagnosis.alternatives.length > 0 && (
        <>
          <h2>Other hypotheses considered</h2>
          <ul>
            {diagnosis.alternatives.map((alternative) => (
              <li key={alternative.hypothesis}>
                <strong>{alternative.hypothesis}:</strong> {alternative.assessment}
              </li>
            ))}
          </ul>
        </>
      )}
      {diagnosis.suggested_actions.length > 0 && (
        <>
          <h2>Suggested actions</h2>
          <p className="hint">SignalOps never acts on its own. Each action needs a person to apply it.</p>
          <ul>
            {diagnosis.suggested_actions.map((suggestion) => (
              <li key={suggestion.action}>{suggestion.action}</li>
            ))}
          </ul>
        </>
      )}
      <h2>Limitations</h2>
      {diagnosis.limitations.length === 0 ? (
        <p className="hint">The investigator recorded no limitations.</p>
      ) : (
        <ul>
          {diagnosis.limitations.map((limitation) => (
            <li key={limitation}>{limitation}</li>
          ))}
        </ul>
      )}
    </>
  );
}

function CitedEvidence({ investigation, cited }: { investigation: Investigation; cited: number }) {
  const query = useQuery({
    queryKey: ["investigation-evidence", investigation.id, investigation.replay_scope],
    queryFn: ({ signal }) =>
      fetchInvestigationEvidence(investigation.id, investigation.replay_scope, signal),
  });
  const evidence = query.data;
  return (
    <>
      <h3>Cited evidence</h3>
      <p className="hint">
        Each citation is checked again against this workspace, replay scope, and the evidence
        cutoff of {formatTime(investigation.evidence_cutoff_at)}.
      </p>
      {query.isPending && <LoadingState label="Loading cited evidence…" />}
      {query.isError && (
        <ErrorState
          title="Cannot load cited evidence"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {evidence && evidence.unverified_signal_ids.length > 0 && (
        <p className="state state-error" role="alert">
          {evidence.unverified_signal_ids.length} of {cited} citations could not be verified in
          scope by the cutoff. Treat this diagnosis with caution.
        </p>
      )}
      {evidence && evidence.items.length > 0 && (
        <ol className="timeline" aria-label="Cited evidence">
          {evidence.items.map((signal) => (
            <CitedSignal key={signal.id} signal={signal} />
          ))}
        </ol>
      )}
    </>
  );
}

function CitedSignal({ signal }: { signal: EvidenceSignal }) {
  return (
    <li id={`signal-${signal.id}`}>
      <p className="timeline-head">
        <time dateTime={signal.occurred_at}>{formatTime(signal.occurred_at)}</time>
        <span className="badge badge-ok">verified</span>
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
        <p>Signal: {signal.id}</p>
        {signal.trace_id && <p>Trace: {signal.trace_id}</p>}
        <pre>{JSON.stringify(signal.attributes, null, 2)}</pre>
      </details>
    </li>
  );
}

function Usage({ usage }: { usage: InvestigationUsage }) {
  const tokens = usage.input_tokens + usage.output_tokens;
  return (
    <>
      <h2>Tokens and cost</h2>
      <dl className="facts">
        <dt>Budget</dt>
        <dd>{formatMicroUsd(usage.budget_microusd)}</dd>
        <dt>Spent</dt>
        <dd>
          {formatMicroUsd(usage.spent_microusd)}
          {usage.spent_microusd > 0 && (
            <span className="hint">
              {" "}
              ({formatMicroUsd(usage.actual_microusd)} actual from provider usage,{" "}
              {formatMicroUsd(usage.estimated_microusd)} estimated)
            </span>
          )}
        </dd>
        <dt>Reserved now</dt>
        <dd>{formatMicroUsd(usage.reserved_microusd)}</dd>
        <dt>Unknown cost</dt>
        <dd>
          {usage.unknown_calls === 0 ? (
            "None"
          ) : (
            <span className="badge-warn">
              {usage.unknown_calls} {usage.unknown_calls === 1 ? "call" : "calls"} with unknown
              cost, holding {formatMicroUsd(usage.unknown_reserved_microusd)}. The provider may
              have billed them.
            </span>
          )}
        </dd>
        <dt>Model calls</dt>
        <dd>{formatCount(usage.model_calls)}</dd>
        <dt>Tokens</dt>
        <dd>
          {formatCount(tokens)}{" "}
          <span className="hint">
            ({formatCount(usage.input_tokens)} in, {formatCount(usage.output_tokens)} out
            {usage.cache_read_tokens + usage.cache_write_tokens > 0 &&
              `, ${formatCount(usage.cache_read_tokens)} cache read, ${formatCount(usage.cache_write_tokens)} cache write`}
            )
          </span>
        </dd>
      </dl>
    </>
  );
}
