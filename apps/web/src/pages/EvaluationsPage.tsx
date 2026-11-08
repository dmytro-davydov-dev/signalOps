import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState, type SyntheticEvent } from "react";
import { Link } from "react-router";
import { postJson } from "../api/client";
import { describeError } from "../api/describeError";
import { fetchEvaluationRuns, type EvaluationRunSummary } from "../api/evaluations";
import { formatTime } from "../api/format";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";

const REFRESH_MS = 10_000;

export function EvaluationsPage() {
  return (
    <OperatorGate>
      <EvaluationList />
    </OperatorGate>
  );
}

function EvaluationList() {
  const query = useQuery({
    queryKey: ["evaluation-runs"],
    queryFn: ({ signal }) => fetchEvaluationRuns(signal),
    refetchInterval: REFRESH_MS,
  });
  const runs = query.data;

  return (
    <section>
      <h1>Evaluation runs</h1>
      <p className="hint">
        Each run replays synthetic scenarios and compares a deterministic rule, a single agent, and
        the multi-agent workflow under the same evidence, tools, and budget.
      </p>
      <StartRunForm />
      {query.isPending && <LoadingState label="Loading evaluation runs…" />}
      {runs === undefined && query.isError && (
        <ErrorState
          title="Cannot load evaluation runs"
          error={query.error}
          onRetry={() => void query.refetch()}
        />
      )}
      {runs !== undefined && query.isRefetchError && (
        <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />
      )}
      {runs?.length === 0 && (
        <p className="state" role="status">
          No evaluation runs yet. Start one above, or with <code>signalops evaluation-run</code>.
        </p>
      )}
      {runs !== undefined && runs.length > 0 && <RunTable runs={runs} />}
    </section>
  );
}

function RunTable({ runs }: { runs: EvaluationRunSummary[] }) {
  return (
    <div className="scroll-x">
      <table>
        <thead>
          <tr>
            <th scope="col">Run</th>
            <th scope="col">Split</th>
            <th scope="col">Status</th>
            <th scope="col">Cases</th>
            <th scope="col">Created</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.id}>
              <th scope="row">
                <Link to={`/evaluations/${run.id}`}>{run.label}</Link>
              </th>
              <td>{run.split}</td>
              <td>
                <span className={`badge badge-${run.status}`}>{run.status}</span>
              </td>
              <td>
                {Object.entries(run.case_counts)
                  .map(([status, count]) => `${String(count)} ${status}`)
                  .join(", ") || "none yet"}
              </td>
              <td>{formatTime(run.created_at)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

function StartRunForm() {
  const queryClient = useQueryClient();
  const [label, setLabel] = useState("");
  const [split, setSplit] = useState("held_out");
  const [trials, setTrials] = useState(3);
  const start = useMutation({
    mutationFn: () =>
      postJson(
        "/v1/evaluation-runs",
        { label: label.trim(), split, trials },
        (value) => value,
        { operator: true },
      ),
    onSuccess: () => {
      setLabel("");
      void queryClient.invalidateQueries({ queryKey: ["evaluation-runs"] });
    },
  });
  const submit = (event: SyntheticEvent) => {
    event.preventDefault();
    if (label.trim()) start.mutate();
  };

  return (
    <form className="filters" aria-label="Start an evaluation run" onSubmit={submit}>
      <label>
        Label
        <input
          value={label}
          maxLength={256}
          onChange={(event) => {
            setLabel(event.target.value);
          }}
        />
      </label>
      <label>
        Split
        <select
          value={split}
          onChange={(event) => {
            setSplit(event.target.value);
          }}
        >
          <option value="held_out">held_out (reporting)</option>
          <option value="development">development</option>
        </select>
      </label>
      <label>
        Trials per agent
        <input
          type="number"
          min={1}
          max={20}
          value={trials}
          onChange={(event) => {
            setTrials(Number(event.target.value));
          }}
        />
      </label>
      <button type="submit" disabled={start.isPending || !label.trim()}>
        {start.isPending ? "Queueing…" : "Queue run"}
      </button>
      {start.isError && (
        <p className="state state-error" role="alert">
          {describeError(start.error).message}
        </p>
      )}
      {start.isSuccess && (
        <p className="state" role="status">
          Queued. An evaluation worker executes it; this list refreshes on its own.
        </p>
      )}
    </form>
  );
}
