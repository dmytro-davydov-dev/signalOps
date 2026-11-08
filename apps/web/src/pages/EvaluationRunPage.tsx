import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router";
import {
  fetchEvaluationRun,
  metric,
  VARIANT_LABELS,
  type EvaluationCaseRow,
  type EvaluationRunDetail,
} from "../api/evaluations";
import { formatMicroUsd, formatTime } from "../api/format";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";
import { StaleNotice } from "../components/StaleNotice";

const REFRESH_MS = 10_000;

export function EvaluationRunPage() {
  return (
    <OperatorGate>
      <RunView />
    </OperatorGate>
  );
}

function RunView() {
  const { runId = "" } = useParams();
  const query = useQuery({
    queryKey: ["evaluation-run", runId],
    queryFn: ({ signal }) => fetchEvaluationRun(runId, signal),
    refetchInterval: (current) =>
      current.state.data?.status === "completed" || current.state.data?.status === "failed"
        ? false
        : REFRESH_MS,
  });
  const run = query.data;

  if (query.isPending) return <LoadingState label="Loading evaluation run…" />;
  if (run === undefined) {
    return (
      <ErrorState
        title="Cannot load this evaluation run"
        error={query.error}
        onRetry={() => void query.refetch()}
      />
    );
  }
  return (
    <section>
      <p>
        <Link to="/evaluations">All evaluation runs</Link>
      </p>
      <h1>{run.label}</h1>
      {query.isRefetchError && <StaleNotice error={query.error} updatedAt={query.dataUpdatedAt} />}
      <RunFacts run={run} />
      {run.model_settings.measures_model_quality === false && (
        <p className="state state-stale" role="note">
          These cases ran on the fake provider, which follows fixed rules. They show that the
          harness works, not how well a model diagnoses.
        </p>
      )}
      {run.error && (
        <p className="state state-error" role="alert">
          The run failed: {run.error}
        </p>
      )}
      {run.metrics === null ? (
        <p className="state" role="status">
          {run.status === "failed"
            ? "No results were stored."
            : "Results appear when the run completes."}
        </p>
      ) : (
        <>
          <Comparison run={run} />
          <Detection metrics={run.metrics} />
        </>
      )}
      <Cases run={run} />
    </section>
  );
}

function RunFacts({ run }: { run: EvaluationRunDetail }) {
  return (
    <dl className="facts">
      <dt>Status</dt>
      <dd>
        <span className={`badge badge-${run.status}`}>{run.status}</span>
      </dd>
      <dt>Split</dt>
      <dd>{run.split === "held_out" ? "held out (reporting only)" : run.split}</dd>
      <dt>Data</dt>
      <dd>Synthetic replays · dataset {run.dataset_hash.slice(0, 12)}</dd>
      <dt>Model</dt>
      <dd>
        {String(run.model_settings.provider)} / {String(run.model_settings.model)}
      </dd>
      <dt>Budget per case</dt>
      <dd>
        {formatMicroUsd(Number(run.limits.budget_microusd))}, {String(run.limits.max_steps)} steps,{" "}
        {String(run.limits.deadline_seconds)} s — the same for every variant
      </dd>
      <dt>Created</dt>
      <dd>{formatTime(run.created_at)}</dd>
    </dl>
  );
}

const percent = (value: number | undefined) =>
  value === undefined ? "—" : `${String(Math.round(value * 1000) / 10)}%`;
const seconds = (value: number | undefined) => (value === undefined ? "—" : `${value.toFixed(1)} s`);

function Comparison({ run }: { run: EvaluationRunDetail }) {
  const variants = run.metrics?.variants;
  return (
    <>
      <h2>Comparison</h2>
      <div className="scroll-x">
        <table>
          <thead>
            <tr>
              <th scope="col">Variant</th>
              <th scope="col">Samples</th>
              <th scope="col">Outcome accuracy (± sd)</th>
              <th scope="col">Root cause accuracy</th>
              <th scope="col">Evidence precision</th>
              <th scope="col">Unsupported diagnoses</th>
              <th scope="col">Abstentions (correct / chances)</th>
              <th scope="col">Latency p50 / p95</th>
              <th scope="col">Mean cost</th>
              <th scope="col">Harness failures</th>
            </tr>
          </thead>
          <tbody>
            {run.variants.map((variant) => {
              const at = (...path: string[]) => metric(variants, variant, ...path);
              const sd = at("accuracy_stdev");
              const cost = at("cost_microusd_mean");
              return (
                <tr key={variant}>
                  <th scope="row">{VARIANT_LABELS[variant] ?? variant}</th>
                  <td>
                    {at("samples") ?? 0} ({at("trials") ?? 0} trials)
                  </td>
                  <td>
                    {percent(at("outcome_accuracy"))}
                    {sd !== undefined && ` ± ${percent(sd)}`}
                  </td>
                  <td>{percent(at("root_cause_accuracy"))}</td>
                  <td>{percent(at("evidence", "precision"))}</td>
                  <td>
                    {at("unsupported_diagnoses") ?? 0} ({percent(at("unsupported_rate"))})
                  </td>
                  <td>
                    {at("abstentions") ?? 0} ({at("correct_abstentions") ?? 0} /{" "}
                    {at("abstention_opportunities") ?? 0})
                  </td>
                  <td>
                    {seconds(at("latency_seconds_p50"))} / {seconds(at("latency_seconds_p95"))}
                  </td>
                  <td>{cost === undefined ? "—" : formatMicroUsd(cost)}</td>
                  <td>{at("harness_failures") ?? 0}</td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      <p className="hint">
        Evidence precision uses the scenarios' labels of which records the fault produced. Human
        audits are counted separately on each case. No score comes from a judge model.
      </p>
    </>
  );
}

function Detection({ metrics }: { metrics: Record<string, unknown> }) {
  const at = (key: string) => metric(metrics, "detection", key);
  return (
    <>
      <h2>Detection</h2>
      <p>
        Precision {percent(at("precision"))}, recall {percent(at("recall"))} over{" "}
        {at("replays_scored") ?? 0} replays ({at("true_positives") ?? 0} matched,{" "}
        {at("false_positives") ?? 0} extra, {at("false_negatives") ?? 0} missed). Delay from fault
        start, in event time: p50 {seconds(at("delay_seconds_p50"))}, p95{" "}
        {seconds(at("delay_seconds_p95"))}.
      </p>
    </>
  );
}

function outcomeText(row: EvaluationCaseRow): string {
  const observed = row.observed;
  if (row.status === "failed") return `harness failure: ${row.error ?? "unknown"}`;
  if (observed === null) return row.status;
  if (observed.investigation_id === null) return "no incident, nothing investigated";
  if (observed.diagnosis_status === "insufficient_evidence") return "abstained";
  if (observed.root_cause_service === null) return observed.investigation_status ?? "—";
  return `${observed.root_cause_service} · ${observed.failure_type ?? "?"}`;
}

function expectedText(row: EvaluationCaseRow): string {
  const expected = row.expected;
  if (!expected.incident_expected) return "no incident";
  if (!expected.diagnosable) return "abstain (cause not in evidence)";
  return `${expected.root_cause_service ?? "?"} · ${expected.failure_type ?? "?"}`;
}

function Cases({ run }: { run: EvaluationRunDetail }) {
  const [onlyFailed, setOnlyFailed] = useState(false);
  const rows = run.cases.filter((row) => !onlyFailed || row.scores?.outcome_correct !== true);
  return (
    <>
      <h2>Cases</h2>
      <label className="checkbox">
        <input
          type="checkbox"
          checked={onlyFailed}
          onChange={(event) => {
            setOnlyFailed(event.target.checked);
          }}
        />
        Only incorrect and failed cases
      </label>
      {run.cases.length === 0 && (
        <p className="state" role="status">
          No cases yet.
        </p>
      )}
      {run.cases.length > 0 && rows.length === 0 && (
        <p className="state" role="status">
          Every case is correct.
        </p>
      )}
      {rows.length > 0 && (
        <div className="scroll-x">
          <table>
            <thead>
              <tr>
                <th scope="col">Scenario</th>
                <th scope="col">Variant</th>
                <th scope="col">Expected</th>
                <th scope="col">Observed</th>
                <th scope="col">Result</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.id}>
                  <th scope="row">
                    <Link to={`/evaluations/${run.id}/cases/${row.id}`}>{row.scenario_name}</Link>
                    {row.synthetic && <span className="badge"> synthetic</span>}
                  </th>
                  <td>
                    {VARIANT_LABELS[row.variant] ?? row.variant} #{row.trial}
                  </td>
                  <td>{expectedText(row)}</td>
                  <td>{outcomeText(row)}</td>
                  <td>
                    {row.scores === null ? (
                      <span className="badge badge-failed">{row.status}</span>
                    ) : row.scores.outcome_correct ? (
                      <span className="badge badge-completed">correct</span>
                    ) : (
                      <span className="badge badge-failed">incorrect</span>
                    )}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
