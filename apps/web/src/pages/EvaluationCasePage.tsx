import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router";
import { describeError } from "../api/describeError";
import {
  fetchEvaluationCase,
  recordAudit,
  VARIANT_LABELS,
  type CitedSignal,
  type EvaluationCaseDetail,
} from "../api/evaluations";
import { formatMicroUsd, formatTime } from "../api/format";
import { ErrorState } from "../components/ErrorState";
import { LoadingState } from "../components/LoadingState";
import { OperatorGate } from "../components/OperatorGate";

export function EvaluationCasePage() {
  return (
    <OperatorGate>
      <CaseView />
    </OperatorGate>
  );
}

function CaseView() {
  const { runId = "", caseId = "" } = useParams();
  const query = useQuery({
    queryKey: ["evaluation-case", runId, caseId],
    queryFn: ({ signal }) => fetchEvaluationCase(runId, caseId, signal),
  });
  const detail = query.data;
  if (query.isPending) return <LoadingState label="Loading case…" />;
  if (detail === undefined) {
    return (
      <ErrorState
        title="Cannot load this case"
        error={query.error}
        onRetry={() => void query.refetch()}
      />
    );
  }
  return (
    <section>
      <p>
        <Link to={`/evaluations/${runId}`}>Back to the run</Link>
      </p>
      <h1>
        {detail.scenario_name} · {VARIANT_LABELS[detail.variant] ?? detail.variant} #{detail.trial}
      </h1>
      {detail.synthetic && (
        <p className="hint">Synthetic scenario ({detail.family.replaceAll("_", " ")}).</p>
      )}
      <Outcomes detail={detail} />
      <Citations runId={runId} detail={detail} />
    </section>
  );
}

function Outcomes({ detail }: { detail: EvaluationCaseDetail }) {
  const { expected, observed, scores } = detail;
  const search = `?replay_scope=${encodeURIComponent(detail.replay_scope)}`;
  return (
    <dl className="facts">
      <dt>Expected</dt>
      <dd>
        {!expected.incident_expected
          ? "No incident"
          : expected.diagnosable
            ? `${expected.root_cause_service ?? "?"} · ${expected.failure_type ?? "?"}`
            : "Abstain: the cause is not in the evidence"}
      </dd>
      <dt>Observed</dt>
      <dd>
        {detail.status === "failed"
          ? `Harness failure: ${detail.error ?? "unknown"}`
          : observed === null
            ? detail.status
            : observed.investigation_id === null
              ? "No incident, nothing investigated"
              : observed.diagnosis_status === "insufficient_evidence"
                ? "Abstained"
                : `${observed.root_cause_service ?? "?"} · ${observed.failure_type ?? "?"}`}
      </dd>
      {scores !== null && (
        <>
          <dt>Result</dt>
          <dd>
            {scores.outcome_correct ? "Correct" : "Incorrect"}
            {scores.false_diagnosis && " · false diagnosis"}
            {scores.unsupported_diagnosis && " · no cited signal is fault evidence"}
          </dd>
          <dt>Evidence</dt>
          <dd>
            {scores.cited_supported} of {scores.cited} citations are fault evidence
          </dd>
        </>
      )}
      {observed !== null && observed.investigation_id !== null && (
        <>
          <dt>Cost</dt>
          <dd>
            {formatMicroUsd(observed.spent_microusd)} in {observed.model_calls} model calls
          </dd>
          <dt>Investigation</dt>
          <dd>
            <Link to={`/investigations/${observed.investigation_id}${search}`}>
              Open the investigation and its evidence
            </Link>
          </dd>
        </>
      )}
    </dl>
  );
}

function Citations({ runId, detail }: { runId: string; detail: EvaluationCaseDetail }) {
  const queryClient = useQueryClient();
  const [auditor, setAuditor] = useState("");
  const audit = useMutation({
    mutationFn: (input: { signal: CitedSignal; supported: boolean }) =>
      recordAudit(runId, detail.id, {
        signal_id: input.signal.signal_id,
        supported: input.supported,
        auditor: auditor.trim(),
      }),
    onSuccess: () =>
      void queryClient.invalidateQueries({ queryKey: ["evaluation-case", runId, detail.id] }),
  });
  const judged = new Map(detail.audits.map((item) => [item.signal_id, item]));

  return (
    <>
      <h2>Cited evidence</h2>
      {detail.citations.length === 0 ? (
        <p className="state" role="status">
          This case cited no evidence.
        </p>
      ) : (
        <>
          <label>
            Auditor name
            <input
              value={auditor}
              maxLength={128}
              onChange={(event) => {
                setAuditor(event.target.value);
              }}
            />
          </label>
          <p className="hint">
            &ldquo;Fault evidence&rdquo; comes from the scenario&apos;s labels. Your judgement is
            stored as a human audit and reported apart from it.
          </p>
          {audit.isError && (
            <p className="state state-error" role="alert">
              {describeError(audit.error).message}
            </p>
          )}
          <div className="scroll-x">
            <table>
              <thead>
                <tr>
                  <th scope="col">Signal</th>
                  <th scope="col">Service</th>
                  <th scope="col">Message</th>
                  <th scope="col">Label</th>
                  <th scope="col">Human audit</th>
                </tr>
              </thead>
              <tbody>
                {detail.citations.map((signal) => {
                  const existing = judged.get(signal.signal_id);
                  return (
                    <tr key={signal.signal_id}>
                      <th scope="row">
                        {signal.kind} · {formatTime(signal.occurred_at)}
                      </th>
                      <td>{signal.service_name ?? "unresolved"}</td>
                      <td>{signal.message}</td>
                      <td>{signal.fault_evidence ? "fault evidence" : "not fault evidence"}</td>
                      <td>
                        {existing !== undefined && (
                          <span>
                            {existing.supported ? "supports" : "does not support"} (
                            {existing.auditor}){" "}
                          </span>
                        )}
                        <button
                          type="button"
                          disabled={!auditor.trim() || audit.isPending}
                          onClick={() => {
                            audit.mutate({ signal, supported: true });
                          }}
                        >
                          Supports
                        </button>{" "}
                        <button
                          type="button"
                          disabled={!auditor.trim() || audit.isPending}
                          onClick={() => {
                            audit.mutate({ signal, supported: false });
                          }}
                        >
                          Does not
                        </button>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </>
      )}
    </>
  );
}
