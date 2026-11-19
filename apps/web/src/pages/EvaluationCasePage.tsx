import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { useState } from "react";
import { Link, useParams } from "react-router";
import { describeError } from "../api/describeError";
import {
  FAILURE_STAGE_LABELS,
  fetchEvaluationCase,
  recordAudit,
  VARIANT_LABELS,
  type CitedSignal,
  type EvaluationCaseDetail,
  type StageClaim,
  type StageOutputView,
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
      <Stages detail={detail} />
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
      {detail.failure_stage && (
        <>
          <dt>First failing stage</dt>
          <dd>
            {FAILURE_STAGE_LABELS[detail.failure_stage] ?? detail.failure_stage}
            {detail.failure_origin && ` (first asserted at ${detail.failure_origin})`}
            {detail.limit_hit && " · a role ended on a limit"}
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

const STEP_LABELS: Record<string, string> = {
  investigate: "Single agent",
  telemetry: "Telemetry specialist",
  deployment: "Deployment specialist",
  dependency: "Dependency specialist",
  synthesize: "Synthesis",
  verify: "Verification",
  follow_up: "Follow-up round",
  reverify: "Final verification",
};

function Stages({ detail }: { detail: EvaluationCaseDetail }) {
  const outputs = detail.stage_outputs ?? [];
  if (detail.investigation_id === null) return null;
  const signals = new Map((detail.stage_signals ?? []).map((item) => [item.signal_id, item]));
  const evidence = `/investigations/${detail.investigation_id}?replay_scope=${encodeURIComponent(detail.replay_scope)}`;
  return (
    <>
      <h2>Stages</h2>
      {outputs.length === 0 ? (
        <p className="state" role="status">
          No stored stage outputs for this case.
        </p>
      ) : (
        <ol className="stages">
          {outputs.map((output) => (
            <StageItem key={output.step_key} output={output} signals={signals} evidence={evidence} />
          ))}
        </ol>
      )}
    </>
  );
}

function StageItem({
  output,
  signals,
  evidence,
}: {
  output: StageOutputView;
  signals: Map<string, CitedSignal>;
  evidence: string;
}) {
  return (
    <li>
      <h3>{STEP_LABELS[output.step_key] ?? output.step_key}</h3>
      <p className="hint">
        {output.ended_by && `Ended: ${output.ended_by.replaceAll("_", " ")}`}
        {output.status && ` · ${output.status.replaceAll("_", " ")}`}
        {output.verdict && ` · verdict ${output.verdict.replaceAll("_", " ")}`}
        {output.note && ` · ${output.note}`}
      </p>
      {output.leading !== null && (
        <p>
          Leading: <Claim claim={output.leading} signals={signals} evidence={evidence} />
        </p>
      )}
      {output.findings.length > 0 && (
        <ul>
          {output.findings.map((claim, index) => (
            <li key={index}>
              <Claim claim={claim} signals={signals} evidence={evidence} />
            </li>
          ))}
        </ul>
      )}
      {output.citation_checks.length > 0 && (
        <p>
          {output.citation_checks.filter((check) => check.supports).length} of{" "}
          {output.citation_checks.length} cited signals judged supporting:{" "}
          {output.citation_checks.map((check) => (
            <SignalLink
              key={check.signal_id}
              id={check.signal_id}
              signals={signals}
              evidence={evidence}
              mark={check.supports ? "✓" : "✗"}
            />
          ))}
        </p>
      )}
      {output.missing_evidence.length > 0 && (
        <p className="hint">Missing: {output.missing_evidence.join("; ")}</p>
      )}
    </li>
  );
}

function Claim({
  claim,
  signals,
  evidence,
}: {
  claim: StageClaim;
  signals: Map<string, CitedSignal>;
  evidence: string;
}) {
  return (
    <>
      <strong>
        {claim.service_name ?? "no service"}
        {claim.failure_type && ` · ${claim.failure_type}`}
      </strong>
      {claim.confidence !== null && ` (confidence ${claim.confidence.toFixed(2)})`} — {claim.claim}{" "}
      {claim.evidence_signal_ids.map((id) => (
        <SignalLink key={id} id={id} signals={signals} evidence={evidence} />
      ))}
    </>
  );
}

function SignalLink({
  id,
  signals,
  evidence,
  mark = "",
}: {
  id: string;
  signals: Map<string, CitedSignal>;
  evidence: string;
  mark?: string;
}) {
  const signal = signals.get(id);
  const label = signal
    ? `${mark}${signal.service_name ?? "unresolved"} ${signal.kind}${signal.fault_evidence ? " (fault evidence)" : ""}`
    : `${mark}unreadable signal`;
  return (
    <Link className="badge" to={`${evidence}#signal-${id}`} title={signal?.message ?? id}>
      {label}
    </Link>
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
