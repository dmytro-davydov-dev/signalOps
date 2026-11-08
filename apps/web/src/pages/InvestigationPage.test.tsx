import { act, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Investigation } from "../api/investigations";
import { clearOperatorToken, setOperatorToken } from "../api/operatorToken";
import type { ProgressEvent } from "../api/progress";
import { fetchByPath, type Route } from "../test/incidentFixtures";
import {
  abstained,
  CITED,
  diagnosed,
  ended,
  evidence,
  INVESTIGATION_ID,
  MISSING,
  noUsage,
  progressPage,
  running,
  sseFrame,
  sseStream,
  step,
  type SseStream,
} from "../test/investigationFixtures";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const PATH = `/investigations/${INVESTIGATION_ID}`;
const API = `/v1/investigations/${INVESTIGATION_ID}`;
const REQUESTED_AT = "2026-11-07T10:00:05Z";

interface StubOptions {
  investigation: () => Response;
  progress?: ProgressEvent[];
  overrides?: Record<string, Route>;
}

/** Each stream connection the page opened, with the cursor it sent. */
interface Connection {
  lastEventId: string | null;
  stream: SseStream;
}

function stub({ investigation, progress = [], overrides = {} }: StubOptions) {
  const connections: Connection[] = [];
  const fetchMock = vi.fn(
    fetchByPath({
      [API]: () => investigation(),
      [`${API}/progress`]: (url) => progressPage(progress, Number(url.searchParams.get("after"))),
      [`${API}/evidence`]: () => jsonResponse(evidence),
      [`${API}/events`]: (_url, init) => {
        const headers = (init?.headers ?? {}) as Record<string, string>;
        const stream = sseStream(init?.signal);
        stream.send("retry: 10\n\n");
        connections.push({ lastEventId: headers["Last-Event-ID"] ?? null, stream });
        return stream.response;
      },
      ...overrides,
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
  return Object.assign(fetchMock, { connections });
}

async function connected(fetchMock: ReturnType<typeof stub>, count = 1): Promise<Connection> {
  await waitFor(() => {
    expect(fetchMock.connections.length).toBeGreaterThanOrEqual(count);
  });
  const connection = fetchMock.connections[count - 1];
  if (connection === undefined) {
    throw new Error("no connection");
  }
  return connection;
}

const callsTo = (fetchMock: ReturnType<typeof stub>, path: string) =>
  fetchMock.mock.calls.filter(([input]) => new URL(input, "http://x").pathname === path);

describe("InvestigationPage", () => {
  beforeEach(() => {
    clearOperatorToken({ rejected: false });
    setOperatorToken("operator-token");
  });

  it("requests cancellation, then shows the durable terminal state", async () => {
    let stored: Investigation = running;
    const fetchMock = stub({ investigation: () => jsonResponse(stored) });
    fetchMock.mockImplementation((input: string, init?: RequestInit) => {
      if (init?.method === "POST") {
        stored = { ...running, cancel_requested_at: REQUESTED_AT };
        return Promise.resolve(jsonResponse(stored));
      }
      return fetchByPath({
        [API]: () => jsonResponse(stored),
        [`${API}/progress`]: () => progressPage([]),
      })(input, init);
    });
    const { queryClient } = renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Cancel investigation" }));

    expect(await screen.findByText(/Cancellation requested\. The status/)).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel investigation" })).not.toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post?.[0]).toBe(`${API}/cancel`);

    stored = {
      ...running,
      status: "cancelled",
      cancel_requested_at: REQUESTED_AT,
      completed_at: "2026-11-07T10:00:09Z",
    };
    await act(() => queryClient.invalidateQueries({ queryKey: ["investigation"] }));

    expect(await screen.findByText("Cancelled")).toBeInTheDocument();
    expect(screen.getByText(/was cancelled\. No diagnosis was stored/)).toBeInTheDocument();
    expect(screen.queryByText(/The status changes/)).not.toBeInTheDocument();
  });

  it("refreshes the investigation when the stream reports its ending", async () => {
    let stored: Investigation = running;
    const fetchMock = stub({
      investigation: () => jsonResponse(stored),
      progress: [step(1, "step.started", "investigate")],
    });
    renderRoute(PATH);
    expect(await screen.findByText("Running")).toBeInTheDocument();
    const connection = await connected(fetchMock);
    // The stream starts after the history already read.
    expect(connection.lastEventId).toBe("1");
    expect(await screen.findByText("Live")).toBeInTheDocument();

    stored = { ...running, status: "failed", completed_at: "2026-11-07T10:00:09Z" };
    connection.stream.send(sseFrame(step(2, "step.failed", "investigate", 1, "permanent")));
    connection.stream.send(sseFrame(ended(3, "failed", null)));

    expect(await screen.findByText("Failed")).toBeInTheDocument();
    expect(screen.getByText("permanent")).toBeInTheDocument();
    expect(screen.queryByLabelText("Live updates")).not.toBeInTheDocument();
    expect(fetchMock.connections).toHaveLength(1);
  });

  it("opens no stream for an investigation that already ended", async () => {
    const fetchMock = stub({
      investigation: () => jsonResponse(abstained),
      progress: [ended(1, "insufficient_evidence", "insufficient_evidence")],
    });
    renderRoute(PATH);

    expect(await screen.findByText("Insufficient evidence")).toBeInTheDocument();
    await screen.findByText("No workflow steps ran.");
    expect(callsTo(fetchMock, `${API}/events`)).toHaveLength(0);
  });

  it("reconnects after a dropped stream with the last event it received", async () => {
    const fetchMock = stub({ investigation: () => jsonResponse(running) });
    renderRoute(PATH);
    const first = await connected(fetchMock);
    expect(first.lastEventId).toBeNull();

    first.stream.send(sseFrame(step(4, "step.started", "load_context")));
    expect(await screen.findByText("load_context")).toBeInTheDocument();
    first.stream.fail();

    expect(await screen.findByText(/missed steps will be replayed/)).toBeInTheDocument();
    const second = await connected(fetchMock, 2);
    expect(second.lastEventId).toBe("4");

    // The server replays what was missed, in order.
    second.stream.send(sseFrame(step(5, "step.completed", "load_context")));
    second.stream.send(sseFrame(step(6, "step.started", "investigate")));
    expect(await screen.findByText("investigate")).toBeInTheDocument();
    expect(await screen.findByText("Live")).toBeInTheDocument();
    const items = within(screen.getByRole("list", { name: "Workflow steps" })).getAllByRole(
      "listitem",
    );
    expect(items.map((item) => item.textContent)).toEqual([
      "load_contextCompleted",
      "investigateRunning",
    ]);
  });

  it("reconnects from its cursor when the server closes at the stream lifetime", async () => {
    const fetchMock = stub({ investigation: () => jsonResponse(running) });
    renderRoute(PATH);
    const first = await connected(fetchMock);
    first.stream.send(sseFrame(step(9, "step.started", "investigate")));
    await screen.findByText("investigate");

    first.stream.close();

    const second = await connected(fetchMock, 2);
    expect(second.lastEventId).toBe("9");
  });

  it("drops held events when the server does not know the cursor", async () => {
    const fetchMock = stub({
      investigation: () => jsonResponse(running),
      progress: [step(1, "step.started", "stale_step")],
    });
    renderRoute(PATH);
    expect(await screen.findByText("stale_step")).toBeInTheDocument();
    const connection = await connected(fetchMock);

    connection.stream.send('event: reset\ndata: {"reason":"unknown_cursor"}\n\n');
    connection.stream.send(sseFrame(step(20, "step.started", "investigate")));

    expect(await screen.findByText("investigate")).toBeInTheDocument();
    expect(screen.queryByText("stale_step")).not.toBeInTheDocument();
  });

  it("ignores a replayed event it already holds", async () => {
    const fetchMock = stub({
      investigation: () => jsonResponse(running),
      progress: [step(1, "step.started", "investigate")],
    });
    renderRoute(PATH);
    const connection = await connected(fetchMock);

    connection.stream.send(sseFrame(step(1, "step.started", "investigate")));
    connection.stream.send(sseFrame(step(2, "step.completed", "investigate")));

    expect(await screen.findByText("Completed")).toBeInTheDocument();
    expect(screen.getAllByText("investigate")).toHaveLength(1);
  });

  it("presents insufficient evidence as a valid outcome", async () => {
    stub({ investigation: () => jsonResponse(abstained) });
    renderRoute(PATH);

    expect(await screen.findByText("Insufficient evidence")).toBeInTheDocument();
    const outcome = screen.getByText(/valid outcome, not a failure/).closest("div");
    expect(outcome).not.toBeNull();
    expect(within(outcome as HTMLElement).getByText("No diagnosis for: checkout")).toBeInTheDocument();
    expect(screen.getByText("Traces from the payment service")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel investigation" })).not.toBeInTheDocument();
    expect(screen.queryByText("Root cause hypothesis")).not.toBeInTheDocument();
  });

  it("shows the diagnosis with verified evidence, hypotheses, actions, and limitations", async () => {
    stub({ investigation: () => jsonResponse(diagnosed) });
    renderRoute(PATH);

    expect(await screen.findByText(diagnosed.diagnosis?.summary ?? "")).toBeInTheDocument();
    expect(screen.getByText("bank-gateway")).toBeInTheDocument();
    expect(screen.getByText("downstream timeout")).toBeInTheDocument();
    expect(screen.getByText(/82%/)).toHaveTextContent("not calibrated");
    const cited = await screen.findByRole("list", { name: "Cited evidence" });
    expect(within(cited).getByText("POST /charge timed out")).toBeInTheDocument();
    expect(within(cited).getByText("verified")).toBeInTheDocument();
    expect(screen.getByText(/Database outage:/)).toBeInTheDocument();
    expect(screen.getByText("Review the gateway timeout")).toBeInTheDocument();
    expect(screen.getByText(/needs a person to apply it/)).toBeInTheDocument();
    expect(screen.getByText("No database connection metrics were available")).toBeInTheDocument();
  });

  it("shows contradictions between specialists, even when it abstains", async () => {
    const disagreeing = {
      ...abstained,
      workflow_version: "v3",
      diagnosis: {
        ...abstained.diagnosis,
        contradictions: [
          {
            description: "The telemetry investigator points to bank-gateway; the deployment investigator points to inventory.",
            roles: ["telemetry_investigator", "deployment_investigator"],
            evidence_signal_ids: [CITED, MISSING],
          },
        ],
      },
    };
    stub({ investigation: () => jsonResponse(disagreeing) });
    renderRoute(PATH);

    expect(await screen.findByText("Contradictions")).toBeInTheDocument();
    expect(screen.getByText(/points to inventory/)).toBeInTheDocument();
    expect(screen.getByText(/telemetry investigator, deployment investigator/)).toBeInTheDocument();
    expect(screen.getByText(/2 cited signals/)).toBeInTheDocument();
    expect(screen.getByText("Insufficient evidence")).toBeInTheDocument();
  });

  it("reads a diagnosis from an older server without contradictions", async () => {
    const older: Record<string, unknown> = { ...diagnosed.diagnosis };
    delete older.contradictions;
    stub({ investigation: () => jsonResponse({ ...diagnosed, diagnosis: older }) });
    renderRoute(PATH);

    expect(await screen.findByText("bank-gateway")).toBeInTheDocument();
    expect(screen.queryByText("Contradictions")).not.toBeInTheDocument();
  });

  it("warns when a citation cannot be verified", async () => {
    stub({
      investigation: () => jsonResponse(diagnosed),
      overrides: {
        [`${API}/evidence`]: () =>
          jsonResponse({ ...evidence, items: [], unverified_signal_ids: [MISSING] }),
      },
    });
    renderRoute(PATH);

    expect(await screen.findByRole("alert")).toHaveTextContent(
      "1 of 1 citations could not be verified",
    );
  });

  it("separates actual spend from estimates", async () => {
    stub({ investigation: () => jsonResponse(diagnosed) });
    renderRoute(PATH);

    expect(await screen.findByText("$0.0781")).toBeInTheDocument();
    expect(screen.getByText(/\$0\.0413 actual from provider usage, \$0\.0368 estimated/)).toBeInTheDocument();
    expect(screen.getByText(/8,200 in, 420 out/)).toBeInTheDocument();
    expect(screen.getByText("None")).toBeInTheDocument();
  });

  it("shows calls of unknown cost and what they hold", async () => {
    stub({
      investigation: () =>
        jsonResponse({
          ...running,
          usage: { ...noUsage, reserved_microusd: 50_000, unknown_calls: 2, unknown_reserved_microusd: 50_000 },
        }),
    });
    renderRoute(PATH);

    expect(
      await screen.findByText(/2 calls with unknown cost, holding \$0\.05\. The provider may have billed them/),
    ).toBeInTheDocument();
  });

  it("shows each step's latest attempt from the progress history", async () => {
    stub({
      investigation: () => jsonResponse(abstained),
      progress: [
        step(1, "step.started", "load_context"),
        step(2, "step.completed", "load_context"),
        step(3, "step.started", "investigate"),
        step(4, "step.failed", "investigate", 1, "transient"),
        step(5, "step.started", "investigate", 2),
        step(6, "step.completed", "investigate", 2),
        ended(7, "insufficient_evidence", "insufficient_evidence"),
      ],
    });
    renderRoute(PATH);

    const steps = await screen.findByRole("list", { name: "Workflow steps" });
    const items = within(steps).getAllByRole("listitem");
    expect(items.map((item) => item.textContent)).toEqual([
      "load_contextCompleted",
      "investigateCompletedattempt 2",
    ]);
  });

  it("explains a cancellation that lost to another ending", async () => {
    stub({
      investigation: () => jsonResponse(running),
      overrides: { [`${API}/cancel`]: () => jsonResponse({ detail: "ended" }, 409) },
    });
    renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Cancel investigation" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("already ended");
  });

  it("shows a replay investigation as read-only", async () => {
    const fetchMock = stub({
      investigation: () => jsonResponse({ ...running, replay_scope: "replay:run" }),
    });
    renderRoute(`${PATH}?replay_scope=replay:run`);

    expect(await screen.findByText("Replay investigations are read-only.")).toBeInTheDocument();
    expect(fetchMock.mock.calls[0]?.[0]).toBe(`${API}?replay_scope=replay%3Arun`);
  });

  it("says when the investigation does not exist", async () => {
    stub({ investigation: () => jsonResponse({ detail: "investigation not found" }, 404) });
    renderRoute(PATH);

    expect(await screen.findByText("Investigation not found")).toBeInTheDocument();
  });

  it("keeps the last state and says it is stale when a refresh fails", async () => {
    let healthy = true;
    stub({ investigation: () => (healthy ? jsonResponse(running) : jsonResponse({}, 500)) });
    const { queryClient } = renderRoute(PATH);
    expect(await screen.findByText("Running")).toBeInTheDocument();

    healthy = false;
    await act(() => queryClient.refetchQueries({ queryKey: ["investigation"] }));

    expect(await screen.findByText(/Last refresh failed/)).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
  });
});
