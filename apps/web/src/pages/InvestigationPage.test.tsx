import { act, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Investigation } from "../api/investigations";
import { clearOperatorToken, setOperatorToken } from "../api/operatorToken";
import type { ProgressEvent } from "../api/progress";
import { fetchByPath, type Route } from "../test/incidentFixtures";
import {
  abstained,
  diagnosed,
  ended,
  evidence,
  INVESTIGATION_ID,
  MISSING,
  noUsage,
  progressPage,
  running,
  step,
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

function stub({ investigation, progress = [], overrides = {} }: StubOptions) {
  const fetchMock = vi.fn(
    fetchByPath({
      [API]: () => investigation(),
      [`${API}/progress`]: (url) => progressPage(progress, Number(url.searchParams.get("after"))),
      [`${API}/evidence`]: () => jsonResponse(evidence),
      ...overrides,
    }),
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
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

  it("refreshes until the status is terminal, then stops", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      let stored: Investigation = running;
      const fetchMock = stub({ investigation: () => jsonResponse(stored) });
      renderRoute(PATH);
      expect(await screen.findByText("Running")).toBeInTheDocument();

      stored = { ...running, status: "failed", completed_at: "2026-11-07T10:00:09Z" };
      await act(() => vi.advanceTimersByTimeAsync(3_000));
      expect(await screen.findByText("Failed")).toBeInTheDocument();

      const calls = callsTo(fetchMock, API).length;
      await act(() => vi.advanceTimersByTimeAsync(30_000));
      expect(callsTo(fetchMock, API).length).toBe(calls);
    } finally {
      vi.useRealTimers();
    }
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

  it("reads only new progress events on each refresh", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      const progress = [step(1, "step.started", "load_context")];
      const fetchMock = stub({ investigation: () => jsonResponse(running), progress });
      renderRoute(PATH);
      expect(await screen.findByText("load_context")).toBeInTheDocument();

      progress.push(step(2, "step.completed", "load_context"));
      await act(() => vi.advanceTimersByTimeAsync(3_000));

      expect(await screen.findByText("Completed")).toBeInTheDocument();
      const afters = callsTo(fetchMock, `${API}/progress`).map(([input]) =>
        new URL(input, "http://x").searchParams.get("after"),
      );
      expect(afters.slice(0, 2)).toEqual(["0", "1"]);
    } finally {
      vi.useRealTimers();
    }
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
