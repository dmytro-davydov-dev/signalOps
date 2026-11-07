import { act, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Investigation } from "../api/investigations";
import { clearOperatorToken, setOperatorToken } from "../api/operatorToken";
import { INCIDENT_ID } from "../test/incidentFixtures";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const ID = "9b1f2c3d-4e5f-4a6b-8c7d-0e1f2a3b4c5d";
const PATH = `/investigations/${ID}`;
const REQUESTED_AT = "2026-11-07T10:00:05Z";

const running: Investigation = {
  id: ID,
  incident_id: INCIDENT_ID,
  replay_scope: "live",
  status: "running",
  created_at: "2026-11-07T10:00:00Z",
  deadline_at: "2026-11-07T10:10:00Z",
  completed_at: null,
  cancel_requested_at: null,
  usage: { budget_microusd: 1000, reserved_microusd: 0, spent_microusd: 0 },
  diagnosis: null,
};

function stub(respond: (init?: RequestInit) => Response) {
  const fetchMock = vi.fn((_input: string, init?: RequestInit) =>
    Promise.resolve(respond(init)),
  );
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("InvestigationPage", () => {
  beforeEach(() => {
    clearOperatorToken({ rejected: false });
    setOperatorToken("operator-token");
  });

  it("requests cancellation, then shows the durable terminal state", async () => {
    let stored = running;
    const fetchMock = stub((init) => {
      if (init?.method === "POST") {
        stored = { ...running, cancel_requested_at: REQUESTED_AT };
      }
      return jsonResponse(stored);
    });
    const { queryClient } = renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Cancel investigation" }));

    expect(await screen.findByText(/Cancellation requested\. The status/)).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel investigation" })).not.toBeInTheDocument();
    const post = fetchMock.mock.calls.find(([, init]) => init?.method === "POST");
    expect(post?.[0]).toBe(`/v1/investigations/${ID}/cancel`);

    // The worker ended it; the next refresh brings the terminal state.
    stored = {
      ...running,
      status: "cancelled",
      cancel_requested_at: REQUESTED_AT,
      completed_at: "2026-11-07T10:00:09Z",
    };
    await act(() => queryClient.invalidateQueries({ queryKey: ["investigation"] }));

    expect(await screen.findByText("Cancelled")).toBeInTheDocument();
    expect(screen.getByText("Ended")).toBeInTheDocument();
    expect(screen.queryByText(/The status changes/)).not.toBeInTheDocument();
  });

  it("polls until the status is terminal", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    try {
      let stored = running;
      const fetchMock = stub(() => jsonResponse(stored));
      renderRoute(PATH);
      expect(await screen.findByText("Running")).toBeInTheDocument();

      stored = { ...running, status: "failed", completed_at: "2026-11-07T10:00:09Z" };
      await act(() => vi.advanceTimersByTimeAsync(3_000));
      expect(await screen.findByText("Failed")).toBeInTheDocument();

      const calls = fetchMock.mock.calls.length;
      await act(() => vi.advanceTimersByTimeAsync(30_000));
      expect(fetchMock.mock.calls.length).toBe(calls);
    } finally {
      vi.useRealTimers();
    }
  });

  it("offers no cancellation once the investigation ended", async () => {
    stub(() =>
      jsonResponse({
        ...running,
        status: "insufficient_evidence",
        completed_at: "2026-11-07T10:01:00Z",
        diagnosis: { status: "insufficient_evidence", summary: "No diagnosis for: checkout" },
      }),
    );
    renderRoute(PATH);

    expect(await screen.findByText("Insufficient evidence")).toBeInTheDocument();
    expect(screen.getByText("No diagnosis for: checkout")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Cancel investigation" })).not.toBeInTheDocument();
  });

  it("explains a cancellation that lost to another ending", async () => {
    stub((init) =>
      init?.method === "POST" ? jsonResponse({ detail: "ended" }, 409) : jsonResponse(running),
    );
    renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Cancel investigation" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("already ended");
  });

  it("shows a replay investigation as read-only", async () => {
    const fetchMock = stub(() => jsonResponse({ ...running, replay_scope: "replay:run" }));
    renderRoute(`${PATH}?replay_scope=replay:run`);

    expect(await screen.findByText("Replay investigations are read-only.")).toBeInTheDocument();
    expect(fetchMock.mock.calls[0]?.[0]).toBe(`/v1/investigations/${ID}?replay_scope=replay%3Arun`);
  });

  it("says when the investigation does not exist", async () => {
    stub(() => jsonResponse({ detail: "investigation not found" }, 404));
    renderRoute(PATH);

    expect(await screen.findByText("Investigation not found")).toBeInTheDocument();
  });

  it("keeps the last state and says it is stale when a refresh fails", async () => {
    let healthy = true;
    stub(() => (healthy ? jsonResponse(running) : jsonResponse({}, 500)));
    const { queryClient } = renderRoute(PATH);
    expect(await screen.findByText("Running")).toBeInTheDocument();

    healthy = false;
    await act(() => queryClient.refetchQueries({ queryKey: ["investigation"] }));

    expect(await screen.findByText(/Last refresh failed/)).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
  });
});
