import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { clearOperatorToken, setOperatorToken } from "../api/operatorToken";
import {
  CHECKOUT,
  detail,
  evidencePage,
  fetchByPath,
  graph,
  INCIDENT_ID,
  incidentRoutes,
  PAYMENT,
  summary,
  type Route,
} from "../test/incidentFixtures";
import { INVESTIGATION_ID, progressPage, running } from "../test/investigationFixtures";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const PATH = `/incidents/${INCIDENT_ID}`;
const API_PATH = `/v1/incidents/${INCIDENT_ID}`;

function stub(overrides: Record<string, Route> = {}) {
  const fetchMock = vi.fn(fetchByPath(incidentRoutes(overrides)));
  vi.stubGlobal("fetch", fetchMock);
  return fetchMock;
}

describe("IncidentDetailPage", () => {
  beforeEach(() => {
    clearOperatorToken({ rejected: false });
    setOperatorToken("operator-token");
  });

  it("shows services, rule firings, and evidence with its quality", async () => {
    stub();
    renderRoute(PATH);

    expect(await screen.findByRole("heading", { name: summary.title })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "checkout" })).toHaveAttribute(
      "href",
      `/incidents?service_id=${CHECKOUT}`,
    );
    expect(screen.getByText("trace_error_rate")).toBeInTheDocument();
    expect(await screen.findByText("POST /charge timed out")).toBeInTheDocument();
    expect(screen.getByText("degraded")).toBeInTheDocument();
    expect(screen.getByText("clock_skew")).toBeInTheDocument();
    expect(screen.getByText(/\[REDACTED\]/)).toBeInTheDocument();
  });

  it("summarizes the quality of the cited evidence", async () => {
    const ref = {
      signal_id: "22222222-2222-4222-8222-222222222222",
      kind: "trace",
      occurred_at: "2026-11-06T17:01:05Z",
      severity: "error",
      relevance: 1,
    };
    stub({
      [API_PATH]: () =>
        jsonResponse({
          ...detail,
          evidence: [
            { ...ref, quality_status: "accepted" },
            { ...ref, signal_id: "33333333-3333-4333-8333-333333333333", quality_status: "degraded" },
          ],
        }),
    });
    renderRoute(PATH);

    expect(
      await screen.findByText(/1 of 2 cited signals are not fully accepted \(1 degraded\)/),
    ).toBeInTheDocument();
  });

  it("says when there is no evidence to assess", async () => {
    stub();
    renderRoute(PATH);

    expect(await screen.findByText(/nothing to assess/)).toBeInTheDocument();
  });

  it("draws the neighborhood as of the incident, not as of now", async () => {
    const fetchMock = stub();
    renderRoute(PATH);

    expect(
      await screen.findByRole("img", { name: "Dependency neighborhood of checkout" }),
    ).toBeInTheDocument();
    const row = screen.getByRole("row", { name: "payment bank-gateway 2" });
    expect(row).toBeInTheDocument();

    const graphCall = fetchMock.mock.calls.find(([url]) => url.includes("/graph"));
    const asOf = new URL(graphCall?.[0] ?? "", "http://signalops.test").searchParams.get("as_of");
    expect(asOf).toBe(summary.last_seen_at);
  });

  it("recenters the neighborhood on another service and explains an empty one", async () => {
    stub();
    renderRoute(PATH);
    await screen.findByRole("img");

    await userEvent.selectOptions(screen.getByLabelText("Centered on"), PAYMENT);

    expect(await screen.findByText(/No dependencies were observed/)).toBeInTheDocument();
    expect(screen.queryByRole("img")).not.toBeInTheDocument();
  });

  it("marks a truncated neighborhood", async () => {
    stub({ [`/v1/services/${CHECKOUT}/graph`]: () => jsonResponse({ ...graph, truncated: true }) });
    renderRoute(PATH);

    expect(await screen.findByText(/reached its size bound/)).toBeInTheDocument();
  });

  it("states when an incident cites no evidence", async () => {
    stub({ [`${API_PATH}/evidence`]: () => jsonResponse({ ...evidencePage, items: [] }) });
    renderRoute(PATH);

    expect(await screen.findByText(/Insufficient evidence/)).toBeInTheDocument();
  });

  it("resolves at the version it read and shows the result", async () => {
    let body: unknown;
    stub({
      [API_PATH]: (_, init) => {
        if (init?.method === "PATCH") {
          body = JSON.parse(init.body as string);
          return jsonResponse({
            ...detail,
            status: "resolved",
            resolved_at: "2026-11-06T17:10:00Z",
            version: 4,
          });
        }
        return jsonResponse(detail);
      },
    });
    renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Resolve incident" }));

    expect(await screen.findByRole("button", { name: "Reopen incident" })).toBeInTheDocument();
    expect(body).toEqual({ status: "resolved", version: 3 });
    expect(screen.getByText("Resolved")).toBeInTheDocument();
  });

  it("explains a version conflict and loads the latest version", async () => {
    let conflicted = false;
    stub({
      [API_PATH]: (_, init) => {
        if (init?.method === "PATCH") {
          conflicted = true;
          return jsonResponse({ detail: "incident is at version 4" }, 409);
        }
        return jsonResponse(conflicted ? { ...detail, version: 4 } : detail);
      },
    });
    renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Resolve incident" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("changed while you were viewing");
    const facts = screen.getByText("Version").nextElementSibling;
    await vi.waitFor(() => {
      expect(facts).toHaveTextContent("4");
    });
  });

  it("reports a failed status change and keeps the incident on screen", async () => {
    stub({
      [API_PATH]: (_, init) =>
        init?.method === "PATCH" ? new Response("", { status: 503 }) : jsonResponse(detail),
    });
    renderRoute(PATH);

    await userEvent.click(await screen.findByRole("button", { name: "Resolve incident" }));

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Status not changed");
    expect(screen.getByRole("heading", { name: summary.title })).toBeInTheDocument();
  });

  it("offers no status change on a replay incident and keeps its scope", async () => {
    const scope = "replay:7d4c0e8a-1111-4a2b-9c3d-000000000002";
    const fetchMock = stub({
      [API_PATH]: () => jsonResponse({ ...detail, replay_scope: scope }),
    });
    renderRoute(`${PATH}?replay_scope=${scope}`);

    expect(await screen.findByText("Replay incidents are read-only.")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Resolve incident" })).not.toBeInTheDocument();
    await screen.findByRole("img");
    const requested = fetchMock.mock.calls.map(([url]) => url);
    expect(requested.every((url) => url.includes("replay_scope=replay"))).toBe(true);
  });

  it("names a missing incident without offering a retry", async () => {
    stub({ [API_PATH]: () => jsonResponse({ detail: "incident not found" }, 404) });
    renderRoute(PATH);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Incident not found");
    expect(within(alert).queryByRole("button")).not.toBeInTheDocument();
  });

  it("keeps the incident when only the neighborhood fails", async () => {
    stub({ [`/v1/services/${CHECKOUT}/graph`]: () => new Response("", { status: 500 }) });
    renderRoute(PATH);

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Cannot load the dependency neighborhood");
    expect(screen.getByRole("heading", { name: summary.title })).toBeInTheDocument();
  });

  describe("investigations", () => {
    const LIST_PATH = `${API_PATH}/investigations`;
    const accepted = {
      id: INVESTIGATION_ID,
      incident_id: INCIDENT_ID,
      status: "queued",
      created_at: "2026-11-07T10:00:00Z",
    };
    const investigationRoutes: Record<string, Route> = {
      [`/v1/investigations/${INVESTIGATION_ID}`]: () =>
        jsonResponse({ ...running, status: "queued" }),
      [`/v1/investigations/${INVESTIGATION_ID}/progress`]: () => progressPage([]),
    };

    beforeEach(() => {
      sessionStorage.clear();
    });

    it("lists the incident's investigations with their status", async () => {
      stub({
        [LIST_PATH]: () =>
          jsonResponse({
            incident_id: INCIDENT_ID,
            replay_scope: "live",
            items: [
              {
                id: INVESTIGATION_ID,
                status: "insufficient_evidence",
                created_at: "2026-11-07T10:00:00Z",
                completed_at: "2026-11-07T10:01:00Z",
                cancel_requested_at: null,
                diagnosis_status: "insufficient_evidence",
              },
            ],
            truncated: false,
          }),
      });
      renderRoute(PATH);

      const heading = await screen.findByRole("heading", { name: "Investigations" });
      expect(heading).toBeInTheDocument();
      expect(await screen.findByText("Insufficient evidence")).toBeInTheDocument();
      const link = screen.getAllByRole("link").find((a) => a.getAttribute("href")?.includes(INVESTIGATION_ID));
      expect(link).toHaveAttribute("href", `/investigations/${INVESTIGATION_ID}`);
    });

    it("starts one investigation however often the button is clicked", async () => {
      let release: () => void = () => undefined;
      const gate = new Promise<void>((resolve) => {
        release = resolve;
      });
      const posts: string[] = [];
      const fetchMock = stub({
        ...investigationRoutes,
        [LIST_PATH]: async (url, init) => {
          if (init?.method === "POST") {
            posts.push((init.headers as Record<string, string>)["Idempotency-Key"] ?? "");
            await gate;
            return jsonResponse(accepted, 202);
          }
          return incidentRoutes()[LIST_PATH]?.(url, init) ?? jsonResponse({}, 500);
        },
      });
      renderRoute(PATH);

      const button = await screen.findByRole("button", { name: "Start investigation" });
      await userEvent.click(button);
      await userEvent.click(button);
      await userEvent.dblClick(button);
      expect(screen.getByRole("button", { name: "Starting…" })).toBeDisabled();
      release();

      expect(await screen.findByRole("heading", { name: "Investigation" })).toBeInTheDocument();
      expect(posts).toHaveLength(1);
      expect(posts[0]).not.toBe("");
      expect(fetchMock.mock.calls.filter(([, init]) => init?.method === "POST")).toHaveLength(1);
      expect(sessionStorage.length).toBe(0);
    });

    it("retries a start whose response was lost with the same key", async () => {
      const posts: string[] = [];
      stub({
        ...investigationRoutes,
        [LIST_PATH]: (url, init) => {
          if (init?.method === "POST") {
            posts.push((init.headers as Record<string, string>)["Idempotency-Key"] ?? "");
            if (posts.length === 1) {
              throw new TypeError("Failed to fetch");
            }
            return jsonResponse(accepted, 202);
          }
          return incidentRoutes()[LIST_PATH]?.(url, init) ?? jsonResponse({}, 500);
        },
      });
      renderRoute(PATH);

      await userEvent.click(await screen.findByRole("button", { name: "Start investigation" }));
      expect(await screen.findByText(/cannot start a second investigation/)).toBeInTheDocument();
      await userEvent.click(screen.getByRole("button", { name: "Retry start" }));

      expect(await screen.findByRole("heading", { name: "Investigation" })).toBeInTheDocument();
      expect(posts).toHaveLength(2);
      expect(posts[1]).toBe(posts[0]);
    });

    it("uses a new key for a deliberate later start", async () => {
      const posts: string[] = [];
      stub({
        [LIST_PATH]: (url, init) => {
          if (init?.method === "POST") {
            posts.push((init.headers as Record<string, string>)["Idempotency-Key"] ?? "");
            return jsonResponse({ detail: "incident not found" }, 404);
          }
          return incidentRoutes()[LIST_PATH]?.(url, init) ?? jsonResponse({}, 500);
        },
      });
      renderRoute(PATH);

      const button = await screen.findByRole("button", { name: "Start investigation" });
      await userEvent.click(button);
      expect(await screen.findByText("Investigation not started")).toBeInTheDocument();
      await userEvent.click(screen.getByRole("button", { name: "Start investigation" }));

      await waitFor(() => {
        expect(posts).toHaveLength(2);
      });
      expect(posts[1]).not.toBe(posts[0]);
    });

    it("offers no start control on a replay incident", async () => {
      const scope = "replay:7d4c0e8a-1111-4a2b-9c3d-000000000002";
      stub({
        [API_PATH]: () => jsonResponse({ ...detail, replay_scope: scope }),
        [LIST_PATH]: () =>
          jsonResponse({ incident_id: INCIDENT_ID, replay_scope: scope, items: [], truncated: false }),
      });
      renderRoute(`${PATH}?replay_scope=${scope}`);

      expect(await screen.findByText("No investigations yet.")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Start investigation" })).not.toBeInTheDocument();
    });
  });
});
