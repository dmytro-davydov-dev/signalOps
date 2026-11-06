import { screen, within } from "@testing-library/react";
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
    expect(screen.getByText("Quality flags: clock_skew")).toBeInTheDocument();
    expect(screen.getByText(/\[REDACTED\]/)).toBeInTheDocument();
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
});
