import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { clearOperatorToken, getOperatorToken, setOperatorToken } from "../api/operatorToken";
import { fetchByPath, INCIDENT_ID, summary } from "../test/incidentFixtures";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const page = { items: [summary], next_cursor: null };

function stubIncidents(respond: (url: URL) => Response) {
  const handler = vi.fn(respond);
  vi.stubGlobal("fetch", vi.fn(fetchByPath({ "/v1/incidents": handler })));
  return handler;
}

describe("IncidentsPage", () => {
  beforeEach(() => {
    clearOperatorToken({ rejected: false });
    setOperatorToken("operator-token");
  });

  it("asks for a token, then sends it with the request", async () => {
    clearOperatorToken({ rejected: false });
    const fetchMock = vi.fn(fetchByPath({ "/v1/incidents": () => jsonResponse(page) }));
    vi.stubGlobal("fetch", fetchMock);
    renderRoute("/incidents");

    expect(screen.getByRole("heading", { name: "Operator sign-in" })).toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();

    await userEvent.type(screen.getByLabelText("Operator token"), "operator-token");
    await userEvent.click(screen.getByRole("button", { name: "Continue" }));

    expect(await screen.findByRole("link", { name: summary.title })).toBeInTheDocument();
    const headers = fetchMock.mock.calls[0]?.[1]?.headers as Record<string, string>;
    expect(headers.Authorization).toBe("Bearer operator-token");
  });

  it("returns to the sign-in form when the API rejects the token", async () => {
    stubIncidents(() => jsonResponse({ detail: "missing or invalid operator credential" }, 401));
    renderRoute("/incidents");

    expect(await screen.findByRole("alert")).toHaveTextContent("rejected that token");
    expect(screen.getByLabelText("Operator token")).toBeInTheDocument();
    expect(getOperatorToken()).toBeNull();
  });

  it("forgets the token on request", async () => {
    stubIncidents(() => jsonResponse(page));
    renderRoute("/incidents");
    await screen.findByRole("link", { name: summary.title });

    await userEvent.click(screen.getByRole("button", { name: "Forget operator token" }));

    expect(screen.getByRole("heading", { name: "Operator sign-in" })).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(sessionStorage.getItem("signalops.operatorToken")).toBeNull();
  });

  it("links each incident to its detail page", async () => {
    stubIncidents(() => jsonResponse(page));
    renderRoute("/incidents");

    const link = await screen.findByRole("link", { name: summary.title });
    expect(link).toHaveAttribute("href", `/incidents/${INCIDENT_ID}`);
    expect(within(screen.getByRole("table")).getByText("high")).toBeInTheDocument();
  });

  it("explains an empty list", async () => {
    stubIncidents(() => jsonResponse({ items: [], next_cursor: null }));
    renderRoute("/incidents");

    expect(await screen.findByText(/No incidents yet/)).toBeInTheDocument();
  });

  it("sends filters to the API and resets to a filtered empty state", async () => {
    const handler = stubIncidents((url) =>
      url.searchParams.has("severity")
        ? jsonResponse({ items: [], next_cursor: null })
        : jsonResponse(page),
    );
    renderRoute("/incidents");
    await screen.findByRole("link", { name: summary.title });

    await userEvent.selectOptions(screen.getByLabelText("Severity"), "critical");

    expect(await screen.findByText("No incidents match these filters.")).toBeInTheDocument();
    expect(handler.mock.lastCall?.[0].searchParams.get("severity")).toBe("critical");
  });

  it("keeps the replay scope on the query and on detail links", async () => {
    const scope = "replay:7d4c0e8a-1111-4a2b-9c3d-000000000002";
    const handler = stubIncidents(() =>
      jsonResponse({ ...page, items: [{ ...summary, replay_scope: scope }] }),
    );
    renderRoute(`/incidents?replay_scope=${scope}`);

    const link = await screen.findByRole("link", { name: summary.title });
    expect(handler.mock.lastCall?.[0].searchParams.get("replay_scope")).toBe(scope);
    expect(link.getAttribute("href")).toContain(`replay_scope=${scope}`);
    expect(screen.getByText(`Replay scope: ${scope}`)).toBeInTheDocument();
  });

  it("appends the next page with the server's cursor", async () => {
    const second = { ...summary, id: "ffffffff-0e1f-4a2b-9c3d-4e5f6a7b8c9d", title: "Older" };
    const handler = stubIncidents((url) =>
      url.searchParams.get("cursor") === "next"
        ? jsonResponse({ items: [second], next_cursor: null })
        : jsonResponse({ items: [summary], next_cursor: "next" }),
    );
    renderRoute("/incidents");
    await screen.findByRole("link", { name: summary.title });

    await userEvent.click(screen.getByRole("button", { name: "Load more" }));

    expect(await screen.findByRole("link", { name: "Older" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: summary.title })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Load more" })).not.toBeInTheDocument();
    expect(handler).toHaveBeenCalledTimes(2);
  });

  it("shows a failure and recovers on retry", async () => {
    let calls = 0;
    stubIncidents(() => {
      calls += 1;
      return calls === 1 ? new Response("", { status: 503 }) : jsonResponse(page);
    });
    renderRoute("/incidents");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Cannot load incidents");
    await userEvent.click(within(alert).getByRole("button", { name: "Retry" }));
    expect(await screen.findByRole("link", { name: summary.title })).toBeInTheDocument();
  });

  it("keeps rows and reports a lost connection when a refresh fails", async () => {
    const fetchMock = vi.fn(fetchByPath({ "/v1/incidents": () => jsonResponse(page) }));
    vi.stubGlobal("fetch", fetchMock);
    const { queryClient } = renderRoute("/incidents");
    await screen.findByRole("link", { name: summary.title });

    fetchMock.mockRejectedValue(new TypeError("Failed to fetch"));
    await queryClient.refetchQueries({ queryKey: ["incidents"] });

    expect(await screen.findByText(/Disconnected from the API/)).toBeInTheDocument();
    expect(screen.getByRole("link", { name: summary.title })).toBeInTheDocument();
    await waitFor(() => {
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    });
  });

  it("rejects a response with an unknown severity", async () => {
    stubIncidents(() =>
      jsonResponse({ items: [{ ...summary, severity: "catastrophic" }], next_cursor: null }),
    );
    renderRoute("/incidents");

    expect(await screen.findByRole("alert")).toHaveTextContent("does not understand");
  });
});
