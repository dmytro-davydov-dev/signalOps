import { screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { jsonResponse, renderRoute } from "../test/renderRoute";

const ready = {
  status: "ready",
  checks: {
    postgres: { status: "ok", latency_ms: 3.2, error: null },
    redis: { status: "ok", latency_ms: 1.1, error: null },
    kafka: { status: "ok", latency_ms: 2.4, error: null },
  },
};

describe("HealthPage", () => {
  it("shows every dependency when the API is ready", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(ready)));
    renderRoute("/status");

    expect(screen.getByRole("status")).toHaveTextContent("Checking dependencies");
    expect(await screen.findByText("All dependencies ready")).toBeInTheDocument();
    expect(screen.getAllByRole("row")).toHaveLength(4);
  });

  it("treats a 503 readiness body as not ready and names the failing dependency", async () => {
    const notReady = {
      status: "not_ready",
      checks: { ...ready.checks, redis: { status: "error", latency_ms: 2000, error: "timeout" } },
    };
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse(notReady, 503)));
    renderRoute("/status");

    expect(await screen.findByText("Not ready")).toBeInTheDocument();
    const redisRow = screen.getByRole("row", { name: /redis/ });
    expect(within(redisRow).getByText("error")).toBeInTheDocument();
    expect(within(redisRow).getByText("timeout")).toBeInTheDocument();
  });

  it("explains an unreachable API and recovers on retry", async () => {
    const fetchMock = vi
      .fn()
      .mockRejectedValueOnce(new TypeError("Failed to fetch"))
      .mockResolvedValue(jsonResponse(ready));
    vi.stubGlobal("fetch", fetchMock);
    renderRoute("/status");

    const alert = await screen.findByRole("alert");
    expect(alert).toHaveTextContent("Cannot load system health");
    expect(alert).toHaveTextContent("could not be reached");
    expect(alert).toHaveTextContent("Check that the api service is running");

    await userEvent.click(within(alert).getByRole("button", { name: "Retry" }));
    expect(await screen.findByText("All dependencies ready")).toBeInTheDocument();
  });

  it("keeps the last results visible and marks them stale when a refresh fails", async () => {
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(ready))
      .mockRejectedValue(new TypeError("Failed to fetch"));
    vi.stubGlobal("fetch", fetchMock);
    const { queryClient } = renderRoute("/status");
    expect(await screen.findByText("All dependencies ready")).toBeInTheDocument();

    await queryClient.refetchQueries({ queryKey: ["health", "ready"] });

    expect(await screen.findByText(/Last refresh failed/)).toBeInTheDocument();
    expect(screen.getByText("All dependencies ready")).toBeInTheDocument();
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("reports an HTTP failure status without crashing", async () => {
    vi.stubGlobal(
      "fetch",
      vi.fn().mockResolvedValue(new Response("<html>Bad gateway</html>", { status: 502 })),
    );
    renderRoute("/status");

    expect(await screen.findByRole("alert")).toHaveTextContent("HTTP 502");
  });

  it("rejects a response body with an unexpected shape", async () => {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(jsonResponse({ status: "maybe" })));
    renderRoute("/status");

    expect(await screen.findByRole("alert")).toHaveTextContent("does not understand");
  });
});
