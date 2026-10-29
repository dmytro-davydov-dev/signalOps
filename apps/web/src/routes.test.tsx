import { screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { renderRoute } from "./test/renderRoute";

describe("routes", () => {
  it("renders the layout and overview at /", () => {
    renderRoute("/");
    expect(screen.getByRole("navigation", { name: "Main" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "SignalOps" })).toBeInTheDocument();
  });

  it("renders a not-found page inside the layout for unknown paths", () => {
    renderRoute("/no-such-page");
    expect(screen.getByRole("heading", { name: "Page not found" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Main" })).toBeInTheDocument();
  });
});
