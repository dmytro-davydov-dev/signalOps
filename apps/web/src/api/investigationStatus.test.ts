import { describe, expect, it } from "vitest";

import { INVESTIGATION_STATUSES } from "./generated/investigationStatus";
import { describeInvestigationStatus, isInvestigationStatus } from "./investigationStatus";

describe("describeInvestigationStatus", () => {
  it("shows a completed investigation as diagnosed", () => {
    expect(describeInvestigationStatus("completed")).toEqual({
      label: "Diagnosed",
      tone: "success",
      terminal: true,
      diagnosis: "diagnosed",
    });
  });

  it("shows insufficient evidence as an abstention, not a failure", () => {
    const view = describeInvestigationStatus("insufficient_evidence");
    expect(view.diagnosis).toBe("insufficient_evidence");
    expect(view.tone).toBe("abstained");
    expect(view.terminal).toBe(true);
  });

  it("expects no diagnosis for other statuses", () => {
    for (const status of INVESTIGATION_STATUSES) {
      if (status === "completed" || status === "insufficient_evidence") continue;
      expect(describeInvestigationStatus(status).diagnosis).toBeNull();
    }
  });

  it("labels every status distinctly", () => {
    const labels = INVESTIGATION_STATUSES.map((status) => describeInvestigationStatus(status).label);
    expect(new Set(labels).size).toBe(INVESTIGATION_STATUSES.length);
  });

  it("recognizes only known statuses", () => {
    expect(isInvestigationStatus("running")).toBe(true);
    expect(isInvestigationStatus("diagnosed")).toBe(false);
    expect(isInvestigationStatus(undefined)).toBe(false);
  });
});
