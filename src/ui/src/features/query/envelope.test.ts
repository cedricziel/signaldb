import { describe, expect, it } from "vitest";

import { viewForResult } from "./envelope";

describe("viewForResult", () => {
  // Task 9.2 — the view is chosen from the declared envelope.
  it("maps each envelope to its view", () => {
    expect(viewForResult("rows")).toBe("list");
    expect(viewForResult("series")).toBe("chart");
    expect(viewForResult("table")).toBe("table");
  });

  it("defaults unknown envelopes to a list", () => {
    expect(viewForResult("mystery")).toBe("list");
  });
});
