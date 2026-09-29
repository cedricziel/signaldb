import { describe, expect, it, vi } from "vitest";
import type { QueryIrResponse } from "./gen";
import { runIrQuery } from "./queryIr";
import { fetchRumErrorGroupsWithBackendCause } from "./rumErrorGroups";
import { resolveRange } from "../lib/time";

vi.mock("./queryIr", async (orig) => ({
  ...(await orig<typeof import("./queryIr")>()),
  runIrQuery: vi.fn(),
}));

const range = resolveRange(
  { type: "relative", seconds: 3600 },
  1_700_000_000_000,
);

describe("fetchRumErrorGroupsWithBackendCause", () => {
  it("returns the groups without a cause when the backend-cause read fails", async () => {
    vi.mocked(runIrQuery)
      .mockResolvedValueOnce({
        result: "table",
        rows: [
          [
            "TypeError",
            null,
            null,
            5,
            1_700_000_000_000_000_000,
            1_700_000_000_000_000_000,
            "session-1",
            0,
            1,
          ],
        ],
      } as QueryIrResponse)
      .mockRejectedValueOnce(new Error("traces unavailable"));

    const groups = await fetchRumErrorGroupsWithBackendCause(
      "storefront-web",
      range,
    );
    expect(groups).toHaveLength(1);
    expect(groups[0]!.backendCause).toBeUndefined();
  });
});
