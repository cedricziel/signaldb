import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { clientWrapper } from "../../test/render";
import * as entityDetailStatsApi from "../../api/entityDetailStats";
import type { EntityKpis } from "../../api/entityDetailStats";
import type { EntityTypeDef } from "./entityTypes";
import { useEntityKpis } from "./useEntityKpis";

vi.mock("../../api/entityDetailStats", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entityDetailStats")>();
  return { ...actual, fetchEntityKpis: vi.fn() };
});

const fetchEntityKpis = vi.mocked(entityDetailStatsApi.fetchEntityKpis);

afterEach(() => {
  vi.clearAllMocks();
});

const service: EntityTypeDef = {
  id: "service",
  label: "Services",
  singular: "service",
  identity: ["service.name"],
  spanKindScope: "Server",
};

const range = { fromMs: 1_000_000, toMs: 4_600_000 };
const pinned = [{ field: "service.name", value: "checkout" }];

const kpis: EntityKpis = {
  current: {
    count: 10,
    ratePerSec: 1,
    errorRate: 0,
    p50Ms: 1,
    p95Ms: 2,
    p99Ms: 3,
    peakRatePerSec: 2,
    lastNs: "1",
  },
  series: { rate: [], errorRate: [], p95: [] },
};

describe("useEntityKpis", () => {
  it("fetches with a step sized for ~30 buckets and returns the result", async () => {
    fetchEntityKpis.mockResolvedValue(kpis);

    const { result } = renderHook(
      () => useEntityKpis(service, range, "rangeKey", pinned),
      { wrapper: clientWrapper() },
    );

    await waitFor(() => expect(result.current.data).toEqual(kpis));

    expect(fetchEntityKpis).toHaveBeenCalledTimes(1);
    const [entityArg, rangeArg, pinnedArg, stepArg] =
      fetchEntityKpis.mock.calls[0]!;
    expect(entityArg).toBe(service);
    expect(rangeArg).toBe(range);
    expect(pinnedArg).toBe(pinned);
    expect(typeof stepArg).toBe("number");
    expect(stepArg).toBeGreaterThan(0);
  });

  it("refetches when the range key changes", async () => {
    fetchEntityKpis.mockResolvedValue(kpis);

    const { rerender } = renderHook(
      ({ rangeKey }) => useEntityKpis(service, range, rangeKey, pinned),
      { wrapper: clientWrapper(), initialProps: { rangeKey: "a" } },
    );
    await waitFor(() => expect(fetchEntityKpis).toHaveBeenCalledTimes(1));

    rerender({ rangeKey: "b" });
    await waitFor(() => expect(fetchEntityKpis).toHaveBeenCalledTimes(2));
  });
});
