import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { clientWrapper } from "../../test/render";
import * as operationSeriesApi from "../../api/operationSeries";
import type { EntityTypeDef } from "./entityTypes";
import { useOperationSeries } from "./useOperationSeries";

vi.mock("../../api/operationSeries", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/operationSeries")>();
  return { ...actual, fetchOperationSeries: vi.fn() };
});

const fetchOperationSeries = vi.mocked(operationSeriesApi.fetchOperationSeries);

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

describe("useOperationSeries", () => {
  it("fetches once for the breakdown field and returns the map", async () => {
    const map = new Map([["GET /cart", [{ tMs: 1, value: 2 }]]]);
    fetchOperationSeries.mockResolvedValue(map);

    const { result } = renderHook(
      () => useOperationSeries(service, "span.name", range, "rangeKey", pinned),
      { wrapper: clientWrapper() },
    );

    await waitFor(() => expect(result.current.data).toEqual(map));
    expect(fetchOperationSeries).toHaveBeenCalledTimes(1);
  });

  it("is disabled when no breakdown field is given", () => {
    const { result } = renderHook(
      () => useOperationSeries(service, undefined, range, "rangeKey", pinned),
      { wrapper: clientWrapper() },
    );

    expect(result.current.fetchStatus).toBe("idle");
    expect(fetchOperationSeries).not.toHaveBeenCalled();
  });
});
