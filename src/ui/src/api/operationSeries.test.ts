import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildOperationSeriesDoc,
  fetchOperationSeries,
} from "./operationSeries";
import { runIrQuery } from "./queryIr";
import type { EntityTypeDef } from "../features/catalog/entityTypes";
import type { EntityPin } from "./catalog";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

const service: EntityTypeDef = {
  id: "service",
  label: "Services",
  singular: "service",
  identity: ["service.name"],
  spanKindScope: "Server",
};

const pinned: EntityPin[] = [{ field: "service.name", value: "checkout" }];

describe("buildOperationSeriesDoc", () => {
  it("groups a single stepped series by the breakdown field", () => {
    const doc = buildOperationSeriesDoc(
      service,
      "span.name",
      range,
      pinned,
      60,
    );
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "series",
      pipeline: [
        { where: { field: "span_kind", op: "eq", value: "Server" } },
        { where: { field: "service.name", op: "eq", value: "checkout" } },
        {
          aggregate: {
            by: ["span.name"],
            aggs: [{ fn: "count", as: "n" }],
            step: "60s",
          },
        },
      ],
    });
  });
});

describe("fetchOperationSeries", () => {
  it("issues one query and decodes it into a map keyed by operation", async () => {
    vi.mocked(runIrQuery).mockResolvedValueOnce({
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: [
        {
          labels: { span_name: "GET /cart" },
          points: [
            [1_000_000_000_000, 5],
            [1_060_000_000_000, 8],
          ],
        },
        {
          labels: { span_name: "POST /checkout" },
          points: [[1_000_000_000_000, 2]],
        },
      ],
    });

    const byOperation = await fetchOperationSeries(
      service,
      "span.name",
      range,
      pinned,
      60,
    );

    expect(runIrQuery).toHaveBeenCalledTimes(1);
    expect(byOperation).toEqual(
      new Map([
        [
          "GET /cart",
          [
            { tMs: 1_000_000, value: 5 },
            { tMs: 1_060_000, value: 8 },
          ],
        ],
        ["POST /checkout", [{ tMs: 1_000_000, value: 2 }]],
      ]),
    );
  });

  it("falls back to a not-set label for a series missing the breakdown label", async () => {
    vi.mocked(runIrQuery).mockResolvedValueOnce({
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: [{ labels: {}, points: [[1_000_000_000_000, 3]] }],
    });

    const byOperation = await fetchOperationSeries(
      service,
      "span.name",
      range,
      pinned,
      60,
    );

    expect(byOperation.get("(not set)")).toEqual([
      { tMs: 1_000_000, value: 3 },
    ]);
  });

  it("decodes to an empty map for an empty result", async () => {
    vi.mocked(runIrQuery).mockResolvedValueOnce({
      result: "series",
      window: { start_ns: 0, end_ns: 0 },
      series: [],
    });

    const byOperation = await fetchOperationSeries(
      service,
      "span.name",
      range,
      pinned,
      60,
    );

    expect(byOperation.size).toBe(0);
  });
});
