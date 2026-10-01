import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildEntityCountSeriesDoc,
  buildEntityStatsDoc,
  fetchEntityKpis,
} from "./entityDetailStats";
import { runIrQuery } from "./queryIr";
import { ERROR_PATTERN } from "./traceGroups";
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

describe("buildEntityStatsDoc", () => {
  it("aggregates count/errors/percentiles/last, pinned and span-kind scoped", () => {
    const doc = buildEntityStatsDoc(service, range, pinned);
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "table",
      pipeline: [
        { where: { field: "span_kind", op: "eq", value: "Server" } },
        { where: { field: "service.name", op: "eq", value: "checkout" } },
        {
          aggregate: {
            by: ["service.name"],
            aggs: [
              { fn: "count", as: "n" },
              {
                fn: "count",
                as: "errors",
                where: {
                  field: "status.code",
                  op: "regex",
                  value: ERROR_PATTERN,
                },
              },
              { fn: "quantile", of: "duration", arg: 0.5, as: "p50" },
              { fn: "quantile", of: "duration", arg: 0.95, as: "p95" },
              { fn: "quantile", of: "duration", arg: 0.99, as: "p99" },
              { fn: "max", of: "start_time_unix_nano", as: "last" },
            ],
          },
        },
        { limit: 1 },
      ],
    });
  });

  it("pins a not-set value to an absence check", () => {
    const doc = buildEntityStatsDoc(service, range, [
      { field: "service.name", value: null },
    ]);
    expect(doc.pipeline?.[1]).toEqual({
      where: { not: { field: "service.name", op: "exists" } },
    });
  });
});

describe("buildEntityCountSeriesDoc", () => {
  it("asks for a stepped count series", () => {
    const doc = buildEntityCountSeriesDoc(service, range, pinned, 60, false);
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
            by: ["service.name"],
            aggs: [{ fn: "count", as: "n" }],
            step: "60s",
          },
        },
      ],
    });
  });

  it("scopes the count to errors when asked", () => {
    const doc = buildEntityCountSeriesDoc(service, range, pinned, 60, true);
    expect(doc.pipeline?.[2]).toEqual({
      aggregate: {
        by: ["service.name"],
        aggs: [
          {
            fn: "count",
            as: "n",
            where: {
              field: "status.code",
              op: "regex",
              value: ERROR_PATTERN,
            },
          },
        ],
        step: "60s",
      },
    });
  });
});

const statsTableResponse = (opts: {
  n: number;
  errors: number;
  p50: number;
  p95: number;
  p99: number;
  last: string;
}) => ({
  result: "table",
  window: { start_ns: 0, end_ns: 0 },
  rows: [
    ["checkout", opts.n, opts.errors, opts.p50, opts.p95, opts.p99, opts.last],
  ],
});

const countSeriesResponse = (points: [number, number][]) => ({
  result: "series",
  window: { start_ns: 0, end_ns: 0 },
  series: [{ labels: { service_name: "checkout" }, points }],
});

describe("fetchEntityKpis", () => {
  it("decodes current + previous stats, peak rate, and sparkline series", async () => {
    const mocked = vi.mocked(runIrQuery);
    mocked
      // current stats
      .mockResolvedValueOnce(
        statsTableResponse({
          n: 360,
          errors: 18,
          p50: 12_000_000,
          p95: 80_000_000,
          p99: 150_000_000,
          last: "4500000000000",
        }),
      )
      // current count series (for rate + peak)
      .mockResolvedValueOnce(
        countSeriesResponse([
          [1_000_000_000_000, 10],
          [1_060_000_000_000, 40],
        ]),
      )
      // current error series
      .mockResolvedValueOnce(
        countSeriesResponse([
          [1_000_000_000_000, 1],
          [1_060_000_000_000, 2],
        ]),
      )
      // current p95 series
      .mockResolvedValueOnce(
        countSeriesResponse([
          [1_000_000_000_000, 70_000_000],
          [1_060_000_000_000, 90_000_000],
        ]),
      )
      // previous stats
      .mockResolvedValueOnce(
        statsTableResponse({
          n: 300,
          errors: 30,
          p50: 10_000_000,
          p95: 75_000_000,
          p99: 140_000_000,
          last: "900000000000",
        }),
      )
      // previous count series (for peak)
      .mockResolvedValueOnce(
        countSeriesResponse([
          [-2_600_000_000_000, 20],
          [-2_540_000_000_000, 15],
        ]),
      );

    const kpis = await fetchEntityKpis(service, range, pinned, 60);

    expect(kpis.current).toEqual({
      count: 360,
      ratePerSec: 360 / 3600,
      errorRate: 0.05,
      p50Ms: 12,
      p95Ms: 80,
      p99Ms: 150,
      peakRatePerSec: 40 / 60,
      lastNs: "4500000000000",
    });
    expect(kpis.previous).toEqual({
      count: 300,
      ratePerSec: 300 / 3600,
      errorRate: 0.1,
      p50Ms: 10,
      p95Ms: 75,
      p99Ms: 140,
      peakRatePerSec: 20 / 60,
      lastNs: "900000000000",
    });
    expect(kpis.series.rate).toEqual([
      { tMs: 1_000_000, value: 10 / 60 },
      { tMs: 1_060_000, value: 40 / 60 },
    ]);
    expect(kpis.series.errorRate).toEqual([
      { tMs: 1_000_000, value: 0.1 },
      { tMs: 1_060_000, value: 0.05 },
    ]);
    expect(kpis.series.p95).toEqual([
      { tMs: 1_000_000, value: 70 },
      { tMs: 1_060_000, value: 90 },
    ]);

    // 4 current-window queries + 2 previous-window queries, not 1-per-row.
    expect(mocked).toHaveBeenCalledTimes(6);
  });

  it("reports no previous period when the shifted window has no rows", async () => {
    const mocked = vi.mocked(runIrQuery);
    mocked
      .mockResolvedValueOnce(
        statsTableResponse({
          n: 5,
          errors: 0,
          p50: 1_000_000,
          p95: 2_000_000,
          p99: 3_000_000,
          last: "4500000000000",
        }),
      )
      .mockResolvedValueOnce(countSeriesResponse([]))
      .mockResolvedValueOnce(countSeriesResponse([]))
      .mockResolvedValueOnce(countSeriesResponse([]))
      .mockResolvedValueOnce({
        result: "table",
        window: { start_ns: 0, end_ns: 0 },
        rows: [],
      })
      .mockResolvedValueOnce(countSeriesResponse([]));

    const kpis = await fetchEntityKpis(service, range, pinned, 60);

    expect(kpis.previous).toBeUndefined();
    expect(kpis.current?.count).toBe(5);
  });

  it("reports no current stats when the window is empty", async () => {
    const mocked = vi.mocked(runIrQuery);
    mocked.mockResolvedValue({
      result: "table",
      window: { start_ns: 0, end_ns: 0 },
      rows: [],
    });

    const kpis = await fetchEntityKpis(service, range, pinned, 60);

    expect(kpis.current).toBeUndefined();
    expect(kpis.previous).toBeUndefined();
    expect(kpis.series.rate).toEqual([]);
  });
});
