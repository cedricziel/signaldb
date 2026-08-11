import { describe, expect, it } from "vitest";
import { buildMetricIrDoc, irSeriesToPromSeries } from "./metricsIr";
import { emptyQuery, type MetricQuery } from "../features/metrics/buildPromQL";
import type { QueryIrResponse } from "./gen";

const range = { fromMs: 1_000, toMs: 4_600 };

describe("buildMetricIrDoc", () => {
  it("returns null when no metric is selected", () => {
    expect(buildMetricIrDoc(emptyQuery("a"), range, 60)).toBeNull();
  });

  it("returns null when a range function is set — no IR stage for rate/*_over_time", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "signaldb.wal.entries_processed",
      range: { fn: "rate", window: "5m" },
    };
    expect(buildMetricIrDoc(q, range, 60)).toBeNull();
  });

  it("compiles a bare metric name filter plus a sum aggregate", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "signaldb.wal.entries_processed",
    };
    expect(buildMetricIrDoc(q, range, 60)).toEqual({
      irVersion: 1,
      from: "metrics",
      range: { from: "1000000000", to: "4600000000" },
      result: "series",
      pipeline: [
        {
          where: {
            field: "metric.name",
            op: "eq",
            value: "signaldb.wal.entries_processed",
          },
        },
        {
          aggregate: {
            by: [],
            aggs: [{ fn: "sum", of: "metric.value", as: "v" }],
            step: "60s",
          },
        },
      ],
    });
  });

  it("compiles label filters onto where clauses, mapping the op", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "signaldb.wal.entries_processed",
      filters: [
        { label: "service_name", op: "=", value: "signaldb-writer" },
        { label: "region", op: "!=", value: "eu" },
        { label: "host", op: "=~", value: "worker-.*" },
        { label: "az", op: "!~", value: "eu-central-1a" },
      ],
    };
    const doc = buildMetricIrDoc(q, range, 60);
    expect(doc?.pipeline?.slice(1, 5)).toEqual([
      { where: { field: "service_name", op: "eq", value: "signaldb-writer" } },
      { where: { field: "region", op: "ne", value: "eu" } },
      { where: { field: "host", op: "regex", value: "worker-.*" } },
      {
        where: {
          not: { field: "az", op: "regex", value: "eu-central-1a" },
        },
      },
    ]);
  });

  it("omits `of` for a count aggregate — the IR rejects it otherwise", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "signaldb.wal.entries_processed",
      agg: { op: "count", by: ["service.name"] },
    };
    const doc = buildMetricIrDoc(q, range, 60);
    const aggStage = doc?.pipeline?.find(
      (s): s is { aggregate: { aggs: unknown[] } } => "aggregate" in s,
    );
    expect(aggStage?.aggregate.aggs).toEqual([{ fn: "count", as: "v" }]);
  });

  it("passes the space-agg's grouping through as the aggregate's `by`", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "signaldb.wal.entries_processed",
      agg: { op: "avg", by: ["service.name", "host.name"] },
    };
    const doc = buildMetricIrDoc(q, range, 60);
    const aggStage = doc?.pipeline?.find(
      (s): s is { aggregate: { by: unknown } } => "aggregate" in s,
    );
    expect(aggStage?.aggregate.by).toEqual(["service.name", "host.name"]);
  });
});

describe("irSeriesToPromSeries", () => {
  it("converts nanosecond timestamps to milliseconds and coerces values to numbers", () => {
    const res: QueryIrResponse = {
      result: "series",
      window: { start_ns: 0, end_ns: 1000 },
      series: [
        {
          labels: { service_name: "signaldb" },
          points: [
            [1_000_000_000, 5],
            [2_000_000_000, "7"],
          ],
        },
      ],
    };
    expect(irSeriesToPromSeries(res)).toEqual([
      {
        labels: { service_name: "signaldb" },
        points: [
          [1000, 5],
          [2000, 7],
        ],
      },
    ]);
  });

  it("returns an empty array when the response carries no series", () => {
    const res: QueryIrResponse = {
      result: "series",
      window: { start_ns: 0, end_ns: 1000 },
    };
    expect(irSeriesToPromSeries(res)).toEqual([]);
  });
});
