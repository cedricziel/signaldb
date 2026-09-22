import { describe, expect, it } from "vitest";
import {
  buildFormulaIrDoc,
  buildMetricIrDoc,
  irSeriesToPromSeries,
  seriesName,
} from "./metrics";
import {
  emptyQuery,
  type MetricQuery,
} from "../../features/metrics/metricQuery";
import type { QueryIrResponse } from "../gen";

const range = { fromMs: 1_000, toMs: 4_600 };

describe("buildMetricIrDoc", () => {
  it("returns null when no metric is selected", () => {
    expect(buildMetricIrDoc(emptyQuery("a"), range, 60)).toBeNull();
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
      {
        where: { field: "service.name", op: "eq", value: "signaldb-writer" },
      },
      { where: { field: "region", op: "ne", value: "eu" } },
      { where: { field: "host", op: "regex", value: "worker-.*" } },
      {
        where: {
          not: { field: "az", op: "regex", value: "eu-central-1a" },
        },
      },
    ]);
  });

  it("drops a filter with an invalid label name rather than sending it", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "up",
      filters: [{ label: "bad label", op: "=", value: "x" }],
    };
    const doc = buildMetricIrDoc(q, range, 60);
    expect(doc?.pipeline?.length).toBe(2);
  });

  it("uses count with no `of` field for the count aggregate", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "up",
      agg: { op: "count", by: [] },
    };
    const doc = buildMetricIrDoc(q, range, 60);
    expect(doc?.pipeline?.[1]).toEqual({
      aggregate: { by: [], aggs: [{ fn: "count", as: "v" }], step: "60s" },
    });
  });

  it("compiles a group-by aggregate, aliasing service_name to service.name", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "up",
      agg: { op: "avg", by: ["service_name", "region"] },
    };
    const doc = buildMetricIrDoc(q, range, 60);
    expect(doc?.pipeline?.[1]).toEqual({
      aggregate: {
        by: ["service.name", "region"],
        aggs: [{ fn: "avg", of: "metric.value", as: "v" }],
        step: "60s",
      },
    });
  });

  it("compiles rate as the aggregate function (IR v6), replacing the space agg", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "http_requests_total",
      range: { fn: "rate" },
      agg: { op: "sum", by: ["service_name"] },
    };
    const doc = buildMetricIrDoc(q, range, 30);
    expect(doc?.pipeline?.[1]).toEqual({
      aggregate: {
        by: ["service.name"],
        aggs: [{ fn: "rate", of: "metric.value", as: "v" }],
        step: "30s",
      },
    });
  });

  it("compiles increase the same way", () => {
    const q: MetricQuery = {
      ...emptyQuery("a"),
      metric: "http_requests_total",
      range: { fn: "increase" },
    };
    const doc = buildMetricIrDoc(q, range, 30);
    expect(doc?.pipeline?.[1]).toEqual({
      aggregate: {
        by: [],
        aggs: [{ fn: "increase", of: "metric.value", as: "v" }],
        step: "30s",
      },
    });
  });
});

describe("buildFormulaIrDoc", () => {
  const qa: MetricQuery = { ...emptyQuery("a"), metric: "errors_total" };
  const qb: MetricQuery = { ...emptyQuery("b"), metric: "requests_total" };

  it("returns null for a blank formula", () => {
    expect(buildFormulaIrDoc([qa, qb], "", range, 60)).toBeNull();
    expect(buildFormulaIrDoc([qa, qb], "   ", range, 60)).toBeNull();
  });

  it("returns null when a query the formula could reference has no metric", () => {
    expect(
      buildFormulaIrDoc([qa, emptyQuery("b")], "a / b", range, 60),
    ).toBeNull();
  });

  it("builds a multi-query document keyed by ref letter", () => {
    const doc = buildFormulaIrDoc([qa, qb], "a / b", range, 60);
    expect(doc).toEqual({
      queries: {
        a: buildMetricIrDoc(qa, range, 60),
        b: buildMetricIrDoc(qb, range, 60),
      },
      formulas: [{ name: "formula", expr: "a / b" }],
      result: "series",
    });
  });
});

describe("seriesName", () => {
  it("returns the bare name with no labels", () => {
    expect(seriesName({ __name__: "up" })).toBe("up");
  });

  it("sorts label pairs and formats them Prometheus-style", () => {
    expect(seriesName({ __name__: "up", b: "2", a: "1" })).toBe(
      'up{a="1", b="2"}',
    );
  });
});

describe("irSeriesToPromSeries", () => {
  it("converts nanosecond timestamps to milliseconds", () => {
    const series: NonNullable<QueryIrResponse["series"]> = [
      {
        labels: { __name__: "up" },
        points: [["1000000000", 1]],
      },
    ];
    expect(irSeriesToPromSeries(series)).toEqual([
      { labels: { __name__: "up" }, points: [[1000, 1]] },
    ]);
  });
});
