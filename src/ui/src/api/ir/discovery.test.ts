import { afterEach, describe, expect, it } from "vitest";
import { fields, metricNames, profileTypes, values } from "./discovery";
import type { QueryIrResponse } from "../gen";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";
import { stubFetchRoutes } from "../../test/render";

const RANGE = { fromMs: 1_000, toMs: 4_600 };

afterEach(resetApiClient);

describe("fields", () => {
  it("submits a describe:fields document and returns the field list", async () => {
    const calls = stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "fields",
        truncated: false,
        cost: {
          mode: "metadata",
          window_scoped: false,
          sampled: false,
          approximate: false,
        },
        fields: [
          {
            name: "service.name",
            type: "string",
            filterable: true,
            origin: "declared",
          },
        ],
      },
    } satisfies QueryIrResponse);

    const result = await fields("logs", RANGE);

    expect(result).toEqual([
      {
        name: "service.name",
        type: "string",
        filterable: true,
        origin: "declared",
      },
    ]);
    expect(calls[0]?.body).toEqual({
      irVersion: 4,
      from: "logs",
      range: { from: "1000000000", to: "4600000000" },
      result: "metadata",
      pipeline: [{ describe: { target: "fields" } }],
    });
  });

  it("returns an empty list when the response carries no metadata", async () => {
    stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
    } satisfies QueryIrResponse);
    expect(await fields("logs", RANGE)).toEqual([]);
  });
});

describe("values", () => {
  it("reads the top values from data when no statistics cover the field", async () => {
    // One stub answers both calls: the describe (empty, mode "none") and the
    // aggregate fallback (rows).
    const calls = stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "none",
          window_scoped: false,
          sampled: false,
          approximate: false,
        },
      },
      rows: [
        ["http.server.request.duration", 295],
        ["", 3],
      ],
    } as QueryIrResponse);

    const result = await values("metrics_histogram", "metric.name", RANGE);

    expect(result).toEqual([
      { value: "http.server.request.duration", partial: false },
    ]);
    expect(calls[1]?.body).toEqual({
      irVersion: 4,
      from: "metrics_histogram",
      range: { from: "1000000000", to: "4600000000" },
      result: "table",
      pipeline: [
        {
          aggregate: {
            by: ["metric.name"],
            aggs: [{ fn: "count", as: "n" }],
          },
        },
        { topk: { of: "n", n: 200 } },
      ],
    });
  });

  it("marks a declared, exact answer as not partial", async () => {
    stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "metadata",
          window_scoped: false,
          sampled: false,
          approximate: false,
        },
        values: [{ value: "Server", origin: "registry" }],
      },
    } satisfies QueryIrResponse);

    expect(await values("traces", "span.kind", RANGE)).toEqual([
      { value: "Server", partial: false },
    ]);
  });

  it("marks a statistics sketch or sampled scan as partial", async () => {
    stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "metadata",
          window_scoped: false,
          sampled: false,
          approximate: true,
        },
        values: [{ value: "/api/orders", count: 900, origin: "statistics" }],
      },
    } satisfies QueryIrResponse);

    expect(await values("traces", "http.route", RANGE)).toEqual([
      { value: "/api/orders", partial: true },
    ]);
  });

  it("passes a limit through when given", async () => {
    const calls = stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "metadata",
          window_scoped: false,
          sampled: false,
          approximate: false,
        },
        values: [],
      },
    } satisfies QueryIrResponse);

    await values("logs", "severity_text", RANGE, 50);
    expect(calls[0]?.body).toMatchObject({
      pipeline: [
        { describe: { target: "values", field: "severity_text", limit: 50 } },
      ],
    });
  });
});

describe("metricNames", () => {
  it("asks for metric.name on the metrics source", async () => {
    const calls = stubApiFetch({
      result: "metadata",
      window: { start_ns: 0, end_ns: 1 },
      metadata: {
        kind: "values",
        truncated: false,
        cost: {
          mode: "metadata",
          window_scoped: false,
          sampled: false,
          approximate: true,
        },
        values: [{ value: "http.server.duration", origin: "statistics" }],
      },
    } satisfies QueryIrResponse);

    expect(await metricNames(RANGE)).toEqual([
      { value: "http.server.duration", partial: true, chartable: true },
    ]);
    expect(calls[0]?.body).toMatchObject({
      from: "metrics",
      pipeline: [{ describe: { target: "values", field: "metric.name" } }],
    });
  });

  it("also asks metrics_histogram, since it's a separate IR source", async () => {
    const calls = stubApiFetch(
      valuesResponse([{ value: "http.server.duration", origin: "registry" }]),
    );
    await metricNames(RANGE);
    expect(
      calls.some((c) => (c.body as { from: string }).from === "metrics"),
    ).toBe(true);
    expect(
      calls.some(
        (c) => (c.body as { from: string }).from === "metrics_histogram",
      ),
    ).toBe(true);
  });

  it("unions scalar and histogram names, deduping and preferring an exact hit", async () => {
    stubFetchRoutes([
      {
        match: "/api/v1/query",
        bodyMatch: (b) => (b as { from?: string }).from === "metrics",
        body: valuesResponse(
          [
            { value: "shared_metric", origin: "statistics" },
            { value: "up", origin: "registry" },
          ],
          true,
        ),
      },
      {
        match: "/api/v1/query",
        bodyMatch: (b) => (b as { from?: string }).from === "metrics_histogram",
        body: valuesResponse([
          { value: "shared_metric", origin: "registry" },
          { value: "http.server.duration", origin: "registry" },
        ]),
      },
    ]);

    const result = await metricNames(RANGE);
    expect(result.map((v) => v.value).sort()).toEqual([
      "http.server.duration",
      "shared_metric",
      "up",
    ]);
    // The exact (registry) hit for the name both sources return wins over
    // the approximate one, and a name present in `metrics` is chartable
    // even though `metrics_histogram` also reports it.
    expect(result.find((v) => v.value === "shared_metric")).toEqual({
      value: "shared_metric",
      partial: false,
      chartable: true,
    });
    // Histogram-only: discoverable, but the builder can't chart it.
    expect(result.find((v) => v.value === "http.server.duration")).toEqual({
      value: "http.server.duration",
      partial: false,
      chartable: false,
    });
    expect(result.find((v) => v.value === "up")).toEqual({
      value: "up",
      partial: true,
      chartable: true,
    });
  });
});

function valuesResponse(
  values: Array<{ value: string; origin: "registry" | "statistics" }>,
  approximate = false,
): QueryIrResponse {
  return {
    result: "metadata",
    window: { start_ns: 0, end_ns: 1 },
    metadata: {
      kind: "values",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate,
      },
      values,
    },
  };
}

describe("profileTypes", () => {
  it("aggregates sample/period type and unit on the profiles source", async () => {
    const calls = stubApiFetch({
      result: "table",
      window: { start_ns: 0, end_ns: 1 },
      columns: [
        { name: "sample.type", type: "string" },
        { name: "sample.unit", type: "string" },
        { name: "period.type", type: "string" },
        { name: "period.unit", type: "string" },
        { name: "n", type: "int" },
      ],
      rows: [["cpu", "nanoseconds", "cpu", "nanoseconds", 42]],
    } satisfies QueryIrResponse);

    expect(await profileTypes(RANGE)).toEqual([
      {
        ID: "cpu:nanoseconds",
        name: "cpu",
        sampleType: "cpu",
        sampleUnit: "nanoseconds",
        periodType: "cpu",
        periodUnit: "nanoseconds",
      },
    ]);
    expect(calls[0]?.body).toMatchObject({
      from: "profiles",
      pipeline: [
        {
          aggregate: {
            by: ["sample.type", "sample.unit", "period.type", "period.unit"],
            aggs: [{ fn: "count", as: "n" }],
          },
        },
      ],
    });
  });
});
