import { afterEach, describe, expect, it } from "vitest";
import { fields, metricNames, profileTypes, values } from "./discovery";
import type { QueryIrResponse } from "../gen";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";

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
      { value: "http.server.duration", partial: true },
    ]);
    expect(calls[0]?.body).toMatchObject({
      from: "metrics",
      pipeline: [{ describe: { target: "values", field: "metric.name" } }],
    });
  });
});

describe("profileTypes", () => {
  it("asks for profile.type on the profiles source", async () => {
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
        values: [{ value: "cpu", origin: "registry" }],
      },
    } satisfies QueryIrResponse);

    expect(await profileTypes(RANGE)).toEqual([
      { value: "cpu", partial: false },
    ]);
    expect(calls[0]?.body).toMatchObject({
      from: "profiles",
      pipeline: [{ describe: { target: "values", field: "profile.type" } }],
    });
  });
});
