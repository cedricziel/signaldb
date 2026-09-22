import { describe, expect, it } from "vitest";
import {
  buildLogRowsDoc,
  buildLogVolumeDoc,
  runLogRows,
  runLogVolume,
} from "./logs";
import type { LabelFilter } from "../../lib/filters";
import type { QueryIrResponse } from "../gen";
import { resetApiClient, stubApiFetch } from "../../test/apiClient";
import { afterEach } from "vitest";

const RANGE = { fromMs: 1_000, toMs: 4_600 };

afterEach(resetApiClient);

describe("buildLogRowsDoc", () => {
  it("builds a rows document ordered newest-first with the row limit", () => {
    expect(buildLogRowsDoc([], "", RANGE, 100)).toEqual({
      irVersion: 1,
      from: "logs",
      range: { from: "1000000000", to: "4600000000" },
      result: "rows",
      fields: [
        "timestamp",
        "body",
        "service.name",
        "severity_text",
        "trace_id",
        "span_id",
        "scope.name",
        "log.attributes",
        "scope.attributes",
        "resource.attributes",
      ],
      pipeline: [{ order: [{ of: "timestamp", dir: "desc" }] }, { limit: 100 }],
    });
  });

  it("canonicalizes level/service_name chips and maps operators", () => {
    const filters: LabelFilter[] = [
      { label: "level", op: "=", value: "error" },
      { label: "service_name", op: "!=", value: "checkout" },
      { label: "host", op: "=~", value: "worker-.*" },
      { label: "az", op: "!~", value: "eu-central-1a" },
    ];
    const doc = buildLogRowsDoc(filters, "", RANGE, 100);
    expect(doc.pipeline?.slice(0, 4)).toEqual([
      { where: { field: "severity_text", op: "eq", value: "error" } },
      { where: { field: "service.name", op: "ne", value: "checkout" } },
      { where: { field: "host", op: "regex", value: "worker-.*" } },
      {
        where: {
          not: { field: "az", op: "regex", value: "eu-central-1a" },
        },
      },
    ]);
  });

  it("adds a body-contains predicate for the search box", () => {
    const doc = buildLogRowsDoc([], "boom", RANGE, 50);
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "body", op: "contains", value: "boom" },
    });
  });
});

describe("buildLogVolumeDoc", () => {
  it("aggregates by severity_text with the given step", () => {
    expect(buildLogVolumeDoc([], "", RANGE, "60s")).toEqual({
      irVersion: 1,
      from: "logs",
      range: { from: "1000000000", to: "4600000000" },
      result: "series",
      pipeline: [
        {
          aggregate: {
            by: ["severity_text"],
            aggs: [{ fn: "count", as: "count" }],
            step: "60s",
          },
        },
      ],
    });
  });
});

describe("runLogRows", () => {
  it("maps rows to LogRow, splitting attribute containers by scope", async () => {
    stubApiFetch({
      result: "rows",
      window: { start_ns: 0, end_ns: 1 },
      columns: [
        { name: "timestamp", type: "timestamp_ns" },
        { name: "body", type: "string" },
        { name: "service_name", type: "string" },
        { name: "severity_text", type: "string" },
        { name: "trace_id", type: "string" },
        { name: "span_id", type: "string" },
        { name: "scope_name", type: "string" },
        { name: "log_attributes", type: "map" },
        { name: "scope_attributes", type: "map" },
        { name: "resource_attributes", type: "map" },
      ],
      rows: [
        [
          "3000000000",
          "boom",
          "checkout",
          "ERROR",
          "abc123",
          "",
          "otel-lib",
          { "http.method": "GET" },
          { "otel.scope.name": "otel-lib" },
          { "service.name": "checkout", "service.name.x": "dup" },
        ],
      ],
    } satisfies QueryIrResponse);

    const rows = await runLogRows([], "", RANGE, 100);
    expect(rows).toEqual([
      {
        tsNs: "3000000000",
        tsMs: 3000,
        body: "boom",
        serviceName: "checkout",
        severityText: "ERROR",
        traceId: "abc123",
        spanId: null,
        scopeName: "otel-lib",
        logAttributes: { "http.method": "GET" },
        scopeAttributes: { "otel.scope.name": "otel-lib" },
        resourceAttributes: {
          "service.name": "checkout",
          "service.name.x": "dup",
        },
      },
    ]);
  });
});

describe("runLogVolume", () => {
  it("maps series to per-severity points in milliseconds", async () => {
    stubApiFetch({
      result: "series",
      window: { start_ns: 0, end_ns: 1 },
      series: [
        {
          labels: { severity_text: "ERROR" },
          points: [
            [1_700_000_000, 3],
            [1_760_000_000, "5"],
          ],
        },
        { labels: {}, points: [[1_700_000_000, 10]] },
      ],
    } satisfies QueryIrResponse);

    const series = await runLogVolume([], "", RANGE, "60s");
    expect(series).toEqual([
      {
        level: "ERROR",
        points: [
          [1_700, 3],
          [1_760, 5],
        ],
      },
      { level: "unknown", points: [[1_700, 10]] },
    ]);
  });
});
