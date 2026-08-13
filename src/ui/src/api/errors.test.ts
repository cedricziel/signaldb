import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildErrorGroupDoc,
  buildErrorGroupVolumeDoc,
  buildErrorOccurrencesDoc,
  fetchErrorGroupVolume,
  fetchErrorGroups,
  fetchErrorOccurrences,
  type ErrorGroup,
} from "./errors";
import { client } from "./gen/client.gen";

beforeEach(() => {
  client.setConfig({ baseUrl: "http://localhost" });
});

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function tableResponse(rows: unknown[][]) {
  return {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [],
    rows,
  };
}

describe("buildErrorGroupDoc", () => {
  it("groups spans with a captured exception by type/message/service/escaped", () => {
    const doc = buildErrorGroupDoc("traces", range);
    expect(doc.from).toBe("traces");
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.type", op: "exists" },
    });
    expect(doc.pipeline).toContainEqual({
      aggregate: {
        by: [
          "exception.type",
          "exception.message",
          "service.name",
          "exception.escaped",
        ],
        aggs: [
          { fn: "count", as: "n" },
          { fn: "min", of: "start_time_unix_nano", as: "first" },
          { fn: "max", of: "start_time_unix_nano", as: "last" },
        ],
      },
    });
  });

  it("uses the logs timestamp field for the logs source", () => {
    const doc = buildErrorGroupDoc("logs", range);
    expect(doc.from).toBe("logs");
    expect(doc.pipeline).toContainEqual({
      aggregate: {
        by: [
          "exception.type",
          "exception.message",
          "service.name",
          "exception.escaped",
        ],
        aggs: [
          { fn: "count", as: "n" },
          { fn: "min", of: "timestamp", as: "first" },
          { fn: "max", of: "timestamp", as: "last" },
        ],
      },
    });
  });
});

describe("fetchErrorGroups", () => {
  it("combines traces and logs exception groups, ranked by count", async () => {
    let call = 0;
    const fetchMock = vi.fn().mockImplementation(() => {
      call += 1;
      // First call: traces. Second call: logs.
      const body =
        call === 1
          ? tableResponse([
              ["std::io::Error", "boom", "signaldb", "true", 3, 1000, 2000],
            ])
          : tableResponse([
              ["ValueError", "bad input", "signaldb-ui", null, 9, 500, 1500],
            ]);
      return Promise.resolve(jsonResponse(body));
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await fetchErrorGroups(range);
    expect(result.groups).toEqual([
      {
        source: "logs",
        exceptionType: "ValueError",
        exceptionMessage: "bad input",
        serviceName: "signaldb-ui",
        escaped: null,
        count: 9,
        firstNs: "500",
        lastNs: "1500",
      },
      {
        source: "traces",
        exceptionType: "std::io::Error",
        exceptionMessage: "boom",
        serviceName: "signaldb",
        escaped: "true",
        count: 3,
        firstNs: "1000",
        lastNs: "2000",
      },
    ]);
    expect(result.truncated).toBe(false);
  });
});

describe("buildErrorOccurrencesDoc", () => {
  it("pins the lookup to the exact group's type/message/service/escaped, newest first", () => {
    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      escaped: "true",
      count: 3,
      firstNs: "1000",
      lastNs: "2000",
    };
    const doc = buildErrorOccurrencesDoc(group, range);
    expect(doc.fields).toEqual([
      "start_time_unix_nano",
      "trace_id",
      "exception.stacktrace",
    ]);
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.type", op: "eq", value: "std::io::Error" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.message", op: "eq", value: "boom" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "service.name", op: "eq", value: "signaldb" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.escaped", op: "eq", value: "true" },
    });
    expect(doc.pipeline).toContainEqual({
      order: [{ of: "start_time_unix_nano", dir: "desc" }],
    });
    expect(doc.pipeline).toContainEqual({ limit: 25 });
  });

  it("uses the logs timestamp field for the logs source", () => {
    const group: ErrorGroup = {
      source: "logs",
      exceptionType: "ValueError",
      exceptionMessage: "x",
      serviceName: "x",
      escaped: "true",
      count: 1,
      firstNs: "1000",
      lastNs: "1000",
    };
    const doc = buildErrorOccurrencesDoc(group, range);
    expect(doc.fields).toEqual([
      "timestamp",
      "trace_id",
      "exception.stacktrace",
    ]);
    expect(doc.pipeline).toContainEqual({
      order: [{ of: "timestamp", dir: "desc" }],
    });
  });

  it("pins an absent dimension to not-exists rather than leaving it unconstrained", () => {
    // A group with no service/message/escaped must not silently match
    // occurrences from a *different* service/message/escaped state.
    const group: ErrorGroup = {
      source: "logs",
      exceptionType: "ValueError",
      exceptionMessage: null,
      serviceName: null,
      escaped: null,
      count: 1,
      firstNs: "1000",
      lastNs: "1000",
    };
    const doc = buildErrorOccurrencesDoc(group, range);
    expect(doc.pipeline).toContainEqual({
      where: { not: { field: "exception.message", op: "exists" } },
    });
    expect(doc.pipeline).toContainEqual({
      where: { not: { field: "service.name", op: "exists" } },
    });
    expect(doc.pipeline).toContainEqual({
      where: { not: { field: "exception.escaped", op: "exists" } },
    });
  });
});

describe("fetchErrorOccurrences", () => {
  it("decodes each occurrence's timestamp, trace id, and stacktrace", async () => {
    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        jsonResponse({
          result: "rows",
          window: { start_ns: 0, end_ns: 0 },
          columns: [],
          rows: [
            ["2000", "abc123", "at foo\n at bar"],
            // A logs-sourced occurrence with no active trace.
            ["1000", null, "at baz"],
          ],
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      escaped: "true",
      count: 3,
      firstNs: "1000",
      lastNs: "2000",
    };
    const occurrences = await fetchErrorOccurrences(group, range);
    expect(occurrences).toEqual([
      { timestampNs: "2000", traceId: "abc123", stacktrace: "at foo\n at bar" },
      { timestampNs: "1000", traceId: null, stacktrace: "at baz" },
    ]);
  });

  it("returns an empty list when no occurrences are found", async () => {
    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        jsonResponse({
          result: "rows",
          window: { start_ns: 0, end_ns: 0 },
          columns: [],
          rows: [],
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const group: ErrorGroup = {
      source: "logs",
      exceptionType: "ValueError",
      exceptionMessage: null,
      serviceName: null,
      escaped: null,
      count: 1,
      firstNs: "1000",
      lastNs: "1000",
    };
    const occurrences = await fetchErrorOccurrences(group, range);
    expect(occurrences).toEqual([]);
  });
});

describe("buildErrorGroupVolumeDoc", () => {
  it("pins the lookup to the exact group and step-buckets a count series", () => {
    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      escaped: "true",
      count: 3,
      firstNs: "1000",
      lastNs: "2000",
    };
    const doc = buildErrorGroupVolumeDoc(group, range, "1m");
    expect(doc.result).toBe("series");
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.type", op: "eq", value: "std::io::Error" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.escaped", op: "eq", value: "true" },
    });
    expect(doc.pipeline).toContainEqual({
      aggregate: {
        by: ["exception.type"],
        aggs: [{ fn: "count", as: "n" }],
        step: "1m",
      },
    });
  });
});

describe("fetchErrorGroupVolume", () => {
  it("converts a series envelope to millisecond-keyed points", async () => {
    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        jsonResponse({
          result: "series",
          window: { start_ns: 0, end_ns: 0 },
          step_ns: 60_000_000_000,
          series: [
            {
              labels: { exception_type: "std::io::Error" },
              points: [
                [60_000_000_000, 2],
                [120_000_000_000, 5],
              ],
            },
          ],
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      escaped: null,
      count: 7,
      firstNs: "1000",
      lastNs: "2000",
    };
    const series = await fetchErrorGroupVolume(group, range, "1m");
    expect(series).toHaveLength(1);
    expect(series[0]!.points).toEqual([
      [60_000, 2],
      [120_000, 5],
    ]);
  });
});
