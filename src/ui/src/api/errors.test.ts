import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import {
  buildErrorGroupDoc,
  buildErrorExampleDoc,
  fetchErrorGroups,
  fetchErrorExample,
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
  it("groups spans with a captured exception by type/message/service", () => {
    const doc = buildErrorGroupDoc("traces", range);
    expect(doc.from).toBe("traces");
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.type", op: "exists" },
    });
    expect(doc.pipeline).toContainEqual({
      aggregate: {
        by: ["exception.type", "exception.message", "service.name"],
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
        by: ["exception.type", "exception.message", "service.name"],
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
              ["std::io::Error", "boom", "signaldb", 3, 1000, 2000],
            ])
          : tableResponse([
              ["ValueError", "bad input", "signaldb-ui", 9, 500, 1500],
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
        count: 9,
        firstNs: "500",
        lastNs: "1500",
      },
      {
        source: "traces",
        exceptionType: "std::io::Error",
        exceptionMessage: "boom",
        serviceName: "signaldb",
        count: 3,
        firstNs: "1000",
        lastNs: "2000",
      },
    ]);
    expect(result.truncated).toBe(false);
  });
});

describe("buildErrorExampleDoc", () => {
  it("pins the example lookup to the exact group's type/message/service", () => {
    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      count: 3,
      firstNs: "1000",
      lastNs: "2000",
    };
    const doc = buildErrorExampleDoc(group, range);
    expect(doc.fields).toEqual(["trace_id", "exception.stacktrace"]);
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.type", op: "eq", value: "std::io::Error" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "exception.message", op: "eq", value: "boom" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "service.name", op: "eq", value: "signaldb" },
    });
    expect(doc.pipeline).toContainEqual({ limit: 1 });
  });
});

describe("fetchErrorExample", () => {
  it("decodes the example row's trace id and stacktrace", async () => {
    const fetchMock = vi.fn().mockImplementation(() =>
      Promise.resolve(
        jsonResponse({
          result: "rows",
          window: { start_ns: 0, end_ns: 0 },
          columns: [],
          rows: [["abc123", "at foo\n at bar"]],
        }),
      ),
    );
    vi.stubGlobal("fetch", fetchMock);

    const group: ErrorGroup = {
      source: "traces",
      exceptionType: "std::io::Error",
      exceptionMessage: "boom",
      serviceName: "signaldb",
      count: 3,
      firstNs: "1000",
      lastNs: "2000",
    };
    const example = await fetchErrorExample(group, range);
    expect(example).toEqual({
      traceId: "abc123",
      stacktrace: "at foo\n at bar",
    });
  });

  it("returns null when no example row is found", async () => {
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
      count: 1,
      firstNs: "1000",
      lastNs: "1000",
    };
    const example = await fetchErrorExample(group, range);
    expect(example).toBeNull();
  });
});
