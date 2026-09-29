import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  BACKEND_CAUSE_WINDOW_MS,
  buildBackendCauseRequestsDoc,
  buildRumErrorGroupsDoc,
  errorGroupKey,
  errorGroupsFromResponse,
  failedRequestsFromResponse,
  joinBackendCause,
  toErrorsPageGroup,
  type RumErrorGroup,
  type RumFailedRequest,
} from "./rumErrorGroups";
import { resolveRange } from "../lib/time";

const range = resolveRange(
  { type: "relative", seconds: 3600 },
  1_700_000_000_000,
);

function table(rows: unknown[][]): QueryIrResponse {
  return { result: "table", rows } as QueryIrResponse;
}

function rowsResponse(columns: string[], rows: unknown[][]): QueryIrResponse {
  return {
    result: "rows",
    columns: columns.map((name) => ({ name })),
    rows,
  } as unknown as QueryIrResponse;
}

describe("buildRumErrorGroupsDoc", () => {
  it("scopes to the app and to records that carry an exception", () => {
    const doc = buildRumErrorGroupsDoc("storefront-web", range) as {
      pipeline: unknown[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "storefront-web" },
    });
    expect(doc.pipeline[1]).toEqual({
      where: { field: "exception.type", op: "exists" },
    });
  });

  it("groups by type/message/escaped, not service — already pinned", () => {
    const doc = buildRumErrorGroupsDoc("storefront-web", range) as {
      pipeline: { aggregate?: { by: string[] } }[];
    };
    const agg = doc.pipeline.find((s) => s.aggregate)!.aggregate!;
    expect(agg.by).toEqual([
      "exception.type",
      "exception.message",
      "exception.escaped",
    ]);
  });

  it("adds a scoped 'other version' count only when a current version is given", () => {
    const withVersion = buildRumErrorGroupsDoc(
      "storefront-web",
      range,
      "2026.09.26-3",
    ) as { pipeline: { aggregate?: { aggs: unknown[] } }[] };
    const aggWith = withVersion.pipeline.find((s) => s.aggregate)!.aggregate!;
    expect(aggWith.aggs).toContainEqual({
      fn: "count",
      as: "other_version",
      where: {
        field: "resource.service.version",
        op: "ne",
        value: "2026.09.26-3",
      },
    });

    const withoutVersion = buildRumErrorGroupsDoc("storefront-web", range) as {
      pipeline: { aggregate?: { aggs: unknown[] } }[];
    };
    const aggWithout = withoutVersion.pipeline.find(
      (s) => s.aggregate,
    )!.aggregate!;
    expect(
      aggWithout.aggs.some(
        (a) => (a as { as?: string }).as === "other_version",
      ),
    ).toBe(false);
  });
});

describe("errorGroupsFromResponse", () => {
  it("decodes count, users, sessions and the latest session id", () => {
    const res = table([
      [
        "TypeError",
        "Cannot read properties of null",
        "false",
        42,
        1_700_000_000_000_000_000,
        1_700_000_100_000_000_000,
        "session-1",
        12,
        18,
        0,
      ],
    ]);
    const [group] = errorGroupsFromResponse(res);
    expect(group).toMatchObject({
      exceptionType: "TypeError",
      exceptionMessage: "Cannot read properties of null",
      escaped: "false",
      count: 42,
      lastSessionId: "session-1",
      users: 12,
      sessions: 18,
      newInCurrentRelease: true,
    });
  });

  it("marks a group with occurrences on another version as not new", () => {
    const res = table([
      [
        "TypeError",
        null,
        null,
        5,
        1_700_000_000_000_000_000,
        1_700_000_000_000_000_000,
        null,
        0,
        1,
        3,
      ],
    ]);
    expect(errorGroupsFromResponse(res)[0]!.newInCurrentRelease).toBe(false);
  });

  it("leaves newInCurrentRelease unknown when no version column is present", () => {
    const res = table([
      [
        "TypeError",
        null,
        null,
        5,
        1_700_000_000_000_000_000,
        1_700_000_000_000_000_000,
        null,
        0,
        1,
      ],
    ]);
    expect(
      errorGroupsFromResponse(res)[0]!.newInCurrentRelease,
    ).toBeUndefined();
  });
});

describe("buildBackendCauseRequestsDoc", () => {
  it("filters to the app's failed client spans within the given sessions", () => {
    const doc = buildBackendCauseRequestsDoc("storefront-web", range, [
      "session-1",
      "session-2",
    ]) as { pipeline: Record<string, unknown>[] };
    expect(doc.pipeline).toContainEqual({
      where: { field: "service.name", op: "eq", value: "storefront-web" },
    });
    expect(doc.pipeline).toContainEqual({
      where: { field: "span_kind", op: "eq", value: "Client" },
    });
    expect(doc.pipeline).toContainEqual({
      where: {
        field: "session.id",
        op: "in",
        value: ["session-1", "session-2"],
      },
    });
    expect(doc.pipeline).toContainEqual({
      where: {
        or: [
          { field: "status.code", op: "eq", value: "Error" },
          { field: "http.response.status_code", op: "gte", value: 400 },
        ],
      },
    });
  });
});

const REQUEST_COLUMNS = [
  "session_id",
  "start_time_unix_nano",
  "trace_id",
  "span_id",
  "http_request_method",
  "url_full",
  "http_response_status_code",
  "duration",
];

function requestRow(overrides: Partial<Record<string, unknown>> = {}) {
  const base: Record<string, unknown> = {
    session_id: "session-1",
    start_time_unix_nano: "1700000000000000000",
    trace_id: "trace-1",
    span_id: "span-1",
    http_request_method: "GET",
    url_full: "https://api.example.com/orders",
    http_response_status_code: 500,
    duration: "12000000",
  };
  const row = { ...base, ...overrides };
  return REQUEST_COLUMNS.map((c) => row[c]);
}

describe("failedRequestsFromResponse", () => {
  it("decodes a batch of failed requests across sessions", () => {
    const res = rowsResponse(REQUEST_COLUMNS, [
      requestRow(),
      requestRow({ session_id: "session-2", span_id: "span-2" }),
    ]);
    const decoded = failedRequestsFromResponse(res);
    expect(decoded).toHaveLength(2);
    expect(decoded[0]).toMatchObject({
      sessionId: "session-1",
      traceId: "trace-1",
      spanId: "span-1",
      method: "GET",
      urlFull: "https://api.example.com/orders",
      statusCode: 500,
    });
  });
});

const group: RumErrorGroup = {
  exceptionType: "TypeError",
  exceptionMessage: null,
  escaped: null,
  count: 3,
  firstMs: 1_000_000,
  lastMs: 1_010_000,
  lastSessionId: "session-1",
  users: 1,
  sessions: 1,
  newInCurrentRelease: undefined,
};

describe("joinBackendCause", () => {
  function request(
    overrides: Partial<RumFailedRequest> = {},
  ): RumFailedRequest {
    return {
      sessionId: "session-1",
      startMs: 1_005_000,
      traceId: "trace-1",
      spanId: "span-1",
      method: "GET",
      urlFull: "https://api.example.com/orders",
      statusCode: 500,
      durationNs: "12000000",
      ...overrides,
    };
  }

  it("attaches the latest failed request in the same session before the group's last event", () => {
    const older = request({ spanId: "older", startMs: 1_002_000 });
    const closer = request({ spanId: "closer", startMs: 1_005_000 });
    const [joined] = joinBackendCause([group], [older, closer]);
    expect(joined!.backendCause?.spanId).toBe("closer");
  });

  it("ignores a failed request in a different session", () => {
    const [joined] = joinBackendCause(
      [group],
      [request({ sessionId: "session-9" })],
    );
    expect(joined!.backendCause).toBeUndefined();
  });

  it("ignores a failed request that started after the group's last event", () => {
    const [joined] = joinBackendCause(
      [group],
      [request({ startMs: group.lastMs + 1 })],
    );
    expect(joined!.backendCause).toBeUndefined();
  });

  it("ignores a failed request outside the backend-cause window", () => {
    const tooEarly = request({
      startMs: group.lastMs - BACKEND_CAUSE_WINDOW_MS - 1,
    });
    const [joined] = joinBackendCause([group], [tooEarly]);
    expect(joined!.backendCause).toBeUndefined();
  });

  it("leaves a group with no session id untouched", () => {
    const [joined] = joinBackendCause(
      [{ ...group, lastSessionId: null }],
      [request()],
    );
    expect(joined!.backendCause).toBeUndefined();
  });
});

describe("errorGroupKey", () => {
  it("is stable for the same (type, message, escaped) and distinct otherwise", () => {
    const a: RumErrorGroup = { ...group, exceptionMessage: "boom" };
    const b: RumErrorGroup = { ...group, exceptionMessage: "boom" };
    const c: RumErrorGroup = { ...group, exceptionMessage: "other" };
    expect(errorGroupKey(a)).toBe(errorGroupKey(b));
    expect(errorGroupKey(a)).not.toBe(errorGroupKey(c));
  });

  it("keeps a null field distinct from the literal string NOT_SET", () => {
    expect(errorGroupKey({ ...group, exceptionMessage: null })).not.toBe(
      errorGroupKey({ ...group, exceptionMessage: "NOT_SET" }),
    );
  });

  it("is stable for a group with no message or escaped value", () => {
    const a: RumErrorGroup = {
      ...group,
      exceptionMessage: null,
      escaped: null,
    };
    const b: RumErrorGroup = {
      ...group,
      exceptionMessage: null,
      escaped: null,
    };
    expect(errorGroupKey(a)).toBe(errorGroupKey(b));
    expect(errorGroupKey(a)).not.toBe(
      errorGroupKey({ ...a, exceptionMessage: "boom" }),
    );
  });
});

describe("toErrorsPageGroup", () => {
  it("adapts a RUM group into api/errors.ts's ErrorGroup shape", () => {
    const adapted = toErrorsPageGroup(
      { ...group, exceptionMessage: "boom" },
      "storefront-web",
    );
    expect(adapted).toEqual({
      source: "logs",
      exceptionType: "TypeError",
      exceptionMessage: "boom",
      serviceName: "storefront-web",
      escaped: null,
      count: 3,
      firstNs: "1000000000000",
      lastNs: "1010000000000",
    });
  });
});
