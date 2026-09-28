import { describe, expect, it } from "vitest";
import type { QueryIrResponse } from "./gen";
import {
  buildSessionLogsDoc,
  buildSessionSpansDoc,
  sessionDetailFromResponses,
  SESSION_DETAIL_CAP,
  type SessionEvent,
} from "./rumSessionDetail";
import { resolveRange } from "../lib/time";

const range = resolveRange(
  { type: "relative", seconds: 3600 },
  1_700_000_000_000,
);

function rowsResponse(columns: string[], rows: unknown[][]): QueryIrResponse {
  return {
    result: "rows",
    columns: columns.map((name) => ({ name })),
    rows,
  } as unknown as QueryIrResponse;
}

const SPAN_COLUMNS = [
  "trace_id",
  "span_id",
  "parent_span_id",
  "span_name",
  "span_kind",
  "service_name",
  "start_time_unix_nano",
  "duration_nanos",
  "status_code",
  "http_request_method",
  "url_full",
  "http_response_status_code",
];

const LOG_COLUMNS = [
  "timestamp",
  "event_name",
  "trace_id",
  "url_template",
  "url_full",
  "browser_web_vital_name",
  "browser_web_vital_rating",
  "browser_web_vital_value",
  "browser_css_selector",
  "browser_tag_name",
  "exception_type",
  "exception_message",
  "exception_stacktrace",
  "resource_attributes",
];

function spanRow(overrides: Partial<Record<string, unknown>> = {}): unknown[] {
  const base: Record<string, unknown> = {
    trace_id: "trace-1",
    span_id: "span-1",
    parent_span_id: null,
    span_name: "GET /api/checkout",
    span_kind: "Client",
    service_name: "storefront-web",
    start_time_unix_nano: "1700000000000000000",
    duration_nanos: "5000000",
    status_code: "Unset",
    http_request_method: "GET",
    url_full: "https://api.example.com/checkout",
    http_response_status_code: 200,
    ...overrides,
  };
  return SPAN_COLUMNS.map((c) => base[c] ?? null);
}

function logRow(overrides: Partial<Record<string, unknown>> = {}): unknown[] {
  const base: Record<string, unknown> = {
    timestamp: "1700000001000000000",
    event_name: "browser.navigation",
    trace_id: null,
    url_template: "/checkout",
    url_full: "https://shop.example.com/checkout",
    browser_web_vital_name: null,
    browser_web_vital_rating: null,
    browser_web_vital_value: null,
    browser_css_selector: null,
    browser_tag_name: null,
    exception_type: null,
    exception_message: null,
    exception_stacktrace: null,
    ...overrides,
  };
  return LOG_COLUMNS.map((c) => base[c] ?? null);
}

describe("buildSessionSpansDoc", () => {
  it("scopes to the session, orders ascending, and caps the page", () => {
    const doc = buildSessionSpansDoc("sess-1", range) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "session.id", op: "eq", value: "sess-1" },
    });
    expect(doc.pipeline[1]).toEqual({
      order: [{ of: "start_time_unix_nano", dir: "asc" }],
    });
    expect(doc.pipeline[2]).toEqual({ limit: SESSION_DETAIL_CAP + 1 });
  });

  it("adds a cursor stage when paginating", () => {
    const doc = buildSessionSpansDoc(
      "sess-1",
      range,
      "1700000000000000000",
    ) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[1]).toEqual({
      where: {
        field: "start_time_unix_nano",
        op: "gt",
        value: "1700000000000000000",
      },
    });
  });
});

describe("buildSessionLogsDoc", () => {
  it("scopes to the session and excludes browser.resource_timing", () => {
    const doc = buildSessionLogsDoc("sess-1", range) as {
      pipeline: Record<string, unknown>[];
    };
    expect(doc.pipeline[0]).toEqual({
      where: { field: "session.id", op: "eq", value: "sess-1" },
    });
    expect(doc.pipeline[1]).toEqual({
      where: {
        field: "event_name",
        op: "ne",
        value: "browser.resource_timing",
      },
    });
  });
});

describe("sessionDetailFromResponses", () => {
  it("merges spans and logs into one ascending, lane-classified list", () => {
    const spans = rowsResponse(SPAN_COLUMNS, [
      spanRow({ start_time_unix_nano: "1700000002000000000" }),
    ]);
    const logs = rowsResponse(LOG_COLUMNS, [
      logRow({ timestamp: "1700000001000000000" }),
    ]);
    const page = sessionDetailFromResponses(spans, logs);
    expect(page.events.map((e) => e.kind)).toEqual(["log", "span"]);
    expect(page.events[0]!.lane).toBe("views");
    expect(page.events[1]!.lane).toBe("network");
    expect(page.hasMore).toBe(false);
    expect(page.moreCount).toBeUndefined();
    expect(page.nextCursorNs).toBeUndefined();
  });

  it("classifies each log event_name into its lane", () => {
    const logs = rowsResponse(LOG_COLUMNS, [
      logRow({ timestamp: "1", event_name: "browser.navigation" }),
      logRow({ timestamp: "2", event_name: "browser.user_action.click" }),
      logRow({ timestamp: "3", event_name: "browser.web_vital" }),
      logRow({ timestamp: "4", event_name: "browser.navigation_timing" }),
      logRow({ timestamp: "5", event_name: "exception" }),
      logRow({ timestamp: "6", event_name: "some.other.event" }),
    ]);
    const page = sessionDetailFromResponses(
      rowsResponse(SPAN_COLUMNS, []),
      logs,
    );
    expect(page.events.map((e) => e.lane)).toEqual([
      "views",
      "actions",
      "perf",
      "perf",
      "errors",
      "logs",
    ]);
  });

  it("puts an error span in the Errors lane ahead of Network", () => {
    const spans = rowsResponse(SPAN_COLUMNS, [
      spanRow({
        start_time_unix_nano: "1",
        http_response_status_code: 502,
        status_code: "Unset",
      }),
      spanRow({
        start_time_unix_nano: "2",
        status_code: "Error",
        http_response_status_code: 200,
      }),
      spanRow({ start_time_unix_nano: "3" }),
    ]);
    const page = sessionDetailFromResponses(
      spans,
      rowsResponse(LOG_COLUMNS, []),
    );
    const events = page.events as SessionEvent[];
    expect(events.map((e) => e.lane)).toEqual(["errors", "errors", "network"]);
    expect(events[0]!.kind === "span" && events[0]!.isError).toBe(true);
    expect(events[2]!.kind === "span" && events[2]!.isError).toBe(false);
  });

  it("decodes a log event's resource attributes off the resource.attributes container", () => {
    const logs = rowsResponse(LOG_COLUMNS, [
      logRow({
        resource_attributes: JSON.stringify({
          "session.id": "sess-1",
          "user.id": "user-42",
        }),
      }),
    ]);
    const page = sessionDetailFromResponses(
      rowsResponse(SPAN_COLUMNS, []),
      logs,
    );
    const event = page.events[0]!;
    expect(event.kind === "log" && event.resourceAttributes).toEqual({
      "session.id": "sess-1",
      "user.id": "user-42",
    });
  });

  it("says how many more when neither source hit its own page limit", () => {
    // cap = 2 means each source was "asked for" 3 (cap + 1); 2 rows each
    // stays under that, so the merged total (4) is an exact count.
    const spans = rowsResponse(
      SPAN_COLUMNS,
      Array.from({ length: 2 }, (_, i) =>
        spanRow({ start_time_unix_nano: String(i + 1) }),
      ),
    );
    const logs = rowsResponse(
      LOG_COLUMNS,
      Array.from({ length: 2 }, (_, i) => logRow({ timestamp: String(i + 3) })),
    );
    const page = sessionDetailFromResponses(spans, logs, 2);
    expect(page.events).toHaveLength(2);
    expect(page.hasMore).toBe(true);
    expect(page.moreCount).toBe(2);
    expect(page.nextCursorNs).toBe("2");
  });

  it("reports 'more exist' with an unknown count when a source hit its own page limit", () => {
    const cap = 2;
    const spans = rowsResponse(
      SPAN_COLUMNS,
      Array.from({ length: cap + 1 }, (_, i) =>
        spanRow({ start_time_unix_nano: String(i + 1) }),
      ),
    );
    const page = sessionDetailFromResponses(
      spans,
      rowsResponse(LOG_COLUMNS, []),
      cap,
    );
    expect(page.hasMore).toBe(true);
    expect(page.moreCount).toBeUndefined();
  });
});
