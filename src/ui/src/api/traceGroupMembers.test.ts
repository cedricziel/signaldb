import { describe, expect, it } from "vitest";
import { ROOT_SPAN_SENTINEL } from "./traceGroups";
import { buildMembersDoc, membersFromIrResponse } from "./traceGroupMembers";

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

describe("buildMembersDoc", () => {
  it("pins each dimension value as a where equality, ordered newest first", () => {
    const doc = buildMembersDoc(
      ["span.name", "service.name"],
      ["POST /api/checkout", "gateway"],
      range,
      [],
      "spans",
      500,
    );
    expect(doc).toEqual({
      irVersion: 1,
      from: "traces",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "rows",
      pipeline: [
        {
          where: { field: "span.name", op: "eq", value: "POST /api/checkout" },
        },
        { where: { field: "service.name", op: "eq", value: "gateway" } },
        { order: [{ of: "start_time_unix_nano", dir: "desc" }] },
        { limit: 500 },
      ],
    });
  });

  it("compiles a null dimension value to a negated exists, not eq null", () => {
    const doc = buildMembersDoc(
      ["resource.env"],
      [null],
      range,
      [],
      "spans",
      500,
    );
    expect(doc.pipeline?.[0]).toEqual({
      where: { not: { field: "resource.env", op: "exists" } },
    });
  });

  it("scopes to root spans at trace grain, before the dimension pins", () => {
    const doc = buildMembersDoc(
      ["span.name"],
      ["GET /health"],
      range,
      [],
      "traces",
      500,
    );
    expect(doc.pipeline?.[0]).toEqual({
      where: {
        field: "parent_span_id",
        op: "eq",
        value: ROOT_SPAN_SENTINEL,
      },
    });
    expect(doc.pipeline?.[1]).toEqual({
      where: { field: "span.name", op: "eq", value: "GET /health" },
    });
  });

  it("omits the root predicate at span grain", () => {
    const doc = buildMembersDoc(
      ["span.name"],
      ["GET /health"],
      range,
      [],
      "spans",
      500,
    );
    expect(JSON.stringify(doc.pipeline)).not.toContain("parent_span_id");
  });

  it("carries the active facet filters through, same as the group query", () => {
    const doc = buildMembersDoc(
      ["span.name"],
      ["GET /health"],
      range,
      [{ field: "service.name", value: "checkout" }],
      "spans",
      500,
    );
    expect(doc.pipeline?.[0]).toEqual({
      where: { field: "service.name", op: "eq", value: "checkout" },
    });
  });

  it("orders newest first and applies the given limit", () => {
    const doc = buildMembersDoc(["span.name"], ["a"], range, [], "spans", 25);
    expect(doc.pipeline?.at(-2)).toEqual({
      order: [{ of: "start_time_unix_nano", dir: "desc" }],
    });
    expect(doc.pipeline?.at(-1)).toEqual({ limit: 25 });
  });
});

describe("membersFromIrResponse", () => {
  /** A `rows` envelope shaped as the server returns it: physical names. */
  const rows = (data: (string | number | null)[][]) => ({
    result: "rows",
    window: { start_ns: 1, end_ns: 2 },
    columns: [
      { name: "trace_id", type: "string" },
      { name: "span_id", type: "string" },
      { name: "parent_span_id", type: "string" },
      { name: "span_name", type: "string" },
      { name: "service_name", type: "string" },
      { name: "start_time_unix_nano", type: "int64" },
      { name: "duration_nanos", type: "int64" },
      { name: "status_code", type: "string" },
    ],
    rows: data,
  });

  it("reads the eight-column projection positionally", () => {
    const members = membersFromIrResponse(
      rows([
        [
          "t1",
          "s1",
          "root",
          "POST /checkout",
          "gateway",
          1_700_000_000_000_000_000,
          412_000_000,
          "Ok",
        ],
      ]),
    );
    expect(members).toEqual([
      {
        traceId: "t1",
        spanId: "s1",
        parentSpanId: "root",
        spanName: "POST /checkout",
        serviceName: "gateway",
        startNs: "1700000000000000000",
        durationNanos: "412000000",
        statusCode: "Ok",
      },
    ]);
  });

  it("reads a root span's null parent as null, not the string 'null'", () => {
    const members = membersFromIrResponse(
      rows([["t1", "s1", null, "POST /checkout", "gateway", 1, 1, "Ok"]]),
    );
    expect(members[0]!.parentSpanId).toBeNull();
  });

  it("tolerates an empty result", () => {
    expect(membersFromIrResponse(rows([]))).toEqual([]);
  });
});
