import { afterEach, describe, expect, it, vi } from "vitest";
import { client } from "./gen/client.gen";
import {
  buildTraceProfilesDoc,
  buildTraceSpansDoc,
  fetchTraceDetail,
  traceFromIrResponses,
} from "./traceDetail";

afterEach(() => {
  vi.unstubAllGlobals();
  client.setConfig({ baseUrl: "" });
});

const RANGE = { fromMs: 1_000_000, toMs: 2_000_000 };

const SPANS_RES = {
  result: "rows",
  window: { start_ns: 0, end_ns: 0 },
  columns: [
    { name: "trace_id", type: "string" },
    { name: "span_id", type: "string" },
    { name: "parent_span_id", type: "string" },
    { name: "span_name", type: "string" },
    { name: "service_name", type: "string" },
    { name: "status_code", type: "string" },
    { name: "status_message", type: "string" },
    { name: "start_time_unix_nano", type: "timestamp_ns" },
    { name: "duration", type: "duration_ns" },
    { name: "span_kind", type: "string" },
    { name: "span_attributes", type: "map<string,string>" },
    { name: "scope_attributes", type: "map<string,string>" },
    { name: "resource_attributes", type: "map<string,string>" },
    { name: "span_events", type: "string" },
  ],
  rows: [
    [
      "t1cafe",
      "root",
      null,
      "POST /api/checkout",
      "gateway",
      "OK",
      null,
      1_000_000_000,
      412_000_000,
      "Server",
      { "http.request.method": "POST" },
      {},
      { "service.version": "1.2.3", "service.namespace": "shop" },
      null,
    ],
    [
      "t1cafe",
      "charge",
      "root",
      "charge",
      "payments",
      "ERROR",
      "card declined",
      1_040_000_000,
      258_000_000,
      "Client",
      { "payment.provider": "stripe" },
      { "otel.library": "x" },
      // A legacy table returns the container as a JSON string.
      '{"service.version":"9.9.9"}',
      '[{"name":"exception","timestamp_unix_nano":1055000000,"attributes":{"exception.type":"PaymentError","exception.message":"card declined"}}]',
    ],
  ],
};

const PROFILES_RES = {
  result: "rows",
  window: { start_ns: 0, end_ns: 0 },
  columns: [
    { name: "profile_id", type: "string" },
    { name: "timestamp", type: "timestamp_ns" },
    { name: "duration_nano", type: "int64" },
    { name: "sample_type", type: "string" },
    { name: "sample_unit", type: "string" },
    { name: "period_type", type: "string" },
    { name: "period_unit", type: "string" },
    { name: "period", type: "int64" },
    { name: "service_name", type: "string" },
    { name: "trace_id", type: "string" },
    { name: "span_id", type: "string" },
  ],
  rows: [
    [
      "p1",
      1_050_000_000,
      10_000_000,
      "cpu",
      "nanoseconds",
      "cpu",
      "nanoseconds",
      10,
      "payments",
      "t1cafe",
      "charge",
    ],
  ],
};

describe("buildTraceSpansDoc", () => {
  it("asks the traces source for the span, its kind, containers, and events, scoped to the trace", () => {
    const doc = buildTraceSpansDoc("t1cafe", RANGE);
    expect(doc.from).toBe("traces");
    expect(doc.result).toBe("rows");
    expect(doc.range).toEqual({ from: "1000000000000", to: "2000000000000" });
    expect(doc.fields).toEqual([
      "trace_id",
      "span_id",
      "parent_span_id",
      "span.name",
      "service.name",
      "status.code",
      "status_message",
      "start_time_unix_nano",
      "duration",
      "span_kind",
      "span.attributes",
      "scope.attributes",
      "resource.attributes",
      "span_events",
    ]);
    expect(doc.pipeline).toEqual([
      { where: { field: "trace_id", op: "eq", value: "t1cafe" } },
    ]);
  });

  it("asks the profiles source for the trace's profiles", () => {
    const doc = buildTraceProfilesDoc("t1cafe", RANGE);
    expect(doc.from).toBe("profiles");
    expect(doc.pipeline).toEqual([
      { where: { field: "trace_id", op: "eq", value: "t1cafe" } },
    ]);
  });
});

describe("traceFromIrResponses", () => {
  it("builds the trace view: spans with flattened attributes, kinds, events, and profiles", () => {
    const trace = traceFromIrResponses("t1cafe", SPANS_RES, PROFILES_RES);
    expect(trace).toBeDefined();
    expect(trace!.traceId).toBe("t1cafe");
    expect(trace!.rootServiceName).toBe("gateway");
    expect(trace!.rootTraceName).toBe("POST /api/checkout");
    expect(trace!.startNs).toBe("1000000000");
    expect(trace!.durationMs).toBe(412);
    expect(trace!.rootError).toBe(false);
    expect(trace!.rootAttributes["resource.service.namespace"]).toBe("shop");

    expect(trace!.spans).toHaveLength(2);
    const [root, charge] = trace!.spans;
    expect(root!.parentSpanId).toBeNull();
    expect(root!.status).toBe("ok");
    expect(root!.kind).toBe("Server");
    expect(root!.attributes).toEqual({
      "http.request.method": "POST",
      "resource.service.version": "1.2.3",
      "resource.service.namespace": "shop",
    });
    expect(root!.events).toEqual([]);

    expect(charge!.parentSpanId).toBe("root");
    expect(charge!.status).toBe("error");
    expect(charge!.statusMessage).toBe("card declined");
    expect(charge!.startNs).toBe("1040000000");
    expect(charge!.durNs).toBe("258000000");
    // Scope attributes get their own prefix; a JSON-string container is
    // parsed like an object one.
    expect(charge!.attributes).toEqual({
      "payment.provider": "stripe",
      "scope.otel.library": "x",
      "resource.service.version": "9.9.9",
    });
    expect(charge!.events).toEqual([
      {
        name: "exception",
        timeUnixNano: "1055000000",
        attributes: {
          "exception.type": "PaymentError",
          "exception.message": "card declined",
        },
      },
    ]);

    expect(trace!.profiles).toEqual([
      {
        profileId: "p1",
        timeUnixNano: "1050000000",
        durationNano: "10000000",
        sampleType: "cpu",
        sampleUnit: "nanoseconds",
        serviceName: "payments",
        spanId: "charge",
      },
    ]);
  });

  it("is undefined for a trace with no spans", () => {
    expect(
      traceFromIrResponses("nope", { ...SPANS_RES, rows: [] }, PROFILES_RES),
    ).toBeUndefined();
  });
});

describe("fetchTraceDetail", () => {
  it("retries with a wide window when the trace is outside the viewer's range", async () => {
    client.setConfig({ baseUrl: "http://localhost" });
    const bodies: unknown[] = [];
    const fetchMock = vi.fn(async (req: Request) => {
      const body = await req.clone().json();
      bodies.push(body);
      // First spans query (viewer range) is empty; the wide retry finds it.
      const empty = body.from === "traces" && bodies.length === 1;
      const payload =
        body.from === "profiles"
          ? PROFILES_RES
          : empty
            ? { ...SPANS_RES, rows: [] }
            : SPANS_RES;
      return new Response(JSON.stringify(payload), {
        status: 200,
        headers: { "Content-Type": "application/json" },
      });
    });
    vi.stubGlobal("fetch", fetchMock);

    const trace = await fetchTraceDetail("t1cafe", RANGE, 5_000_000);
    expect(trace?.spans).toHaveLength(2);
    const spanDocs = bodies.filter(
      (b) => (b as { from: string }).from === "traces",
    ) as { range: { from: string; to: string } }[];
    expect(spanDocs).toHaveLength(2);
    expect(spanDocs[0]!.range).toEqual({
      from: "1000000000000",
      to: "2000000000000",
    });
    // Wide retry: 30 days back from "now", to now.
    expect(BigInt(spanDocs[1]!.range.from)).toBeLessThan(
      BigInt(spanDocs[0]!.range.from),
    );
    expect(spanDocs[1]!.range.to).toBe("5000000000000");
  });

  it("returns undefined when even the wide window has no spans", async () => {
    client.setConfig({ baseUrl: "http://localhost" });
    vi.stubGlobal(
      "fetch",
      vi.fn(async (req: Request) => {
        const body = await req.clone().json();
        const payload =
          body.from === "profiles" ? PROFILES_RES : { ...SPANS_RES, rows: [] };
        return new Response(JSON.stringify(payload), {
          status: 200,
          headers: { "Content-Type": "application/json" },
        });
      }),
    );
    expect(await fetchTraceDetail("nope", RANGE, 5_000_000)).toBeNull();
  });
});
