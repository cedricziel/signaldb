import { describe, expect, it } from "vitest";
import { clientServerSplit } from "./sessionTraceSplit";
import type { TempoSpan } from "../../api/traceTypes";

function span(overrides: Partial<TempoSpan> = {}): TempoSpan {
  return {
    spanId: "s1",
    parentSpanId: null,
    name: "span",
    serviceName: "storefront-web",
    status: "unset",
    startNs: "1000",
    durNs: "1000",
    attributes: {},
    events: [],
    ...overrides,
  };
}

describe("clientServerSplit", () => {
  it("splits browser+network time from the first server child's duration", () => {
    const spans: TempoSpan[] = [
      span({
        spanId: "client-1",
        kind: "Client",
        startNs: "1000",
        durNs: "500000000", // 500ms
      }),
      span({
        spanId: "server-1",
        parentSpanId: "client-1",
        kind: "Server",
        serviceName: "checkout-svc",
        startNs: "2000",
        durNs: "300000000", // 300ms
      }),
    ];
    const split = clientServerSplit(spans, "client-1");
    expect(split).toEqual({
      browserNetworkMs: 200,
      backendMs: 300,
      backendServiceName: "checkout-svc",
      backendSpanId: "server-1",
    });
  });

  it("picks the earliest server-kind child when there are several", () => {
    const spans: TempoSpan[] = [
      span({ spanId: "client-1", kind: "Client", durNs: "500000000" }),
      span({
        spanId: "server-late",
        parentSpanId: "client-1",
        kind: "Server",
        startNs: "5000",
        durNs: "100000000",
      }),
      span({
        spanId: "server-early",
        parentSpanId: "client-1",
        kind: "Server",
        startNs: "1500",
        durNs: "250000000",
      }),
    ];
    const split = clientServerSplit(spans, "client-1");
    expect(split?.backendSpanId).toBe("server-early");
  });

  it("clamps browser+network time to zero when the backend outlasts the client span", () => {
    const spans: TempoSpan[] = [
      span({ spanId: "client-1", kind: "Client", durNs: "100000000" }),
      span({
        spanId: "server-1",
        parentSpanId: "client-1",
        kind: "Server",
        durNs: "300000000",
      }),
    ];
    expect(clientServerSplit(spans, "client-1")?.browserNetworkMs).toBe(0);
  });

  it("is undefined for a client span with no server-kind child", () => {
    const spans: TempoSpan[] = [span({ spanId: "client-1", kind: "Client" })];
    expect(clientServerSplit(spans, "client-1")).toBeUndefined();
  });

  it("is undefined when the client span id isn't in the trace", () => {
    expect(clientServerSplit([span()], "missing")).toBeUndefined();
  });
});
