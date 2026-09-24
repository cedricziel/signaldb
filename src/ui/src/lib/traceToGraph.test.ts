import { describe, expect, it } from "vitest";
import type { TempoSpan } from "../api/traceTypes";
import { traceToGraph } from "./traceToGraph";

const span = (over: Partial<TempoSpan>): TempoSpan => ({
  spanId: "s",
  parentSpanId: null,
  name: "op",
  serviceName: "svc",
  status: "ok",
  startNs: "0",
  durNs: "1000000",
  attributes: {},
  events: [],
  ...over,
});

describe("traceToGraph", () => {
  it("returns one node per service with summed time", () => {
    const { nodes } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway", durNs: "1000000" }),
      span({
        spanId: "b",
        parentSpanId: "a",
        serviceName: "payments",
        durNs: "500000",
      }),
      span({
        spanId: "c",
        parentSpanId: "a",
        serviceName: "payments",
        durNs: "250000",
      }),
    ]);
    expect(nodes.map((n) => n.service).sort()).toEqual(["gateway", "payments"]);
    const payments = nodes.find((n) => n.service === "payments")!;
    expect(payments.durationMs).toBeCloseTo(0.75);
  });

  it("creates an edge for a parent->child call to a different service", () => {
    const { edges } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", parentSpanId: "a", serviceName: "payments" }),
    ]);
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({
      from: "gateway",
      to: "payments",
      count: 1,
      failed: false,
    });
  });

  it("does not create an edge for a same-service parent/child", () => {
    const { edges } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", parentSpanId: "a", serviceName: "gateway" }),
    ]);
    expect(edges).toHaveLength(0);
  });

  it("counts repeated calls between the same two services on one edge", () => {
    const { edges } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", parentSpanId: "a", serviceName: "payments" }),
      span({ spanId: "c", parentSpanId: "a", serviceName: "payments" }),
    ]);
    expect(edges).toHaveLength(1);
    expect(edges[0]!.count).toBe(2);
  });

  it("marks an edge and its target node failed when the child span errored", () => {
    const { edges, nodes } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({
        spanId: "b",
        parentSpanId: "a",
        serviceName: "payments",
        status: "error",
      }),
    ]);
    expect(edges[0]!.failed).toBe(true);
    expect(nodes.find((n) => n.service === "payments")!.failed).toBe(true);
    expect(nodes.find((n) => n.service === "gateway")!.failed).toBe(false);
  });

  it("does not mark an edge failed when only an unrelated span errored", () => {
    const { edges } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", parentSpanId: "a", serviceName: "payments" }),
      span({
        spanId: "c",
        parentSpanId: "b",
        serviceName: "payments",
        status: "error",
      }),
    ]);
    expect(edges[0]!.failed).toBe(false);
  });

  it("uses the direct parent as the caller even past an internal hop", () => {
    // gateway -> gateway (internal) -> payments: the direct parent of the
    // payments span is the internal gateway span, so the edge is still
    // gateway -> payments.
    const { edges } = traceToGraph([
      span({ spanId: "a", serviceName: "gateway" }),
      span({ spanId: "b", parentSpanId: "a", serviceName: "gateway" }),
      span({ spanId: "c", parentSpanId: "b", serviceName: "payments" }),
    ]);
    expect(edges).toHaveLength(1);
    expect(edges[0]).toMatchObject({ from: "gateway", to: "payments" });
  });

  it("treats a span with a missing parent as a root with no edge", () => {
    const { edges, nodes } = traceToGraph([
      span({
        spanId: "orphan",
        parentSpanId: "not-in-payload",
        serviceName: "x",
      }),
    ]);
    expect(edges).toHaveLength(0);
    expect(nodes).toHaveLength(1);
  });

  it("returns an empty graph for no spans", () => {
    expect(traceToGraph([])).toEqual({ nodes: [], edges: [] });
  });
});
