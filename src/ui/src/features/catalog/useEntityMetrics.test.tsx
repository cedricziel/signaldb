import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clientWrapper } from "../../test/render";
import * as entityMetricsApi from "../../api/entityMetrics";
import type { MetricHit } from "../schema/api";
import type { EntityTypeDef } from "./entityTypes";
import { useEntityMetrics } from "./useEntityMetrics";

vi.mock("../../api/entityMetrics", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entityMetrics")>();
  return {
    ...actual,
    discoverObservedMetricNames: vi.fn(),
    fetchMetricDefinitions: vi.fn(),
  };
});

const discoverObservedMetricNames = vi.mocked(
  entityMetricsApi.discoverObservedMetricNames,
);
const fetchMetricDefinitions = vi.mocked(
  entityMetricsApi.fetchMetricDefinitions,
);

const host: EntityTypeDef = {
  id: "host",
  label: "Hosts",
  singular: "host",
  identity: ["host.name"],
  registryEntity: "host",
};

function metric(name: string, entities: string[]): MetricHit {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument: "gauge",
    unit: "1",
    attributes: [],
    entity_associations: entities,
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as MetricHit;
}

beforeEach(() => {
  discoverObservedMetricNames.mockReset();
  fetchMetricDefinitions.mockReset();
  discoverObservedMetricNames.mockResolvedValue(["system.cpu.time"]);
  fetchMetricDefinitions.mockResolvedValue([
    metric("system.cpu.time", ["host"]),
    metric("otelcol_receiver_accepted_spans", []),
  ]);
});

afterEach(() => {
  vi.restoreAllMocks();
});

const r1 = { fromMs: 1_000_000, toMs: 4_600_000 };
const r2 = { fromMs: 2_000_000, toMs: 4_600_000 };

describe("useEntityMetrics", () => {
  it("reports the entity's associated metrics that the window holds", async () => {
    const { result } = renderHook(
      () => useEntityMetrics(host, r1, "1h|acme|prod"),
      { wrapper: clientWrapper() },
    );

    await waitFor(() =>
      expect(result.current.metrics.map((m) => m.name)).toEqual([
        "system.cpu.time",
      ]),
    );
  });

  it("re-discovers on a range change but does not re-read the registry", async () => {
    // What a window holds is a fact about the window; what a metric measures
    // is not. Keying both on the range would re-fetch definitions every time
    // someone touches the range picker, for an identical answer.
    const wrapper = clientWrapper();
    const { rerender } = renderHook(
      ({ range, key }: { range: typeof r1; key: string }) =>
        useEntityMetrics(host, range, key),
      { wrapper, initialProps: { range: r1, key: "1h|acme|prod" } },
    );

    await waitFor(() =>
      expect(fetchMetricDefinitions).toHaveBeenCalledTimes(1),
    );
    // One discovery per metric source: `metrics` and `metrics_histogram` are
    // two IR sources for one OTel signal.
    expect(discoverObservedMetricNames.mock.calls.map((c) => c[0])).toEqual([
      "metrics",
      "metrics_histogram",
    ]);

    rerender({ range: r2, key: "30m|acme|prod" });

    await waitFor(() =>
      expect(discoverObservedMetricNames).toHaveBeenCalledTimes(4),
    );
    expect(fetchMetricDefinitions).toHaveBeenCalledTimes(1);
  });

  it("re-reads the registry for a different tenant", async () => {
    // Registries are per tenant: the same metric name can mean something else
    // next door, and an entity association is exactly what differs.
    const wrapper = clientWrapper();
    const { rerender } = renderHook(
      ({ key }: { key: string }) => useEntityMetrics(host, r1, key),
      { wrapper, initialProps: { key: "1h|acme|prod" } },
    );

    await waitFor(() =>
      expect(fetchMetricDefinitions).toHaveBeenCalledTimes(1),
    );
    rerender({ key: "1h|other|prod" });

    await waitFor(() =>
      expect(fetchMetricDefinitions).toHaveBeenCalledTimes(2),
    );
  });

  it("asks for no definitions when the entity type has no registry name", async () => {
    const database: EntityTypeDef = {
      id: "database",
      label: "Databases",
      singular: "database",
      identity: ["db.namespace"],
    };

    const { result } = renderHook(
      () => useEntityMetrics(database, r1, "1h|acme|prod"),
      { wrapper: clientWrapper() },
    );

    await waitFor(() => expect(result.current.isPending).toBe(false));
    expect(result.current.metrics).toEqual([]);
    expect(discoverObservedMetricNames).not.toHaveBeenCalled();
    expect(fetchMetricDefinitions).not.toHaveBeenCalled();
  });
});
