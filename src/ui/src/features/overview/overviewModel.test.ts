import { describe, expect, it } from "vitest";
import type { CatalogEntity } from "../../api/catalog";
import {
  ago,
  healthOf,
  kpiFigures,
  lastDeployLabel,
  rateFigure,
  serviceRows,
  setupSteps,
  type ServiceRow,
} from "./overviewModel";

function entity(
  name: string,
  traces: number,
  errors: number,
  p95Ms: number,
  sources: string[] = ["traces"],
): CatalogEntity {
  return {
    values: [name, null],
    observations: sources.map((source) => ({ source, count: 1 })),
    lastNs: "0",
    red: traces ? { traces, errors, p50Ms: p95Ms / 2, p95Ms } : undefined,
  };
}

describe("healthOf", () => {
  it("applies the design's thresholds", () => {
    expect(healthOf(0.02, 10)).toBe("critical");
    expect(healthOf(0.005, 10)).toBe("degraded");
    expect(healthOf(0.001, 501)).toBe("degraded");
    expect(healthOf(0.001, 500)).toBe("healthy");
  });
});

describe("serviceRows", () => {
  it("sorts worst health first, then by rate", () => {
    const rows = serviceRows(
      [
        entity("fast", 3600, 0, 10),
        entity("busy", 7200, 0, 10),
        entity("slow", 360, 0, 900),
        entity("broken", 36, 10, 10),
      ],
      3600,
    );
    expect(rows.map((r) => [r.name, r.health])).toEqual([
      ["broken", "critical"],
      ["slow", "degraded"],
      ["busy", "healthy"],
      ["fast", "healthy"],
    ]);
    expect(rows[2]!.ratePerSec).toBe(2);
  });
});

describe("kpiFigures", () => {
  const kpis = {
    current: {
      count: 7200,
      ratePerSec: 2,
      errorRate: 0.0074,
      p50Ms: 10,
      p95Ms: 214,
      p99Ms: 900,
      peakRatePerSec: 4,
      lastNs: "0",
    },
    previous: {
      count: 3600,
      ratePerSec: 1,
      errorRate: 0.0043,
      p50Ms: 10,
      p95Ms: 196,
      p99Ms: 900,
      peakRatePerSec: 2,
      lastNs: "0",
    },
    series: { rate: [], errorRate: [], p95: [] },
  };

  it("colours rising errors and latency bad, rising traffic good", () => {
    const [req, err, p95, ingest] = kpiFigures(
      kpis,
      [],
      [
        { key: "logs", points: [[1, 60]] },
        { key: "traces", points: [[1, 30]] },
        { key: "metrics", points: [[1, 10]] },
      ],
    );
    expect(req).toMatchObject({
      value: "2",
      unit: "req/s",
      change: { text: "+100%", tone: "good" },
    });
    expect(err).toMatchObject({
      value: "0.74",
      valueTone: "error",
      change: { text: "+0.31 pp", tone: "bad" },
    });
    expect(p95).toMatchObject({
      value: "214",
      unit: "ms",
      change: { text: "+18 ms", tone: "bad" },
    });
    expect(ingest).toMatchObject({
      value: "100",
      unit: "records",
      detail: "logs 60% · traces 30%",
    });
  });

  it("omits the comparison without a previous period", () => {
    const [req] = kpiFigures({ ...kpis, previous: undefined }, [], []);
    expect(req!.change).toBeUndefined();
  });

  it("switches to per-minute below one request a second", () => {
    expect(rateFigure(0.5)).toEqual({ value: "30", unit: "req/min" });
  });
});

describe("lastDeployLabel", () => {
  const latest = new Map([
    ["cart", { service: "cart", version: "v2", firstMs: 0, lastMs: 0 }],
  ]);
  it("names an in-window deploy with its age, else the running version", () => {
    expect(
      lastDeployLabel(
        "cart",
        [{ service: "cart", version: "v2", atMs: 0 }],
        latest,
        16 * 60_000,
      ),
    ).toBe("v2 · 16m ago");
    expect(lastDeployLabel("cart", [], latest, 0)).toBe("v2");
    expect(lastDeployLabel("search", [], latest, 0)).toBe("—");
    expect(ago(3 * 3_600_000)).toBe("3h ago");
  });
});

describe("setupSteps", () => {
  const row = (name: string, sources: string[]): ServiceRow => ({
    key: name,
    values: [name],
    name,
    ratePerSec: 0,
    errorRate: 0,
    p95Ms: 0,
    health: "healthy",
    sources: new Set(sources),
  });

  it("reads coverage off the services and names the ones missing traces", () => {
    const steps = setupSteps({
      rows: [
        row("api", ["traces", "logs", "profiles"]),
        row("cart", ["traces", "logs"]),
        row("worker", ["logs"]),
      ],
      githubLinked: false,
      memberCount: 1,
      canManage: true,
    });
    expect(steps.map((s) => [s.id, s.done, s.detail])).toEqual([
      ["traces", true, "2 services sending spans"],
      ["services", false, "2 of 3 services send traces · worker send none"],
      ["logs", true, "2 of 2 instrumented services"],
      ["profiles", false, "1 of 2 services send profiles"],
      ["github", false, "not connected"],
      ["team", false, "you are the only member"],
    ]);
  });

  it("drops the team step when the member count is unknown", () => {
    const steps = setupSteps({
      rows: [],
      githubLinked: true,
      memberCount: undefined,
      canManage: false,
    });
    expect(steps.map((s) => s.id)).not.toContain("team");
    expect(steps.find((s) => s.id === "github")).toMatchObject({
      done: true,
      cta: undefined,
    });
  });
});
