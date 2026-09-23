import { screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { compositeKey } from "../../lib/traceGroups";
import { resolveRange } from "../../lib/time";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { renderWithClient } from "../../test/render";
import { EntityDetail } from "./EntityDetail";
import { entityType, type EntityTypeDef } from "./entityTypes";
import * as catalogApi from "../../api/catalog";
import * as membersApi from "../../api/traceGroupMembers";
import * as dependencyBreakdownApi from "../../api/dependencyBreakdown";
import * as errorsApi from "../../api/errors";
import * as entityMetricSeriesApi from "../../api/entityMetricSeries";
import * as operationSeriesApi from "../../api/operationSeries";
import * as entityMetricsHook from "./useEntityMetrics";
import * as entityKpisHook from "./useEntityKpis";
import type { CatalogEntity } from "../../api/catalog";
import type { TraceGroupMember } from "../../api/traceGroupMembers";
import type { EntityKpis } from "../../api/entityDetailStats";

vi.mock("../../api/catalog", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/catalog")>();
  return { ...actual, fetchCatalogEntities: vi.fn() };
});
vi.mock("../../api/traceGroupMembers", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/traceGroupMembers")>();
  return { ...actual, fetchTraceGroupMembers: vi.fn() };
});
vi.mock("../../api/dependencyBreakdown", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/dependencyBreakdown")>();
  return { ...actual, fetchDependencyBreakdown: vi.fn() };
});
vi.mock("../../api/errors", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../../api/errors")>();
  return {
    ...actual,
    fetchErrorGroups: vi.fn(),
    fetchErrorGroupVolume: vi.fn(),
  };
});
vi.mock("../../api/entityMetricSeries", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/entityMetricSeries")>();
  return { ...actual, fetchEntityMetricSeries: vi.fn() };
});
vi.mock("../../api/operationSeries", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/operationSeries")>();
  return { ...actual, fetchOperationSeries: vi.fn() };
});
vi.mock("./useEntityMetrics", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./useEntityMetrics")>();
  return { ...actual, useEntityMetrics: vi.fn() };
});
vi.mock("./useEntityKpis", async (importOriginal) => {
  const actual = await importOriginal<typeof import("./useEntityKpis")>();
  return { ...actual, useEntityKpis: vi.fn() };
});

const fetchCatalogEntities = vi.mocked(catalogApi.fetchCatalogEntities);
const fetchTraceGroupMembers = vi.mocked(membersApi.fetchTraceGroupMembers);
const fetchDependencyBreakdown = vi.mocked(
  dependencyBreakdownApi.fetchDependencyBreakdown,
);
const fetchErrorGroups = vi.mocked(errorsApi.fetchErrorGroups);
const fetchErrorGroupVolume = vi.mocked(errorsApi.fetchErrorGroupVolume);
const fetchEntityMetricSeries = vi.mocked(
  entityMetricSeriesApi.fetchEntityMetricSeries,
);
const fetchOperationSeries = vi.mocked(operationSeriesApi.fetchOperationSeries);
const useEntityMetrics = vi.mocked(entityMetricsHook.useEntityMetrics);
const useEntityKpis = vi.mocked(entityKpisHook.useEntityKpis);

/** A default, "there's traffic" KPI result, so a test that doesn't care
 * about the KPI cards still sees the row instead of a loading skeleton or
 * the empty state. */
const defaultKpis: EntityKpis = {
  current: {
    count: 1240,
    ratePerSec: 2.3,
    errorRate: 0.05,
    p50Ms: 12,
    p95Ms: 48,
    p99Ms: 90,
    peakRatePerSec: 3.1,
    lastNs: "1700000000000000000",
  },
  previous: {
    count: 1000,
    ratePerSec: 2,
    errorRate: 0.02,
    p50Ms: 10,
    p95Ms: 40,
    p99Ms: 80,
    peakRatePerSec: 2.5,
    lastNs: "1699999000000000000",
  },
  series: {
    rate: [
      { tMs: 0, value: 1 },
      { tMs: 60_000, value: 2 },
    ],
    errorRate: [
      { tMs: 0, value: 0.01 },
      { tMs: 60_000, value: 0.05 },
    ],
    p95: [
      { tMs: 0, value: 40 },
      { tMs: 60_000, value: 48 },
    ],
  },
};

afterEach(() => {
  vi.restoreAllMocks();
});

beforeEach(() => {
  fetchCatalogEntities.mockReset();
  fetchTraceGroupMembers.mockReset();
  fetchDependencyBreakdown.mockReset();
  fetchErrorGroups.mockReset();
  fetchErrorGroupVolume.mockReset();
  fetchCatalogEntities.mockResolvedValue({ entities: [], truncated: false });
  fetchTraceGroupMembers.mockResolvedValue([]);
  fetchDependencyBreakdown.mockResolvedValue([]);
  fetchErrorGroups.mockResolvedValue({ groups: [], truncated: false });
  fetchErrorGroupVolume.mockResolvedValue([]);
  fetchEntityMetricSeries.mockReset();
  fetchEntityMetricSeries.mockResolvedValue(new Map());
  fetchOperationSeries.mockReset();
  fetchOperationSeries.mockResolvedValue(new Map());
  useEntityMetrics.mockReset();
  useEntityMetrics.mockReturnValue({
    metrics: [
      {
        name: "process.cpu.utilization",
        instrument: "gauge",
        unit: "1",
        entity_associations: ["service"],
      } as never,
    ],
    isPending: false,
    isError: false,
  });
  useEntityKpis.mockReset();
  useEntityKpis.mockReturnValue({
    data: defaultKpis,
    isPending: false,
    isError: false,
    error: null,
  } as never);
});

function group(
  values: (string | null)[],
  traces: number,
  errors: number,
  p50Ms: number,
  p95Ms: number,
  lastNs: string,
): CatalogEntity {
  return {
    values,
    observations: [{ source: "traces", count: traces }],
    lastNs,
    red: { traces, errors, p50Ms, p95Ms },
  };
}

function member(
  traceId: string,
  spanId: string,
  spanName: string,
  serviceName: string,
): TraceGroupMember {
  return {
    traceId,
    spanId,
    parentSpanId: null,
    spanName,
    serviceName,
    startNs: "1700000000000000000",
    durationNanos: "5000000",
    statusCode: "Ok",
  };
}

function renderView(
  state: Partial<ExploreState> = {},
  entity: EntityTypeDef = entityType("service")!,
) {
  const update = vi.fn();
  renderWithClient(
    <EntityDetail
      entity={entity}
      range={resolveRange(DEFAULT_STATE.range, Date.now())}
      state={{
        ...DEFAULT_STATE,
        signal: "catalog",
        catalogEntity: entity.id,
        catalogPrimary: compositeKey(["gateway", "edge"]),
        ...state,
      }}
      update={update}
    />,
  );
  return update;
}

describe("EntityDetail", () => {
  it("shows a breadcrumb naming the entity type and the drilled-into entity", async () => {
    renderView();
    const crumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    expect(within(crumb).getByText("catalog")).toBeInTheDocument();
    expect(within(crumb).getByText("Services")).toBeInTheDocument();
    expect(within(crumb).getByText("gateway · edge")).toBeInTheDocument();
  });

  it("clicking catalog or the entity-type crumb returns to the list", async () => {
    const update = renderView();
    const user = userEvent.setup();
    const crumb = screen.getByRole("navigation", { name: "Breadcrumb" });
    await user.click(within(crumb).getByText("catalog"));
    // A crumb hop is a real navigation, same as drilling in — so Back steps
    // out of it one level at a time, not a no-op against the drill-in push.
    expect(update).toHaveBeenCalledWith(
      { catalogPrimary: "", catalogSecondary: "" },
      { push: true },
    );
  });

  it("shows the rate, error, and duration KPI cards from useEntityKpis", async () => {
    renderView();
    expect(
      await screen.findByText("Rate", { selector: ".kpi-label" }),
    ).toBeInTheDocument();
    expect(screen.getByText("2.3/s")).toBeInTheDocument();
    expect(screen.getByText("peak 3.1/s · 1,240 total")).toBeInTheDocument();

    expect(
      screen.getByText("Errors", { selector: ".kpi-label" }),
    ).toBeInTheDocument();
    expect(screen.getByText("5%")).toBeInTheDocument();
    expect(screen.getByText("62 failed")).toBeInTheDocument();

    expect(
      screen.getByText("Duration", { selector: ".kpi-label" }),
    ).toBeInTheDocument();
    expect(screen.getByText("48 ms")).toBeInTheDocument();
    expect(screen.getByText("p50 12 ms · p99 90 ms")).toBeInTheDocument();
  });

  it("colors the error-rate value when it's above zero", async () => {
    renderView();
    const errorsCard = (
      await screen.findByText("Errors", { selector: ".kpi-label" })
    ).closest(".kpi-card") as HTMLElement;
    expect(within(errorsCard).getByText("5%")).toHaveClass("kpi-value-error");
  });

  it("shows a change figure vs. the previous period, toned by direction", async () => {
    renderView();
    // Rate is up but stays neutral — an increase isn't inherently good.
    const rateCard = (
      await screen.findByText("Rate", { selector: ".kpi-label" })
    ).closest(".kpi-card") as HTMLElement;
    expect(within(rateCard).getByText("+15% vs prev")).toHaveClass(
      "kpi-change-neutral",
    );
    // The error rate rose (2% -> 5%), which is bad.
    const errorsCard = screen
      .getByText("Errors", { selector: ".kpi-label" })
      .closest(".kpi-card") as HTMLElement;
    expect(within(errorsCard).getByText("+3pp vs prev")).toHaveClass(
      "kpi-change-bad",
    );
    // p95 rose too (40ms -> 48ms), also bad for duration.
    const durationCard = screen
      .getByText("Duration", { selector: ".kpi-label" })
      .closest(".kpi-card") as HTMLElement;
    expect(within(durationCard).getByText("+20% vs prev")).toHaveClass(
      "kpi-change-bad",
    );
  });

  it("hides the change figure when there's no previous-period data", async () => {
    useEntityKpis.mockReturnValue({
      data: { ...defaultKpis, previous: undefined },
      isPending: false,
      isError: false,
      error: null,
    } as never);
    renderView();
    await screen.findByText("Rate", { selector: ".kpi-label" });
    expect(screen.queryByText(/vs prev/)).not.toBeInTheDocument();
  });

  it("shows a loading skeleton while the KPIs are pending", () => {
    useEntityKpis.mockReturnValue({
      data: undefined,
      isPending: true,
      isError: false,
      error: null,
    } as never);
    renderView();
    expect(
      screen.queryByText("Rate", { selector: ".kpi-label" }),
    ).not.toBeInTheDocument();
  });

  it("shows the empty note when there are no matching spans", async () => {
    useEntityKpis.mockReturnValue({
      data: {
        current: undefined,
        previous: undefined,
        series: { rate: [], errorRate: [], p95: [] },
      },
      isPending: false,
      isError: false,
      error: null,
    } as never);
    renderView();
    expect(
      await screen.findByText("No matching spans in this window."),
    ).toBeInTheDocument();
    expect(
      screen.queryByText("Rate", { selector: ".kpi-label" }),
    ).not.toBeInTheDocument();
  });

  // Regression: a null second-identity-dimension pin used to be dropped
  // entirely, leaving that dimension unconstrained — the KPIs could then
  // come from a different (gateway, <some other namespace>) entity instead
  // of the "(not set)" one actually drilled into.
  it("pins a null identity dimension instead of leaving it unconstrained", async () => {
    renderView({ catalogPrimary: compositeKey(["gateway", null]) });
    await waitFor(() => expect(fetchCatalogEntities).toHaveBeenCalled());
    const call = fetchCatalogEntities.mock.calls.find(
      (c) => c[0].id === "service",
    )!;
    expect(call[3]).toEqual([
      { field: "service.name", value: "gateway" },
      { field: "service.namespace", value: null },
    ]);
  });

  it("prefixes last-seen with the date on a multi-day range", async () => {
    const update = vi.fn();
    renderWithClient(
      <EntityDetail
        entity={entityType("service")!}
        range={resolveRange(
          { type: "relative", seconds: 7 * 86400 },
          Date.now(),
        )}
        state={{
          ...DEFAULT_STATE,
          signal: "catalog",
          catalogEntity: "service",
          catalogPrimary: compositeKey(["gateway", "edge"]),
          range: { type: "relative", seconds: 7 * 86400 },
        }}
        update={update}
      />,
    );
    expect(
      await screen.findByText(/^Last seen \d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}/),
    ).toBeInTheDocument();
  });

  it("names the signals covering the entity, without sample counts, next to the title", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.id === "service") {
        return {
          entities: [
            {
              values: ["gateway", "edge"],
              observations: [
                { source: "traces", count: 400 },
                { source: "logs", count: 2000 },
              ],
              lastNs: "1700000000000000000",
              red: { traces: 400, errors: 0, p50Ms: 12, p95Ms: 48 },
            },
          ],
          truncated: false,
        };
      }
      return { entities: [], truncated: false };
    });
    renderView();
    // Which signals see this entity is worth knowing — it is what explains a
    // missing RED measurement. How many samples each carries is not.
    const observed = await screen.findByText("traces");
    const titleRow = observed.closest(".entity-detail-title") as HTMLElement;
    expect(within(titleRow).getByText("logs")).toBeInTheDocument();
    expect(within(titleRow).queryByText("400")).not.toBeInTheDocument();
    expect(within(titleRow).queryByText("2000")).not.toBeInTheDocument();
  });

  it("shows the breakdown table for an entity type that defines one", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "span.name") {
        return {
          entities: [
            group(["GET /health"], 400, 0, 5, 9, "1700000000000000000"),
          ],
          truncated: false,
        };
      }
      return { entities: [], truncated: false };
    });
    renderView();
    expect(await screen.findByText("Operations")).toBeInTheDocument();
    expect(await screen.findByText("GET /health")).toBeInTheDocument();
  });

  it("drills a breakdown row into the secondary pin", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "span.name") {
        return {
          entities: [
            group(["GET /health"], 400, 0, 5, 9, "1700000000000000000"),
          ],
          truncated: false,
        };
      }
      return { entities: [], truncated: false };
    });
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("GET /health"));
    expect(update).toHaveBeenCalledWith(
      { catalogSecondary: "GET /health" },
      { push: true },
    );
  });

  it("hides the breakdown table at the secondary pin depth", async () => {
    renderView({ catalogSecondary: "GET /health" });
    expect(screen.queryByText("Operations")).not.toBeInTheDocument();
  });

  it("shows the slowest traces and opens one into the trace waterfall", async () => {
    // Regression: this used to only set `trace`, which is meaningless
    // within the catalog signal (only TracesView renders a waterfall for
    // it) — the row silently did nothing visible. It must also switch to
    // the traces signal, same as the "View matching traces" escape hatch.
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1", "s1", "GET /health", "gateway"),
    ]);
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByText("GET /health"));
    expect(update).toHaveBeenCalledWith(
      { signal: "traces", trace: "t1" },
      { push: true },
    );
  });

  it("opens the slowest traces table sorted by duration, not time", async () => {
    fetchTraceGroupMembers.mockResolvedValue([
      member("t1", "s1", "GET /health", "gateway"),
    ]);
    renderView();
    await screen.findByText("GET /health");
    expect(
      screen.getByRole("columnheader", { name: "Duration" }),
    ).toHaveAttribute("aria-sort", "descending");
    expect(
      screen.getByRole("columnheader", { name: "Time" }),
    ).not.toHaveAttribute("aria-sort");
  });

  it('the "Traces" button filters Traces to this entity', async () => {
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Traces" }));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: "gateway" }],
      },
      { push: true },
    );
  });

  it("fetches the slowest traces as root spans, sorted by duration, capped at 8", async () => {
    renderView();
    await waitFor(() => expect(fetchTraceGroupMembers).toHaveBeenCalled());
    expect(fetchTraceGroupMembers).toHaveBeenCalledWith(
      ["service.name", "service.namespace"],
      ["gateway", "edge"],
      expect.anything(),
      [],
      "traces",
      8,
      { field: "duration", dir: "desc" },
    );
  });

  it('the Slowest traces section\'s "Open in Traces" button filters Traces to this entity', async () => {
    const update = renderView();
    const user = userEvent.setup();
    await user.click(
      await screen.findByRole("button", { name: "Open in Traces" }),
    );
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [{ field: "service.name", value: "gateway" }],
      },
      { push: true },
    );
  });

  it('the "Traces" button also pins the operation at the breakdown level', async () => {
    const update = renderView({
      catalogSecondary: compositeKey(["POST /checkout"]),
    });
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Traces" }));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "traces",
        traceFilters: [
          { field: "service.name", value: "gateway" },
          { field: "name", value: "POST /checkout" },
        ],
      },
      { push: true },
    );
  });

  it('the "Logs" button filters Logs to this entity\'s identity attributes', async () => {
    const update = renderView();
    const user = userEvent.setup();
    await user.click(await screen.findByRole("button", { name: "Logs" }));
    expect(update).toHaveBeenCalledWith(
      {
        signal: "logs",
        filters: [
          { label: "service.name", op: "=", value: "gateway" },
          { label: "service.namespace", op: "=", value: "edge" },
        ],
      },
      { push: true },
    );
  });

  it("has no Logs button for an entity type pinned on a span-only attribute", async () => {
    renderView(
      { catalogPrimary: compositeKey(["prod", "postgres"]) },
      entityType("database")!,
    );
    await screen.findByRole("button", { name: "Traces" });
    expect(
      screen.queryByRole("button", { name: "Logs" }),
    ).not.toBeInTheDocument();
  });

  it("shows a read-only top-values table for an entity type that defines one", async () => {
    fetchCatalogEntities.mockImplementation(async (entityType) => {
      if (entityType.identity[0] === "db.query.text") {
        return {
          entities: [
            group(
              ["SELECT * FROM users WHERE id = ?"],
              300,
              0,
              4,
              8,
              "1700000000000000000",
            ),
          ],
          truncated: false,
        };
      }
      return { entities: [], truncated: false };
    });
    const update = renderView(
      { catalogPrimary: compositeKey(["prod", "postgres"]) },
      entityType("database")!,
    );

    expect(await screen.findByText("Top statements")).toBeInTheDocument();
    const cell = await screen.findByText("SELECT * FROM users WHERE id = ?");
    expect(cell).toBeInTheDocument();

    // Read-only: clicking a row doesn't drill in or otherwise call update.
    const user = userEvent.setup();
    await user.click(cell);
    expect(update).not.toHaveBeenCalled();
  });

  describe("dependency-type breakdown", () => {
    it("shows a proportional bar and legend for a service's own page", async () => {
      fetchDependencyBreakdown.mockResolvedValue([
        { key: "database", label: "Database", durationNs: 300, count: 3 },
        { key: "http", label: "HTTP", durationNs: 100, count: 1 },
      ]);
      renderView(); // default state: catalogEntity "service", primary "gateway · edge"

      expect(screen.getByText("Time by dependency")).toBeInTheDocument();
      // The section headline renders immediately; its content is a
      // separate async query, so wait for that to settle too.
      expect(await screen.findByText("Database")).toBeInTheDocument();
      expect(screen.getByText(/75\.0%/)).toBeInTheDocument();
      expect(screen.getByText("HTTP")).toBeInTheDocument();
      expect(screen.getByText(/25\.0%/)).toBeInTheDocument();
      expect(fetchDependencyBreakdown).toHaveBeenCalledWith(
        "gateway",
        expect.anything(),
      );
    });

    it("shows an empty note rather than an empty bar when there's no dependency traffic", async () => {
      fetchDependencyBreakdown.mockResolvedValue([]);
      renderView();

      expect(
        await screen.findByText(/No database, HTTP, RPC, or messaging calls/),
      ).toBeInTheDocument();
    });

    it("is not shown for non-service entity types", async () => {
      renderView(
        { catalogPrimary: compositeKey(["prod", "postgres"]) },
        entityType("database")!,
      );

      await screen.findByText("Slowest traces");
      expect(screen.queryByText("Time by dependency")).not.toBeInTheDocument();
      expect(fetchDependencyBreakdown).not.toHaveBeenCalled();
    });

    it("keeps the metrics panel describing the entity, not the breakdown row", async () => {
      // A breakdown row is a dimension *within* the entity — an operation, a
      // statement — not something a resource attribute identifies. Pinning
      // the panel to it would ask for metrics that cannot exist.
      fetchCatalogEntities.mockImplementation(async (entityType) => {
        if (entityType.identity[0] === "span.name") {
          return {
            entities: [
              group(["GET /health"], 400, 0, 5, 9, "1700000000000000000"),
            ],
            truncated: false,
          };
        }
        return { entities: [], truncated: false };
      });
      renderView({ catalogSecondary: "GET /health" });

      await screen.findByText("Slowest traces");
      await waitFor(() => expect(fetchEntityMetricSeries).toHaveBeenCalled());
      for (const call of fetchEntityMetricSeries.mock.calls) {
        expect(call[1]).toEqual([
          { field: "service.name", value: "gateway" },
          { field: "service.namespace", value: "edge" },
        ]);
      }
    });

    it("is not shown for non-service entity types", async () => {
      renderView(
        { catalogPrimary: compositeKey(["prod", "postgres"]) },
        entityType("database")!,
      );

      await screen.findByText("Slowest traces");
      expect(screen.queryByText("Error groups")).not.toBeInTheDocument();
    });

    it("is hidden at the breakdown drill-in depth", async () => {
      fetchCatalogEntities.mockImplementation(async (entityType) => {
        if (entityType.identity[0] === "span.name") {
          return {
            entities: [
              group(["GET /health"], 400, 0, 5, 9, "1700000000000000000"),
            ],
            truncated: false,
          };
        }
        return { entities: [], truncated: false };
      });
      renderView({ catalogSecondary: "GET /health" });

      await screen.findByText("Slowest traces");
      expect(screen.queryByText("Time by dependency")).not.toBeInTheDocument();
    });
  });
});
