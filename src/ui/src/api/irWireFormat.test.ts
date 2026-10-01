// Pins the exact bytes the UI's IR builders send: `JSON.stringify` keeps key
// order, which `toEqual` ignores, so a typing refactor that reorders or drops
// a key shows up here.
import { describe, expect, it } from "vitest";

import type { EntityTypeDef } from "../features/catalog/entityTypes";
import { emptyQuery } from "../features/metrics/metricQuery";
import { buildIrDocument } from "../features/query/buildIr";
import type { MetricHit } from "../features/schema/api";
import { buildWindowTotalDoc } from "../features/traces/unresolvedGroup";
import { filterStages } from "../lib/traceFilters";
import { buildEntitySourceDoc } from "./catalog";
import { buildEntityStatsDoc } from "./entityDetailStats";
import { buildEntityMetricDocs } from "./entityMetricSeries";
import { buildSparklineDoc } from "./entitySparkline";
import { buildErrorGroupDoc, buildErrorOccurrencesDoc } from "./errors";
import { buildRunsDoc, buildStatsDoc } from "./evals";
import { buildLogRowsDoc, buildLogVolumeDoc } from "./ir/logs";
import { buildMetricIrDoc } from "./ir/metrics";
import { buildOperationSeriesDoc } from "./operationSeries";
import { buildSlowestEndpointsDoc, buildVersionSightingsDoc } from "./overview";
import {
  buildBreakdownDoc,
  buildKpisDoc,
  buildNetworkCorrelateDoc,
  buildTracedShareDoc,
} from "./rum";
import {
  buildBackendCauseRequestsDoc,
  buildRumErrorGroupsDoc,
} from "./rumErrorGroups";
import { buildSessionLogsDoc } from "./rumSessionDetail";
import { buildSessionsListDoc } from "./rumSessions";
import { buildTraceProfilesDoc, buildTraceSpansDoc } from "./traceDetail";
import { buildFacetDoc } from "./traceFacets";
import { buildGroupDoc } from "./traceGroups";
import { buildMembersDoc } from "./traceGroupMembers";
import {
  buildTraceLatencyHeatmapDoc,
  buildTraceVolumeDoc,
} from "./traceVolume";

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

const service: EntityTypeDef = {
  id: "service",
  label: "Services",
  singular: "service",
  identity: ["service.name", "service.namespace"],
  spanKindScope: "Server",
};

const pins = [
  { field: "service.name", value: "api" },
  { field: "service.namespace", value: null },
];

function metric(name: string, instrument: string): MetricHit {
  return {
    name,
    brief: "",
    group_id: `metric.${name}`,
    instrument,
    unit: "1",
    attributes: [],
    entity_associations: ["host"],
    namespace: "otel",
    source: "bundled",
    version: "1.43.0",
  } as MetricHit;
}

const filters = [
  { field: "service.name", value: "api" },
  { field: "kind", value: "Server" },
  { field: "kind", value: "Client" },
  { field: "host.name", value: "", op: "absent" as const },
];

const builders: [string, () => unknown][] = [
  [
    "buildIrDocument",
    () =>
      buildIrDocument({
        source: "logs",
        result: "series",
        range: { from: "now-1h", to: "now" },
        filters: [
          { field: "service.name", op: "eq", value: "api" },
          { field: "body", op: "regex", value: "x", negate: true },
          { field: "trace_id", op: "exists" },
        ],
        aggregate: {
          by: ["service.name"],
          aggs: [{ fn: "count", as: "n" }],
          step: "1m",
        },
        fields: ["body"],
      }),
  ],
  ["filterStages", () => filterStages(filters)],
  ["buildWindowTotalDoc", () => buildWindowTotalDoc(range, filters, "traces")],
  [
    "buildEntitySourceDoc",
    () => buildEntitySourceDoc(service, "traces", range, pins),
  ],
  ["buildEntityStatsDoc", () => buildEntityStatsDoc(service, range, pins)],
  [
    "buildEntityMetricDocs",
    () =>
      buildEntityMetricDocs(
        [
          metric("system.cpu.utilization", "gauge"),
          metric("http.server.request.duration", "histogram"),
        ],
        pins,
        range,
        60,
      ),
  ],
  [
    "buildSparklineDoc",
    () =>
      buildSparklineDoc(
        metric("http.server.request.duration", "histogram"),
        ["host.name"],
        range,
        60,
      ),
  ],
  ["buildErrorGroupDoc", () => buildErrorGroupDoc("traces", range, "api")],
  [
    "buildErrorOccurrencesDoc",
    () =>
      buildErrorOccurrencesDoc(
        {
          source: "logs",
          exceptionType: "TypeError",
          exceptionMessage: null,
          serviceName: "api",
        } as Parameters<typeof buildErrorOccurrencesDoc>[0],
        range,
      ),
  ],
  [
    "buildStatsDoc",
    () =>
      buildStatsDoc(range, { agent: "a", runIds: ["r1", "r2"] }, ["eval.name"]),
  ],
  ["buildRunsDoc", () => buildRunsDoc(range, { agent: "a" })],
  [
    "buildLogRowsDoc",
    () =>
      buildLogRowsDoc(
        [{ label: "service_name", op: "!~", value: "a.*" }],
        "boom",
        range,
        50,
      ),
  ],
  [
    "buildLogVolumeDoc",
    () =>
      buildLogVolumeDoc(
        [{ label: "level", op: "=", value: "error" }],
        "",
        range,
        "1m",
      ),
  ],
  [
    "buildMetricIrDoc",
    () =>
      buildMetricIrDoc(
        {
          ...emptyQuery("a"),
          metric: "signaldb.wal.entries_processed",
          filters: [{ label: "region", op: "!=", value: "eu" }],
        },
        range,
        60,
      ),
  ],
  [
    "buildOperationSeriesDoc",
    () => buildOperationSeriesDoc(service, "span.name", range, pins, 60),
  ],
  [
    "buildSlowestEndpointsDoc",
    () => buildSlowestEndpointsDoc(range, "prod", 5),
  ],
  ["buildVersionSightingsDoc", () => buildVersionSightingsDoc(range, "prod")],
  [
    "buildKpisDoc",
    () =>
      buildKpisDoc("web", range, 30, [
        "sessions",
        "users",
        "sessions_with_errors",
        "page_views",
      ]),
  ],
  ["buildTracedShareDoc", () => buildTracedShareDoc("web", range, 30)],
  [
    "buildBreakdownDoc",
    () =>
      buildBreakdownDoc("web", range, "browser.brands", {
        requireField: true,
        limit: 5,
      }),
  ],
  ["buildNetworkCorrelateDoc", () => buildNetworkCorrelateDoc("web", range)],
  [
    "buildRumErrorGroupsDoc",
    () => buildRumErrorGroupsDoc("web", range, "1.2.3"),
  ],
  [
    "buildBackendCauseRequestsDoc",
    () => buildBackendCauseRequestsDoc("web", range, ["s1", "s2"]),
  ],
  ["buildSessionLogsDoc", () => buildSessionLogsDoc("s1", range, "123")],
  ["buildSessionsListDoc", () => buildSessionsListDoc("web", range, ["s1"])],
  ["buildTraceSpansDoc", () => buildTraceSpansDoc("abc", range)],
  ["buildTraceProfilesDoc", () => buildTraceProfilesDoc("abc", range)],
  ["buildFacetDoc", () => buildFacetDoc("service.name", range, filters)],
  [
    "buildGroupDoc",
    () => buildGroupDoc(["span.name"], range, filters, "traces"),
  ],
  [
    "buildMembersDoc",
    () =>
      buildMembersDoc(
        ["span.name", "http.route"],
        ["GET", null],
        range,
        filters,
        "spans",
        20,
        undefined,
        "Server",
      ),
  ],
  ["buildTraceVolumeDoc", () => buildTraceVolumeDoc(range, "1m", filters)],
  [
    "buildTraceLatencyHeatmapDoc",
    () => buildTraceLatencyHeatmapDoc(range, "1m", filters),
  ],
];

describe("IR wire format", () => {
  it.each(builders)("%s", (_name, build) => {
    expect(JSON.stringify(build())).toMatchSnapshot();
  });
});
