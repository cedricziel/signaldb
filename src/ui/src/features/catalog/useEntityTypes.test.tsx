import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { clientWrapper } from "../../test/render";
import * as sourceFieldsApi from "../../api/sourceFields";
import * as schemaApi from "../schema/api";
import { ENTITY_TYPES } from "./entityTypes";
import { toRegistryEntities, useCatalogEntityTypes } from "./useEntityTypes";

vi.mock("../../api/sourceFields", async (importOriginal) => {
  const actual =
    await importOriginal<typeof import("../../api/sourceFields")>();
  return { ...actual, fetchAllSourceFields: vi.fn() };
});
vi.mock("../schema/api", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../schema/api")>();
  return { ...actual, searchEntities: vi.fn() };
});

const fetchAllSourceFields = vi.mocked(sourceFieldsApi.fetchAllSourceFields);
const searchEntities = vi.mocked(schemaApi.searchEntities);

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

beforeEach(() => {
  fetchAllSourceFields.mockReset();
  searchEntities.mockReset();
  searchEntities.mockResolvedValue([]);
});

afterEach(() => {
  vi.restoreAllMocks();
});

function render() {
  return renderHook(() => useCatalogEntityTypes(range, "r1"), {
    wrapper: clientWrapper(),
  });
}

describe("toRegistryEntities", () => {
  it("narrows registry hits to name and both attribute roles", () => {
    // Descriptive attributes come along because they are the fallback
    // identity for the 28 OTel entity types that declare no identifying
    // attribute at all — dropping them here would silently un-catalogue
    // hosts and containers.
    expect(
      toRegistryEntities([
        { name: "process", identifying: [{ key: "process.pid" }] },
        { name: "host", descriptive: [{ key: "host.name" }] },
      ]),
    ).toEqual([
      { name: "process", identifying: ["process.pid"], descriptive: [] },
      { name: "host", identifying: [], descriptive: ["host.name"] },
    ]);
  });
});

describe("useCatalogEntityTypes", () => {
  it("names the registry entity even when no source has been analyzed", async () => {
    // Which metrics measure a host is a fact about the registry, not about
    // what compaction has indexed. Withholding the registry name until field
    // metadata lands would leave an un-compacted deployment with no metrics
    // panel and no sparklines over data it is already storing.
    fetchAllSourceFields.mockResolvedValue(new Map());
    searchEntities.mockResolvedValue([
      {
        name: "host",
        brief: "",
        group_id: "entity.host",
        identifying: [],
        descriptive: [{ key: "host.name", role: "descriptive" }],
      },
    ] as never);

    const { result } = render();

    // `analyzed` is already false before either fetch lands, so waiting on it
    // would assert against the pre-fetch state.
    await waitFor(() =>
      expect(
        result.current.types.find((e) => e.id === "host")?.registryEntity,
      ).toBe("host"),
    );
    expect(result.current.analyzed).toBe(false);
  });

  it("keeps the entity types some source carries", async () => {
    fetchAllSourceFields.mockResolvedValue(
      new Map([
        [
          "traces",
          {
            fields: new Set(["service.name"]),
            analyzed: true,
            asOf: "2026-08-18 07:00:00",
          },
        ],
        [
          "metrics",
          {
            fields: new Set(["process.pid"]),
            analyzed: true,
            asOf: "2026-08-18 06:00:00",
          },
        ],
      ]),
    );

    const { result } = render();

    await waitFor(() => expect(result.current.isPending).toBe(false));
    const ids = result.current.types.map((e) => e.id);
    expect(ids).toContain("service");
    expect(ids).toContain("process");
    expect(ids).not.toContain("k8s_pod");
    expect(result.current.analyzed).toBe(true);
    // The oldest stamp, not the newest: one stale source makes the merged
    // answer that stale.
    expect(result.current.asOf).toBe("2026-08-18 06:00:00");
  });

  it("falls back to the curated list when no source reports metadata", async () => {
    // A deployment that has never been compacted, or describe calls that
    // failed. Filtering on that emptiness would blank the nav and report a
    // working deployment as having no entities at all.
    fetchAllSourceFields.mockResolvedValue(
      new Map([["traces", { fields: new Set<string>(), analyzed: false }]]),
    );

    const { result } = render();

    await waitFor(() => expect(result.current.isPending).toBe(false));
    expect(result.current.types).toEqual(ENTITY_TYPES);
    expect(result.current.analyzed).toBe(false);
  });

  it("adds a registry-only entity type that a source carries", async () => {
    searchEntities.mockResolvedValue([
      {
        name: "service.instance",
        identifying: [{ key: "service.instance.id", role: "identifying" }],
      },
    ] as never);
    fetchAllSourceFields.mockResolvedValue(
      new Map([
        [
          "metrics",
          { fields: new Set(["service.instance.id"]), analyzed: true },
        ],
      ]),
    );

    const { result } = render();

    await waitFor(() => expect(result.current.isPending).toBe(false));
    expect(result.current.types.map((e) => e.id)).toContain("service_instance");
  });
});
