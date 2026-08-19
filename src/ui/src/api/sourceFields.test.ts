import { afterEach, describe, expect, it, vi } from "vitest";
import {
  buildFieldValuesDoc,
  buildSourceFieldsDoc,
  fetchAllSourceFields,
  fetchFieldValueSketch,
  fetchSourceFields,
} from "./sourceFields";
import { runIrQuery } from "./queryIr";

vi.mock("./queryIr", () => ({ runIrQuery: vi.fn() }));

afterEach(() => {
  vi.clearAllMocks();
});

const range = { fromMs: 1_000_000, toMs: 4_600_000 };

function fieldsResponse(names: string[], asOf?: string) {
  return {
    result: "metadata",
    window: { start_ns: 1, end_ns: 2 },
    metadata: {
      kind: "fields",
      fields: names.map((name) => ({ name, type: "string" })),
      cost: { mode: "metadata", ...(asOf ? { as_of: asOf } : {}) },
    },
  };
}

describe("buildSourceFieldsDoc", () => {
  it("asks for a field description, not for data", () => {
    expect(buildSourceFieldsDoc("metrics", range)).toEqual({
      irVersion: 4,
      from: "metrics",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "metadata",
      pipeline: [{ describe: { target: "fields", limit: 500 } }],
    });
  });
});

describe("fetchSourceFields", () => {
  it("returns the source's field names and the metadata's age", async () => {
    vi.mocked(runIrQuery).mockResolvedValue(
      fieldsResponse(
        ["service.name", "process.pid"],
        "2026-08-18 07:14:31",
      ) as never,
    );

    const result = await fetchSourceFields("metrics", range);

    expect([...result.fields]).toEqual(["service.name", "process.pid"]);
    expect(result.asOf).toBe("2026-08-18 07:14:31");
    expect(result.analyzed).toBe(true);
  });

  it("reports a source nothing covers as unanalyzed, not as empty", async () => {
    vi.mocked(runIrQuery).mockResolvedValue(fieldsResponse([]) as never);

    const result = await fetchSourceFields("profiles", range);

    // "no metadata covers this" and "this carries no fields" are different
    // claims: the first is a gap in what we know, the second a fact about
    // the data. Only the second may be rendered as "nothing here".
    expect(result.analyzed).toBe(false);
    expect(result.fields.size).toBe(0);
  });

  it("reports a failing source as unanalyzed rather than propagating", async () => {
    vi.mocked(runIrQuery).mockRejectedValue(new Error("boom"));

    const result = await fetchSourceFields("metrics", range);

    // One signal erroring must not delete every entity type that only that
    // signal carries — nor take the whole catalog down with it.
    expect(result.analyzed).toBe(false);
    expect(result.fields.size).toBe(0);
  });
});

describe("fetchAllSourceFields", () => {
  it("asks every catalog source and keys the answers by source", async () => {
    vi.mocked(runIrQuery).mockImplementation(
      async (doc) =>
        fieldsResponse(
          doc.from === "metrics" ? ["process.pid"] : ["service.name"],
        ) as never,
    );

    const result = await fetchAllSourceFields(range, ["traces", "metrics"]);

    expect([...result.keys()]).toEqual(["traces", "metrics"]);
    expect([...result.get("metrics")!.fields]).toEqual(["process.pid"]);
    expect([...result.get("traces")!.fields]).toEqual(["service.name"]);
  });
});

function valuesResponse(
  values: { value: string; count: number }[],
  asOf?: string,
) {
  return {
    result: "metadata",
    window: { start_ns: 1, end_ns: 2 },
    metadata: {
      kind: "values",
      values: values.map((v) => ({ ...v, origin: "statistics" })),
      cost: {
        mode: "metadata",
        // The sketch describes what compaction last saw, not the queried
        // window. Everything this module claims about it rests on this.
        window_scoped: false,
        approximate: true,
        ...(asOf ? { as_of: asOf } : {}),
      },
    },
  };
}

describe("buildFieldValuesDoc", () => {
  it("asks for a field's values, bounded", () => {
    expect(buildFieldValuesDoc("metrics", "container.name", range)).toEqual({
      irVersion: 4,
      from: "metrics",
      range: { from: "1000000000000", to: "4600000000000" },
      result: "metadata",
      pipeline: [
        { describe: { target: "values", field: "container.name", limit: 5 } },
      ],
    });
  });
});

describe("fetchFieldValueSketch", () => {
  it("reports what the sketch knows, with its age", async () => {
    vi.mocked(runIrQuery).mockResolvedValue(
      valuesResponse(
        [
          { value: "ix-signaldb-otelcol-1", count: 2400 },
          { value: "ix-signaldb-mcp-1", count: 2160 },
        ],
        "2026-08-19 07:13:57",
      ) as never,
    );

    const sketch = await fetchFieldValueSketch(
      "metrics",
      "container.name",
      range,
    );

    expect(sketch).toEqual({
      distinct: 2,
      examples: ["ix-signaldb-otelcol-1", "ix-signaldb-mcp-1"],
      asOf: "2026-08-19 07:13:57",
    });
  });

  it("is undefined when nothing covers the field", async () => {
    // The common case: no declared value set and no maintained statistics.
    // Callers must render this as "we have not looked", never as "there is
    // nothing here" — the distinction is the entire reason to consult it.
    vi.mocked(runIrQuery).mockResolvedValue(valuesResponse([]) as never);

    expect(
      await fetchFieldValueSketch("metrics", "container.name", range),
    ).toBeUndefined();
  });

  it("is undefined when the lookup fails, rather than propagating", async () => {
    vi.mocked(runIrQuery).mockRejectedValue(new Error("boom"));

    expect(
      await fetchFieldValueSketch("metrics", "container.name", range),
    ).toBeUndefined();
  });
});
