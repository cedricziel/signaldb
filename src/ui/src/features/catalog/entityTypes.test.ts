import { describe, expect, it } from "vitest";
import { ENTITY_TYPES, RESOURCE_SOURCES, entityType } from "./entityTypes";

describe("ENTITY_TYPES", () => {
  it("has a unique id for every entity type", () => {
    const ids = ENTITY_TYPES.map((e) => e.id);
    expect(new Set(ids).size).toBe(ids.length);
  });

  it("gives every entity type at least one identity dimension", () => {
    for (const e of ENTITY_TYPES) {
      expect(e.identity.length).toBeGreaterThan(0);
    }
  });

  it("scopes only the service entity type to server-kind spans", () => {
    const scoped = ENTITY_TYPES.filter((e) => e.spanKindScope !== undefined);
    expect(scoped.map((e) => e.id)).toEqual(["service"]);
    expect(scoped[0]?.spanKindScope).toBe("Server");
  });

  it("registers service as the first (default) entity type", () => {
    expect(ENTITY_TYPES[0]?.id).toBe("service");
  });

  it("gives each entity type with a breakdown a real, distinct dimension", () => {
    for (const e of ENTITY_TYPES) {
      if (!e.breakdown) continue;
      // The breakdown dimension must differ from every identity dimension,
      // or it would just repeat a column the entity is already keyed by.
      expect(e.identity).not.toContain(e.breakdown.field);
    }
  });

  it("breaks services down by operation, inheriting the server-kind scope", () => {
    expect(entityType("service")?.breakdown).toEqual({
      field: "span.name",
      label: "Operations",
    });
  });

  it("breaks infrastructure entity types down by the services observed on them", () => {
    for (const id of ["host", "k8s_pod", "k8s_node", "container", "process"]) {
      expect(entityType(id)?.breakdown).toEqual({
        field: "service.name",
        label: "Services",
      });
    }
  });

  it("discovers resource-scoped entity types from every signal", () => {
    for (const id of [
      "service",
      "host",
      "k8s_pod",
      "k8s_node",
      "container",
      "process",
    ]) {
      expect(entityType(id)?.sources).toEqual(RESOURCE_SOURCES);
    }
  });

  it("counts every queryable signal as carrying resource attributes", () => {
    // A resource attribute rides on everything an SDK emits, so leaving a
    // signal out of this list hides any entity that only reports through it —
    // which is exactly what kept processes (`process.pid`, a metrics-only
    // attribute in practice) invisible while their data was already stored.
    expect(RESOURCE_SOURCES).toEqual([
      "traces",
      "logs",
      "metrics",
      "metrics_histogram",
      "profiles",
    ]);
  });

  it("leaves span-attribute entity types trace-only", () => {
    for (const id of ["database", "messaging_destination"]) {
      expect(entityType(id)?.sources).toBeUndefined();
    }
  });
});

describe("entityType", () => {
  it("looks up a registered entity type by id", () => {
    expect(entityType("database")?.label).toBe("Databases");
  });

  it("returns undefined for an unknown id", () => {
    expect(entityType("not-a-real-type")).toBeUndefined();
  });
});
