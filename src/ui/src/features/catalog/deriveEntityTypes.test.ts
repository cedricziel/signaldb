import { describe, expect, it } from "vitest";
import {
  deriveEntityTypes,
  observedEntityTypes,
  type RegistryEntity,
} from "./deriveEntityTypes";
import { ENTITY_TYPES, entityType } from "./entityTypes";

const registry: RegistryEntity[] = [
  { name: "service", identifying: ["service.name"] },
  { name: "process", identifying: ["process.pid", "process.creation.time"] },
  { name: "k8s.pod", identifying: ["k8s.pod.uid"] },
  { name: "service.instance", identifying: ["service.instance.id"] },
  { name: "telemetry.sdk", identifying: ["telemetry.sdk.name"] },
  // The registry models these, but every attribute they carry is
  // descriptive — see the fallback tests below.
  { name: "host", identifying: [] },
  { name: "container", identifying: [] },
];

describe("deriveEntityTypes", () => {
  it("adds entity types the curated list never mentioned", () => {
    const ids = deriveEntityTypes(registry).map((e) => e.id);
    expect(ids).toContain("service_instance");
    expect(ids).toContain("telemetry_sdk");
  });

  it("maps a dotted registry name onto an underscored route id", () => {
    const derived = deriveEntityTypes([
      { name: "cicd.pipeline.run", identifying: ["cicd.pipeline.run.id"] },
    ]);
    expect(derived.map((e) => e.id)).toContain("cicd_pipeline_run");
  });

  it("labels a registry-only type from its own name", () => {
    const sdk = deriveEntityTypes(registry).find(
      (e) => e.id === "telemetry_sdk",
    );
    expect(sdk?.label).toBe("Telemetry sdks");
    expect(sdk?.singular).toBe("telemetry sdk");
  });

  it("keeps the curated identity where one exists, over the registry's", () => {
    // The registry identifies a pod by `k8s.pod.uid`, which is unique but
    // opaque and frequently absent from real telemetry; the curated identity
    // is the readable name plus its namespace. Changing it would regress a
    // page that works, so the curated one wins.
    const pod = deriveEntityTypes(registry).find((e) => e.id === "k8s_pod");
    expect(pod?.identity).toEqual(entityType("k8s_pod")?.identity);
    expect(pod?.identity).not.toContain("k8s.pod.uid");
  });

  it("keeps curated presentation that the registry cannot express", () => {
    const service = deriveEntityTypes(registry).find((e) => e.id === "service");
    expect(service?.label).toBe("Services");
    expect(service?.spanKindScope).toBe("Server");
    expect(service?.breakdown).toEqual({
      field: "span.name",
      label: "Operations",
    });
  });

  it("catalogs an entity the registry leaves unidentified but we curate", () => {
    // `host` and `container` declare every attribute as descriptive —
    // `host.name` is not an identifying attribute in the OTel registry. A
    // naive "drop anything with no identifying attribute" would delete two
    // entity types that work today.
    const ids = deriveEntityTypes(registry).map((e) => e.id);
    expect(ids).toContain("host");
    expect(ids).toContain("container");
    expect(
      deriveEntityTypes(registry).find((e) => e.id === "host")?.identity,
    ).toEqual(["host.name"]);
  });

  it("drops a registry entity with neither an identity nor a curated one", () => {
    const ids = deriveEntityTypes([{ name: "browser", identifying: [] }]).map(
      (e) => e.id,
    );
    expect(ids).not.toContain("browser");
  });

  it("keeps curated types the registry does not model at all", () => {
    // `database` and `messaging_destination` are keyed by span attributes,
    // which the entity registry has no concept of.
    const ids = deriveEntityTypes(registry).map((e) => e.id);
    expect(ids).toContain("database");
    expect(ids).toContain("messaging_destination");
  });

  it("lists curated types first, in their curated order", () => {
    const ids = deriveEntityTypes(registry).map((e) => e.id);
    expect(ids.slice(0, ENTITY_TYPES.length)).toEqual(
      ENTITY_TYPES.map((e) => e.id),
    );
  });
});

describe("observedEntityTypes", () => {
  const fields = new Map<string, Set<string>>([
    ["traces", new Set(["service.name", "host.name", "db.namespace"])],
    ["logs", new Set(["service.name", "host.name"])],
    ["metrics", new Set(["service.name", "process.pid", "container.name"])],
    ["profiles", new Set(["service.name"])],
  ]);

  const derived = deriveEntityTypes(registry);

  it("keeps a type whose primary identity attribute any source carries", () => {
    const ids = observedEntityTypes(derived, fields).map((e) => e.id);
    expect(ids).toContain("service");
    expect(ids).toContain("process");
    expect(ids).toContain("container");
  });

  it("drops a type no source carries, rather than showing an empty tab", () => {
    const ids = observedEntityTypes(derived, fields).map((e) => e.id);
    expect(ids).not.toContain("k8s_pod");
    expect(ids).not.toContain("telemetry_sdk");
  });

  it("narrows each type's sources to those that carry its identity", () => {
    const observed = observedEntityTypes(derived, fields);
    // Nothing but metrics carries `process.pid`, so querying traces, logs
    // and profiles for processes is four round trips that cannot match.
    expect(observed.find((e) => e.id === "process")?.sources).toEqual([
      "metrics",
    ]);
    expect(observed.find((e) => e.id === "service")?.sources).toEqual([
      "traces",
      "logs",
      "metrics",
      "profiles",
    ]);
  });

  it("keeps a span-attribute type scoped to the sources it declared", () => {
    const database = observedEntityTypes(derived, fields).find(
      (e) => e.id === "database",
    );
    expect(database?.sources).toEqual(["traces"]);
  });

  it("reports nothing observed when no source carries any identity", () => {
    expect(observedEntityTypes(derived, new Map())).toEqual([]);
  });
});
