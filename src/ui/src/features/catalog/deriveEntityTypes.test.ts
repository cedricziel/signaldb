import { describe, expect, it } from "vitest";
import {
  deriveEntityTypes,
  ENTITY_TYPE_BUDGET,
  observedEntityTypes,
  type RegistryEntity,
} from "./deriveEntityTypes";
import { ENTITY_TYPES, entityType } from "./entityTypes";

const registry: RegistryEntity[] = [
  { name: "service", identifying: ["service.name"], descriptive: [] },
  {
    name: "process",
    identifying: ["process.pid", "process.creation.time"],
    descriptive: ["process.command"],
  },
  {
    name: "k8s.pod",
    identifying: ["k8s.pod.uid"],
    descriptive: ["k8s.pod.name"],
  },
  {
    name: "service.instance",
    identifying: ["service.instance.id"],
    descriptive: [],
  },
  { name: "telemetry.sdk", identifying: ["telemetry.sdk.name"], descriptive: [] },
  // The registry models these with no identifying attribute at all — every
  // attribute they carry is descriptive. OTel 1.43 really is like this.
  {
    name: "host",
    identifying: [],
    descriptive: ["host.id", "host.name", "host.type"],
  },
  {
    name: "container",
    identifying: [],
    descriptive: ["container.name", "container.id"],
  },
];

describe("deriveEntityTypes", () => {
  it("adds entity types the curated list never mentioned", () => {
    const ids = deriveEntityTypes(registry).map((e) => e.id);
    expect(ids).toContain("service_instance");
    expect(ids).toContain("telemetry_sdk");
  });

  it("maps a dotted registry name onto an underscored route id", () => {
    const derived = deriveEntityTypes([
      {
        name: "cicd.pipeline.run",
        identifying: ["cicd.pipeline.run.id"],
        descriptive: [],
      },
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
    const ids = deriveEntityTypes([
      { name: "browser", identifying: [], descriptive: [] },
    ]).map(
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

describe("identity resolved from the schema", () => {
  it("uses the identifying attributes the data actually carries", () => {
    // A registry-only type, so nothing overrides its identity. Both keys are
    // declared identifying; only one is carried, and the absent one is
    // dropped rather than grouping every instance under a single null.
    const fields = new Map([["traces", new Set(["cicd.pipeline.name"])]]);
    const pipeline = observedEntityTypes(
      deriveEntityTypes([
        {
          name: "cicd.pipeline",
          identifying: ["cicd.pipeline.name", "cicd.pipeline.run.id"],
          descriptive: [],
        },
      ]),
      fields,
    ).find((e) => e.id === "cicd_pipeline");
    expect(pipeline?.identity).toEqual(["cicd.pipeline.name"]);
  });

  it("falls back to a descriptive attribute when the registry identifies nothing", () => {
    // `host` declares no identifying attribute in OTel 1.43. Without a
    // fallback the entity type would be undiscoverable — which is why this
    // used to need a hand-written identity per type.
    const fields = new Map([["traces", new Set(["host.name"])]]);
    const host = observedEntityTypes(deriveEntityTypes(registry), fields).find(
      (e) => e.id === "host",
    );
    expect(host?.identity).toEqual(["host.name"]);
  });

  it("prefers the declared identity over a descriptive one when both are carried", () => {
    const registryOnly: RegistryEntity[] = [
      {
        name: "k8s.replicaset",
        identifying: ["k8s.replicaset.uid"],
        descriptive: ["k8s.replicaset.name"],
      },
    ];
    const fields = new Map([
      ["traces", new Set(["k8s.replicaset.uid", "k8s.replicaset.name"])],
    ]);
    const rs = observedEntityTypes(deriveEntityTypes(registryOnly), fields).find(
      (e) => e.id === "k8s_replicaset",
    );
    expect(rs?.identity).toEqual(["k8s.replicaset.uid"]);
  });

  it("falls back when the declared identity is absent from the data", () => {
    // A uid is unique but frequently absent; the name is what real telemetry
    // carries. Falling back keeps the entity type usable instead of dropping
    // it for want of an attribute nobody sends.
    const registryOnly: RegistryEntity[] = [
      {
        name: "k8s.replicaset",
        identifying: ["k8s.replicaset.uid"],
        descriptive: ["k8s.replicaset.name"],
      },
    ];
    const fields = new Map([["traces", new Set(["k8s.replicaset.name"])]]);
    const rs = observedEntityTypes(deriveEntityTypes(registryOnly), fields).find(
      (e) => e.id === "k8s_replicaset",
    );
    expect(rs?.identity).toEqual(["k8s.replicaset.name"]);
  });

  it("keeps the scoping attribute on a process, whose pid is per-host", () => {
    const fields = new Map([
      ["metrics", new Set(["process.pid", "host.name"])],
    ]);
    const process = observedEntityTypes(
      deriveEntityTypes(registry),
      fields,
    ).find((e) => e.id === "process");
    expect(process?.identity).toEqual(["process.pid", "host.name"]);
  });

  it("drops a type when neither its declared nor its descriptive identity is carried", () => {
    const fields = new Map([["traces", new Set(["service.name"])]]);
    const ids = observedEntityTypes(deriveEntityTypes(registry), fields).map(
      (e) => e.id,
    );
    expect(ids).not.toContain("container");
  });

  it("keeps a scoping attribute the registry does not model", () => {
    // A pid is unique only within a host and a service name only within a
    // namespace; the registry's identity model has no way to say so. Where
    // the catalog adds a scoping attribute it is a correctness fix, so it
    // must survive schema-derived identity.
    const fields = new Map([
      ["traces", new Set(["service.name", "service.namespace"])],
    ]);
    const service = observedEntityTypes(deriveEntityTypes(registry), fields).find(
      (e) => e.id === "service",
    );
    expect(service?.identity).toEqual(["service.name", "service.namespace"]);
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

  it("bounds how many entity types a catalog paint can fan out over", () => {
    // The nav issues one query per entity type per source it carries. The
    // registry is served with a limit of 200, so an unbounded list is an
    // unbounded number of concurrent requests from a single page paint —
    // the ceiling the hand-written list used to provide by construction.
    const many: RegistryEntity[] = Array.from({ length: 200 }, (_, i) => ({
      name: `synthetic.type${i}`,
      identifying: [`synthetic.type${i}.id`],
      descriptive: [],
    }));
    const everything = new Map([
      [
        "traces",
        new Set(many.map((e) => e.identifying[0]!).concat("service.name")),
      ],
    ]);

    const observed = observedEntityTypes(deriveEntityTypes(many), everything);

    expect(observed.length).toBeLessThanOrEqual(ENTITY_TYPE_BUDGET);
  });

  it("keeps curated types when the budget truncates registry ones", () => {
    // Curated types lead the list, so truncation drops the tail — the
    // registry-derived long tail — rather than the eight pages that carry
    // the presentation work.
    const many: RegistryEntity[] = Array.from({ length: 200 }, (_, i) => ({
      name: `synthetic.type${i}`,
      identifying: [`synthetic.type${i}.id`],
      descriptive: [],
    }));
    const everything = new Map([
      [
        "traces",
        new Set(many.map((e) => e.identifying[0]!).concat("service.name")),
      ],
    ]);

    const ids = observedEntityTypes(deriveEntityTypes(many), everything).map(
      (e) => e.id,
    );

    expect(ids).toContain("service");
  });
});
