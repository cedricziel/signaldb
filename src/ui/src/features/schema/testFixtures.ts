// Shared fixtures for the schema hub tests: whoami variants, a registry
// list, a small Weaver-format registry document, and resolution responses.
// Shapes mirror the generated types in `api/gen/types.gen.ts`.

export const WHOAMI_INSTANCE_ADMIN = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: true,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

export const WHOAMI_TENANT_ADMIN = {
  ...WHOAMI_INSTANCE_ADMIN,
  user: { ...WHOAMI_INSTANCE_ADMIN.user, is_instance_admin: false },
};

export const WHOAMI_MEMBER = {
  ...WHOAMI_TENANT_ADMIN,
  memberships: [{ tenant_id: "acme", role: "member" }],
};

export const REGISTRIES = {
  registries: [
    {
      namespace: "acme",
      version: "1.0.0",
      source: "custom",
      read_only: false,
      attribute_count: 5,
      entity_count: 2,
      metric_count: 1,
      updated_at: "2026-08-14T10:00:00Z",
      description: "Acme Corp conventions layered on OpenTelemetry semconv.",
    },
    {
      namespace: "signaldb",
      version: "0.1.0",
      source: "bundled",
      read_only: true,
      attribute_count: 12,
      entity_count: 0,
      metric_count: 0,
      updated_at: null,
    },
    {
      namespace: "otel",
      version: "1.43.0",
      source: "bundled",
      read_only: true,
      attribute_count: 921,
      entity_count: 40,
      metric_count: 310,
      updated_at: null,
    },
  ],
};

export const REGISTRIES_BUNDLED_ONLY = {
  registries: REGISTRIES.registries.filter((r) => r.source === "bundled"),
};

/** A trimmed `otel`-shaped document: k8s.pod attributes, entity, metric. */
export const OTEL_DOCUMENT = {
  name: "otel",
  version: "1.43.0",
  schema_url: "https://opentelemetry.io/schemas/1.43.0",
  groups: [
    {
      id: "registry.k8s",
      type: "attribute_group",
      display_name: "Kubernetes Attributes",
      brief: "Kubernetes resource attributes.",
      attributes: [
        {
          id: "k8s.pod.uid",
          type: "string",
          stability: "stable",
          brief: "The UID of the Pod.",
          examples: ["275ecb36-5aa8-4c2a-9c47-d8bb681b9aff"],
        },
        {
          id: "k8s.pod.name",
          type: "string",
          stability: "stable",
          brief: "The name of the Pod.",
          examples: ["opentelemetry-pod-autoconf"],
        },
        {
          id: "k8s.pod.label",
          type: "template[string]",
          stability: "development",
          brief: "The label placed on the Pod.",
        },
        {
          id: "k8s.namespace.name",
          type: "string",
          stability: "stable",
          brief: "The name of the namespace that the pod is running in.",
        },
      ],
    },
    {
      id: "registry.service",
      type: "attribute_group",
      display_name: "Service Attributes",
      brief: "A service instance.",
      attributes: [
        {
          id: "service.name",
          type: "string",
          stability: "stable",
          requirement_level: "required",
          brief: "Logical name of the service.",
          note: "MUST be the same for all instances of horizontally scaled services.",
          examples: ["shoppingcart"],
        },
      ],
    },
    {
      id: "entity.k8s.pod",
      type: "entity",
      name: "k8s.pod",
      stability: "development",
      brief: "A Kubernetes Pod object.",
      attributes: [
        {
          ref: "k8s.pod.uid",
          role: "identifying",
          requirement_level: "required",
        },
        { ref: "k8s.pod.name", role: "descriptive" },
        { ref: "k8s.pod.label", role: "descriptive" },
      ],
    },
    {
      id: "entity.service",
      type: "entity",
      name: "service",
      stability: "development",
      brief: "A service instance.",
      attributes: [
        {
          ref: "service.name",
          role: "identifying",
          requirement_level: "required",
        },
      ],
    },
    {
      id: "metric.k8s.pod.cpu.time",
      type: "metric",
      metric_name: "k8s.pod.cpu.time",
      instrument: "counter",
      unit: "s",
      stability: "development",
      brief: "Total CPU time consumed.",
      entity_associations: ["k8s.pod"],
      attributes: [],
    },
  ],
};

export const OTEL_REGISTRY = {
  ...REGISTRIES.registries[2],
  document: OTEL_DOCUMENT,
};

export const ACME_DOCUMENT = {
  name: "acme",
  version: "1.0.0",
  schema_url: "https://acme.example/schemas/1.0.0",
  description: "Acme Corp conventions layered on OpenTelemetry semconv.",
  groups: [
    {
      id: "registry.acme.order",
      type: "attribute_group",
      display_name: "Acme Order Attributes",
      brief: "Attributes describing an Acme order.",
      attributes: [
        {
          id: "acme.order.id",
          type: "string",
          stability: "development",
          brief: "Internal order identifier.",
          examples: ["ord_8f21a"],
        },
        {
          id: "acme.order.total",
          type: "double",
          stability: "development",
          brief: "Order total in the tenant's billing currency.",
        },
        {
          id: "service.name",
          type: "string",
          stability: "development",
          brief: "Our service naming: <team>-<system>, see naming ADR-12.",
          examples: ["payments-checkout"],
        },
      ],
    },
    {
      id: "entity.acme.order",
      type: "entity",
      name: "acme.order",
      stability: "development",
      brief: "A customer order flowing through Acme's checkout.",
      attributes: [
        {
          ref: "acme.order.id",
          role: "identifying",
          requirement_level: "required",
        },
        { ref: "acme.order.total", role: "descriptive" },
      ],
    },
    {
      id: "metric.acme.orders.placed",
      type: "metric",
      metric_name: "acme.orders.placed",
      instrument: "counter",
      unit: "{order}",
      stability: "development",
      brief: "Orders placed.",
      entity_associations: ["acme.order"],
      attributes: [{ ref: "acme.order.total" }],
    },
  ],
};

export const ACME_REGISTRY = {
  ...REGISTRIES.registries[0],
  document: ACME_DOCUMENT,
};

const OTEL_SERVICE_NAME_HIT = {
  key: "service.name",
  type: "string",
  group_id: "registry.service",
  group_display_name: "Service Attributes",
  brief: "Logical name of the service.",
  note: "MUST be the same for all instances of horizontally scaled services.",
  examples: ["shoppingcart"],
  stability: "stable",
  requirement_level: "required",
  entity_roles: [{ namespace: "otel", entity: "service", role: "identifying" }],
  namespace: "otel",
  version: "1.43.0",
  source: "bundled",
};

const ACME_SERVICE_NAME_HIT = {
  key: "service.name",
  type: "string",
  group_id: "registry.acme.order",
  group_display_name: "Acme Order Attributes",
  brief: "Our service naming: <team>-<system>, see naming ADR-12.",
  examples: ["payments-checkout"],
  stability: "development",
  entity_roles: [{ namespace: "otel", entity: "service", role: "identifying" }],
  namespace: "acme",
  version: "1.0.0",
  source: "custom",
};

/** `service.name` defined by both acme (primary) and otel. */
export const SERVICE_NAME_RESOLUTION = {
  key: "service.name",
  primary: ACME_SERVICE_NAME_HIT,
  hits: [ACME_SERVICE_NAME_HIT, OTEL_SERVICE_NAME_HIT],
};

export const K8S_POD_UID_RESOLUTION = {
  key: "k8s.pod.uid",
  primary: {
    key: "k8s.pod.uid",
    type: "string",
    group_id: "registry.k8s",
    group_display_name: "Kubernetes Attributes",
    brief: "The UID of the Pod.",
    examples: ["275ecb36-5aa8-4c2a-9c47-d8bb681b9aff"],
    stability: "stable",
    entity_roles: [
      { namespace: "otel", entity: "k8s.pod", role: "identifying" },
    ],
    namespace: "otel",
    version: "1.43.0",
    source: "bundled",
  },
  hits: [] as unknown[],
};
K8S_POD_UID_RESOLUTION.hits = [K8S_POD_UID_RESOLUTION.primary];

export const EMPTY_RESOLUTION = (key: string) => ({
  key,
  primary: null,
  hits: [],
});

export const K8S_POD_ENTITY_RESOLUTION = {
  key: "k8s.pod",
  primary: {
    name: "k8s.pod",
    group_id: "entity.k8s.pod",
    brief: "A Kubernetes Pod object.",
    stability: "development",
    identifying: [
      {
        key: "k8s.pod.uid",
        role: "identifying",
        requirement_level: "required",
      },
    ],
    descriptive: [
      { key: "k8s.pod.name", role: "descriptive" },
      { key: "k8s.pod.label", role: "descriptive" },
    ],
    metrics: ["k8s.pod.cpu.time"],
    extended_by: ["acme/acme.k8s.pod"],
    namespace: "otel",
    version: "1.43.0",
    source: "bundled",
  },
  hits: [] as unknown[],
};
K8S_POD_ENTITY_RESOLUTION.hits = [K8S_POD_ENTITY_RESOLUTION.primary];

export const K8S_POD_CPU_TIME_RESOLUTION = {
  key: "k8s.pod.cpu.time",
  primary: {
    name: "k8s.pod.cpu.time",
    group_id: "metric.k8s.pod.cpu.time",
    brief: "Total CPU time consumed.",
    instrument: "counter",
    unit: "s",
    stability: "development",
    attributes: [],
    entity_associations: ["k8s.pod"],
    namespace: "otel",
    version: "1.43.0",
    source: "bundled",
  },
  hits: [] as unknown[],
};
K8S_POD_CPU_TIME_RESOLUTION.hits = [K8S_POD_CPU_TIME_RESOLUTION.primary];

export const VALIDATION_OK = {
  namespace: "acme",
  version: "1.0.0",
  attribute_count: 3,
  entity_count: 1,
  metric_count: 1,
  errors: [],
};

export const VALIDATION_FAILED = {
  namespace: "acme",
  version: "1.0.0",
  attribute_count: 3,
  entity_count: 1,
  metric_count: 1,
  errors: [
    {
      path: "groups[1].attributes[0].ref",
      message: "unresolvable attribute ref `acme.order.missing`",
    },
  ],
};
