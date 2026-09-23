import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import { OutletContextProvider } from "../../stories/OutletContextProvider";
import {
  irCatchAll,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { schemaRoutes } from "./routes";

const who = sampleWhoami({
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: true,
  },
});

const registries = {
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

const acmeDocument = {
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

const acmeRegistry = { ...registries.registries[0], document: acmeDocument };

const managedSchema = {
  logical_schema_version: "2026-08-01",
  logical: [
    {
      name: "service.name",
      level: null,
      source: "core",
      value_type: "string",
      kind: "dimension",
      filterability: "filterable",
      non_native: false,
    },
    {
      name: "status_code",
      level: "span",
      source: "otel",
      value_type: "int64",
      kind: "dimension",
      filterability: "filterable",
      non_native: false,
    },
    {
      name: "order.id",
      level: "span",
      source: "custom",
      value_type: "string",
      kind: "dimension",
      filterability: "retrieval_only",
      non_native: true,
    },
  ],
  physical: [
    {
      source: "traces",
      version: "v2",
      is_current: true,
      description: "Current trace span layout.",
      partition_by: ["service.name"],
      fields: [
        {
          name: "trace_id",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
        {
          name: "span_id",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
        {
          name: "service.name",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
        {
          name: "duration_ns",
          field_type: "int64",
          required: false,
          computed: "end_ns - start_ns",
          physical_only: true,
        },
      ],
    },
    {
      source: "logs",
      version: "v1",
      is_current: true,
      description: "Current log record layout.",
      partition_by: ["service.name"],
      fields: [
        {
          name: "timestamp",
          field_type: "timestamp",
          required: true,
          computed: null,
          physical_only: false,
        },
        {
          name: "body",
          field_type: "string",
          required: false,
          computed: null,
          physical_only: false,
        },
        {
          name: "service.name",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
      ],
    },
  ],
};

function SchemaPage({
  routes,
  initialEntries,
}: {
  routes: JsonRoute[];
  initialEntries: string[];
}) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={initialEntries}>
          <OutletContextProvider
            value={{
              state: { ...DEFAULT_STATE, tenant: "acme" },
              update: () => {},
            }}
          >
            {schemaRoutes()}
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Schema",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof SchemaPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const baseRoutes: JsonRoute[] = [
  irCatchAll,
  { match: "/api/v1/whoami", body: who },
  { match: "/api/v1/schema/registries", body: registries },
];

export const Conventions: Story = {
  render: () => (
    <SchemaPage initialEntries={["/schema/conventions"]} routes={baseRoutes} />
  ),
};

export const RegistryBrowser: Story = {
  render: () => (
    <SchemaPage
      initialEntries={["/schema/conventions/acme/1.0.0"]}
      routes={[
        ...baseRoutes,
        { match: "/api/v1/schema/registries/acme/1.0.0", body: acmeRegistry },
      ]}
    />
  ),
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <SchemaPage
        initialEntries={["/schema/conventions/acme/1.0.0"]}
        routes={[
          ...baseRoutes,
          { match: "/api/v1/schema/registries/acme/1.0.0", body: acmeRegistry },
        ]}
      />
    </DarkScope>
  ),
};

export const RegistryEditor: Story = {
  render: () => (
    <SchemaPage
      initialEntries={["/schema/conventions/acme/1.0.0/edit"]}
      routes={[
        ...baseRoutes,
        { match: "/api/v1/schema/registries/acme/1.0.0", body: acmeRegistry },
      ]}
    />
  ),
};

export const StorageExplorer: Story = {
  render: () => (
    <SchemaPage
      initialEntries={["/schema/storage"]}
      routes={[
        ...baseRoutes,
        { match: "/api/v1/schema", body: managedSchema },
      ]}
    />
  ),
};
