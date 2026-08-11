import { screen } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";
import { MemoryRouter, Route, Routes } from "react-router";
import { renderWithClient, stubFetchRoutes } from "../../test/render";
import { SchemaExplorer } from "./SchemaExplorer";

function renderSchemaExplorer() {
  return renderWithClient(
    <MemoryRouter initialEntries={["/schema"]}>
      <Routes>
        <Route path="/schema" element={<SchemaExplorer />} />
        <Route path="/logs" element={<div>Logs page</div>} />
      </Routes>
    </MemoryRouter>,
  );
}

const WHOAMI_INSTANCE_ADMIN = {
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

const WHOAMI_TENANT_ADMIN_ONLY = {
  ...WHOAMI_INSTANCE_ADMIN,
  user: { ...WHOAMI_INSTANCE_ADMIN.user, is_instance_admin: false },
};

const SCHEMA = {
  logical_schema_version: "otel-2026-08",
  logical: [
    {
      source: "traces",
      level: null,
      name: "span.name",
      value_type: "string",
      filterability: "filterable",
      kind: "record_metadata",
      non_native: false,
    },
    {
      source: "traces",
      level: "resource",
      name: "service.name",
      value_type: "string",
      filterability: "filterable",
      kind: "attribute",
      non_native: false,
    },
    {
      source: "logs",
      level: null,
      name: "body",
      value_type: "any_value",
      filterability: "retrieval_only",
      kind: "record_metadata",
      non_native: false,
    },
  ],
  physical: [
    {
      source: "traces",
      version: "physical-v1",
      is_current: false,
      description: "Initial traces schema",
      partition_by: ["timestamp"],
      fields: [
        {
          name: "trace_id",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
      ],
    },
    {
      source: "traces",
      version: "physical-v2",
      is_current: true,
      description: "Adds span_kind",
      partition_by: ["timestamp"],
      fields: [
        {
          name: "trace_id",
          field_type: "string",
          required: true,
          computed: null,
          physical_only: false,
        },
        {
          name: "date_day",
          field_type: "date",
          required: true,
          computed: "date_from_timestamp",
          physical_only: true,
        },
      ],
    },
  ],
};

afterEach(() => {
  vi.restoreAllMocks();
});

describe("SchemaExplorer", () => {
  it("redirects a non-instance-admin to /logs", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_TENANT_ADMIN_ONLY },
    ]);
    renderSchemaExplorer();

    expect(await screen.findByText("Logs page")).toBeInTheDocument();
  });

  it("renders logical fields grouped by source with their qualified name", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: "/api/v1/manage/schema", body: SCHEMA },
    ]);
    renderSchemaExplorer();

    expect(await screen.findByText("span.name")).toBeInTheDocument();
    // Level-prefixed field renders qualified, not bare.
    expect(screen.getByText("resource.service.name")).toBeInTheDocument();
    expect(screen.getByText("otel-2026-08")).toBeInTheDocument();
  });

  it("marks the current physical schema version and shows its columns", async () => {
    stubFetchRoutes([
      { match: "/api/v1/whoami", body: WHOAMI_INSTANCE_ADMIN },
      { match: "/api/v1/manage/schema", body: SCHEMA },
    ]);
    renderSchemaExplorer();

    expect(await screen.findByText("physical-v2")).toBeInTheDocument();
    expect(screen.getByText("current")).toBeInTheDocument();
    expect(screen.getByText("date_from_timestamp")).toBeInTheDocument();
  });
});
