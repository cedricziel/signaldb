import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { testQueryClient } from "../../lib/queryClient";
import {
  irCatchAll,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { ManagementPanel } from "./ManagementPanel";

const tenant = "acme";

const who = sampleWhoami({
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: true,
  },
});

const routes: JsonRoute[] = [
  irCatchAll,
  {
    match: `/api/v1/manage/tenants/${tenant}/api-keys`,
    body: [
      {
        id: "key-1",
        name: "collector-production",
        scopes: ["metrics:write", "logs:write", "traces:write"],
        dataset_ids: ["production"],
        allowed_origins: [],
        revoked: false,
        created_at: "2026-08-01T12:00:00Z",
      },
      {
        id: "key-2",
        name: "old-agent",
        scopes: ["logs:write"],
        dataset_ids: null,
        allowed_origins: [],
        revoked: true,
        created_at: "2026-05-15T09:30:00Z",
      },
    ],
  },
  {
    match: `/api/v1/manage/tenants/${tenant}/memberships`,
    body: [
      {
        user_id: "user-1",
        email: "alice@example.com",
        role: "admin",
        granted_by: "local",
      },
      {
        user_id: "user-2",
        email: "bob@example.com",
        role: "member",
        granted_by: "local",
      },
      {
        user_id: "user-3",
        email: "carol@example.com",
        role: "viewer",
        granted_by: "oidc_mapping",
      },
    ],
  },
  {
    match: `/api/v1/tenants/${tenant}/tables`,
    body: {
      tenant_id: tenant,
      tables: [
        { name: "traces", dataset: "production", description: "Trace spans" },
        { name: "logs", dataset: "production", description: "Log records" },
        { name: "metrics", dataset: "staging", description: "Metric points" },
      ],
      datasets: [
        {
          dataset: "production",
          tables: [
            {
              name: "traces",
              dataset: "production",
              description: "Trace spans",
            },
            { name: "logs", dataset: "production", description: "Log records" },
          ],
        },
        {
          dataset: "staging",
          tables: [
            {
              name: "metrics",
              dataset: "staging",
              description: "Metric points",
            },
          ],
        },
      ],
    },
  },
];

function ManagePage() {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <ManagementPanel
          who={who}
          onClose={() => {}}
          onTenantCreated={() => {}}
        />
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Manage",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ManagePage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <ManagePage />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <ManagePage />
    </DarkScope>
  ),
};
