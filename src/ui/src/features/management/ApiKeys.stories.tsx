import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter, Route } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import { OutletContextProvider } from "../../stories/OutletContextProvider";
import {
  irCatchAll,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { ApiKeys } from "./ApiKeys";

const tenant = "acme";
const who = sampleWhoami();

const routes: JsonRoute[] = [
  irCatchAll,
  { match: `/api/v1/whoami`, body: who },
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
        name: "ci-smoke-tests",
        scopes: ["schema:read"],
        dataset_ids: null,
        allowed_origins: ["https://ci.example.com"],
        revoked: false,
        created_at: "2026-07-10T08:15:00Z",
      },
      {
        id: "key-3",
        name: "old-agent",
        scopes: ["logs:write"],
        dataset_ids: null,
        allowed_origins: [],
        revoked: true,
        created_at: "2026-05-15T09:30:00Z",
      },
    ],
  },
];

function ApiKeysPage() {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/api-keys"]}>
          <OutletContextProvider
            value={{ state: { ...DEFAULT_STATE, tenant }, update: () => {} }}
          >
            <Route path="*" element={<ApiKeys />} />
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/API Keys",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ApiKeysPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <ApiKeysPage />,
};
