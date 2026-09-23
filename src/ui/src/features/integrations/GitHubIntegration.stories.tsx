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
import { GitHubIntegration } from "./GitHubIntegration";

const tenant = "acme";
const who = sampleWhoami();

function page(routes: JsonRoute[]) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/integrations/github"]}>
          <OutletContextProvider
            value={{ state: { ...DEFAULT_STATE, tenant }, update: () => {} }}
          >
            <Route path="*" element={<GitHubIntegration />} />
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/GitHub Integration",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof page>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Linked: Story = {
  render: () =>
    page([
      irCatchAll,
      { match: "/api/v1/whoami", body: who },
      {
        match: `/api/v1/manage/tenants/${tenant}/github-installations`,
        body: {
          configured: true,
          installations: [
            {
              installation_id: 4821,
              account_login: "acme-corp",
              account_type: "Organization",
              stale: false,
              linked_by_github_login: "alice",
              manage_url:
                "https://github.com/organizations/acme-corp/settings/installations/4821",
              repositories: [
                "acme-corp/storefront",
                "acme-corp/payments-service",
              ],
              repositories_synced_at: "2026-09-22T10:00:00Z",
            },
          ],
        },
      },
    ]),
};

export const NotLinked: Story = {
  render: () =>
    page([
      irCatchAll,
      { match: "/api/v1/whoami", body: who },
      {
        match: `/api/v1/manage/tenants/${tenant}/github-installations`,
        body: { configured: true, installations: [] },
      },
    ]),
};

export const NotConfigured: Story = {
  render: () =>
    page([
      irCatchAll,
      { match: "/api/v1/whoami", body: who },
      {
        match: `/api/v1/manage/tenants/${tenant}/github-installations`,
        body: { configured: false, installations: [] },
      },
    ]),
};
