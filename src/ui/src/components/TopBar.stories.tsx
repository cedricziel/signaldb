import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../lib/queryClient";
import { DEFAULT_STATE } from "../lib/urlState";
import type { WhoamiResponse } from "../api/session";
import type { JsonRoute } from "../stories/fetchStub";
import { StoryFetchStub } from "../stories/StoryFetchStub";
import { TopBar } from "./TopBar";

const WHO: WhoamiResponse = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [
    { id: "production", slug: "production", is_default: true },
    { id: "staging", slug: "staging", is_default: false },
  ],
  default_dataset: "production",
};

const CURRENT_SESSION = {
  user: WHO.user,
  tenant: "acme",
  dataset: "production",
  memberships: WHO.memberships,
};

function routesFor(isDemo: boolean): JsonRoute[] {
  return [
    { match: "/api/v1/whoami", body: WHO },
    {
      match: "/ui/session",
      method: "GET",
      body: { ...CURRENT_SESSION, user: { ...WHO.user, is_demo: isDemo } },
    },
  ];
}

function TopBarStub({ isDemo }: { isDemo: boolean }) {
  return (
    <StoryFetchStub routes={routesFor(isDemo)}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter>
          <TopBar
            state={{ ...DEFAULT_STATE, tenant: "acme", dataset: "production" }}
            update={() => {}}
            who={WHO}
            canManage={WHO.memberships[0]?.role === "admin"}
            isDemo={isDemo}
          />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Components/TopBar",
  component: TopBarStub,
} satisfies Meta<typeof TopBarStub>;

export default meta;
type Story = StoryObj<typeof meta>;

export const SignedInWithTenant: Story = {
  args: { isDemo: false },
};

export const DemoMode: Story = {
  args: { isDemo: true },
};
