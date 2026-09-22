import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../lib/queryClient";
import { DEFAULT_STATE } from "../lib/urlState";
import {
  sampleCurrentSession,
  sampleWhoami,
  type JsonRoute,
} from "../stories/fetchStub";
import { StoryFetchStub } from "../stories/StoryFetchStub";
import { TopBar } from "./TopBar";

const WHO = sampleWhoami();

function routesFor(isDemo: boolean): JsonRoute[] {
  return [
    { match: "/api/v1/whoami", body: WHO },
    {
      match: "/ui/session",
      method: "GET",
      body: sampleCurrentSession(WHO, {
        user: { ...WHO.user, is_demo: isDemo },
      }),
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
