import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter, Route } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import { OutletContextProvider } from "../../stories/OutletContextProvider";
import { irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { SelectTenant, type SelectTenantProps } from "./SelectTenant";

const session: SelectTenantProps["session"] = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
    is_demo: false,
  },
  tenant: "acme",
  dataset: "production",
  memberships: [
    { tenant_id: "acme", role: "admin", name: "Acme Corp" },
    { tenant_id: "globex", role: "viewer", name: "Globex" },
  ],
};

const routes: JsonRoute[] = [
  irCatchAll,
  {
    match: "/api/v1/whoami",
    bodyMatch: () => true,
    body: {
      datasets: [
        { id: "production", slug: "production", is_default: true },
        { id: "staging", slug: "staging", is_default: false },
      ],
    },
  },
];

function SelectTenantPage() {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/select-tenant"]}>
          <OutletContextProvider
            value={{
              state: { ...DEFAULT_STATE, tenant: "acme" },
              update: () => {},
            }}
          >
            <Route path="*" element={<SelectTenant session={session} />} />
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Select Tenant",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof SelectTenantPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <SelectTenantPage />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <SelectTenantPage />
    </DarkScope>
  ),
};

export const NoAccess: Story = {
  render: () => (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/select-tenant"]}>
          <OutletContextProvider
            value={{
              state: { ...DEFAULT_STATE, tenant: "" },
              update: () => {},
            }}
          >
            <Route
              path="*"
              element={
                <SelectTenant
                  session={{
                    user: {
                      id: "user-2",
                      email: "new.user@example.com",
                      display_name: "New User",
                      is_instance_admin: false,
                      is_demo: false,
                    },
                    tenant: "",
                    dataset: "",
                    memberships: [],
                  }}
                />
              }
            />
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  ),
};
