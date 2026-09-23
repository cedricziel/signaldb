import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import type { LoginConfigResponse } from "../../api/session";
import type { JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { LoginRoute } from "./LoginRoute";

function routesFor(config: LoginConfigResponse): JsonRoute[] {
  return [
    // "/ui/session" is a substring of "/ui/session/config" — anchor the
    // session route's end so it doesn't also swallow the config request
    // (routes are matched last-defined-wins, see installFetchStub).
    {
      match: /\/ui\/session$/,
      method: "GET",
      status: 401,
      body: { error: "not signed in" },
    },
    { match: "/ui/session/config", method: "GET", body: config },
  ];
}

function LoginPage({ config }: { config: LoginConfigResponse }) {
  return (
    <StoryFetchStub routes={routesFor(config)}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/login"]}>
          <LoginRoute />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Login",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof LoginPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const PasswordOnly: Story = {
  render: () => (
    <LoginPage config={{ demo: null, oidc: null, password_enabled: true }} />
  ),
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <LoginPage
        config={{ demo: null, oidc: { name: "Okta" }, password_enabled: true }}
      />
    </DarkScope>
  ),
};

export const SsoAndPassword: Story = {
  render: () => (
    <LoginPage
      config={{
        demo: null,
        oidc: { name: "Okta" },
        password_enabled: true,
      }}
    />
  ),
};

export const DemoEnabled: Story = {
  render: () => (
    <LoginPage
      config={{
        demo: { username: "demo@example.com", password: "demo" },
        oidc: null,
        password_enabled: true,
      }}
    />
  ),
};
