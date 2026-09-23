import { useState, type ReactNode } from "react";
import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import type { ConsentContextResponse } from "../../api/consent";
import type { JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { ConsentView } from "./ConsentView";

const AUTHORIZE_QUERY =
  "?client_id=claude-desktop&redirect_uri=https%3A%2F%2Fclaude.ai%2Fapi%2Fmcp%2Fauth_callback&code_challenge=abc123&code_challenge_method=S256";

/** `ConsentView` reads its OAuth params straight from `window.location.search`
 * (it isn't router-driven — the real page is a plain redirect target, not a
 * route with params) — set it before the first render, same lazy-`useState`
 * timing `StoryFetchStub` uses, and restore the location on unmount. */
function WithAuthorizeQuery({ children }: { children: ReactNode }) {
  const [prev] = useState(() => {
    const before = window.location.search;
    window.history.replaceState(null, "", `/oauth/consent${AUTHORIZE_QUERY}`);
    return before;
  });
  useState(() => () => {
    window.history.replaceState(null, "", `/oauth/consent${prev}`);
  });
  return children;
}

function context(
  tenants: ConsentContextResponse["tenants"],
): ConsentContextResponse {
  return { client_name: "Claude", tenants };
}

const SINGLE_TENANT = context([
  {
    id: "acme",
    role: "admin",
    datasets: [
      { id: "production", name: "production" },
      { id: "staging", name: "staging" },
    ],
  },
]);

const MULTI_TENANT = context([
  {
    id: "acme",
    role: "admin",
    datasets: [{ id: "production", name: "production" }],
  },
  {
    id: "globex",
    role: "member",
    datasets: [{ id: "production", name: "production" }],
  },
]);

function ConsentPage({ context }: { context: ConsentContextResponse }) {
  const routes: JsonRoute[] = [
    { match: "/oauth/consent/context", method: "GET", body: context },
  ];
  return (
    <WithAuthorizeQuery>
      <StoryFetchStub routes={routes}>
        <QueryClientProvider client={testQueryClient()}>
          <MemoryRouter initialEntries={["/oauth/consent"]}>
            <ConsentView />
          </MemoryRouter>
        </QueryClientProvider>
      </StoryFetchStub>
    </WithAuthorizeQuery>
  );
}

const meta = {
  title: "Pages/Consent",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ConsentPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const SingleTenant: Story = {
  render: () => <ConsentPage context={SINGLE_TENANT} />,
};

export const MultipleTenants: Story = {
  render: () => <ConsentPage context={MULTI_TENANT} />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <ConsentPage context={SINGLE_TENANT} />
    </DarkScope>
  ),
};
