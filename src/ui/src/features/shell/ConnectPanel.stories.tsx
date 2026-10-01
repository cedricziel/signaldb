import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import type { JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import {
  connectionInfoBody,
  MCP_ENDPOINT as MCP,
} from "../../test/connectionInfo";
import { pageFrame } from "../../stories/PageFrame";
import { ConnectPanel, type ConnectTab } from "./ConnectPanel";

function Panel({
  routes,
  initialTab,
}: {
  routes: JsonRoute[];
  initialTab?: ConnectTab;
}) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter>
          <ConnectPanel
            state={{ tenant: "acme", dataset: "production" }}
            canManage
            initialTab={initialTab}
            onClose={() => {}}
          />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const withMcp: JsonRoute[] = [
  { match: "/api/v1/connection", body: connectionInfoBody({ mcp: MCP }) },
];

const meta = {
  title: "Shell/Connect Panel",
  component: Panel,
  parameters: { layout: "fullscreen" },
  decorators: [pageFrame],
  args: { routes: withMcp },
} satisfies Meta<typeof Panel>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Mcp: Story = {
  name: "MCP",
  args: { initialTab: "mcp" },
};

export const Cli: Story = {
  name: "CLI",
  args: { initialTab: "cli" },
};

export const HttpApi: Story = {
  name: "HTTP API",
  args: { initialTab: "api" },
};

export const Dark: Story = {
  render: (args) => (
    <DarkScope>
      <Panel {...args} />
    </DarkScope>
  ),
};

export const NoMcpLocalhostFallback: Story = {
  args: {
    routes: [
      {
        match: "/api/v1/connection",
        body: connectionInfoBody({
          mcp: null,
          public_endpoints_configured: false,
          notes: [
            "[public].api_url is unset; falling back to http://localhost:3000",
          ],
        }),
      },
    ],
  },
};
