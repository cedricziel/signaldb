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
import { ConnectPanel } from "./ConnectPanel";

function Panel({ routes }: { routes: JsonRoute[] }) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter>
          <ConnectPanel
            state={{ tenant: "acme", dataset: "production" }}
            canManage
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
} satisfies Meta<typeof Panel>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = { args: { routes: withMcp } };

export const Dark: Story = {
  args: { routes: withMcp },
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
