import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { userEvent, within } from "storybook/test";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import { OutletContextProvider } from "../../stories/OutletContextProvider";
import {
  irCatchAll,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { processorsRoutes } from "./routes";

const who = sampleWhoami();

const processorsList = {
  processors: [
    {
      name: "redact-emails",
      description: "Redact user emails",
      signal: "logs",
      dataset: "production",
      enabled: true,
      priority: 100,
      error_mode: "ignore",
      statements: ['set(attributes["user.email"], "[redacted]")'],
      tenant_id: "acme",
      created_at: "2026-08-01T00:00:00Z",
      updated_at: "2026-08-14T10:00:00Z",
      status: "ok",
    },
    {
      name: "strip-query",
      description: null,
      signal: "traces",
      dataset: null,
      enabled: false,
      priority: 50,
      error_mode: "silent",
      statements: ['replace_pattern(attributes["url.full"], "\\\\?.*$", "")'],
      tenant_id: "acme",
      created_at: "2026-07-01T00:00:00Z",
      updated_at: "2026-07-02T00:00:00Z",
      status: "invalid",
    },
    {
      name: "tag-region",
      description: "Adds deployment region",
      signal: "metrics",
      dataset: null,
      enabled: true,
      priority: 10,
      error_mode: "propagate",
      statements: ['set(attributes["deployment.region"], "us-east-1")'],
      tenant_id: "acme",
      created_at: "2026-06-10T00:00:00Z",
      updated_at: "2026-06-20T00:00:00Z",
      status: "ok",
    },
  ],
};

const redactEmails = processorsList.processors[0];

const testResponse = {
  payload: {
    resourceLogs: [
      {
        resource: { attributes: [] },
        scopeLogs: [
          {
            scope: {},
            logRecords: [
              {
                body: { stringValue: "order placed" },
                attributes: [
                  { key: "user.email", value: { stringValue: "[redacted]" } },
                ],
              },
            ],
          },
        ],
      },
    ],
  },
  statements: [{ processor: "redact-emails", index: 0, matched: 1, errors: 0 }],
};

function ProcessorsPage({
  routes,
  initialEntries,
}: {
  routes: JsonRoute[];
  initialEntries: string[];
}) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={initialEntries}>
          <OutletContextProvider
            value={{
              state: { ...DEFAULT_STATE, tenant: "acme" },
              update: () => {},
            }}
          >
            {processorsRoutes()}
          </OutletContextProvider>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Processors",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ProcessorsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const List: Story = {
  render: () => (
    <ProcessorsPage
      initialEntries={["/processors"]}
      routes={[
        irCatchAll,
        { match: "/api/v1/whoami", body: who },
        { match: "/api/v1/processors", body: processorsList },
      ]}
    />
  ),
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <ProcessorsPage
        initialEntries={["/processors"]}
        routes={[
          irCatchAll,
          { match: "/api/v1/whoami", body: who },
          { match: "/api/v1/processors", body: processorsList },
        ]}
      />
    </DarkScope>
  ),
};

export const Editor: Story = {
  render: () => (
    <ProcessorsPage
      initialEntries={["/processors/redact-emails/edit"]}
      routes={[
        irCatchAll,
        { match: "/api/v1/whoami", body: who },
        { match: "/api/v1/processors/redact-emails", body: redactEmails },
      ]}
    />
  ),
};

export const TestRun: Story = {
  render: () => (
    <ProcessorsPage
      initialEntries={["/processors/redact-emails/edit"]}
      routes={[
        irCatchAll,
        { match: "/api/v1/whoami", body: who },
        { match: "/api/v1/processors/redact-emails", body: redactEmails },
        { match: "/api/v1/processors:test", body: testResponse },
      ]}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    await userEvent.click(await canvas.findByText("Run test"));
    await canvas.findByText("Diff");
  },
};
