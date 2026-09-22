import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { testQueryClient } from "../../lib/queryClient";
import { irCatchAll, irBody, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { Instrumentation } from "./Instrumentation";

/** Ingest-status count queries (`fetchIngestStatus`, `from: <signal>`,
 * `result: "table"`) — traces/logs receiving, metrics/profiles quiet. */
function ingestCountRoute(from: string, n: number): JsonRoute {
  return {
    match: "/api/v1/query",
    bodyMatch: irBody((b) => b.from === from && b.result === "table"),
    body: { result: "table", rows: n > 0 ? [[n]] : [] },
  };
}

const routes: JsonRoute[] = [
  irCatchAll,
  {
    match: "/api/v1/connection",
    body: {
      ingest: {
        otlp_grpc: { authority: "otlp.acme.signaldb.dev:4317", tls: true },
        otlp_http: { authority: "otlp.acme.signaldb.dev:4318", tls: true },
        prometheus_remote_write:
          "https://otlp.acme.signaldb.dev/api/v1/prometheus/write",
      },
      headers: {
        authorization: "Bearer sdb_live_examplekey",
        "x-tenant-id": "acme",
        "x-dataset-id": "production",
      },
      otel_env: {
        OTEL_EXPORTER_OTLP_ENDPOINT: "https://otlp.acme.signaldb.dev:4317",
        OTEL_EXPORTER_OTLP_PROTOCOL: "grpc",
        OTEL_EXPORTER_OTLP_HEADERS:
          "Authorization=Bearer sdb_live_examplekey,X-Tenant-ID=acme,X-Dataset-ID=production",
      },
      notes: [],
    },
  },
  ingestCountRoute("traces", 4820),
  ingestCountRoute("logs", 12_400),
  ingestCountRoute("metrics", 0),
  ingestCountRoute("profiles", 0),
];

function InstrumentationPage() {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <Instrumentation state={{ tenant: "acme", dataset: "production" }} />
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Instrumentation",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof InstrumentationPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <InstrumentationPage />,
};
