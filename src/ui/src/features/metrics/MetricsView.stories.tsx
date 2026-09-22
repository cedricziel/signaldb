import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { MetricsView } from "./MetricsView";

const START_NS = 1_700_000_000_000_000_000;
const STEP_NS = 60_000_000_000;

function seriesRoute(): JsonRoute {
  return {
    match: "/api/v1/query",
    bodyMatch: (b) => (b as { result?: string }).result === "series",
    body: {
      result: "series",
      window: { start_ns: START_NS, end_ns: START_NS + 12 * STEP_NS },
      series: [
        {
          labels: { service_name: "checkout" },
          points: Array.from({ length: 12 }, (_, i) => [
            START_NS + i * STEP_NS,
            120 + i * 4,
          ]),
        },
        {
          labels: { service_name: "payments" },
          points: Array.from({ length: 12 }, (_, i) => [
            START_NS + i * STEP_NS,
            60 + (i % 5) * 3,
          ]),
        },
        {
          labels: { service_name: "inventory" },
          points: Array.from({ length: 12 }, (_, i) => [
            START_NS + i * STEP_NS,
            30 + (i % 4) * 2,
          ]),
        },
      ],
    },
  };
}

const metricNamesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) => {
    const body = b as { pipeline?: Array<{ describe?: { target?: string } }> };
    return body.pipeline?.[0]?.describe?.target === "values";
  },
  body: {
    result: "metadata",
    window: { start_ns: 0, end_ns: 0 },
    metadata: {
      kind: "values",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate: false,
      },
      values: [
        { value: "signaldb.wal.entries_processed" },
        { value: "signaldb.http.request.duration" },
      ],
    },
  },
};

const fieldsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) => {
    const body = b as { pipeline?: Array<{ describe?: { target?: string } }> };
    return body.pipeline?.[0]?.describe?.target === "fields";
  },
  body: {
    result: "metadata",
    window: { start_ns: 0, end_ns: 0 },
    metadata: {
      kind: "fields",
      truncated: false,
      cost: {
        mode: "metadata",
        window_scoped: false,
        sampled: false,
        approximate: false,
      },
      fields: [
        {
          name: "service_name",
          type: "string",
          filterable: true,
          origin: "declared",
        },
      ],
    },
  },
};

const routes: JsonRoute[] = [
  irCatchAll,
  metricNamesRoute,
  fieldsRoute,
  seriesRoute(),
];

function MetricsPage({ state }: { state: ExploreState }) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/metrics"]}>
          <MetricsView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Metrics",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof MetricsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const RAN_QUERY = JSON.stringify({
  queries: [
    { ref: "a", metric: "signaldb.wal.entries_processed", filters: [] },
  ],
  formula: "",
});

export const Default: Story = {
  render: () => (
    <MetricsPage
      state={{ ...DEFAULT_STATE, signal: "metrics", metricQuery: RAN_QUERY }}
    />
  ),
};

export const Empty: Story = {
  render: () => <MetricsPage state={{ ...DEFAULT_STATE, signal: "metrics" }} />,
};
