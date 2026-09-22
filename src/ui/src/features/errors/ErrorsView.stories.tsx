import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { irBody, irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { ErrorsView } from "./ErrorsView";

/** `buildErrorGroupDoc` rows: [type, message, service, escaped, count, first, last]. */
const GROUP_ROWS_TRACES = [
  [
    "std::io::Error",
    "connection reset by peer",
    "signaldb-writer",
    "true",
    182,
    "1700000000000000000",
    "1700003600000000000",
  ],
  [
    "ParquetError",
    "invalid column metadata",
    "signaldb-compactor",
    "false",
    34,
    "1700000100000000000",
    "1700002000000000000",
  ],
];
const GROUP_ROWS_LOGS = [
  [
    "serde_json::Error",
    "missing field `tenant_id`",
    "signaldb-acceptor",
    "true",
    97,
    "1700000200000000000",
    "1700003500000000000",
  ],
];

const groupsTracesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "table" && b.from === "traces"),
  body: { result: "table", rows: GROUP_ROWS_TRACES },
};
const groupsLogsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "table" && b.from === "logs"),
  body: { result: "table", rows: GROUP_ROWS_LOGS },
};

const occurrencesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "rows"),
  body: {
    result: "rows",
    rows: [
      ["1700003600000000000", "t1cafe", "at write_wal\n at flush_segment"],
      ["1700003500000000000", "t2beef", null],
      ["1700003000000000000", null, "at write_wal\n at retry_write"],
    ],
  },
};

const volumeRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "series"),
  body: {
    result: "series",
    series: [
      {
        labels: { "exception.type": "std::io::Error" },
        points: Array.from({ length: 20 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 180_000_000_000,
          i % 4,
        ]),
      },
    ],
  },
};

const baseRoutes: JsonRoute[] = [
  irCatchAll,
  groupsTracesRoute,
  groupsLogsRoute,
];
const detailRoutes: JsonRoute[] = [
  ...baseRoutes,
  occurrencesRoute,
  volumeRoute,
];

function ErrorsPage({
  state,
  routes,
}: {
  state: ExploreState;
  routes: JsonRoute[];
}) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/errors"]}>
          <ErrorsView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Errors",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ErrorsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const SELECTED_GROUP = JSON.stringify([
  "traces",
  "std::io::Error",
  "connection reset by peer",
  "signaldb-writer",
  "true",
]);

export const Default: Story = {
  render: () => (
    <ErrorsPage
      state={{ ...DEFAULT_STATE, signal: "errors" }}
      routes={baseRoutes}
    />
  ),
};

export const GroupDetail: Story = {
  render: () => (
    <ErrorsPage
      state={{ ...DEFAULT_STATE, signal: "errors", group: SELECTED_GROUP }}
      routes={detailRoutes}
    />
  ),
};

export const Empty: Story = {
  render: () => (
    <ErrorsPage
      state={{ ...DEFAULT_STATE, signal: "errors" }}
      routes={[
        irCatchAll,
        { ...groupsTracesRoute, body: { result: "table", rows: [] } },
        { ...groupsLogsRoute, body: { result: "table", rows: [] } },
      ]}
    />
  ),
};
