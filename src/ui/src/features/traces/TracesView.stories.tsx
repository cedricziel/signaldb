import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import type { JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { TracesView } from "./TracesView";

/** Matches a Query IR request body by its `result` envelope and `from`
 * source — enough to route the traces tab's several distinct queries
 * (groups, volume, members, trace/profile detail) through one endpoint,
 * mirroring `TracesView.test.tsx`'s `isTraceDetailQuery`. */
function irBody(
  match: (body: {
    result?: string;
    from?: string;
    fields?: unknown;
  }) => boolean,
) {
  return (b: unknown) =>
    match((b ?? {}) as { result?: string; from?: string; fields?: unknown });
}

const GROUP_ROWS = [
  [
    "POST /api/checkout",
    182,
    6,
    45_000_000,
    210_000_000,
    "1700000000000000000",
  ],
  ["GET /api/cart", 140, 1, 12_000_000, 60_000_000, "1700000000000000000"],
  ["GET /api/inventory", 98, 0, 8_000_000, 30_000_000, "1700000000000000000"],
  [
    "POST /api/payments/charge",
    76,
    14,
    90_000_000,
    420_000_000,
    "1700000000000000000",
  ],
];

const groupRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "table"),
  body: { result: "table", rows: GROUP_ROWS },
};

const volumeRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "series"),
  body: {
    result: "series",
    series: [
      {
        labels: { "status.code": "ok" },
        points: Array.from({ length: 12 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 60_000_000_000,
          40 + i,
        ]),
      },
      {
        labels: { "status.code": "error" },
        points: Array.from({ length: 12 }, (_, i) => [
          1_700_000_000_000_000_000 + i * 60_000_000_000,
          2 + (i % 3),
        ]),
      },
    ],
  },
};

const membersRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody(
    (b) => b.result === "rows" && b.from === "traces" && !b.fields,
  ),
  body: {
    result: "rows",
    rows: [
      [
        "t1cafe",
        "root",
        null,
        "POST /api/checkout",
        "gateway",
        "1700000000000000000",
        "412000000",
        "OK",
      ],
    ],
  },
};

const SPAN_COLUMNS = [
  { name: "trace_id", type: "string" },
  { name: "span_id", type: "string" },
  { name: "parent_span_id", type: "string" },
  { name: "span_name", type: "string" },
  { name: "service_name", type: "string" },
  { name: "status_code", type: "string" },
  { name: "status_message", type: "string" },
  { name: "start_time_unix_nano", type: "timestamp_ns" },
  { name: "duration_nanos", type: "duration_ns" },
  { name: "span_kind", type: "string" },
  { name: "span_attributes", type: "map<string,string>" },
  { name: "scope_attributes", type: "map<string,string>" },
  { name: "resource_attributes", type: "map<string,string>" },
  { name: "span_events", type: "string" },
];

const spanRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody(
    (b) =>
      b.result === "rows" && b.from === "traces" && Array.isArray(b.fields),
  ),
  body: {
    result: "rows",
    columns: SPAN_COLUMNS,
    rows: [
      [
        "t1cafe",
        "root",
        null,
        "POST /api/checkout",
        "gateway",
        "OK",
        null,
        1_000_000_000,
        412_000_000,
        "server",
        {},
        {},
        {},
        null,
      ],
      [
        "t1cafe",
        "charge",
        "root",
        "charge",
        "payments",
        "ERROR",
        "card declined",
        1_040_000_000,
        258_000_000,
        "client",
        { "payment.provider": "stripe" },
        {},
        {},
        JSON.stringify([
          {
            name: "exception",
            timestamp_unix_nano: 1_055_000_000,
            attributes: {
              "exception.type": "PaymentError",
              "exception.message": "card declined",
            },
          },
        ]),
      ],
      [
        "t1cafe",
        "inventory-check",
        "root",
        "check inventory",
        "inventory",
        "OK",
        null,
        1_060_000_000,
        90_000_000,
        "client",
        {},
        {},
        {},
        null,
      ],
    ],
  },
};

const profilesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "rows" && b.from === "profiles"),
  body: { result: "rows", columns: [], rows: [] },
};

const baseRoutes: JsonRoute[] = [groupRoute, volumeRoute, membersRoute];
const detailRoutes: JsonRoute[] = [...baseRoutes, spanRoute, profilesRoute];

function TracesPage({
  state,
  routes,
}: {
  state: ExploreState;
  routes: JsonRoute[];
}) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/traces"]}>
          <TracesView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Traces",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof TracesPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => (
    <TracesPage
      state={{ ...DEFAULT_STATE, signal: "traces" }}
      routes={baseRoutes}
    />
  ),
};

export const TraceDetail: Story = {
  render: () => (
    <TracesPage
      state={{ ...DEFAULT_STATE, signal: "traces", trace: "t1cafe" }}
      routes={detailRoutes}
    />
  ),
};
