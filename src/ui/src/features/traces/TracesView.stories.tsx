import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { fireEvent, within } from "storybook/test";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import {
  irBody,
  irOperationSeriesResponse,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { TracesView } from "./TracesView";

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

/** The span.kind facet's value-count query (`api/traceFacets.ts`) is a
 * `table` too; without its own route the group rows above answer it and
 * every kind reads 0. */
const kindFacetRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) =>
    (b.pipeline ?? []).some(
      (stage) => stage.aggregate?.by?.[0] === "span_kind",
    ),
  ),
  body: {
    result: "table",
    rows: [
      ["Internal", 1_184],
      ["Server", 496],
      ["Client", 412],
      ["Producer", 38],
    ],
  },
};

/** One-minute buckets over the hour ending at the request's own `range.to`,
 * so the histogram's axis spans the story's window rather than a fixed 2023
 * timestamp far outside it. */
function volumeFor(b: unknown) {
  const toNs = Number((b as { range?: { to?: string } }).range?.to ?? 0);
  const points = (value: (i: number) => number): [number, number][] =>
    Array.from({ length: 60 }, (_, i) => [
      toNs - (59 - i) * 60_000_000_000,
      value(i),
    ]);
  return irOperationSeriesResponse("status.code", {
    ok: points((i) => 40 + (i % 12)),
    error: points((i) => 2 + (i % 3)),
  });
}

const volumeRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: irBody((b) => b.result === "series"),
  bodyFor: volumeFor,
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

const baseRoutes: JsonRoute[] = [
  groupRoute,
  kindFacetRoute,
  volumeRoute,
  membersRoute,
];
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

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <TracesPage
        state={{ ...DEFAULT_STATE, signal: "traces" }}
        routes={baseRoutes}
      />
    </DarkScope>
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

/** Waterfall | Map | Both, switched to Both — `spanRoute` above already
 * carries a failed `payments` call, so the map's `gateway -> payments` edge
 * and node render failed. */
export const TraceMap: Story = {
  render: () => (
    <TracesPage
      state={{ ...DEFAULT_STATE, signal: "traces", trace: "t1cafe" }}
      routes={detailRoutes}
    />
  ),
  play: async ({ canvasElement }) => {
    const canvas = within(canvasElement);
    const both = await canvas.findByRole("button", { name: "Both" });
    fireEvent.click(both);
  },
};
