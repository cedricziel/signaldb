import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import {
  describeFieldsResponse,
  emptyIrSeries,
  irLogRowsResponse,
  irLogVolumeResponse,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { LogsView } from "./LogsView";

const isRowsQuery = (b: unknown) =>
  (b as { result?: string }).result === "rows";
const isSeriesQuery = (b: unknown) =>
  (b as { result?: string }).result === "series";
const isFieldsQuery = (b: unknown) =>
  (b as { pipeline?: { describe?: { target?: string } }[] }).pipeline?.[0]
    ?.describe?.target === "fields";

const SERVICES = [
  "checkout",
  "payments",
  "cart",
  "inventory",
  "shipping",
] as const;
const LEVELS = ["info", "info", "info", "warn", "error"] as const;

const MESSAGES: Record<string, string[]> = {
  checkout: ["checkout started", "checkout completed", "order confirmed"],
  payments: [
    "charge authorized",
    "charge failed: card_declined",
    "refund issued",
  ],
  cart: ["item added to cart", "cart updated", "cart abandoned"],
  inventory: ["stock reserved", "stock released", "low stock warning"],
  shipping: ["label created", "shipment dispatched", "delivery confirmed"],
};

// A fixed instant (2023-11-14T22:13:20Z), not `Date.now()` — every row and
// bucket below is offset from this constant, so the story is deterministic
// regardless of when or where it renders.
const BASE_NS = 1_700_000_000_000_000_000n;
const BASE_MS = 1_700_000_000_000;

function buildLogRows(count: number) {
  return Array.from({ length: count }, (_, i) => {
    const service = SERVICES[i % SERVICES.length]!;
    const level = LEVELS[i % LEVELS.length]!;
    const messages = MESSAGES[service]!;
    return {
      tsNs: (BASE_NS - BigInt(i) * 1_000_000_000n).toString(),
      body: messages[i % messages.length]!,
      serviceName: service,
      severityText: level,
    };
  });
}

function buildVolume() {
  const uniqueLevels = [...new Set(LEVELS)];
  return uniqueLevels.map((level) => ({
    level,
    points: Array.from({ length: 12 }, (_, i): [number, number] => [
      (BASE_MS - i * 60_000) * 1_000_000,
      3 + ((i + level.length) % 5),
    ]),
  }));
}

const fieldsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: isFieldsQuery,
  body: describeFieldsResponse(["severity_text", "service.name"]),
};

const populatedRoutes: JsonRoute[] = [
  {
    match: "/api/v1/query",
    bodyMatch: isRowsQuery,
    body: irLogRowsResponse(buildLogRows(20)),
  },
  {
    match: "/api/v1/query",
    bodyMatch: isSeriesQuery,
    body: irLogVolumeResponse(buildVolume()),
  },
  fieldsRoute,
];

const emptyRoutes: JsonRoute[] = [
  {
    match: "/api/v1/query",
    bodyMatch: isRowsQuery,
    body: irLogRowsResponse([]),
  },
  { match: "/api/v1/query", bodyMatch: isSeriesQuery, body: emptyIrSeries },
  fieldsRoute,
];

function LogsPage({ routes }: { routes: JsonRoute[] }) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/logs"]}>
          <LogsView state={DEFAULT_STATE} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Logs",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof LogsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <LogsPage routes={populatedRoutes} />,
};

export const Empty: Story = {
  render: () => <LogsPage routes={emptyRoutes} />,
};
