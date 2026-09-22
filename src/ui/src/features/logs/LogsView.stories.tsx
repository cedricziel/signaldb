import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import {
  emptyMatrix,
  logsResponse,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { LogsView } from "./LogsView";

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

const BASE_NS = 1_700_000_000_000_000_000n;

function buildLogRows(count: number) {
  const rows: { tsNs: string; line: string; labels: Record<string, string> }[] =
    [];
  for (let i = 0; i < count; i++) {
    const service = SERVICES[i % SERVICES.length]!;
    const level = LEVELS[i % LEVELS.length]!;
    const messages = MESSAGES[service]!;
    const line = messages[i % messages.length]!;
    rows.push({
      tsNs: (BASE_NS - BigInt(i) * 1_000_000_000n).toString(),
      line,
      labels: { level, service_name: service },
    });
  }
  return rows;
}

const populatedRoutes: JsonRoute[] = [
  {
    match: /query_range.*direction=backward/,
    body: logsResponse(buildLogRows(20)),
  },
  {
    match: /query_range.*step=/,
    body: {
      status: "success",
      data: {
        resultType: "matrix",
        result: LEVELS.filter((l, i, arr) => arr.indexOf(l) === i).map(
          (level) => ({
            metric: { level },
            values: Array.from({ length: 12 }, (_, i) => [
              1_700_000_000 - i * 60,
              String(3 + ((i + level.length) % 5)),
            ]),
          }),
        ),
      },
    },
  },
  {
    match: "/loki/api/v1/labels",
    body: { status: "success", data: ["level", "service_name"] },
  },
];

const emptyRoutes: JsonRoute[] = [
  { match: /query_range.*direction=backward/, body: logsResponse([]) },
  { match: /query_range.*step=/, body: emptyMatrix },
  {
    match: "/loki/api/v1/labels",
    body: { status: "success", data: ["level", "service_name"] },
  },
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
