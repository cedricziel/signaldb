// The app frame (sidebar, page header, ⌘K palette) every page renders
// inside. `Default`/`Dark` show it around the real logs view; `Blank Page`
// renders it with no router or backend at all, the way a design built from
// the design system uses it.
import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter, Route, Routes } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { useExploreState } from "../../lib/urlState";
import {
  describeFieldsResponse,
  irLogRowsResponse,
  irLogVolumeResponse,
  sampleWhoami,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { connectionInfoBody, MCP_ENDPOINT } from "../../test/connectionInfo";
import { DarkScope } from "../../stories/DarkScope";
import { pageFrame } from "../../stories/PageFrame";
import { ExploreView } from "../explore/ExploreView";
import { AppShell } from "./AppShell";
import type { PageId } from "./navModel";

const WHO = sampleWhoami();

const isRowsQuery = (b: unknown) =>
  (b as { result?: string }).result === "rows";
const isSeriesQuery = (b: unknown) =>
  (b as { result?: string }).result === "series";
const isFieldsQuery = (b: unknown) =>
  (b as { pipeline?: { describe?: { target?: string } }[] }).pipeline?.[0]
    ?.describe?.target === "fields";

const SERVICES = ["checkout", "payments", "cart"] as const;
const LEVELS = ["info", "info", "warn", "error"] as const;
const MESSAGES: Record<string, string[]> = {
  checkout: ["checkout started", "checkout completed", "order confirmed"],
  payments: ["charge authorized", "charge failed: card_declined"],
  cart: ["item added to cart", "cart updated"],
};

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

const routes: JsonRoute[] = [
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
  {
    match: "/api/v1/query",
    bodyMatch: isFieldsQuery,
    body: describeFieldsResponse(["severity_text", "service.name"]),
  },
  // What the header's Connect button opens.
  {
    match: "/api/v1/connection",
    body: connectionInfoBody({ mcp: MCP_ENDPOINT }),
  },
];

function LogsInShell() {
  const [state, update] = useExploreState();
  return (
    <AppShell who={WHO} state={state} update={update}>
      <ExploreView state={state} update={update} />
    </AppShell>
  );
}

function LogsPage() {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/logs?tenant=acme&dataset=production"]}>
          <Routes>
            <Route path=":signal" element={<LogsInShell />} />
          </Routes>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

function EmptyFrame({
  page = "overview",
  detail,
}: {
  page?: PageId;
  detail?: string;
}) {
  return (
    <QueryClientProvider client={testQueryClient()}>
      <AppShell page={page} detail={detail} who={WHO}>
        <div style={{ padding: "var(--gutter)" }}>
          <h1 style={{ fontSize: "var(--text-title)", margin: 0 }}>
            Page title
          </h1>
          <p style={{ color: "var(--dim)", margin: "4px 0 var(--gutter)" }}>
            A page's own content goes in the main column.
          </p>
          <section
            style={{
              background: "var(--surface)",
              border: "1px solid var(--border)",
              borderRadius: 6,
              padding: "var(--gutter)",
              color: "var(--dim)",
            }}
          >
            Panel
          </section>
        </div>
      </AppShell>
    </QueryClientProvider>
  );
}

const meta = {
  title: "Shell/App Shell",
  parameters: { layout: "fullscreen" },
  decorators: [pageFrame],
} satisfies Meta<typeof LogsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <LogsPage />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <LogsPage />
    </DarkScope>
  ),
};

export const BlankPage: Story = {
  render: () => <EmptyFrame />,
};

/** A detail view: the breadcrumb gains a leaf after the page, and the page
 * crumb links back to its section. */
export const DetailPage: Story = {
  render: () => <EmptyFrame page="traces" detail="4bf92f35" />,
};
