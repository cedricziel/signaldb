// The app frame — top bar + signal tab bar + a content view — the "canvas"
// every generated page design drops into (see the shell/login-stories
// task). Composed from the real components rather than a placeholder:
// TopBar's container (whoami/session hooks) and ExploreView on the logs
// tab, with LogsView.stories's fixtures.
import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { TopBar } from "../shell/TopBar";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE } from "../../lib/urlState";
import type { WhoamiResponse } from "../../api/session";
import {
  describeFieldsResponse,
  irLogRowsResponse,
  irLogVolumeResponse,
  type JsonRoute,
} from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { ExploreView } from "./ExploreView";

const WHO: WhoamiResponse = {
  user: {
    id: "user-1",
    email: "alice@example.com",
    display_name: "Alice",
    is_instance_admin: false,
  },
  memberships: [{ tenant_id: "acme", role: "admin" }],
  tenant: { id: "acme", slug: "acme", name: "Acme Corp" },
  datasets: [{ id: "production", slug: "production", is_default: true }],
  default_dataset: "production",
};

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
  { match: "/api/v1/whoami", body: WHO },
  {
    match: "/ui/session",
    method: "GET",
    body: {
      user: WHO.user,
      tenant: "acme",
      dataset: "production",
      memberships: WHO.memberships,
    },
  },
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
];

function AppShellPage() {
  const state = { ...DEFAULT_STATE, tenant: "acme", dataset: "production" };
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/logs?tenant=acme&dataset=production"]}>
          <div className="app-frame">
            <TopBar state={state} update={() => {}} />
            <main className="app-main">
              <ExploreView state={state} update={() => {}} />
            </main>
          </div>
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/App Shell",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof AppShellPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => <AppShellPage />,
};
