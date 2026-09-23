import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import type { JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { QueryView } from "./QueryView";

const ROWS_COLUMNS = [
  { name: "timestamp", type: "timestamp_ns" },
  { name: "body", type: "string" },
  { name: "service_name", type: "string" },
  { name: "severity_text", type: "string" },
];

const rowsRoute: JsonRoute = {
  match: "/api/v1/query",
  body: {
    result: "rows",
    window: { start_ns: 0, end_ns: 0 },
    columns: ROWS_COLUMNS,
    rows: [
      [
        "1700000000000000000",
        "checkout request completed in 182ms",
        "checkout",
        "INFO",
      ],
      [
        "1700000005000000000",
        "payment declined: insufficient funds",
        "payments",
        "WARN",
      ],
      [
        "1700000012000000000",
        "inventory reservation released",
        "inventory",
        "INFO",
      ],
      [
        "1700000018000000000",
        "cache miss for product 40211",
        "catalog",
        "DEBUG",
      ],
    ],
  },
};

function QueryPage({ state }: { state: ExploreState }) {
  return (
    <StoryFetchStub routes={[rowsRoute]}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/query"]}>
          <QueryView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Query",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof QueryPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const populatedState = {
  ...DEFAULT_STATE,
  signal: "query",
  querySource: "logs",
  queryResult: "rows",
  queryRun: true,
} as const;

export const Default: Story = {
  render: () => <QueryPage state={populatedState} />,
};

export const Dark: Story = {
  render: () => (
    <DarkScope>
      <QueryPage state={populatedState} />
    </DarkScope>
  ),
};
