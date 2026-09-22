import { QueryClientProvider } from "@tanstack/react-query";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { ProfilesView } from "./ProfilesView";

function isDescribeFields(body: unknown): boolean {
  const b = body as { pipeline?: Array<{ describe?: { target?: string } }> };
  return b.pipeline?.[0]?.describe?.target === "fields";
}
function isDescribeValues(field: string) {
  return (body: unknown): boolean => {
    const b = body as {
      pipeline?: Array<{ describe?: { target?: string; field?: string } }>;
    };
    return (
      b.pipeline?.[0]?.describe?.target === "values" &&
      b.pipeline[0]?.describe?.field === field
    );
  };
}
function isProfileTypesAggregate(body: unknown): boolean {
  const b = body as {
    from?: string;
    pipeline?: Array<{ aggregate?: { by?: string[] } }>;
  };
  return (
    b.from === "profiles" &&
    b.pipeline?.[0]?.aggregate?.by?.[0] === "sample.type"
  );
}

const profileTypesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: isProfileTypesAggregate,
  body: {
    result: "table",
    window: { start_ns: 0, end_ns: 0 },
    columns: [
      { name: "sample.type", type: "string" },
      { name: "sample.unit", type: "string" },
      { name: "period.type", type: "string" },
      { name: "period.unit", type: "string" },
      { name: "n", type: "int" },
    ],
    rows: [["cpu", "nanoseconds", "cpu", "nanoseconds", 1]],
  },
};

const fieldsRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: isDescribeFields,
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
          name: "service.name",
          type: "string",
          filterable: true,
          origin: "declared",
        },
      ],
    },
  },
};

const serviceValuesRoute: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: isDescribeValues("service.name"),
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
        { value: "signaldb-router" },
        { value: "signaldb-querier" },
        { value: "signaldb-writer" },
      ],
    },
  },
};

/** A realistic-ish call stack for the flamegraph/top-table (levels are
 * `[offset, total, self, nameIdx]` groups, per level — see `FlameGraph.tsx`
 * and `lib/flamebearer.ts`'s decoder). */
const FLAMEGRAPH_ROUTE: JsonRoute = {
  match: "/api/v1/query",
  bodyMatch: (b) => (b as { result?: string }).result === "flamegraph",
  body: {
    result: "flamegraph",
    window: { start_ns: 0, end_ns: 0 },
    flamegraph: {
      truncated: false,
      names: [
        "total",
        "main",
        "http::handle_request",
        "router::route",
        "querier::execute_query",
        "datafusion::physical_plan::scan",
        "parquet::read_row_group",
        "arrow::compute::filter",
      ],
      levels: [
        [0, 1000, 0, 0],
        [0, 1000, 20, 1],
        [0, 980, 40, 2],
        [0, 940, 60, 3],
        [0, 880, 120, 4],
        [0, 760, 260, 5],
        [0, 500, 300, 6],
        [500, 200, 200, 7],
      ],
      total: 1000,
      max_self: 300,
      locations: [null, null, null, null, null, null, null, null],
    },
  },
};

const routes: JsonRoute[] = [
  irCatchAll,
  profileTypesRoute,
  fieldsRoute,
  serviceValuesRoute,
  FLAMEGRAPH_ROUTE,
];

function ProfilesPage({ state }: { state: ExploreState }) {
  return (
    <StoryFetchStub routes={routes}>
      <QueryClientProvider client={testQueryClient()}>
        <MemoryRouter initialEntries={["/profiles"]}>
          <ProfilesView state={state} update={() => {}} />
        </MemoryRouter>
      </QueryClientProvider>
    </StoryFetchStub>
  );
}

const meta = {
  title: "Pages/Profiles",
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280, height: 800 }}>
        <Story />
      </div>
    ),
  ],
} satisfies Meta<typeof ProfilesPage>;

export default meta;
type Story = StoryObj<typeof meta>;

export const Default: Story = {
  render: () => (
    <ProfilesPage state={{ ...DEFAULT_STATE, signal: "profiles" }} />
  ),
};
