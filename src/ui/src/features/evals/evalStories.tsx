// The frame every `Pages/Evals/*` story shares: the page under a query
// client and router, answering `/api/v1/query` from `evalFixtures.ts`.
import { QueryClientProvider } from "@tanstack/react-query";
import type { Decorator } from "@storybook/react-vite";
import type { ComponentType } from "react";
import { MemoryRouter } from "react-router";
import { testQueryClient } from "../../lib/queryClient";
import type { ShellContext } from "../../lib/outletState";
import { DEFAULT_STATE, type ExploreState } from "../../lib/urlState";
import { irCatchAll, type JsonRoute } from "../../stories/fetchStub";
import { StoryFetchStub } from "../../stories/StoryFetchStub";
import { DarkScope } from "../../stories/DarkScope";
import { evalsIrResponse } from "./evalFixtures";

const SCENARIO_ROUTES: JsonRoute[] = [
  { match: "/api/v1/query", body: {}, bodyFor: evalsIrResponse },
];

/** Every query answers empty: the pages' "nothing yet" states. */
export const EMPTY_ROUTES: JsonRoute[] = [irCatchAll];

/** A page component rendering `View` at `path` with `state` over the
 * scenario's tenant; `stubRoutes` swaps the fixture answers. */
export function evalPage(
  View: ComponentType<ShellContext>,
  path: string,
  state: Partial<ExploreState> = {},
) {
  const pageState: ExploreState = {
    ...DEFAULT_STATE,
    tenant: "acme",
    dataset: "production",
    ...state,
  };
  return function EvalPage({
    stubRoutes = SCENARIO_ROUTES,
  }: {
    stubRoutes?: JsonRoute[];
  }) {
    return (
      <StoryFetchStub routes={stubRoutes}>
        <QueryClientProvider client={testQueryClient()}>
          <MemoryRouter initialEntries={[path]}>
            <View state={pageState} update={() => {}} />
          </MemoryRouter>
        </QueryClientProvider>
      </StoryFetchStub>
    );
  };
}

export const evalPageMeta = {
  parameters: { layout: "fullscreen" },
  decorators: [
    (Story) => (
      <div style={{ width: 1280 }}>
        <Story />
      </div>
    ),
  ] satisfies Decorator[],
};

/** The light and dark renders of `Page`. */
export function pageStories(Page: ComponentType) {
  return {
    Default: { render: () => <Page /> },
    Dark: {
      render: () => (
        <DarkScope>
          <Page />
        </DarkScope>
      ),
    },
  };
}
