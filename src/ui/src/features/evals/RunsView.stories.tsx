// Runs (`/evals/runs`) with the support-triage scenario: the baseline
// (v1.7.3) and candidate (v1.8.0) runs of triage-golden-200, both
// complete. Answers come from `evalFixtures.ts`'s `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { RunsView } from "./RunsView";
import { SCENARIO_RANGE } from "./evalFixtures";
import {
  EMPTY_ROUTES,
  evalPage,
  evalPageMeta,
  pageStories,
} from "./evalStories";

const RunsPage = evalPage(RunsView, "/evals/runs", { range: SCENARIO_RANGE });

const meta = {
  title: "Pages/Evals/Runs",
  ...evalPageMeta,
} satisfies Meta<typeof RunsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(RunsPage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;

/** Nothing has run yet. */
export const Empty: Story = {
  render: () => <RunsPage stubRoutes={EMPTY_ROUTES} />,
};
