// Agents & scores (`/evals`) with the support-triage scenario: two
// evaluators trending down (Correctness dips from judge timeouts) and
// three holding steady. Every `/api/v1/query` answer comes from
// `evalFixtures.ts`'s `evalsIrResponse`, keyed off the request's own
// range, so the trend chart and the table agree with whatever window a
// design-sync capture (frozen clock) asks for.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { AgentsScoresView } from "./AgentsScoresView";
import { SCENARIO_RANGE } from "./evalFixtures";
import {
  EMPTY_ROUTES,
  evalPage,
  evalPageMeta,
  pageStories,
} from "./evalStories";

const AgentsScoresPage = evalPage(AgentsScoresView, "/evals", {
  range: SCENARIO_RANGE,
});

const meta = {
  title: "Pages/Evals/AgentsScores",
  ...evalPageMeta,
} satisfies Meta<typeof AgentsScoresPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(AgentsScoresPage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;

/** No evaluator results have arrived in the window at all. */
export const NoEvaluations: Story = {
  render: () => <AgentsScoresPage stubRoutes={EMPTY_ROUTES} />,
};
