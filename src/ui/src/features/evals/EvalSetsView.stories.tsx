// Eval sets (`/evals/sets`) with the support-triage scenario: four sets,
// triage-golden-200 replayed by the baseline and candidate runs. Sets come
// from `evalFixtures.ts`'s `EVAL_SET_*`, runs from `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { EvalSetsView } from "./EvalSetsView";
import { SCENARIO_RANGE } from "./evalFixtures";
import {
  evalPage,
  evalPageMeta,
  NO_SETS_ROUTES,
  pageStories,
} from "./evalStories";

const SetsPage = evalPage(EvalSetsView, "/evals/sets", {
  range: SCENARIO_RANGE,
});

const meta = {
  title: "Pages/Evals/Eval Sets",
  ...evalPageMeta,
} satisfies Meta<typeof SetsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(SetsPage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;

/** No eval sets in the dataset yet. */
export const Empty: Story = {
  render: () => <SetsPage stubRoutes={NO_SETS_ROUTES} />,
};
