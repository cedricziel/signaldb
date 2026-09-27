// Evaluators (`/evals/evaluators`) with the support-triage scenario: five
// evaluators — a code check, an LLM judge, two score/label evaluators
// scoped to offline runs, and a production-only classifier. Answers come
// from `evalFixtures.ts`'s `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { EvaluatorsView } from "./EvaluatorsView";
import { SCENARIO_RANGE } from "./evalFixtures";
import { evalPage, evalPageMeta, pageStories } from "./evalStories";

const EvaluatorsPage = evalPage(EvaluatorsView, "/evals/evaluators", {
  range: SCENARIO_RANGE,
});

const meta = {
  title: "Pages/Evals/Evaluators",
  ...evalPageMeta,
} satisfies Meta<typeof EvaluatorsPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(EvaluatorsPage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;
