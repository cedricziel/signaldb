// Compare (`/evals/compare`) with the support-triage scenario: v1.7.3 vs
// v1.8.0 on triage-golden-200. case-117 regresses (the candidate skips the
// check_policy tool call), case-064 improves (a new search_kb call), and
// case-088 is unchanged (a repeated lookup_order call in both runs).
// Answers come from `evalFixtures.ts`'s `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { CompareView } from "./CompareView";
import { evalPage, evalPageMeta, pageStories } from "./evalStories";

const ComparePage = evalPage(CompareView, "/evals/compare");

const meta = {
  title: "Pages/Evals/Compare",
  ...evalPageMeta,
} satisfies Meta<typeof ComparePage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(ComparePage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;
