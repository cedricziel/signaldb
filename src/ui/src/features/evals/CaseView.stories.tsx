// Case drilldown (`/evals/compare/case`) on case-117 — "Refund order
// #88213, it arrived broken" — where the candidate (v1.8.0) skips the
// check_policy tool call the baseline (v1.7.3) made, and both
// ToolTrajectory and Correctness fail as a result. Answers come from
// `evalFixtures.ts`'s `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import { DEFAULT_EVAL_PARAMS } from "../../lib/urlState";
import { CaseView } from "./CaseView";
import { evalPage, evalPageMeta, pageStories } from "./evalStories";

const CasePage = evalPage(CaseView, "/evals/compare/case", {
  evals: { ...DEFAULT_EVAL_PARAMS, case: "case-117" },
});

const meta = {
  title: "Pages/Evals/Case",
  ...evalPageMeta,
} satisfies Meta<typeof CasePage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(CasePage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;
