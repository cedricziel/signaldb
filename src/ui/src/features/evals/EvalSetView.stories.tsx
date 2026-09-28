// One eval set (`/evals/sets/triage-golden-200`) with the support-triage
// scenario: its cases scored by the candidate run (v1.8.0), and Compare
// linking the baseline and candidate runs. Sets come from
// `evalFixtures.ts`'s `EVAL_SET_*`, runs and scores from `evalsIrResponse`.
import type { Meta, StoryObj } from "@storybook/react-vite";
import type { ShellContext } from "../../lib/outletState";
import { EvalSetView } from "./EvalSetView";
import { SCENARIO_RANGE } from "./evalFixtures";
import {
  evalPage,
  evalPageMeta,
  NO_SETS_ROUTES,
  pageStories,
} from "./evalStories";

function TriageGolden(shell: ShellContext) {
  return <EvalSetView {...shell} name="triage-golden-200" />;
}

const SetPage = evalPage(TriageGolden, "/evals/sets/triage-golden-200", {
  range: SCENARIO_RANGE,
});

const meta = {
  title: "Pages/Evals/Eval Set",
  ...evalPageMeta,
} satisfies Meta<typeof SetPage>;

export default meta;
type Story = StoryObj<typeof meta>;

const stories = pageStories(SetPage);
export const Default: Story = stories.Default;
export const Dark: Story = stories.Dark;

/** The set isn't in this dataset (the API answers 404). */
export const NotFound: Story = {
  render: () => <SetPage stubRoutes={NO_SETS_ROUTES} />,
};
