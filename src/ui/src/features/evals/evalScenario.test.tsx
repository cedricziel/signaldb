// Every Pages/Evals story rendered against the shared scenario
// (`evalFixtures.ts`), so the fixtures the stories and design-sync rely on
// keep answering each page's queries.
import { composeStories } from "@storybook/react-vite";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import * as agents from "./AgentsScoresView.stories";
import * as caseStories from "./CaseView.stories";
import * as compare from "./CompareView.stories";
import * as evaluators from "./EvaluatorsView.stories";
import * as sets from "./EvalSetsView.stories";
import * as oneSet from "./EvalSetView.stories";
import * as runs from "./RunsView.stories";

const A = composeStories(agents);
const C = composeStories(caseStories);
const P = composeStories(compare);
const E = composeStories(evaluators);
const R = composeStories(runs);
const S = composeStories(sets);
const T = composeStories(oneSet);

describe("Evaluate scenario stories", () => {
  it("Agents & scores: KPIs, the mean-score chart and its bucket tooltip", async () => {
    render(<A.Default />);
    expect(
      await screen.findByText("Mean score by evaluator", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    expect(screen.getByText("Worst-moving evaluator")).toBeInTheDocument();
    const buckets = screen.getAllByRole("button", { name: /^Open runs for/ });
    expect(buckets.length).toBeGreaterThan(1);
    fireEvent.pointerEnter(buckets[0]!);
    fireEvent.focus(buckets[0]!);
    fireEvent.blur(buckets[0]!);
    fireEvent.click(buckets[0]!);
  });

  it("Agents & scores: dark and no-evaluations variants render", async () => {
    render(<A.Dark />);
    expect(
      await screen.findByText("Mean score by evaluator", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    render(<A.NoEvaluations />);
    expect(
      await screen.findByText("No evaluations yet", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
  });

  it("Runs: lists the scenario's runs and the empty state", async () => {
    render(<R.Default />);
    expect(
      await screen.findByText("run-0927-1004", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    render(<R.Dark />);
    render(<R.Empty />);
    expect(
      await screen.findByText("No runs yet", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
  });

  it("Compare: the regressed case and every filter", async () => {
    render(<P.Default />);
    expect(
      await screen.findByText("case-117", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    for (const name of [/Improvements/, /Unchanged/, /Regressions/]) {
      fireEvent.click(screen.getByRole("button", { name }));
    }
    render(<P.Dark />);
  });

  it("Case: trajectory, cards and every view mode", async () => {
    render(<C.Default />);
    expect(
      await screen.findByText("expected, not called", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    const toggles = screen.getAllByRole("button", { expanded: true });
    fireEvent.click(toggles[0]!);
    await waitFor(() =>
      expect(toggles[0]).toHaveAttribute("aria-expanded", "false"),
    );
    render(<C.Dark />);
  });

  it("Eval sets: the scenario's sets, their last run, and the empty state", async () => {
    render(<S.Default />);
    expect(
      await screen.findByText(
        "traces 3 · hand-written 9",
        {},
        { timeout: 5000 },
      ),
    ).toBeInTheDocument();
    expect(await screen.findByText("v1.8.0")).toBeInTheDocument();
    render(<S.Dark />);
    render(<S.Empty />);
    expect(
      await screen.findByText("No eval sets yet", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
  });

  it("Eval set: cases scored by the newest run, Add traces, and a missing set", async () => {
    render(<T.Default />);
    expect(
      await screen.findByText("3 failing", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Add traces…" }));
    fireEvent.click(screen.getByRole("button", { name: "Add up to 50 cases" }));
    expect(
      await screen.findByRole("status", { name: "Last add" }),
    ).toHaveTextContent("Matches 214");
    render(<T.Dark />);
    render(<T.NotFound />);
    expect(
      await screen.findByText(
        "No eval set named triage-golden-200",
        {},
        { timeout: 5000 },
      ),
    ).toBeInTheDocument();
  });

  it("Compare: saving the regressions is offered against the stored set", async () => {
    render(<P.Default />);
    const save = await screen.findByRole(
      "button",
      { name: "Save 1 regressed cases as eval set" },
      { timeout: 5000 },
    );
    await waitFor(() => expect(save).toBeEnabled());
  });

  it("Evaluators: the discovered evaluators", async () => {
    render(<E.Default />);
    expect(
      await screen.findByText("ToolTrajectory", {}, { timeout: 5000 }),
    ).toBeInTheDocument();
    render(<E.Dark />);
  });
});
