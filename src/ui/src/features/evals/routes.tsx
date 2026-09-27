// Route tree for the Evaluate section (`/evals`), spliced into the app's
// routes. Every page reads the shell's URL-backed state.
import type { ComponentType } from "react";
import { Route } from "react-router";
import { useOutletState, type ShellContext } from "../../lib/outletState";
import { AgentsScoresView } from "./AgentsScoresView";
import { CaseView } from "./CaseView";
import { CompareView } from "./CompareView";
import { EvaluatorsView } from "./EvaluatorsView";
import { RunsView } from "./RunsView";

function withShell(View: ComponentType<ShellContext>) {
  return function EvalRoute() {
    const { state, update } = useOutletState();
    return <View state={state} update={update} />;
  };
}

const AgentsScoresRoute = withShell(AgentsScoresView);
const CompareRoute = withShell(CompareView);
const CaseRoute = withShell(CaseView);
const RunsRoute = withShell(RunsView);
const EvaluatorsRoute = withShell(EvaluatorsView);

export function evalsRoutes() {
  return (
    <Route path="evals">
      <Route index element={<AgentsScoresRoute />} />
      <Route path="compare" element={<CompareRoute />} />
      <Route path="compare/case" element={<CaseRoute />} />
      <Route path="runs" element={<RunsRoute />} />
      <Route path="evaluators" element={<EvaluatorsRoute />} />
    </Route>
  );
}
