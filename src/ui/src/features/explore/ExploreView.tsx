import { Link } from "react-router";
import { TimeRangePicker } from "../shell/TimeRangePicker";
import { CatalogView } from "../catalog/CatalogView";
import { ErrorsView } from "../errors/ErrorsView";
import { LogsView } from "../logs/LogsView";
import { MetricsView } from "../metrics/MetricsView";
import { ProfilesView } from "../profiles/ProfilesView";
import { TracesView } from "../traces/TracesView";
import { QueryView } from "../query/QueryView";
import {
  crossSignalSearch,
  type ExploreState,
  type Signal,
} from "../../lib/urlState";
import "./explore.css";

const SIGNAL_TABS: { id: Signal; label: string }[] = [
  { id: "catalog", label: "Catalog" },
  { id: "logs", label: "Logs" },
  { id: "traces", label: "Traces" },
  { id: "metrics", label: "Metrics" },
  { id: "profiles", label: "Profiles" },
  { id: "errors", label: "Errors" },
  { id: "query", label: "Query" },
];

interface Props {
  state: ExploreState;
  update: (patch: Partial<ExploreState>) => void;
}

export function ExploreView({ state, update }: Props) {
  return (
    <div className="explore">
      <div className="explore-controls">
        <div className="signal-tabs" role="tablist" aria-label="Signal">
          {SIGNAL_TABS.map((tab) => (
            <Link
              key={tab.id}
              role="tab"
              className="sigtab"
              aria-selected={state.signal === tab.id}
              // Every click — including re-clicking the tab you're already
              // on — targets that signal's bare main view: crossSignalSearch
              // drops filters/search/drill-down state, and (for the traces
              // tab specifically) the target path has no :traceId segment,
              // so it also steps back out of a single-trace view.
              to={`/${tab.id}${crossSignalSearch(state)}`}
            >
              {tab.label}
            </Link>
          ))}
        </div>
        <div className="explore-controls-right">
          <TimeRangePicker
            range={state.range}
            onChange={(range) => update({ range })}
          />
          <button
            className="livebtn"
            aria-pressed={state.live}
            onClick={() => update({ live: !state.live })}
          >
            <span className="live-pip" /> Live
          </button>
        </div>
      </div>

      {state.signal === "catalog" && (
        <CatalogView state={state} update={update} />
      )}
      {state.signal === "logs" && <LogsView state={state} update={update} />}
      {state.signal === "traces" && (
        <TracesView state={state} update={update} />
      )}
      {state.signal === "metrics" && (
        <MetricsView state={state} update={update} />
      )}
      {state.signal === "profiles" && (
        <ProfilesView state={state} update={update} />
      )}
      {state.signal === "errors" && (
        <ErrorsView state={state} update={update} />
      )}
      {state.signal === "query" && <QueryView range={state.range} />}
    </div>
  );
}
