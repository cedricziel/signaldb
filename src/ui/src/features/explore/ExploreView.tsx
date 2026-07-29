import { TimeRangePicker } from "../shell/TimeRangePicker";
import { LogsView } from "../logs/LogsView";
import { MetricsView } from "../metrics/MetricsView";
import { TracesView } from "../traces/TracesView";
import { useExploreState, type Signal } from "../../lib/urlState";
import "./explore.css";

const SIGNAL_TABS: { id: Signal; label: string }[] = [
  { id: "logs", label: "Logs" },
  { id: "traces", label: "Traces" },
  { id: "metrics", label: "Metrics" },
];

export function ExploreView() {
  const [state, update] = useExploreState();

  return (
    <div className="explore">
      <div className="explore-controls">
        <div className="signal-tabs" role="tablist" aria-label="Signal">
          {SIGNAL_TABS.map((tab) => (
            <button
              key={tab.id}
              role="tab"
              className="sigtab"
              aria-selected={state.signal === tab.id}
              onClick={() => update({ signal: tab.id })}
            >
              {tab.label}
            </button>
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

      {state.signal === "logs" && <LogsView state={state} update={update} />}
      {state.signal === "traces" && (
        <TracesView state={state} update={update} />
      )}
      {state.signal === "metrics" && (
        <MetricsView state={state} update={update} />
      )}
    </div>
  );
}
