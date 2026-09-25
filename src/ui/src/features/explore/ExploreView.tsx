import { useEffect, useRef, useState } from "react";
import { Link } from "react-router";
import { RefreshButton } from "../../components/RefreshButton";
import { TimeRangePicker } from "../../components/TimeRangePicker";
import { CatalogView } from "../catalog/CatalogView";
import { ErrorsView } from "../errors/ErrorsView";
import { LogsView } from "../logs/LogsView";
import { MetricsView } from "../metrics/MetricsView";
import { ProfilesView } from "../profiles/ProfilesView";
import { TracesView } from "../traces/TracesView";
import { QueryView } from "../query/QueryView";
import { supportsLive } from "../../lib/live";
import { DEFAULT_RANGE } from "../../lib/time";
import { computeEdgeOverflow } from "./scrollFade";
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
  // Checked against a relative placeholder range so this isolates the
  // signal-only half of supportsLive's check, kept distinct from the range
  // check below so the button's title can name the actual reason.
  const liveUnsupportedView = !supportsLive(state.signal, DEFAULT_RANGE);
  const liveUnsupportedRange = state.range.type === "absolute";
  const liveDisabled = liveUnsupportedView || liveUnsupportedRange;
  const liveTitle = liveUnsupportedView
    ? "Live tail isn't available on this view"
    : liveUnsupportedRange
      ? "Live tail needs a relative time range"
      : undefined;
  // Views read `state.live` straight from what they're handed to decide
  // whether to poll; when Live isn't actually available (wrong signal or an
  // absolute range) that must read false there too, or a view can keep
  // polling a fixed window after the toggle itself goes disabled.
  const viewState: ExploreState = liveDisabled
    ? { ...state, live: false }
    : state;
  // On a phone the tab strip scrolls sideways; keep the active tab in view
  // when landing directly on one past the edge (e.g. /query).
  const activeTabRef = useRef<HTMLAnchorElement>(null);
  useEffect(() => {
    activeTabRef.current?.scrollIntoView?.({
      block: "nearest",
      inline: "nearest",
    });
  }, [state.signal]);
  // The scroll itself has no visible affordance otherwise — a tab strip that
  // silently scrolls looks like the full set, so a tab off the right edge
  // (Errors/Query from /logs, say) is easy to miss entirely. Mask-fade
  // whichever edge still has hidden tabs, recomputed on scroll/resize since
  // scrollIntoView above and window rotation both move it without a signal
  // change.
  const tabsRef = useRef<HTMLDivElement>(null);
  const [edgeFade, setEdgeFade] = useState({ left: false, right: false });
  useEffect(() => {
    const el = tabsRef.current;
    if (!el) return;
    const update = () => setEdgeFade(computeEdgeOverflow(el));
    update();
    el.addEventListener("scroll", update, { passive: true });
    window.addEventListener("resize", update);
    return () => {
      el.removeEventListener("scroll", update);
      window.removeEventListener("resize", update);
    };
  }, []);

  return (
    <div className="explore">
      <div className="explore-controls">
        <div
          className={`signal-tabs${edgeFade.left ? " fade-left" : ""}${edgeFade.right ? " fade-right" : ""}`}
          ref={tabsRef}
          role="tablist"
          aria-label="Signal"
        >
          {/* Screen-reader users get every tab regardless via native list
              semantics; the fade is a sighted-only "more content" hint. */}
          {SIGNAL_TABS.map((tab) => (
            <Link
              key={tab.id}
              role="tab"
              className="sigtab"
              aria-selected={state.signal === tab.id}
              ref={state.signal === tab.id ? activeTabRef : undefined}
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
          <RefreshButton />
          <button
            className="livebtn btn"
            aria-pressed={liveDisabled ? false : state.live}
            aria-disabled={liveDisabled}
            disabled={liveDisabled}
            title={liveTitle}
            onClick={() => update({ live: !state.live })}
          >
            <span className="live-pip" /> Live
          </button>
        </div>
      </div>

      {state.signal === "catalog" && (
        <CatalogView state={viewState} update={update} />
      )}
      {state.signal === "logs" && (
        <LogsView state={viewState} update={update} />
      )}
      {state.signal === "traces" && (
        <TracesView state={viewState} update={update} />
      )}
      {state.signal === "metrics" && (
        <MetricsView state={viewState} update={update} />
      )}
      {state.signal === "profiles" && (
        <ProfilesView state={viewState} update={update} />
      )}
      {state.signal === "errors" && (
        <ErrorsView state={viewState} update={update} />
      )}
      {state.signal === "query" && (
        <QueryView state={viewState} update={update} />
      )}
    </div>
  );
}
