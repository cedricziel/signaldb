// The Real users page's session detail (`?session=`): a header, the lane
// timeline (`SessionTimeline`), the same events as an ordered list, the
// selected event's own detail panel (`SessionEventDetail` — an inline trace
// waterfall for a network event; an exception's stack frames ship in a
// later branch), and the session's own resource attributes. Selecting an
// event highlights it in both the timeline and the list.
import { useMemo, useState } from "react";
import { EmptyState } from "../../components/EmptyState";
import { QueryError } from "../../components/QueryError";
import { formatTimestamp } from "../../lib/time";
import { formatDurationMs } from "../../lib/waterfall";
import type { ExploreState } from "../../lib/urlState";
import { Panel } from "./Panel";
import { SessionEventDetail } from "./SessionEventDetail";
import { eventKey, SessionTimeline } from "./SessionTimeline";
import type { SessionEvent } from "../../api/rumSessionDetail";
import { SESSION_LANE_LABELS, sessionEventLabel } from "./rumModel";
import { useRumSessionDetail, type RumScope } from "./useRumData";

interface Props {
  scope: RumScope;
  state: ExploreState;
  sessionId: string;
}

export function SessionDetailView({ scope, state, sessionId }: Props) {
  const detail = useRumSessionDetail(scope, sessionId);
  const [selected, setSelected] = useState<SessionEvent | null>(null);

  const span = useMemo(() => {
    if (detail.events.length === 0) return null;
    const first = BigInt(detail.events[0]!.tsNs || "0");
    const last = BigInt(detail.events[detail.events.length - 1]!.tsNs || "0");
    return { first, last: last > first ? last : first + 1n };
  }, [detail.events]);

  const latestLog = [...detail.events]
    .reverse()
    .find((e): e is Extract<SessionEvent, { kind: "log" }> => e.kind === "log");
  const attributes = latestLog?.resourceAttributes ?? {};

  return (
    <div className="rum-stack">
      <Panel
        title={`Session ${sessionId}`}
        meta={
          span
            ? `${formatTimestamp(Number(span.first / 1_000_000n))} · ${detail.events.length} records`
            : undefined
        }
      >
        {detail.isError ? (
          <QueryError what="session detail" error={detail.error} />
        ) : detail.isPending ? (
          <div className="rum-placeholder">Loading…</div>
        ) : detail.events.length === 0 ? (
          <EmptyState title="No records for this session in the window" />
        ) : (
          <>
            {span && (
              <SessionTimeline
                events={detail.events}
                span={span}
                selected={selected}
                onSelect={setSelected}
              />
            )}
            {detail.hasMore && (
              <div className="rum-session-cap-notice">
                <span>
                  {detail.moreCount !== undefined
                    ? `${detail.moreCount.toLocaleString()} more records exist beyond the first ${detail.events.length.toLocaleString()}.`
                    : "More records exist beyond what's shown."}
                </span>
                <button
                  type="button"
                  className="btn"
                  disabled={detail.isLoadingMore}
                  onClick={detail.loadMore}
                >
                  {detail.isLoadingMore ? "Loading…" : "Load more"}
                </button>
              </div>
            )}
          </>
        )}
      </Panel>

      {detail.events.length > 0 && (
        <Panel title="Events">
          <SessionEventList
            events={detail.events}
            selected={selected}
            onSelect={setSelected}
          />
        </Panel>
      )}

      {selected && selected.kind === "span" && (
        <Panel title="Selected event">
          <SessionEventDetail
            scope={scope}
            state={state}
            event={selected}
            events={detail.events}
            onSelectEvent={setSelected}
          />
        </Panel>
      )}

      {Object.keys(attributes).length > 0 && (
        <Panel
          title="Session attributes"
          meta="resource attributes of the latest record"
        >
          <dl className="rum-kv">
            {Object.entries(attributes).map(([key, value]) => (
              <div className="rum-kv-row" key={key}>
                <dt className="mono dim rum-kv-term">{key}</dt>
                <dd className="rum-kv-desc mono">{String(value)}</dd>
              </div>
            ))}
          </dl>
        </Panel>
      )}
    </div>
  );
}

function SessionEventList({
  events,
  selected,
  onSelect,
}: {
  events: SessionEvent[];
  selected: SessionEvent | null;
  onSelect: (event: SessionEvent) => void;
}) {
  // The event's own index in `events` disambiguates `eventKey` for logs
  // that share a timestamp and event name — see its own doc comment.
  const selectedIndex = selected ? events.indexOf(selected) : -1;
  const selectedKey =
    selected && selectedIndex >= 0 ? eventKey(selected, selectedIndex) : null;
  return (
    <div className="rum-session-events">
      {events.map((event, index) => (
        <button
          key={eventKey(event, index)}
          type="button"
          className={
            selectedKey === eventKey(event, index)
              ? "rum-session-event-row on"
              : "rum-session-event-row"
          }
          aria-current={
            selectedKey === eventKey(event, index) ? "true" : undefined
          }
          onClick={() => onSelect(event)}
        >
          <span className="mono dim">
            {formatTimestamp(
              Math.round(Number(BigInt(event.tsNs || "0")) / 1_000_000),
            )}
          </span>
          <span
            className={
              event.lane === "errors"
                ? "rum-session-event-lane err"
                : "rum-session-event-lane"
            }
          >
            {SESSION_LANE_LABELS[event.lane]}
          </span>
          <span className="ell">{sessionEventLabel(event)}</span>
          {event.kind === "span" && (
            <span className="mono num dim">
              {formatDurationMs(Number(BigInt(event.durationNs || "0")) / 1e6)}
            </span>
          )}
        </button>
      ))}
    </div>
  );
}
