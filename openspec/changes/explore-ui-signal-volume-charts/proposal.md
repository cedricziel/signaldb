## Why

The logs tab's volume histogram is unreadable on real data, and the traces tab
has no volume chart at all.

Measured against `_system/_monitoring` on a live deployment, last 24h — the
backend returned all 25 buckets over the full window (725,812 lines, nothing
truncated), but the chart rendered as one tall bar and a flat line:

| bucket | lines   | rendered height (of 68px) |
| ------ | ------- | ------------------------- |
| 09:00  | 525     | 3.0px                     |
| 12:00  | 20,369  | 4.7px                     |
| 23:00  | 125,461 | 24.2px                    |
| 08:00  | 373,329 | 69.9px                    |
| 09:00  | 10,240  | 3.8px                     |

Five compounding defects in `src/ui/src/features/logs/Histogram.tsx`:

1. **Linear scale against a 36:1 dynamic range.** The peak bucket sets `max`,
   so the body of the distribution renders at 5% of the available height.
2. **The 1px floor is applied per stacked segment, not per bar.** A bucket with
   three levels present gets a 3px floor regardless of count, so 525 lines
   (3.0px) and 10,240 lines (3.8px) are visually identical.
3. **No y-axis.** A genuinely-empty stretch and an 8,000-lines/hour stretch are
   indistinguishable.
4. **Time-axis labels are time-of-day only.** `formatTimestamp` always renders
   `HH:MM:SS.mmm`, so a 24h range labels itself `09:00:00.000 → 09:00:00.000`.
5. **The tooltip is unreachable.** `.histo-bar` is sized by its content inside
   an `align-items: flex-end` row, so the hover target _is_ the drawn bar —
   measured at 2.0px tall in a 72px plot. The existing native `title` also
   reports only the bucket total, never the per-level split that is the entire
   reason the chart is stacked.

Separately, the `500 rows (limit 500)` badge is rendered inside the chart's own
`.histo-wrap` container. Two unrelated queries share one visual box, so a flat
chart reads as "the row limit truncated the chart" — it did not, and it cannot:
the histogram request carries no `limit`, and `state.limit` is not in its React
Query key.

The traces tab has no chart. The tempting shortcut — bucketing the
`tempoSearch` result client-side — would be limited to the newest `limit`
traces and would genuinely produce the truncated-looking chart users already
believe they are seeing. The correct vehicle already exists and was verified
working against a live deployment: a Query IR `series` document over the
`traces` source returns full-window, unlimited, time-bucketed counts, and
`status.code` resolves to `Error`/`Unspecified` — the direct analogue of logs'
`level`.

## What Changes

- Extract a shared `SignalHistogram` component from `Histogram.tsx`. It takes
  generic `{labels, points}` series plus a series-ordering/colour strategy, so
  logs (by `level`) and traces (by `status.code`) are two callers of one
  component rather than two copies of six rendering defects.
- **Bar height**: move the 1px minimum from the stacked segment to the bar, and
  distribute the bar's height across segments by each series' linear share.
- **Scale**: linear by default with a persisted log toggle. Log scales the bar
  _total_ (`log1p(total)/log1p(max)`) and splits that height linearly by series
  share — `log(a)+log(b) ≠ log(a+b)`, so segments cannot be log-transformed
  individually without lying about composition.
- **Y-axis**: gridlines with a labelled maximum, so a flat region is readable
  as a number.
- **Time axis**: labels include the date when the window spans more than one
  calendar day.
- **Hover**: a full-plot-height transparent hit zone per bucket, a hover
  highlight, and a styled tooltip listing every series' value and the bucket
  total — replacing the native `title` attribute.
- **Traces**: the traces tab renders a volume chart driven by a Query IR
  `series` document (`from: "traces"`, `aggregate` with `step`), stacked by
  `status.code`, covering the full selected window independently of the trace
  row limit.
- **Limit badge**: the row-count/limit indicator moves out of the chart
  container so it no longer reads as a statement about the chart.

Not breaking: no API, wire-format, or storage change. `/api/v1/query` and the
Loki compat endpoints are unchanged, and `queryIr` is already present in the
generated TypeScript client, so no OpenAPI or SDK regeneration is required.

Surfaces explicitly scoped out: this is a rendering change to the web UI only.
There is no CLI or HTTP-API counterpart to a chart, and every query it issues
uses an endpoint that already exists and is already generated.

## Capabilities

### New Capabilities

- `explore-ui-signal-volume`: the explore UI presents signal volume over the
  selected window as a stacked time-series chart whose values are readable and
  interrogable — driven by a server-side aggregate over the full window rather
  than by client-side bucketing of a row-limited result set.

### Modified Capabilities

(none — `explore-ui-navigation` governs tab/URL-state navigation and is
unchanged.)

## Impact

- **src/ui** only. New `src/ui/src/features/explore/SignalHistogram.tsx` (plus
  tests and CSS); `src/ui/src/features/logs/Histogram.tsx` becomes a thin
  logs-specific adapter or is removed in favour of a direct caller;
  `src/ui/src/features/logs/LogsView.tsx` and
  `src/ui/src/features/traces/TracesView.tsx` gain the chart wiring;
  `src/ui/src/lib/time.ts` gains a date-aware axis formatter;
  `src/ui/src/features/explore/explore.css` gains axis/tooltip/hover styles.
- **No Rust crates are touched.** No `acceptor`, `router`, `querier`, `writer`,
  `common`, or `tests-integration` change.
- Docs: the explore-UI user documentation gains the scale toggle and tooltip
  behaviour (routed via the docs skill).

## Out of scope

Two unrelated defects surfaced while verifying the Query IR path against a live
deployment. Both are backend bugs with no bearing on this change and are filed
separately:

- An unknown `aggregate.by` field name silently resolves to a null-valued
  column — `by: ["status"]`, `["span.status"]`, `["span_status"]` each return
  HTTP 200 with a single series labelled `"null"`, while `["statusCode"]`
  correctly returns 500 `FieldNotFound`. The silent branch returns confidently
  wrong data to a caller who typo'd a field.
- That 500's message reads `Profile query failed` for a `from: "traces"`
  document — wrong signal name in the error path.
