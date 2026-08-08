## Context

`Histogram.tsx` was written for one caller. It hard-codes `LEVEL_ORDER` and
`LEVEL_VAR`, accepts a Loki-shaped `HistogramSeries[]`, and owns bucketing,
epoch-grid padding, scaling, and rendering in one file. Adding a traces chart by
copying it would fork all five rendering defects into a second file, so the
component is generalised first and both tabs land on top of it.

No backend work is required. The logs chart's query is already correct; the
traces chart's query uses `/api/v1/query` (Query IR), which is already in the
generated TypeScript client as `queryIr`.

## Goals / Non-Goals

**Goals**

- One chart component, two callers, no duplicated rendering logic.
- Every rendered bucket's value is obtainable by the user without leaving the
  chart.
- Charts reflect the full selected window regardless of any row limit.

**Non-Goals**

- Brush-to-zoom / range selection by dragging on the chart. Worth doing, but it
  is navigation behaviour and belongs with `explore-ui-navigation`.
- Charts for the metrics and profiles tabs. Metrics already has `MetricsChart`;
  profiles has no natural volume dimension. Both can adopt `SignalHistogram`
  later if it earns it.
- A charting library. The current CSS-flexbox rendering is small, fast, and
  themeable; the defects are in the maths and the hit-testing, not the
  technology.

## Decisions

### Bar height: floor at the bar, share within it

Today's `Math.max(1, (count / max) * (height - 4))` runs inside `levels.map()`,
so the floor multiplies by the number of present series. The fix computes the
bar height once and divides it:

```
bar_px = total > 0 ? max(MIN_BAR_PX, scale(total) * available) : 0
seg_px = bar_px * (count_series / total)
```

`MIN_BAR_PX = 1`. A non-zero bucket therefore always renders as at least one
visible pixel, but never more than one pixel's worth of floor, so 525 and
10,240 stop colliding.

### Scale: linear default, log toggle, log applies to the total only

Stacked segments cannot be log-transformed individually — `log(a) + log(b) ≠
log(a+b)`, so the segment heights would not sum to the bar and the composition
would be a lie. Log mode therefore scales the **bar total** and splits that
height by each series' **linear** share:

```
linear:  scale(v) = v / max
log:     scale(v) = log1p(v) / log1p(max)
```

On the measured data (`max` 373,329): 525 → 49%, 10,240 → 72%, 373,329 → 100%.

Linear is the default. In an observability tool the spike is usually the thing
you came for, and log flattens exactly that; with a y-axis and a working
tooltip, linear is honest and readable. The toggle is persisted in explore URL
state so a shared link reproduces what the sender saw.

### Traces volume comes from Query IR, never from search results

Bucketing `tempoSearch`'s response client-side would bound the chart by
`state.limit` and reproduce the exact truncation artefact this change exists to
remove. The chart issues its own aggregate instead:

```json
{
  "irVersion": 1,
  "from": "traces",
  "range": { "from": "<window start>", "to": "<window end>" },
  "result": "series",
  "pipeline": [
    {
      "aggregate": {
        "by": ["status.code"],
        "aggs": [{ "fn": "count", "as": "n" }],
        "step": "<step>"
      }
    }
  ]
}
```

Verified against a live deployment: returns one series per `status.code` value
(`Error`, `Unspecified`), full-window, with no limit applied anywhere on the
path. `status.code` is chosen over `service.name` as the default stack because
it mirrors logs-by-level and answers "is anything failing" at a glance.

The response's `points` are `[epoch_ns, value]`; the logs path yields
`[epoch_s, value]` from the Loki matrix. Both are normalised to milliseconds at
the adapter boundary so `SignalHistogram` only ever sees milliseconds.

### Hover: a hit zone independent of the bar

The hover target becomes a full-plot-height transparent column per bucket,
layered behind the drawn bar, so a 1px bar is as easy to interrogate as a 70px
one. The native `title` attribute is dropped in favour of a rendered tooltip
that lists each series' value and the bucket total — the per-series split being
the point of a stacked chart, and the one thing `title` cannot express.

Keyboard parity: buckets are focusable and arrow keys move between them, so the
values are reachable without a pointer.

### Series ordering and colour stay with the caller

`SignalHistogram` takes an ordered series list and a colour per series key. Logs
supply `debug → info → warn → error → other` with the existing CSS variables;
traces supply `ok → unspecified → error`. The component never inspects label
semantics.

## Risks / Trade-offs

- **Log mode understates spikes.** Mitigated by keeping linear the default and
  labelling the axis with the true maximum in both modes.
- **`status.code` values are OTLP-shaped** (`Unspecified`, `Error`) rather than
  friendly. The adapter maps them to `ok`/`error`/`unset` for display; the
  mapping lives in the traces adapter, not the shared component.
- **A tenant with no traces** yields an empty series list. The chart renders its
  zero-filled grid and an explicit empty state rather than collapsing, matching
  the logs behaviour.

## Migration

None. No persisted state, wire format, or stored query shape changes. The one
user-visible URL-state addition (the scale toggle) is optional and absent from
existing links, which continue to resolve to the linear default.
