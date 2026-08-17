---
audience: user
type: reference
status: living
sources:
  - src/common/src/self_monitoring/app_metrics.rs
  - src/common/src/flight/trace_context.rs
  - src/router/src/ui.rs
  - src/ui/src/telemetry/serverTiming.ts
  - src/ui/src/telemetry/serverCorrelationSpanProcessor.ts
  - src/ui/src/telemetry/documentTraceContext.ts
---

# Trace Context on HTTP Responses

Every SignalDB HTTP response returns the trace context of the server span that
handled the request, plus server-side timing. This lets you correlate your own
telemetry (browser RUM, an instrumented client) with SignalDB's trace of the
same request, and see where server time went without opening a trace viewer.

The one exception, in the other direction, is the explore UI's own document
request: it carries trace context inline in the HTML body, not just headers —
see [Trace context in the document body](#trace-context-in-the-document-body)
below.

## Headers

For a request that SignalDB traced (self-monitoring enabled), the response
carries:

```text
Server-Timing: traceparent;desc="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", querier;dur=12.480, convert;dur=0.312, total;dur=14.102
traceresponse: 00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01
Timing-Allow-Origin: *
```

| Header / entry                          | Meaning                                                                                                                                                                                                                         |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `Server-Timing: traceparent;desc="..."` | W3C trace context of SignalDB's server span. The span id is the server's own span, not the caller's. Browsers expose this via the Performance API even for requests JavaScript never made (the HTML document, scripts, images). |
| `traceresponse`                         | The same value as a [W3C Trace Context Level 2](https://w3c.github.io/trace-context/#traceresponse-header) header, for standards-compliant clients.                                                                             |
| `<stage>;dur=<ms>`                      | Server-side stage timings in milliseconds. `total` is always present; query endpoints add stages (e.g. `querier` for the Flight round-trip, `convert` for result conversion).                                                   |
| `Timing-Allow-Origin: *`                | Lets cross-origin pages (e.g. a Grafana instance) read the `serverTiming` performance entries.                                                                                                                                  |

The final `-01`/`-00` field reflects SignalDB's sampling decision: `01` means
the server span was sampled and will appear in the self-monitoring trace
store; `00` means it was sampled out.

No headers are emitted when self-monitoring tracing is disabled, or for
`_system` tenant requests (SignalDB's own telemetry traffic).

Handlers accumulate the `<stage>;dur=<ms>` entries via `ServerTimings`
(`src/common/src/self_monitoring/app_metrics.rs`), a small ordered
name/duration list unrelated to the counter/histogram instruments the same
file defines — those are the `signaldb.*` metrics (WAL, Flight and RPC
counters, histograms and gauges) exported over OTLP, not part of the response
headers described here.

## Correlating a request with its trace

If your client already sends a `traceparent` request header, SignalDB joins
your trace: the returned trace id equals the one you sent, so your existing
trace contains the server's spans. The response header is then confirmation —
if the returned trace id differs from what you sent, an intermediary stripped
your header.

If your client cannot send `traceparent` — above all the browser's initial
document request — the response header is a fallback way to correlate: the
SignalDB UI reads the navigation entry's `serverTiming` `traceparent` metric
and links its `documentLoad` span to the server span that served the page.
This is only a fallback because the explore UI has a more reliable mechanism —
see the next section.

To find a request's trace by hand: copy the trace id from the `traceresponse`
header (visible in browser DevTools → Network → Headers, or `curl -sD -`) and
look it up in the trace view of the self-monitoring tenant.

## Trace context in the document body

The `Server-Timing`/`traceresponse` headers above cover every SignalDB
response _except_ one: the explore UI's own `index.html`. Its `serverTiming`
performance entry is only reliably populated _after_ the navigation finishes —
by which point the browser has already created its `documentLoad` span, and a
`SpanProcessor` cannot change a span's parent after creation (see
`src/ui/src/telemetry/serverCorrelationSpanProcessor.ts`).

So for `index.html` specifically, the router (`serve_index_html` in
`src/router/src/ui.rs`) injects the trace context directly into the response
**body**, ahead of `</head>`:

```html
<meta
  name="traceparent"
  content="00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
/>
```

The UI reads this tag before any spans exist (`documentTraceParentContext()`
in `src/ui/src/telemetry/documentTraceContext.ts`) and uses it as the **real
parent** of the `documentLoad` span — not a link, unlike every other
correlation described on this page. That is a deliberate trade-off: it makes
`documentLoad` a genuine child of the router's request span (so span
navigation, not just a same-trace-id link, works between them), at the cost
that OpenTelemetry JS's default `ParentBasedSampler` drops the whole
`documentLoad` subtree whenever the server's span was sampled out
(`traceparent` flags `00`) — the browser accepts its parent's sampling
decision, including "don't record this". A busier self-monitoring sampler
ratio therefore now also thins out browser RUM data for the page loads it
lands on, not just server spans.

No tag is emitted when self-monitoring is disabled entirely (no active span to
read), and the UI falls back to the `Server-Timing` link described above when
the tag is absent — an older router build, or a dev proxy serving `index.html`
directly without going through the router.

## Reading timings without any tooling

```bash
curl -sD - -o /dev/null \
  -H "Authorization: Bearer $API_KEY" -H "X-Tenant-ID: acme" \
  "https://signaldb.example.com/tempo/api/traces/0af7651916cd43dd8448eb211c80319c" \
  | grep -i -E "server-timing|traceresponse"
```

Browser DevTools render `Server-Timing` graphically: Network panel → select a
request → Timing tab.
