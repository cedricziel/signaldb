---
audience: user
type: reference
status: living
sources:
  - src/common/src/self_monitoring/app_metrics.rs
  - src/common/src/flight/trace_context.rs
  - src/ui/src/telemetry/serverTiming.ts
  - src/ui/src/telemetry/serverCorrelationSpanProcessor.ts
---

# Trace Context on HTTP Responses

Every SignalDB HTTP response returns the trace context of the server span that
handled the request, plus server-side timing. This lets you correlate your own
telemetry (browser RUM, an instrumented client) with SignalDB's trace of the
same request, and see where server time went without opening a trace viewer.

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
file defines — those are the `signaldb.*` metrics exported over OTLP, not
part of the response headers described here.

## Correlating a request with its trace

If your client already sends a `traceparent` request header, SignalDB joins
your trace: the returned trace id equals the one you sent, so your existing
trace contains the server's spans. The response header is then confirmation —
if the returned trace id differs from what you sent, an intermediary stripped
your header.

If your client cannot send `traceparent` — above all the browser's initial
document request — the response header is the only way to correlate. The
SignalDB UI does this itself: it reads the navigation entry's `serverTiming`
`traceparent` metric and links its `documentLoad` span to the server span
that served the page.

To find a request's trace by hand: copy the trace id from the `traceresponse`
header (visible in browser DevTools → Network → Headers, or `curl -sD -`) and
look it up in the trace view of the self-monitoring tenant.

## Reading timings without any tooling

```bash
curl -sD - -o /dev/null \
  -H "Authorization: Bearer $API_KEY" -H "X-Tenant-ID: acme" \
  "https://signaldb.example.com/tempo/api/traces/0af7651916cd43dd8448eb211c80319c" \
  | grep -i -E "server-timing|traceresponse"
```

Browser DevTools render `Server-Timing` graphically: Network panel → select a
request → Timing tab.
