# http-response-trace-context — Delta Spec

## Purpose

Every SignalDB HTTP response returns the server-side trace context and server-side stage timings to the caller, so browsers, RUM SDKs, and standards-compliant clients can correlate their own telemetry with SignalDB's trace and inspect server timing without a trace viewer.

## ADDED Requirements

### Requirement: HTTP responses carry server trace context via Server-Timing

Every HTTP response from a SignalDB service that minted a server span for the request SHALL include a `Server-Timing` header entry of the form `traceparent;desc="<version>-<trace-id>-<span-id>-<trace-flags>"`, where the value in `desc` is the W3C Trace Context representation of the server span handling the request (the span id is the server span's own id, not the caller's).

#### Scenario: API response returns the server span context

- **WHEN** a client sends a request to a SignalDB HTTP endpoint (with or without a `traceparent` request header) and self-monitoring tracing is active
- **THEN** the response includes `Server-Timing: traceparent;desc="00-<trace-id>-<span-id>-<flags>"` where `<trace-id>` matches the trace of the server span recorded for the request

#### Scenario: Caller-supplied trace context is joined, not replaced

- **WHEN** a client sends a valid sampled `traceparent` request header
- **THEN** the `trace-id` in the response's `Server-Timing: traceparent` entry equals the trace id from the request header, and the `span-id` differs from the caller's span id

### Requirement: HTTP responses carry the W3C traceresponse header

Alongside the `Server-Timing` entry, responses SHALL include a `traceresponse` header (W3C Trace Context Level 2) with the same `<version>-<trace-id>-<span-id>-<trace-flags>` value.

#### Scenario: traceresponse matches Server-Timing traceparent

- **WHEN** a response includes the `Server-Timing: traceparent` entry
- **THEN** it also includes a `traceresponse` header whose value equals the `desc` value of the `Server-Timing` traceparent entry

### Requirement: Server-Timing carries stage duration metrics

Responses that carry trace context SHALL also include at least one `Server-Timing` entry with a `dur` value reporting total server-side processing time in milliseconds (entry name `total`). Endpoints MAY append further named entries for internal stages (e.g. query planning, execution, storage access) when those timings are available; every `dur` value SHALL be reported in milliseconds per the Server-Timing specification.

#### Scenario: Total duration reported

- **WHEN** a request completes on an instrumented HTTP endpoint
- **THEN** the response includes a `Server-Timing` entry `total;dur=<ms>` where `<ms>` is a non-negative number approximating the server-side handling duration

### Requirement: No headers without a valid span context

When the request produced no valid server span context — self-monitoring tracing is disabled, or the request is exempt from instrumentation (e.g. the `_system` self-monitoring tenant bypass) — the response SHALL NOT include a `traceresponse` header or a `Server-Timing: traceparent` entry. An all-zero or otherwise invalid trace context SHALL never be emitted.

#### Scenario: Self-monitoring disabled

- **WHEN** self-monitoring tracing is not configured and a client sends a request
- **THEN** the response contains no `traceresponse` header and no `Server-Timing` `traceparent` entry

#### Scenario: Self-monitoring tenant bypass

- **WHEN** a request carries the self-monitoring tenant id (`_system`)
- **THEN** the response contains no `traceresponse` header and no `Server-Timing` `traceparent` entry

### Requirement: Sampling decision is reflected in trace flags

The `trace-flags` field of the returned context SHALL reflect the server's actual sampling decision for the span (`01` sampled, `00` not sampled), so clients can avoid parenting telemetry under a span that will never be exported.

#### Scenario: Unsampled server span

- **WHEN** the server's sampler decides not to sample the request's span
- **THEN** the returned trace context ends in trace-flags `00`

### Requirement: Timing metadata is readable cross-origin

Responses carrying `Server-Timing` entries SHALL include a `Timing-Allow-Origin` header permitting configured cross-origin consumers (e.g. a Grafana instance hosting the SignalDB plugin) to read `serverTiming` performance entries. Same-origin consumers require no configuration.

#### Scenario: Cross-origin consumer reads serverTiming

- **WHEN** a browser page on an allowed cross-origin host fetches a SignalDB API endpoint
- **THEN** the response's `Timing-Allow-Origin` header permits that origin, making the `serverTiming` entries visible to the page's Performance API

### Requirement: All HTTP services emit uniformly

The behavior above SHALL apply to every SignalDB service exposing an HTTP surface (router and acceptor today, including monolithic mode), via shared middleware — not per-service opt-in — so future HTTP surfaces inherit it automatically.

#### Scenario: Acceptor OTLP HTTP response

- **WHEN** a client posts OTLP data to the acceptor's HTTP endpoint with tracing active
- **THEN** the response carries the same `Server-Timing` traceparent, `traceresponse`, and `total;dur=` entries as router responses
