# ui-server-correlation — Delta Spec

## Purpose

The SignalDB browser UI picks up the trace context the server returns on responses and ties client-side telemetry to it, closing the correlation gap for requests the client cannot instrument — above all the initial document load.

## ADDED Requirements

### Requirement: Document load telemetry references the server's document-request trace

The UI's document-load telemetry SHALL read the navigation performance entry's `serverTiming` list, parse a `traceparent` entry when present, and attach the server's trace context to the document-load span as a span link carrying the server-side trace id and span id.

#### Scenario: Server returned trace context on the document response

- **WHEN** the SPA document response included `Server-Timing: traceparent;desc="00-<trace-id>-<span-id>-01"` and the UI initializes telemetry
- **THEN** the exported document-load span carries a span link whose trace id and span id equal the server-returned values

#### Scenario: No serverTiming on the document response

- **WHEN** the document response carried no `Server-Timing` traceparent entry (older server, proxy stripped it, or tracing disabled)
- **THEN** document-load telemetry is produced exactly as before, with no link and no errors

### Requirement: Malformed or unsampled server context degrades gracefully

The UI SHALL validate the parsed context before use: a malformed `desc` value SHALL be ignored, and a context with trace-flags `00` (server sampled out) SHALL NOT be used to parent client spans. Telemetry initialization SHALL never fail because of server-timing parsing.

#### Scenario: Malformed desc value

- **WHEN** the navigation entry contains `traceparent;desc="garbage"`
- **THEN** the UI ignores it, emits no link, and telemetry initialization completes normally

#### Scenario: Server sampled out

- **WHEN** the server-returned context has trace-flags `00`
- **THEN** the document-load span is not parented under the server span (a link MAY still be recorded)
