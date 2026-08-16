# client-retry-on-throttle Specification

## Purpose

Defines the retry contract shared by SignalDB's generated clients — the Rust SDK behind the CLI and MCP server, and the TypeScript client behind the UI — so a throttled or transiently failing request is retried the same way on every surface and only surfaces to a person or agent once the client has genuinely given up.

## Requirements

### Requirement: Throttled requests are retried before they fail

A SignalDB client SHALL retry a request that SignalDB rejects with `429 Too Many Requests`, regardless of HTTP method, because a throttled request was not processed. It SHALL retry a request that fails with `502`, `503`, `504`, a connection failure, or a request timeout only when the method is idempotent (`GET`, `HEAD`, `PUT`, `DELETE`). Other failures (`4xx` other than `429`, `500`, malformed responses) SHALL NOT be retried.

#### Scenario: A burst-throttled read succeeds on retry

- **WHEN** a client's request is answered `429` with `Retry-After: 1` and the same request would succeed a second later
- **THEN** the client waits and re-sends it, and the caller receives the successful result with no error surfaced

#### Scenario: A throttled write is retried too

- **WHEN** a `POST` (for example a Query IR request or an API-key creation) is answered `429`
- **THEN** the client retries it, because a `429` means the server did not act on it

#### Scenario: A non-idempotent transient failure is not retried

- **WHEN** a `POST` fails with `503` or a connection reset
- **THEN** the client does not retry and reports the failure, because the server may have acted on the request

#### Scenario: A client error is not retried

- **WHEN** a request is answered `400`, `401`, `403`, or `404`
- **THEN** the client reports it immediately without waiting

### Requirement: The wait honours the server, then backs off

When the rejection carries `Retry-After` (seconds or an HTTP date), the client SHALL wait at least that long before the next attempt. Otherwise it SHALL wait an exponentially growing, fully jittered delay starting from a sub-second base. Every wait SHALL be capped by a per-attempt ceiling; if the server asks for a wait beyond that ceiling the client SHALL fail immediately with the throttling error rather than block, so callers that cannot afford the wait learn at once.

#### Scenario: Retry-After is respected

- **WHEN** a `429` carries `Retry-After: 2`
- **THEN** the client's next attempt is not sent before two seconds have elapsed

#### Scenario: Missing Retry-After falls back to jittered backoff

- **WHEN** a retryable failure carries no `Retry-After`
- **THEN** successive waits grow exponentially with random jitter, none exceeding the per-attempt ceiling

#### Scenario: An unaffordable wait fails fast

- **WHEN** a `429` carries a `Retry-After` larger than the policy's per-attempt ceiling
- **THEN** the client does not wait and reports the throttling error, including the server-stated wait, immediately

### Requirement: Retries are bounded and cancellable

The client SHALL stop after a bounded number of attempts and a bounded total wait; the last failure is what the caller sees. The policy SHALL be configurable per client and disable-able. In the browser the client SHALL respect the caller's `AbortSignal` so leaving a page or superseding a query cancels an in-progress wait; the Rust SDK SHALL respect the request's overall timeout the same way.

#### Scenario: Attempts are bounded

- **WHEN** every attempt up to the policy's maximum is answered `429`
- **THEN** the client stops and reports the final `429`, and the total number of requests sent equals the maximum attempts

#### Scenario: Cancellation ends a wait

- **WHEN** a UI request is aborted while the client is waiting to retry
- **THEN** no further attempt is sent and the caller receives the abort, not a throttling error

#### Scenario: Retry can be disabled

- **WHEN** a consumer constructs a client with retry disabled
- **THEN** every failure, including `429`, is reported on the first attempt

### Requirement: Exhaustion is reported with the server-stated wait

When retries are exhausted on a `429`, each surface SHALL tell its caller that the request was throttled and, when the server stated one, how long it asked to wait: the Rust SDK exposes the wait on the error so consumers can render it; the CLI prints it and exits with the throttled exit code; the MCP server returns a throttled tool error carrying it; the UI's shared error surface shows a throttling message distinct from a generic failure.

#### Scenario: CLI reports throttling

- **WHEN** a CLI command is throttled past the retry budget with `Retry-After: 5` on the last response
- **THEN** stderr states the command was rate limited and the server asked to retry in 5 seconds, and the process exits with the throttled exit code

#### Scenario: MCP reports throttling

- **WHEN** an MCP tool call is throttled past the retry budget
- **THEN** the tool returns a throttled error naming the server-stated wait, not a generic internal error

#### Scenario: UI reports throttling

- **WHEN** an Explore request is throttled past the retry budget
- **THEN** the panel shows a throttling message that names the wait rather than a generic "request failed"

### Requirement: Retry is visible while it happens

While a client is waiting to retry, the surface SHALL make that visible where a person is watching — the UI shows the panel as still loading (with an unobtrusive "retrying after throttling" hint), and the CLI writes a single stderr note per retry when connected to a terminal — and the Rust SDK emits one structured `debug` event per retry (`attempt`, `wait_ms`, `status`) so retries are observable in self-monitoring without being noisy.

#### Scenario: UI keeps loading during a retry

- **WHEN** an Explore panel's request is being retried after a `429`
- **THEN** the panel remains in its loading state and does not flash an error between attempts

#### Scenario: Retries are traceable

- **WHEN** an SDK request is retried
- **THEN** one `debug` event per retry is recorded with the attempt number, the wait, and the triggering status
