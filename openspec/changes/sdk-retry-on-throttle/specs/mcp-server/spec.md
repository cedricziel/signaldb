## ADDED Requirements

### Requirement: Throttling is retried before it is reported

When a tool's downstream call is throttled by SignalDB, the MCP server SHALL let the SDK's shared retry policy absorb the throttling first (waiting the server-stated `Retry-After` within the policy's bounds) and SHALL only surface an error once retries are exhausted. That error SHALL be a distinct throttled tool error — not a generic internal error — and SHALL name the wait the server asked for when one was stated, so an agent can decide to wait or narrow the query.

#### Scenario: A brief throttle is invisible to the agent

- **WHEN** a tool's downstream request is answered `429` with `Retry-After: 1` and succeeds on the retry
- **THEN** the tool returns the successful result and no error is reported to the MCP client

#### Scenario: Exhausted retries name the wait

- **WHEN** a tool's downstream request is still throttled after the retry policy is exhausted, the last response carrying `Retry-After: 30`
- **THEN** the tool returns a throttled error whose message states the server asked to retry in 30 seconds

#### Scenario: Throttled error is not an internal error

- **WHEN** an MCP client inspects a throttled tool error
- **THEN** it is distinguishable from an internal failure (distinct message prefix and structured `retryAfterMs` in the error data)
