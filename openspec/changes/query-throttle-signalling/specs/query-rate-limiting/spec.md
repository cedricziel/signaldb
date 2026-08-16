## Purpose

Defines how SignalDB throttles a tenant's HTTP query and management traffic and how a throttled request is signalled, so every client — the UI, the CLI, the MCP server, and Grafana — can back off precisely instead of guessing.

## ADDED Requirements

### Requirement: Per-tenant query request budget

The router SHALL enforce a configurable per-tenant request-rate budget on its HTTP query and management surfaces (Tempo, Loki, Prometheus, Pyroscope/profiles, Query IR, and the tenant-scoped `/api/v1` management, schema, and identity endpoints). The budget SHALL be a sustained rate (`max_query_requests_per_sec`) with a burst allowance expressed in seconds of budget (`burst_seconds`, default 10.0, minimum 1.0), so a client that fans out a page or investigation worth of requests in one instant is admitted as long as its sustained rate stays under the limit. A tenant with no configured limit SHALL NOT be throttled. Administrative endpoints authenticated with the admin key are governed by their own quotas, not by this budget.

#### Scenario: Interactive fan-out within the burst is admitted

- **WHEN** a tenant limited to 100 requests/second sends 40 requests in the same instant after an idle period
- **THEN** all 40 are admitted, because the burst allowance (10 s of budget) covers them

#### Scenario: Sustained overrun is throttled

- **WHEN** a tenant sends requests faster than its sustained rate for longer than its burst allowance covers
- **THEN** the router rejects the excess requests with `429 Too Many Requests` and does not forward them to the querier

#### Scenario: No limit configured means unlimited

- **WHEN** no query request limit is configured for a tenant (neither a default nor a per-tenant override)
- **THEN** the router admits every request from that tenant without throttling

### Requirement: Throttled responses say when to retry

Every rate-limit rejection issued by SignalDB over HTTP SHALL carry a `Retry-After` header holding the whole number of seconds (rounded up, never below 1) until the rejected request would be admitted, computed from the tenant's actual budget state, plus `X-RateLimit-Limit` (the per-second budget of the rejected dimension) and `X-RateLimit-Burst` (the burst allowance in the dimension's unit). This applies to the router's query budget, the admin API's per-tenant quotas, and the acceptor's OTLP/HTTP and Prometheus `remote_write` limits alike, so a client needs one retry rule for all of SignalDB.

#### Scenario: Retry-After reflects the bucket

- **WHEN** a tenant with a 10 requests/second budget has exhausted its burst and is rejected
- **THEN** the response carries `Retry-After: 1` (the time until one token refills, rounded up), `X-RateLimit-Limit: 10`, and `X-RateLimit-Burst` equal to its burst allowance

#### Scenario: A longer wait is stated honestly

- **WHEN** a rejected request's cost cannot be admitted for several seconds under the current budget
- **THEN** `Retry-After` states that number of seconds rather than a fixed placeholder, so a client that cannot afford the wait can fail fast

#### Scenario: Admin quota rejections carry the same headers

- **WHEN** an admin API call is rejected because a per-tenant quota such as `max_api_keys` is exhausted
- **THEN** the `429` response carries `Retry-After` and `X-RateLimit-Limit`, matching the query surfaces

### Requirement: Throttled responses use the surface's error envelope

A router `429` SHALL be a JSON body in the same error envelope as the surface's other errors — `status: "error"`, `errorType: "rate_limited"`, a human-readable `error`, and `retryAfterMs` (the same wait as the header, in milliseconds) — never a bare text body, so clients that already parse the envelope for `bad_data`/`not_found` need no special case for throttling. The OpenAPI document SHALL declare the `429` response and its headers on every rate-limited operation, so generated clients expose them typed.

#### Scenario: Structured 429 on a query surface

- **WHEN** a Tempo, Loki, Prometheus, profiles, or Query IR request is throttled
- **THEN** the body is `{"status":"error","errorType":"rate_limited","error":"…","retryAfterMs":N}` with `Content-Type: application/json`

#### Scenario: Generated clients see the 429 contract

- **WHEN** the OpenAPI document is regenerated
- **THEN** every rate-limited operation declares a `429` response with the `Retry-After`, `X-RateLimit-Limit`, and `X-RateLimit-Burst` headers and the envelope schema, and both generated clients (Rust SDK, TypeScript) build without hand edits

### Requirement: Rejections are observable

The router SHALL make throttling visible to operators: each rejection SHALL emit one structured warning log carrying the tenant, the rejected surface, and `retry_after_ms`, and SHALL increment a `signaldb_rate_limit_rejections_total` counter labelled by surface and rejected dimension, so an operator can tell a client bug from an undersized budget without reproducing it.

#### Scenario: A rejection is counted and logged once

- **WHEN** one request is rejected by the query budget
- **THEN** exactly one warning is logged with `tenant_id`, `surface`, and `retry_after_ms`, and `signaldb_rate_limit_rejections_total{surface="query",kind="query_requests"}` increases by one

### Requirement: Generous defaults

The shipped defaults SHALL favour interactive clients: `burst_seconds` defaults to 10.0, and the example limits documented for operators SHALL be sized so a single Explore page load or an agent's multi-tool investigation does not trip a freshly configured deployment. Deployments that set explicit limits keep their values; only the burst default changes for them.

#### Scenario: A freshly configured deployment survives a page load

- **WHEN** an operator enables the documented example default limits and a user opens the Explore UI, which issues a fan-out of requests at once
- **THEN** none of the page's requests are throttled

#### Scenario: Explicit configuration is preserved

- **WHEN** an operator has set `max_query_requests_per_sec` and `burst_seconds` explicitly
- **THEN** upgrading SignalDB does not change those values
