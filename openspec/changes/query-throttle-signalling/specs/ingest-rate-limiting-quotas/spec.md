## MODIFIED Requirements

### Requirement: Per-tenant ingest rate limiting

The acceptor SHALL enforce a configurable per-tenant ingest rate limit. When
a tenant exceeds its configured rate, the acceptor SHALL reject the request
with a retryable overload signal and ingest no data from that request. HTTP
rejections SHALL state when to retry: `Retry-After` (whole seconds, rounded
up, at least 1) computed from the tenant's actual budget state, together with
`X-RateLimit-Limit` and `X-RateLimit-Burst` for the rejected dimension, so
ingest clients apply the same retry rule as query clients.

#### Scenario: Request within budget is accepted

- **WHEN** a tenant sends ingest requests within its configured rate budget
- **THEN** the acceptor accepts them

#### Scenario: Request exceeding the rate is rejected as retryable

- **WHEN** a tenant exceeds its configured ingest rate
- **THEN** the acceptor rejects the request (OTLP/gRPC `RESOURCE_EXHAUSTED`,
  OTLP/HTTP and Prometheus `429 Too Many Requests`) with the reason, and the
  client may retry after backoff

#### Scenario: HTTP rejection carries Retry-After

- **WHEN** an OTLP/HTTP or Prometheus `remote_write` request is rejected by
  the ingest rate limit
- **THEN** the `429` response carries `Retry-After` with the seconds until the
  request would be admitted, plus `X-RateLimit-Limit` and `X-RateLimit-Burst`
  for the rejected dimension (requests/second or bytes/second)

#### Scenario: No limit configured means unlimited

- **WHEN** no ingest rate limit is configured for a tenant
- **THEN** the acceptor does not rate-limit that tenant's ingest
