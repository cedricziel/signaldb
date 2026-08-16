# ingest-rate-limiting-quotas Specification

## Purpose
Defines the per-tenant ingest rate limits and storage quotas the acceptor
enforces to protect the system from overload and to bound per-tenant storage
consumption. Shared by all OTLP signals and the Prometheus `remote_write`
path. Both controls are unlimited unless configured.
## Requirements
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

### Requirement: Per-tenant storage quota enforcement

The acceptor SHALL enforce a configurable per-tenant storage quota. A tenant
whose measured storage usage is at or above its quota SHALL be prevented
from ingesting more data until usage drops or the quota is raised.

#### Scenario: Ingest under quota is accepted

- **WHEN** a tenant's storage usage is below its configured quota
- **THEN** the acceptor accepts the ingest

#### Scenario: Ingest at or over quota is rejected

- **WHEN** a tenant's storage usage is at or above its configured quota
- **THEN** the acceptor rejects the request (OTLP/gRPC `RESOURCE_EXHAUSTED`
  with a quota-exceeded reason, OTLP/HTTP and Prometheus `429`) and ingests
  no data

#### Scenario: No quota configured means unlimited

- **WHEN** no storage quota is configured for a tenant
- **THEN** the acceptor does not apply a storage limit to that tenant

