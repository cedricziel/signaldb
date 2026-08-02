## Purpose

Defines the durability and delivery guarantee the acceptor makes to ingest
clients: data is acknowledged only after it is durably written, and accepted
data is delivered to storage at least once even across transient writer
failures. Shared by all OTLP signals and the Prometheus `remote_write` path.

## ADDED Requirements

### Requirement: Write-ahead durability before acknowledgement

The acceptor SHALL write ingested data to the write-ahead log (WAL) and
flush it to durable storage before acknowledging the export to the client.
An export is acknowledged only once the data is durably persisted in the
WAL.

#### Scenario: Successful ingest is durable before ack

- **WHEN** an ingest request is accepted
- **THEN** the acceptor writes and flushes the data to the WAL before
  returning a success response to the client

#### Scenario: WAL write failure is signalled as retryable

- **WHEN** the acceptor cannot durably write the data to the WAL
- **THEN** it rejects the export as retryable (OTLP/gRPC `UNAVAILABLE`,
  OTLP/HTTP `503`) so the client retries rather than dropping its copy

### Requirement: Per-tenant, per-dataset, per-signal WAL organization

The acceptor SHALL organize WAL storage by tenant, dataset, and signal type
so that each tenant's and dataset's ingested data is isolated on disk.

#### Scenario: Isolation across tenants and datasets

- **WHEN** two different `(tenant, dataset, signal)` combinations ingest data
- **THEN** their WAL entries are stored under separate, isolated locations

### Requirement: At-least-once delivery to storage

After durably accepting data, the acceptor SHALL forward it to a
Storage-capable writer. A forwarding failure after WAL persistence MUST NOT
fail the export; the WAL entry remains unprocessed and is re-forwarded until
the writer accepts it, then marked processed.

#### Scenario: Forward succeeds and entry is marked processed

- **WHEN** the acceptor forwards a durably-accepted batch and the writer
  accepts it
- **THEN** the WAL entry is marked processed so its storage can be reclaimed

#### Scenario: Forward failure does not fail the export

- **WHEN** the writer is unavailable at ingest time but the data is already
  durable in the WAL
- **THEN** the acceptor still acknowledges the export and leaves the entry
  unprocessed for later delivery

#### Scenario: Unprocessed entries are re-forwarded

- **WHEN** WAL entries remain unprocessed past their minimum retry age
- **THEN** a background retry consumer re-forwards them to a writer and
  marks them processed on success
