## MODIFIED Requirements

### Requirement: New tenants are usable the moment they are created

A tenant or dataset added to a registered source SHALL become usable for both
ingest and query **without a process restart and without editing the
configuration file**. Once creation is acknowledged, the next authenticated
request for that tenant/dataset SHALL be served: writes are accepted and
queries resolve the catalog. Queryability SHALL NOT require a corresponding
`[[auth.tenants]]` block in `signaldb.toml`, and SHALL NOT be deferred to a
restart.

Queryability SHALL NOT be deferred to first ingest either. A registered
tenant/dataset that has never been written to SHALL answer metadata, label, and
search queries successfully with empty results, for every signal type the
deployment accepts. A query MUST NOT fail because a signal's storage table has
not yet been brought into existence.

#### Scenario: Admin-API tenant is immediately queryable

- **WHEN** a tenant is created through the admin API and telemetry is then
  ingested for it
- **THEN** a query for that tenant succeeds against the running service without
  any restart or configuration-file change

#### Scenario: Newly created dataset is immediately usable

- **WHEN** a new dataset is added to an existing tenant at runtime
- **THEN** the next ingest into it is accepted and the next query over it
  resolves its catalog, with no restart

#### Scenario: Catalog is resolved on demand for a not-yet-registered tenant

- **WHEN** an authenticated query targets a registry tenant whose DataFusion
  catalog has not yet been registered in the running querier
- **THEN** the querier resolves and registers that tenant's catalog and object
  store on demand and serves the query, rather than failing with a
  catalog-resolution error

#### Scenario: Dataset with no ingested data answers queries

- **WHEN** a client issues a label, metadata, or search query against a
  registered tenant/dataset for which no telemetry has ever been ingested
- **THEN** the query succeeds and returns an empty result set, rather than
  failing with an error naming a missing table

## ADDED Requirements

### Requirement: Absent signal storage reads as empty, never as an error

A read against a signal whose storage does not exist for a registered
tenant/dataset SHALL be answered as "no data" rather than as a failure. This
SHALL hold for every signal type and every read surface — label and metadata
lookups, searches, and instant/range queries over the Tempo, LogQL, and
Prometheus-compatible APIs — and SHALL hold both before a dataset's first write
and permanently for signal types the deployment has disabled.

Errors that a client can act on SHALL remain distinguishable from this case:
authentication failures, unknown tenants, and malformed queries SHALL continue
to return their own errors rather than an empty result. An error surfaced from
a signal's read path SHALL identify that signal, so a failure on one signal type
is not reported as a failure of another.

#### Scenario: Label query on a signal with no storage

- **WHEN** a label-names or label-values query targets a signal type that has no
  storage for the requested tenant/dataset
- **THEN** an empty label set is returned with a success status

#### Scenario: Disabled signal type stays empty, not broken

- **WHEN** a deployment has disabled a signal type and a client queries that
  signal for a registered tenant/dataset
- **THEN** the query returns an empty result rather than an error, on every
  subsequent query as well

#### Scenario: Actionable errors are still reported

- **WHEN** a query carries invalid credentials, names a tenant that is not in
  the registry, or is syntactically invalid
- **THEN** the corresponding error is returned, and it is not masked as an empty
  result

#### Scenario: Error identifies the signal it came from

- **WHEN** a read against one signal type fails
- **THEN** the reported error identifies that signal type, rather than
  attributing the failure to a different signal
