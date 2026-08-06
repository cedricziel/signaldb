# tenant-catalog-registry Specification

## Purpose

Defines a single, source-agnostic registry of the active tenants and datasets
SignalDB serves, so that any tenant — however it was created — is queryable and
lifecycle-managed identically. Config is only the bootstrap seed; the registry
is the union of all tenant sources and is the sole authority every query- and
lifecycle-side subsystem consults.

## Requirements

### Requirement: Source-agnostic tenant registry

SignalDB SHALL expose a single registry of active tenants and datasets that is
the union of all registered tenant sources. Config-file tenants are a
**bootstrap** source seeded at startup; database-created tenants (via the admin
API) are an equally first-class source. The registry SHALL record each tenant's
origin, and its membership MUST NOT depend on which source a tenant came from.
The design SHALL allow additional sources to be added later without changing
the subsystems that consume the registry.

#### Scenario: Config and database tenants both appear

- **WHEN** the registry is enumerated on a deployment that has both
  config-defined tenants and admin-API-created tenants
- **THEN** every tenant from both sources is present, each tagged with its
  origin, with no tenant omitted because of its source

#### Scenario: A database-created tenant is a full member

- **WHEN** a tenant is created solely through the admin API (no
  `[[auth.tenants]]` block in configuration)
- **THEN** it is present in the registry with the same descriptor shape
  (identifier, slug, datasets, default dataset, effective storage) as a
  config-defined tenant

### Requirement: Uniform queryability regardless of source

Every tenant present in the registry SHALL be queryable through SignalDB's
query APIs (Tempo, LogQL, Prometheus) against its datasets. A tenant that can
successfully ingest data MUST NOT be un-queryable solely because of how it was
created; there SHALL be no "ingest-only" tenant state caused by source of
origin.

#### Scenario: Admin-API tenant is queryable after data lands

- **WHEN** a client ingests telemetry for a database-created tenant and the
  data has been persisted
- **THEN** a query for that tenant over the Tempo/LogQL/Prometheus APIs
  resolves its catalog and returns results, rather than failing with a
  catalog-resolution error

#### Scenario: No ingest-only-but-unqueryable state

- **WHEN** any tenant's ingest requests are accepted by the acceptor
- **THEN** that tenant's datasets are resolvable by the querier, so accepted
  data is never silently unreadable

### Requirement: Uniform lifecycle coverage regardless of source

Compaction, retention enforcement, and orphan cleanup SHALL operate over every
tenant and dataset in the registry. A database-created tenant SHALL receive the
same lifecycle management as a config-defined tenant, subject to the same
policy resolution (global → tenant → dataset overrides).

#### Scenario: Retention applies to a database tenant

- **WHEN** retention enforcement runs and a database-created tenant holds data
  older than the effective retention for its signal type
- **THEN** that tenant's data is subject to retention exactly as a
  config-defined tenant's data would be

#### Scenario: Compaction plans a database tenant's datasets

- **WHEN** the compactor builds a plan
- **THEN** datasets belonging to database-created tenants are considered as
  compaction candidates alongside config-defined tenants' datasets

### Requirement: Cross-source descriptor resolution

For each tenant and dataset, the registry SHALL provide everything a consumer
needs to register a catalog and locate storage uniformly across sources: a
tenant slug, a dataset slug, the tenant's default dataset, and an effective
storage location. For records that carry no explicit override (typically
database-sourced), the registry SHALL derive a deterministic slug and apply the
tenant/global storage, schema, and limit fallbacks; explicit config-sourced
values SHALL be preserved unchanged. Resolving these descriptors MUST NOT change
any dataset's on-disk (Iceberg/object-store) namespace.

#### Scenario: Database dataset inherits default storage

- **WHEN** the registry resolves a database-created dataset that specifies no
  storage override
- **THEN** its effective storage location is the tenant or global default, and
  its namespace path matches where its ingested data was written

#### Scenario: Config overrides are preserved

- **WHEN** the registry resolves a config-defined tenant that specifies an
  explicit slug or per-dataset storage override
- **THEN** those explicit values are used unchanged

### Requirement: New tenants are usable the moment they are created

A tenant or dataset added to a registered source SHALL become usable for both
ingest and query **without a process restart and without editing the
configuration file**. Once creation is acknowledged, the next authenticated
request for that tenant/dataset SHALL be served: writes are accepted and
queries resolve the catalog. Queryability SHALL NOT require a corresponding
`[[auth.tenants]]` block in `signaldb.toml`, and SHALL NOT be deferred to a
restart.

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
