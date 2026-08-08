## Purpose

Guarantees that every dataset SignalDB knows about holds the full set of storage
tables for the signal types it is configured to accept, so a dataset is complete
and queryable from the moment it exists rather than only after telemetry happens
to arrive for each signal.

## ADDED Requirements

### Requirement: Every registered dataset has its enabled signal tables

SignalDB SHALL ensure that each tenant/dataset in the tenant registry has a
storage table for every enabled signal type, without requiring that data has
been ingested for that signal. The set of tables SHALL be exactly the set the
deployment's schema configuration enables — traces, logs, each metrics
representation (gauge, sum, histogram, exponential histogram, summary), and
profiles — so that provisioning never creates a table for a signal type the
operator has disabled.

Provisioned tables SHALL be indistinguishable from tables the write path would
have created: same schema, partitioning, and table properties, in the same
namespace the ingest path writes to. Provisioning SHALL NOT alter the schema of
a table that already exists.

#### Scenario: New tenant has tables before any ingest

- **WHEN** a tenant with a default dataset is created through the admin API and
  no telemetry has ever been ingested for it
- **THEN** its dataset holds a table for every enabled signal type

#### Scenario: Disabled signal type is not provisioned

- **WHEN** a deployment disables a signal type in its schema configuration and a
  dataset is provisioned
- **THEN** no table is created for that signal type, and the remaining enabled
  signal types are still provisioned

#### Scenario: Provisioned table matches the ingest path's table

- **WHEN** telemetry is ingested into a dataset whose tables were provisioned
  ahead of any write
- **THEN** the data is written into those same tables and is queryable, with no
  second table created and no schema conflict

### Requirement: Provisioning converges continuously and is idempotent

Table provisioning SHALL be a convergence process over the tenant registry, not
a one-shot action taken at creation time. It SHALL run when a service starts and
SHALL re-run periodically, so that datasets created while a service was down,
datasets that predate this capability, and datasets added to an existing tenant
at runtime all converge without operator action and without a process restart.

Repeated provisioning SHALL be idempotent: once a dataset's tables exist,
subsequent passes SHALL leave table contents, schemas, and version history
unchanged. The interval between passes SHALL be operator-configurable, including
an option to disable periodic provisioning.

#### Scenario: Dataset added at runtime converges

- **WHEN** a dataset is added to an existing tenant while services are running
- **THEN** its enabled signal tables exist without restarting any service or
  editing the configuration file

#### Scenario: Pre-existing dataset is backfilled

- **WHEN** a deployment that already holds datasets created before this
  capability starts up
- **THEN** those datasets gain any missing enabled signal tables

#### Scenario: Repeat passes change nothing

- **WHEN** provisioning runs repeatedly against a dataset whose tables already
  exist
- **THEN** no new table version, snapshot, or data file is produced by those
  passes

#### Scenario: Operator disables periodic provisioning

- **WHEN** an operator disables periodic provisioning in configuration
- **THEN** no periodic passes run, and the deployment continues to serve ingest
  and queries

### Requirement: Provisioning failures degrade gracefully

A failure to provision a table SHALL NOT prevent a service from starting,
prevent ingest, or fail an unrelated query. Failures SHALL be reported through
SignalDB's logs with the tenant, dataset, and table they concern, and SHALL be
retried on a later pass. Because the ingest path independently creates any table
it needs, a persistent provisioning failure SHALL degrade to the prior
create-on-first-write behavior rather than to data loss.

#### Scenario: Catalog is unreachable at startup

- **WHEN** provisioning cannot reach the catalog during a startup pass
- **THEN** the service still starts and serves requests, the failure is logged
  with the affected tenant and dataset, and provisioning is retried on the next
  pass

#### Scenario: One dataset's failure does not block others

- **WHEN** provisioning fails for one dataset during a pass
- **THEN** the remaining tenants and datasets in that pass are still provisioned
