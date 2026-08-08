## Purpose

Guarantees that every dataset SignalDB knows about holds the full set of storage
tables for the signal types its tenant is configured to accept, so a dataset is
complete and queryable from the moment it exists rather than only after telemetry
happens to arrive for each signal.

## Requirements

### Requirement: Every registered dataset has its enabled signal tables

SignalDB SHALL ensure that each tenant/dataset in the tenant registry has a
storage table for every signal type enabled **for that tenant**. Where a tenant
carries its own schema configuration, that configuration SHALL determine the set;
otherwise the deployment default applies. The set is drawn from traces, logs,
each metrics representation (gauge, sum, histogram, exponential histogram,
summary), and profiles, so provisioning never creates a table for a signal type
the operator has disabled globally or for that tenant. Custom, operator-defined
tables are outside this capability and SHALL NOT be provisioned.

Provisioned tables SHALL be indistinguishable from tables the write path would
have created: same schema, partitioning, and table properties, in the same
namespace the ingest path writes to. Provisioning SHALL NOT alter the schema of
a table that already exists.

#### Scenario: New tenant has tables before any ingest

- **WHEN** a tenant with a default dataset is created through the admin API and
  no telemetry has ever been ingested for it
- **THEN** its dataset holds a table for every signal type enabled for that
  tenant

#### Scenario: Tenant override narrows the set

- **WHEN** a tenant's own schema configuration disables a signal type that the
  deployment default enables
- **THEN** no table for that signal type is created for that tenant's datasets,
  while other tenants still receive it

#### Scenario: Globally disabled signal type is not provisioned

- **WHEN** a deployment disables a signal type and a dataset is provisioned
- **THEN** no table is created for that signal type, and the remaining enabled
  signal types are still provisioned

#### Scenario: Custom tables are left alone

- **WHEN** a deployment declares custom table schemas in its configuration
- **THEN** provisioning creates none of them and reports no failure for them

#### Scenario: Provisioned table matches the ingest path's table

- **WHEN** telemetry is ingested into a dataset whose tables were provisioned
  ahead of any write
- **THEN** the data is written into those same tables and is queryable, with no
  second table created and no schema conflict

### Requirement: A tenant's default dataset is provisioned even before it is separately recorded

A tenant SHALL have its default dataset provisioned on the strength of the
tenant record alone. Where a tenant names a default dataset that has no separate
dataset record, provisioning SHALL still create that dataset's tables, so a
tenant created in one step is complete without a follow-up dataset creation call.

#### Scenario: Tenant created with only a default dataset name

- **WHEN** a tenant is created through the admin API naming a default dataset,
  and no separate dataset creation call is made
- **THEN** that default dataset's enabled signal tables are provisioned

#### Scenario: Separately recorded datasets are still provisioned

- **WHEN** a tenant has both a default dataset and additional datasets recorded
  for it
- **THEN** every one of them is provisioned, with no dataset provisioned twice

### Requirement: Provisioning converges continuously and is idempotent

Table provisioning SHALL be a convergence process over the tenant registry, not
a one-shot action taken at creation time. It SHALL run when a service starts and
SHALL re-run periodically, so that datasets created while a service was down,
datasets that predate this capability, and datasets added to an existing tenant
at runtime all converge without operator action and without a process restart.
Provisioning SHALL consider tenants from every registered tenant source, not only
those defined in the configuration file.

Repeated provisioning SHALL be idempotent: once a dataset's tables exist and
carry current table properties, subsequent passes SHALL leave table contents,
schemas, and version history unchanged. The interval between passes SHALL be
operator-configurable, including an option to disable periodic provisioning.
Operators SHALL also be able to trigger provisioning for a tenant on demand
rather than waiting for the next pass.

#### Scenario: Runtime-created tenant is provisioned

- **WHEN** a tenant that exists only in the runtime tenant store — with no
  configuration-file entry — is enumerated by a provisioning pass
- **THEN** its datasets are provisioned exactly as a configuration-defined
  tenant's would be

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
  exist and already carry current table properties
- **THEN** no new table version, snapshot, or data file is produced by those
  passes

#### Scenario: Operator triggers provisioning on demand

- **WHEN** an operator asks SignalDB to create a tenant's default tables through
  its administrative interface
- **THEN** that tenant's enabled signal tables are provisioned before the call
  reports success, and reporting success without having created them is not
  permitted

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

#### Scenario: One table's failure does not block its siblings

- **WHEN** provisioning fails for one table within a dataset
- **THEN** the dataset's remaining enabled tables are still provisioned, and the
  failure is reported for that table specifically
