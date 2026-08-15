## MODIFIED Requirements

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
namespace the ingest path writes to. Provisioning a table that already exists
SHALL NOT alter its partitioning, table properties, or data, but SHALL bring
its schema forward when the table's tracked schema version is behind the
current definition, per the table-schema-evolution capability. Provisioning
SHALL NOT perform any schema change beyond that additive/removal catch-up —
in particular it SHALL NOT rewrite, backfill, or delete existing data.

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

#### Scenario: Existing table's schema is brought forward, not left behind

- **WHEN** provisioning runs against a dataset whose traces table already
  exists and is recorded at an older schema version than the current
  definition
- **THEN** the table's schema is evolved to the current version and its
  existing data and partitioning are unchanged
