## Purpose

Lets a signal table's physical schema catch up to its current definition
after that definition changes, so a schema fix reaches every tenant/dataset
table that already exists, not only tables created after the fix ships.

## ADDED Requirements

### Requirement: Every signal table tracks the schema version it was last reconciled to

Every signal table whose physical schema is sourced from a versioned
schema definition (traces and logs today; a signal not yet migrated onto a
versioned definition is out of this capability's scope until it is) SHALL
record, in its own table metadata, the schema version it currently
conforms to. This record SHALL travel with the table itself (not a
separate store external to it), so the recorded version and the table's
actual columns can never diverge from each other independently of the
table's own commit history.

#### Scenario: Newly created table starts at the current version

- **WHEN** a signal table is created for the first time
- **THEN** its recorded schema version is the current version for that
  signal type

#### Scenario: Table predating this capability has no recorded version

- **WHEN** an existing table has no recorded schema version because it was
  created before this capability existed
- **THEN** it is treated as being at the earliest known version for its
  signal type, not as an error

### Requirement: An existing table's schema evolves toward the current definition

When a signal's schema definition advances past the version a table is
recorded at, the system SHALL bring that table forward one version at a
time, in order, applying each intervening version's column additions and
removals until the table reaches the current version or a step fails. A
column addition SHALL be nullable so historical rows require no rewrite. A
column removal SHALL stop the column being read or written going forward
without deleting it from data already committed under an earlier schema.
No step SHALL modify, reinterpret, or delete existing row data.

Two concurrent attempts to evolve the same table to the same or an
overlapping set of versions SHALL result in exactly one of them succeeding
per version step; the other SHALL detect the conflict and re-read the
table's new state rather than corrupt or duplicate the change.

#### Scenario: Table two versions behind catches up incrementally

- **WHEN** a table has a recorded version and the current version is two
  hops ahead of it
- **THEN** the table is brought forward one hop at a time, each as a
  distinct step, rather than jumping directly to the current version

#### Scenario: A table with no recorded version never loses an existing field

- **WHEN** a table has no recorded schema version and its live columns
  already include one not declared by the earliest known version for its
  signal (for example, because it was created before this capability
  existed with a fuller shape than the oldest tracked version)
- **THEN** bringing that table to the current version adds any columns it
  is missing but does not remove the undeclared column

#### Scenario: Historical rows are unaffected by a new column

- **WHEN** a version adds a new column to a table that already holds rows
  written under an earlier version
- **THEN** those existing rows report an absent value for the new column
  and are not rewritten

#### Scenario: Concurrent evolution attempts do not corrupt the table

- **WHEN** two writer processes both attempt to evolve the same table to
  the same version at the same time
- **THEN** the table ends up at that version exactly once, with no
  duplicated or lost column, and the losing attempt observes the change
  and does not retry the same step

#### Scenario: A version step fails partway through a multi-step catch-up

- **WHEN** a table is more than one version behind and a step in the
  middle of the catch-up fails
- **THEN** the table remains at the last version it successfully reached,
  is not left in a partially-applied state, and a later attempt resumes
  from that version

### Requirement: Schema evolution never rewrites or discards existing table data

Bringing a table's schema forward SHALL be additive-only from the
perspective of already-written data: it SHALL NOT rewrite existing data
files, backfill values into a newly added column for historical rows, or
delete data underlying a removed column. Recovering or populating
historical values for a changed column is explicitly out of scope for this
capability.

#### Scenario: Removed column's historical data is left in place

- **WHEN** a column is removed by a schema version step from a table that
  has historical data files containing that column
- **THEN** those data files are left untouched and the column is simply no
  longer part of the table's active schema
