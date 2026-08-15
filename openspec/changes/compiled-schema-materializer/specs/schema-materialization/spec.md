## Purpose

Turns a declared schema and a source of typed values into a physical
Arrow batch through a plan resolved once per schema version, so adding an
ordinary field is a schema change plus a small named extraction rule, not
new per-column materialization code, and so ingest throughput does not pay
a per-row or per-batch cost for schema resolution.

## ADDED Requirements

### Requirement: Column resolution happens once per schema version, not per row or batch

The mapping from a declared field to its physical column position and the
extraction rule that produces its value SHALL be resolved once per schema
version and reused for every subsequent batch materialized against that
version. Materializing a batch SHALL NOT perform a name-based lookup per
field per batch.

#### Scenario: Repeated materialization reuses a resolved plan

- **WHEN** many batches are materialized against the same schema version
- **THEN** the column-to-extractor resolution work happens once, not once
  per batch

#### Scenario: A new schema version gets its own resolution

- **WHEN** a schema version changes (a field is added or removed)
- **THEN** materialization against the new version resolves its own plan
  independently, without corrupting or reusing the prior version's plan

### Requirement: An ordinary new field requires no new per-column code

Adding a field whose value is read verbatim from its source or via an
already-registered named extraction rule SHALL require only a schema
declaration and, where no matching rule exists yet, one new small,
independently testable extraction rule — never a change to the
batch-construction control flow itself.

#### Scenario: A verbatim field needs no new materialization code

- **WHEN** a new field is declared whose value comes directly from an
  existing, already-modeled source value with a matching extraction rule
- **THEN** the batch-construction code that assembles physical columns is
  unchanged; only the schema declaration changes

### Requirement: Materialized output is behaviorally identical across the transition to plan-based construction

Replacing hand-written, per-field materialization code with plan
execution SHALL NOT change the physical output for any existing field:
same values, same types, same nullability, for the same input.

#### Scenario: Plan-based materialization matches prior hand-written output

- **WHEN** the same input is materialized by the plan-based path and by
  the hand-written code it replaces
- **THEN** the two produce identical physical batches
