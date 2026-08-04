## Purpose

Defines the tiered physical substrate that preserves OTLP `AnyValue` without loss
and lets the registry resolve a logical field to a typed value by retrieval, plus
the invariant that promoting a field to a column affects only performance. Three
tiers: a cold typed store, a warm derived index that actually prunes, and a hot
promoted-column tier.

## ADDED Requirements

### Requirement: One canonical typed home per field; off-type values in a lossless binary residue

Each logical field SHALL have exactly one canonical typed home (a per-type map or,
when promoted, a column) determined by the registry's canonical type. A value
whose sent type matches the canonical type SHALL be stored typed in that home; a
value whose type does not match, and any array/kvlist/bytes value, SHALL be
preserved in a structured **binary** residue (self-describing, e.g. CBOR/msgpack —
not JSON text), retrievable without loss. A single logical field SHALL NOT be
scattered across multiple typed homes.

#### Scenario: Canonical-typed value is stored typed

- **WHEN** a value matching a field's canonical integer type is persisted
- **THEN** it is stored under the integer home and retrievable as an integer, not
  as the string form of the number

#### Scenario: Off-type and structured values round-trip via the binary residue

- **WHEN** an off-type scalar, or an array/kvlist/bytes value, is persisted
- **THEN** it is retained in the binary residue and retrievable without loss after
  a storage round-trip

### Requirement: AnyValue fidelity requires fixing lossy conversion at the OTLP boundary

Lossless preservation SHALL hold from the OTLP boundary, not merely from the
storage write. The OTLP→internal conversion SHALL preserve `BytesValue` as bytes
(distinct from a string), preserve string-table-indexed values, and preserve
duplicate keys and key order to the extent the residue represents them — rather
than mapping bytes to a possibly-invalid string or dropping interned values to
null.

#### Scenario: Bytes are not degraded to a string

- **WHEN** a `BytesValue` attribute is ingested
- **THEN** it is retrievable as bytes, distinguishable from a string attribute, and
  not corrupted by a UTF-8 conversion

### Requirement: Warm tier — a derived typed containment index prunes before promotion

Because Parquet maintains no per-key statistics or bloom filters inside a map, the
substrate SHALL provide a derived, per-type containment index (a typed
generalization of the existing `attr_tokens` approach) that enables row-group/file
pruning for `key = value` predicates on unpromoted fields. The typed maps
themselves SHALL NOT be claimed to prune.

#### Scenario: Unpromoted equality predicate prunes via the derived index

- **WHEN** a `key = value` predicate targets an unpromoted attribute
- **THEN** the derived containment index is used to skip row groups/files that
  cannot contain the pair, rather than scanning every map value

#### Scenario: Typed-map read without the index is an unpruned scan

- **WHEN** a predicate cannot use the derived index (e.g. a range predicate on an
  unpromoted field)
- **THEN** the result is still correct, read as an unpruned scan of the typed map
  — this is a cheaper (cast-free) scan, not a pruned one

### Requirement: Hot tier — promotion is only performance, bounded and demotable

Promotion of a field to a typed column SHALL affect only performance: for any
query, the result set AND the result types SHALL be identical whether the field is
promoted or served from the cold typed home. Because there is one canonical home,
this holds without cross-home coalescing. Promotion SHALL use Iceberg
schema-evolution field-id assignment, SHALL be bounded by an explicit
promoted-column budget per table, and SHALL support demotion (a cold field folds
back into the typed map on next compaction) so live-schema width does not grow
unbounded as the hot-key set drifts.

#### Scenario: Same result and type before and after promotion

- **WHEN** the same query runs against a canonical field served from the typed home
  and later after that field is promoted to a column
- **THEN** both return the same result set with the same field types, differing
  only in performance

#### Scenario: Promotion budget bounds live-schema width

- **WHEN** the demand-hot key set drifts over time beyond the promoted-column budget
- **THEN** cold promoted columns are demoted (folded back into the typed map on
  compaction) so the number of live promoted columns stays within budget

### Requirement: Physical attribute growth is bounded by query patterns, not cardinality

Attribute key cardinality SHALL NOT widen the physical schema: distinct keys live
as map entries and (optionally) index tokens, never as columns, except for the
bounded, demand-selected, budgeted promoted set.

#### Scenario: High attribute cardinality does not widen the schema

- **WHEN** ingested data contains a very large number of distinct attribute keys
- **THEN** physical column count does not grow proportionally to attribute
  cardinality; only budgeted demand-selected keys are promoted

### Requirement: Legacy stringified attributes read via explicit typed coercion, scoped to migration

Where attribute data was persisted under the prior `Map<String,String>` layout,
the read path SHALL present it through the same logical fields, using an explicit
typed coercion (safe cast: a value that does not parse to the canonical type reads
as null, never a hard query error). The no-read-time-cast guarantee applies to
typed-substrate files only; consistency across the legacy/typed boundary is
result-level and holds fully only once the compactor has rewritten legacy files.

#### Scenario: Legacy value that cannot be coerced reads as null, not an error

- **WHEN** a legacy stringified value cannot be safely coerced to the canonical
  type (e.g. `"200 OK"` to integer)
- **THEN** it reads as null for the typed predicate and the query does not error,
  and the discrepancy resolves once compaction rewrites the file to the typed home
  or residue
