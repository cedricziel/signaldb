## MODIFIED Requirements

### Requirement: Registry-mediated field resolution independent of promotion

Every logical field SHALL be resolved through the attribute registry at plan time
to its one canonical physical home — a promoted typed column, the cold typed store,
or (for off-type/array/kvlist/bytes values) the structured residue — and typed
values SHALL be returned under the field's registry-owned canonical type by
retrieval rather than by reconstructing the type from a stringified value. The
canonical type SHALL be the same one enforced at ingest (write) as at query (read).
The result of a query — set and types — SHALL NOT depend on whether a field is
currently promoted; promotion state SHALL affect only performance. This holds
because a field has exactly one canonical home, so resolution never coalesces
across competing typed homes.

Resolution SHALL distinguish two performance properties that are NOT implied by
typing alone: (a) **cast-free retrieval** — always available from the typed store
or a promoted column; and (b) **pruning/pushdown** — available only from a promoted
column (row-group stats + bloom) or the derived typed containment index, never from
the typed map itself, since Parquet keeps no per-key statistics inside a map.

#### Scenario: Same result and type before and after promotion

- **WHEN** the same IR query is executed against a field served from the cold typed
  store, and later against the same field after it has been promoted to a typed
  physical column
- **THEN** both executions return the same result set with the same field types,
  differing only in performance

#### Scenario: Typed retrieval does not reconstruct from a string

- **WHEN** an IR query reads or filters a canonical-typed field served from the
  typed store
- **THEN** the value is returned under its stored canonical type without casting a
  stringified value, even though an unpromoted range predicate over it is an
  unpruned scan

#### Scenario: Pruning comes from promotion or the derived index, not the map

- **WHEN** an unpromoted equality predicate `key = value` is planned
- **THEN** pruning is obtained from the derived containment index (or from a
  promoted column when present), and the plan does not claim row-group pruning from
  the typed map's value leaf

#### Scenario: One scan resolves mixed physical layouts to one typed column

- **WHEN** a single table scan spans files in the typed-store layout and files in
  a promoted-column layout for the same field (generations from before/after a
  promotion or demotion)
- **THEN** the registry resolves each file's physical representation to the one
  logical field and returns a single column under the canonical type — files
  predating the promoted column read from the typed home, never a query error
