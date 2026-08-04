## Purpose

Defines how each logical field's one canonical type is chosen and owned by the
attribute registry, scoped per tenant+dataset. The stored value is always the
OTLP `AnyValue` as sent; semantic conventions and config are _hints/overrides
that select the canonical typed home_, never a license to rewrite a sender's
value.

## ADDED Requirements

### Requirement: Canonical type is one per tenant+dataset+field and monotonic

The registry SHALL own exactly one canonical type per (tenant, dataset, logical
field). The canonical type SHALL be stable and monotonic: once established it
SHALL NOT be changed by later ingested data, and existing stored values SHALL NOT
be retyped. It changes only by explicit operator action (config) or a declared
schema-version bump. No other component SHALL assert a competing canonical type.

#### Scenario: Canonical type is scoped per tenant+dataset

- **WHEN** two tenants send the same key with different value types
- **THEN** each tenant+dataset resolves its own canonical type, and one tenant's
  data cannot change another tenant's canonical type

#### Scenario: Later conflicting data does not flip the canonical type

- **WHEN** a key's canonical type is already established and a later record sends
  that key with a different `AnyValue` type
- **THEN** the canonical type is unchanged and already-stored values are not
  retyped

### Requirement: Canonical-home selection precedence

The registry SHALL select a field's canonical typed home using the precedence:
(1) an operator **config** override; else (2) a **semantic-convention** type hint
for the key, taken from a pinned semconv snapshot and selected by the applicable
resource-/scope-level `schema_url`; else (3) the **observed OTLP `AnyValue`** type
(first-seen for the scope). Selection chooses which typed home is canonical; it
SHALL NOT rewrite or coerce the sender's stored value.

#### Scenario: Config override selects the home

- **WHEN** an operator configures a type for a key
- **THEN** that type is the canonical home regardless of semconv hint or observed
  `AnyValue`

#### Scenario: Semconv hint selects the home when present

- **WHEN** a key is covered by the pinned semconv snapshot for the applicable
  resource-/scope-level `schema_url`, and there is no config override
- **THEN** the semconv-declared type is the canonical home

#### Scenario: Observed AnyValue selects the home by default

- **WHEN** a key has no config override and no applicable semconv hint
- **THEN** the first observed `AnyValue` type becomes the canonical home

### Requirement: schema_url is resource/scope only and hints, never retypes

The registry SHALL read `schema_url` only from resource and scope levels (OTLP
defines it nowhere else) and use it solely to pick the semconv snapshot for a
type _hint_. A missing or unrecognized `schema_url` SHALL fall through to the
observed-`AnyValue` home without error. This change SHALL NOT implement
cross-version semconv attribute renaming.

#### Scenario: Missing schema_url falls through without error

- **WHEN** a record carries no resource-/scope-level `schema_url` (the common case)
- **THEN** the canonical home is selected from the observed `AnyValue` type and
  ingestion is not rejected

#### Scenario: schema_url selects a type hint only

- **WHEN** a resource/scope `schema_url` maps to a semconv snapshot that declares a
  key's type
- **THEN** that type is used to select the canonical home, and no sender value is
  rewritten or renamed on account of the `schema_url`

### Requirement: Values not matching the canonical type are retained losslessly

When a record sends a key with an `AnyValue` type that does not match the field's
canonical type, the value SHALL NOT be dropped and SHALL NOT be written to a
second typed home. It SHALL be retained losslessly in the structured residue
(retrievable, not typed-queryable), and the type mismatch SHALL be exposed as
discoverable metadata.

#### Scenario: Off-type value goes to the residue, not a second home

- **WHEN** a field canonically typed integer receives a record sending that key as
  a string
- **THEN** the string is retained in the residue (retrievable), the integer home
  is unaffected, and the field is reported as having off-type occurrences

### Requirement: The registry is authoritative at read and write

The canonical type SHALL govern both write (which typed home a value is encoded
into, or the residue) and read (literal coercion and result typing). Read and
write SHALL use the same registry resolution for a given (tenant, dataset, field).

#### Scenario: Same resolution governs write and read

- **WHEN** a value is encoded at ingest and a query literal is later coerced for
  the same field
- **THEN** both use the registry's one canonical type for that (tenant, dataset,
  field)
