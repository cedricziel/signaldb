## MODIFIED Requirements

### Requirement: Ingest never drops records on type mismatch

The ingest path SHALL NOT reject or silently discard a record because an
attribute's sent type does not match the canonical type. The value SHALL be
retained in the residue and the condition SHALL be observable. With the typed
carrier (see `ingest-wire-format`), "losslessly" SHALL mean the original
`AnyValue` exactly as decoded at the acceptor: its type and value (including
non-finite doubles, bytes and empty values) and every occurrence of a
duplicated key, in sent order. A duplicated key's canonical typed value SHALL
be its last occurrence. While a batch travels on the legacy JSON carrier
(mixed-version operation or legacy replay), duplicate keys and non-finite
doubles are not preserved, and this SHALL be documented.

#### Scenario: Off-type value is retained, not dropped

- **WHEN** an attribute value does not match the field's canonical type
- **THEN** the record is still ingested, its decoded `AnyValue` is retained in the
  residue, and the mismatch is surfaced (metric/log) rather than dropped

#### Scenario: Duplicate key keeps every occurrence

- **WHEN** a record arrives over the typed carrier with key `k` sent twice,
  first as integer `1` and then as integer `2`, and `k` is canonically an
  integer
- **THEN** the typed read of `k` returns `2`, and the raw duplicate-preserving
  read returns both occurrences in sent order

### Requirement: Ingest wire format compatibility during migration

Moving the internal ingest carrier from JSON text to the typed carrier SHALL
NOT require any change from OTLP or Prometheus remote-write clients. During
the deprecation window, writers SHALL keep accepting the JSON carrier, so that
data queued by an earlier release is stored. The internal Flight wire and WAL
payload changes SHALL be marked breaking for operators and SHALL follow the
negotiation, replay and drain rules of `ingest-wire-format`.

#### Scenario: Existing OTLP clients keep working

- **WHEN** an OTLP client sends attributes exactly as it does today
- **THEN** ingestion succeeds and the values are stored typed, with no client-side
  change required

#### Scenario: Legacy carrier still accepted during the window

- **WHEN** a writer within the deprecation window receives a JSON-carrier batch
- **THEN** it stores the batch with JSON-carrier fidelity rather than
  rejecting it
