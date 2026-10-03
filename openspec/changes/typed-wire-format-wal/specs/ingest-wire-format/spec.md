## Purpose

Defines the internal ingest carrier between the acceptor and the writer: the
Flight `do_put` batch and the payload of both write-ahead logs. It states which
encodings exist, the fidelity each guarantees for OTLP `AnyValue`s, how
acceptors and writers of different releases agree on an encoding, and how
entries written by an earlier release are replayed or drained.

## ADDED Requirements

### Requirement: The typed carrier preserves every AnyValue exactly

When the typed carrier (wire schema version `v3`) is in use, every
`AnyValue`-bearing field of an ingested record SHALL travel from the
acceptor's OTLP decode to the writer's storage placement without loss. That
covers resource, scope and record attributes, span event and link attributes,
log body, and exemplar filtered attributes. Each value's type and value SHALL
survive, including non-finite doubles, bytes and empty values. So SHALL key
order, duplicate keys, and nesting of arrays and key-value lists. A value the
acceptor accepted SHALL NOT be rejected or dead-lettered later because the
carrier could not represent it. An absent container and a present-but-empty
container SHALL remain distinguishable.

#### Scenario: Duplicate keys survive the wire and the WAL

- **WHEN** an OTLP span carries the attribute key `k` twice with values `"a"`
  and `"b"` and is ingested over the typed carrier
- **THEN** the batch received by the writer, and the entries replayed from
  either WAL, carry both occurrences in sent order

#### Scenario: Non-finite doubles are not turned into null

- **WHEN** a log record carries a double attribute whose value is NaN
- **THEN** the writer receives a double NaN, distinct from an empty value

#### Scenario: A map shaped like the bytes carrier stays a map

- **WHEN** an attribute is a key-value list with exactly the keys
  `$otlp_type` = `"bytes"` and `base64`
- **THEN** the writer receives a key-value list, not bytes

#### Scenario: Nesting within the depth bound round-trips

- **WHEN** an OTLP export carries a key-value list nested exactly
  `[acceptor].max_value_depth` levels deep
- **THEN** the batch is stored and the value reads back unchanged

#### Scenario: Over-deep values are rejected before acknowledgement

- **WHEN** an OTLP export carries a value nested deeper than
  `[acceptor].max_value_depth`
- **THEN** the acceptor rejects it with `InvalidArgument` before writing the
  WAL, and no WAL entry is ever dead-lettered because of a value's depth

#### Scenario: The writer bounds decode depth independently

- **WHEN** a Flight client sends a v3 batch directly to a writer with a
  `*_pb` cell nested deeper than the bound
- **THEN** the writer rejects the batch as `invalid_argument` without
  recursing into the value

### Requirement: Encoding is negotiated so no writer receives an encoding it cannot process

A writer that can process the typed carrier SHALL advertise this through
service discovery. An acceptor SHALL encode new ingest with the typed carrier
only when its configured policy allows it. Under the default policy that
means every live storage writer advertises the capability. A queued entry
encoded with the typed carrier SHALL be converted to the legacy carrier when
it is forwarded to a writer without the capability. That conversion SHALL be
observable as a metric, and it SHALL NOT drop or dead-letter the entry. An
operator SHALL be able to pin either carrier explicitly.

#### Scenario: A mixed fleet stays on the legacy carrier

- **WHEN** at least one live storage writer does not advertise the typed
  carrier and the acceptor runs the default policy
- **THEN** new ingest is encoded with the legacy carrier and every writer
  accepts it

#### Scenario: A fully upgraded fleet switches automatically

- **WHEN** every live storage writer advertises the typed carrier
- **THEN** the acceptor encodes new ingest with the typed carrier without a
  configuration change

#### Scenario: A queued typed entry meets a rolled-back writer

- **WHEN** an acceptor forwards a typed-carrier WAL entry to a writer that
  does not advertise the capability
- **THEN** the entry is converted to the legacy carrier, delivered, marked
  processed, and the conversion is counted

### Requirement: Entries written by an earlier release replay without migration

A writer SHALL accept both the legacy (JSON) carrier and the typed carrier for
at least one full release after the typed carrier ships. It SHALL determine
each batch's carrier from the batch itself (its declared schema version and
its column types), and SHALL reject a batch whose declared version and columns
disagree, with an error naming both. Acceptor and writer WAL entries written
by the previous release SHALL replay and be stored after an upgrade, with no
offline migration step. The WAL record framing SHALL be unchanged.

#### Scenario: Old acceptor WAL entries replay after an upgrade

- **WHEN** an acceptor restarts on the new release with unprocessed entries
  written by the previous release
- **THEN** those entries are forwarded and stored with the fidelity the
  legacy carrier provides, and none is dead-lettered for its format

#### Scenario: Old writer WAL entries commit after an upgrade

- **WHEN** a writer restarts on the new release with unprocessed WAL entries
  written by the previous release
- **THEN** it commits them through the legacy-carrier path

#### Scenario: A mislabelled batch is rejected, not mis-stored

- **WHEN** a batch declares the typed carrier but has legacy JSON columns, or
  the reverse
- **THEN** the writer rejects it at `do_put` with an error naming the
  declared version and the mismatched column

### Requirement: Downgrade requires draining, and the order is documented

The operations documentation SHALL state the deploy order (queriers, then
writers, then acceptors) and the rollback order, and SHALL require draining a
writer's WAL before downgrading it to a release without the typed carrier. A
typed-carrier entry that reaches a release unable to process it SHALL be
retained in the dead-letter store under its retention policy, not deleted.

#### Scenario: A drained writer downgrades cleanly

- **WHEN** an operator flushes a writer, waits for its pending WAL entries to
  reach zero, and downgrades it
- **THEN** no entry is lost or dead-lettered, and acceptors fall back to the
  legacy carrier for that writer

### Requirement: Retiring the legacy carrier is gated on empty legacy backlogs

A release that stops accepting the legacy carrier SHALL detect legacy entries
remaining in any WAL at startup. It SHALL report them, and SHALL leave them
unprocessed rather than discard them.

#### Scenario: Legacy entries block, not vanish

- **WHEN** a writer on the release without legacy support finds legacy-carrier
  entries in its WAL
- **THEN** it reports them as requiring the previous release to drain and
  keeps them, while continuing to process typed-carrier entries
