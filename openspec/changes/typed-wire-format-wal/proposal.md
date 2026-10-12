# Proposal

## Why

The typed storage layout (`otel-native-schema`) can hold every OTLP `AnyValue`
exactly: one canonical typed home per key, plus a CBOR residue for the rest.
But values reach it through a lossy carrier. Between the OTLP decoder in the
acceptor and the type split at the writer's commit, every attribute container,
log body, span event/link attribute set, and exemplar attribute set travels as
**JSON text in a `Utf8` column**. That covers the acceptor → writer Flight
`do_put`, the acceptor WAL, and the writer WAL. JSON-in-Utf8 is lossy in ways
the storage tier can neither detect nor undo:

- **Duplicate keys collapse.** `serde_json::Map` (with `preserve_order`) keeps
  the first key's position and the last value. OTLP receivers can and do get
  duplicate keys from non-conforming SDKs and from collector pipelines that
  merge attribute sets.
- **Non-finite doubles become `null`.** `Number::from_f64(NaN | ±Inf)` is
  `None`, so a NaN attribute reads back as an empty value. (Metric
  `data_json` already works around this with string sentinels; attributes
  do not.)
- **Bytes and maps collide.** Bytes travel as the carrier object
  `{"$otlp_type":"bytes","base64":…}`, so a genuine key-value list with
  exactly those keys is decoded as bytes.
- **Nesting depth turns into data loss.** `serde_json` refuses documents
  nested deeper than 128 levels. A kvlist that is legal OTLP and was accepted
  and acknowledged by the acceptor fails at the writer's parse. The batch is
  rejected as `invalid_argument` and **the whole batch is dead-lettered**.
- **Every value is re-parsed** from text at the writer, once per container per
  row.

The `typed-attribute-storage` and `ingest-type-enforcement` specs scoped these
losses out of phase 1 explicitly and deferred them to "the typed-wire phase".
This change is that phase.

## What Changes

- **BREAKING (Flight wire, acceptor → writer `do_put`):** a new wire schema
  version **`v3`**. It has the v1 field set, but every `AnyValue`-bearing
  column (`attributes_json`, `resource_json`, `scope_attributes`/`scope_json`,
  span event/link `attributes_json`, log `body`, and exemplar
  `filtered_attributes`) is replaced by a `Binary` column holding the **OTLP
  protobuf encoding** of the original `KeyValueList` / `AnyValue`. The bytes
  are exactly what the client sent, re-encoded by `prost`. They are not a
  SignalDB-defined format. Column names change too (`*_json` → `*_pb`), so a
  v1 batch and a v3 batch cannot be mistaken for each other.
- **BREAKING (WAL payload, acceptor and writer):** WAL entries written by this
  release carry v3 batches (acceptor) or v3-carrier physical batches
  (writer). The WAL record framing (`SDBW`/`SDBR`, format v1) is unchanged.
  The Arrow IPC schema inside the payload changes, and the entry metadata
  records `"schema_version": "v3"`. A previous release cannot replay these
  entries. Rollback requires draining both WALs first (design D6).
- **BREAKING (residue document, storage):** the CBOR residue gains a second
  document form, used **only** for a container that has duplicate keys: a
  tagged array of `[key, value]` pairs in sent order. Containers without
  duplicates keep today's CBOR map byte-for-byte. Old queriers cannot read
  the new form, so queriers must be upgraded before writers.
- **Negotiated rollout:** writers advertise a new `TypedWire` service
  capability. Acceptors choose v3 only when every live Storage writer
  advertises it (`[acceptor].wire_format = "auto" | "json" | "typed"`,
  default `auto`). An acceptor that has to forward a v3 WAL entry to a writer
  without the capability down-converts it to v1 (lossy, same as today) and
  counts the event. Writers accept v1 and v3 for the whole deprecation window,
  so acceptor WALs written by an older release replay unchanged.
- **Fidelity at the storage boundary:** the writer places values directly
  from decoded `AnyValue`s through the type authority. NaN/±Inf go to the
  double home. Duplicate keys put their **last** occurrence in the canonical
  home (OTel's last-wins), and the residue keeps every occurrence in order.
  Nesting depth is bounded explicitly (`[acceptor].max_value_depth`,
  default 64): an over-deep value is rejected at the acceptor before
  acknowledgement, and the writer re-checks the bound on the wire bytes
  before decoding. `prost` runs with `no-recursion-limit` in this workspace,
  so no implicit limit exists today; a value is never rejected after it was
  acknowledged.
- **IR result change (additive):** a new retrieval-only field per container,
  `{scope}.attribute_list`, returns `[{"key", "value"}]` with duplicates in
  sent order. The JSON result encoding gains carrier objects for values JSON
  cannot express: `{"$otlp_type":"double","value":"NaN"|"+Inf"|"-Inf"}`, and
  `{"$otlp_type":"kvlist","entries":[[k,v],…]}` for a nested kvlist that has
  duplicate keys or a `$otlp_type` key. The existing `{scope}.attributes` bag
  keeps its shape: one entry per key, last-wins.
- **Later, separate release (BREAKING):** writers stop accepting wire v1. This
  is gated on an operator check that no acceptor WAL still holds v1 entries.

OTLP ingest (gRPC/HTTP), the Tempo/Loki/Prometheus/Pyroscope surfaces, and the
Iceberg table schema (`physical-vN`) do **not** change.

## Capabilities

### New Capabilities

- `ingest-wire-format`: the contract for the internal ingest carrier (Flight
  `do_put` and both WALs). Covers which encodings exist, their fidelity
  guarantee, version negotiation between acceptors and writers, replay of
  entries written by earlier releases, mixed-version operation during a
  rolling deploy, and the drain-before-downgrade rule.

### Modified Capabilities

- `ingest-type-enforcement`: "Ingest never drops records on type mismatch"
  loses its phase-1 carve-out (duplicate keys and order are now preserved).
  "Ingest wire format compatibility during migration" becomes the v1→v3
  migration contract: the JSON carrier is still accepted for a deprecation
  window, and OTLP clients are unaffected.
- `typed-attribute-storage`: "AnyValue fidelity requires fixing lossy
  conversion at the OTLP boundary" becomes full fidelity (duplicates,
  non-finite doubles, no bytes/kvlist collision, no depth-induced loss).
  "Residue values are read through an explicit raw accessor" gains the
  duplicate-preserving `attribute_list` accessor and the carrier encodings.

## Impact

- **common**: `flight/schema.rs` (v3 schemas), `flight/conversion/*` (protobuf
  extraction replaces `extract_value` → JSON for the v3 path; `extract_value`
  stays for v1 and the down-converter), `attrs/typed.rs` (place from
  `AnyValue`; residue v2 encode/decode), `wal` (metadata `schema_version`
  only), `flight/transport.rs` (`ServiceCapability::TypedWire`), and a
  `v3 → v1` down-converter.
- **acceptor**: OTLP and Prometheus handlers emit v3 under the negotiation
  gate; the forwarder down-converts for writers without the capability;
  `[acceptor].wire_format`.
- **writer**: `flight_iceberg.rs` accepts v1 and v3; `schema_transform.rs`
  gains the v3 plan (the carrier columns pass through to the writer WAL as
  `Binary`); `storage/iceberg.rs` placement reads `AnyValue`s; the
  `TypedWire` capability is registered.
- **querier**: residue v2 decode; the `attribute_list` field; result encoding
  carriers.
- **router / signaldb-sdk / ui**: OpenAPI field docs for the new carriers and
  `attribute_list`; regenerated clients; UI attribute tables render the
  carriers.
- **compactor**: none (it copies the residue column opaquely). A test asserts
  that.
- **tests-integration**: a fidelity round-trip per signal, a mixed-version
  acceptor/writer matrix, and WAL replay across the upgrade.
- **docs/skills**: `flight-schemas` (wire axis gains v3),
  `storage-layout` (residue forms, WAL payload versions), operations upgrade
  notes (ordering, drain-before-downgrade), `docs/users/querying-ir.md`
  (`attribute_list`, carriers).
- No new dependencies: `prost` and `opentelemetry-proto` are already in
  `common`, and `ciborium` already backs the residue.
