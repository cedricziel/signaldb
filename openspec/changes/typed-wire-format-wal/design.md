# Design

## Context

See proposal.md (Why). Current state, from the code:

- **The ingest path has three hops, and the AnyValue becomes JSON text at the
  first one.** The acceptor decodes OTLP (`prost`) and runs tenant OTTL
  processors on the decoded request. `common::flight::conversion` then builds
  a Flight **v1** `RecordBatch`, in which every `AnyValue`-bearing field is a
  JSON string produced by `extract_value` → `serde_json::to_string`:
  - traces: `attributes_json`, `resource_json`, `scope_attributes`, and
    `events[].attributes_json` / `links[].attributes_json` inside
    `List<Struct>`
  - logs: `attributes_json`, `resource_json`, `scope_json`, and `body` (an
    encoded AnyValue)
  - metrics: `attributes_json`, `resource_json`, `scope_json`, and the
    exemplar `filtered_attributes` inside `data_json`
  - profiles: `attributes_json`, `resource_json`, `scope_json`

  The batch goes to the acceptor WAL as Arrow IPC (`record_batch_to_bytes`),
  with the entry metadata JSON
  `{"schema_version":"v1","signal_type",…,"ingest_id"}`. It is then forwarded
  to a writer by `do_put` with the same metadata as `app_metadata`.

- **Writer.** `flight_iceberg.rs` reads `schema_version`. For `"v1"` it runs
  `transform_for_signal` to the last pre-typed physical shape (`physical-v4`
  traces / `physical-v3` logs), where attribute columns are still JSON
  strings. For anything else (`"v2"`) it **passes the batch through
  untransformed**. It then appends the result to the writer's own
  per-tenant/dataset/signal WAL. At commit, `IcebergTableWriter` parses each
  JSON container (`attrs::typed::parse_json_object_rows`), asks the type
  authority for the canonical type, and splits values into
  `{c}_str/_int/_double/_bool` plus the CBOR `{c}_residue`
  (`attrs::typed::encode_residue`, a CBOR **map**).
- **WAL framing is independent of the payload.** `SDBW`/`SDBR` records are
  checksummed byte ranges, and the payload is opaque Arrow IPC. A payload
  schema change does not touch the framing.
- **Discovery tolerates unknown capabilities.** `catalog::parse_capabilities`
  skips names it does not know with a warning. A new capability string is
  therefore safe for older readers.
- **The JSON losses are listed in proposal.md.** The duplicate-key loss
  happens at the acceptor's `Map::insert`, before anything is durable. Fixing
  it anywhere downstream is impossible.

FDAP constraint: every Arrow type used here (`Binary`, `List<Struct>`) comes
from the `datafusion::arrow` re-export, so the acceptor, writer and querier
stay version-aligned with DataFusion.

## Goals / Non-Goals

**Goals:**

- Bit-exact preservation of every `AnyValue` from the acceptor's OTLP decode
  to the storage write: type, value (including NaN/±Inf and bytes), key
  order, duplicate keys, and nesting.
- No dead-lettered batch caused by the carrier's own limits.
- A rolling deploy in either order that never loses or dead-letters data,
  plus a documented, data-safe rollback.
- Duplicates retrievable from storage. The canonical (filterable) value of a
  duplicated key is defined.

**Non-Goals:**

- Changing OTLP ingest, the compatibility query surfaces, or the Iceberg
  table schema (`physical-vN`). The residue column stays `Binary`; only the
  document inside it gains a second form.
- **Global key order in storage across typed homes.** The typed homes are
  per-type maps, so the relative order of distinct keys that land in
  different homes is not recorded. Duplicates keep their relative order.
  Recording a full order vector is an open question, not part of this change.
- Changing how log `body` is **stored** (today a string column with the
  `encode_log_body` encoding). The wire and WAL carry it exactly, and the
  writer's existing body encoding still runs at commit. Body storage fidelity
  for kvlists with duplicate keys is an open question.
- Moving metric numeric payloads out of `data_json`. Only the exemplar
  `filtered_attributes` leave it (they are an attribute container).

## Decisions

### D1: The carrier is OTLP's own protobuf encoding, in `Binary` columns

Each `AnyValue`-bearing field becomes a nullable `Binary` column:

| v1 column (Utf8 JSON)                                 | v3 column (Binary)                                                                                   | Bytes                                    |
| ----------------------------------------------------- | ---------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `attributes_json`                                     | `attributes_pb`                                                                                      | `KeyValueList` (the record's attributes) |
| `resource_json`                                       | `resource_attributes_pb`                                                                             | `KeyValueList`                           |
| `scope_attributes` / `scope_json`                     | `scope_attributes_pb` (+ `scope_name`, `scope_version` as Utf8 where v1 nested them in `scope_json`) | `KeyValueList`                           |
| `events[].attributes_json`, `links[].attributes_json` | `events[].attributes_pb`, `links[].attributes_pb`                                                    | `KeyValueList`                           |
| logs `body`                                           | `body_pb`                                                                                            | `AnyValue`                               |
| exemplar `filtered_attributes` in `data_json`         | `exemplar_filtered_attributes_pb: List<Binary>` aligned with the exemplars in `data_json`            | `KeyValueList` each                      |

`null` means the container was absent. An empty `KeyValueList` means it was
present and empty. That distinction is kept on purpose, because the
read-side `null` vs `{}` rule already depends on it.

**Why protobuf over the alternatives:**

- _CBOR with a SignalDB-defined AnyValue mapping:_ it can be made exact (byte
  strings, floats, arrays of pairs), but it is a second encoding of OTLP's
  type system for SignalDB to keep in sync with every `AnyValue` evolution.
  The protobuf bytes **are** OTLP's encoding. `prost` is already the
  acceptor's decoder, and an `AnyValue` variant OTLP adds later would at
  worst decode as "value unset" in an older writer, the same behaviour as
  any OTLP receiver. Rejected as the carrier. CBOR stays the **storage**
  residue format, where the spike chose it and the querier already reads it.
- _Columnar Arrow (`List<Struct<key, type, str, int, double, bool, bytes>>`
  or a dense union):_ Arrow types cannot be recursive, so arrays and kvlists
  would still need an opaque nested encoding. The writer places values row
  by row through the type authority anyway, so columnar layout buys nothing
  on this path. Rejected.
- _JSON with escapes (`$otlp_type` for NaN, kvlist-as-pairs):_ this patches
  each loss separately, still re-parses text, and still hits `serde_json`'s
  depth limit. Rejected.

**Encoding cost.** The acceptor re-encodes each container from the decoded
request (`KeyValueList { values }.encode_to_vec()`). The cost is comparable
to today's `serde_json::to_string`, and smaller on the wire because there is
no key quoting or base64. Slicing the original request bytes would avoid
re-encoding, but OTTL processors mutate the decoded request first, so
re-encoding is the only correct source.

**Depth.** The acceptor's own OTLP decode already applies `prost`'s
recursion limit (100). Anything that reached the acceptor therefore
re-encodes and decodes again within the same limit. The carrier introduces
no new limit, so a legal request can no longer be dead-lettered by its own
nesting.

### D2: Wire schema version `v3`, self-describing by column type

`app_metadata` and the WAL entry metadata carry `"schema_version": "v3"`.
`"v2"` keeps its meaning (storage-shaped passthrough). The writer dispatches
on the version **and** checks the batch's Arrow schema: a v3 version with
Utf8 `*_json` columns, or v1 with `*_pb` columns, is rejected as
`invalid_argument` with a precise message. A metadata/schema mismatch is
therefore an attributable error, never a silent mis-transform.

Why a new version value rather than reusing `v1` and switching on column
type: an **old** writer sends any non-`v1` batch down the passthrough path
untransformed. A v3 batch that reached an old writer would be written to its
WAL and then fail at commit, getting dead-lettered rather than rejected at
`do_put` (where the acceptor would keep it and retry). D3's negotiation
exists to make sure an old writer never sees v3. The version value is the
second line of defence and the audit trail in WAL metadata.

### D3: Rollout negotiation through a `TypedWire` capability

- **Writers** of this release register `ServiceCapability::TypedWire`
  alongside `Storage`. Older acceptors ignore the unknown name (it is skipped
  with a warning).
- **Acceptors** run `[acceptor].wire_format`:
  - `auto` (default): encode **new** ingest as v3 only when every live
    `Storage` writer in discovery advertises `TypedWire`; otherwise v1. The
    decision is re-evaluated on each discovery refresh.
  - `json`: always v1 (pins the old carrier, e.g. during a rollback).
  - `typed`: always v3. The acceptor refuses to start forwarding to a writer
    without the capability and leaves entries in the WAL (at-least-once,
    never dropped).
- **Forwarding a v3 WAL entry** (encoded while all writers were upgraded) to
  a writer that does **not** advertise `TypedWire` (a writer was rolled back
  meanwhile) **down-converts** the batch to v1 with the existing
  `extract_value` path: lossy exactly as today. It increments
  `signaldb.acceptor.wire_downconversions` with signal and tenant attributes,
  and logs once per entry. Data is delivered, never dead-lettered.
- **Writers accept v1 and v3** for at least one full release after the
  typed wire ships (the deprecation window). An acceptor WAL written by the
  old release therefore replays into a new writer unchanged: v1 entries go
  through today's transform and JSON split.

| Acceptor \ Writer | old writer                      | new writer                       |
| ----------------- | ------------------------------- | -------------------------------- |
| old acceptor      | v1 (today)                      | v1 accepted (deprecation window) |
| new acceptor      | v1 (gate closed) / down-convert | v3                               |

### D4: Writer WAL and commit path

For v3, the writer's `transform_for_signal` uses a v3 plan that matches the
v1 plan field for field, except that the `*_pb` columns **pass through as
`Binary`** into the writer-WAL batch, still named `*_pb` there. The legacy
`span_attributes`/`log_attributes` JSON carrier columns are not produced. At
commit, `IcebergTableWriter` detects which carrier a batch holds from its
schema and either:

- **v3:** decodes each `KeyValueList` with `prost`. For each key it resolves
  the canonical type once per batch (as today) and places the value:
  - a canonical-type match goes to its home (NaN/±Inf doubles included);
  - an off-type, array, kvlist, bytes, or empty value goes to the residue as
    CBOR (bytes → CBOR byte string, double → CBOR float, empty → CBOR null);
  - a **duplicated key** puts its **last** occurrence in the home, if
    canonical-typed, and records **all** occurrences in the residue's v2 form
    (D5).
- **legacy (Utf8 JSON):** today's `parse_json_object_rows` path, unchanged.
  This serves writer-WAL entries written by the previous release and v1 wire
  batches.

Replay of the writer WAL after an upgrade therefore needs no migration: each
entry's own Arrow schema selects the path. Log `body_pb` is decoded into the
`AnyValue` and passed to the existing `encode_log_body` at commit, so stored
bodies are byte-identical to what v1 produces for every body without
duplicate kvlist keys.

### D5: Residue v2: a second document form, only for duplicates

Today the residue is a CBOR **map** `{key: value}` of the keys that have no
typed home. This change:

- keeps that exact encoding for every container **without** duplicate keys.
  That is nearly all of them, and it means no storage churn and no reader
  change for most rows;
- for a container **with** a duplicated key, writes
  `Tag(55801, Array[[key, value], …])`: the residue keys plus **every
  occurrence of each duplicated key, in sent order**, including the
  occurrence that also went to a typed home. 55801 is a value from the CBOR
  first-come-first-served tag range, reserved for this form in
  `attrs::typed`.

**Decoding rule:** a map is v1. Tag 55801 is v2. Anything else is a corrupt
residue (the reader's existing error path). Map-shaped consumers (the
`{scope}.attributes` bag, residue lookups for a single key) read a v2
document by **last occurrence wins**, which matches the typed home. Only
`attribute_list` exposes every occurrence.

**Why not always write v2:** that would rewrite every residue for no gain
and widen the downgrade blast radius to every row. **Why not
`Map` with repeated keys:** RFC 8949 treats duplicate map keys as invalid,
and decoders (including `ciborium`'s `Value` → struct paths) may reject or
silently collapse them. A tagged pair array is unambiguous.

The compactor copies the residue column as opaque `Binary` and never decodes
it. A test pins that, so the compactor needs no change.

### D6: Rollback and downgrade: drain before downgrade

The **BREAKING** surfaces are exactly three, each with its own rule:

1. **Wire v3 at an old writer:** prevented by D3. On rollback, set
   `[acceptor].wire_format = "json"` first, or let `auto` see the old
   writer, and then downgrade writers. Any v3 entry still queued in an
   acceptor WAL is down-converted on forward.
2. **Writer WAL entries with `*_pb` carriers at an old writer:** an old
   writer cannot replay them. Before downgrading a writer, **drain** it:
   `do_action("flush")` per tenant (the existing operational primitive),
   then wait for the writer's `signaldb.wal.entries_pending` gauge to reach 0. If
   that is skipped, the old writer dead-letters the entries at commit (a
   schema mismatch), where they stay under `[wal].dead_letter_retention` and
   can be replayed by re-upgrading. They are retained, not lost, but not
   ingested either. The operations guide documents the drain as a required
   step.
3. **Residue v2 at an old querier:** the old reader errors on a tagged
   document, failing queries that touch an affected row's raw bag. Deploy
   order is **queriers before writers**, and on rollback **writers before
   queriers**. A querier downgrade after v2 residues exist is unsupported, in
   line with the forward-only policy for the typed layout.

**Upgrade order:** queriers → writers → acceptors. Acceptors flip
automatically under `auto` once every writer advertises `TypedWire`.

**End of the deprecation window (a separate, later release, BREAKING):**
writers reject wire v1 and the legacy writer-WAL carrier. Before upgrading
to that release, operators must confirm that no acceptor or writer WAL holds
v1 entries. A startup check reports any it finds and refuses to drop them:
the writer keeps serving v3 and leaves v1 entries unprocessed, with a clear
error, until they are drained by the previous release.

### D7: Query-side exposure

- `{scope}.attributes` (the existing retrieval-only bag) keeps its shape: one
  JSON entry per key, value from the **last** occurrence.
- New retrieval-only `{scope}.attribute_list`:
  `[{"key": k, "value": v}, …]`. Every key appears, and duplicates appear as
  separate entries in sent order. The relative order of distinct keys follows
  home order, then residue order. It is documented as **not** the sent order
  (Non-Goals).
- JSON result encoding of values JSON cannot express, applied wherever an
  `AnyValue` is rendered (bags, `attribute_list`, event/link attributes):
  - non-finite double → `{"$otlp_type":"double","value":"NaN"|"+Inf"|"-Inf"}`.
    A typed `float64` column cell stays `null`, as today; the carrier is only
    used inside AnyValue renderings;
  - a nested kvlist with a duplicate key, or with a key spelled
    `$otlp_type`, → `{"$otlp_type":"kvlist","entries":[[k, v], …]}`. This
    ends the bytes/kvlist ambiguity in results too.
- These are **IR result changes** (additive for ordinary data; a value that
  rendered `null` for NaN now renders the carrier). They ship gated on the
  next IR version at implementation time, so stored dashboards on older
  versions keep today's rendering.

## Risks / Trade-offs

- [A writer rolled back without draining dead-letters its WAL entries] →
  Drain is a documented required step. Dead-lettered entries are retained
  and replayable. A startup log on the old release cannot be added
  retroactively, so the new release's downgrade notes carry the warning.
- [Mixed fleet stuck on v1 because one writer never upgrades] → `auto` keeps
  correctness (today's fidelity) and exposes the gate state as a metric
  (`signaldb.acceptor.wire_format{format}`), so the stall is visible.
- [Down-conversion silently reduces fidelity during a partial rollback] →
  It is counted per signal and tenant, and it is the documented behaviour of
  rollback.
- [Canonical home takes the last duplicate, but a consumer expected the
  first] → Last-wins is OTel SDK `SetAttribute` semantics and matches
  today's stored value. `attribute_list` exposes all occurrences.
- [Residue v2 readers in third-party tooling reading Iceberg directly] → The
  residue is documented as SignalDB-internal. The storage-layout doc
  describes both forms.
- [Wire batch size changes alter the acceptor's chunking behaviour] →
  Protobuf is smaller than the JSON it replaces, so chunk thresholds still
  hold. The fidelity integration test covers a near-limit batch.

## Migration Plan

Stacked PRs (tasks.md sketch). Every step keeps `main` deployable:

1. Residue v2 **reader** in the querier (no writer emits it yet).
2. v3 schemas, protobuf extraction, and the down-converter in `common`
   (unused).
3. Writer accepts v3 (the v3 transform plan plus the commit path from
   `AnyValue`s) and advertises `TypedWire`. Acceptors still send v1, so no
   behaviour changes.
4. Writer emits residue v2 for duplicates (reachable only from v3 batches).
5. Acceptor negotiation (`wire_format`, default **`json`** in this PR),
   forward-time down-conversion, metrics.
6. Flip the default to `auto`; ship query-side `attribute_list` and the
   carriers under the next IR version; docs.
7. (Later release) remove v1 acceptance, after the deprecation window.

Rollback per D6.

## Open Questions

- Should storage record full key order (a per-container order vector in the
  residue) so `attribute_list` can return the exact sent order? This is
  deferrable: it is a residue v3 form with the same tag mechanism, and it
  does not change this change's wire or WAL.
- Log body storage for kvlist bodies with duplicate keys: switch the body to
  a residue-style CBOR column, or accept last-wins in the stored string? This
  needs a `physical-vN` bump, so it is a separate change.
- How long is the v1 deprecation window: one release, or time-based (≥ the
  longest plausible acceptor WAL backlog plus `dead_letter_retention`)?
