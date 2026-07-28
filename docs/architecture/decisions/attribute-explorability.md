---
audience: contributor
type: decision-record
status: record
---

# Attribute explorability: beyond the materialized-label allowlist

**Date**: 2026-07-28
**Status**: Accepted direction; layers land incrementally

## Context

SignalDB stores OTLP attributes as flat JSON strings in `*_attributes`
columns. Filtering on an attribute is a substring match on the serialized
`"key":"value"` fragment — inexact (can over-match), limited to `=`/`!=`,
and unable to prune Parquet data.

The materialized-labels feature (PRs #723–#728) added a per-signal
allowlist: configured keys are promoted at ingest to dedicated
`label_<key>` columns, matched exactly with regex and (for logs) ordered
comparisons. That fixed the *declared* labels — but the design principle
this record exists to capture is:

> **It is the database's job to make unknown unknowns explorable.**
> A user must be able to filter, group, and discover attributes nobody
> declared in advance.

An allowlist optimizes known knowns. This record sets the direction for
the rest.

## What we learned from prior art

Three systems were analyzed in depth (July 2026):

- **ClickHouse** (`JSON` type, 24.8+/25.x): every JSON path automatically
  becomes a typed subcolumn, bounded by a path budget (default 1024 per
  part); overflow paths land in a *seekable, typed* "shared data" tier
  (25.8's advanced format made selective overflow reads ~58× faster).
  Which paths hold columns is renegotiated **by frequency at every
  merge**. SigNoz runs the inverse: everything in the overflow tier plus
  query-pattern-driven promotion — 99% less data scanned.
- **Honeycomb** (Retriever): every field is always its own sparse column
  file, created lazily on first appearance; no indexes at all. The
  bounded resource is **dimensionality** (2,000 fields/event), never
  value cardinality. Brute-force scan + time pruning + elastic fan-out.
- **Grafana Loki** (3.x): kept its tiny label index (≤15 labels) and
  layered on **structured metadata** (per-entry KVs, stored but never
  indexed), **bloom filters over metadata pairs** (a whole
  Planner/Builder/Gateway subsystem), and UI-side
  `detected_labels`/`detected_fields` APIs for discovery. Promotion
  remains a static OTLP allowlist — the weakest part of its design.

**Convergent findings:**

1. Explorability does **not** come from materializing everything. It
   comes from making the *unmaterialized* tier good: typed, seekable,
   prunable — plus a discovery API.
2. Bound **dimensionality** (column count), never value cardinality.
   Parquet's monolithic footer degrades with thousands of sparse
   columns; a materialized-column budget in the low hundreds per table
   is realistic. Loki's cardinality caps are about *stream/partition*
   explosion — for SignalDB the analogous constraint is on partition
   keys, not columns.
3. Promotion should be **demand-driven and applied at the rewrite
   point** — for SignalDB, the compactor (ClickHouse promotes at merge;
   the compactor rewrite also solves existing-table backfill and the
   schema-evolution gap in the pinned iceberg-rust fork by committing
   the rewrite and `SetCurrentSchema` together).
4. **Parquet gives us Loki's bloom subsystem for free**: split-block
   bloom filters and row-group statistics are written in-format; Iceberg
   manifests add file-level pruning above them.
5. **Iceberg V3's Variant type with shredding** (ratified June 2025)
   standardizes exactly this two-tier design (typed shredded subcolumns
   + binary residual). Adoption depends on arrow-rs/DataFusion/
   iceberg-rust support maturing.

## Decision

Layered evolution, in value order. The existing allowlist becomes the
"pinned" subset of an eventually-automatic system; its plumbing
(`materialized_column_name`, schema injection, writer extraction, per-
table query routing) is reused by every layer.

- **Layer 1 — typed overflow tier.** Migrate `*_attributes` from JSON
  strings to `Map<Utf8,Utf8>` columns. Every attribute — declared or
  not — becomes exactly matchable (`attributes['key'] = 'v'`) with
  dictionary encoding and predicate pushdown, eliminating the substring
  approximation everywhere. Design with a migration path to Iceberg V3
  Variant.
- **Layer 2 — format-native acceleration.** Enable Parquet bloom
  filters on `label_<key>` columns and on a derived `key=value` token
  column for the long tail (Loki 3.3's trick, inside the file format;
  no new services).
- **Layer 3 — discovery API.** A `detected_fields`-style endpoint:
  for a selection, return the attribute keys present with type, approx
  cardinality, and volume — from footers/dictionaries for materialized
  columns, sampling for the rest. This is the product face of "unknown
  unknowns explorable" (powers Grafana Logs Drilldown-style UX) and
  requires no indexing. Independent of Layer 1; works today via
  sampling the JSON columns.
- **Layer 4 — compactor auto-promotion.** Per-key statistics
  (presence, HyperLogLog cardinality, query-hit counters at the
  existing `attribute_expr`/`matcher_expr` chokepoints) drive
  promotion of the top-N keys under a schema-width budget at compaction
  rewrite, with symmetric demotion and hysteresis across cycles.
  Honeycomb's type rules apply: first-seen type wins, coerce on
  conflict, never reject the event; guard against generated key names
  (runaway schemas).

## Consequences

- Promotion latency equals compaction cadence (hours, not seconds) —
  acceptable because Layer 1 makes unpromoted keys decently queryable
  in the interim.
- A hard materialized-column budget per table (low hundreds) must be
  enforced by Layer 4; the overflow tier is the pressure valve.
- Partitioning stays low-cardinality regardless; materialized columns
  are not partition keys.
- The current JSON-substring path remains as the compatibility fallback
  until Layer 1 lands, and for tables predating any given column.
