## Context

See proposal.md — Why. This section records only what was measured against a
live deployment, because several design decisions turn on it.

Verified against hive (`_system` tenant, one-hour window, 2026-08-18):

- `GET /api/v1/schema/entities` returns 64 entity definitions from the bundled
  `otel@1.43.0` registry; 36 carry at least one identifying attribute.
  `process` → `[process.pid, process.creation.time]` plus 13 associated metric
  names.
- `describe { target: "fields" }` (irVersion 4) returns per-source field
  descriptors carrying `coverage`, `cardinality {estimate, at_least}`, `origin`,
  and a metadata `cost` with an `as_of` stamp. On metrics: `process.pid`
  coverage 0.039 / cardinality 1, `container.name` 0.293 / 3,
  `service.instance.id` 0.391 / 4. On traces those three are **absent**
  entirely, while `service.name` and `host.name` report coverage 1.0.
- These statistics are live and refreshing — `as_of` was under an hour old on
  both sources at time of writing.
- `describe { target: "values" }` answers declared enumerations from the
  registry (`span_kind` → the five kinds, `origin: "registry"`), but for
  `service.name` and `process.pid` it reports that no maintained statistics
  cover the field and names the query that would answer it. The value sketch
  behind that path (#1329) is not in the deployed release.

So the two halves of maintained statistics are in different states: per-key
**presence and cardinality** are deployed and fresh; per-key **value sketches**
are not. The design depends only on the former.

The catalog today issues one query per entity type per source that both
discovers and measures — `buildEntitySourceDoc` in `src/ui/src/api/catalog.ts`
— which is the root of the conflated-count problem the specs now forbid.

## Goals / Non-Goals

**Goals:**

- Detection cost that does not grow with the number of registry entity types:
  one metadata call per source, not one per (type × source).
- A path that degrades honestly when statistics are missing or stale.
- Accept the value sketch as an optimization later without re-architecting.

**Non-Goals:**

- Changing any backend crate. Every surface consumed here is deployed.
- Entity relationships or topology (which entity talks to which). The registry
  models `extends`, not edges; out of scope.
- Reworking the detail page's existing tables (top values, dependency
  breakdown, waterfall navigation). Their requirements are untouched.

## Decisions

### Two tiers, split at the point where cost changes shape

Detection reads metadata; instance listing reads data. Keeping them in one query
is what forced spans and metric points into one column.

- **Tier 1 — which entity types exist.** One `describe { target: "fields" }` per
  source (5 calls total, independent of entity-type count). Intersect each
  source's field set with each registry entity type's identifying attributes. An
  entity type is present in a source when its _primary_ identifying attribute is
  present there. This drives the nav and the per-type signal attribution.
- **Tier 2 — which instances exist.** On opening one entity type, one grouped
  aggregate per source in which it is present, grouping by the surviving
  identity tuple. This is the only data read, and only for the type the user
  asked for.

_Alternative rejected — answer tier 2 from value discovery too._ This was the
original shape of the idea and it does not work: `describe {target:"values"}`
returns values for **one attribute at a time**, so reconstructing a two-attribute
identity would mean cross-producting independent value lists and inventing
instances that were never observed together. Identity is a tuple; the statistics
are per key. A grouped read is the only thing that answers it correctly. This is
also why the cardinality cap is not the blocker it first appeared to be — the cap
degrades value lists, which tier 1 does not use, and tier 1's presence and
cardinality figures survive capping.

### Entity types from the registry, presentation from a local overlay

Fetch `/api/v1/schema/entities`, keep those with ≥1 identifying attribute, and
order identity dimensions as the registry declares them (first = primary).

A small UI-side overlay keyed by entity name supplies what the registry cannot
express: display label and plural, nav ordering, the `breakdown` / `topValues`
columns the existing detail-page requirements rely on, and `spanKindScope`.

The overlay is presentation-only and never gates discovery — an entity type with
no overlay entry is still listed, labelled from its registry name. This is what
keeps "a new entity type needs no code change" true rather than nominal.

_`spanKindScope` stays in the overlay, not the registry._ Scoping services to
Server-kind spans is a statement about SignalDB's trace query, not about the
OTel entity definition; the registry has no place to put it and should not.

### Identity tuple degrades per source

`process` is declared as `(process.pid, process.creation.time)`, but
`process.creation.time` appears in no source on hive. Grouping by it would put
every process in one null-valued bucket — the same defect the current Processes
page shows with `host.name`.

Rule: drop identifying attributes absent from that source's field list; if the
_primary_ is absent, the source contributes nothing. Because tier 1 already
holds each source's field list, this needs no extra call.

### Nav lists observed types only

36 catalogable types against a handful with data would otherwise be 30+ empty
tabs. Tier 1 already knows which are present, so the nav is exactly the observed
set. Where the current fixed list is a superset of what is observed, this is
strictly less noise; where it is a subset (processes, containers, service
instances on hive today) it is strictly more signal.

### Unanalyzed is a third state, not an empty list

A source with no statistics covering an identity attribute is not the same as a
source with no such entities. The distinction is user-visible per the specs, and
`as_of` is surfaced so a stale answer is legible as stale. Concretely this is
what a freshly-ingesting partition looks like before its first compaction —
common enough that treating it as "no entities" would misinform routinely.

## Risks / Trade-offs

- **Tier 1 inherits compaction lag.** An entity type whose telemetry arrived
  after the last compaction pass is invisible to detection until statistics
  catch up. → Surfaced rather than hidden, via the unanalyzed state and the
  `as_of` stamp. A user who suspects staleness can still open the type and get a
  tier-2 read over live data.
- **`coverage > 0` is presence, not existence-now.** Statistics summarize what
  compaction scanned, which may not align with the user's window. → Tier 2 is
  window-scoped and authoritative; tier 1 only decides what to offer. A type
  offered but empty on open is acceptable; the reverse (never offered) is not.
- **Cardinality estimates are approximate and capped at 10,000.** A capped
  identity attribute still reports presence, which is all tier 1 consumes, but
  any instance-count hint derived from `cardinality.estimate` would be a lower
  bound. → Do not present cardinality as an instance count; `at_least` must be
  honoured wherever it is shown at all.
- **Registry breadth can surprise.** A tenant publishing a broad custom registry
  gets many catalogable types at once. → Bounded by the observed-only nav rule.
- **More round trips on first paint** (5 describes + 1 entities fetch) than the
  current single fan-out. → All are metadata reads, cacheable across time-range
  changes since none is window-scoped in the way tier-2 reads are; the fan-out
  they replace scaled with entity-type count and read data.

## Migration Plan

No data migration; no backend deploy. The change is additive within the UI and
reversible by reverting the frontend.

Sequenced as a stack to keep each PR reviewable (see tasks.md):

1. Split detection from measurement behind the existing fixed type list — no
   user-visible change beyond RED honesty, and independently revertable.
2. Registry-derived types with the presentation overlay.
3. All-signal detection via tier 1, observed-only nav, unanalyzed state.

Rollback is per-PR; nothing shares state with the backend.

### The spec is mechanism-neutral so an entity table can replace this later

Field statistics are how this design answers tier 1, but the spec deliberately
constrains only the observable properties — every signal covered, no
signal-data scan, presence attributed per source, staleness legible. A future
materialized entity table, keyed by `(entity_type, identity tuple)` and written
on ingest, would satisfy the same requirements unchanged: it would serve both
tiers as point reads, make multi-attribute identity correct by construction, and
retire the unanalyzed state entirely.

The schema already reserves the join key such a table needs —
`resource.identity` is declared as a `SignalDbDefined` logical field on every
source (`common/src/schema/logical.rs:84`) with no producer anywhere in the
workspace. This design neither implements nor depends on it; the point is that
nothing here forecloses it. Tracked as #1339 (entity table) and #1340 (the
unproduced `resource.identity` field) — write-path work that wants its own
proposal.

## Open Questions

- Whether the value sketch (#1329), once released, should serve tier-2 listing
  for entity types whose identity is a single low-cardinality attribute. It
  cannot serve multi-attribute identities at all (see Decisions), so this is a
  narrow optimization, decidable after measuring tier-2 read cost in practice.
  It changes no spec and no task here.
