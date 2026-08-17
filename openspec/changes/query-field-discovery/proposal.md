## Why

`query-ir-core` lets the UI _submit_ a structured query and get a bounded result
back, but the structured builder is still a blank box: nothing tells a client
**what it can build on**. Which signal sources does this tenant have? Which
fields are queryable on each, with what type? What values does a field take? How
common is a field, and how many values does it have?

Today that knowledge only exists behind dialect-specific compatibility endpoints
— Loki `labels`/`label_values`/`detected_fields`, Tempo `search/tags` and
`search/tag/{tag}/values`, Prometheus `labels` — every one of which answers by
**scanning data through the querier**. They are lossy (Prometheus-safe label
names, not OTel dotted keys), inconsistent per signal, and they put an
interactive UI affordance (typing in a field picker) on the query engine's hot
path. First-party surfaces are not allowed to read through the compat APIs at
all (CLAUDE.md — "Query IR is our own query surface"), so today the native
Explore UI, the CLI, and the MCP server have **no legitimate way to answer
"what can I filter on"**.

Meanwhile the ingredients for a metadata-tier answer already exist and are
unused: the canonical logical field catalog (`LogicalSchema::core()`), the
tenant's semantic-convention schema registries (shipped by `schema-registry` —
types, briefs, enum members), and the compactor's attribute statistics
(`attribute_stats`: per key presence, approximate distinct count, query demand)
which exactly one endpoint reads today (`/prometheus/api/v1/label_stats`).

This change makes discovery a first-class native surface answered from that
metadata tier — issue #820's headline requirement: **served from the registry
plus statistics, not by scanning data**.

## Scope split: tail and pagination leave this change

The stub bundled two unrelated things under "UI-facing companions to core
execution". They are now separated, because they share nothing but that framing:

- **Live tail moves to epic #437** (_Streaming Query Results — Live Tail for All
  Signal Types_), which already owns the streaming substrate (WAL broadcast
  #439, Acceptor Flight tail #440, router WebSocket #441, SSE #445, cursor
  reconnection #444). The only thing this change hands over is a **constraint**:
  when #441 builds the native tail, its filter language must be the IR document
  and its records must use the same logical namespace, so a tail is "the same
  query, streamed" rather than a second query language. Nothing about tail
  remains in this change.
- **Pagination / continuation moves to a new stub change,
  `query-result-pagination`** — delivery-side, unary, and orthogonal to both
  streaming (#437's cursors are for reconnecting a live stream, not for walking
  a completed result) and discovery.

What remains here is build-side discovery only, which is also what #820 asks
for. The capability keeps its name: `query-field-discovery` now means exactly
what it says.

## What Changes

- **A `metadata` result envelope** (the one `query-ir-core` deferred to this
  change) and a terminal **`describe` stage**, at `irVersion` 4. A discovery
  request is an ordinary IR document — same versioning, same source registry,
  same logical namespace, same auth and tenant scoping — declaring
  `result: "metadata"` with a terminal `describe` stage. (The `scalar` envelope
  stays with `query-metrics-model`; `trace` with `query-structural-traces`.)
- **`describe: fields`** — the queryable fields of a source, as logical dotted
  OTel names with their canonical type, attribute level, filterability, and
  where known a coverage fraction and an approximate distinct-value count. Never
  a physical column name, never promotion state.
- **`describe: values`** — value suggestions for one field. Answered exactly and
  for free where the value set is _declared_ (registry enum members, span kind,
  status code, severity). Otherwise the response says so: it returns no values
  and names what would answer it, rather than quietly scanning. A client that
  wants the data-derived answer must ask for it explicitly (`sample: true`),
  and then gets a bounded, sampled answer with its cost stated in the response.
- **`GET /api/v1/query/sources`** — the tenant's available signal sources,
  returning the same `metadata` envelope shape. This is a **bounded, deliberate
  exception** to "first-party reads go through `POST /api/v1/query`": an IR
  document requires a `from`, and "which sources exist" is the one question that
  has no source to name. It is not licence to add further side endpoints —
  anything that _can_ be phrased as a document about a source stays a document.
- **Cost is part of the contract.** Every discovery response carries a `cost`
  object: which tier answered (`metadata` or `sampled_scan`), whether the answer
  is time-window-scoped, whether it is sampled/approximate, and how stale the
  statistics behind it are (`asOf`). A discovery response can never be a silent
  full scan.
- **Answered in the router**, from the catalog and the in-process registries. A
  `describe` document does not reach a querier and never becomes a DataFusion
  plan — except on the explicitly opted-in sampled-values path, which reuses the
  existing bounded label/tag-value tickets.
- **Surface parity**: HTTP (above), CLI (`signaldb discover fields|values|
sources` re-pointed at the native surface), MCP (`discover_fields`,
  `discover_field_values`, `discover_sources` — the existing
  `discover_attributes` tool keeps working, unchanged, for compat callers).

### BREAKING: the IR surface version moves to 4

`irVersion` 4 is a version bump on our own query surface, so it is labelled
**BREAKING** even though it is additive in effect. Precisely what it does and
does not change:

- Every existing `irVersion` 1/2/3 document keeps its exact meaning and keeps
  executing. Nothing is re-interpreted, and no alias or shim is introduced
  (post-1.0 the project does not ship them).
- A document declaring `irVersion` 3 (or lower) that carries a `describe` stage
  or the `metadata` envelope is **rejected with a typed error naming the version
  the stage requires** — never silently coerced to 4, never executed with the
  stage dropped. This follows the existing `heatmap`→v2 and
  `histogram_quantile`→v3 gates, and is pinned by a test.
- A document declaring an unsupported version (`5`, say) keeps the existing
  behaviour: rejected with the supported range reported.

**Explicitly scoped out, in writing:**

- **The Explore UI's field pickers are not re-pointed in this change.** The UI
  today gets its suggestions from the Loki/Tempo compat endpoints via
  hand-written call sites that also feed the trace/log facets; moving them is a
  UI refactor with its own regression surface, tracked as part of #769 (Explore
  UI field audit). This change ships the endpoint the UI will consume and its
  generated TypeScript client; the swap is a follow-up. Every other surface
  (HTTP, CLI, MCP) ships here.
- **Predicate-scoped discovery** ("given these filters, what else can I filter
  on"). See design D6: it cannot be answered from unconditional statistics, and
  the exact-but-expensive answer is _already expressible today_ as an ordinary
  IR query (`where` + `aggregate by [field]` + `topk`). A `where` stage before
  `describe` is therefore **rejected** with an error that names that equivalent
  query, rather than being silently ignored. The stage slot stays open for a
  future sketch-backed answer.
- **Rewriting the compat endpoints.** Loki/Tempo/Prometheus metadata endpoints
  keep their current scan-backed behaviour for external clients (Grafana);
  `trace-attribute-discovery`'s guarantees are untouched. Discovery **replaces**
  those endpoints for first-party callers; it does not extend them.

## Honest limits today (what waits on #813)

The attribute-registry epic's foundational table (#813 — `attribute_registry`,
the authoritative key→physical mapping) **does not exist yet**, and the
querier's resolver deliberately treats any unknown name as an attribute-map
extraction. So a registry alone cannot enumerate a tenant's attribute keys. This
change is honest about which tier answers which part:

| Question                                            | Answered from                            | Available today                                                                    |
| --------------------------------------------------- | ---------------------------------------- | ---------------------------------------------------------------------------------- |
| Which sources exist                                 | IR source registry + the tenant's tables | yes                                                                                |
| Which fields are declared, and their canonical type | `LogicalSchema::core()`                  | yes                                                                                |
| What a key means, its type, its enum members        | tenant + bundled schema registries       | yes                                                                                |
| Which attribute keys this tenant actually has       | `attribute_stats` (compactor analyzer)   | yes, but as-of the last compaction pass, not window-scoped                         |
| Which keys are promoted / where they live           | `attribute_registry` (#813)              | **no** — deliberately not surfaced (it is a performance detail, never a name)      |
| Top values per key with counts                      | value sketches (see design D5)           | **no** — this change adds the surface and the fallback; the sketch is its own task |

The response's `origin` per item and `cost.asOf` make that visible to the
client, so the surface is forward-compatible rather than blocked: when #813 and
the value sketches land, the same request returns better-sourced items with no
contract change.

## Capabilities

### New Capabilities

- `query-field-discovery`: the tenant-scoped introspection surface that tells a
  client what it can query — available signal sources, queryable fields per
  source as logical dotted OTel names with canonical types, and value
  suggestions for a field — answered from the schema registry and maintained
  statistics rather than by scanning signal data, with the cost and provenance
  of every answer stated in the response.

### Modified Capabilities

- `query-ir-core`: gains the `metadata` result envelope and the terminal
  `describe` stage at `irVersion` 4 (the envelope it explicitly deferred to this
  change).

## Impact

- **Crates**: `common` (IR `describe` stage + `metadata` envelope + version 4;
  a discovery assembly module merging logical schema, schema registry, and
  attribute statistics), `router` (describe handling on `POST /api/v1/query`,
  `GET /api/v1/query/sources`, OpenAPI), `signaldb-sdk` + `signaldb-cli`
  (regenerated SDK, `discover` command), `mcp-server` (three read-only tools),
  `compactor` (value sketches, if that task lands in this change),
  `tests-integration`.
- **Issues**: implements #820. Cross-links, does not duplicate: #813 (attribute
  registry — the better field source, once it exists), #819 (LogQL label
  resolution via the registry — the compat-side sibling), #818 (virtual-schema
  TableProvider), #769 (Explore UI field audit — consumer), #263 (entities),
  #437 (received live tail), #732 (`detected_fields`, whose native replacement
  this is).
- **API surfaces**: additive. `POST /api/v1/query` gains a document shape it
  previously rejected; `GET /api/v1/query/sources` is new. No ingest, Flight
  wire schema, or on-disk Iceberg/WAL change. Compat dialects unchanged.
- **Config**: bounds for discovery (`max_fields`, `max_values`, sampled-scan row
  cap) under the existing querier/router limits; no new required keys.
