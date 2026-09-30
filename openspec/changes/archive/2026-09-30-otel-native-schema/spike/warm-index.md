# Spike 0.1 — Warm typed containment index

Prototype + proof for the **warm tier** requirement of `otel-native-schema`
(`specs/typed-attribute-storage/spec.md`, "Warm tier — a derived typed
containment index prunes before promotion").

- Code: `spikes/otel-native-spike/src/warm_index.rs` (library),
  `spikes/otel-native-spike/src/bin/warm_index_demo.rs` (proof binary).
- Run: `cd spikes/otel-native-spike && cargo run --release --bin warm_index_demo`

## Question

Parquet keeps **no** per-key statistics or bloom filters inside a `MAP`. The spec
therefore requires a derived, per-type containment index — a _typed_
generalization of today's untyped `attr_tokens: List<Utf8>` — that prunes row
groups / files for a `key = value` predicate on an **unpromoted** attribute
without stringifying typed literals. This spike proves the pruning actually
happens and quantifies it, and establishes whether DataFusion 54.1 can exploit
the list-leaf bloom **natively** or whether a **custom pre-filter** is required.

## Prior art studied

- `src/common/src/schema/mod.rs`: `ATTR_TOKENS_COLUMN`,
  `bloom_filter_property_for_attr_tokens()` → today's untyped
  `attr_tokens: List<Utf8>` with a Parquet bloom on the list leaf
  `attr_tokens.list.item` (Iceberg property
  `write.parquet.bloom-filter-enabled.column.attr_tokens.list.item = true`).
- `src/writer/src/schema_transform.rs::attr_tokens_column`: derives one
  `key=value` **string** token per attribute across resource/scope/record
  scopes, deduplicated per row, list-never-null.
- `src/querier/src/query/logql.rs` + `logs.rs`: when the column is present the
  querier ANDs `array_has(attr_tokens, 'key=value')` onto equality predicates,
  for equality operators only. Comment claims this is "bloom-prunable" — this
  spike tests that claim directly (see Finding 1).

## Token encoding design (the typed generalization)

Today's token is a `key=value` **string** — only good for string equality, and
`status_code=200` (int) collides with a string attribute whose value is the text
`"200"`. The typed generalization keeps the "fuse key+value into one bloom leaf
value" idea but makes it **per canonical type** and **binary**:

```
token = key_utf8 || 0x1F (unit separator) || canonical_value_bytes
```

| column          | Arrow type     | value bytes                     |
| --------------- | -------------- | ------------------------------- |
| `attr_tok_str`  | `List<Binary>` | UTF-8 of the string value       |
| `attr_tok_int`  | `List<Binary>` | `i64::to_be_bytes` (8 bytes)    |
| `attr_tok_f64`  | `List<Binary>` | `f64` IEEE-754 BE (generalizes) |
| `attr_tok_bool` | `List<Binary>` | single `0x00`/`0x01` byte       |

The spike proves `str` + `int` (the two the task requires); `f64`/`bool`
generalize identically.

**Why per-type columns.** One bloom per canonical type means the int token for
`http.response.status_code = 200` lives in a different bloom filter than any
string token — no cross-type collision, and the int literal is probed as 8
big-endian bytes, never as the text `"200"`. This is the concrete difference
from the untyped `attr_tokens`.

**Why `0x1F` and no length prefix.** Attribute keys are dotted identifiers that
never contain `0x1F`, so the key/value boundary is unambiguous. A single
canonical byte string per `(key, value)` keeps the encoding a pure function.

**No false negatives.** A bloom answers "definitely absent" or "maybe present".
A false _negative_ (wrongly skipping a file that contains the pair) can only
happen if the same `(key, value)` hashed to different bytes at write time vs
query time. The encoding is deterministic, so write and query bytes are
identical — no false negatives. Two _different_ `(key, value)` pairs colliding
to the same bytes only ever cause a false _positive_ (an extra file scanned),
which the bloom already tolerates and which the exact predicate over the typed
map filters out. The demo asserts `false_negatives == 0`.

**Leaf path.** Arrow-rs writes `List<Binary>` as the 3-level
`<col> (LIST) > list (repeated) > item`, so the Parquet bloom is enabled on the
leaf `<col>.list.item` — exactly the leaf the product's
`bloom_filter_property_for_attr_tokens()` targets on the Iceberg path. The spike
writes with `WriterProperties::set_column_bloom_filter_enabled(ColumnPath::new(["attr_tok_int","list","item"]), true)`.

## Experiment

Run header: `files=40 rows/file=8000 rg_size=2000 (4 rg/file) bloom_fpp=0.01`,
dataset generated in 314 ms, 17.92 MiB on disk (160 row groups). Each target
pair occurs in **4/40 files** (one row group per carrying file):
`http.request.host = rare.internal.svc.local` (str) in files {3,13,23,33},
`http.response.status_code = 418` (int) in files {5,14,23,32}.

- Note vs. reality: real hive files have exactly **1 row group per file**
  (see `data-characterization.md`), so the demo's per-row-group numbers
  collapse onto its per-file numbers in production — file-level pruning is the
  operative axis.
- Targets: `http.request.host = rare.internal.svc.local` (str),
  `http.response.status_code = 418` (int).
- Three measurements: native DataFusion `array_has`, negative-control bare MAP,
  custom footer+bloom pre-filter. Deterministic (seeded).

## Findings

### Finding 1 — DataFusion 54.1 does NOT prune natively on `array_has(list, literal)`

Measured with `bloom_filter_on_read=true, pushdown_filters=true`:

| predicate             | rg pruned (bloom) | rg pruned (stats) | files pruned | rows pruned by pushdown | matched |
| --------------------- | ----------------- | ----------------- | ------------ | ----------------------- | ------- |
| int `status_code=418` | **0**             | 0                 | **0**        | 319,980                 | 20      |
| str `host=rare...`    | **0**             | 0                 | **0**        | 319,980                 | 20      |

> **Correction (from spike 0.3):** the demo reads these counters via
> `MetricsSet::sum_by_name`, which silently returns 0 for DataFusion 54.1's
> `MetricValue::PruningMetrics` variant — so this table cannot distinguish
> "pruned nothing" from "counter unreadable". The conclusion still holds on
> independent evidence: `pushdown_rows_pruned=319,980` proves every row group
> was read, the source analysis below explains why, and spike 0.3 re-verified
> with structural metric matching + `EXPLAIN ANALYZE`. Fix the demo's metric
> reading before citing this table on its own.

**Root cause (from the DataFusion 54.1 source, not just measurement):** the
Parquet row-group bloom pruner
(`datafusion-datasource-parquet/src/bloom_filter.rs`) only loads blooms for the
columns returned by `PruningPredicate::literal_columns()`, each mapped to a
Parquet leaf via `parquet::arrow::parquet_column(...)`:

1. `array_has(list_col, lit)` is a scalar UDF, not a comparison, so it produces
   **no `LiteralGuarantee`** → `literal_columns()` does not include the token
   column.
2. Even if it did, the guarantee's column would be the **`List` column**
   (`attr_tok_int`), and `parquet_column()` cannot resolve a `List` column to
   its primitive `.list.item` leaf → the bloom is never loaded.

So `row_groups_pruned_bloom_filter == 0`. With `pushdown_filters = true` the
predicate still runs as a **late-materialization row filter** — it prunes
_rows_ (`pushdown_rows_pruned`) but never skips a row group or a file. **This
contradicts the "bloom-prunable" comment in the current querier**: the existing
`array_has(attr_tokens, …)` conjunct does not trigger row-group bloom pruning
either. It only helps by filtering rows after the row group is already read.

### Finding 2 — Negative control: the bare typed MAP prunes nothing

`attr_int_map['http.response.status_code'] = 418` over the typed MAP column:
`row_groups_pruned_bloom=0, row_groups_pruned_stats=0, files_pruned_stats=0,
pushdown_rows_pruned=0` — not even late-materialization row pruning engages.

Confirms the spec's premise: an equality predicate over the typed `MAP` column
skips no row groups/files. The map leaf carries no bloom and no per-key
min/max, so there is nothing to prune against — a correct but fully-scanned
(cast-free) read.

### Finding 3 — Custom footer+bloom pre-filter prunes the overwhelming majority

| predicate             | files kept | files pruned | row groups kept | rg pruned | probe cost (whole dataset) |
| --------------------- | ---------- | ------------ | --------------- | --------- | -------------------------- |
| int `status_code=418` | 4/40       | **90.0%**    | 4/160           | **97.5%** | 4.1 ms                     |
| str `host=rare...`    | 4/40       | **90.0%**    | 4/160           | **97.5%** | 4.2 ms                     |

False negatives: **0** for both (asserted by the demo — every carrying file
survives the filter).

Reading each file's footer with bloom reading enabled and probing the leaf
bloom for the encoded token (`SerializedFileReader` +
`get_column_bloom_filter(leaf_idx).check(token)`) — the work a custom
`TableProvider` / row-group pruning hook would do in **layer 4.3** — prunes the
files/row groups that cannot contain the pair, with **zero false negatives**.

### Overhead

Bloom bytes across the dataset: int index 330,240 B (1.758% of total file
bytes), str index 23,040 B — **1.88% combined** of the 17.92 MiB dataset. The
int index is ~14× the str index because the demo's int tokens have far more
distinct values per row group (ndv drives bloom sizing), a realistic shape for
high-cardinality int attributes.

## Verdict

**The warm typed containment index works, but only with a custom pruning
hook.** DataFusion 54.1 exploits the list-leaf bloom **zero percent** natively
(`array_has` yields no `LiteralGuarantee`, and a `List` column cannot be
resolved to its `.list.item` leaf by the pruner) — for both the new typed
tokens and today's product `attr_tokens`. A custom footer+bloom pre-filter —
the layer-4.3 hook — prunes **90% of files / 97.5% of row groups** on an
unpromoted `key = value` predicate for str and int alike, with zero false
negatives, at 1.88% **bloom-byte** overhead (spike 0.3 correction: the token
_columns_ add 24–36% total storage — see `bench.md` finding 4) and ~0.1 ms
probe cost per file. The
negative control confirms the bare typed map prunes nothing (0 everywhere).
Task 0.1's feasibility question is answered: **yes, with the hook; never
natively.**

## Implications for the design (layer 4.3)

1. **The warm index must ship with a custom pruning hook, not a bare
   `array_has` conjunct.** DataFusion will not turn `array_has(list, lit)` into
   a row-group bloom probe. The index needs a file/row-group pre-filter that
   reads footers and probes the leaf bloom directly (as prototyped here), fed
   into a `ParquetAccessPlan` / custom `TableProvider`, or a `PhysicalExpr`
   that the pruner recognizes (e.g. rewriting `array_has` into a form that
   yields a `LiteralGuarantee` on a scalar-typed shadow). The footer+bloom
   probe is cheap (~4 ms for the 40-file dataset, ~0.1 ms/file).
2. **Per-type binary tokens are the right encoding.** They keep int/str/float
   equality typed and cross-type-collision-free with one bloom leaf per type,
   at 1.88% size overhead.
3. **The existing `attr_tokens` `array_has` conjunct should be re-evaluated.**
   It is currently doing row-level filtering, not row-group pruning — the
   claimed bloom benefit is not being realized on the row-group path. The same
   custom-pre-filter mechanism this spike prototypes would let both the legacy
   untyped tokens and the new typed tokens actually prune.

## Limitations / surprises

- **parquet 58.4 API drift:** `ReadOptionsBuilder::with_read_bloom_filter` is
  gone; bloom reading now rides `ReaderProperties::builder().set_read_bloom_filter(true)`
  via `with_reader_properties` — and `Sbbf::check` takes the unsized `[u8]`
  directly. Two compile fixes were needed against the workspace pin.
- **The product's "bloom-prunable" comment is wrong today.** The querier's
  `array_has(attr_tokens, …)` conjunct only ever row-filters after IO; the
  bloom written by `bloom_filter_property_for_attr_tokens()` is never probed
  on the row-group path. Worth a follow-up issue independent of this change.
- **Probe cost scales with file count, not size** (one footer + one bloom read
  per file). On hive's tiny-file population (10–80k files/table) the pre-filter
  itself becomes a fan-out cost — another reason compaction and the tiered
  design matter; the bench (0.3) should measure the probe at that file count.
- The demo's `rows_pushdown_pruned=319,980` shows late materialization doing
  the correctness work today — reading every row group to discard 99.99% of
  rows.
