# Spike 0.3 — Attribute-layout benchmark on real hive data

Measurement record, 2026-08-04. Numbers come from the run logged below; do not
edit them to match later behaviour.

- Code: `spikes/otel-native-spike/src/bench.rs` (library),
  `spikes/otel-native-spike/src/bin/bench_demo.rs` (runnable proof).
- Run: `cd spikes/otel-native-spike && cargo run --release --bin bench_demo`
- Env: `SPIKE_DATA_DIR` (default `.data/spike/hive`), `BENCH_OUT`,
  `BENCH_REPS` (default 3), `BENCH_EXPLAIN=1` (dump `EXPLAIN ANALYZE` for the
  promoted-column query), `BENCH_TRACE_ROWS`/`BENCH_LOG_ROWS` (harness
  shakeout only — every number here uses the constants).

## Question

Which of the four attribute-storage strategies in
`specs/typed-attribute-storage/spec.md` actually pays, on the data SignalDB
really stores, and what do the auxiliary mechanisms (residue decode, registry
lookup, warm-index probe) cost? `data-characterization.md` established the
population; this spike prices the layouts against it.

## Configuration

```
seed=0x0742454E43483033  reps=3 (median reported)
200,000 rows: 150,000 from _system/_monitoring/traces (4,001 files)
               50,000 from _system/_monitoring/logs   (471 files)
attrs/row avg 14.0; 83 distinct keys → str 55, int 22, double 2, bool 1, json 3
off-type keys: bench.conflicted 5.0% (synthesized), params 50.0% (the one real
               mixed-type key on hive, n=4)
regimes: small-flush 100 rows/file → 2,000 files | compacted 50,000 rows/file → 4 files
one row group per file (matches hive); ZSTD(1) + dictionary; bloom fpp 0.01
planted rare values in 20 rows, 2 clusters → carrying files: small-flush
{400, 1200}, compacted {0, 2}
```

Four layouts, all built from the same in-memory rows so only the physical shape
differs:

| variant | layout                                                                                                                                              |
| ------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| V0      | legacy — one `Map<Utf8,Utf8>`, every value stringified (what hive stores now)                                                                       |
| V1      | typed per-type maps (`str`/`int`/`double`/`bool`) + top-level `Binary` residue (one CBOR array of `(key, type_tag, raw)` per row)                   |
| V2      | V1 + `target`→`attr_target` (Utf8) and `thread.id`→`attr_thread_id` (Int64) promoted to top-level columns with stats + bloom, removed from the maps |
| V3      | V1 + the warm typed containment index of spike 0.1 (`attr_tok_str`/`attr_tok_int` `List<Binary>` with list-leaf blooms)                             |

Two perturbations are synthetic, because the real population contains no usable
specimen (see `data-characterization.md`):

1. `bench.conflicted` — canonical `int` (190,064 rows), 5% (9,936 rows) carry
   the **OTLP-string** value `"42"`. The text is numeric on purpose: the legacy
   string map cannot distinguish it from the integer `42`, the typed layout
   routes it to the residue. 1,845 rows hold the integer 42.
2. Planted rare values (`target='bench.rare.target'`, `busy_ns=424242424242`,
   `thread.id=987654321`) in 20 rows, so pruning has a ground truth.

**Load caveat.** The machine ran parallel cargo builds throughout. Two full runs
were taken; storage numbers are byte-identical (deterministic), wall times vary
by up to 2× between runs while the **ratios between variants stayed stable**.
Both runs are quoted where the difference matters. Treat every millisecond
figure as an order of magnitude, not a benchmark-grade constant.

## Storage and write cost

| variant | regime      | files | total MiB | B/row | footer % | bloom % | footer B/file | write µs/row (run 1 / run 2) |
| ------- | ----------- | ----: | --------: | ----: | -------: | ------: | ------------: | ---------------------------: |
| V0      | small-flush | 2,000 |     39.05 | 204.7 |     8.3% |   0.00% |         1,707 |               13.1 / **5.4** |
| V1      | small-flush | 2,000 |     42.45 | 222.6 |    16.7% |   0.00% |         3,720 |               17.6 / **7.2** |
| V2      | small-flush | 2,000 |     44.00 | 230.7 |    18.0% |   0.42% |         4,158 |               15.2 / **7.2** |
| V3      | small-flush | 2,000 |     52.52 | 275.4 |    16.1% |   0.85% |         4,440 |              47.9 / **10.9** |
| V0      | compacted   |     4 |     15.36 |  80.5 |     0.0% |   0.00% |         1,775 |                8.7 / **4.0** |
| V1      | compacted   |     4 |     10.50 |  55.1 |     0.1% |   0.00% |         3,981 |               14.0 / **5.3** |
| V2      | compacted   |     4 |     10.39 |  54.5 |     0.2% |   0.05% |         4,445 |                8.8 / **6.5** |
| V3      | compacted   |     4 |     14.32 |  75.1 |     0.1% |   1.45% |         4,764 |              10.2 / **10.2** |

**The sign of the storage verdict flips with file size.** Compacted, the typed
layout is **31.6% smaller** than legacy (10.50 vs 15.36 MiB). At the real
small-flush size it is **8.7% larger** (42.45 vs 39.05 MiB), because the footer
more than doubles: 1,707 → 3,720 bytes per file, paid on every file.

Per-column compressed bytes, compacted regime, explain both halves:

| column                           |        V0 |       V1 |        V3 | raw → compressed |
| -------------------------------- | --------: | -------: | --------: | ---------------- |
| `attributes` values (V0)         |     13.86 |        — |         — | 231 MB → 16.7×   |
| `attributes_residue`             |         — |     4.97 |      4.97 | 143 MB → 28.8×   |
| `attributes_int` values          |         — |     2.32 |      2.32 | 3.4 MB → 1.5×    |
| `attributes_str` values          |         — |     1.42 |      1.42 | 77.7 MB → 54.8×  |
| map keys (all)                   |      0.21 |     0.26 |      0.26 |                  |
| `attr_tok_int` / `attr_tok_str`  |         — |        — |      3.77 |                  |
| **attribute payload total (MB)** | **14.07** | **8.97** | **12.74** |                  |

Splitting one heterogeneous string column into homogeneous typed columns buys
~36% on the attribute payload — ZSTD compresses the JSON residue 28.8× and the
short strings 54.8× once they are no longer interleaved with each other. That
saving is real but it is smaller than the footer tax at 100 rows/file, and hive
writes 1.7–132 rows/file.

Extrapolating the footer delta to the live population: `_system/_monitoring/logs`
holds 82,668 files, so +2.0 KB/file is **+166 MB of pure footer**, against a
table that is 864 MB of parquet today. The direction of the storage argument for
this change depends on compaction landing first.

Write cost: V1 costs **+33%** over V0 per row (both runs agree on the ratio;
5.4 → 7.2 µs/row in run 2), V2 is indistinguishable from V1, and V3 costs
**+100% to +266%** — the token columns and their per-file blooms, not the typed
builders, are what hurts.

## Query classes

Both tables are from run 2. `f-stat`/`rg-stat`/`rg-bloom` are file-range and
row-group prunes by statistics and bloom; `pre ms` is the V3 warm-index probe;
`files fed` is how many files reached DataFusion at all.

### small-flush regime (2,000 files)

| class                       | variant | median ms | pre ms |       rows | f-stat | rg-stat | rg-bloom | MiB read |            files fed |
| --------------------------- | ------- | --------: | -----: | ---------: | -----: | ------: | -------: | -------: | -------------------: |
| 1 str eq, unpromoted        | V0      |     231.4 |      — |         20 |      0 |       0 |        0 |    32.45 |                2,000 |
|                             | V1      |     237.3 |      — |         20 |      0 |       0 |        0 |     5.47 |                2,000 |
|                             | V2 †    |     284.8 |      — |         20 |     24 |      10 |    1,464 |     0.07 |                2,000 |
|                             | V3      |   **2.7** |  192.7 |         20 |      0 |       0 |        0 |     0.02 | **6 (99.7% pruned)** |
| 2 int eq, unpromoted        | V0      |     230.0 |      — |         20 |      0 |       0 |        0 |    32.45 |                2,000 |
|                             | V1      |     250.2 |      — |         20 |      0 |       0 |        0 |     3.19 |                2,000 |
|                             | V2      |     299.6 |      — |         20 |      0 |       0 |        0 |     2.91 |                2,000 |
|                             | V3 ‡    |     132.0 |  100.4 |         20 |      0 |       0 |        0 |     1.14 |   580 (71.0% pruned) |
| 3 eq on promoted key        | V0      |     226.3 |      — |         20 |      0 |       0 |        0 |    32.45 |                2,000 |
|                             | V1      |     274.0 |      — |         20 |      0 |       0 |        0 |     3.19 |                2,000 |
|                             | V2      |     192.4 |      — |         20 |  1,505 |       0 |        0 | **0.00** |                2,000 |
|                             | V3 ‡    |      74.2 |   92.3 |         20 |      0 |       0 |        0 |     1.01 |   516 (74.2% pruned) |
| 4 int range (no pruning)    | V0      |     239.5 |      — |     65,563 |      0 |       0 |        0 |    32.45 |                2,000 |
|                             | V1      |     360.9 |      — |     65,563 |      0 |       0 |        0 |     3.19 |                2,000 |
|                             | V2      |     307.1 |      — |     65,563 |      0 |       0 |        0 |     2.91 |                2,000 |
|                             | V3      |     291.9 |      — |     65,563 |      0 |       0 |        0 |     3.19 |                2,000 |
| 5 conflicted key, int form  | V0      |     283.8 |      — | **11,781** |      0 |       0 |        0 |    32.45 |                2,000 |
|                             | V1      |     255.6 |      — |  **1,845** |      0 |       0 |        0 |     3.19 |                2,000 |
|                             | V2      |     337.7 |      — |      1,845 |      0 |       0 |        0 |     2.91 |                2,000 |
|                             | V3      |     209.1 |   84.4 |      1,845 |      0 |       0 |        0 |     2.63 | 1,562 (21.9% pruned) |
| 7 time-slice full-row fetch | V0      |     140.9 |      — |      2,001 |  1,947 |       0 |        0 |     1.16 |                2,000 |
|                             | V1      |     188.8 |      — |      2,001 |  1,948 |       0 |        0 |     1.11 |                2,000 |
|                             | V2      |     186.4 |      — |      2,001 |  1,947 |       0 |        0 |     1.11 |                2,000 |
|                             | V3      |     284.7 |      — |      2,001 |  1,947 |       0 |        0 |     1.30 |                2,000 |

† In V2 the key is a promoted column, so class 1 measures `attr_target = …`,
not a map lookup. ‡ Under-sized bloom — see "Bloom sizing" below; with a
correctly sized bloom both drop to 6/2,000.

Class 6 (residue retrieval) is measured separately, below.

### compacted regime (4 files)

| class                       | variant | median ms | pre ms |       rows | f-stat | rg-bloom | MiB read | files fed |
| --------------------------- | ------- | --------: | -----: | ---------: | -----: | -------: | -------: | --------: |
| 1 str eq, unpromoted        | V0      |      94.5 |      — |         20 |      0 |        0 |    13.42 |       4/4 |
|                             | V1      |      85.4 |      — |         20 |      0 |        0 |     1.53 |       4/4 |
|                             | V2      |   **2.9** |      — |         20 |      0 |        1 |     0.02 |       4/4 |
|                             | V3      |      37.9 |    0.6 |         20 |      0 |        0 |     0.94 |       2/4 |
| 2 int eq, unpromoted        | V0      |      86.4 |      — |         20 |      0 |        0 |    13.42 |       4/4 |
|                             | V1      |      12.0 |      — |         20 |      0 |        0 |     2.29 |       4/4 |
|                             | V2      |      11.8 |      — |         20 |      0 |        0 |     2.00 |       4/4 |
|                             | V3      |      11.1 |    0.3 |         20 |      0 |        0 |     1.53 |       2/4 |
| 3 eq on promoted key        | V0      |     101.7 |      — |         20 |      0 |        0 |    13.42 |       4/4 |
|                             | V1      |      11.3 |      — |         20 |      0 |        0 |     2.29 |       4/4 |
|                             | V2      |   **2.1** |      — |         20 |      3 |        0 |     0.05 |       4/4 |
|                             | V3      |       9.8 |    0.2 |         20 |      0 |        0 |     1.53 |       2/4 |
| 4 int range (no pruning)    | V0      |      89.9 |      — |     65,563 |      0 |        0 |    13.42 |       4/4 |
|                             | V1      |      12.2 |      — |     65,563 |      0 |        0 |     2.29 |       4/4 |
|                             | V2      |      11.6 |      — |     65,563 |      0 |        0 |     2.00 |       4/4 |
|                             | V3      |      13.2 |      — |     65,563 |      0 |        0 |     2.29 |       4/4 |
| 5 conflicted key, int form  | V0      |     100.1 |      — | **11,781** |      0 |        0 |    13.42 |       4/4 |
|                             | V1      |      11.2 |      — |  **1,845** |      0 |        0 |     2.29 |       4/4 |
|                             | V2      |      10.6 |      — |      1,845 |      0 |        0 |     2.00 |       4/4 |
|                             | V3      |      11.7 |    0.3 |      1,845 |      0 |        0 |     2.29 |       4/4 |
| 7 time-slice full-row fetch | V0      |   **7.9** |      — |      2,001 |      5 |        0 |     1.45 |       4/4 |
|                             | V1      |      11.6 |      — |      2,001 |      5 |        0 |     2.11 |       4/4 |
|                             | V2      |      11.8 |      — |      2,001 |      5 |        0 |     2.08 |       4/4 |
|                             | V3      |      14.2 |      — |      2,001 |      5 |        0 |     2.87 |       4/4 |

## Findings

### 1. The bare typed map prunes nothing — confirmed, everywhere

Every V1 row in both tables shows `f-stat=0, rg-stat=0, rg-bloom=0`. The typed
`MAP` carries no per-key statistics and no bloom, so an equality predicate over
it reads every file. This reproduces spike 0.1's negative control on real data
and at production file counts: **typing alone buys zero pruning**. What it does
buy is a 6–10× smaller scan (1.53–3.19 MiB vs 13.42–32.45 MiB) and, compacted,
a 7–9× faster query (11–12 ms vs 86–102 ms) because the reader no longer drags
the JSON blobs and the digit-strings through the same column.

### 2. Promotion prunes bytes, not files

V2's promoted column is the only layout DataFusion prunes natively: 1,464
row groups by bloom and 24 file ranges by statistics on the string key, 1,505
file ranges on the int key, cutting bytes read from 32.45 MiB to 0.07 MiB and
0.00 MiB. Compacted, that is a 33–48× speed-up (2.1–2.9 ms vs 94–102 ms). At
small-flush it is **worth nothing in wall time** (192–285 ms, no better than
V0): the file range is pruned only after its footer has been read, and 2,007
file opens cost more than all the data. `files fed` stays 2,000 in every V2 row.

The two promoted keys prune through different mechanisms: the rare int falls
outside every file's min/max, so statistics alone suffice, while the rare string
sorts inside the min/max range of most files and only the bloom eliminates them.
Promotion of a string key therefore has to carry a bloom, not just statistics.

### 3. The warm index is the only strategy that removes files, and its probe is not free

V3 pruned 99.7% of files on the string probe (6 of 2,000 survive against a
ground truth of 2 carrying files) and the surviving scan took 2.7 ms instead of
231 ms. The pre-filter dropped no carrying file in any class: V3 returned the
same row count as the full scan every time (20, 20, 20, 65,563, 1,845, 2,001). But the probe itself cost **192.7 ms** for 2,000
files (~96 µs/file), so end-to-end V3 was 195 ms against V0's 231 ms — a 16%
win, not the 85× the scan number suggests. At 4 files the probe is 0.2–0.6 ms
and the picture inverts to a clean win. **The warm index's benefit is bounded
by file count, exactly as spike 0.1 predicted.**

Class 5 shows the other edge: for a _common_ value the probe keeps 78% of files
and adds 84 ms of pure overhead. A pre-filter must be skipped when the
predicate is not selective.

### 4. The warm index costs 24–36% of storage, not 1.88%

Spike 0.1 reported 1.88% overhead — that counted **bloom bytes only**. The
token _columns_ are the real cost: `attr_tok_int` + `attr_tok_str` are 3.77 MB
of a 14.32 MiB compacted dataset (26%), taking V1 → V3 from 10.50 to 14.32 MiB
(**+36.4%** compacted, **+23.7%** small-flush). This is a correction to 0.1's
overhead figure and it changes the tier's cost/benefit: the warm index is a
one-third storage surcharge, which is why it belongs behind a per-key or
per-dataset policy rather than on by default.

### 5. Bloom sizing is a correctness-grade detail for the warm index

parquet-rs resolves an unset bloom NDV to the row group's **row** count. A
containment index writes one token per _attribute_, so the default under-sizes
the bloom by the attributes-per-row factor (14× here). Measured at small-flush:

| bloom NDV               | bloom B/file | total MiB | write ms | str files kept | int files kept |
| ----------------------- | -----------: | --------: | -------: | -------------: | -------------: |
| default (= 100 rows/rg) |          234 |     52.52 |    2,177 |        6/2,000 |  **580/2,000** |
| explicit (1,400 tokens) |          525 |     53.08 |    3,912 |        6/2,000 |    **6/2,000** |

Ground truth is 2 carrying files. The high-cardinality int tokens saturate the
default 144-byte filter and the false-positive rate reaches ~29%, collapsing
pruning from 99.7% to 71.0%; the low-cardinality string tokens happen to
survive. Setting `set_column_bloom_filter_ndv(rows_per_row_group ×
attrs_per_row)` restores full selectivity for **+1.1% storage** and ~+80% write
time on that column. Any implementation of layer 4.3 must set the NDV
explicitly.

### 6. Off-type values are a correctness difference, not a performance one

Class 5, both regimes: V0 returns **11,781** rows for
`attributes['bench.conflicted'] = '42'`; V1/V2/V3 return **1,845** for
`attributes_int['bench.conflicted'] = 42`. The legacy string map conflates the
integer 42 with the 9,936 rows carrying the OTLP string `"42"` — a 6.4×
over-count that no amount of tuning fixes. The typed layout routes those values
to the residue, where they remain retrievable but are not matched by an int
predicate.

### 7. Residue is cheap to parse and dominant in bytes

Class 6, over the same 200,000 rows:

| regime      | rows with residue | entries | raw payload | fetch ms | decode ms | ns/row | ns/entry |
| ----------- | ----------------: | ------: | ----------: | -------: | --------: | -----: | -------: |
| small-flush |            24,749 |  33,331 |  136.86 MiB |      359 |      40.2 |  1,624 |    1,206 |
| compacted   |            24,749 |  33,331 |  136.86 MiB |      203 |      56.8 |  2,295 |    1,704 |

Decoding CBOR into owned `(String, String, String)` triples costs **1.2–1.7 µs
per entry**, ~2 µs per row — negligible next to the 203–359 ms it takes to
fetch the column. The bytes are the story: the residue is 4.97 MB compressed of
V1's 11.0 MB compacted total (**45%**), because this tenant stores whole Iceberg
schemas and table-property blobs as attributes. Those same bytes sit in V0's
string map today, so this is not new cost — but it does mean residue is the
biggest column in the typed layout, and it must never be read for a query that
does not ask for it.

### 8. Registry lookup is not the write-path bottleneck

Per-attribute resolution at ~100% hit rate, real key sets (49 traces keys, 45
logs keys), at hive's actual batch shapes:

| cache shape                    | batch rows | ns/lookup (traces) | ns/lookup (logs) | lookups/sec |
| ------------------------------ | ---------: | -----------------: | ---------------: | ----------: |
| hoisted per-scope map          |          1 |               31.0 |             48.0 |     20–32 M |
| hoisted per-scope map          |          3 |               16.8 |             29.0 |     34–60 M |
| hoisted per-scope map          |      1,000 |               18.0 |             28.6 |     35–56 M |
| global map, composite key      |     1–1000 |            177–494 |          439–455 |       2–5 M |
| shared `RwLock`, 1 guard/batch |          1 |               14.0 |             25.8 |     39–71 M |
| cold miss (read + insert)      |          — |              483.2 |                — |         2 M |

At 14 attributes/row this is **0.2–0.7 µs of lookup per row** against a total
build+write cost of 5.4–17.6 µs/row: **2–5% of the write path**. The design's
worry (D: "write-path cost is the registry lookup, not the builders") is
inverted by measurement — the builders and the extra columns cost 33% (V1) to
266% (V3), the lookups cost single-digit percent.

Two implementation notes fall out. First, never key a global map by an owned
5-tuple: allocating the composite key per attribute costs 177–494 ns, 10–15×
the hoisted lookup. Second, at 1-row batches the _scope_ resolution stops being
amortized — that is why the hoisted shape (which allocates four `String`s per
batch to build the scope key) is slower at 1 row than the `RwLock` shape that
does not. Resolve the scope once per batch without allocating.

### 9. At hive's file counts, file count beats every layout choice

The single largest effect in the small-flush table is that all four variants
land in 140–360 ms whether they read 32.45 MiB or 0.02 MiB. Compacting the same
200,000 rows into 4 files makes the _worst_ layout (V0, 94 ms) faster than the
_best_ layout at small-flush (V3 end-to-end, 195 ms). Every layout conclusion
above is second-order to that.

## Corrections to earlier spike output

- **Spike 0.1's pruning counters were unreadable, not zero.** DataFusion 54.1
  reports pruning as `MetricValue::PruningMetrics` (displayed `N total → M
matched`), and `MetricsSet::sum_by_name` deliberately ignores that variant —
  `as_usize()` returns 0 for it. `warm_index_demo` reads them with
  `sum_by_name`, so it prints 0 for every pruning metric regardless of what
  happened. This bench matches the variant structurally instead (see
  `walk_metrics` in `bench.rs`) and was verified against `EXPLAIN ANALYZE`.
  0.1's conclusion still holds — its `pushdown_rows_pruned=319,980` proves the
  row groups were read — but its metric table cannot be used as evidence, and
  the demo should be fixed before the doc is cited further.
- **Warm-index overhead is 24–36%, not 1.88%** (finding 4).

## Implications for the design

1. **Sequence compaction before, or with, the layout cutover.** The typed
   layout is 31.6% smaller compacted and 8.7% larger at today's file sizes, and
   every pruning mechanism is neutralized by per-file overhead at 2,000 files.
   Shipping the layout onto the current small-flush population would make
   storage worse and queries no faster.
2. **Promotion is the only mechanism DataFusion exploits natively**; the warm
   index needs the custom hook from 0.1, and that hook must set an explicit
   bloom NDV and must be skipped for non-selective predicates.
3. **Budget the warm index as a one-third storage surcharge**, per key or per
   dataset, not as a default-on tier.
4. **Registry lookup is affordable**; spend the design attention on the
   builders, the residue encoder and the token columns instead.
5. **Keep the residue out of the read path by default.** It is the largest
   column in the typed layout on this tenant (45% of compacted bytes) and only
   needs decoding when a query actually asks for an off-type or complex value.
