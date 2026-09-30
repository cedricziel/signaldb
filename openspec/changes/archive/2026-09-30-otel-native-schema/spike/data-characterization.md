# Spike data characterization — real hive traces/logs (task 0.3 prereq)

Sample fetched 2026-08-04 (read-only) from hive
(`/mnt/hive/apps/signaldb/storage`) into `.data/spike/hive/`, mirroring the
tenant/dataset/table layout. Per table: newest 4000 parquet by mtime + 200
largest by size (deduped) + latest Iceberg metadata.json/manifests. See
`.data/spike/hive/MANIFEST.txt`. Analysis via duckdb + a raw footer-length scan.

## Population on hive (full, for context)

| table                      | parquet files | parquet bytes | Iceberg metadata        |
| -------------------------- | ------------- | ------------- | ----------------------- |
| jobradar/production/traces | 18,148        | 210 MB        | **20 GB, 54,531 files** |
| jobradar/production/logs   | 30,977        | 282 MB        | (similar pathology)     |
| `_system/_monitoring/traces` | 28,290        | 452 MB        | —                       |
| `_system/_monitoring/logs`   | 82,668        | 864 MB        | dir totals 413 GB       |

Headline: **metadata dwarfs data by ~100×** on the long-lived tables. #895
(delete-after-commit) is deployed but the backlog persists. Any spike conclusion
about footer/metadata overhead is not hypothetical — it is the dominant cost on
the live system today.

## Sample inventory (16,254 files, 445 MB)

| table           | files | rows    | rows/file avg | max rows | row groups/file |
| --------------- | ----- | ------- | ------------- | -------- | --------------- |
| jobradar traces | 4,086 | 6,951   | **1.7**       | 378      | 1.0             |
| jobradar logs   | 4,049 | 11,493  | **2.8**       | 240      | 1.0             |
| `_system` traces  | 4,026 | 155,532 | 38.6          | 5,293    | 1.0             |
| `_system` logs    | 4,093 | 539,536 | 131.8         | 41,091   | 1.0             |

Every file has exactly **one row group** — row-group pruning within a file is
moot on this population; **file-level pruning is the only pruning that exists**.
The benchmark's "row-group pruned %" metric collapses into "files pruned %".

## Footer overhead (raw footer-length scan, whole sample)

| table           | p50 file size | p50 footer | footer % @ p50 | footer % (files ≤16 KB) | footer % (files >128 KB) |
| --------------- | ------------- | ---------- | -------------- | ----------------------- | ------------------------ |
| jobradar traces | 11,553 B      | 8,563 B    | **74.1%**      | 74.1%                   | n/a (none)               |
| jobradar logs   | 9,107 B       | 6,758 B    | **74.0%**      | 74.0%                   | n/a (none)               |
| `_system` traces  | 17,304 B      | 8,928 B    | 51.4%          | 61.1%                   | 3.9%                     |
| `_system` logs    | 11,860 B      | 6,970 B    | 58.9%          | 59.5%                   | 3.0%                     |

At the realistic small-flush size, **~3/4 of every jobradar file is footer**.
Any layout change that widens the schema (per-type maps ×4, token index
columns, residue) grows the footer of every tiny file; the benchmark must price
that. Conversely, larger flush files amortize footers to ~3–4%.

## Physical schema observed (v2 names confirmed)

- traces: `trace_id/span_id/parent_span_id/span_name/service_name/
start_time_unix_nano/end_time_unix_nano/duration_nanos/span_kind/status_code/
status_message/is_root` + `span_attributes`, `resource_attributes`,
  `scope_attributes` all `Map<String,String>`; `events`/`links` are JSON-text
  varchar (100% empty `[]` in the jobradar sample); partition cols
  `date_day/hour/timestamp_hour` (hive-partition dirs `timestamp_hour=NNNNNN`).
- logs: `timestamp/observed_timestamp/trace_id/span_id/trace_flags/
severity_text/severity_number/service_name/body` + the three attr maps.
- **No `attr_tokens` column exists in any hive file, including the newest** —
  the derived-index idea is unreleased there. The live baseline has **zero
  pruning aids: ZSTD + dictionary encoding everywhere, no bloom filter on any
  column** (checked via parquet metadata: `bloom_filter_offset` null throughout).

## Attribute shape

Distinct keys are small: jobradar traces span=2, resource=1; jobradar logs=17;
`_system` traces span=40; `_system` logs log=66, resource=20. Cardinality is not the
problem on this deployment; typing is.

Observed stringified-value classes per key are almost perfectly clean
(single-class per key). Representative typed keys (100% single class):

- int: `jobradar.posting_id`, `num_completed_jobs`, `num_jobs_running`,
  `thread.id`, `code.line.number`, `busy_ns`, `idle_ns`, `entry_count`,
  `partition_count`, `data_offset`, `data_size`, `log.line`, `rows_returned`
- string: `jobradar.source`, `queue`, `target`, `code.file.path`, `tenant_id`,
  `dataset_id`, `signal_type`, `error`
- json (array/kvlist → residue candidates): `table_properties`,
  `partition_fields` (100% json-shaped)

**Conflicted keys: essentially absent.** The only natural mixed-type key in the
whole sample is `params` in `_system/_monitoring/logs` — n=4, classes
json+string. The benchmark's conflicted/off-type scenario must therefore be
**synthesized** (inject a controlled off-type fraction into a copy of the data);
`params` is nominated as the real-world specimen but is statistically useless.

## Implications for the 0.3 benchmark

1. Benchmark at TWO file-size regimes: as-is tiny files (1–3 rows!) and a
   compacted variant (coalesce the sample into ~64–128 MB files) — conclusions
   will differ radically because footers dominate the tiny regime.
2. Files-pruned % is the only pruning axis (1 row group/file).
3. Baseline has no blooms and no attr_tokens: today's cost = full scan of every
   file. Even modest warm-index pruning is a step change.
4. Typed-map benefit on this data is mostly cast-free retrieval + smaller
   values (ints stored as ints); pruning must come from the warm index or
   promotion — matching the design's prediction (~0 files pruned for the bare
   typed map).
5. Registry-lookup benchmark should use the realistic key sets above
   (2–66 keys/table, heavily repeated) — cache hit rate will be ~100% after
   warm-up; the interesting number is the per-attribute overhead at that hit
   rate, on 1–3-row batches (per-batch amortization is near zero!).
