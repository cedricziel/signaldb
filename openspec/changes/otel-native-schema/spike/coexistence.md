# Spike 0.2 — typed-layout + promotion-evolution viability

Prove the pinned datafusion-iceberg provider (cedricziel/iceberg-rust rev
98b8f88) can serve the **typed layout** (per-type attribute maps + binary
residue) and **field-id promotion evolution** under one table scan. Scope note:
this task originally proved legacy `Map<String,String>` coexistence too; that
was dropped mid-spike by user decision — the typed layout replaces the legacy
layout in a one-shot breaking cutover (see proposal/design).

- Code: `spikes/otel-native-spike/src/coexistence.rs`,
  `src/bin/coexistence_demo.rs` (PASS/FAIL per probe; non-zero exit on failure),
  `src/bin/residue_probe.rs` (diagnostic for the one failure).
- Run: `cd spikes/otel-native-spike && cargo run --release --bin coexistence_demo`
  (`SPIKE_KEEP_TMP=1` keeps the written files for inspection).

## Setup

Temp SQLite catalog (`iceberg-sql-catalog`) + local filesystem object store —
the same catalog/write machinery the product uses (`write_parquet_partitioned`,
snapshot commit per generation). Table schema — id/timestamp columns plus:

- `attributes_str: Map<String,Utf8>`
- `attributes_int: Map<String,Int64>`
- `attributes_double: Map<String,Float64>`
- `attributes_bool: Map<String,Boolean>`
- `attributes_residue: Map<String,Binary>` (CBOR-encoded off-type values)

Two generations: gen-1 written pre-promotion; then schema evolution ADDs a
promoted column `attr_http_response_status_code: Int64`; gen-2 written carrying
it.

## Probe results — 10/11 PASS

| probe                                                                                                                                                                            | result               |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| typed_layout_write (per-type maps + Binary residue map written & committed)                                                                                                      | PASS                 |
| typed_maps_roundtrip (all four per-type maps + residue return as Arrow Map columns)                                                                                              | PASS                 |
| map_element_access (`attributes_int['http.response.status_code']` → 200/500 typed)                                                                                               | PASS                 |
| typed_map_predicate (`WHERE attributes_int['…'] = 200` filters correctly)                                                                                                        | PASS                 |
| residue_cbor_roundtrip                                                                                                                                                           | **FAIL — see below** |
| promotion_evolution (AddSchema+SetCurrentSchema; new field id **19** continues past tree max 18 — true evolution, not create-time `max(id)+1`)                                   | PASS                 |
| post_promotion_write (gen-2 files carry the promoted Int64 column)                                                                                                               | PASS                 |
| promotion_single_scan_null_fill (ONE scan over both generations: gen-1 rows null-fill the promoted column, gen-2 rows serve it, no error)                                        | PASS                 |
| field_id_projection_not_positional (pre-promotion file still reads its maps correctly after the column count changed → provider maps parquet **by field-id**, not name/position) | PASS                 |
| promoted_column_predicate (predicate on the promoted primitive across both generations)                                                                                          | PASS                 |
| demotion_evolution (column dropped via evolution; both generations still scan; files carrying the dropped field read fine)                                                       | PASS                 |

## The one failure — Binary-valued map content nulls through the provider

`attributes_residue: Map<String,Binary>`:

- **Write path is fine.** The written parquet contains the exact CBOR bytes
  (verified with duckdb: `{http.response.status_code=\xA2dtype fstring evalue bOK}`).
- **Plain DataFusion parquet read is fine.** `residue_probe` registers the same
  file with `register_parquet` and gets the full value:
  `attributes_residue['…']` → `Binary` with the exact CBOR
  (`a2647479706566737472696e676576616c7565624f4b`), `map_extract` works too.
- **Through the datafusion_iceberg provider the same map reads as NULL** (whole
  column, all rows), so `WHERE attributes_residue IS NOT NULL` returns zero rows.
- Likely mechanism (from the fork's source, `datafusion_iceberg/src/table/mod.rs`):
  `apply_arrow_field_id_overrides` reshapes `PARQUET:field_id` metadata for
  **top-level fields only**; the map's nested key/value field-ids (17/18 here)
  are not reshaped. Int/str/double/bool maps survive regardless — the failure is
  specific to Binary-valued map content. Not fully root-caused; time-boxed.

**Mitigations (pick in layer 4.2, both viable):**

1. Fix the provider in the fork (we own the pin) so nested Binary map values
   project correctly — then `Map<String,Binary>` stands.
2. Make the residue a **top-level `Binary` column** (one self-describing CBOR
   map per row holding all residue entries). Top-level primitive columns
   demonstrably work through the provider (the promoted Int64 column does);
   this also shrinks per-file schema width. Must be re-verified for Binary
   specifically at layer 4.2 start.

## Verdict

**The typed layout and promotion lifecycle are viable through the pinned
provider today**: per-type maps round-trip with typed access and predicates;
promotion/demotion work via genuine Iceberg field-id evolution; one scan spans
pre-/post-promotion generations with null-fill and field-id (not positional)
projection — exactly the guarantees layers 4 and 6 need. The single caveat is
the Binary-valued residue map, which is a provider read bug (data is intact on
disk, plain parquet reads serve it) with two clean mitigations. No blocker for
committing to the layout; the residue's physical shape (map vs top-level
column) is a layer-4.2 decision gated on mitigation choice.
