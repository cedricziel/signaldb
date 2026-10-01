# Spike 0.2 — typed-layout + promotion-evolution viability

Prove the pinned datafusion-iceberg provider (cedricziel/iceberg-rust rev
46c41af) can serve the **typed layout** (per-type attribute maps + binary
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
- `attributes_residue: Binary` (one CBOR document per row containing off-type values)

Two generations: gen-1 written pre-promotion; then schema evolution ADDs a
promoted column `attr_http_response_status_code: Int64`; gen-2 written carrying
it.

## Probe results — 11/11 PASS

| probe                                                                                                                                                                            | result               |
| -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------- |
| typed_layout_write (per-type maps + top-level Binary residue written & committed)                                                                                                | PASS                 |
| typed_maps_roundtrip (all four per-type maps + top-level Binary residue return with their Arrow types)                                                                           | PASS                 |
| map_element_access (`attributes_int['http.response.status_code']` → 200/500 typed)                                                                                               | PASS                 |
| typed_map_predicate (`WHERE attributes_int['…'] = 200` filters correctly)                                                                                                        | PASS                 |
| residue_cbor_roundtrip (top-level Binary CBOR document decodes through the provider)                                                                                             | PASS                 |
| promotion_evolution (AddSchema+SetCurrentSchema; new field id **17** continues past tree max 16 — true evolution, not create-time `max(id)+1`)                                   | PASS                 |
| post_promotion_write (gen-2 files carry the promoted Int64 column)                                                                                                               | PASS                 |
| promotion_single_scan_null_fill (ONE scan over both generations: gen-1 rows null-fill the promoted column, gen-2 rows serve it, no error)                                        | PASS                 |
| field_id_projection_not_positional (pre-promotion file still reads its maps correctly after the column count changed → provider maps parquet **by field-id**, not name/position) | PASS                 |
| promoted_column_predicate (predicate on the promoted primitive across both generations)                                                                                          | PASS                 |
| demotion_evolution (column dropped via evolution; both generations still scan; files carrying the dropped field read fine)                                                       | PASS                 |

## Residue representation

`attributes_residue` is a top-level `Binary` column containing one CBOR document
per row. The document maps each off-type attribute key to its typed value. The
provider returns the binary column intact and the probe decodes
`http.response.status_code: { type: "string", value: "OK" }` through the
Iceberg scan. This avoids the provider's nested `Map<String,Binary>` projection
limitation while preserving multiple residue entries per row.

## Verdict

**The typed layout and promotion lifecycle are viable through the pinned
provider today**: per-type maps round-trip with typed access and predicates;
promotion/demotion work via genuine Iceberg field-id evolution; one scan spans
pre-/post-promotion generations with null-fill and field-id (not positional)
projection — exactly the guarantees layers 4 and 6 need. The top-level Binary
residue also round-trips through the provider, so no provider workaround remains
for the selected representation. No blocker for committing to the layout; layer
4.2 can use the validated top-level Binary CBOR representation.
