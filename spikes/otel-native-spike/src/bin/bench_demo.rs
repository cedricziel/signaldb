//! Attribute-storage benchmark on real hive data — proof binary (spike 0.3).
//!
//! Reads a bounded, deterministic slice of real production parquet, injects the
//! two documented synthetic perturbations, materializes the four layout
//! variants at two file-size regimes, and prints:
//!
//! 1. a config header (seed, row budget, source files, inferred typing),
//! 2. storage + write cost per (variant × regime),
//! 3. seven query classes per (variant × regime) with wall time and pruning,
//! 4. residue fetch/decode cost,
//! 5. the write-path registry-lookup microbenchmark.
//!
//! Run (from `spikes/otel-native-spike/`, release is mandatory for timing):
//!   cargo run --release --bin bench_demo
//!
//! Env overrides: `SPIKE_DATA_DIR` (default `<repo>/.data/spike/hive`),
//! `BENCH_OUT` (default `$TMPDIR/otel-native-bench`), `BENCH_REPS` (default 3).

use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result};
use arrow::datatypes::Schema;

use otel_native_spike::bench::{
    self, COMPACTED, CONFLICTED_KEY, CONFLICTED_PROBE, Canon, Layout, PROMOTED_INT_COLUMN,
    PROMOTED_INT_KEY, PROMOTED_STR_COLUMN, PROMOTED_STR_KEY, RARE_INT_KEY, RARE_INT_VALUE,
    RARE_STR_VALUE, RARE_THREAD_ID, Regime, SEED, SMALL_FLUSH, ScanResult, SourceSpec, Variant,
    WriteStats,
};
use otel_native_spike::warm_index::{TokenColumn, encode_int_token, encode_str_token};

/// Range predicate threshold for query class 4 (unpruned scan).
const RANGE_THRESHOLD: i64 = 1_000_000;
/// Rows the time-slice retrieval (query class 7) should return.
const SLICE_ROWS: usize = 2_000;

struct Materialized {
    variant: Variant,
    regime: Regime,
    stats: WriteStats,
    paths: Vec<String>,
    schema: Schema,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let reps: usize = std::env::var("BENCH_REPS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1);
    let data_dir = std::env::var("SPIKE_DATA_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|_| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.data/spike/hive")
        });
    let out_dir = std::env::var("BENCH_OUT")
        .map(PathBuf::from)
        .unwrap_or_else(|_| std::env::temp_dir().join("otel-native-bench"));
    std::fs::create_dir_all(&out_dir)?;

    println!("== Attribute-storage benchmark on real hive data — spike 0.3 ==");
    println!("seed=0x{SEED:016X}  reps={reps} (median reported)");
    println!("source={}", data_dir.display());
    println!("out={}", out_dir.display());

    // ---- 1. load real rows ------------------------------------------------
    let t0 = Instant::now();
    // Row budgets are overridable only to shake the harness out on a tiny
    // slice; every reported run uses the constants.
    let env_rows = |name: &str, default: usize| -> usize {
        std::env::var(name)
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(default)
    };
    let (mut rows, trace_files) = bench::load_source(&SourceSpec {
        dir: data_dir.join("_system/_monitoring/traces"),
        record_attr_col: "span_attributes",
        name_col: "span_name",
        budget: env_rows("BENCH_TRACE_ROWS", bench::TRACE_ROWS),
    })
    .context("load traces")?;
    let (log_rows, log_files) = bench::load_source(&SourceSpec {
        dir: data_dir.join("_system/_monitoring/logs"),
        record_attr_col: "log_attributes",
        name_col: "body",
        budget: env_rows("BENCH_LOG_ROWS", bench::LOG_ROWS),
    })
    .context("load logs")?;
    let trace_row_count = rows.len();
    rows.extend(log_rows);
    let load_ms = t0.elapsed().as_secs_f64() * 1e3;

    anyhow::ensure!(
        !rows.is_empty(),
        "no rows loaded from {} — is SPIKE_DATA_DIR pointing at a fetched hive sample?",
        data_dir.display()
    );
    let attr_total: usize = rows.iter().map(|r| r.attrs.len()).sum();
    println!(
        "\nloaded {} rows in {load_ms:.0} ms  (traces {trace_row_count} from {trace_files} files, \
logs {} from {log_files} files)  attrs/row avg {:.1}",
        rows.len(),
        rows.len() - trace_row_count,
        attr_total as f64 / rows.len() as f64
    );

    // ---- 2. synthetic perturbations + typing ------------------------------
    let plant = bench::plant(&mut rows);
    let layout = Layout::from_rows(&rows);
    println!(
        "planted: conflicted key `{CONFLICTED_KEY}` int={} off-type-string={} (probe value {CONFLICTED_PROBE} \
matches {} typed rows); rare markers in {} rows",
        plant.conflicted_int_rows,
        plant.conflicted_off_type_rows,
        plant.conflicted_probe_matches,
        plant.planted_rows
    );
    println!(
        "inferred typing over {} distinct keys: str={} int={} double={} bool={} residue(json)={}",
        layout.types.len(),
        layout.count(Canon::Str),
        layout.count(Canon::Int),
        layout.count(Canon::Double),
        layout.count(Canon::Bool),
        layout.count(Canon::Residue),
    );
    let mixed: Vec<String> = layout
        .stats
        .iter()
        .filter(|(_, s)| s.off_type_fraction() > 0.0)
        .map(|(k, s)| format!("{k}({:.1}% off-type)", 100.0 * s.off_type_fraction()))
        .collect();
    println!(
        "keys with any off-type values: {}",
        if mixed.is_empty() {
            "none".to_string()
        } else {
            mixed.join(", ")
        }
    );
    println!(
        "planted-file ground truth: small-flush {:?}, compacted {:?}",
        bench::planted_files(SMALL_FLUSH),
        bench::planted_files(COMPACTED)
    );

    // Time slice for query class 7: exactly ~SLICE_ROWS rows.
    let mut sorted_ts: Vec<i64> = rows.iter().map(|r| r.ts_nanos).collect();
    sorted_ts.sort_unstable();
    let lo_idx = sorted_ts.len() / 2;
    let (slice_lo, slice_hi) = (
        sorted_ts[lo_idx],
        sorted_ts[(lo_idx + SLICE_ROWS).min(sorted_ts.len() - 1)],
    );

    // ---- 3. materialize ---------------------------------------------------
    println!("\n--- storage & write cost (ZSTD(1), 1 row group per file) ---");
    println!(
        "{:<28} {:<12} {:>6} {:>11} {:>9} {:>8} {:>8} {:>10} {:>9}",
        "variant",
        "regime",
        "files",
        "total MiB",
        "B/row",
        "footer%",
        "bloom%",
        "write ms",
        "ns/row"
    );
    let mut sets: Vec<Materialized> = Vec::new();
    for regime in [SMALL_FLUSH, COMPACTED] {
        for variant in Variant::ALL {
            let stats = bench::materialize(variant, regime, &rows, &layout, &out_dir, None, "")?;
            println!(
                "{:<28} {:<12} {:>6} {:>11.2} {:>9.1} {:>7.1}% {:>7.2}% {:>10.0} {:>9.0}",
                variant.label(),
                regime.name,
                stats.files,
                stats.total_bytes as f64 / (1024.0 * 1024.0),
                stats.bytes_per_row(),
                stats.footer_pct(),
                stats.bloom_pct(),
                stats.build_write_ms,
                stats.ns_per_row(),
            );
            let schema = bench::build_batch(variant, &rows[..1], 0, &layout)?
                .schema()
                .as_ref()
                .clone();
            sets.push(Materialized {
                variant,
                regime,
                paths: stats.path_strings(),
                stats,
                schema,
            });
        }
    }

    // ---- 4. queries -------------------------------------------------------
    let ctx = bench::session();
    // `BENCH_EXPLAIN=1` dumps EXPLAIN ANALYZE for the promoted-column query, so
    // the pruning metrics reported below can be checked against the plan.
    if std::env::var("BENCH_EXPLAIN").is_ok() {
        for set in sets.iter().filter(|s| s.variant == Variant::V2Promoted) {
            let _ = ctx.deregister_table("t");
            let df = ctx
                .read_parquet(
                    set.paths.clone(),
                    datafusion::prelude::ParquetReadOptions::default().schema(&set.schema),
                )
                .await?;
            ctx.register_table("t", df.into_view())?;
            let sql = format!(
                "EXPLAIN ANALYZE SELECT count(*) FROM t WHERE {PROMOTED_STR_COLUMN} = '{RARE_STR_VALUE}'"
            );
            println!("\n--- EXPLAIN ANALYZE ({}) ---", set.regime.name);
            ctx.sql(&sql).await?.show().await?;
        }
    }
    let str_token = encode_str_token(PROMOTED_STR_KEY, RARE_STR_VALUE);
    let int_token = encode_int_token(RARE_INT_KEY, RARE_INT_VALUE);
    let thread_token = encode_int_token(PROMOTED_INT_KEY, RARE_THREAD_ID);
    let conflicted_token = encode_int_token(CONFLICTED_KEY, CONFLICTED_PROBE);

    for regime in [SMALL_FLUSH, COMPACTED] {
        println!("\n=== queries — {} regime ===", regime.name);
        println!(
            "{:<40} {:<26} {:>9} {:>7} {:>8} {:>8} {:>8} {:>8} {:>7} {:>10} {:>13}",
            "query class",
            "variant",
            "median ms",
            "pre ms",
            "rows",
            "f-stat",
            "rg-stat",
            "rg-bloom",
            "opened",
            "MiB read",
            "files fed"
        );

        for (class, label) in QUERY_CLASSES {
            for set in sets.iter().filter(|s| s.regime.name == regime.name) {
                let Some(sql) = sql_for(class, set.variant, slice_lo, slice_hi) else {
                    continue;
                };
                // V3 runs the layer-4.3 warm-index pre-filter first; only the
                // survivors are handed to DataFusion.
                let (paths, prefilter_ms) = if set.variant == Variant::V3Warm {
                    match warm_probe(class) {
                        Some((column, token)) => {
                            let (kept, ms) = bench::prefilter(
                                &set.stats.paths,
                                column,
                                match token {
                                    Probe::Str => &str_token,
                                    Probe::Int => &int_token,
                                    Probe::Thread => &thread_token,
                                    Probe::Conflicted => &conflicted_token,
                                },
                                reps,
                            )?;
                            (kept, ms)
                        }
                        None => (set.paths.clone(), 0.0),
                    }
                } else {
                    (set.paths.clone(), 0.0)
                };

                let mut res: ScanResult =
                    bench::timed_query(&ctx, &paths, &set.schema, &sql, reps, set.stats.files)
                        .await?;
                res.prefilter_ms = prefilter_ms;
                res.files_scanned = paths.len();
                print_row(label, set.variant, &res);
            }
        }

        // ---- residue cost (typed variants only) --------------------------
        let v1 = sets
            .iter()
            .find(|s| s.regime.name == regime.name && s.variant == Variant::V1Typed)
            .expect("V1 materialized");
        let residue = bench::residue_cost(&ctx, &v1.paths, &v1.schema).await?;
        println!(
            "\n6 residue retrieval ({}): {} rows / {} entries / {:.2} MiB  fetch {:.0} ms  decode {:.1} ms \
({:.0} ns/row, {:.0} ns/entry)",
            regime.name,
            residue.rows,
            residue.entries,
            residue.bytes as f64 / (1024.0 * 1024.0),
            residue.fetch_ms,
            residue.decode_ms,
            residue.ns_per_row(),
            residue.ns_per_entry(),
        );
    }

    // ---- 4b. warm-index bloom sizing -------------------------------------
    //
    // parquet-rs resolves an unset bloom NDV to the row group's ROW count. A
    // containment index writes one token per *attribute*, so the default
    // under-sizes the bloom by the attributes-per-row factor and the index
    // saturates. Re-materialize V3 at the small-flush regime with an explicit
    // NDV and compare pruning selectivity for the same two probes.
    println!("\n=== warm-index bloom sizing (small-flush, ground truth: 2 carrying files) ===");
    let attrs_per_row = (attr_total as f64 / rows.len() as f64).ceil() as u64;
    let explicit_ndv = SMALL_FLUSH.rows_per_file as u64 * attrs_per_row;
    let ndv_stats = bench::materialize(
        Variant::V3Warm,
        SMALL_FLUSH,
        &rows,
        &layout,
        &out_dir,
        Some(explicit_ndv),
        "-ndv",
    )?;
    let default_v3 = sets
        .iter()
        .find(|s| s.regime.name == SMALL_FLUSH.name && s.variant == Variant::V3Warm)
        .expect("V3 materialized");
    println!(
        "{:<34} {:>12} {:>12} {:>11} {:>16} {:>16}",
        "bloom NDV", "bloom B/file", "total MiB", "write ms", "str files kept", "int files kept"
    );
    for (label, stats) in [
        (
            format!("default (= {} rows/rg)", SMALL_FLUSH.rows_per_file),
            &default_v3.stats,
        ),
        (format!("explicit ({explicit_ndv} tokens)"), &ndv_stats),
    ] {
        let (str_kept, _) = bench::prefilter(&stats.paths, TokenColumn::Str, &str_token, 1)?;
        let (int_kept, _) = bench::prefilter(&stats.paths, TokenColumn::Int, &int_token, 1)?;
        println!(
            "{:<34} {:>12.0} {:>12.2} {:>11.0} {:>16} {:>16}",
            label,
            stats.bloom_bytes as f64 / stats.files as f64,
            stats.total_bytes as f64 / (1024.0 * 1024.0),
            stats.build_write_ms,
            format!("{}/{}", str_kept.len(), stats.files),
            format!("{}/{}", int_kept.len(), stats.files),
        );
    }

    // ---- 5. registry microbenchmark ---------------------------------------
    println!("\n=== write-path registry lookup (no DataFusion) ===");
    let trace_keys: Vec<String> = bench::infer_types(&rows[..trace_row_count])
        .keys()
        .cloned()
        .collect();
    let log_keys: Vec<String> = bench::infer_types(&rows[trace_row_count..])
        .keys()
        .cloned()
        .collect();
    println!(
        "key sets: traces={} keys, logs={} keys (real hive shapes)",
        trace_keys.len(),
        log_keys.len()
    );
    println!(
        "{:<32} {:>10} {:>12} {:>14} {:>16}",
        "cache shape", "batch rows", "lookups", "ns/lookup", "lookups/sec"
    );
    for (keys, key_label) in [(&trace_keys, "traces"), (&log_keys, "logs")] {
        for (batch_rows, batches) in [(1usize, 200_000usize), (3, 66_666), (1000, 200)] {
            for s in bench::registry_bench(keys, batch_rows, batches) {
                println!(
                    "{:<32} {:>10} {:>12} {:>14.1} {:>16.0}",
                    format!("{key_label}: {}", s.shape),
                    s.batch_rows,
                    s.lookups,
                    s.ns_per_lookup,
                    s.lookups_per_sec,
                );
            }
        }
    }
    let cold = bench::registry_cold_miss(100_000);
    println!(
        "{:<32} {:>10} {:>12} {:>14.1} {:>16.0}",
        "cold miss (read+insert)", 1, cold.lookups, cold.ns_per_lookup, cold.lookups_per_sec
    );

    println!("\ndone. artifacts under {}", out_dir.display());
    Ok(())
}

// ---------------------------------------------------------------------------
// Query classes
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Class {
    StrEq,
    IntEq,
    PromotedEq,
    IntRange,
    ConflictedEq,
    Slice,
}

#[derive(Clone, Copy)]
enum Probe {
    Str,
    Int,
    Thread,
    Conflicted,
}

const QUERY_CLASSES: [(Class, &str); 6] = [
    (Class::StrEq, "1 str eq, unpromoted (target=rare)"),
    (Class::IntEq, "2 int eq, unpromoted (busy_ns=rare)"),
    (Class::PromotedEq, "3 eq on promoted key (thread.id)"),
    (Class::IntRange, "4 int range (full scan)"),
    (Class::ConflictedEq, "5 conflicted key, int form"),
    (Class::Slice, "7 time-slice full-row fetch"),
];

/// Which warm-index leaf the V3 pre-filter probes for a query class.
fn warm_probe(class: Class) -> Option<(TokenColumn, Probe)> {
    match class {
        Class::StrEq => Some((TokenColumn::Str, Probe::Str)),
        Class::IntEq => Some((TokenColumn::Int, Probe::Int)),
        Class::PromotedEq => Some((TokenColumn::Int, Probe::Thread)),
        Class::ConflictedEq => Some((TokenColumn::Int, Probe::Conflicted)),
        // Range and retrieval predicates are not containment probes.
        Class::IntRange | Class::Slice => None,
    }
}

fn sql_for(class: Class, variant: Variant, slice_lo: i64, slice_hi: i64) -> Option<String> {
    let legacy = variant == Variant::V0Legacy;
    let promoted = variant == Variant::V2Promoted;
    Some(match class {
        Class::StrEq => {
            if promoted {
                // The key is a promoted column in V2 — measured as class 3's
                // string counterpart instead of a map lookup.
                format!("SELECT count(*) FROM t WHERE {PROMOTED_STR_COLUMN} = '{RARE_STR_VALUE}'")
            } else if legacy {
                format!(
                    "SELECT count(*) FROM t WHERE attributes['{PROMOTED_STR_KEY}'] = '{RARE_STR_VALUE}'"
                )
            } else {
                format!(
                    "SELECT count(*) FROM t WHERE attributes_str['{PROMOTED_STR_KEY}'] = '{RARE_STR_VALUE}'"
                )
            }
        }
        Class::IntEq => {
            if legacy {
                format!(
                    "SELECT count(*) FROM t WHERE attributes['{RARE_INT_KEY}'] = '{RARE_INT_VALUE}'"
                )
            } else {
                format!(
                    "SELECT count(*) FROM t WHERE attributes_int['{RARE_INT_KEY}'] = {RARE_INT_VALUE}"
                )
            }
        }
        Class::PromotedEq => {
            if promoted {
                format!("SELECT count(*) FROM t WHERE {PROMOTED_INT_COLUMN} = {RARE_THREAD_ID}")
            } else if legacy {
                format!(
                    "SELECT count(*) FROM t WHERE attributes['{PROMOTED_INT_KEY}'] = '{RARE_THREAD_ID}'"
                )
            } else {
                format!(
                    "SELECT count(*) FROM t WHERE attributes_int['{PROMOTED_INT_KEY}'] = {RARE_THREAD_ID}"
                )
            }
        }
        Class::IntRange => {
            if legacy {
                // The legacy layout must cast every value out of the string map.
                format!(
                    "SELECT count(*) FROM t WHERE TRY_CAST(attributes['{RARE_INT_KEY}'] AS BIGINT) > {RANGE_THRESHOLD}"
                )
            } else {
                format!(
                    "SELECT count(*) FROM t WHERE attributes_int['{RARE_INT_KEY}'] > {RANGE_THRESHOLD}"
                )
            }
        }
        Class::ConflictedEq => {
            if legacy {
                format!(
                    "SELECT count(*) FROM t WHERE attributes['{CONFLICTED_KEY}'] = '{CONFLICTED_PROBE}'"
                )
            } else {
                format!(
                    "SELECT count(*) FROM t WHERE attributes_int['{CONFLICTED_KEY}'] = {CONFLICTED_PROBE}"
                )
            }
        }
        Class::Slice => {
            format!("SELECT * FROM t WHERE ts_nanos >= {slice_lo} AND ts_nanos <= {slice_hi}")
        }
    })
}

fn print_row(label: &str, variant: Variant, res: &ScanResult) {
    println!(
        "{:<40} {:<26} {:>9.1} {:>7.1} {:>8} {:>8} {:>8} {:>8} {:>7} {:>10.2} {:>13}",
        label,
        variant.label(),
        res.median_ms,
        res.prefilter_ms,
        res.rows,
        res.metrics.files_ranges_pruned_statistics,
        res.metrics.row_groups_pruned_statistics,
        res.metrics.row_groups_pruned_bloom_filter,
        res.metrics.files_opened,
        res.metrics.bytes_scanned as f64 / (1024.0 * 1024.0),
        format!(
            "{}/{} ({:.1}%)",
            res.files_scanned,
            res.files_total,
            res.files_pruned_pct()
        ),
    );
}
