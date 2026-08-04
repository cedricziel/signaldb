//! Warm typed containment index — proof binary (spike task 0.1).
//!
//! Generates N Parquet files where only a small fraction contain a target
//! `key = value` pair (one string attr, one int attr), then demonstrates:
//!
//! 1. NATIVE DataFusion: an `array_has(attr_tok_<type>, <typed literal>)`
//!    predicate does NOT skip row groups/files via the list-leaf bloom
//!    (`row_groups_pruned_bloom_filter == 0`). Filter pushdown only prunes
//!    *rows* late, never row groups or files.
//! 2. NEGATIVE CONTROL: an equality predicate over the bare typed MAP prunes
//!    nothing either (~0 row groups), and the map leaf carries no bloom at all.
//! 3. CUSTOM PRE-FILTER: reading footers + probing the leaf bloom ourselves
//!    (what a layer-4.3 TableProvider hook would do) prunes the overwhelming
//!    majority of files and row groups — with zero false negatives.
//!
//! Run (from `spikes/otel-native-spike/`, release preferred for timing):
//!   cargo run --release --bin warm_index_demo

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use datafusion::common::ScalarValue;
use datafusion::functions_nested::expr_fn::array_has;
use datafusion::physical_plan::metrics::MetricsSet;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col, lit};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use otel_native_spike::warm_index::{self, Row, TokenColumn, encode_int_token, encode_str_token};

// ---- Dataset shape -------------------------------------------------------

const N_FILES: usize = 40;
const ROWS_PER_FILE: usize = 8_000;
const ROW_GROUP_SIZE: usize = 2_000; // → 4 row groups per file
const BLOOM_FPP: f64 = 0.01;

const STR_KEY: &str = "http.request.host";
const INT_KEY: &str = "http.response.status_code";
const STR_TARGET: &str = "rare.internal.svc.local";
const INT_TARGET: i64 = 418;

/// Common (non-target) values, so target tokens are genuinely rare.
const COMMON_HOSTS: &[&str] = &[
    "svc-a.example.com",
    "svc-b.example.com",
    "svc-c.example.com",
    "svc-d.example.com",
    "api.example.com",
    "web.example.com",
];
const COMMON_STATUS: &[i64] = &[200, 201, 204, 301, 302, 400, 404, 500];

/// Files that carry the string target / int target, and in which row group.
fn is_str_target_file(idx: usize) -> bool {
    idx % 10 == 3
}
fn is_int_target_file(idx: usize) -> bool {
    idx % 9 == 5
}
fn str_target_rg(idx: usize) -> usize {
    idx % (ROWS_PER_FILE / ROW_GROUP_SIZE)
}
fn int_target_rg(idx: usize) -> usize {
    (idx + 1) % (ROWS_PER_FILE / ROW_GROUP_SIZE)
}

fn gen_file_rows(idx: usize, rng: &mut StdRng) -> Vec<Row> {
    let rgs = ROWS_PER_FILE / ROW_GROUP_SIZE;
    let str_rg = if is_str_target_file(idx) {
        Some(str_target_rg(idx))
    } else {
        None
    };
    let int_rg = if is_int_target_file(idx) {
        Some(int_target_rg(idx))
    } else {
        None
    };

    (0..ROWS_PER_FILE)
        .map(|r| {
            let rg = r / ROW_GROUP_SIZE;
            let _ = rgs;
            // Target rows: only the first few rows of the target row group.
            let host = if str_rg == Some(rg) && (r % ROW_GROUP_SIZE) < 5 {
                STR_TARGET.to_string()
            } else {
                COMMON_HOSTS[rng.random_range(0..COMMON_HOSTS.len())].to_string()
            };
            let status = if int_rg == Some(rg) && (r % ROW_GROUP_SIZE) < 5 {
                INT_TARGET
            } else {
                COMMON_STATUS[rng.random_range(0..COMMON_STATUS.len())]
            };
            // Plus some noise attributes so blooms hold realistic cardinality.
            Row {
                id: (idx * ROWS_PER_FILE + r) as i64,
                str_attrs: vec![
                    (STR_KEY.to_string(), host),
                    (
                        "http.route".to_string(),
                        format!("/v1/resource/{}", rng.random_range(0..64)),
                    ),
                ],
                int_attrs: vec![
                    (INT_KEY.to_string(), status),
                    (
                        "http.request.body_size".to_string(),
                        rng.random_range(0..4096),
                    ),
                ],
            }
        })
        .collect()
}

// ---- DataFusion metric harness ------------------------------------------

#[derive(Default, Debug, Clone, Copy)]
struct PruneMetrics {
    row_groups_pruned_statistics: usize,
    row_groups_pruned_bloom_filter: usize,
    row_groups_matched_bloom_filter: usize,
    files_ranges_pruned_statistics: usize,
    pushdown_rows_pruned: usize,
    pushdown_rows_matched: usize,
    output_rows: usize,
}

fn walk_metrics(plan: &Arc<dyn ExecutionPlan>, acc: &mut PruneMetrics) {
    if let Some(set) = plan.metrics() {
        let m: MetricsSet = set.aggregate_by_name();
        let get = |name: &str| m.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
        acc.row_groups_pruned_statistics += get("row_groups_pruned_statistics");
        acc.row_groups_pruned_bloom_filter += get("row_groups_pruned_bloom_filter");
        acc.row_groups_matched_bloom_filter += get("row_groups_matched_bloom_filter");
        acc.files_ranges_pruned_statistics += get("files_ranges_pruned_statistics");
        acc.pushdown_rows_pruned += get("pushdown_rows_pruned");
        acc.pushdown_rows_matched += get("pushdown_rows_matched");
        acc.output_rows += get("output_rows");
    }
    for child in plan.children() {
        walk_metrics(child, acc);
    }
}

async fn run_native(
    ctx: &SessionContext,
    dir: &Path,
    predicate: datafusion::prelude::Expr,
) -> Result<(PruneMetrics, usize)> {
    let df = ctx
        .read_parquet(
            dir.to_string_lossy().to_string(),
            ParquetReadOptions::default(),
        )
        .await?;
    let plan = df.filter(predicate)?.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let matched: usize = batches.iter().map(|b| b.num_rows()).sum();
    let mut m = PruneMetrics::default();
    walk_metrics(&plan, &mut m);
    Ok((m, matched))
}

// ---- Custom bloom pre-filter --------------------------------------------

#[derive(Default, Debug)]
struct CustomResult {
    files_total: usize,
    files_kept: usize,
    row_groups_total: usize,
    row_groups_kept: usize,
    bloom_bytes: u64,
    file_bytes: u64,
    elapsed_ms: f64,
    false_negatives: usize,
}

fn run_custom(
    files: &[(PathBuf, Vec<Row>)],
    column: TokenColumn,
    token: &[u8],
    key: &str,
    str_val: &str,
    int_val: i64,
) -> Result<CustomResult> {
    let start = Instant::now();
    let mut res = CustomResult {
        files_total: files.len(),
        ..Default::default()
    };
    for (path, rows) in files {
        let r = warm_index::prefilter_file(path, column, token)?;
        res.row_groups_total += r.row_groups_total;
        res.row_groups_kept += r.row_groups_kept;
        res.bloom_bytes += r.bloom_bytes;
        res.file_bytes += r.file_bytes;
        if r.file_kept {
            res.files_kept += 1;
        }
        // Correctness: a file that truly contains the pair must be kept.
        let truth = warm_index::file_contains(rows, column, key, str_val, int_val);
        if truth && !r.file_kept {
            res.false_negatives += 1;
        }
    }
    res.elapsed_ms = start.elapsed().as_secs_f64() * 1e3;
    Ok(res)
}

fn pct(part: usize, whole: usize) -> f64 {
    if whole == 0 {
        0.0
    } else {
        100.0 * (whole - part) as f64 / whole as f64
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let seed: u64 = 0x0741_5445_4C4E_4154; // deterministic
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = tempfile::tempdir()?;
    println!("== Warm typed containment index — spike 0.1 ==");
    println!(
        "files={N_FILES} rows/file={ROWS_PER_FILE} rg_size={ROW_GROUP_SIZE} (\
{} rg/file) bloom_fpp={BLOOM_FPP}",
        ROWS_PER_FILE / ROW_GROUP_SIZE
    );

    // --- generate ---------------------------------------------------------
    let mut files: Vec<(PathBuf, Vec<Row>)> = Vec::with_capacity(N_FILES);
    let gen_start = Instant::now();
    for idx in 0..N_FILES {
        let rows = gen_file_rows(idx, &mut rng);
        let path = dir.path().join(format!("part-{idx:03}.parquet"));
        warm_index::write_file(&path, &rows, ROW_GROUP_SIZE, BLOOM_FPP)?;
        files.push((path, rows));
    }
    let gen_ms = gen_start.elapsed().as_secs_f64() * 1e3;

    let str_target_files: Vec<usize> = (0..N_FILES).filter(|&i| is_str_target_file(i)).collect();
    let int_target_files: Vec<usize> = (0..N_FILES).filter(|&i| is_int_target_file(i)).collect();
    println!("generated in {gen_ms:.0} ms");
    println!(
        "string target ({STR_KEY}={STR_TARGET}) in files {str_target_files:?}  ({}/{N_FILES})",
        str_target_files.len()
    );
    println!(
        "int    target ({INT_KEY}={INT_TARGET}) in files {int_target_files:?}  ({}/{N_FILES})\n",
        int_target_files.len()
    );

    let str_token = encode_str_token(STR_KEY, STR_TARGET);
    let int_token = encode_int_token(INT_KEY, INT_TARGET);

    // --- native DataFusion ------------------------------------------------
    let mut cfg = SessionConfig::new();
    {
        let opts = cfg.options_mut();
        opts.execution.parquet.bloom_filter_on_read = true;
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
    }
    let ctx = SessionContext::new_with_config(cfg);

    let (nat_int, nat_int_rows) = run_native(
        &ctx,
        dir.path(),
        array_has(
            col("attr_tok_int"),
            lit(ScalarValue::Binary(Some(int_token.clone()))),
        ),
    )
    .await?;
    let (nat_str, nat_str_rows) = run_native(
        &ctx,
        dir.path(),
        array_has(
            col("attr_tok_str"),
            lit(ScalarValue::Binary(Some(str_token.clone()))),
        ),
    )
    .await?;

    // --- negative control: bare typed MAP ---------------------------------
    // Register the dir as a table and filter on the map value directly.
    ctx.register_parquet(
        "t",
        dir.path().to_string_lossy().to_string(),
        ParquetReadOptions::default(),
    )
    .await?;
    let map_ctl = match ctx
        .sql(&format!(
            "SELECT count(*) FROM t WHERE attr_int_map['{INT_KEY}'] = {INT_TARGET}"
        ))
        .await
    {
        Ok(df) => {
            let plan = df.create_physical_plan().await?;
            let _ = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            let mut m = PruneMetrics::default();
            walk_metrics(&plan, &mut m);
            Some(m)
        }
        Err(e) => {
            println!("(map[] predicate not expressible in DF SQL: {e})");
            None
        }
    };

    // --- custom pre-filter ------------------------------------------------
    let cust_int = run_custom(
        &files,
        TokenColumn::Int,
        &int_token,
        INT_KEY,
        "",
        INT_TARGET,
    )?;
    let cust_str = run_custom(&files, TokenColumn::Str, &str_token, STR_KEY, STR_TARGET, 0)?;

    let total_rgs = cust_int.row_groups_total;
    let total_file_bytes = cust_int.file_bytes;

    // --- report -----------------------------------------------------------
    println!("--- NATIVE DataFusion: array_has(tok, typed-literal) ---");
    println!("(bloom_filter_on_read=true, pushdown_filters=true)");
    print_native("int  http.response.status_code=418", &nat_int, nat_int_rows);
    print_native("str  http.request.host=rare...", &nat_str, nat_str_rows);

    println!("\n--- NEGATIVE CONTROL: bare typed MAP  attr_int_map['..']=418 ---");
    match map_ctl {
        Some(m) => println!(
            "row_groups_pruned_bloom={}  row_groups_pruned_stats={}  files_pruned_stats={}  pushdown_rows_pruned={}",
            m.row_groups_pruned_bloom_filter,
            m.row_groups_pruned_statistics,
            m.files_ranges_pruned_statistics,
            m.pushdown_rows_pruned
        ),
        None => {
            println!("map leaf carries NO bloom and no per-key stats → cannot prune (structural)")
        }
    }

    println!("\n--- CUSTOM footer+bloom pre-filter (layer 4.3 hook) ---");
    print_custom("int  status_code=418", &cust_int, total_rgs);
    print_custom("str  host=rare...", &cust_str, total_rgs);

    let overhead_pct = 100.0 * cust_int.bloom_bytes as f64 / total_file_bytes as f64;
    println!("\n--- overhead ---");
    println!(
        "total on-disk: {:.2} MiB across {N_FILES} files ({} row groups)",
        total_file_bytes as f64 / (1024.0 * 1024.0),
        total_rgs
    );
    println!(
        "int-index bloom bytes: {} ({:.3} % of file bytes)   str-index bloom bytes: {}",
        cust_int.bloom_bytes, overhead_pct, cust_str.bloom_bytes
    );
    println!(
        "both-index bloom overhead: {:.3} % of total file size",
        100.0 * (cust_int.bloom_bytes + cust_str.bloom_bytes) as f64 / total_file_bytes as f64
    );

    println!("\n--- verdict ---");
    println!(
        "native row-group bloom pruning: int={} str={}  (0 == not exploited natively)",
        nat_int.row_groups_pruned_bloom_filter, nat_str.row_groups_pruned_bloom_filter
    );
    println!(
        "custom pre-filter files pruned: int={:.1}% str={:.1}%   row groups pruned: int={:.1}% str={:.1}%",
        pct(cust_int.files_kept, cust_int.files_total),
        pct(cust_str.files_kept, cust_str.files_total),
        pct(cust_int.row_groups_kept, cust_int.row_groups_total),
        pct(cust_str.row_groups_kept, cust_str.row_groups_total),
    );
    println!(
        "false negatives: int={} str={}  (MUST be 0)",
        cust_int.false_negatives, cust_str.false_negatives
    );
    Ok(())
}

fn print_native(label: &str, m: &PruneMetrics, matched_rows: usize) {
    println!(
        "{label:34}  rg_pruned_bloom={:<3} rg_pruned_stats={:<3} files_pruned={:<3} rows_pushdown_pruned={:<7} matched_rows={}",
        m.row_groups_pruned_bloom_filter,
        m.row_groups_pruned_statistics,
        m.files_ranges_pruned_statistics,
        m.pushdown_rows_pruned,
        matched_rows,
    );
}

fn print_custom(label: &str, r: &CustomResult, total_rgs: usize) {
    println!(
        "{label:22}  files {:>2}/{:<2} kept ({:.1}% pruned)   row groups {:>3}/{:<3} kept ({:.1}% pruned)   {:.1} ms",
        r.files_kept,
        r.files_total,
        pct(r.files_kept, r.files_total),
        r.row_groups_kept,
        total_rgs,
        pct(r.row_groups_kept, r.row_groups_total),
        r.elapsed_ms,
    );
}
