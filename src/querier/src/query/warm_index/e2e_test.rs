//! End-to-end coverage of [`super::table::WarmIndexTable`] through a real
//! Parquet-backed `ListingTable` (not the full Iceberg/IR stack — DataFusion's
//! planner is what actually calls `TableProvider::scan`, so a `ListingTable`
//! exercises the identical code path IR/LogQL/TraceQL/PromQL go through,
//! without a real Iceberg catalog's setup cost).
#![cfg(test)]

use std::collections::HashMap;
use std::sync::Arc;

use common::schema::type_authority::CanonicalType;
use common::schema::typed_attributes::home_column;
use common::schema::{WARM_INDEX_ENCODING_PROPERTY, WARM_INDEX_ENCODING_VERSION};
use datafusion::arrow::array::Int64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::TableProvider;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::physical_plan::ParquetSource;
use datafusion::datasource::source::DataSourceExec;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::Expr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{SessionContext, ident, lit};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt};
use url::Url;

use super::test_support::{HomeRow, write_file_without_warm_index, write_home_file};
use super::{WarmIndexGate, WarmIndexTable};

/// A gate that always samples every file in these ≤10-file tests, so the
/// assertions below are never masked by the gate declining to probe at all.
fn eager_gate() -> WarmIndexGate {
    WarmIndexGate {
        min_files: 1,
        sample_files: 10,
        max_keep_ratio: 0.5,
        probe_concurrency: 4,
    }
}

fn known_properties() -> HashMap<String, String> {
    HashMap::from([(
        WARM_INDEX_ENCODING_PROPERTY.to_string(),
        WARM_INDEX_ENCODING_VERSION.to_string(),
    )])
}

/// Writes `files` (`f0.parquet`, ...) to a fresh in-memory store and returns
/// a `ListingTable` over them, its schema inferred from the real files.
async fn build_table(files: Vec<Vec<u8>>) -> (SessionContext, Arc<dyn TableProvider>) {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (i, bytes) in files.into_iter().enumerate() {
        store
            .put(&Path::from(format!("f{i}.parquet")), bytes.into())
            .await
            .unwrap();
    }

    let ctx = SessionContext::new();
    ctx.runtime_env()
        .register_object_store(&Url::parse("memory://").unwrap(), store);
    let url = ListingTableUrl::parse("memory:///").unwrap();
    let options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = options.infer_schema(&ctx.state(), &url).await.unwrap();
    let config = ListingTableConfig::new(url)
        .with_listing_options(options)
        .with_schema(schema);
    let table = Arc::new(ListingTable::try_new(config).unwrap()) as Arc<dyn TableProvider>;
    (ctx, table)
}

/// Sums surviving `PartitionedFile`s across every Parquet scan node under
/// `plan`.
fn count_parquet_files(plan: &Arc<dyn ExecutionPlan>) -> usize {
    if let Some(exec) = plan.downcast_ref::<DataSourceExec>()
        && let Some((config, _parquet)) = exec.downcast_to_file_source::<ParquetSource>()
    {
        return config.file_groups.iter().map(|group| group.len()).sum();
    }
    plan.children().into_iter().map(count_parquet_files).sum()
}

fn collect_ids(batches: &[RecordBatch]) -> Vec<i64> {
    let mut ids: Vec<i64> = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column_by_name("id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    ids.sort_unstable();
    ids
}

/// Runs `filter` against `table` through the standard DataFusion pipeline
/// (so an `Inexact` pushdown still gets a correctness-guaranteeing `Filter`
/// above the scan), returning the physical plan and the sorted `id`s of the
/// rows it produced.
async fn run_filtered(
    table: Arc<dyn TableProvider>,
    ctx: &SessionContext,
    filter: Expr,
) -> (Arc<dyn ExecutionPlan>, Vec<i64>) {
    let df = ctx.read_table(table).unwrap().filter(filter).unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let batches = df.collect().await.unwrap();
    (plan, collect_ids(&batches))
}

/// Builds a 10-file table (`f0..f9`, `id` = index) from `rows`, scans it
/// once through `WarmIndexTable::maybe_wrap` and once through the plain
/// provider, and asserts both the pruned file count and that the two scans
/// return identical rows — the "never a false negative" contract.
async fn assert_pruned_and_correct(
    rows: Vec<Vec<u8>>,
    filter: Expr,
    expected_files: usize,
    expected_ids: Vec<i64>,
) {
    let (ctx, table) = build_table(rows).await;
    let wrapped = WarmIndexTable::maybe_wrap(Arc::clone(&table), &known_properties(), eager_gate());
    let (wrapped_plan, wrapped_ids) = run_filtered(wrapped, &ctx, filter.clone()).await;
    let (_baseline_plan, baseline_ids) = run_filtered(table, &ctx, filter).await;

    assert_eq!(count_parquet_files(&wrapped_plan), expected_files);
    assert_eq!(wrapped_ids, expected_ids);
    assert_eq!(
        wrapped_ids, baseline_ids,
        "prefilter must return exactly the rows an unpruned scan would"
    );
}

fn int_files(home: &str, key: &str, values: impl IntoIterator<Item = i64>) -> Vec<Vec<u8>> {
    values
        .into_iter()
        .enumerate()
        .map(|(i, v)| write_home_file(i as i64, home, key, HomeRow::Int(v)))
        .collect()
}

#[tokio::test]
async fn int_equality_keeps_two_of_ten_files_and_matches_the_unpruned_result() {
    let home = home_column("span_attributes", CanonicalType::Int64);
    let rows = int_files(
        &home,
        "status",
        (0..10).map(|i| if i < 2 { 200 } else { 500 }),
    );
    let filter = get_field(ident(home), "status").eq(lit(200i64));

    assert_pruned_and_correct(rows, filter, 2, vec![0, 1]).await;
}

#[tokio::test]
async fn string_equality_keeps_two_of_ten_files_and_matches_the_unpruned_result() {
    let home = home_column("span_attributes", CanonicalType::String);
    let rows: Vec<Vec<u8>> = (0..10)
        .map(|i| {
            let value = if i < 2 {
                "a.example.com"
            } else {
                "b.example.com"
            };
            write_home_file(i, &home, "host", HomeRow::Str(value))
        })
        .collect();
    let filter = get_field(ident(home), "host").eq(lit("a.example.com"));

    assert_pruned_and_correct(rows, filter, 2, vec![0, 1]).await;
}

#[tokio::test]
async fn a_value_present_in_every_file_trips_the_gate_but_still_returns_correct_rows() {
    let home = home_column("span_attributes", CanonicalType::Int64);
    let rows = int_files(&home, "sampled", std::iter::repeat_n(1, 10));
    let filter = get_field(ident(home), "sampled").eq(lit(1i64));

    // Every file matches; the gate must keep every file, never dropping a
    // real hit for being "unselective".
    assert_pruned_and_correct(rows, filter, 10, (0..10).collect()).await;
}

#[tokio::test]
async fn a_range_predicate_is_not_probed_and_still_returns_correct_rows() {
    let home = home_column("span_attributes", CanonicalType::Int64);
    let rows = int_files(&home, "status", 0..10);
    let filter = get_field(ident(home), "status").gt(lit(5i64));

    // Unrecognized shape (a range): probe_clauses returns nothing, so no
    // file is pruned by the warm index at all.
    assert_pruned_and_correct(rows, filter, 10, vec![6, 7, 8, 9]).await;
}

#[tokio::test]
async fn a_not_equal_predicate_is_not_probed_and_still_returns_correct_rows() {
    let home = home_column("span_attributes", CanonicalType::Int64);
    let rows = int_files(&home, "status", 0..10);
    let filter = get_field(ident(home), "status").not_eq(lit(3i64));

    assert_pruned_and_correct(rows, filter, 10, vec![0, 1, 2, 4, 5, 6, 7, 8, 9]).await;
}

#[tokio::test]
async fn a_table_without_the_warm_index_column_is_not_wrapped_end_to_end() {
    let files: Vec<Vec<u8>> = (0..3).map(|_| write_file_without_warm_index()).collect();
    let (_ctx, table) = build_table(files).await;

    let wrapped = WarmIndexTable::maybe_wrap(Arc::clone(&table), &known_properties(), eager_gate());

    assert!(
        Arc::ptr_eq(&table, &wrapped),
        "a table with no attr_index column must be returned untouched"
    );
}
