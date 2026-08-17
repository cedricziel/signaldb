//! Ordered queries stay correct over any mix of attested and unattested files.
//!
//! This is the permanent regression test for the risk the whole
//! `declared-data-ordering` contract is shaped around: when a scan declares an
//! ordering, DataFusion may drop a sort it believes is redundant. If the
//! declaration is not honest for every file relied upon, the query silently
//! returns mis-ordered rows — a wrong answer, not a slow one. So the tests
//! here check results against an engine-independent expectation, and check the
//! plan shape that produced them.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use datafusion::arrow::array::RecordBatch;
use datafusion::physical_plan::displayable;
use datafusion::prelude::SessionContext;
use futures::stream;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use object_store::memory::InMemory;
use std::sync::Arc;
use tests_integration::compaction_helpers::{
    aligned_hour_start, attested_sort_order_ids, context_for, load_table, trace_sort_keys,
};
use tests_integration::generators;
use writer::IcebergTableWriter;

const DATASET: &str = "order-dataset";
const TABLE: &str = "traces";
const ORDER_BY_LIMIT: &str = "SELECT timestamp, trace_id FROM traces \
                              ORDER BY timestamp DESC, trace_id DESC LIMIT 10";

/// One row's declared sort key, as it is compared for ordering.
type Key = (i64, String);

struct Fixture {
    catalog_manager: Arc<CatalogManager>,
    tenant: String,
}

impl Fixture {
    /// A traces table seeded with one attested ingest file per entry of
    /// `timestamps`, each holding `ids_per_file` spans stamped at that
    /// instant. Distinct timestamps give the files non-overlapping key
    /// ranges, which is what lets a fully attested scan drop its sort.
    async fn seeded(tenant: &str, timestamps: &[i64], ids_per_file: usize) -> Result<Self> {
        let config = common::testing::TestConfigBuilder::new()
            .in_memory()
            .with_tenant(tenant, DATASET)
            .build();
        let catalog_manager = Arc::new(CatalogManager::new(config).await?);
        let mut writer = IcebergTableWriter::new(
            &catalog_manager,
            Arc::new(InMemory::new()),
            tenant.to_string(),
            DATASET.to_string(),
            TABLE.to_string(),
        )
        .await?;

        for (file, timestamp) in timestamps.iter().enumerate() {
            // Ids descend within the file, so the writer's sort has real work
            // to do and a file that skipped it would be visibly wrong.
            let ids: Vec<String> = (0..ids_per_file)
                .rev()
                .map(|row| format!("trace-{file}-{row:03}"))
                .collect();
            generators::generate_trace_files_with_ids(&mut writer, &[ids], *timestamp).await?;
        }

        Ok(Self {
            catalog_manager,
            tenant: tenant.to_string(),
        })
    }

    async fn table(&self) -> Result<iceberg_rust::table::Table> {
        load_table(&self.catalog_manager, &self.tenant, DATASET, TABLE).await
    }

    async fn context(&self, split_file_groups_by_statistics: bool) -> Result<SessionContext> {
        context_for(TABLE, self.table().await?, split_file_groups_by_statistics)
    }

    /// Copy the newest rows already in the table back into it through the
    /// unattested write path — the shape a build predating the ordering
    /// contract left behind. The copy overlaps the attested files' key range
    /// exactly, so it lands inside any top-n window they answer.
    async fn add_legacy_copy_of_newest_rows(&self) -> Result<()> {
        let ctx = self.context(false).await?;
        let newest = ctx
            .sql(&format!(
                "SELECT * FROM {TABLE} ORDER BY timestamp DESC, trace_id DESC LIMIT 5"
            ))
            .await?
            .collect()
            .await?
            .into_iter()
            .find(|batch: &RecordBatch| batch.num_rows() > 0)
            .context("the seeded table must have rows to copy")?;

        let mut table = self.table().await?;
        let files = write_parquet_partitioned(&table, stream::iter(vec![Ok(newest)]), None).await?;
        table
            .new_transaction(None)
            .append_data(files)
            .commit()
            .await?;
        Ok(())
    }

    /// Every row's key, read without asking for any order — the raw
    /// population, from which the true answer to an ordered query is derived
    /// in plain Rust rather than by the engine under test.
    async fn all_keys(&self) -> Result<Vec<Key>> {
        self.keys_of(
            &self.context(false).await?,
            "SELECT timestamp, trace_id FROM traces",
        )
        .await
    }

    async fn keys_of(&self, ctx: &SessionContext, sql: &str) -> Result<Vec<Key>> {
        trace_sort_keys(&ctx.sql(sql).await?.collect().await?)
    }

    async fn plan_of(&self, ctx: &SessionContext, sql: &str) -> Result<String> {
        let plan = ctx.sql(sql).await?.create_physical_plan().await?;
        Ok(displayable(plan.as_ref()).indent(true).to_string())
    }

    /// The query answers with the true top n, whether or not the engine is
    /// allowed to regroup files by their statistics.
    async fn assert_top_n_is_exact(&self, n: usize) -> Result<()> {
        let mut expected = self.all_keys().await?;
        expected.sort_by(|a, b| b.cmp(a));
        expected.truncate(n);

        for split in [true, false] {
            let ctx = self.context(split).await?;
            assert_eq!(
                self.keys_of(&ctx, ORDER_BY_LIMIT).await?,
                expected,
                "top-{n} must be the true top-{n} (split_file_groups_by_statistics={split})"
            );
        }
        Ok(())
    }
}

/// Four attested files at distinct instants inside one hour partition.
fn timestamps() -> Vec<i64> {
    let base = aligned_hour_start(2);
    (0..4).map(|i| base + i * 60_000).collect()
}

#[tokio::test]
async fn a_mixed_table_answers_order_by_limit_with_the_true_top_n() -> Result<()> {
    tests_integration::init_test_logging();
    let fixture = Fixture::seeded("mixed-tenant", &timestamps(), 8).await?;
    fixture.add_legacy_copy_of_newest_rows().await?;

    // The premise: the table really does hold both kinds of file, and the
    // unattested one overlaps the range the limit reads from.
    let attestations = attested_sort_order_ids(&fixture.table().await?).await?;
    assert!(
        attestations.values().any(|id| id.is_none())
            && attestations.values().any(|id| *id == Some(1)),
        "the table must hold both attested and unattested files: {attestations:?}"
    );

    fixture.assert_top_n_is_exact(10).await
}

#[tokio::test]
async fn a_fully_attested_table_answers_order_by_limit_with_the_true_top_n() -> Result<()> {
    // The optimized side of the same guarantee: when every file is attested
    // and the engine is free to elide sorts, the answer must still be exact.
    tests_integration::init_test_logging();
    let fixture = Fixture::seeded("attested-tenant", &timestamps(), 8).await?;

    fixture.assert_top_n_is_exact(10).await
}

#[tokio::test]
async fn the_plan_elides_the_sort_only_when_every_file_is_attested() -> Result<()> {
    tests_integration::init_test_logging();
    let sql = format!("SELECT timestamp, trace_id FROM {TABLE} ORDER BY timestamp ASC");

    // Fully attested, non-overlapping files: the scan declares the order it
    // was written in, and the redundant sort disappears.
    let attested = Fixture::seeded("plan-attested-tenant", &timestamps(), 8).await?;
    let ctx = attested.context(true).await?;
    let plan = attested.plan_of(&ctx, &sql).await?;
    assert!(
        plan.contains("output_ordering="),
        "an attested scan must declare its ordering:\n{plan}"
    );
    assert!(
        !plan.contains("SortExec"),
        "an attested, non-overlapping scan must not re-sort:\n{plan}"
    );

    // One unattested file is enough to withdraw the claim for the range.
    let mixed = Fixture::seeded("plan-mixed-tenant", &timestamps(), 8).await?;
    mixed.add_legacy_copy_of_newest_rows().await?;
    let ctx = mixed.context(true).await?;
    let plan = mixed.plan_of(&ctx, &sql).await?;
    assert!(
        plan.contains("SortExec"),
        "a mixed scan must keep its explicit sort:\n{plan}"
    );

    Ok(())
}
