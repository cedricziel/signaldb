//! # Parquet footer/metadata caching for the query path
//!
//! A single-trace lookup is bound by *opening* Parquet files, not by scanning
//! them: statistics and bloom pruning already reduce a `WHERE trace_id = ?`
//! scan to a handful of bytes, but every candidate file's footer still has to
//! be fetched and decoded first. On object storage each footer is a network
//! round-trip, and the same files are re-opened on every query (#880).
//!
//! DataFusion already carries a footer cache on the `RuntimeEnv`
//! ([`FileMetadataCache`]), but only the built-in `ListingTable`/`ParquetFormat`
//! path wires it up: it installs a [`CachedParquetFileReaderFactory`] on the
//! scan. A [`ParquetSource`] built by any other provider — including the
//! `datafusion_iceberg` provider SignalDB reads through — falls back to
//! `DefaultParquetFileReaderFactory`, which re-reads and re-decodes the footer
//! on every single scan.
//!
//! [`CacheParquetMetadata`] closes that gap as a physical optimizer rule: it
//! rewrites every `DataSourceExec` over a factory-less [`ParquetSource`] to use
//! a [`CachedParquetFileReaderFactory`] bound to the session's shared
//! `RuntimeEnv` cache. Because the querier's per-request contexts are cloned
//! from one root context (`SessionStateBuilder::new_from_existing`), that cache
//! is shared process-wide and survives across requests and tenants.
//!
//! It lives here rather than in the querier because nothing about it is
//! querier-specific: any service that reads Parquet through a provider other
//! than `ListingTable` needs the same wiring. The proper home is the provider
//! itself — the rule should be deleted once `datafusion_iceberg` installs the
//! factory the way `ParquetFormat` does. Until then
//! `tests-integration/tests/querier/trace_point_lookup.rs` asserts end to end
//! that the footers of a real Iceberg scan land in the cache, so the rule
//! silently ceasing to match the provider's plan shape fails a test rather
//! than quietly regressing to uncached reads.
//!
//! ## Known cost
//!
//! Rewriting means cloning the scan's `FileScanConfig` (its file list above
//! all) once per Parquet scan per query. That is a real per-query cost the
//! provider-side fix would not have, and the reason to prefer that fix; it is
//! still far below the object-store round-trips it removes.
//!
//! ## Safety of caching
//!
//! Iceberg data files are immutable — compaction writes new files at new paths
//! and never rewrites one in place — so a cached footer can only ever go
//! *unused*, never stale. DataFusion validates each hit against the current
//! `ObjectMeta` (size + last modified) anyway before serving it, so a
//! path that was somehow replaced re-reads instead of returning wrong metadata.

use std::sync::Arc;

use datafusion::common::Result;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::datasource::physical_plan::parquet::CachedParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

/// Physical optimizer rule that makes Parquet scans read their footers through
/// the session's [`FileMetadataCache`](datafusion::execution::cache::cache_manager::FileMetadataCache).
///
/// Register it last in a session's rule chain (as
/// `querier::flight::session_context_with_limits` does), so it observes the
/// final scan shape after projection, limit and filter pushdown have run.
#[derive(Debug)]
pub struct CacheParquetMetadata {
    runtime: Arc<RuntimeEnv>,
}

impl CacheParquetMetadata {
    /// The rule caches into `runtime`'s metadata cache and resolves object
    /// stores from the same runtime, so it must be the runtime the session
    /// actually executes on.
    pub fn new(runtime: Arc<RuntimeEnv>) -> Self {
        Self { runtime }
    }

    /// Rewrite one node, or leave it untouched when it is not a Parquet scan
    /// that still uses the default (uncached) reader factory.
    fn rewrite(&self, plan: Arc<dyn ExecutionPlan>) -> Result<Transformed<Arc<dyn ExecutionPlan>>> {
        let Some(exec) = plan.downcast_ref::<DataSourceExec>() else {
            return Ok(Transformed::no(plan));
        };
        let Some((config, parquet)) = exec.downcast_to_file_source::<ParquetSource>() else {
            return Ok(Transformed::no(plan));
        };
        // A provider that already installed its own factory knows better than
        // this rule does; never override it.
        if parquet.parquet_file_reader_factory().is_some() {
            return Ok(Transformed::no(plan));
        }

        // An unregistered object store is not a query error here: leaving the
        // scan alone yields the default factory, which resolves the store
        // itself at execution time and fails there with the real message.
        let Ok(store) = self.runtime.object_store(&config.object_store_url) else {
            tracing::debug!(
                url = %config.object_store_url,
                "No object store registered for scan; leaving the default Parquet reader factory in place"
            );
            return Ok(Transformed::no(plan));
        };

        let factory = Arc::new(CachedParquetFileReaderFactory::new(
            store,
            self.runtime.cache_manager.get_file_metadata_cache(),
        ));
        let source = Arc::new(parquet.clone().with_parquet_file_reader_factory(factory));
        let config = FileScanConfigBuilder::from(config.clone())
            .with_source(source)
            .build();
        Ok(Transformed::yes(
            DataSourceExec::from_data_source(config) as Arc<dyn ExecutionPlan>
        ))
    }
}

impl PhysicalOptimizerRule for CacheParquetMetadata {
    fn name(&self) -> &str {
        "signaldb_cache_parquet_metadata"
    }

    fn schema_check(&self) -> bool {
        true
    }

    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_down(|node| self.rewrite(node)).data()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::datasource::listing::PartitionedFile;
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::physical_plan::FileGroup;
    use datafusion::datasource::table_schema::TableSchema;
    use datafusion::execution::object_store::ObjectStoreUrl;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use object_store::memory::InMemory;

    fn schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]))
    }

    /// A `DataSourceExec` over one Parquet file, as a provider would build it:
    /// no reader factory, so footers are read uncached.
    fn parquet_scan(url: ObjectStoreUrl) -> Arc<dyn ExecutionPlan> {
        let source = Arc::new(ParquetSource::new(TableSchema::new(schema(), vec![])));
        let config = FileScanConfigBuilder::new(url, source)
            .with_file_group(FileGroup::new(vec![PartitionedFile::new("a.parquet", 100)]))
            .build();
        DataSourceExec::from_data_source(config)
    }

    fn runtime_with_store(url: &ObjectStoreUrl) -> Arc<RuntimeEnv> {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        runtime.register_object_store(url.as_ref(), Arc::new(InMemory::new()));
        runtime
    }

    #[test]
    fn installs_a_cached_reader_factory_on_parquet_scans() {
        let url = ObjectStoreUrl::parse("memory://").unwrap();
        let runtime = runtime_with_store(&url);
        let rule = CacheParquetMetadata::new(Arc::clone(&runtime));

        let optimized = rule
            .optimize(parquet_scan(url), &ConfigOptions::default())
            .unwrap();

        let (_config, parquet) = optimized
            .downcast_ref::<DataSourceExec>()
            .expect("still a DataSourceExec")
            .downcast_to_file_source::<ParquetSource>()
            .expect("still a Parquet file scan");
        assert!(
            parquet.parquet_file_reader_factory().is_some(),
            "the scan must read footers through the cached factory"
        );
    }

    #[test]
    fn leaves_scans_without_a_registered_object_store_alone() {
        // No store registered for this URL: the rule must not fail the query,
        // it just declines to rewrite.
        let url = ObjectStoreUrl::parse("memory://").unwrap();
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        let rule = CacheParquetMetadata::new(runtime);

        let optimized = rule
            .optimize(parquet_scan(url), &ConfigOptions::default())
            .unwrap();

        let (_config, parquet) = optimized
            .downcast_ref::<DataSourceExec>()
            .unwrap()
            .downcast_to_file_source::<ParquetSource>()
            .unwrap();
        assert!(parquet.parquet_file_reader_factory().is_none());
    }

    #[test]
    fn leaves_non_parquet_sources_untouched() {
        let runtime = Arc::new(RuntimeEnvBuilder::new().build().unwrap());
        let rule = CacheParquetMetadata::new(runtime);
        let plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(
            MemorySourceConfig::try_new(&[vec![]], schema(), None).unwrap(),
        );

        let optimized = rule
            .optimize(Arc::clone(&plan), &ConfigOptions::default())
            .unwrap();
        assert!(
            optimized
                .downcast_ref::<DataSourceExec>()
                .unwrap()
                .data_source()
                .downcast_ref::<MemorySourceConfig>()
                .is_some(),
            "a non-Parquet source must pass through unchanged"
        );
    }
}
