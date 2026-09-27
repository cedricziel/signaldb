//! Wires [`super::probe::probe_clauses`]/[`super::prefilter::prefilter_files`]
//! into a table's physical scan: [`WarmIndexTable::maybe_wrap`] wraps a table
//! provider only when it actually carries the warm containment index, and its
//! `scan` prunes the inner provider's Parquet [`DataSourceExec`] nodes against
//! any recognized equality filter before returning the plan.
use std::collections::HashMap;
use std::sync::Arc;

use common::attrs::warm_index::WARM_INDEX_COLUMN;
use common::schema::{WARM_INDEX_ENCODING_PROPERTY, WARM_INDEX_ENCODING_VERSION};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::Session;
use datafusion::common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion::datasource::TableProvider;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::ParquetAccessPlan;
use datafusion::datasource::physical_plan::{FileScanConfig, FileScanConfigBuilder, ParquetSource};
use datafusion::datasource::source::DataSourceExec;
use datafusion::error::Result as DFResult;
use datafusion::execution::cache::cache_manager::FileMetadataCache;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use futures::future::join_all;
use object_store::ObjectStore;

use super::prefilter::{KeptFile, KeptRowGroups, PrefilterOutcome, WarmIndexGate, prefilter_files};
use super::probe::{ProbeClause, probe_clauses};

/// A [`TableProvider`] that prunes `inner`'s Parquet scan against the warm
/// containment index before returning it.
#[derive(Debug)]
pub(crate) struct WarmIndexTable {
    inner: Arc<dyn TableProvider>,
    gate: WarmIndexGate,
}

impl WarmIndexTable {
    /// Wraps `inner` unless it is not an opted-in typed table: `inner`'s
    /// schema must carry [`WARM_INDEX_COLUMN`], and `properties` (the
    /// table's Iceberg properties) must carry a
    /// [`WARM_INDEX_ENCODING_PROPERTY`] this build understands. Either
    /// miss and `inner` is returned untouched — no pruning is attempted
    /// against an index this code can't safely read.
    pub(crate) fn maybe_wrap(
        inner: Arc<dyn TableProvider>,
        properties: &HashMap<String, String>,
        gate: WarmIndexGate,
    ) -> Arc<dyn TableProvider> {
        let has_warm_index = inner.schema().index_of(WARM_INDEX_COLUMN).is_ok();
        let known_encoding = properties
            .get(WARM_INDEX_ENCODING_PROPERTY)
            .is_some_and(|version| version == WARM_INDEX_ENCODING_VERSION);
        if !has_warm_index || !known_encoding {
            return inner;
        }
        Arc::new(Self { inner, gate })
    }
}

#[async_trait::async_trait]
impl TableProvider for WarmIndexTable {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> datafusion::datasource::TableType {
        self.inner.table_type()
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        self.inner.supports_filters_pushdown(filters)
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        let clauses = probe_clauses(filters);
        if clauses.is_empty() {
            return Ok(plan);
        }

        let runtime = state.runtime_env();
        let scans = collect_parquet_scans(&plan, runtime)?;
        if scans.is_empty() {
            return Ok(plan);
        }
        let cache = runtime.cache_manager.get_file_metadata_cache();
        let replacements = probe_all(scans, &cache, &clauses, &self.gate).await;
        let (rewritten, outcome) = splice_replacements(plan, replacements)?;
        tracing::debug!(
            probed = outcome.probed,
            pruned_files = outcome.pruned_files,
            pruned_row_groups = outcome.pruned_row_groups,
            gated = outcome.gated,
            "warm-index prefilter applied to scan"
        );
        Ok(rewritten)
    }
}

/// Merges `other` into `outcome`, field by field.
fn merge_outcome(outcome: &mut PrefilterOutcome, other: PrefilterOutcome) {
    outcome.probed += other.probed;
    outcome.pruned_files += other.pruned_files;
    outcome.pruned_row_groups += other.pruned_row_groups;
    outcome.gated |= other.gated;
}

/// A node's identity across the three rewrite passes: the data pointer of
/// its `Arc`, which every clone of the same node shares, and which is still
/// valid in pass (c) because nothing under `plan` is rewritten between
/// passes (a) and (c) — only read.
fn node_key(plan: &Arc<dyn ExecutionPlan>) -> usize {
    Arc::as_ptr(plan) as *const () as usize
}

/// One Parquet scan node found by pass (a), keyed for pass (c) to splice its
/// probed replacement back in.
struct ScanNode {
    key: usize,
    store: Arc<dyn ObjectStore>,
    config: FileScanConfig,
}

/// Pass (a): a synchronous, read-only walk collecting every Parquet
/// [`DataSourceExec`] node's [`FileScanConfig`] and object store. A node
/// with no object store registered is skipped — left for the scan itself to
/// fail with the real error, not this prefilter.
fn collect_parquet_scans(
    plan: &Arc<dyn ExecutionPlan>,
    runtime: &RuntimeEnv,
) -> DFResult<Vec<ScanNode>> {
    let mut scans = Vec::new();
    plan.apply(|node| {
        if let Some(exec) = node.downcast_ref::<DataSourceExec>()
            && let Some((config, _parquet)) = exec.downcast_to_file_source::<ParquetSource>()
            && let Ok(store) = runtime.object_store(&config.object_store_url)
        {
            scans.push(ScanNode {
                key: node_key(node),
                store,
                config: config.clone(),
            });
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(scans)
}

/// Pass (b): probes every scan node found by pass (a) concurrently in one
/// flat step (`join_all`, not nested inside the tree walk), returning each
/// node's already-rewritten replacement plan keyed for pass (c).
async fn probe_all(
    scans: Vec<ScanNode>,
    cache: &Arc<FileMetadataCache>,
    clauses: &[ProbeClause],
    gate: &WarmIndexGate,
) -> HashMap<usize, (Arc<dyn ExecutionPlan>, PrefilterOutcome)> {
    let probes = scans.into_iter().map(|scan| {
        let cache = Arc::clone(cache);
        async move {
            let files: Vec<PartitionedFile> = scan
                .config
                .file_groups
                .iter()
                .flat_map(|group| group.iter().cloned())
                .collect();
            let (kept, outcome) = prefilter_files(scan.store, cache, files, clauses, gate).await;
            let plan = rebuild_scan(&scan.config, kept);
            (scan.key, (plan, outcome))
        }
    });
    join_all(probes).await.into_iter().collect()
}

/// Rebuilds `config`'s file groups from `kept` (pruned files dropped, empty
/// groups kept so the partition count is unchanged) and wraps the result
/// back into a `DataSourceExec`.
fn rebuild_scan(config: &FileScanConfig, kept: Vec<KeptFile>) -> Arc<dyn ExecutionPlan> {
    let mut kept_by_path: HashMap<String, KeptFile> = kept
        .into_iter()
        .map(|k| (k.file.object_meta.location.to_string(), k))
        .collect();
    let file_groups = config
        .file_groups
        .iter()
        .map(|group| {
            let files = group
                .iter()
                .filter_map(|file| {
                    kept_by_path
                        .remove(file.object_meta.location.as_ref())
                        .map(apply_access_plan)
                })
                .collect();
            datafusion::datasource::physical_plan::FileGroup::new(files)
        })
        .collect();
    let new_config = FileScanConfigBuilder::from(config.clone())
        .with_file_groups(file_groups)
        .build();
    DataSourceExec::from_data_source(new_config) as Arc<dyn ExecutionPlan>
}

/// Pass (c): an ordinary synchronous [`TreeNode::transform_up`] splicing
/// pass (b)'s precomputed replacements back into `plan` by node identity.
/// Returns the rewritten plan and the combined [`PrefilterOutcome`] of every
/// scan node touched.
fn splice_replacements(
    plan: Arc<dyn ExecutionPlan>,
    mut replacements: HashMap<usize, (Arc<dyn ExecutionPlan>, PrefilterOutcome)>,
) -> DFResult<(Arc<dyn ExecutionPlan>, PrefilterOutcome)> {
    let mut outcome = PrefilterOutcome::default();
    let transformed = plan.transform_up(|node| match replacements.remove(&node_key(&node)) {
        Some((replacement, node_outcome)) => {
            merge_outcome(&mut outcome, node_outcome);
            Ok(Transformed::yes(replacement))
        }
        None => Ok(Transformed::no(node)),
    })?;
    Ok((transformed.data, outcome))
}

/// Attaches a [`ParquetAccessPlan`] to `kept.file` when the probe narrowed it
/// to a subset of its row groups; an unprobed/fully-kept file passes through
/// unchanged.
fn apply_access_plan(kept: KeptFile) -> PartitionedFile {
    match kept.row_groups {
        Some(KeptRowGroups { kept: rgs, total }) => {
            let mut plan = ParquetAccessPlan::new_none(total);
            for row_group in rgs {
                plan.scan(row_group);
            }
            kept.file.with_extension(plan)
        }
        None => kept.file,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};

    fn schema_with(fields: Vec<Field>) -> SchemaRef {
        Arc::new(Schema::new(fields))
    }

    fn warm_index_field() -> Field {
        Field::new(
            WARM_INDEX_COLUMN,
            DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
            true,
        )
    }

    #[derive(Debug)]
    struct StubTable {
        schema: SchemaRef,
    }

    #[async_trait::async_trait]
    impl TableProvider for StubTable {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
        fn table_type(&self) -> datafusion::datasource::TableType {
            datafusion::datasource::TableType::Base
        }
        async fn scan(
            &self,
            _state: &dyn Session,
            _projection: Option<&Vec<usize>>,
            _filters: &[Expr],
            _limit: Option<usize>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            unimplemented!("not exercised by these tests")
        }
    }

    fn properties_with_known_encoding() -> HashMap<String, String> {
        HashMap::from([(
            WARM_INDEX_ENCODING_PROPERTY.to_string(),
            WARM_INDEX_ENCODING_VERSION.to_string(),
        )])
    }

    #[test]
    fn a_table_without_the_warm_index_column_is_not_wrapped() {
        let inner: Arc<dyn TableProvider> = Arc::new(StubTable {
            schema: schema_with(vec![Field::new("a", DataType::Int64, true)]),
        });
        let wrapped = WarmIndexTable::maybe_wrap(
            Arc::clone(&inner),
            &properties_with_known_encoding(),
            WarmIndexGate::default(),
        );
        assert!(Arc::ptr_eq(&inner, &wrapped));
    }

    #[test]
    fn a_table_with_an_unknown_encoding_version_is_not_wrapped() {
        let inner: Arc<dyn TableProvider> = Arc::new(StubTable {
            schema: schema_with(vec![warm_index_field()]),
        });
        let properties =
            HashMap::from([(WARM_INDEX_ENCODING_PROPERTY.to_string(), "99".to_string())]);
        let wrapped =
            WarmIndexTable::maybe_wrap(Arc::clone(&inner), &properties, WarmIndexGate::default());
        assert!(Arc::ptr_eq(&inner, &wrapped));
    }

    #[test]
    fn a_table_with_the_warm_index_column_and_known_encoding_is_wrapped() {
        let inner: Arc<dyn TableProvider> = Arc::new(StubTable {
            schema: schema_with(vec![warm_index_field()]),
        });
        let wrapped = WarmIndexTable::maybe_wrap(
            Arc::clone(&inner),
            &properties_with_known_encoding(),
            WarmIndexGate::default(),
        );
        assert!(!Arc::ptr_eq(&inner, &wrapped));
        assert_eq!(wrapped.schema(), inner.schema());
    }
}
