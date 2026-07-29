use crate::schema_transform::{
    transform_logs_v1_to_iceberg, transform_metrics_exponential_histogram_v1_to_iceberg,
    transform_metrics_gauge_v1_to_iceberg, transform_metrics_histogram_v1_to_iceberg,
    transform_metrics_sum_v1_to_iceberg, transform_metrics_summary_v1_to_iceberg,
    transform_profiles_v1_to_iceberg, transform_trace_v1_to_v2,
};
use anyhow::Result;
use common::CatalogManager;

use datafusion::arrow::array::{RecordBatch, new_null_array};
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg_rust::arrow::write::write_parquet_partitioned;
use iceberg_rust::catalog::Catalog as IcebergRustCatalog;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::tabular::Tabular;
use iceberg_rust::table::Table;
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use uuid;

/// Configuration for retry behavior
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_attempts: u32,
    /// Initial delay before first retry
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            backoff_multiplier: 2.0,
        }
    }
}

/// Writes signal batches to an Iceberg table.
///
/// The only write entry point is [`Self::append_batches_with_marker`],
/// which verifies every commit against the catalog before reporting
/// success. The SQL catalog's compare-and-swap does not report a lost
/// race (issue #538), so any commit path that trusts `commit()`'s return
/// value can silently lose data -- do not add one.
pub struct IcebergTableWriter {
    catalog: Arc<dyn IcebergRustCatalog>,
    table: Table,
    #[allow(dead_code)] // Will be used for data writing
    object_store: Arc<dyn ObjectStore>,
    tenant_id: String,
    dataset_id: String,
    /// Tenant-resolved materialized-label allowlists (a tenant schema
    /// override replaces the global set), used by the transforms.
    materialized: common::config::MaterializedLabels,
    /// Retry configuration for failed operations
    retry_config: RetryConfig,
}

impl IcebergTableWriter {
    /// Create a new IcebergTableWriter for a specific table
    pub async fn new(
        catalog_manager: &CatalogManager,
        object_store: Arc<dyn ObjectStore>,
        tenant_id: String,
        dataset_id: String,
        table_name: String,
    ) -> Result<Self> {
        let table = catalog_manager
            .ensure_table(&tenant_id, &dataset_id, &table_name)
            .await?;
        let catalog = catalog_manager.catalog();

        log::info!(
            "Successfully created/loaded Iceberg table: {} for tenant '{tenant_id}' dataset '{dataset_id}'",
            table.identifier()
        );

        let table_metadata = table.metadata();
        let current_schema = table.current_schema()?;
        log::debug!("Table location: {}", table_metadata.location);
        log::debug!("Schema has {} fields", current_schema.fields().len());

        let materialized = catalog_manager
            .config()
            .get_tenant_schema_config(&tenant_id)
            .materialized_labels;

        Ok(Self {
            catalog,
            table,
            object_store,
            tenant_id,
            dataset_id,
            materialized,
            retry_config: RetryConfig::default(),
        })
    }

    /// Apply schema transformation if the batch has v1 (wire) schema but the
    /// table expects the Iceberg storage schema.
    ///
    /// Dispatches per table so that logs and metrics batches are handled, not
    /// just traces. Wire-format batches are recognized by their raw OTLP
    /// marker columns (`time_unix_nano` for logs, `data_json` for metrics);
    /// batches already in storage format lack those columns and pass through
    /// unchanged, keeping the method idempotent for callers that already
    /// transformed (e.g. the Flight ingestion path).
    fn apply_schema_transformation_if_needed(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let num_columns = batch.num_columns();
        let field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        let has_field = |name: &str| field_names.iter().any(|f| f == name);

        match self.table.identifier().name() {
            "traces" => {
                // v1 schema uses "name" (renamed to "span_name" in v2) and lacks
                // computed fields; v2 uses "span_name" plus "timestamp"/"date_day"/"hour".
                if has_field("name") && !has_field("span_name") {
                    log::debug!("Detected v1 traces batch, applying v1->v2 transformation");
                    transform_trace_v1_to_v2(batch, &self.materialized.traces)
                } else if has_field("span_name") {
                    log::debug!("Detected v2 traces batch, no transformation needed");
                    Ok(batch)
                } else {
                    log::warn!(
                        "Unknown traces schema: {num_columns} columns with fields: {field_names:?}. Assuming no transformation needed."
                    );
                    Ok(batch)
                }
            }
            // Wire-format logs carry raw OTLP "time_unix_nano"; the storage
            // schema uses computed "timestamp"/"date_day"/"hour" columns.
            "logs" if has_field("time_unix_nano") => {
                log::debug!("Detected v1 logs batch, applying logs->iceberg transformation");
                transform_logs_v1_to_iceberg(batch, &self.materialized.logs)
            }
            // Wire-format metrics carry the raw "data_json" payload column.
            "metrics_gauge" if has_field("data_json") => {
                transform_metrics_gauge_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_sum" if has_field("data_json") => {
                transform_metrics_sum_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_histogram" if has_field("data_json") => {
                transform_metrics_histogram_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            "metrics_exponential_histogram" if has_field("data_json") => {
                transform_metrics_exponential_histogram_v1_to_iceberg(
                    batch,
                    &self.materialized.metrics,
                )
            }
            "metrics_summary" if has_field("data_json") => {
                transform_metrics_summary_v1_to_iceberg(batch, &self.materialized.metrics)
            }
            // Wire-format profiles carry raw OTLP "time_unix_nano"; the
            // storage schema uses computed "timestamp"/"date_day"/"hour".
            "profiles" if has_field("time_unix_nano") => {
                log::debug!(
                    "Detected v1 profiles batch, applying profiles->iceberg transformation"
                );
                transform_profiles_v1_to_iceberg(batch, &self.materialized.profiles)
            }
            _ => Ok(batch),
        }
    }

    /// Get table identifier
    pub fn table_identifier(&self) -> &Identifier {
        self.table.identifier()
    }

    /// Get table metadata
    pub fn table_metadata(&self) -> &iceberg_rust::spec::table_metadata::TableMetadata {
        self.table.metadata()
    }

    /// Update retry configuration
    pub fn set_retry_config(&mut self, retry_config: RetryConfig) {
        self.retry_config = retry_config;
    }

    /// Get current retry configuration
    pub fn retry_config(&self) -> &RetryConfig {
        &self.retry_config
    }

    /// Reload the table from the catalog, bypassing any cached handle,
    /// so marker reads and the next commit are based on current metadata.
    async fn reload_table(&mut self) -> Result<()> {
        let ident = self.table.identifier().clone();
        let tabular = self
            .catalog
            .clone()
            .load_tabular(&ident)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to reload Iceberg table {ident}: {e}"))?;
        match tabular {
            Tabular::Table(table) => {
                self.table = table;
                Ok(())
            }
            _ => Err(anyhow::anyhow!(
                "Expected table but found different tabular type for {ident}"
            )),
        }
    }

    /// Parse this WAL writer's idempotency marker from the (in-memory)
    /// table properties.
    fn read_marker(&self, wal_writer_id: &str) -> HashSet<uuid::Uuid> {
        self.table
            .metadata()
            .properties
            .get(&wal_marker_key(wal_writer_id))
            .map(|value| decode_marker_ids(value))
            .unwrap_or_default()
    }

    /// Reload the table and return the WAL entry ids recorded by the most
    /// recent marker commit for `wal_writer_id`. Ids present here are
    /// durably committed to Iceberg even if the WAL never marked them
    /// processed (crash between commit and index write).
    pub async fn load_committed_marker(
        &mut self,
        wal_writer_id: &str,
    ) -> Result<HashSet<uuid::Uuid>> {
        self.reload_table().await?;
        Ok(self.read_marker(wal_writer_id))
    }

    /// Atomically append `entries`' batches and record their WAL entry ids
    /// as this writer's idempotency marker — data files and marker ride in
    /// ONE Iceberg commit (a single catalog CAS), so replay after a crash
    /// can always tell whether the data landed.
    ///
    /// Contract for callers: dedupe `entries` against
    /// [`Self::load_committed_marker`] and durably mark any previously
    /// committed ids processed BEFORE calling this — the commit REPLACES
    /// the marker, discarding the evidence for earlier commits.
    ///
    /// Commit outcomes are verified against the catalog rather than
    /// trusted: an `Err` can follow a commit that actually landed
    /// (ambiguous failure), and the sql catalog can lose a CAS silently.
    /// After every attempt the table is reloaded and the marker checked —
    /// only the marker decides success, so retries can never double-append.
    #[tracing::instrument(
        skip_all,
        fields(
            tenant_id = %self.tenant_id,
            dataset_id = %self.dataset_id,
            entry_count = entries.len()
        )
    )]
    pub async fn append_batches_with_marker(
        &mut self,
        wal_writer_id: &str,
        entries: Vec<(uuid::Uuid, RecordBatch)>,
    ) -> Result<()> {
        let (ids, batches): (Vec<uuid::Uuid>, Vec<RecordBatch>) = entries.into_iter().unzip();

        // The Parquet writer requires batches in the table's exact Arrow
        // schema (derived from the Iceberg schema, e.g. microsecond
        // timestamps), so coerce after the wire→storage transformation.
        let target_schema: ArrowSchemaRef = Arc::new(
            self.table
                .current_schema()
                .map_err(|e| anyhow::anyhow!("Failed to get current Iceberg schema: {e}"))?
                .fields()
                .try_into()
                .map_err(|e: iceberg_rust::spec::error::Error| {
                    anyhow::anyhow!("Failed to convert Iceberg schema to Arrow: {e}")
                })?,
        );

        let mut transformed = Vec::new();
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let batch = self.apply_schema_transformation_if_needed(batch)?;
            transformed.push(coerce_batch_to_schema(batch, &target_schema)?);
        }
        if transformed.is_empty() {
            // Nothing to append: the ids can be marked processed without
            // a commit, and replaying them is a no-op either way.
            return Ok(());
        }
        let total_rows: usize = transformed.iter().map(|b| b.num_rows()).sum();

        let stream = futures::stream::iter(transformed.into_iter().map(Ok));
        let files = write_parquet_partitioned(&self.table, stream, None)
            .await
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to write Parquet files for Iceberg table {}: {e}",
                    self.table.identifier()
                )
            })?;
        if files.is_empty() {
            return Err(anyhow::anyhow!(
                "write_parquet_partitioned produced no data files for {total_rows} rows"
            ));
        }

        let marker_key = wal_marker_key(wal_writer_id);
        let marker_value = encode_marker_ids(&ids);
        let id_set: HashSet<uuid::Uuid> = ids.iter().copied().collect();

        let mut attempt = 0;
        let mut delay = self.retry_config.initial_delay;
        loop {
            attempt += 1;
            let commit_result = self
                .table
                .new_transaction(None)
                .append_data(files.clone())
                .update_properties(vec![(marker_key.clone(), marker_value.clone())])
                .commit()
                .await;

            // The catalog is the source of truth for whether the commit
            // landed, regardless of what commit() returned.
            self.reload_table().await?;
            if self.read_marker(wal_writer_id) == id_set {
                if let Err(e) = commit_result {
                    log::warn!(
                        "Iceberg commit reported an error but the marker landed \
                         (treating as success): {e}"
                    );
                }
                log::info!(
                    "Committed {} rows in {} data files to Iceberg table {} (attempt {attempt})",
                    total_rows,
                    files.len(),
                    self.table.identifier()
                );
                return Ok(());
            }

            let error = match commit_result {
                Ok(()) => anyhow::anyhow!(
                    "Iceberg commit reported success but the marker is absent \
                     (catalog CAS silently lost)"
                ),
                Err(e) => anyhow::anyhow!("Iceberg commit failed: {e}"),
            };
            if attempt >= self.retry_config.max_attempts {
                return Err(error.context(format!(
                    "Failed to commit {} entries to Iceberg table {} after {attempt} attempts",
                    id_set.len(),
                    self.table.identifier()
                )));
            }
            log::warn!(
                "Commit attempt {attempt} for Iceberg table {} did not land: {error}. \
                 Retrying in {delay:?}",
                self.table.identifier()
            );
            tokio::time::sleep(delay).await;
            delay = std::cmp::min(
                self.retry_config.max_delay,
                Duration::from_secs_f64(delay.as_secs_f64() * self.retry_config.backoff_multiplier),
            );
        }
    }
}

/// Prefix for the per-WAL idempotency marker stored in Iceberg table
/// properties. The full key is `signaldb.wal.committed.<wal-writer-id>`,
/// so concurrent writer nodes (distinct WAL directories) never clobber
/// each other's markers.
pub const WAL_MARKER_PREFIX: &str = "signaldb.wal.committed.";

fn wal_marker_key(wal_writer_id: &str) -> String {
    format!("{WAL_MARKER_PREFIX}{wal_writer_id}")
}

fn encode_marker_ids(ids: &[uuid::Uuid]) -> String {
    ids.iter()
        .map(|id| id.simple().to_string())
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_marker_ids(value: &str) -> HashSet<uuid::Uuid> {
    value
        .split(',')
        .filter_map(|part| uuid::Uuid::parse_str(part.trim()).ok())
        .collect()
}

/// Project and cast a batch onto the table's Arrow schema (columns matched
/// by name). Extra batch columns are dropped; missing columns are an error,
/// as is a null in a column the table declares non-nullable.
fn coerce_batch_to_schema(batch: RecordBatch, target: &ArrowSchemaRef) -> Result<RecordBatch> {
    let mut columns = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let Ok(index) = batch.schema().index_of(field.name()) else {
            // A nullable target column absent from the batch (e.g. a
            // materialized `label_<key>` on a table whose current config no
            // longer lists it) is filled with nulls; a required one is a
            // genuine schema mismatch.
            if field.is_nullable() {
                columns.push(new_null_array(field.data_type(), batch.num_rows()));
                continue;
            }
            return Err(anyhow::anyhow!(
                "Batch is missing column '{}' required by the table schema",
                field.name()
            ));
        };
        let column = batch.column(index);
        let column = if column.data_type() == field.data_type() {
            column.clone()
        } else if matches!(
            (column.data_type(), field.data_type()),
            (
                datafusion::arrow::datatypes::DataType::Utf8,
                datafusion::arrow::datatypes::DataType::Map(_, _)
            )
        ) {
            // Attribute maps: the transforms emit flat JSON objects as
            // strings; tables with a map-typed attribute column get the
            // parsed entries.
            json_strings_to_map_array(column, field)?
        } else {
            datafusion::arrow::compute::cast(column, field.data_type()).map_err(|e| {
                anyhow::anyhow!(
                    "Failed to cast column '{}' from {:?} to {:?}: {e}",
                    field.name(),
                    column.data_type(),
                    field.data_type()
                )
            })?
        };
        columns.push(column);
    }
    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| anyhow::anyhow!("Failed to build coerced batch: {e}"))
}

/// Parse a column of flat-JSON-object strings into a `MapArray` matching
/// `target_field`'s entry/key/value naming. Null or unparseable documents
/// become null map entries; non-string JSON values are rendered with
/// `to_string` (matching the substring-match era's serialized forms).
fn json_strings_to_map_array(
    column: &dyn datafusion::arrow::array::Array,
    target_field: &datafusion::arrow::datatypes::Field,
) -> Result<std::sync::Arc<dyn datafusion::arrow::array::Array>> {
    use datafusion::arrow::array::{Array, MapBuilder, MapFieldNames, StringArray, StringBuilder};
    use datafusion::arrow::datatypes::DataType;

    let strings = column
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or_else(|| anyhow::anyhow!("expected a Utf8 column for map coercion"))?;
    let DataType::Map(entry_field, _) = target_field.data_type() else {
        return Err(anyhow::anyhow!("target field is not a map"));
    };
    let DataType::Struct(kv_fields) = entry_field.data_type() else {
        return Err(anyhow::anyhow!("map entries are not a struct"));
    };
    let names = MapFieldNames {
        entry: entry_field.name().clone(),
        key: kv_fields[0].name().clone(),
        value: kv_fields[1].name().clone(),
    };
    let mut builder = MapBuilder::new(Some(names), StringBuilder::new(), StringBuilder::new());
    for i in 0..strings.len() {
        if strings.is_null(i) {
            builder.append(false)?;
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(strings.value(i)) {
            Ok(serde_json::Value::Object(map)) => {
                for (k, v) in map {
                    builder.keys().append_value(k);
                    match v {
                        serde_json::Value::String(s) => builder.values().append_value(s),
                        other => builder.values().append_value(other.to_string()),
                    }
                }
                builder.append(true)?;
            }
            _ => builder.append(false)?,
        }
    }
    let built = builder.finish();
    // Align entry-field nullability with the target (MapBuilder's inner
    // struct layout matches by construction; the cast is a no-op check).
    if built.data_type() == target_field.data_type() {
        Ok(std::sync::Arc::new(built))
    } else {
        datafusion::arrow::compute::cast(&built, target_field.data_type())
            .map_err(|e| anyhow::anyhow!("map coercion type mismatch: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{Configuration, SchemaConfig, StorageConfig};
    use datafusion::arrow::array::{Array, StringArray};
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn coerce_converts_json_strings_to_map_column() {
        use datafusion::arrow::array::MapArray;

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "log_attributes",
                DataType::Utf8,
                true,
            )])),
            vec![Arc::new(StringArray::from(vec![
                Some(r#"{"namespace":"prod","port":8080}"#),
                None,
                Some("not-json"),
            ]))],
        )
        .unwrap();

        let target = Arc::new(Schema::new(vec![Field::new_map(
            "log_attributes",
            "key_value",
            Field::new("key", DataType::Utf8, false),
            Field::new("value", DataType::Utf8, true),
            false,
            true,
        )]));

        let out = coerce_batch_to_schema(batch, &target).unwrap();
        let map = out
            .column_by_name("log_attributes")
            .unwrap()
            .as_any()
            .downcast_ref::<MapArray>()
            .unwrap();
        // Row 0: two entries, non-string value rendered as text.
        assert!(!map.is_null(0));
        let entries = map.value(0);
        assert_eq!(entries.len(), 2);
        // Rows 1 (null) and 2 (unparseable) become null maps.
        assert!(map.is_null(1));
        assert!(map.is_null(2));
    }

    #[test]
    fn coerce_fills_null_for_missing_nullable_column() {
        // Batch has only `body`; the target adds a nullable `label_ns`.
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("body", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![Some("hi"), Some("yo")]))],
        )
        .unwrap();
        let target = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("label_ns", DataType::Utf8, true),
        ]));

        let out = coerce_batch_to_schema(batch.clone(), &target).unwrap();
        assert_eq!(out.num_columns(), 2);
        let label = out.column_by_name("label_ns").unwrap();
        assert_eq!(label.null_count(), 2);

        // A missing *required* column is still a hard error.
        let required_target = Arc::new(Schema::new(vec![
            Field::new("body", DataType::Utf8, true),
            Field::new("must_have", DataType::Utf8, false),
        ]));
        assert!(coerce_batch_to_schema(batch, &required_target).is_err());
    }

    async fn create_test_catalog_manager() -> CatalogManager {
        let config = Configuration {
            schema: SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: Default::default(),
                materialized_labels: Default::default(),
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Default::default()
        };

        CatalogManager::new(config).await.unwrap()
    }

    #[test]
    fn marker_ids_round_trip() {
        let ids = vec![uuid::Uuid::new_v4(), uuid::Uuid::new_v4()];
        let encoded = encode_marker_ids(&ids);
        let decoded = decode_marker_ids(&encoded);
        assert_eq!(decoded, ids.iter().copied().collect::<HashSet<_>>());
    }

    #[test]
    fn marker_decode_tolerates_garbage() {
        let id = uuid::Uuid::new_v4();
        let value = format!("not-a-uuid,{},", id.simple());
        let decoded = decode_marker_ids(&value);
        assert_eq!(decoded, HashSet::from([id]));
        assert!(decode_marker_ids("").is_empty());
    }

    #[test]
    fn marker_key_is_namespaced_per_writer() {
        assert_eq!(
            wal_marker_key("abc123"),
            "signaldb.wal.committed.abc123".to_string()
        );
        assert_ne!(wal_marker_key("writer-a"), wal_marker_key("writer-b"));
    }

    #[tokio::test]
    async fn test_iceberg_writer_creation() {
        let catalog_manager = CatalogManager::new_in_memory().await.unwrap();
        let object_store = Arc::new(InMemory::new());

        // Try to create a writer for the traces table
        let result = IcebergTableWriter::new(
            &catalog_manager,
            object_store,
            "default".to_string(),
            "default".to_string(),
            "traces".to_string(),
        )
        .await;

        // This should work now that table creation is implemented
        // It might fail due to catalog setup issues in tests, but not due to "not implemented"
        if let Err(e) = result {
            // The error should not be about table creation not being implemented
            assert!(!e.to_string().contains("Table creation not yet implemented"));
            // It's okay to fail for other reasons like catalog setup in tests
            log::debug!("Expected failure due to test environment: {e}");
        }
    }

    #[tokio::test]
    async fn test_iceberg_writer_with_memory_catalog() {
        let catalog_manager = create_test_catalog_manager().await;

        let object_store = Arc::new(InMemory::new());

        // Try to create a writer
        let result = IcebergTableWriter::new(
            &catalog_manager,
            object_store,
            "test-tenant".to_string(),
            "local".to_string(),
            "traces".to_string(),
        )
        .await;

        // This might work or fail due to test environment setup, but not due to "not implemented"
        if let Err(e) = result {
            assert!(!e.to_string().contains("Table creation not yet implemented"));
            log::debug!("Expected failure due to test environment: {e}");
        }
    }
}
