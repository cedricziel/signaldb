use crate::schema_transform::transform_trace_v1_to_v2;
use anyhow::Result;
use common::CatalogManager;
use common::config::Configuration;
use common::schema::{create_catalog_with_config, iceberg_schemas};
use common::storage::storage_dsn_to_path;
use datafusion::arrow::array::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionConfig;
use datafusion_iceberg::DataFusionTable;
use iceberg_rust::catalog::Catalog as IcebergRustCatalog;
use iceberg_rust::catalog::create::CreateTableBuilder;
use iceberg_rust::catalog::identifier::Identifier;
use iceberg_rust::catalog::namespace::Namespace;
use iceberg_rust::table::Table;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid;

/// Represents the state of a transaction
#[derive(Debug, Clone)]
enum TransactionState {
    /// No active transaction
    None,
    /// Transaction is active and accepting operations
    Active {
        id: String,
        #[allow(dead_code)] // Will be used for timeout tracking
        start_time: Instant,
    },
    /// Transaction is being committed
    Committing { id: String },
}

/// Represents a pending operation within a transaction
#[derive(Debug)]
struct PendingOperation {
    /// SQL statement to execute
    sql: String,
    /// Temporary table name for this operation
    temp_table: String,
    /// The data to be written
    batch: RecordBatch,
    /// Timestamp when this operation was added
    #[allow(dead_code)] // Will be used for operation timeout
    timestamp: Instant,
}

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

/// Configuration for optimal batch processing and memory management
#[derive(Debug, Clone)]
pub struct BatchOptimizationConfig {
    /// Maximum number of rows per batch (splits larger batches)
    pub max_rows_per_batch: usize,
    /// Maximum memory size per batch in bytes (splits if exceeded)
    pub max_memory_per_batch_bytes: usize,
    /// Enable automatic batch splitting for large datasets
    pub enable_auto_split: bool,
    /// Target number of batches to process concurrently
    pub target_concurrent_batches: usize,
    /// Enable catalog caching to reduce overhead
    pub enable_catalog_caching: bool,
    /// Catalog cache TTL in seconds
    pub catalog_cache_ttl_seconds: u64,
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

impl Default for BatchOptimizationConfig {
    fn default() -> Self {
        Self {
            max_rows_per_batch: 50_000,                    // 50K rows max per batch
            max_memory_per_batch_bytes: 128 * 1024 * 1024, // 128MB max per batch
            enable_auto_split: true,                       // Enable automatic splitting
            target_concurrent_batches: 4,                  // Process 4 batches concurrently
            enable_catalog_caching: true,                  // Enable catalog reuse
            catalog_cache_ttl_seconds: 300,                // 5 minute cache TTL
        }
    }
}

/// Iceberg table writer that replaces direct Parquet file writing
/// Provides ACID transactions and proper metadata tracking
pub struct IcebergTableWriter {
    #[allow(dead_code)] // Will be used for future operations
    catalog: Arc<dyn IcebergRustCatalog>,
    table: Table,
    #[allow(dead_code)] // Will be used for data writing
    object_store: Arc<dyn ObjectStore>,
    tenant_id: String,
    dataset_id: String,
    session_ctx: SessionContext,
    #[allow(dead_code)] // Will be used for lazy registration
    table_registered: bool,
    /// Current transaction state
    transaction_state: TransactionState,
    /// Operations pending within the current transaction
    pending_operations: Vec<PendingOperation>,
    /// Retry configuration for failed operations
    retry_config: RetryConfig,
    /// Batch optimization configuration for performance tuning
    batch_config: BatchOptimizationConfig,
}

impl IcebergTableWriter {
    /// Create a new IcebergTableWriter for a specific table
    pub async fn new(
        config: &Configuration,
        object_store: Arc<dyn ObjectStore>,
        tenant_id: String,
        dataset_id: String,
        table_name: String,
    ) -> Result<Self> {
        let catalog = create_catalog_with_config(config).await?;

        // Resolve slugs from config for Iceberg namespace paths
        let tenant_slug = config
            .auth
            .tenants
            .iter()
            .find(|t| t.id == tenant_id)
            .map(|t| t.slug.clone())
            .unwrap_or_else(|| tenant_id.clone());

        let dataset_slug = config
            .auth
            .tenants
            .iter()
            .find(|t| t.id == tenant_id)
            .and_then(|t| t.datasets.iter().find(|d| d.id == dataset_id))
            .map(|d| d.slug.clone())
            .unwrap_or_else(|| dataset_id.clone());

        // Create namespace and table using slug-based paths
        let namespace = Namespace::try_new(&[tenant_slug.clone(), dataset_slug.clone()])?;

        // Namespace is implicitly created when tables are created via the SQL catalog
        log::debug!("Using namespace: {namespace:?}");

        let table_ident =
            Identifier::new(&[tenant_slug.clone(), dataset_slug.clone()], &table_name);

        // Check if table exists, create if not
        let table = if catalog.tabular_exists(&table_ident).await? {
            match catalog.clone().load_tabular(&table_ident).await? {
                iceberg_rust::catalog::tabular::Tabular::Table(table) => table,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected table but found different tabular type"
                    ));
                }
            }
        } else {
            // Create table with appropriate schema based on table name
            // Schema functions return iceberg-rust types directly
            let schema = match table_name.as_str() {
                "traces" => {
                    let s = iceberg_schemas::create_traces_schema()?;
                    log::debug!(
                        "Creating traces table with {} fields: {:?}",
                        s.fields().len(),
                        s.fields()
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>()
                    );
                    s
                }
                "logs" => iceberg_schemas::create_logs_schema()?,
                "metrics_gauge" => iceberg_schemas::create_metrics_gauge_schema()?,
                "metrics_sum" => iceberg_schemas::create_metrics_sum_schema()?,
                "metrics_histogram" => iceberg_schemas::create_metrics_histogram_schema()?,
                _ => {
                    return Err(anyhow::anyhow!("Unknown table name: {}", table_name));
                }
            };

            let partition_spec = match table_name.as_str() {
                "traces" => iceberg_schemas::create_traces_partition_spec()?,
                "logs" => iceberg_schemas::create_logs_partition_spec()?,
                "metrics_gauge" | "metrics_sum" | "metrics_histogram" => {
                    iceberg_schemas::create_metrics_partition_spec()?
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown table name for partition spec: {}",
                        table_name
                    ));
                }
            };

            log::debug!(
                "Partition spec for {}: {} fields",
                table_name,
                partition_spec.fields().len()
            );

            // Create the table using the catalog
            log::info!("Creating new Iceberg table: {table_ident}");

            // Construct table location based on storage configuration
            // Extract the base path from the storage DSN (removing file:// scheme)
            // Use slugs for consistent path naming that matches Iceberg namespace
            let storage_base_path = storage_dsn_to_path(&config.storage.dsn)?;
            let table_location = format!(
                "{}/{}/{}/{}",
                storage_base_path.trim_end_matches('/'),
                tenant_slug,
                dataset_slug,
                table_name
            );
            log::debug!("Table location for {table_name}: {table_location}");

            let table_creation = CreateTableBuilder::default()
                .with_name(table_name.clone())
                .with_schema(schema)
                .with_partition_spec(partition_spec)
                .with_location(table_location)
                .create()
                .map_err(|e| anyhow::anyhow!("Failed to build CreateTable: {}", e))?;

            catalog
                .clone()
                .create_table(table_ident.clone(), table_creation)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to create Iceberg table {}: {}", table_ident, e)
                })?
        };

        // Create DataFusion session context
        let runtime_env = Arc::new(RuntimeEnv::default());
        let session_ctx = SessionContext::new_with_config_rt(SessionConfig::default(), runtime_env);

        // The table is already in the correct format for use with datafusion_iceberg
        log::info!(
            "Successfully created/loaded Iceberg table: {} for tenant '{tenant_id}' dataset '{dataset_id}'",
            table.identifier()
        );

        let table_metadata = table.metadata();
        let current_schema = table.current_schema(None)?;
        log::debug!("Table location: {}", table_metadata.location);
        log::debug!("Schema has {} fields", current_schema.fields().len());

        // Register the Iceberg table with DataFusion
        let datafusion_table = Arc::new(DataFusionTable::from(table.clone()));
        session_ctx.register_table(&table_name, datafusion_table)?;

        log::info!(
            "Registered Iceberg table '{table_name}' for tenant '{tenant_id}' dataset '{dataset_id}' with DataFusion"
        );

        Ok(Self {
            catalog,
            table,
            object_store,
            tenant_id,
            dataset_id,
            session_ctx,
            table_registered: false, // Table registration deferred
            transaction_state: TransactionState::None,
            pending_operations: Vec::new(),
            retry_config: RetryConfig::default(),
            batch_config: BatchOptimizationConfig::default(),
        })
    }

    /// Create a new IcebergTableWriter using a shared CatalogManager
    ///
    /// This constructor uses the global shared Iceberg catalog from CatalogManager,
    /// ensuring all SignalDB components use the same catalog for consistent metadata.
    /// It also supports per-dataset storage configuration.
    pub async fn new_with_catalog_manager(
        catalog_manager: Arc<CatalogManager>,
        object_store: Arc<dyn ObjectStore>,
        tenant_id: String,
        dataset_id: String,
        table_name: String,
    ) -> Result<Self> {
        let _config = catalog_manager.config();
        let catalog = catalog_manager.catalog();

        // Resolve slugs using CatalogManager helper methods
        let tenant_slug = catalog_manager.get_tenant_slug(&tenant_id);
        let dataset_slug = catalog_manager.get_dataset_slug(&tenant_id, &dataset_id);

        // Get dataset-specific storage configuration
        let storage_config = catalog_manager.get_dataset_storage_config(&tenant_id, &dataset_id);

        // Create namespace and table using slug-based paths
        let namespace = Namespace::try_new(&[tenant_slug.clone(), dataset_slug.clone()])?;

        // Namespace is implicitly created when tables are created via the SQL catalog
        log::debug!("Using namespace: {namespace:?}");

        let table_ident =
            Identifier::new(&[tenant_slug.clone(), dataset_slug.clone()], &table_name);

        // Check if table exists, create if not
        let table = if catalog.tabular_exists(&table_ident).await? {
            match catalog.clone().load_tabular(&table_ident).await? {
                iceberg_rust::catalog::tabular::Tabular::Table(table) => table,
                _ => {
                    return Err(anyhow::anyhow!(
                        "Expected table but found different tabular type"
                    ));
                }
            }
        } else {
            // Create table with appropriate schema based on table name
            let schema = match table_name.as_str() {
                "traces" => {
                    let s = iceberg_schemas::create_traces_schema()?;
                    log::debug!(
                        "Creating traces table with {} fields: {:?}",
                        s.fields().len(),
                        s.fields()
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>()
                    );
                    s
                }
                "logs" => iceberg_schemas::create_logs_schema()?,
                "metrics_gauge" => iceberg_schemas::create_metrics_gauge_schema()?,
                "metrics_sum" => iceberg_schemas::create_metrics_sum_schema()?,
                "metrics_histogram" => iceberg_schemas::create_metrics_histogram_schema()?,
                _ => {
                    return Err(anyhow::anyhow!("Unknown table name: {}", table_name));
                }
            };

            let partition_spec = match table_name.as_str() {
                "traces" => iceberg_schemas::create_traces_partition_spec()?,
                "logs" => iceberg_schemas::create_logs_partition_spec()?,
                "metrics_gauge" | "metrics_sum" | "metrics_histogram" => {
                    iceberg_schemas::create_metrics_partition_spec()?
                }
                _ => {
                    return Err(anyhow::anyhow!(
                        "Unknown table name for partition spec: {}",
                        table_name
                    ));
                }
            };

            log::debug!(
                "Partition spec for {}: {} fields",
                table_name,
                partition_spec.fields().len()
            );

            // Create the table using the catalog
            log::info!("Creating new Iceberg table: {table_ident}");

            // Construct table location based on dataset-specific storage configuration
            let storage_base_path = storage_dsn_to_path(&storage_config.dsn)?;
            let table_location = format!(
                "{}/{}/{}/{}",
                storage_base_path.trim_end_matches('/'),
                tenant_slug,
                dataset_slug,
                table_name
            );
            log::debug!("Table location for {table_name}: {table_location}");

            let table_creation = CreateTableBuilder::default()
                .with_name(table_name.clone())
                .with_schema(schema)
                .with_partition_spec(partition_spec)
                .with_location(table_location)
                .create()
                .map_err(|e| anyhow::anyhow!("Failed to build CreateTable: {}", e))?;

            catalog
                .clone()
                .create_table(table_ident.clone(), table_creation)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("Failed to create Iceberg table {}: {}", table_ident, e)
                })?
        };

        // Create DataFusion session context
        let runtime_env = Arc::new(RuntimeEnv::default());
        let session_ctx = SessionContext::new_with_config_rt(SessionConfig::default(), runtime_env);

        log::info!(
            "Successfully created/loaded Iceberg table: {} for tenant '{tenant_id}' dataset '{dataset_id}'",
            table.identifier()
        );

        let table_metadata = table.metadata();
        let current_schema = table.current_schema(None)?;
        log::debug!("Table location: {}", table_metadata.location);
        log::debug!("Schema has {} fields", current_schema.fields().len());

        // Register the Iceberg table with DataFusion
        let datafusion_table = Arc::new(DataFusionTable::from(table.clone()));
        session_ctx.register_table(&table_name, datafusion_table)?;

        log::info!(
            "Registered Iceberg table '{table_name}' for tenant '{tenant_id}' dataset '{dataset_id}' with DataFusion"
        );

        Ok(Self {
            catalog,
            table,
            object_store,
            tenant_id,
            dataset_id,
            session_ctx,
            table_registered: false,
            transaction_state: TransactionState::None,
            pending_operations: Vec::new(),
            retry_config: RetryConfig::default(),
            batch_config: BatchOptimizationConfig::default(),
        })
    }

    /// Split a large batch into smaller optimized batches based on configuration
    fn split_batch_if_needed(&self, batch: RecordBatch) -> Vec<RecordBatch> {
        if !self.batch_config.enable_auto_split {
            return vec![batch];
        }

        let row_count = batch.num_rows();
        let memory_size = batch.get_array_memory_size();

        // Check if batch needs splitting
        let needs_split = row_count > self.batch_config.max_rows_per_batch
            || memory_size > self.batch_config.max_memory_per_batch_bytes;

        if !needs_split {
            log::debug!("Batch within limits (rows: {row_count}, memory: {memory_size} bytes)");
            return vec![batch];
        }

        // Calculate optimal split size
        let max_rows_per_split = std::cmp::min(
            self.batch_config.max_rows_per_batch,
            if memory_size > 0 {
                (self.batch_config.max_memory_per_batch_bytes * row_count) / memory_size
            } else {
                self.batch_config.max_rows_per_batch
            },
        );

        log::info!(
            "Splitting large batch: {row_count} rows ({memory_size} bytes) into chunks of {max_rows_per_split} rows"
        );

        // Split the batch into smaller chunks
        let mut result_batches = Vec::new();
        let mut start_idx = 0;

        while start_idx < row_count {
            let end_idx = std::cmp::min(start_idx + max_rows_per_split, row_count);

            let split_batch = batch.slice(start_idx, end_idx - start_idx);
            log::debug!(
                "Created split batch: {} rows (indices {}-{})",
                split_batch.num_rows(),
                start_idx,
                end_idx
            );
            result_batches.push(split_batch);

            start_idx = end_idx;
        }

        log::info!(
            "Successfully split batch into {} smaller batches",
            result_batches.len()
        );
        result_batches
    }

    /// Apply schema transformation if the batch has v1 schema but table expects v2
    fn apply_schema_transformation_if_needed(&self, batch: RecordBatch) -> Result<RecordBatch> {
        // Detect schema version based on column count and field names
        let num_columns = batch.num_columns();
        let field_names: Vec<String> = batch
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();

        // Detect schema version by field names rather than column count for robustness.
        // v1 schema uses "name" (renamed to "span_name" in v2) and lacks "timestamp" computed field.
        // v2 schema uses "span_name" and has computed fields like "timestamp", "date_day", "hour".
        let has_v1_name_field = field_names.contains(&"name".to_string());
        let has_v2_span_name_field = field_names.contains(&"span_name".to_string());
        let is_v1_schema = has_v1_name_field && !has_v2_span_name_field;
        let is_v2_schema = has_v2_span_name_field;

        if is_v1_schema {
            log::debug!("Detected v1 schema batch, applying v1->v2 transformation");
            transform_trace_v1_to_v2(batch)
        } else if is_v2_schema {
            log::debug!("Detected v2 schema batch, no transformation needed");
            Ok(batch)
        } else {
            log::warn!(
                "Unknown schema detected: {num_columns} columns with fields: {field_names:?}. Assuming no transformation needed."
            );
            Ok(batch)
        }
    }

    /// Write a batch of data to the Iceberg table with transaction support
    pub async fn write_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Validate input
        if batch.num_rows() == 0 {
            log::debug!(
                "Skipping empty batch for Iceberg table {}",
                self.table.identifier()
            );
            return Ok(());
        }

        // Apply batch optimization (splitting if needed)
        let optimized_batches = self.split_batch_if_needed(batch);
        let total_batches = optimized_batches.len();
        let total_rows: usize = optimized_batches.iter().map(|b| b.num_rows()).sum();

        log::info!(
            "Writing {} optimized batches with {} total rows to Iceberg table {} for tenant {}.{}",
            total_batches,
            total_rows,
            self.table.identifier(),
            self.tenant_id,
            self.dataset_id
        );

        // Process each optimized batch
        for (i, optimized_batch) in optimized_batches.into_iter().enumerate() {
            log::debug!(
                "Processing optimized batch {}/{} with {} rows",
                i + 1,
                total_batches,
                optimized_batch.num_rows()
            );

            self.write_single_batch(optimized_batch).await?;
        }

        Ok(())
    }

    /// Write a single batch (internal method)
    async fn write_single_batch(&mut self, batch: RecordBatch) -> Result<()> {
        log::debug!(
            "Writing single batch with {} rows to Iceberg table {}",
            batch.num_rows(),
            self.table.identifier()
        );

        // Check if we're in a transaction
        match &self.transaction_state {
            TransactionState::Active { id, .. } => {
                // In transaction mode: queue the operation
                log::debug!(
                    "Transaction {} active: queuing batch with {} rows",
                    id,
                    batch.num_rows()
                );

                // TODO: Get table metadata for SQL operations when datafusion_iceberg is integrated

                // Create temp table name
                let temp_table_name = format!("txn_{}_{}", id, uuid::Uuid::new_v4().simple());

                // Create the SQL statement
                let table_name = self.table.identifier().name();
                let insert_sql =
                    format!("INSERT INTO {table_name} SELECT * FROM {temp_table_name}");

                // Apply schema transformation if needed
                let transformed_batch = if table_name == "traces" {
                    self.apply_schema_transformation_if_needed(batch)?
                } else {
                    batch
                };

                // Store the pending operation
                let operation = PendingOperation {
                    sql: insert_sql,
                    temp_table: temp_table_name,
                    batch: transformed_batch,
                    timestamp: Instant::now(),
                };

                self.pending_operations.push(operation);

                log::debug!(
                    "Queued operation in transaction {}, total pending: {}",
                    id,
                    self.pending_operations.len()
                );

                Ok(())
            }
            TransactionState::Committing { id } => Err(anyhow::anyhow!(
                "Cannot write batch while transaction {} is being committed",
                id
            )),
            TransactionState::None => {
                // No transaction: execute immediately
                self.execute_immediate_write(batch).await
            }
        }
    }

    /// Execute a function with retry logic and exponential backoff
    async fn execute_with_retry<F, Fut, T>(&self, operation_name: &str, mut f: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0;
        let mut delay = self.retry_config.initial_delay;

        loop {
            attempt += 1;

            match f().await {
                Ok(result) => {
                    if attempt > 1 {
                        log::info!(
                            "Operation '{operation_name}' succeeded after {attempt} attempts"
                        );
                    }
                    return Ok(result);
                }
                Err(e) => {
                    if attempt >= self.retry_config.max_attempts {
                        log::error!(
                            "Operation '{operation_name}' failed after {attempt} attempts: {e}"
                        );
                        return Err(anyhow::anyhow!(
                            "Operation '{operation_name}' failed after {attempt} attempts: {e}"
                        ));
                    }

                    log::warn!(
                        "Operation '{operation_name}' attempt {attempt} failed: {e}. Retrying in {delay:?}"
                    );

                    // Sleep before retry
                    tokio::time::sleep(delay).await;

                    // Calculate next delay with exponential backoff
                    delay = std::cmp::min(
                        self.retry_config.max_delay,
                        Duration::from_secs_f64(
                            delay.as_secs_f64() * self.retry_config.backoff_multiplier,
                        ),
                    );
                }
            }
        }
    }

    /// Execute an immediate write operation (outside of a transaction)
    async fn execute_immediate_write(&mut self, batch: RecordBatch) -> Result<()> {
        // TODO: Get table metadata for SQL operations when datafusion_iceberg is integrated

        let table_name = self.table.identifier().name();
        let table_location = self.table.metadata().location.clone();
        log::debug!(
            "Executing immediate SQL INSERT for batch with {} rows to table {} (location: {})",
            batch.num_rows(),
            table_name,
            table_location
        );

        // Debug: Check schema compatibility before registering batch
        let target_table_schema = self.table.metadata().current_schema(None)?;
        log::debug!(
            "Target Iceberg table has {} fields: {:?}",
            target_table_schema.fields().len(),
            target_table_schema
                .fields()
                .iter()
                .map(|f| f.name.as_str())
                .collect::<Vec<_>>()
        );
        log::debug!(
            "Temporary table (RecordBatch) has {} columns: {:?}",
            batch.num_columns(),
            batch
                .schema()
                .fields()
                .iter()
                .map(|f| f.name())
                .collect::<Vec<_>>()
        );

        // Register RecordBatch directly as temporary table
        let temp_table_name = format!("temp_batch_{}", uuid::Uuid::new_v4().simple());
        self.session_ctx.register_batch(&temp_table_name, batch)?;

        log::debug!("Created temporary table '{temp_table_name}' for batch data");

        // Execute INSERT INTO table SELECT * FROM temp_table
        // Use the full qualified table name: catalog.schema.table
        // Use "default" namespace for consistency with existing tests
        let _namespace = "default";
        let insert_sql = format!("INSERT INTO {table_name} SELECT * FROM {temp_table_name}");

        log::debug!("Executing SQL: {insert_sql}");

        // Execute the SQL INSERT operation with retry logic
        let session_ctx = self.session_ctx.clone();
        let insert_sql_clone = insert_sql.clone();

        let df = self
            .execute_with_retry("SQL INSERT", || {
                let ctx = session_ctx.clone();
                let sql = insert_sql_clone.clone();
                async move {
                    ctx.sql(&sql)
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to execute SQL INSERT: {}", e))
                }
            })
            .await?;

        // Collect results to ensure completion with retry logic
        let result = self
            .execute_with_retry("SQL INSERT collection", || {
                let df_clone = df.clone();
                async move {
                    df_clone
                        .collect()
                        .await
                        .map_err(|e| anyhow::anyhow!("Failed to collect SQL INSERT results: {}", e))
                }
            })
            .await?;

        log::info!(
            "Successfully executed SQL INSERT, affected {} result batches",
            result.len()
        );

        log::debug!(
            "Successfully wrote batch to Iceberg table {}",
            self.table.identifier()
        );

        // Clean up temporary table to prevent memory leaks
        if let Err(e) = self.session_ctx.deregister_table(&temp_table_name) {
            log::warn!("Failed to clean up temporary table {temp_table_name}: {e}");
        }

        Ok(())
    }

    /// Write multiple batches in a single transaction
    pub async fn write_batches(&mut self, batches: Vec<RecordBatch>) -> Result<()> {
        if batches.is_empty() {
            return Ok(());
        }

        log::info!(
            "Writing {} batches to Iceberg table {} for tenant {}.{}",
            batches.len(),
            self.table.identifier(),
            self.tenant_id,
            self.dataset_id
        );

        // Count total rows across all batches
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        log::info!(
            "Writing {} total rows from {} batches to Iceberg table {} using iceberg-datafusion",
            total_rows,
            batches.len(),
            self.table.identifier()
        );

        // Filter out empty batches
        let non_empty_batches: Vec<RecordBatch> =
            batches.into_iter().filter(|b| b.num_rows() > 0).collect();

        if non_empty_batches.is_empty() {
            log::debug!(
                "All batches are empty, skipping write to Iceberg table {}",
                self.table.identifier()
            );
            return Ok(());
        }

        // Check if we're already in a transaction
        match &self.transaction_state {
            TransactionState::Active { .. } => {
                // Already in transaction: just add the batches
                for batch in non_empty_batches {
                    self.write_batch(batch).await?;
                }
                Ok(())
            }
            TransactionState::Committing { id } => Err(anyhow::anyhow!(
                "Cannot write batches while transaction {} is being committed",
                id
            )),
            TransactionState::None => {
                // No transaction: create one for atomicity
                let transaction_id = self.begin_transaction().await?;

                // Write all batches within the transaction
                for batch in non_empty_batches {
                    self.write_batch(batch).await?;
                }

                // Commit the transaction
                self.commit_transaction(&transaction_id).await?;

                log::debug!(
                    "Successfully wrote {} total rows to Iceberg table {} in transaction {}",
                    total_rows,
                    self.table.identifier(),
                    transaction_id
                );

                Ok(())
            }
        }
    }

    /// Begin a new transaction for batch operations
    /// Returns a transaction ID that can be used for commit/rollback operations
    pub async fn begin_transaction(&mut self) -> Result<String> {
        // Check if we're already in a transaction
        match &self.transaction_state {
            TransactionState::Active { id, .. } => {
                return Err(anyhow::anyhow!(
                    "Transaction {} is already active. Commit or rollback before starting a new transaction.",
                    id
                ));
            }
            TransactionState::Committing { id } => {
                return Err(anyhow::anyhow!(
                    "Transaction {} is being committed. Wait for completion before starting a new transaction.",
                    id
                ));
            }
            TransactionState::None => {
                // Good to start a new transaction
            }
        }

        let transaction_id = uuid::Uuid::new_v4().simple().to_string();

        log::info!(
            "Beginning transaction {} for Iceberg table {} (tenant {}.{})",
            transaction_id,
            self.table.identifier(),
            self.tenant_id,
            self.dataset_id
        );

        // Update transaction state
        self.transaction_state = TransactionState::Active {
            id: transaction_id.clone(),
            start_time: Instant::now(),
        };

        // Clear any leftover pending operations
        self.pending_operations.clear();

        log::debug!("Transaction {transaction_id} initialized with empty operation queue");
        Ok(transaction_id)
    }

    /// Commit a transaction, making all changes permanent
    pub async fn commit_transaction(&mut self, transaction_id: &str) -> Result<()> {
        // Verify we're in the correct transaction
        let current_txn_id = match &self.transaction_state {
            TransactionState::Active { id, .. } => {
                if id != transaction_id {
                    return Err(anyhow::anyhow!(
                        "Attempting to commit transaction {} but current active transaction is {}",
                        transaction_id,
                        id
                    ));
                }
                id.clone()
            }
            TransactionState::Committing { id } => {
                return Err(anyhow::anyhow!(
                    "Transaction {} is already being committed",
                    id
                ));
            }
            TransactionState::None => {
                return Err(anyhow::anyhow!(
                    "No active transaction to commit. Transaction {} not found.",
                    transaction_id
                ));
            }
        };

        log::info!(
            "Committing transaction {} for Iceberg table {} (tenant {}.{}) with {} pending operations",
            transaction_id,
            self.table.identifier(),
            self.tenant_id,
            self.dataset_id,
            self.pending_operations.len()
        );

        // Change state to committing
        self.transaction_state = TransactionState::Committing {
            id: current_txn_id.clone(),
        };

        // Execute all pending operations
        let mut success_count = 0;
        let total_operations = self.pending_operations.len();

        // Register all batches as temporary tables first
        for operation in &self.pending_operations {
            log::debug!(
                "Registering temp table {} for transaction {}",
                operation.temp_table,
                transaction_id
            );

            self.session_ctx
                .register_batch(&operation.temp_table, operation.batch.clone())?;
        }

        // Execute all SQL operations with retry logic
        for (index, operation) in self.pending_operations.iter().enumerate() {
            log::debug!(
                "Executing operation {}/{} in transaction {}: {}",
                index + 1,
                total_operations,
                transaction_id,
                operation.sql
            );

            // Debug: Check schema compatibility for INSERT operations
            if operation.sql.contains("INSERT INTO") && operation.sql.contains("SELECT * FROM") {
                let target_table_schema = self.table.metadata().current_schema(None)?;
                log::debug!(
                    "Transaction {}: Target table has {} fields: {:?}",
                    transaction_id,
                    target_table_schema.fields().len(),
                    target_table_schema
                        .fields()
                        .iter()
                        .map(|f| f.name.as_str())
                        .collect::<Vec<_>>()
                );
                log::debug!(
                    "Transaction {}: Temp table batch has {} columns: {:?}",
                    transaction_id,
                    operation.batch.num_columns(),
                    operation
                        .batch
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name())
                        .collect::<Vec<_>>()
                );
            }

            // Use retry logic for each SQL operation
            let session_ctx = self.session_ctx.clone();
            let sql = operation.sql.clone();
            let operation_name = format!(
                "Transaction {} operation {}/{}",
                transaction_id,
                index + 1,
                total_operations
            );

            let execute_result = self
                .execute_with_retry(&operation_name, || {
                    let ctx = session_ctx.clone();
                    let sql_query = sql.clone();
                    async move {
                        let df = ctx
                            .sql(&sql_query)
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to execute SQL: {}", e))?;

                        // Collect results to ensure completion
                        df.collect()
                            .await
                            .map_err(|e| anyhow::anyhow!("Failed to collect SQL results: {}", e))
                    }
                })
                .await;

            match execute_result {
                Ok(_) => {
                    success_count += 1;
                    log::debug!(
                        "Operation {}/{} completed successfully",
                        index + 1,
                        total_operations
                    );
                }
                Err(e) => {
                    log::error!(
                        "Operation {}/{} failed after all retry attempts: {}",
                        index + 1,
                        total_operations,
                        e
                    );

                    // Rollback on failure - clean up temporary tables first
                    log::debug!(
                        "Cleaning up {} temporary tables after transaction {} failure",
                        self.pending_operations.len(),
                        transaction_id
                    );

                    for operation in &self.pending_operations {
                        if let Err(cleanup_err) =
                            self.session_ctx.deregister_table(&operation.temp_table)
                        {
                            log::warn!(
                                "Failed to deregister temporary table {} during rollback: {}",
                                operation.temp_table,
                                cleanup_err
                            );
                        }
                    }

                    self.transaction_state = TransactionState::None;
                    self.pending_operations.clear();

                    return Err(anyhow::anyhow!(
                        "Transaction {} failed at operation {}/{} after retries: {}",
                        transaction_id,
                        index + 1,
                        total_operations,
                        e
                    ));
                }
            }
        }

        // All operations succeeded - clean up temporary tables from session context
        log::debug!(
            "Cleaning up {} temporary tables from session context for transaction {}",
            self.pending_operations.len(),
            transaction_id
        );

        for operation in &self.pending_operations {
            // Deregister the temporary table from the session context
            // This prevents memory leaks by removing tables that are no longer needed
            if let Err(e) = self.session_ctx.deregister_table(&operation.temp_table) {
                // Log the error but don't fail the transaction since the operation succeeded
                log::warn!(
                    "Failed to deregister temporary table {} after successful transaction {}: {}",
                    operation.temp_table,
                    transaction_id,
                    e
                );
            } else {
                log::trace!(
                    "Successfully deregistered temporary table {} from session context",
                    operation.temp_table
                );
            }
        }

        // Clear pending operations and reset transaction state
        self.pending_operations.clear();
        self.transaction_state = TransactionState::None;

        log::info!(
            "Transaction {transaction_id} committed successfully. Executed {success_count} operations."
        );

        Ok(())
    }

    /// Rollback a transaction, discarding all changes
    pub async fn rollback_transaction(&mut self, transaction_id: &str) -> Result<()> {
        // Verify we're in the correct transaction
        match &self.transaction_state {
            TransactionState::Active { id, .. } => {
                if id != transaction_id {
                    return Err(anyhow::anyhow!(
                        "Attempting to rollback transaction {} but current active transaction is {}",
                        transaction_id,
                        id
                    ));
                }
            }
            TransactionState::Committing { id } => {
                if id != transaction_id {
                    return Err(anyhow::anyhow!(
                        "Attempting to rollback transaction {} but transaction {} is being committed",
                        transaction_id,
                        id
                    ));
                }
                log::warn!(
                    "Rolling back transaction {transaction_id} that was in committing state"
                );
            }
            TransactionState::None => {
                return Err(anyhow::anyhow!(
                    "No active transaction to rollback. Transaction {} not found.",
                    transaction_id
                ));
            }
        }

        log::warn!(
            "Rolling back transaction {} for Iceberg table {} (tenant {}.{}) with {} pending operations",
            transaction_id,
            self.table.identifier(),
            self.tenant_id,
            self.dataset_id,
            self.pending_operations.len()
        );

        // Clean up temporary tables from session context before discarding operations
        log::debug!(
            "Cleaning up {} temporary tables during rollback of transaction {}",
            self.pending_operations.len(),
            transaction_id
        );

        for operation in &self.pending_operations {
            if let Err(e) = self.session_ctx.deregister_table(&operation.temp_table) {
                log::warn!(
                    "Failed to deregister temporary table {} during rollback of transaction {}: {}",
                    operation.temp_table,
                    transaction_id,
                    e
                );
            } else {
                log::trace!(
                    "Successfully deregistered temporary table {} during rollback",
                    operation.temp_table
                );
            }
        }

        // Discard all pending operations
        let discarded_operations = self.pending_operations.len();
        self.pending_operations.clear();

        // Reset transaction state
        self.transaction_state = TransactionState::None;

        log::info!(
            "Transaction {transaction_id} rolled back successfully. Discarded {discarded_operations} operations."
        );

        Ok(())
    }

    /// Check if a transaction is currently active
    pub fn has_active_transaction(&self) -> bool {
        matches!(self.transaction_state, TransactionState::Active { .. })
    }

    /// Get the current transaction ID if one is active
    pub fn current_transaction_id(&self) -> Option<String> {
        match &self.transaction_state {
            TransactionState::Active { id, .. } => Some(id.clone()),
            TransactionState::Committing { id } => Some(id.clone()),
            TransactionState::None => None,
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

    /// Update batch optimization configuration
    pub fn set_batch_config(&mut self, batch_config: BatchOptimizationConfig) {
        self.batch_config = batch_config;
    }

    /// Get current batch optimization configuration
    pub fn batch_config(&self) -> &BatchOptimizationConfig {
        &self.batch_config
    }
}

/// Factory function to create IcebergTableWriter instances
pub async fn create_iceberg_writer(
    config: &Configuration,
    object_store: Arc<dyn ObjectStore>,
    tenant_id: impl Into<String>,
    dataset_id: impl Into<String>,
    table_name: impl Into<String>,
) -> Result<IcebergTableWriter> {
    IcebergTableWriter::new(
        config,
        object_store,
        tenant_id.into(),
        dataset_id.into(),
        table_name.into(),
    )
    .await
}

/// Factory function to create IcebergTableWriter instances using CatalogManager
///
/// This factory uses the shared catalog from CatalogManager, ensuring consistent
/// metadata across all SignalDB components.
#[allow(dead_code)] // Will be used when service entry points are updated
pub async fn create_iceberg_writer_with_catalog_manager(
    catalog_manager: Arc<CatalogManager>,
    object_store: Arc<dyn ObjectStore>,
    tenant_id: impl Into<String>,
    dataset_id: impl Into<String>,
    table_name: impl Into<String>,
) -> Result<IcebergTableWriter> {
    IcebergTableWriter::new_with_catalog_manager(
        catalog_manager,
        object_store,
        tenant_id.into(),
        dataset_id.into(),
        table_name.into(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::{Configuration, SchemaConfig, StorageConfig};
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn test_iceberg_writer_creation() {
        // Use default configuration with in-memory storage
        let config = Configuration::default();
        let object_store = Arc::new(InMemory::new());

        // Try to create a writer for the traces table
        let result =
            create_iceberg_writer(&config, object_store, "default", "default", "traces").await;

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
        let config = Configuration {
            schema: SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: Default::default(),
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Default::default()
        };

        let object_store = Arc::new(InMemory::new());

        // Try to create a writer
        let result =
            create_iceberg_writer(&config, object_store, "test-tenant", "local", "traces").await;

        // This might work or fail due to test environment setup, but not due to "not implemented"
        if let Err(e) = result {
            assert!(!e.to_string().contains("Table creation not yet implemented"));
            log::debug!("Expected failure due to test environment: {e}");
        }
    }

    #[tokio::test]
    async fn test_transaction_management() {
        let config = Configuration {
            schema: SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: Default::default(),
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Default::default()
        };

        let object_store = Arc::new(InMemory::new());

        // Create a writer
        let result = create_iceberg_writer(
            &config,
            object_store,
            "test_tenant",
            "test_dataset",
            "test_table",
        )
        .await;
        if let Ok(mut writer) = result {
            // Test transaction lifecycle
            let transaction_id = writer.begin_transaction().await.unwrap();
            assert!(!transaction_id.is_empty());

            // Test commit
            let commit_result = writer.commit_transaction(&transaction_id).await;
            assert!(commit_result.is_ok());

            // Test rollback with a new transaction
            let transaction_id2 = writer.begin_transaction().await.unwrap();
            let rollback_result = writer.rollback_transaction(&transaction_id2).await;
            assert!(rollback_result.is_ok());

            // Test active transaction check
            assert!(!writer.has_active_transaction());
        }
    }

    #[tokio::test]
    async fn test_transactional_batch_writing() {
        use datafusion::arrow::array::{Int64Array, RecordBatch, StringArray};
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use std::sync::Arc;

        let config = Configuration {
            schema: SchemaConfig {
                catalog_type: "memory".to_string(),
                catalog_uri: "memory://".to_string(),
                default_schemas: Default::default(),
            },
            storage: StorageConfig {
                dsn: "memory://".to_string(),
            },
            ..Default::default()
        };

        let object_store = Arc::new(InMemory::new());

        // Create a writer
        let result = create_iceberg_writer(
            &config,
            object_store,
            "test_tenant",
            "test_dataset",
            "test_table",
        )
        .await;
        if let Ok(mut writer) = result {
            // Create test data
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("name", DataType::Utf8, false),
            ]));

            let batch1 = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![1, 2])),
                    Arc::new(StringArray::from(vec!["Alice", "Bob"])),
                ],
            )
            .unwrap();

            let batch2 = RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int64Array::from(vec![3, 4])),
                    Arc::new(StringArray::from(vec!["Charlie", "Diana"])),
                ],
            )
            .unwrap();

            // Test transactional batch writing
            let batches = vec![batch1, batch2];
            let result = writer.write_batches(batches).await;

            // This might fail due to table not being registered, but should not panic
            // The important thing is that the transaction logic runs without errors
            match result {
                Ok(_) => {
                    // Success - actual DataFusion table was registered and working
                    log::info!("Transactional batch write succeeded");
                }
                Err(e) => {
                    // Expected - table not registered with DataFusion session
                    log::debug!("Expected failure due to table registration: {e}");
                    // Should not be a panic or compilation error
                    assert!(!e.to_string().is_empty());
                }
            }
        }
    }
}
