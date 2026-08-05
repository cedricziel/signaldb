//! Integration test for logs table compaction
//!
//! This test verifies that the compactor correctly consolidates small log files
//! and applies appropriate sorting for query performance.

use anyhow::{Context, Result};
use common::catalog_manager::CatalogManager;
use common::flight::conversion::conversion_logs::otlp_logs_to_arrow;
use compactor::executor::{CompactionExecutor, CompactionStatus, ExecutorConfig};
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use datafusion::arrow::array::{Array, Int64Array, StringArray};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::SessionContext;
use iceberg_rust::catalog::tabular::Tabular;
use object_store::memory::InMemory;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use std::sync::Arc;
use writer::IcebergTableWriter;

/// Initialize test logging
fn init_test_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Debug)
        .is_test(true)
        .try_init()
        .ok();
}

/// Creates a test resource with the given service name
fn make_resource(service_name: &str) -> Resource {
    Resource {
        attributes: vec![KeyValue {
            key_strindex: 0,
            key: "service.name".to_string(),
            value: Some(AnyValue {
                value: Some(Value::StringValue(service_name.to_string())),
            }),
        }],
        dropped_attributes_count: 0,
        entity_refs: vec![],
    }
}

/// Builds a logs request with multiple severity levels
fn build_logs_request_multi_severity(
    batch_num: usize,
    rows: usize,
    severities: &[&str],
) -> ExportLogsServiceRequest {
    let base_time = 1704067200000000000; // 2024-01-01 00:00:00 UTC in nanoseconds
    let service_names = ["frontend", "backend", "database"];

    let mut log_records = Vec::new();

    for i in 0..rows {
        let severity_idx = i % severities.len();
        let service_idx = i % service_names.len();

        // Severity numbers: ERROR=17, WARN=13, INFO=9, DEBUG=5
        let severity_number = match severities[severity_idx] {
            "ERROR" => 17,
            "WARN" => 13,
            "INFO" => 9,
            "DEBUG" => 5,
            _ => 9,
        };

        log_records.push(LogRecord {
            time_unix_nano: base_time + (batch_num * 1000 + i) as u64 * 1000000, // millisecond increments
            observed_time_unix_nano: base_time + (batch_num * 1000 + i) as u64 * 1000000,
            severity_number,
            severity_text: severities[severity_idx].to_string(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!(
                    "Log message {} from batch {}",
                    i, batch_num
                ))),
            }),
            attributes: vec![
                KeyValue {
                    key_strindex: 0,
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue(service_names[service_idx].to_string())),
                    }),
                },
                KeyValue {
                    key_strindex: 0,
                    key: "batch.number".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::IntValue(batch_num as i64)),
                    }),
                },
            ],
            dropped_attributes_count: 0,
            flags: 0,
            trace_id: vec![],
            span_id: vec![],
            event_name: String::new(),
        });
    }

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(make_resource("test-service")),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Creates a logs batch for testing
fn create_logs_batch(batch_num: usize, rows: usize) -> RecordBatch {
    let severities = ["ERROR", "WARN", "INFO", "DEBUG"];
    let request = build_logs_request_multi_severity(batch_num, rows, &severities);
    otlp_logs_to_arrow(&request).expect("conversion should succeed")
}

/// Test logs table compaction:
/// 1. Write 10 small batches with multiple severity levels
/// 2. Execute compaction
/// 3. Verify files are consolidated
/// 4. Verify sorting works correctly (timestamp ASC, service_name ASC, severity_text ASC)
#[tokio::test]
async fn test_logs_table_compaction() -> Result<()> {
    init_test_logging();

    log::info!("=== Starting logs compaction test ===");

    // Setup: Create in-memory catalog and object store
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "logs";

    // Phase 1: Create initial small files via Writer
    log::info!("Phase 1: Creating small log files");

    let writer_result = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await;

    let mut writer =
        writer_result.context("Failed to create IcebergTableWriter for in-memory test table")?;

    // Write 10 small batches (100 rows each = 1000 logs total)
    for i in 0..10 {
        let batch = create_logs_batch(i, 100);
        writer
            .append_batches_with_marker("seed", vec![(uuid::Uuid::new_v4(), batch)])
            .await
            .with_context(|| format!("Failed to write log batch {i}"))?;
        log::debug!("Wrote log batch {i}");
    }

    log::info!("Initial writes complete: 10 small batches created");

    // Phase 2: Setup and execute compaction
    log::info!("Phase 2: Setting up compaction executor");

    let executor_config = ExecutorConfig::default();
    let metrics = CompactionMetrics::new();
    let executor =
        CompactionExecutor::new(catalog_manager.clone(), executor_config, metrics.clone());

    // Create compaction candidate manually
    // In real scenario, this would come from the planner
    let candidate = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: "all".to_string(),
        stats: PartitionStats {
            file_count: 10,
            total_size_bytes: 10 * 5 * 1024 * 1024, // 10 files * ~5MB
            avg_file_size_bytes: 5 * 1024 * 1024,
        },
    };

    log::info!("Phase 3: Executing compaction");
    let result = executor.execute_candidate(candidate).await;

    // Phase 4: Verify results
    log::info!("Phase 4: Verifying results");

    let result = result.context("Compaction execution failed")?;
    log::info!("Compaction completed with status: {:?}", result.status);
    log::info!("Duration: {:?}", result.duration);
    log::info!(
        "Files: {} input -> {} output",
        result.input_files_count,
        result.output_files_count
    );

    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "Compaction must succeed: {:?}",
        result.error
    );
    assert!(
        result.error.is_none(),
        "Successful compaction must not report an error: {:?}",
        result.error
    );

    // Verify consolidation happened
    if result.input_files_count > 0 {
        assert!(
            result.output_files_count < result.input_files_count,
            "Should consolidate files: {} input files -> {} output files",
            result.input_files_count,
            result.output_files_count
        );
        log::info!("✓ File consolidation verified");
    }

    // Check metrics
    let summary = metrics.summary();
    log::info!("=== Compaction Metrics ===");
    log::info!("Jobs started: {}", summary.jobs_started);
    log::info!("Jobs succeeded: {}", summary.jobs_succeeded);
    log::info!("Jobs failed: {}", summary.jobs_failed);
    log::info!("Total input files: {}", summary.total_input_files);
    log::info!("Total output files: {}", summary.total_output_files);

    // Verify at least one job succeeded
    assert_eq!(
        summary.jobs_succeeded, 1,
        "Should have succeeded 1 compaction job"
    );

    log::info!("=== Logs compaction test completed successfully ===");
    Ok(())
}

/// Test that logs compaction produces data sorted by
/// (timestamp ASC, service_name ASC, severity_text ASC) for query
/// performance, per `ParquetRewriter::get_sort_columns`. Batches are
/// written deliberately out of timestamp order so that observing sorted
/// output afterward can only be explained by the compactor's sort step,
/// not by insertion order happening to already be sorted.
#[tokio::test]
async fn test_logs_compaction_with_sorting_verification() -> Result<()> {
    init_test_logging();

    log::info!("=== Starting logs sorting verification test ===");

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let tenant_id = "test-tenant";
    let dataset_id = "test-dataset";
    let table_name = "logs";

    // Create writer and write varied severity logs
    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        tenant_id.to_string(),
        dataset_id.to_string(),
        table_name.to_string(),
    )
    .await
    .context("Failed to create IcebergTableWriter for in-memory test table")?;

    // Write batches out of timestamp order (batch 9 first, batch 0 last).
    // `create_logs_batch` derives each row's timestamp from `batch_num`, so
    // inserting batches in descending `batch_num` order means the raw,
    // unsorted data files are NOT already timestamp-ordered by insertion
    // sequence. Any post-compaction ordering we observe below can therefore
    // only be explained by the compactor's explicit sort step, not by
    // insertion order happening to line up.
    for i in (0..10).rev() {
        let batch = create_logs_batch(i, 100);
        writer
            .append_batches_with_marker("seed", vec![(uuid::Uuid::new_v4(), batch)])
            .await
            .with_context(|| format!("Failed to write log batch {i}"))?;
    }

    log::info!("Wrote 10 batches (out of timestamp order) with mixed severities");

    // Execute compaction
    let executor_config = ExecutorConfig::default();
    let metrics = CompactionMetrics::new();
    let executor =
        CompactionExecutor::new(catalog_manager.clone(), executor_config, metrics.clone());

    let candidate = CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: table_name.to_string(),
        partition_id: "all".to_string(),
        stats: PartitionStats {
            file_count: 10,
            total_size_bytes: 10 * 5 * 1024 * 1024,
            avg_file_size_bytes: 5 * 1024 * 1024,
        },
    };

    let result = executor
        .execute_candidate(candidate)
        .await
        .context("Compaction execution failed")?;

    log::info!("Compaction result: {:?}", result.status);
    assert_eq!(
        result.status,
        CompactionStatus::Success,
        "Compaction must succeed: {:?}",
        result.error
    );
    assert!(
        result.error.is_none(),
        "Successful compaction must not report an error: {:?}",
        result.error
    );

    // Verify metrics tracked the compaction
    let summary = metrics.summary();
    assert_eq!(summary.jobs_started, 1);
    assert_eq!(summary.jobs_succeeded, 1);

    // Verify the compacted table's rows are actually sorted by
    // (timestamp ASC, service_name ASC, severity_text ASC), per
    // `ParquetRewriter::get_sort_columns` for the "logs" table. The SELECT
    // below has no ORDER BY, so this reflects the physical row order the
    // compactor wrote to Parquet, not an order DataFusion imposed for us.
    let identifier = catalog_manager.build_table_identifier(tenant_id, dataset_id, table_name);
    let tabular = catalog_manager
        .catalog()
        .load_tabular(&identifier)
        .await
        .context("Failed to load compacted logs table")?;
    let table = match tabular {
        Tabular::Table(table) => table,
        _ => anyhow::bail!("expected a table, got a view"),
    };

    let ctx = SessionContext::new();
    let provider = Arc::new(datafusion_iceberg::DataFusionTable::new(
        Tabular::Table(table),
        None,
        None,
        None,
    )) as Arc<dyn datafusion::datasource::TableProvider>;
    ctx.register_table(table_name, provider)?;

    let batches = ctx
        .sql("SELECT arrow_cast(timestamp, 'Int64') AS ts, service_name, severity_text FROM logs")
        .await?
        .collect()
        .await?;

    let mut prev: Option<(i64, String, String)> = None;
    let mut row_count = 0usize;
    for batch in &batches {
        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("timestamp column should cast to Int64Array");
        let service = batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("service_name column should be StringArray");
        let severity = batch
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("severity_text column should be StringArray");

        for row in 0..batch.num_rows() {
            let current = (
                ts.value(row),
                service.value(row).to_string(),
                severity.value(row).to_string(),
            );
            if let Some(prev_key) = &prev {
                assert!(
                    prev_key <= &current,
                    "compacted logs table is not sorted by (timestamp, service_name, severity_text): {prev_key:?} appears before {current:?}"
                );
            }
            prev = Some(current);
            row_count += 1;
        }
    }

    assert_eq!(
        row_count, 1000,
        "expected all 1000 rows to survive compaction"
    );

    log::info!("✓ Sorting verification test completed");

    Ok(())
}
