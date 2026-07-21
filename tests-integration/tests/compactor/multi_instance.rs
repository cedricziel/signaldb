//! Multi-instance compactor integration test (Phase 4)
//!
//! Runs two live compactor instances — distinct instance IDs with independent
//! lease-catalog connections over a shared SQLite database file — against the
//! same Iceberg tables, and verifies the multi-instance safety guarantees:
//!
//! 1. Two instances racing over the same candidate list never execute
//!    compaction for the same partition concurrently (lease mutual exclusion)
//! 2. Every candidate is compacted by exactly one of the racing instances
//!    within a scheduling cycle (work is shared, not duplicated)
//! 3. A partition whose lease holder crashes is taken over after lease
//!    expiry and the work still completes (`expire_stale` + re-acquire)
//!
//! Unlike the lease unit tests, this exercises the full acquire → execute →
//! release flow from `main.rs` with real Parquet rewrites, and coordinates
//! through separate SQLite connections the way separate processes would.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::Result;
use common::catalog::Catalog;
use common::catalog_manager::CatalogManager;
use common::flight::conversion::conversion_logs::otlp_logs_to_arrow;
use compactor::executor::{CompactionExecutor, ExecutorConfig};
use compactor::lease::LeaseManager;
use compactor::metrics::CompactionMetrics;
use compactor::planner::{CompactionCandidate, PartitionStats};
use object_store::memory::InMemory;
use opentelemetry_proto::tonic::{
    collector::logs::v1::ExportLogsServiceRequest,
    common::v1::{AnyValue, KeyValue, any_value::Value},
    logs::v1::{LogRecord, ResourceLogs, ScopeLogs},
    resource::v1::Resource,
};
use uuid::Uuid;
use writer::IcebergTableWriter;

fn init_test_logging() {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .is_test(true)
        .try_init()
        .ok();
}

fn build_logs_request(batch_num: usize, rows: usize) -> ExportLogsServiceRequest {
    let base_time = 1704067200000000000u64; // 2024-01-01 00:00:00 UTC
    let log_records: Vec<LogRecord> = (0..rows)
        .map(|i| LogRecord {
            time_unix_nano: base_time + (batch_num * rows + i) as u64 * 1_000_000,
            observed_time_unix_nano: base_time + (batch_num * rows + i) as u64 * 1_000_000,
            severity_number: 9,
            severity_text: "INFO".to_string(),
            body: Some(AnyValue {
                value: Some(Value::StringValue(format!(
                    "multi-instance test log {batch_num}/{i}"
                ))),
            }),
            ..Default::default()
        })
        .collect();

    ExportLogsServiceRequest {
        resource_logs: vec![ResourceLogs {
            resource: Some(Resource {
                attributes: vec![KeyValue {
                    key_strindex: 0,
                    key: "service.name".to_string(),
                    value: Some(AnyValue {
                        value: Some(Value::StringValue("multi-instance-test".to_string())),
                    }),
                }],
                dropped_attributes_count: 0,
                entity_refs: vec![],
            }),
            scope_logs: vec![ScopeLogs {
                scope: None,
                log_records,
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
    }
}

/// Write `batches` small log files into `tenant/dataset/logs`.
///
/// Returns false when the test environment cannot create the writer, in which
/// case the caller should skip (mirrors the other compactor tests).
async fn write_small_log_files(
    catalog_manager: &Arc<CatalogManager>,
    object_store: Arc<InMemory>,
    tenant_id: &str,
    dataset_id: &str,
    batches: usize,
) -> Result<bool> {
    let writer_result = IcebergTableWriter::new(
        catalog_manager,
        object_store,
        tenant_id.to_string(),
        dataset_id.to_string(),
        "logs".to_string(),
    )
    .await;

    let mut writer = match writer_result {
        Ok(w) => w,
        Err(e) => {
            log::warn!("Could not create writer in test environment: {e}");
            return Ok(false);
        }
    };

    for i in 0..batches {
        let batch = otlp_logs_to_arrow(&build_logs_request(i, 100));
        if let Err(e) = writer.write_batch(batch).await {
            log::warn!("Failed to write log batch {i}: {e}");
            return Ok(false);
        }
    }

    Ok(true)
}

fn make_candidate(tenant_id: &str, dataset_id: &str) -> CompactionCandidate {
    CompactionCandidate {
        tenant_id: tenant_id.to_string(),
        dataset_id: dataset_id.to_string(),
        table_name: "logs".to_string(),
        partition_id: "all".to_string(),
        stats: PartitionStats {
            file_count: 10,
            total_size_bytes: 1024 * 1024,
            avg_file_size_bytes: 102_400,
        },
    }
}

fn candidate_key(candidate: &CompactionCandidate) -> String {
    format!(
        "{}/{}/{}/{}",
        candidate.tenant_id, candidate.dataset_id, candidate.table_name, candidate.partition_id
    )
}

/// A single recorded execution: which instance ran which candidate and how
/// many files went in and out.
#[derive(Debug, Clone)]
struct ExecutionRecord {
    key: String,
    instance: String,
    input_files: usize,
    output_files: usize,
}

/// One scheduling cycle of a compactor instance, mirroring the
/// acquire → execute → release loop in `main.rs`.
///
/// `in_flight` guards each candidate with a counter that must never exceed 1 —
/// if two instances ever execute the same candidate concurrently, the lease
/// guarantee is broken and the test fails.
async fn run_instance_cycle(
    instance_name: &str,
    lease_manager: LeaseManager,
    executor: Arc<CompactionExecutor>,
    candidates: Vec<CompactionCandidate>,
    in_flight: Arc<HashMap<String, AtomicUsize>>,
    executions: Arc<tokio::sync::Mutex<Vec<ExecutionRecord>>>,
) -> Result<()> {
    for candidate in candidates {
        let key = candidate_key(&candidate);
        match lease_manager.try_acquire_default(&candidate).await? {
            None => {
                log::info!("[{instance_name}] lease for {key} held elsewhere, skipping");
                continue;
            }
            Some(lease) => {
                let counter = in_flight
                    .get(&key)
                    .expect("candidate key must be registered");
                let concurrent = counter.fetch_add(1, Ordering::SeqCst);
                assert_eq!(
                    concurrent, 0,
                    "lease mutual exclusion violated: {key} executed concurrently"
                );

                // Hold the lease long enough that the other instance is
                // guaranteed to contend while this candidate is in flight.
                tokio::time::sleep(Duration::from_millis(100)).await;

                let result = executor.execute_candidate(candidate).await?;
                log::info!(
                    "[{instance_name}] compacted {key}: {} files -> {} files (status {:?})",
                    result.input_files_count,
                    result.output_files_count,
                    result.status
                );

                counter.fetch_sub(1, Ordering::SeqCst);
                executions.lock().await.push(ExecutionRecord {
                    key: key.clone(),
                    instance: instance_name.to_string(),
                    input_files: result.input_files_count,
                    output_files: result.output_files_count,
                });
                lease_manager.release(&lease).await?;
            }
        }
    }
    Ok(())
}

/// Verify a table end-to-end after compaction: fresh metadata load, live file
/// count from the snapshot's manifests, and total row count from a scan.
async fn verify_table(
    catalog_manager: &Arc<CatalogManager>,
    tenant_id: &str,
    dataset_id: &str,
    expected_rows: usize,
) -> Result<usize> {
    let ident = catalog_manager.build_table_identifier(tenant_id, dataset_id, "logs");
    let table = match catalog_manager.catalog().load_tabular(&ident).await {
        Ok(iceberg_rust::catalog::tabular::Tabular::Table(t)) => t,
        other => anyhow::bail!("expected table for {ident}, got {other:?}"),
    };

    let files = compactor::iceberg::ManifestReader::new()
        .get_snapshot_files(&table)
        .await?;

    let ctx = datafusion::prelude::SessionContext::new();
    ctx.register_table(
        "logs",
        Arc::new(datafusion_iceberg::DataFusionTable::from(table)),
    )?;
    let batches = ctx.table("logs").await?.collect().await?;
    let rows: usize = batches.iter().map(|b| b.num_rows()).sum();

    assert_eq!(
        rows, expected_rows,
        "row count for {tenant_id}/{dataset_id} must be preserved through compaction"
    );

    Ok(files.len())
}

/// Two live instances race over the same candidate list; leases must ensure
/// mutual exclusion per candidate and full coverage of the work.
#[tokio::test]
async fn test_two_instances_compact_without_duplicate_work() -> Result<()> {
    init_test_logging();

    // Shared Iceberg catalog + object store (the storage layer both instances
    // point at), and a shared SQLite file for lease coordination accessed
    // through two independent connections — the way two processes would.
    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let lease_db_dir = tempfile::tempdir()?;
    let lease_dsn = format!(
        "sqlite://{}/leases.db",
        lease_db_dir.path().to_str().expect("utf-8 temp path")
    );
    let lease_catalog_a = Arc::new(Catalog::new(&lease_dsn).await?);
    let lease_catalog_b = Arc::new(Catalog::new(&lease_dsn).await?);

    let tenant_id = "multi-instance-tenant";
    let datasets = ["dataset-a", "dataset-b"];

    for dataset_id in &datasets {
        if !write_small_log_files(
            &catalog_manager,
            object_store.clone(),
            tenant_id,
            dataset_id,
            10,
        )
        .await?
        {
            log::info!("Test skipped due to environment limitations");
            return Ok(());
        }
    }

    let candidates: Vec<CompactionCandidate> = datasets
        .iter()
        .map(|d| make_candidate(tenant_id, d))
        .collect();

    let in_flight: Arc<HashMap<String, AtomicUsize>> = Arc::new(
        candidates
            .iter()
            .map(|c| (candidate_key(c), AtomicUsize::new(0)))
            .collect(),
    );
    let executions = Arc::new(tokio::sync::Mutex::new(Vec::new()));

    // Instance A and instance B: distinct IDs, distinct metrics, distinct
    // lease-catalog connections, shared storage.
    let metrics_a = CompactionMetrics::new();
    let metrics_b = CompactionMetrics::new();
    let executor_a = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics_a.clone(),
    ));
    let executor_b = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics_b.clone(),
    ));
    let lease_manager_a =
        LeaseManager::new(lease_catalog_a, Uuid::new_v4(), Duration::from_secs(300));
    let lease_manager_b =
        LeaseManager::new(lease_catalog_b, Uuid::new_v4(), Duration::from_secs(300));

    let (result_a, result_b) = tokio::join!(
        run_instance_cycle(
            "instance-a",
            lease_manager_a,
            executor_a,
            candidates.clone(),
            in_flight.clone(),
            executions.clone(),
        ),
        run_instance_cycle(
            "instance-b",
            lease_manager_b,
            executor_b,
            candidates.clone(),
            in_flight.clone(),
            executions.clone(),
        ),
    );
    result_a?;
    result_b?;

    // Leases guarantee mutual exclusion, not exactly-once: an instance that
    // re-acquires a candidate after the other released it re-executes, but the
    // executor detects the already-compacted table (≤1 live file) and no-ops.
    // Assert exactly one REAL compaction per candidate; any additional runs
    // must have been no-ops.
    let executed = executions.lock().await;
    for candidate in &candidates {
        let key = candidate_key(candidate);
        let runs: Vec<_> = executed.iter().filter(|r| r.key == key).collect();
        assert!(!runs.is_empty(), "candidate {key} was never executed");

        let real: Vec<_> = runs.iter().filter(|r| r.input_files > 1).collect();
        assert_eq!(
            real.len(),
            1,
            "candidate {key} should be really compacted exactly once, got {runs:?}"
        );
        assert!(
            real[0].output_files < real[0].input_files,
            "compaction must consolidate files for {key}: {runs:?}"
        );
        for run in runs.iter().filter(|r| r.input_files <= 1) {
            assert_eq!(
                run.input_files, run.output_files,
                "re-execution of {key} by {} must be a no-op: {runs:?}",
                run.instance
            );
        }
    }

    // No jobs failed on either instance.
    assert_eq!(metrics_a.jobs_failed() + metrics_b.jobs_failed(), 0);

    // End-to-end integrity: every dataset still holds all 1000 rows and the
    // snapshot references exactly one consolidated data file.
    for dataset_id in &datasets {
        let live_files = verify_table(&catalog_manager, tenant_id, dataset_id, 1000).await?;
        assert_eq!(
            live_files, 1,
            "dataset {dataset_id} should be compacted to a single live file"
        );
    }

    Ok(())
}

/// A lease held by a crashed instance (never released, never renewed) must be
/// recoverable: after TTL expiry a healthy instance takes over and completes
/// the compaction.
#[tokio::test]
async fn test_crashed_instance_lease_taken_over() -> Result<()> {
    init_test_logging();

    let catalog_manager = Arc::new(CatalogManager::new_in_memory().await?);
    let object_store = Arc::new(InMemory::new());

    let lease_db_dir = tempfile::tempdir()?;
    let lease_dsn = format!(
        "sqlite://{}/leases.db",
        lease_db_dir.path().to_str().expect("utf-8 temp path")
    );
    let lease_catalog_crashed = Arc::new(Catalog::new(&lease_dsn).await?);
    let lease_catalog_healthy = Arc::new(Catalog::new(&lease_dsn).await?);

    let tenant_id = "takeover-tenant";
    let dataset_id = "takeover-dataset";

    if !write_small_log_files(
        &catalog_manager,
        object_store.clone(),
        tenant_id,
        dataset_id,
        10,
    )
    .await?
    {
        log::info!("Test skipped due to environment limitations");
        return Ok(());
    }

    let candidate = make_candidate(tenant_id, dataset_id);

    // "Crashed" instance: acquires with a short TTL and then disappears
    // without executing or releasing. The TTL must be long enough that the
    // immediately following "blocked" assertion runs while the lease is
    // still live, but short enough to expire within the test.
    let crashed = LeaseManager::new(
        lease_catalog_crashed,
        Uuid::new_v4(),
        Duration::from_secs(300),
    );
    let stuck_lease = crashed
        .try_acquire(&candidate, Duration::from_millis(800))
        .await?;
    assert!(stuck_lease.is_some(), "crashed instance should hold lease");

    // Healthy instance cannot proceed while the lease is live.
    let healthy = LeaseManager::new(
        lease_catalog_healthy,
        Uuid::new_v4(),
        Duration::from_secs(300),
    );
    let blocked = healthy.try_acquire_default(&candidate).await?;
    assert!(
        blocked.is_none(),
        "healthy instance must not steal a live lease"
    );

    // After TTL expiry the periodic stale-lease sweep clears it and the
    // healthy instance takes over and completes the work.
    tokio::time::sleep(Duration::from_millis(1000)).await;
    healthy.expire_stale().await?;

    let lease = healthy
        .try_acquire_default(&candidate)
        .await?
        .expect("healthy instance should take over expired lease");

    let metrics = CompactionMetrics::new();
    let executor = Arc::new(CompactionExecutor::new(
        catalog_manager.clone(),
        ExecutorConfig::default(),
        metrics.clone(),
    ));
    let result = executor.execute_candidate(candidate).await?;
    healthy.release(&lease).await?;

    log::info!(
        "Takeover compaction: {} files -> {} files (status {:?})",
        result.input_files_count,
        result.output_files_count,
        result.status
    );
    assert_eq!(
        metrics.jobs_succeeded(),
        1,
        "takeover instance should complete the compaction job"
    );
    assert_eq!(metrics.jobs_failed(), 0);
    assert!(
        result.input_files_count > 1,
        "takeover compaction should have real input files"
    );
    assert!(
        result.output_files_count < result.input_files_count,
        "takeover compaction must consolidate files"
    );

    // End-to-end integrity: all 1000 rows survive and one live file remains.
    let live_files = verify_table(&catalog_manager, tenant_id, dataset_id, 1000).await?;
    assert_eq!(live_files, 1, "table should be compacted to one live file");

    Ok(())
}
