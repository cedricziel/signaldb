//! Read-path benchmarks through the REAL querier service.
//!
//! `querier_read_paths.rs` times raw DataFusion SQL over a registered table,
//! which isolates scan + pruning cost but bypasses everything the querier
//! adds on top: its session config (`[querier.datafusion]` pushdown options),
//! per-tenant catalog resolution, the trace/logs/metrics services, and the
//! LogQL / PromQL engines. This bench drives `QuerierFlightService::do_get`
//! in-process with the exact ticket formats the router sends, so a regression
//! anywhere on the query path — planner, engine lowering, session setup, or
//! scan — shows up here.
//!
//! Seeded via the writer against a shared catalog + a temp-dir file store
//! (the querier registers stores by DSN, so an `InMemory` store cannot be
//! shared with it), then measured:
//!
//! - **find_trace_by_id** — single-trace lookup with no time hint. Files are
//!   seeded so `trace_id` min/max never prune (shared low/high sentinels);
//!   only the Parquet bloom filter on `trace_id` can, so this is the #826
//!   bloom-pruned point lookup end to end.
//! - **find_trace_by_id_hinted** — the same lookup with the router's
//!   ±1h start/end hint (unix seconds), i.e. hint-driven partition pruning.
//! - **search_traces_recent** — the Explore UI's trace list: `search_traces`
//!   with a limit over the seeded window.
//! - **promql_range_avg_by_service** — a PromQL range query through the
//!   real engine (`query_promql`).
//! - **logql_line_filter** — a LogQL stream selector + `|=` line filter
//!   through the real engine (`query_logs`).
//!
//! In-memory catalog, local-disk Parquet: relative regression signal, not
//! production latency.

use std::hint::black_box;
use std::sync::Arc;

use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::FlightService;
use common::catalog_manager::CatalogManager;
use common::config::QuerierConfig;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::{StreamExt, TryStreamExt};
use querier::flight::QuerierFlightService;
use tempfile::TempDir;
use tests_integration::fixtures::{DataGeneratorConfig, PartitionGranularity};
use tests_integration::generators::{self, BLOOM_TARGET_TRACE_ID};
use tokio::runtime::Runtime;
use tonic::Request;
use writer::IcebergTableWriter;

const TENANT: &str = "bench-tenant";
const DATASET: &str = "bench-dataset";

/// The bloom-pruning point-lookup target (see
/// `generators::generate_bloom_prunable_trace_files`).
const TARGET: &str = BLOOM_TARGET_TRACE_ID;
/// Bloom-only-prunable trace files: 3 filler + 1 holding the target.
const BLOOM_FILES: usize = 4;

/// Bulk seeding shape shared by all three signals: 3 day-partitions × 4
/// files × 1_000 rows.
const PARTITION_COUNT: usize = 3;
const FILES_PER_PARTITION: usize = 4;
const ROWS_PER_FILE: usize = 1_000;
const DAY_MS: i64 = 24 * 3_600_000;

struct Env {
    service: QuerierFlightService,
    /// Seeding origin, epoch millis (3 days ago).
    base_ts_ms: i64,
    /// Keeps the file-backed object store alive for the bench's lifetime.
    _storage: TempDir,
}

async fn seed() -> Env {
    let storage = tempfile::tempdir().expect("tempdir");
    let storage_dsn = format!("file://{}", storage.path().display());
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_storage_dsn(&storage_dsn)
        .with_tenant(TENANT, DATASET)
        .build();
    let catalog_manager = Arc::new(
        CatalogManager::new(config.clone())
            .await
            .expect("catalog manager"),
    );
    let object_store =
        common::storage::create_object_store_from_dsn(&storage_dsn).expect("object store");

    let base_ts_ms = chrono::Utc::now().timestamp_millis() - PARTITION_COUNT as i64 * DAY_MS;
    let gen_config = DataGeneratorConfig {
        partition_count: PARTITION_COUNT,
        files_per_partition: FILES_PER_PARTITION,
        rows_per_file: ROWS_PER_FILE,
        base_timestamp: base_ts_ms,
        partition_granularity: PartitionGranularity::Day,
    };

    // Traces: bulk volume + bloom-only-prunable files holding the target.
    let mut traces = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        TENANT.to_string(),
        DATASET.to_string(),
        "traces".to_string(),
    )
    .await
    .expect("traces writer");
    generators::generate_traces(&mut traces, &gen_config)
        .await
        .expect("seed bulk traces");
    generators::generate_bloom_prunable_trace_files(
        &mut traces,
        BLOOM_FILES,
        Some(BLOOM_FILES - 1),
        base_ts_ms,
    )
    .await
    .expect("seed bloom-prunable traces");

    let mut logs = IcebergTableWriter::new(
        &catalog_manager,
        object_store.clone(),
        TENANT.to_string(),
        DATASET.to_string(),
        "logs".to_string(),
    )
    .await
    .expect("logs writer");
    generators::generate_logs(&mut logs, &gen_config)
        .await
        .expect("seed logs");

    let mut metrics = IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        TENANT.to_string(),
        DATASET.to_string(),
        "metrics_gauge".to_string(),
    )
    .await
    .expect("metrics writer");
    generators::generate_metrics(&mut metrics, &gen_config)
        .await
        .expect("seed metrics");

    // The querier as the monolith/`signaldb querier` builds it: default
    // limits, tenant catalogs registered from the config, object stores
    // registered from each dataset's DSN.
    let bootstrap = ServiceBootstrap::new(config, ServiceType::Querier, "127.0.0.1:0".to_string())
        .await
        .expect("bootstrap");
    let transport = Arc::new(InMemoryFlightTransport::new(bootstrap));
    let service = QuerierFlightService::new_with_catalog_manager(
        transport,
        catalog_manager,
        QuerierConfig::default(),
    )
    .await
    .expect("querier service");

    Env {
        service,
        base_ts_ms,
        _storage: storage,
    }
}

/// Issue one `do_get` with `ticket` and drain the response, returning the
/// number of rows across all returned batches.
async fn do_get_rows(service: &QuerierFlightService, ticket: &str) -> usize {
    let response = service
        .do_get(Request::new(Ticket::new(ticket.to_string())))
        .await
        .unwrap_or_else(|e| panic!("do_get({ticket}) failed: {e}"));
    let flight_data = response.into_inner().map_err(FlightError::from);
    let mut batches = FlightRecordBatchStream::new_from_flight_data(flight_data);
    let mut rows = 0;
    while let Some(batch) = batches.next().await {
        rows += batch.expect("decode flight batch").num_rows();
    }
    rows
}

fn bench_querier_service(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let env = rt.block_on(seed());
    let service = &env.service;

    let base_s = env.base_ts_ms / 1_000;
    let end_s = base_s + PARTITION_COUNT as i64 * 86_400 + 3_600;
    let base_ns = env.base_ts_ms * 1_000_000;
    let end_ns = base_ns + PARTITION_COUNT as i64 * DAY_MS * 1_000_000;

    let find_trace = format!("find_trace:{TENANT}:{DATASET}:{TARGET}");
    let find_trace_hinted = format!(
        "find_trace:{TENANT}:{DATASET}:{TARGET}:{}:{}",
        base_s - 3_600,
        base_s + 3_600
    );
    let search_traces = format!(
        "search_traces:{TENANT}:{DATASET}:{}",
        serde_json::json!({ "limit": 20, "start": base_s, "end": end_s })
    );
    let promql = format!(
        "query_promql:{TENANT}:{DATASET}:{}",
        serde_json::json!({
            "query": "avg by (service_name) (cpu_usage)",
            "start": base_ns,
            "end": end_ns,
            "step": 3_600_000_000_000_i64,
        })
    );
    let logql = format!(
        "query_logs:{TENANT}:{DATASET}:{}",
        serde_json::json!({
            "query": r#"{service_name="service-0"} |= "partition 1""#,
            "start": base_ns,
            "end": end_ns,
            "limit": 1000,
            "direction": "backward",
        })
    );

    // Sanity, once: every path must actually return data, or the bench
    // would silently time an empty result.
    let (t, th, s, p, l) = rt.block_on(async {
        tokio::join!(
            do_get_rows(service, &find_trace),
            do_get_rows(service, &find_trace_hinted),
            do_get_rows(service, &search_traces),
            do_get_rows(service, &promql),
            do_get_rows(service, &logql),
        )
    });
    assert_eq!(t, 1, "find_trace must return exactly the target span");
    assert_eq!(
        th, 1,
        "hinted find_trace must return exactly the target span"
    );
    assert!(s > 0, "search_traces returned no rows");
    assert!(p > 0, "query_promql returned no rows");
    assert!(l > 0, "query_logs returned no rows");
    eprintln!("seeded: find_trace={t} hinted={th} search={s} promql={p} logql={l}");

    let mut group = c.benchmark_group("querier_service");
    for (name, ticket) in [
        ("find_trace_by_id", &find_trace),
        ("find_trace_by_id_hinted", &find_trace_hinted),
        ("search_traces_recent", &search_traces),
        ("promql_range_avg_by_service", &promql),
        ("logql_line_filter", &logql),
    ] {
        group.bench_function(name, |b| {
            b.to_async(&rt).iter(|| async {
                black_box(do_get_rows(service, ticket).await);
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_querier_service);
criterion_main!(benches);
