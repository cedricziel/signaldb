//! Acceptance test for #1359: the querier sorts over the same wide-row
//! tables as the compactor, but exposed neither `batch_size` nor
//! `sort_spill_reservation_bytes` under `[querier.datafusion]`, so a bounded
//! memory pool hit the same failure PR #1353 fixed for compaction.
//!
//! DataFusion's `ExternalSorter` reserves roughly twice an incoming batch's
//! bytes the moment the batch arrives, and that reservation cannot spill —
//! with nothing accumulated yet there is nothing to write out. DataFusion's
//! batch size is counted in *rows*, so the default of 8192 is only safe for
//! narrow rows: an 8192-row batch of ~2 MB profile-style rows is a multi-GB
//! allocation. See
//! `compactor::partition_scoped_compaction::wide_rows_compact_only_under_a_bounded_scan_batch`,
//! whose querier analogue this is.
//!
//! This pins both halves for the querier's own sort path: DataFusion's
//! default batch size fails attributably ("Resources exhausted"), and the
//! `[querier.datafusion].batch_size` knob bounds the reservation so the same
//! sort succeeds and returns every row.

use std::sync::Arc;

use anyhow::{Context, Result};
use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use common::catalog_manager::CatalogManager;
use common::config::{QuerierConfig, QuerierDataFusionConfig};
use common::flight::decode::flight_data_vec_to_batches;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use futures::StreamExt;
use object_store::memory::InMemory;
use querier::QuerierFlightService;
use tests_integration::compaction_helpers::{MILLIS_PER_HOUR, aligned_hour_start};
use tests_integration::generators;
use tonic::Request;
use writer::IcebergTableWriter;

const TENANT: &str = "wide-sort-tenant";
const DATASET: &str = "wide-sort-dataset";
const TABLE: &str = "traces";

// 64 rows of 256 KB per file, two files: mirrors the compactor's wide-row
// fixture exactly, so the same reservation math applies.
const FILES: usize = 2;
const ROWS_PER_FILE: usize = 64;
const PAYLOAD_BYTES: usize = 256 * 1024;

/// A sorted, whole-row Query IR scan of the wide table — the Query IR
/// surface every first-party reader uses (see `docs/users/querying-ir.md`),
/// not the raw-SQL ticket path: raw SQL runs under an implicit
/// `LIMIT max_sql_rows` (`QuerierFlightService::execute_query`), and
/// DataFusion turns `ORDER BY` + `LIMIT` into an always-unspillable `TopK`
/// plan regardless of the limit's size, which would test `TopK` instead of
/// the `ExternalSorter` this change bounds. An `order` stage with no `limit`
/// stage plans a real sort. `span.attributes` (the logical name for the
/// storage-schema `span_attributes` column that `generate_wide_trace_files`
/// fills via `attributes_json`) keeps the wide payload in the sort's row
/// width instead of letting projection push it away. `generate_wide_trace_files`
/// interleaves ids across files so no file arrangement is already ordered by
/// `trace_id`, forcing a real sort rather than a concatenation.
fn sorted_scan_ticket(base_timestamp_ms: i64) -> Ticket {
    let from_ns = (base_timestamp_ms - MILLIS_PER_HOUR) * 1_000_000;
    let to_ns = (base_timestamp_ms + MILLIS_PER_HOUR) * 1_000_000;
    let params = serde_json::json!({
        "document": {
            "irVersion": 1,
            "from": "traces",
            "range": { "from": from_ns.to_string(), "to": to_ns.to_string() },
            "result": "rows",
            "fields": ["trace_id", "span.attributes"],
            "pipeline": [
                { "order": [{ "of": "trace_id", "dir": "asc" }] }
            ]
        },
        "now_ns": chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
    });
    Ticket::new(format!("query_ir:{TENANT}:{DATASET}:{params}"))
}

/// Run the sorted scan through the real Flight `do_get` path, as every
/// first-party reader does. `Ok` carries the row count; `Err` carries the
/// gRPC status message so the caller can inspect the failure cause.
async fn run_sorted_scan(
    service: &QuerierFlightService,
    base_timestamp_ms: i64,
) -> Result<usize, String> {
    let ticket = sorted_scan_ticket(base_timestamp_ms);
    let stream = service
        .do_get(Request::new(ticket))
        .await
        .map_err(|status| status.message().to_string())?
        .into_inner();
    let flight_data: Vec<_> = stream
        .map(|data| data.expect("result stream must not error"))
        .collect()
        .await;
    let batches = flight_data_vec_to_batches(flight_data)
        .await
        .map_err(|e| e.to_string())?;
    Ok(batches.iter().map(|b| b.num_rows()).sum())
}

/// Wide rows must sort only under a scan batch bounded in rows — the
/// querier's version of the compactor's identically named property.
#[tokio::test]
async fn wide_row_sorts_succeed_only_under_a_bounded_scan_batch() -> Result<()> {
    let config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();

    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    let object_store = Arc::new(InMemory::new());
    let mut writer = IcebergTableWriter::new(
        &catalog_manager,
        object_store,
        TENANT.to_string(),
        DATASET.to_string(),
        TABLE.to_string(),
    )
    .await
    .context("Failed to create writer")?;

    let base_timestamp_ms = aligned_hour_start(5);
    generators::generate_wide_trace_files(
        &mut writer,
        FILES,
        ROWS_PER_FILE,
        PAYLOAD_BYTES,
        base_timestamp_ms,
    )
    .await
    .context("Failed to generate wide trace data")?;

    // A batch of 4 wide rows is ~1 MB, so its 2 MB reservation leaves the
    // 16 MB pool room to accumulate and then spill.
    for (batch_size, must_succeed) in [(0usize, false), (4usize, true)] {
        let limits = QuerierConfig {
            memory_limit_mb: Some(16),
            memory_pool_fraction: 1.0,
            datafusion: QuerierDataFusionConfig {
                batch_size,
                sort_spill_reservation_mb: 2,
                // Isolate batch_size as the one variable under test: left
                // at the default (0, DataFusion's auto fan-out to core
                // count), multiple concurrent `ExternalSorter`s divide the
                // pool between their own unspillable merge reservations
                // regardless of batch_size (#1064) — a real effect, but a
                // different one than this test pins.
                target_partitions: 1,
                ..Default::default()
            },
            ..QuerierConfig::default()
        };

        let bootstrap = ServiceBootstrap::new(
            config.clone(),
            ServiceType::Querier,
            "localhost:0".to_string(),
        )
        .await?;
        let service = QuerierFlightService::new_with_catalog_manager(
            Arc::new(InMemoryFlightTransport::new(bootstrap)),
            catalog_manager.clone(),
            limits,
        )
        .await?;

        let result = run_sorted_scan(&service, base_timestamp_ms).await;

        if must_succeed {
            let rows = result.unwrap_or_else(|e| panic!("bounded batch size must not OOM: {e}"));
            assert_eq!(
                rows,
                FILES * ROWS_PER_FILE,
                "a bounded sort must still return every row"
            );
        } else {
            let error = result.expect_err(
                "DataFusion's default batch size must not fit these rows in the pool — if it \
                 now does, this test no longer covers the case it was written for",
            );
            assert!(
                error.contains("Resources exhausted"),
                "the failure must name the memory cause, got: {error}"
            );
        }
    }
    Ok(())
}
