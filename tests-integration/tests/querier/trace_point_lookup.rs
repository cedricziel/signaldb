//! Point-lookup guarantees for a single trace: **completeness** across files
//! and hour partitions, and **cheap repetition** through the Parquet footer
//! cache. Both halves of #880 that are not the declared-sort-order work
//! (#936).
//!
//! ## Why these two
//!
//! A trace is not confined to one Parquet file or one hour partition: its
//! spans arrive over the trace's whole lifetime, across writer commits, and a
//! long trace straddles an hour boundary. A lookup that reads only one
//! partition silently returns a *partial* trace — a wrong answer that looks
//! like a right one. `trace_spanning_partitions_returns_every_span` pins the
//! full-fidelity case, including the boundary straddle, through the Query IR
//! surface (`query_ir` Flight ticket) that every first-party reader uses.
//!
//! The cost of that lookup is dominated by *opening* the candidate files, not
//! by scanning them (pruning already reduces the scan to a few bytes — see
//! `trace_bloom_pruning`). `repeated_lookup_reads_footers_from_the_cache`
//! pins that a second lookup re-reads no footer at all.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use arrow_flight::Ticket;
use arrow_flight::flight_service_server::FlightService;
use common::catalog_manager::CatalogManager;
use common::config::{Configuration, DiscoveryConfig, QuerierConfig};
use common::flight::decode::flight_data_vec_to_batches;
use common::flight::transport::InMemoryFlightTransport;
use common::service_bootstrap::{ServiceBootstrap, ServiceType};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use object_store::memory::InMemory;
use querier::QuerierFlightService;
use tests_integration::generators::{self, BLOOM_HIGH_SENTINEL, BLOOM_LOW_SENTINEL};
use tonic::Request;
use writer::IcebergTableWriter;

const TENANT: &str = "lookup-tenant";
const DATASET: &str = "lookup-dataset";
const TABLE: &str = "traces";

/// The trace under test. A leading `8` keeps it strictly between the sentinel
/// ids every filler file carries, so no file's `trace_id` min/max can prune it
/// (real trace ids are random — see `trace_bloom_pruning`).
const TARGET: &str = "88888888888888888888888888888888";

const HOUR_MS: i64 = 3_600_000;

/// Spans of `TARGET`, one per seeded instant. Ground truth for completeness.
const TARGET_SPAN_COUNT: usize = 4;

/// An exact hour boundary in the recent past. Spans are placed relative to it
/// so the straddle is unambiguous rather than clock-dependent.
fn hour_boundary() -> i64 {
    let recent = chrono::Utc::now().timestamp_millis() - (2 * 24 * HOUR_MS);
    recent - recent.rem_euclid(HOUR_MS)
}

/// The four instants `TARGET` has a span at. Each is written as its own file
/// (`generate_trace_files_with_ids` writes one file per entry), and the four
/// land in four distinct `Hour(timestamp)` partitions:
///
/// - `boundary - 1s` and `boundary + 1s` straddle one hour boundary: adjacent
///   partitions, two seconds apart. A lookup whose partition bound is rounded
///   the wrong way drops one of them.
/// - `+26h` and `+50h` are far-flung, so no plausible narrow window covers the
///   whole trace and the scan really does have to reach several partitions.
fn target_instants(boundary: i64) -> [i64; TARGET_SPAN_COUNT] {
    [
        boundary - 1_000,
        boundary + 1_000,
        boundary + 26 * HOUR_MS,
        boundary + 50 * HOUR_MS,
    ]
}

/// Seed a `traces` table holding `TARGET`'s spans spread over several hour
/// partitions plus filler files that bracket it in `trace_id` order.
async fn seed_traces(catalog_manager: &Arc<CatalogManager>, boundary: i64) -> Result<()> {
    let mut writer = IcebergTableWriter::new(
        catalog_manager,
        Arc::new(InMemory::new()),
        TENANT.to_string(),
        DATASET.to_string(),
        TABLE.to_string(),
    )
    .await?;

    // One file per target instant, each holding a single TARGET span next to
    // the two sentinels so statistics can never prune the file.
    for instant in target_instants(boundary) {
        generators::generate_trace_files_with_ids(
            &mut writer,
            &[vec![
                BLOOM_LOW_SENTINEL.to_string(),
                BLOOM_HIGH_SENTINEL.to_string(),
                TARGET.to_string(),
            ]],
            instant,
        )
        .await?;
    }

    // Filler files in partitions the target does not touch, so the lookup has
    // real candidates to prune and the footer cache has more than the target's
    // own files to hold.
    for hours in [3_i64, 12, 36] {
        generators::generate_bloom_prunable_trace_files(
            &mut writer,
            2,
            None,
            boundary + hours * HOUR_MS,
        )
        .await?;
    }
    Ok(())
}

/// Build a querier session over the seeded table, registered exactly as the
/// querier registers it, and run the point lookup on it. Returns the row count
/// alongside the session so the caller can inspect its footer cache.
async fn lookup_on_session(
    catalog_manager: &CatalogManager,
    limits: QuerierConfig,
) -> Result<(SessionContext, usize)> {
    let ctx = querier::flight::session_context_with_limits(&limits);
    let identifier = catalog_manager.build_table_identifier(TENANT, DATASET, TABLE);
    let tabular = catalog_manager.catalog().load_tabular(&identifier).await?;
    ctx.register_table(
        TABLE,
        Arc::new(datafusion_iceberg::DataFusionTable::new(
            tabular, None, None, None,
        )),
    )?;
    let rows = point_lookup(&ctx).await;
    Ok((ctx, rows))
}

/// `WHERE trace_id = TARGET`, no time bound: every candidate file gets opened.
async fn point_lookup(ctx: &SessionContext) -> usize {
    let batches = ctx
        .sql(&format!(
            "SELECT trace_id FROM {TABLE} WHERE trace_id = '{TARGET}'"
        ))
        .await
        .expect("plan point lookup")
        .collect()
        .await
        .expect("run point lookup");
    batches.iter().map(|b| b.num_rows()).sum()
}

fn test_configuration() -> Configuration {
    let mut config = common::testing::TestConfigBuilder::new()
        .in_memory()
        .with_tenant(TENANT, DATASET)
        .build();
    config.discovery = Some(DiscoveryConfig {
        dsn: "sqlite::memory:".to_string(),
        heartbeat_interval: Duration::from_secs(5),
        poll_interval: Duration::from_secs(60),
        ttl: Duration::from_secs(30),
    });
    config
}

/// A `query_ir` Flight ticket selecting `TARGET`'s spans inside `[from, to]`
/// (unix nanoseconds).
fn trace_lookup_ticket(from_ns: i64, to_ns: i64) -> Ticket {
    let params = serde_json::json!({
        "document": {
            "irVersion": 1,
            "from": "traces",
            "range": { "from": from_ns.to_string(), "to": to_ns.to_string() },
            "result": "rows",
            "fields": ["trace_id", "span_id"],
            "pipeline": [
                { "where": { "field": "trace_id", "op": "eq", "value": TARGET } }
            ]
        },
        "now_ns": chrono::Utc::now().timestamp_nanos_opt().unwrap_or_default(),
    });
    Ticket::new(format!("query_ir:{TENANT}:{DATASET}:{params}"))
}

async fn run_lookup(service: &QuerierFlightService, from_ns: i64, to_ns: i64) -> usize {
    let stream = service
        .do_get(Request::new(trace_lookup_ticket(from_ns, to_ns)))
        .await
        .unwrap_or_else(|status| panic!("IR trace lookup failed: {}", status.message()))
        .into_inner();
    let flight_data: Vec<_> = stream
        .map(|data| data.expect("IR result stream must not error"))
        .collect()
        .await;
    let batches = flight_data_vec_to_batches(flight_data)
        .await
        .expect("IR result decodes");
    batches.iter().map(|b| b.num_rows()).sum()
}

/// A trace whose spans live in four files across four hour partitions — two of
/// them one second apart on either side of an hour boundary — must come back
/// whole. This is the "complete" half of #880: an incomplete trace is a wrong
/// answer that no error surfaces.
#[tokio::test]
async fn trace_spanning_partitions_returns_every_span() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let boundary = hour_boundary();
    let config = test_configuration();
    let catalog_manager = Arc::new(CatalogManager::new(config.clone()).await?);
    seed_traces(&catalog_manager, boundary).await?;

    let bootstrap =
        ServiceBootstrap::new(config, ServiceType::Querier, "localhost:0".to_string()).await?;
    let service = QuerierFlightService::new_with_catalog_manager(
        Arc::new(InMemoryFlightTransport::new(bootstrap)),
        catalog_manager,
        QuerierConfig::default(),
    )
    .await?;

    let ms_to_ns = |ms: i64| ms * 1_000_000;

    // A window covering the whole spread returns every span.
    let rows = run_lookup(
        &service,
        ms_to_ns(boundary - HOUR_MS),
        ms_to_ns(boundary + 60 * HOUR_MS),
    )
    .await;
    assert_eq!(
        rows, TARGET_SPAN_COUNT,
        "a trace spread over four files and four hour partitions must return all its spans"
    );

    // The straddle on its own: a two-minute window centered on the hour
    // boundary must return BOTH neighbours. Partition bounds are derived from
    // the window by rounding onto the `timestamp` partition column, and
    // rounding the lower bound the wrong way prunes the earlier partition
    // away — dropping the span one second before the boundary.
    let straddle = run_lookup(
        &service,
        ms_to_ns(boundary - 60_000),
        ms_to_ns(boundary + 60_000),
    )
    .await;
    assert_eq!(
        straddle, 2,
        "both spans on either side of the hour boundary must survive partition pruning"
    );

    // Control: a window strictly inside the later hour sees only that side, so
    // the assertions above are about the data, not about pruning being off.
    let one_side = run_lookup(
        &service,
        ms_to_ns(boundary + 1),
        ms_to_ns(boundary + 60_000),
    )
    .await;
    assert_eq!(
        one_side, 1,
        "a window that excludes the earlier partition must return only the later span"
    );

    Ok(())
}

/// The second identical lookup must not re-read a single Parquet footer.
///
/// Pruning already reduces the scan itself to a handful of bytes, so what a
/// repeated trace lookup actually pays for is re-opening the candidate files
/// (#880: ~53 ms opening footers vs 0.9 ms scanning, on object storage one
/// round-trip per file). The querier's session installs a cached Parquet
/// reader factory (`common::parquet_metadata_cache`); this test measures the
/// result through DataFusion's own per-entry hit counters.
#[tokio::test]
async fn repeated_lookup_reads_footers_from_the_cache() -> Result<()> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();

    let boundary = hour_boundary();
    let config = test_configuration();
    let catalog_manager = Arc::new(CatalogManager::new(config).await?);
    seed_traces(&catalog_manager, boundary).await?;

    // Exactly the session the querier runs on, footer cache and all.
    let (ctx, cold_rows) = lookup_on_session(&catalog_manager, QuerierConfig::default()).await?;
    assert_eq!(
        cold_rows, TARGET_SPAN_COUNT,
        "the point lookup must find every span of the target trace"
    );

    let cache = ctx.runtime_env().cache_manager.get_file_metadata_cache();
    let cached_files = cache.len();
    assert!(
        cached_files > 0,
        "the cold lookup must populate the footer cache"
    );
    let hits = || -> usize { cache.list_entries().values().map(|e| e.hits).sum() };
    assert_eq!(hits(), 0, "nothing can be served from a cold cache");

    // Same query again: every footer it needs is already cached.
    assert_eq!(
        point_lookup(&ctx).await,
        cold_rows,
        "the warm lookup returns the same rows"
    );
    assert!(
        hits() >= cached_files,
        "the repeated lookup must serve every opened file's footer from the cache: \
         {} hits over {cached_files} cached files",
        hits()
    );
    assert_eq!(
        cache.len(),
        cached_files,
        "a repeated lookup must not insert new footer entries"
    );

    // Control: with footer caching switched off the very same lookup caches
    // nothing, so the assertions above measure the cached reader factory and
    // not some incidental DataFusion behaviour.
    let (uncached, uncached_rows) = lookup_on_session(
        &catalog_manager,
        QuerierConfig {
            parquet_metadata_cache_mb: 0,
            ..QuerierConfig::default()
        },
    )
    .await?;
    assert_eq!(uncached_rows, cold_rows);
    assert_eq!(
        uncached
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache()
            .len(),
        0,
        "with parquet_metadata_cache_mb = 0 no footer may be cached"
    );

    Ok(())
}
