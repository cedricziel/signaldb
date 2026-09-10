//! The recovered-on-restart backlog seeded into
//! `signaldb.wal.entries_pending` must carry this WAL's *final* role/signal
//! attribution, not a placeholder.
//!
//! `Wal::new` cannot know its `role` ("acceptor" | "writer") up front —
//! `WalManager::get_wal` only attaches it afterwards, via
//! `with_gauge_attribution`, once construction returns. An earlier version of
//! this fix emitted the seed's one-time contribution immediately inside
//! `Wal::new`, tagged `role="unknown"`, and then switched every later
//! emission (`append`, `mark_processed_many`, reconciliation) to the real
//! role. That splits one directory's backlog across two attribute series: a
//! permanently orphaned `role="unknown"` point nothing ever corrects again
//! (reconciliation only compares a *total* belief against the true count, so
//! it cannot see a value stranded on the wrong series), and the real series
//! that all later activity lands on. Per-role attribution — the entire point
//! of this metric getting `role` in the first place (#1493) — would then be
//! wrong in exactly the post-restart-with-backlog case that produced the
//! original incident.
//!
//! `Wal::flush_recovered_seed` fixes this by deferring the seed's emission
//! until the first real gauge-touching call, by which point
//! `with_gauge_attribution` (if the caller uses it, as `WalManager::get_wal`
//! always does) has already run. This test reproduces the restart-with-
//! backlog scenario end to end and asserts the recovered backlog lands
//! entirely on the `role="writer"` series, with no `role="unknown"` point at
//! all.
//!
//! This file holds a single test on purpose: `app_metrics()` binds to the
//! global meter provider exactly once per process, so the provider must be
//! installed before anything else in the binary touches it.

use std::path::PathBuf;

use common::wal::{Wal, WalConfig, WalOperation};
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};
use tempfile::TempDir;

fn wal_config(dir: PathBuf) -> WalConfig {
    WalConfig {
        wal_dir: dir,
        max_segment_size: 1024 * 1024,
        max_buffer_entries: 100,
        flush_interval_secs: 60,
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    }
}

/// Every `signaldb.wal.entries_pending` data point's `role` attribute value
/// (as recorded in the latest snapshot) paired with its value, so the test
/// can assert both "no unknown-role point exists" and "the writer-role point
/// carries the whole recovered backlog".
fn pending_gauge_points_by_role(exporter: &InMemoryMetricExporter) -> Vec<(String, i64)> {
    let finished = exporter.get_finished_metrics().expect("collected metrics");
    let mut latest: Vec<(String, i64)> = Vec::new();
    for rm in &finished {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() != "signaldb.wal.entries_pending" {
                    continue;
                }
                if let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() {
                    latest = sum
                        .data_points()
                        .map(|p| {
                            let role = p
                                .attributes()
                                .find(|kv| kv.key.as_str() == "role")
                                .map(|kv| kv.value.to_string())
                                .unwrap_or_else(|| "<missing>".to_string());
                            (role, p.value())
                        })
                        .collect();
                }
            }
        }
    }
    latest
}

#[tokio::test]
async fn recovered_backlog_is_attributed_to_the_final_role_not_a_placeholder() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    // First process: append entries and leave them unprocessed on disk —
    // this is the backlog a restart will recover. Attributed exactly like
    // `WalManager::get_wal` always does in production, so every point this
    // test sees is real production shape, not an artifact of a raw,
    // unattributed test `Wal`.
    const ENTRY_COUNT: usize = 4;
    {
        let wal = Wal::new(wal_config(wal_dir.clone()))
            .await
            .unwrap()
            .with_gauge_attribution("writer", "traces");
        for i in 0..ENTRY_COUNT {
            wal.append(
                WalOperation::WriteTraces,
                format!("payload-{i}").into_bytes(),
                None,
            )
            .await
            .unwrap();
        }
        wal.flush().await.unwrap();
    }

    // A real restart zeroes the process-global counter; only the seed-claim
    // reset is reachable from a test (same rationale as
    // `wal_pending_gauge.rs`), so assertions below are on the *net* effect
    // of what happens after this point, not the absolute value.
    provider.force_flush().unwrap();
    let before_restart: i64 = pending_gauge_points_by_role(&exporter)
        .iter()
        .filter(|(role, _)| role == "writer")
        .map(|(_, v)| v)
        .sum();

    // Second process: recover the same directory exactly as
    // `WalManager::get_wal` does — construct, then attach role/signal
    // attribution before anything else touches the WAL.
    Wal::reset_pending_seed_claims_for_test();
    let wal = Wal::new(wal_config(wal_dir))
        .await
        .unwrap()
        .with_gauge_attribution("writer", "traces");

    // Trigger the deferred seed emission via a normal gauge-touching call —
    // a fresh append, exactly like real traffic arriving after recovery.
    wal.append(WalOperation::WriteTraces, b"post-restart".to_vec(), None)
        .await
        .unwrap();
    wal.flush().await.unwrap();

    provider.force_flush().unwrap();
    let points = pending_gauge_points_by_role(&exporter);

    assert!(
        !points.iter().any(|(role, _)| role == "unknown"),
        "the recovered backlog must never land on a role=\"unknown\" series; \
         got data points {points:?}"
    );

    let writer_total: i64 = points
        .iter()
        .filter(|(role, _)| role == "writer")
        .map(|(_, v)| v)
        .sum();
    assert_eq!(
        writer_total - before_restart,
        ENTRY_COUNT as i64 + 1,
        "recovering the {ENTRY_COUNT} still-pending entries plus the one \
         post-restart append must net to {} more on the role=\"writer\" \
         series (a real restart would net the same after zeroing the \
         counter); got data points {points:?}",
        ENTRY_COUNT + 1
    );
}
