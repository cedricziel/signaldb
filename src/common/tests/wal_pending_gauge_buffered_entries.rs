//! Reconciling `signaldb.wal.entries_pending` must not treat buffered
//! (appended but not yet flushed) entries as drift.
//!
//! `Wal::append` bumps the gauge's belief as soon as an entry is buffered,
//! before it is ever flushed into a segment — durability semantics aside,
//! that entry is pending the moment `append` returns its id. But
//! `get_unprocessed_entries` (what the writer's drain loop and the
//! acceptor's retry consumer list every cycle, and what they pass to
//! `reconcile_pending_gauge_with_count`) only scans segments, so it cannot
//! see an entry still sitting in the buffer.
//!
//! Without accounting for the buffer, reconciliation would see
//! `believed > segment-only count` for any WAL with unflushed entries and
//! "correct" that phantom gap by subtracting real, still-pending entries
//! from the gauge — a false drift correction, caught in review on the PR for
//! this fix. `reconcile_pending_gauge_with_count` adds
//! `Wal::buffered_entry_count` to the caller's segment-only count before
//! comparing against belief, so this never fires.
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
        // High enough that the appends below never cross the auto-flush
        // threshold, and a long flush interval so the background timer
        // cannot flush them out from under the test either — they must stay
        // buffered for the whole test.
        max_buffer_entries: 1000,
        flush_interval_secs: 3600,
        tenant_id: "test-tenant".to_string(),
        dataset_id: "test-dataset".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    }
}

/// Latest cumulative value of `signaldb.wal.entries_pending`.
fn pending_gauge_value(exporter: &InMemoryMetricExporter) -> i64 {
    let finished = exporter.get_finished_metrics().expect("collected metrics");
    let mut latest = 0i64;
    for rm in &finished {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() != "signaldb.wal.entries_pending" {
                    continue;
                }
                if let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() {
                    latest = sum.data_points().map(|p| p.value()).sum();
                }
            }
        }
    }
    latest
}

#[tokio::test]
async fn reconciliation_does_not_subtract_buffered_unflushed_entries() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let temp_dir = TempDir::new().unwrap();
    let wal = Wal::new(wal_config(temp_dir.path().to_path_buf()))
        .await
        .unwrap()
        .with_gauge_attribution("writer", "traces");

    const ENTRY_COUNT: usize = 5;
    for i in 0..ENTRY_COUNT {
        wal.append(
            WalOperation::WriteTraces,
            format!("payload-{i}").into_bytes(),
            None,
        )
        .await
        .unwrap();
    }
    // Deliberately not flushed: these entries are buffered, not yet in a
    // segment.

    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        ENTRY_COUNT as i64,
        "appending {ENTRY_COUNT} entries must leave {ENTRY_COUNT} pending, \
         buffered or not"
    );

    // Exactly what the writer's drain loop and the acceptor's retry
    // consumer compute each cycle: a segment-only count, since neither ever
    // reads the in-memory buffer.
    let segment_only_count = wal.get_unprocessed_entries().await.unwrap().len();
    assert_eq!(
        segment_only_count, 0,
        "nothing has been flushed into a segment yet"
    );
    assert_eq!(
        wal.buffered_entry_count().await,
        ENTRY_COUNT,
        "all {ENTRY_COUNT} entries must still be sitting in the buffer"
    );

    wal.reconcile_pending_gauge_with_count(segment_only_count)
        .await;

    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        ENTRY_COUNT as i64,
        "reconciling with a segment-only count of 0 must not subtract the \
         {ENTRY_COUNT} entries that are still genuinely pending in the \
         buffer — a false drift correction"
    );
}
