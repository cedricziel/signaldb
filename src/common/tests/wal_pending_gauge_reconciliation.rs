//! `signaldb.wal.entries_pending` must self-heal when it drifts from the
//! true on-disk pending set.
//!
//! Issue #1493: on hive, the gauge climbed to ~33.7k after a restart, dropped
//! to ~23.3k, then sat there — flat — for 53 hours while the writer committed
//! new entries every ~5s. Either that backlog was genuinely stuck, or the
//! gauge was reporting entries that no longer existed. Auditing every path
//! that adds or removes an entry from the pending set (`append`,
//! `mark_processed_many`, and everything built on it — dead-lettering,
//! legacy-directory adoption, segment reclamation) turned up no unbalanced
//! one: each already keeps `signaldb.wal.entries_pending` in lockstep. That
//! makes the incremental counter *currently* correct but not *provably*
//! correct — a future path that forgets to touch it (a new bulk-retirement
//! route, say) would drift exactly like #1493 and nothing would notice.
//!
//! `Wal::reconcile_pending_gauge` closes that class structurally: on the same
//! cadence the writer's drain loop and the acceptor's retry consumer already
//! walk every cached WAL, it diffs the gauge's own belief for that directory
//! against a fresh on-disk count and corrects any difference. This test
//! simulates the defect class directly — an entry leaves the pending set
//! without the gauge being told, via the test-only
//! `mark_processed_bypassing_gauge_for_test` escape hatch — and shows the
//! gauge stays wrong until a reconciliation pass runs, then self-heals.
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

/// Latest cumulative value of `signaldb.wal.entries_pending`, summed across
/// every attributed data point (tenant/dataset/signal/role) it now reports.
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
async fn reconciliation_corrects_a_gauge_that_drifted_from_the_true_backlog() {
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
    let mut ids = Vec::with_capacity(ENTRY_COUNT);
    for i in 0..ENTRY_COUNT {
        let id = wal
            .append(
                WalOperation::WriteTraces,
                format!("payload-{i}").into_bytes(),
                None,
            )
            .await
            .unwrap();
        ids.push(id);
    }
    wal.flush().await.unwrap();

    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        ENTRY_COUNT as i64,
        "appending {ENTRY_COUNT} entries must leave {ENTRY_COUNT} pending"
    );

    // Simulate the defect class: 3 of the 5 entries leave the pending set
    // (marked processed on disk) through a path that never touches the
    // gauge — exactly what a future unbalanced path would do.
    for id in &ids[..3] {
        wal.mark_processed_bypassing_gauge_for_test(*id)
            .await
            .unwrap();
    }
    assert_eq!(
        wal.pending_count().await,
        2,
        "the true on-disk backlog must reflect the 3 entries marked processed"
    );

    // RED: without a reconciliation pass, the gauge is stuck reporting the
    // pre-drift value — it has no way to know the true backlog shrank. This
    // is the #1493 symptom: a plateau that no longer matches reality.
    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        ENTRY_COUNT as i64,
        "before reconciliation the gauge must still show the stale, drifted value"
    );

    // GREEN: reconciliation diffs the gauge's belief against the true
    // on-disk count and corrects the difference.
    wal.reconcile_pending_gauge().await;

    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        2,
        "reconciliation must correct the gauge to the true on-disk pending count"
    );

    // Normal operation afterwards must still be correct: processing the
    // remaining 2 entries through the real path drains the gauge to zero.
    wal.mark_processed_many(&ids[3..]).await.unwrap();
    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        0,
        "draining the remaining entries normally after a correction must reach zero"
    );

    // A second reconciliation pass over an already-consistent WAL must be a
    // no-op: it must not re-emit a spurious correction.
    wal.reconcile_pending_gauge().await;
    provider.force_flush().unwrap();
    assert_eq!(
        pending_gauge_value(&exporter),
        0,
        "reconciling an already-consistent WAL must not change the gauge"
    );
}
