//! `signaldb.wal.entries_pending` must stay a faithful count of the pending
//! set across a restart.
//!
//! The gauge is an `UpDownCounter`: it is incremented when an entry enters the
//! pending set and decremented when it leaves. The pending set outlives the
//! process, though — entries recovered from disk at startup were incremented by
//! a *previous* process, so unless recovery re-seeds the counter their eventual
//! processing decrements with no matching increment and the gauge drifts
//! negative. A negative pending count is impossible by construction, which
//! makes the metric useless as a backlog alarm.
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

/// Latest cumulative value of `signaldb.wal.entries_pending`.
///
/// `get_finished_metrics` returns one snapshot per flush, so the current value
/// is the last snapshot that carries the instrument — summing across snapshots
/// would add up history instead.
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
async fn pending_gauge_returns_to_zero_after_entries_recovered_from_disk_are_processed() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let temp_dir = TempDir::new().unwrap();
    let wal_dir = temp_dir.path().join("wal");

    // First process: append entries and leave them unprocessed on disk.
    const ENTRY_COUNT: usize = 8;
    {
        let wal = Wal::new(wal_config(wal_dir.clone())).await.unwrap();
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

    provider.force_flush().unwrap();
    let before_restart = pending_gauge_value(&exporter);
    assert_eq!(
        before_restart, ENTRY_COUNT as i64,
        "appending {ENTRY_COUNT} entries must leave {ENTRY_COUNT} pending"
    );

    // Second process: recover the same directory and drain the backlog.
    //
    // The assertion is on this phase's *net* effect. A real restart zeroes the
    // counter, so only what the recovering process contributes is observable;
    // running both phases in one process is what lets the test see it at all.
    // Recovering N entries and processing all N must net to zero — without the
    // recovery seed it nets to -N, and on a real restart that is exactly the
    // negative reading the gauge drifts to.
    let wal = Wal::new(wal_config(wal_dir)).await.unwrap();
    let recovered = wal.get_unprocessed_entries().await.unwrap();
    assert_eq!(
        recovered.len(),
        ENTRY_COUNT,
        "all entries must survive the restart"
    );

    let ids: Vec<_> = recovered.iter().map(|e| e.id).collect();
    wal.mark_processed_many(&ids).await.unwrap();

    provider.force_flush().unwrap();
    let net = pending_gauge_value(&exporter) - before_restart;
    assert_eq!(
        net, 0,
        "recovering {ENTRY_COUNT} entries and processing all of them must net to \
         zero, got {net}: entries recovered from disk decrement the gauge without \
         a matching increment, so the pending count drifts negative after a restart"
    );
}
