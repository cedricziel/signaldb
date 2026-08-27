//! `signaldb.wal.instances` must not drift when `WalManager::clear_cache`
//! leaves the adopted legacy drain-only WAL in place (#1308).
//!
//! Like `wal_pending_gauge.rs`, this file holds a single test on purpose:
//! `app_metrics()` binds to the global meter provider exactly once per
//! process, so the provider must be installed before anything else in the
//! binary touches it.

use std::path::PathBuf;

use common::wal::manager::WalManager;
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
        tenant_id: "default".to_string(),
        dataset_id: "default".to_string(),
        retention_secs: 3600,
        cleanup_interval_secs: 300,
        compaction_threshold: 0.5,
    }
}

/// Latest cumulative value of `signaldb.wal.instances`.
///
/// `get_finished_metrics` returns one snapshot per flush, so the current
/// value is the last snapshot that carries the instrument — summing across
/// snapshots would add up history instead.
fn instances_gauge_value(exporter: &InMemoryMetricExporter) -> i64 {
    let finished = exporter.get_finished_metrics().expect("collected metrics");
    let mut latest = 0i64;
    for rm in &finished {
        for sm in rm.scope_metrics() {
            for metric in sm.metrics() {
                if metric.name() != "signaldb.wal.instances" {
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
async fn clear_cache_does_not_decrement_the_gauge_for_the_surviving_legacy_wal() {
    let exporter = InMemoryMetricExporter::default();
    let provider = SdkMeterProvider::builder()
        .with_periodic_exporter(exporter.clone())
        .build();
    opentelemetry::global::set_meter_provider(provider.clone());

    let temp_dir = TempDir::new().unwrap();
    let base_path = temp_dir.path().to_path_buf();

    // A pre-#932 writer left one legacy segment directly in the base dir.
    let legacy = Wal::new(wal_config(base_path.clone())).await.unwrap();
    legacy
        .append(WalOperation::WriteTraces, b"legacy".to_vec(), None)
        .await
        .unwrap();
    legacy.flush().await.unwrap();
    drop(legacy);

    let manager = WalManager::uniform(wal_config(base_path.clone()));
    assert!(manager.adopt_root_segments().await.unwrap());
    manager
        .get_wal("acme", "production", "traces")
        .await
        .unwrap();
    manager.get_wal("acme", "production", "logs").await.unwrap();

    provider.force_flush().unwrap();
    let before_clear = instances_gauge_value(&exporter);

    manager.clear_cache().await;

    provider.force_flush().unwrap();
    let after_clear = instances_gauge_value(&exporter);

    assert_eq!(
        before_clear - after_clear,
        2,
        "clear_cache must decrement the gauge by exactly the 2 ordinary WALs \
         it closed, not by that plus the surviving legacy WAL"
    );
}
