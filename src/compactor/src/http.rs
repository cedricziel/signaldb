//! # HTTP Observability Endpoints
//!
//! Serves operational endpoints for the compactor service:
//!
//! - `GET /metrics` — Prometheus text exposition of all compaction, retention,
//!   and orphan-cleanup counters
//! - `GET /status` — JSON snapshot of the same counters plus instance metadata,
//!   intended for operators and the admin API
//! - `GET /health` — liveness probe (always `200 "ok"` once the process is
//!   serving); degraded cycles are visible on `/status` and the
//!   `compactor_cycle_panics_total` / `compactor_cycle_down` metrics instead
//!   (see [`crate::metrics::CycleHealth`]), so a cycle recovering from a
//!   panic never fails the probe a container orchestrator restarts on
//!
//! The endpoint listens on `[compactor] metrics_addr` (default `0.0.0.0:9091`,
//! overridable via `COMPACTOR_METRICS_ADDR`).

use std::sync::Arc;
use std::time::Instant;

use axum::http::StatusCode;
use axum::{Json, Router, extract::State, routing::get};
use uuid::Uuid;

use crate::metrics::{CompactionMetrics, Cycle, CycleHealth};
use crate::orphan::OrphanMetrics;
use crate::retention::metrics::RetentionMetrics;

/// Shared state for the observability endpoints.
///
/// Holds clones of the service-lifetime metric handles; all metric structs are
/// `Arc`-backed, so the endpoint observes the same counters the background
/// loops update.
#[derive(Clone)]
pub struct ObservabilityState {
    inner: Arc<StateInner>,
}

struct StateInner {
    instance_id: Uuid,
    started_at: Instant,
    compaction: CompactionMetrics,
    retention: RetentionMetrics,
    orphan: OrphanMetrics,
    cycle_health: CycleHealth,
}

impl ObservabilityState {
    pub fn new(
        instance_id: Uuid,
        compaction: CompactionMetrics,
        retention: RetentionMetrics,
        orphan: OrphanMetrics,
        cycle_health: CycleHealth,
    ) -> Self {
        Self {
            inner: Arc::new(StateInner {
                instance_id,
                started_at: Instant::now(),
                compaction,
                retention,
                orphan,
                cycle_health,
            }),
        }
    }

    /// Render all counters in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let s = &self.inner;
        let mut out = String::with_capacity(4096);

        let mut counter = |name: &str, help: &str, value: u64| {
            out.push_str(&format!(
                "# HELP {name} {help}\n# TYPE {name} counter\n{name} {value}\n"
            ));
        };

        // Compaction execution
        counter(
            "compactor_jobs_started_total",
            "Compaction jobs started",
            s.compaction.jobs_started() as u64,
        );
        counter(
            "compactor_jobs_succeeded_total",
            "Compaction jobs completed successfully",
            s.compaction.jobs_succeeded() as u64,
        );
        counter(
            "compactor_jobs_failed_total",
            "Compaction jobs failed",
            s.compaction.jobs_failed() as u64,
        );
        counter(
            "compactor_conflicts_detected_total",
            "Iceberg commit conflicts detected during compaction",
            s.compaction.conflicts_detected() as u64,
        );
        counter(
            "compactor_retries_attempted_total",
            "Compaction retries after commit conflicts",
            s.compaction.retries_attempted() as u64,
        );
        counter(
            "compactor_stale_leases_expired_total",
            "Compaction leases reclaimed from crashed instances",
            s.compaction.stale_leases_expired(),
        );
        counter(
            "compactor_input_files_total",
            "Input files consumed by compaction jobs",
            s.compaction.total_input_files() as u64,
        );
        counter(
            "compactor_output_files_total",
            "Output files produced by compaction jobs",
            s.compaction.total_output_files() as u64,
        );
        counter(
            "compactor_bytes_before_compaction_total",
            "Bytes read by compaction jobs (pre-compaction size)",
            s.compaction.bytes_before_compaction(),
        );
        counter(
            "compactor_bytes_after_compaction_total",
            "Bytes written by compaction jobs (post-compaction size)",
            s.compaction.bytes_after_compaction(),
        );
        counter(
            "compactor_compaction_duration_ms_total",
            "Cumulative wall-clock milliseconds spent in successful compaction jobs",
            s.compaction.total_duration_ms(),
        );

        // Retention enforcement
        counter(
            "compactor_retention_cutoffs_computed_total",
            "Retention cutoff computations performed",
            s.retention.cutoffs_computed() as u64,
        );
        counter(
            "compactor_partitions_evaluated_total",
            "Partitions evaluated for retention",
            s.retention.partitions_evaluated() as u64,
        );
        counter(
            "compactor_partitions_dropped_total",
            "Partitions dropped by retention enforcement",
            s.retention.partitions_dropped() as u64,
        );
        counter(
            "compactor_snapshots_expired_total",
            "Iceberg snapshots expired",
            s.retention.snapshots_expired() as u64,
        );
        counter(
            "compactor_bytes_reclaimed_total",
            "Bytes reclaimed by retention enforcement",
            s.retention.bytes_reclaimed(),
        );
        counter(
            "compactor_unclassifiable_files_total",
            "Data files whose timestamp_hour partition value could not be determined; kept and excluded from retention",
            s.retention.unclassifiable_files() as u64,
        );
        counter(
            "compactor_retention_failures_total",
            "Retention enforcement passes that failed (per tenant/dataset or per table)",
            s.retention.enforcement_failures() as u64,
        );
        counter(
            "compactor_retention_duration_ms_total",
            "Cumulative wall-clock milliseconds spent enforcing retention",
            s.retention.total_duration_ms(),
        );

        // Orphan cleanup
        counter(
            "compactor_orphan_candidates_identified_total",
            "Orphan file candidates identified",
            s.orphan.candidates_identified() as u64,
        );
        counter(
            "compactor_files_deleted_total",
            "Orphan files deleted",
            s.orphan.files_deleted() as u64,
        );
        counter(
            "compactor_bytes_freed_total",
            "Bytes freed by orphan file deletion",
            s.orphan.bytes_freed(),
        );
        counter(
            "compactor_deletion_failures_total",
            "Orphan file deletion failures",
            s.orphan.deletion_failures() as u64,
        );

        // Skipped cleanups carry a reason label (currently only the live-file
        // threshold guard from issue #475).
        let skipped = s.orphan.cleanup_skipped_threshold() as u64;
        out.push_str(&format!(
            "# HELP compactor_orphan_cleanup_skipped_total Orphan cleanup runs skipped\n\
             # TYPE compactor_orphan_cleanup_skipped_total counter\n\
             compactor_orphan_cleanup_skipped_total{{reason=\"live_files_threshold_exceeded\"}} {skipped}\n"
        ));

        // Lifecycle-cycle panics, labelled by cycle: each of the four
        // background tasks (compaction, lease_expiry, retention,
        // orphan_cleanup) is guarded against panics and retried with
        // backoff rather than ending permanently (issue #1011) — this is
        // the counter that makes a recovered panic visible instead of only
        // logged.
        out.push_str(
            "# HELP compactor_cycle_panics_total Lifecycle cycle iterations that panicked and were recovered\n\
             # TYPE compactor_cycle_panics_total counter\n",
        );
        for cycle in Cycle::all() {
            let label = cycle.label();
            let panics = s.cycle_health.panics(cycle);
            out.push_str(&format!(
                "compactor_cycle_panics_total{{cycle=\"{label}\"}} {panics}\n"
            ));
        }
        out.push_str(
            "# HELP compactor_cycle_down Whether a lifecycle cycle is currently in its post-panic backoff (1) or not (0)\n\
             # TYPE compactor_cycle_down gauge\n",
        );
        for cycle in Cycle::all() {
            let label = cycle.label();
            let down = u8::from(s.cycle_health.is_down(cycle));
            out.push_str(&format!(
                "compactor_cycle_down{{cycle=\"{label}\"}} {down}\n"
            ));
        }

        // Uptime gauge
        out.push_str(&format!(
            "# HELP compactor_uptime_seconds Seconds since the compactor instance started\n\
             # TYPE compactor_uptime_seconds gauge\n\
             compactor_uptime_seconds {}\n",
            s.started_at.elapsed().as_secs()
        ));

        out
    }

    /// Build the JSON status document served at `/status`.
    pub fn status_json(&self) -> serde_json::Value {
        let s = &self.inner;
        let compaction = s.compaction.summary();
        serde_json::json!({
            "instance_id": s.instance_id.to_string(),
            "uptime_seconds": s.started_at.elapsed().as_secs(),
            "compaction": {
                "jobs_started": compaction.jobs_started,
                "jobs_succeeded": compaction.jobs_succeeded,
                "jobs_failed": compaction.jobs_failed,
                "conflicts_detected": compaction.conflicts_detected,
                "retries_attempted": compaction.retries_attempted,
                "stale_leases_expired": compaction.stale_leases_expired,
                "input_files": compaction.total_input_files,
                "output_files": compaction.total_output_files,
                "bytes_before": compaction.bytes_before_compaction,
                "bytes_after": compaction.bytes_after_compaction,
                "compression_ratio": compaction.compression_ratio,
                "avg_duration_ms": compaction.avg_duration_ms,
            },
            "retention": {
                "cutoffs_computed": s.retention.cutoffs_computed(),
                "partitions_evaluated": s.retention.partitions_evaluated(),
                "partitions_dropped": s.retention.partitions_dropped(),
                "snapshots_expired": s.retention.snapshots_expired(),
                "unclassifiable_files": s.retention.unclassifiable_files(),
                "bytes_reclaimed": s.retention.bytes_reclaimed(),
                "enforcement_failures": s.retention.enforcement_failures(),
            },
            "orphan_cleanup": {
                "candidates_identified": s.orphan.candidates_identified(),
                "files_deleted": s.orphan.files_deleted(),
                "bytes_freed": s.orphan.bytes_freed(),
                "deletion_failures": s.orphan.deletion_failures(),
                "skipped_threshold": s.orphan.cleanup_skipped_threshold(),
            },
            "lifecycle": Cycle::all().map(|cycle| {
                (cycle.label().to_string(), serde_json::json!({
                    "panics": s.cycle_health.panics(cycle),
                    "down": s.cycle_health.is_down(cycle),
                }))
            }).into_iter().collect::<serde_json::Map<_, _>>(),
        })
    }
}

/// Build the axum router for the observability endpoints.
pub fn router(state: ObservabilityState) -> Router {
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/status", get(status_handler))
        .route("/health", get(health_handler))
        .with_state(state)
}

async fn metrics_handler(State(state): State<ObservabilityState>) -> String {
    state.render_prometheus()
}

/// Pure liveness probe: `200 "ok"` whenever the process is serving requests.
///
/// A lifecycle cycle recovering from a panic is, by construction, still
/// making progress on its own retry/backoff schedule (see
/// [`crate::metrics::CycleHealth`]) — failing this endpoint while that
/// happens would let a container orchestrator's liveness probe restart the
/// process mid-recovery, aborting any in-flight job and replacing a bounded
/// backoff with a crash-restart loop. Degraded cycles stay visible per-cycle
/// on `/status` and via the `compactor_cycle_panics_total` /
/// `compactor_cycle_down` metrics, which are the right signal to alert on —
/// this endpoint is not.
async fn health_handler(State(_state): State<ObservabilityState>) -> (StatusCode, String) {
    (StatusCode::OK, "ok".to_string())
}

async fn status_handler(State(state): State<ObservabilityState>) -> Json<serde_json::Value> {
    Json(state.status_json())
}

/// Serve the observability endpoints until the process shuts down.
pub async fn serve(addr: std::net::SocketAddr, state: ObservabilityState) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    tracing::info!(%addr, "Compactor observability HTTP endpoint listening");
    axum::serve(listener, router(state)).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn test_state() -> ObservabilityState {
        let compaction = CompactionMetrics::new();
        let retention = RetentionMetrics::new();
        let orphan = OrphanMetrics::new();

        compaction.record_job_start();
        compaction.record_job_success(10, 2, 2048, 1024, Duration::from_millis(500));
        retention.record_partitions_dropped(3);
        retention.record_snapshots_expired(4);
        retention.record_bytes_reclaimed(4096);
        retention.record_unclassifiable_files(2);
        orphan.record_files_deleted(7);
        orphan.record_bytes_freed(8192);

        ObservabilityState::new(
            Uuid::nil(),
            compaction,
            retention,
            orphan,
            CycleHealth::new(),
        )
    }

    #[test]
    fn prometheus_output_contains_documented_metric_names() {
        let rendered = test_state().render_prometheus();

        // Names promised in the operator documentation must not drift.
        for name in [
            "compactor_partitions_dropped_total 3",
            "compactor_snapshots_expired_total 4",
            "compactor_files_deleted_total 7",
            "compactor_bytes_freed_total 8192",
            "compactor_deletion_failures_total 0",
            "compactor_jobs_succeeded_total 1",
            "compactor_input_files_total 10",
            "compactor_bytes_reclaimed_total 4096",
            "compactor_unclassifiable_files_total 2",
            "compactor_orphan_cleanup_skipped_total{reason=\"live_files_threshold_exceeded\"} 0",
            "compactor_cycle_panics_total{cycle=\"compaction\"} 0",
            "compactor_cycle_down{cycle=\"lease_expiry\"} 0",
        ] {
            assert!(rendered.contains(name), "missing `{name}` in:\n{rendered}");
        }
    }

    /// Every sample line's metric name must have a preceding `# TYPE` (and by
    /// construction, `# HELP`) declaration. This does NOT require a 1:1 line
    /// count — `compactor_cycle_panics_total` and `compactor_cycle_down` each
    /// declare their TYPE once and then emit one sample per [`Cycle`], which
    /// is standard Prometheus text exposition for a labelled metric.
    #[test]
    fn prometheus_output_has_help_and_type_for_every_sample() {
        let rendered = test_state().render_prometheus();
        let declared_types: std::collections::HashSet<&str> = rendered
            .lines()
            .filter_map(|l| l.strip_prefix("# TYPE "))
            .filter_map(|rest| rest.split_whitespace().next())
            .collect();

        for line in rendered.lines() {
            if line.starts_with('#') || line.is_empty() {
                continue;
            }
            let name = line.split(['{', ' ']).next().unwrap_or(line);
            assert!(
                declared_types.contains(name),
                "sample `{line}` has no matching `# TYPE {name}` line"
            );
        }
    }

    #[test]
    fn status_json_reports_all_sections() {
        let status = test_state().status_json();
        assert_eq!(status["compaction"]["jobs_succeeded"], 1);
        assert_eq!(status["retention"]["partitions_dropped"], 3);
        assert_eq!(status["retention"]["unclassifiable_files"], 2);
        assert_eq!(status["orphan_cleanup"]["files_deleted"], 7);
        assert_eq!(status["lifecycle"]["compaction"]["panics"], 0);
        assert_eq!(status["lifecycle"]["compaction"]["down"], false);
        assert!(status["instance_id"].is_string());
    }

    /// `/health` must stay a pure liveness signal: a cycle recovering from a
    /// panic is still making progress on its own backoff schedule, and
    /// failing this endpoint while that happens would let a container
    /// orchestrator restart the process mid-recovery instead of letting the
    /// bounded backoff run its course.
    #[tokio::test]
    async fn health_handler_reports_ok_regardless_of_cycle_health() {
        let cycle_health = CycleHealth::new();
        let state = ObservabilityState::new(
            Uuid::nil(),
            CompactionMetrics::new(),
            RetentionMetrics::new(),
            OrphanMetrics::new(),
            cycle_health.clone(),
        );

        let (status, body) = health_handler(State(state.clone())).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body, "ok");

        cycle_health.record_panic(Cycle::LeaseExpiry);
        let (status, body) = health_handler(State(state)).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "a cycle in backoff must not fail liveness"
        );
        assert_eq!(body, "ok");
    }

    #[tokio::test]
    async fn endpoints_serve_over_http() {
        let state = test_state();
        let addr: std::net::SocketAddr = "127.0.0.1:0".parse().unwrap();
        let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
        let bound = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, router(state)).await.unwrap();
        });

        let body = reqwest_get(bound, "/metrics").await;
        assert!(body.contains("compactor_jobs_succeeded_total 1"));

        let health = reqwest_get(bound, "/health").await;
        assert_eq!(health, "ok");

        let status = reqwest_get(bound, "/status").await;
        let parsed: serde_json::Value = serde_json::from_str(&status).unwrap();
        assert_eq!(parsed["orphan_cleanup"]["files_deleted"], 7);
    }

    /// Minimal HTTP GET over a raw TCP stream to avoid pulling in an HTTP
    /// client dependency for tests.
    async fn reqwest_get(addr: std::net::SocketAddr, path: &str) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream
            .write_all(
                format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n")
                    .as_bytes(),
            )
            .await
            .unwrap();
        let mut response = Vec::new();
        stream.read_to_end(&mut response).await.unwrap();
        let response = String::from_utf8(response).unwrap();
        response
            .split_once("\r\n\r\n")
            .map(|(_, body)| body.to_string())
            .unwrap_or_default()
    }
}
