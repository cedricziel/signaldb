//! Anti-loop guard for self-monitoring telemetry.
//!
//! When SignalDB dogfoods its own observability pipeline, processing a
//! self-monitoring OTLP export must not itself generate more self-monitoring
//! telemetry — otherwise every export triggers instrumentation whose export
//! triggers more instrumentation, an infinite feedback loop.
//!
//! The guard works with a tokio task-local marker: request handlers wrap the
//! processing of `_system`-tenant requests in [`suppress_self_telemetry`], and
//! the OpenTelemetry `tracing` layers are filtered with
//! [`SelfTelemetrySuppressionFilter`] so that no spans or log records are
//! exported while the marker is set. Ingestion still happens normally — the
//! telemetry data itself is stored — it just isn't re-instrumented.

use std::future::Future;

/// Tenant ID reserved for SignalDB's own telemetry (dogfooding).
///
/// Requests authenticated under this tenant are subject to the anti-loop
/// guard: their processing is not re-instrumented.
pub const SELF_MONITORING_TENANT: &str = "_system";

/// Dataset ID used for SignalDB's own telemetry by default.
pub const SELF_MONITORING_DATASET: &str = "_monitoring";

tokio::task_local! {
    static SUPPRESS_SELF_TELEMETRY: ();
}

/// Returns true when `tenant_id` is the reserved self-monitoring tenant.
pub fn is_self_monitoring_tenant(tenant_id: &str) -> bool {
    tenant_id == SELF_MONITORING_TENANT
}

/// Run `fut` with self-monitoring telemetry export suppressed.
///
/// Spans and events created while the future is being polled are still
/// visible to the fmt (console) layer but are filtered out of the
/// OpenTelemetry export layers, breaking the telemetry feedback loop.
pub async fn suppress_self_telemetry<F: Future>(fut: F) -> F::Output {
    SUPPRESS_SELF_TELEMETRY.scope((), fut).await
}

/// Run `f` synchronously with self-monitoring telemetry export suppressed.
pub fn suppress_self_telemetry_sync<T>(f: impl FnOnce() -> T) -> T {
    SUPPRESS_SELF_TELEMETRY.sync_scope((), f)
}

/// Whether the current task is processing a self-monitoring request.
///
/// Used by the OTel export layer filters and by metric recording sites that
/// must not count `_system` traffic (anti-loop guard for counters).
pub fn self_telemetry_suppressed() -> bool {
    SUPPRESS_SELF_TELEMETRY.try_with(|_| ()).is_ok()
}

/// A per-layer `tracing_subscriber` filter that drops all spans and events
/// while [`self_telemetry_suppressed`] is true.
///
/// Attach with `.with_filter(SelfTelemetrySuppressionFilter)` to the
/// OpenTelemetry span/log bridge layers only — the fmt layer stays unfiltered
/// so operators still see console output for `_system` request processing.
#[derive(Clone, Copy, Debug, Default)]
pub struct SelfTelemetrySuppressionFilter;

impl<S> tracing_subscriber::layer::Filter<S> for SelfTelemetrySuppressionFilter {
    fn enabled(
        &self,
        _meta: &tracing::Metadata<'_>,
        _cx: &tracing_subscriber::layer::Context<'_, S>,
    ) -> bool {
        !self_telemetry_suppressed()
    }

    fn callsite_enabled(
        &self,
        _meta: &'static tracing::Metadata<'static>,
    ) -> tracing::subscriber::Interest {
        // The verdict depends on runtime task-local state, so it can never be
        // cached per-callsite.
        tracing::subscriber::Interest::sometimes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn suppression_flag_is_scoped_to_future() {
        assert!(!self_telemetry_suppressed());
        suppress_self_telemetry(async {
            assert!(self_telemetry_suppressed());
            // nested awaits keep the flag
            tokio::task::yield_now().await;
            assert!(self_telemetry_suppressed());
        })
        .await;
        assert!(!self_telemetry_suppressed());
    }

    #[tokio::test]
    async fn suppression_does_not_leak_to_other_tasks() {
        // A freshly spawned task has its own task-local context.
        let spawned_suppressed = suppress_self_telemetry(async {
            let handle = tokio::spawn(async { self_telemetry_suppressed() });
            handle.await.unwrap()
        })
        .await;
        assert!(!spawned_suppressed);
    }

    #[test]
    fn sync_scope_sets_and_clears_flag() {
        assert!(!self_telemetry_suppressed());
        suppress_self_telemetry_sync(|| assert!(self_telemetry_suppressed()));
        assert!(!self_telemetry_suppressed());
    }

    #[test]
    fn tenant_check() {
        assert!(is_self_monitoring_tenant("_system"));
        assert!(!is_self_monitoring_tenant("acme"));
        assert!(!is_self_monitoring_tenant("_monitoring"));
    }
}
