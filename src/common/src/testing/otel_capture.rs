//! Test probe for the OpenTelemetry export layers.
//!
//! Regression harness for the self-monitoring anti-loop guard: processing
//! `_system`-tenant data must not export any spans or log records (issue
//! #760). The probe mirrors the production layer stack — a recording layer
//! guarded by [`OtelExportFilter`] — and counts what would be exported.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tracing_subscriber::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::registry::LookupSpan;

use crate::self_monitoring::OtelExportFilter;

/// Counts the tracing events and spans that would reach the OpenTelemetry
/// export layers, i.e. that pass [`OtelExportFilter`].
///
/// # Example
///
/// ```rust,ignore
/// let probe = OtelExportProbe::default();
/// {
///     let _guard = probe.install();
///     process_system_tenant_batch().await;
/// }
/// assert_eq!(probe.exported_events(), 0);
/// ```
#[derive(Clone, Default)]
pub struct OtelExportProbe {
    events: Arc<AtomicUsize>,
    spans: Arc<AtomicUsize>,
}

impl OtelExportProbe {
    /// Create a new probe with zeroed counters.
    pub fn new() -> Self {
        Self::default()
    }

    /// Install a thread-default subscriber that counts everything passing
    /// the export filter. Counting stops when the returned guard is
    /// dropped.
    ///
    /// Mirrors the production stack built by `cli::utils::init_logging`: a
    /// global info-level filter (the default log level) plus the per-layer
    /// [`OtelExportFilter`] guarding the export layers.
    #[must_use]
    pub fn install(&self) -> tracing::subscriber::DefaultGuard {
        let layer = CountingLayer {
            events: self.events.clone(),
            spans: self.spans.clone(),
        };
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::INFO)
            .with(layer.with_filter(OtelExportFilter));
        tracing::subscriber::set_default(subscriber)
    }

    /// Number of events (log records) that passed the export filter.
    pub fn exported_events(&self) -> usize {
        self.events.load(Ordering::SeqCst)
    }

    /// Number of spans that passed the export filter.
    pub fn exported_spans(&self) -> usize {
        self.spans.load(Ordering::SeqCst)
    }
}

struct CountingLayer {
    events: Arc<AtomicUsize>,
    spans: Arc<AtomicUsize>,
}

impl<S> Layer<S> for CountingLayer
where
    S: tracing::Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_event(
        &self,
        _event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.events.fetch_add(1, Ordering::SeqCst);
    }

    fn on_new_span(
        &self,
        _attrs: &tracing::span::Attributes<'_>,
        _id: &tracing::span::Id,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        self.spans.fetch_add(1, Ordering::SeqCst);
    }
}
