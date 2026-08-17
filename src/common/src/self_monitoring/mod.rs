//! OTLP-based self-monitoring infrastructure for SignalDB services.
//!
//! Services instrument themselves via the OpenTelemetry SDK and export
//! telemetry through the normal OTLP write pipeline (dogfooding).

pub mod app_metrics;
pub mod metrics;
pub mod profiling;
// Self-profiling samples CPU via pyroscope's pprof-rs backend, which (like
// the external agent in `profiling`) is unavailable on Windows.
pub mod sanitize;
#[cfg(not(target_os = "windows"))]
pub mod self_profiling;
pub mod span_error;
pub mod spans;
pub mod suppress;

pub use app_metrics::{
    AppMetrics, ServerTimings, app_metrics, http_metrics_middleware, http_trace_context_middleware,
    record_rate_limit_rejection, should_count_tenant,
};
pub use profiling::{ProfilingHandle, init_profiling};
#[cfg(not(target_os = "windows"))]
pub use self_profiling::SelfProfilingHandle;
pub use span_error::record_span_exception;
pub use suppress::{
    OtelExportFilter, SELF_MONITORING_DATASET, SELF_MONITORING_TENANT,
    SelfTelemetrySuppressionFilter, is_self_monitoring_tenant, maybe_suppress_self_telemetry,
    self_telemetry_suppressed, suppress_self_telemetry, suppress_self_telemetry_sync,
};

use anyhow::{Context, Result};
use opentelemetry::KeyValue;
use opentelemetry_otlp::{
    LogExporter, MetricExporter, SpanExporter, WithExportConfig, WithTonicConfig,
};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use tonic::metadata::MetadataMap;

use crate::config::Configuration;

pub struct Telemetry {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
    sampler_description: String,
    _metrics_handle: metrics::MetricsHandle,
}

impl Telemetry {
    pub fn tracer_provider(&self) -> &SdkTracerProvider {
        &self.tracer_provider
    }

    /// Human-readable description of the active trace sampler, for startup
    /// logging.
    pub fn sampler_description(&self) -> &str {
        &self.sampler_description
    }

    pub fn logger_provider(&self) -> &SdkLoggerProvider {
        &self.logger_provider
    }

    pub fn meter_provider(&self) -> &SdkMeterProvider {
        &self.meter_provider
    }

    pub fn shutdown(self) {
        if let Err(e) = self.meter_provider.force_flush() {
            tracing::warn!(error = %e, "Failed to flush meter provider");
        }
        if let Err(e) = self.logger_provider.force_flush() {
            tracing::warn!(error = %e, "Failed to flush logger provider");
        }

        if let Err(e) = self.tracer_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown tracer provider");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown logger provider");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            tracing::warn!(error = %e, "Failed to shutdown meter provider");
        }
    }
}

/// The production tracing→OTel bridge layer.
///
/// Disables the tracing-opentelemetry convenience attributes (`busy_ns`,
/// `idle_ns`, `target`, `code.*`): none of them exist in otel/registry/ or
/// upstream semconv, so each one is a violation under
/// `weaver registry live-check`. Boundary spans carry their curated
/// attribute sets from the [`spans`] factories instead.
pub fn otel_span_layer<S>(
    tracer: opentelemetry_sdk::trace::SdkTracer,
) -> tracing_opentelemetry::OpenTelemetryLayer<S, opentelemetry_sdk::trace::SdkTracer>
where
    S: tracing::Subscriber + for<'span> tracing_subscriber::registry::LookupSpan<'span>,
{
    tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_location(false)
        .with_tracked_inactivity(false)
        .with_target(false)
}

pub(crate) fn build_metadata(config: &Configuration) -> MetadataMap {
    let mut map = MetadataMap::new();
    map.insert(
        "x-tenant-id",
        config
            .self_monitoring
            .tenant_id
            .parse()
            .expect("valid tenant header"),
    );
    map.insert(
        "x-dataset-id",
        config
            .self_monitoring
            .dataset_id
            .parse()
            .expect("valid dataset header"),
    );
    if let Some(ref admin_key) = config.auth.admin_api_key
        && let Ok(val) = format!("Bearer {admin_key}").parse()
    {
        map.insert("authorization", val);
    }
    map
}

/// The OpenTelemetry semantic-conventions version SignalDB's own telemetry
/// targets. Single source of truth: the resource `schema_url` derives from
/// it, and the convention registry (`otel/registry/manifest.yaml`) declares
/// the same version as its upstream dependency.
pub const SEMCONV_SCHEMA_URL: &str = "https://opentelemetry.io/schemas/1.43.0";

/// SignalDB's own semantic-convention registry version
/// (`https://cedricziel.github.io/signaldb/schemas/<version>`), read from
/// `otel/registry/manifest.yaml` by the build script. Every instrumentation
/// scope SignalDB emits carries it, since the `signaldb.*` attributes are
/// defined there (layered on [`SEMCONV_SCHEMA_URL`]). The version tracks the
/// SignalDB release: release-please bumps the manifest with the `common`
/// crate, and `tests/registry_pins.rs` asserts the two stay equal.
pub const SIGNALDB_SCHEMA_URL: &str = env!("SIGNALDB_SCHEMA_URL");

/// Per-process `service.instance.id`, generated once so the
/// `(service.namespace, service.name, service.instance.id)` triplet is
/// globally unique per run and stable across every provider built in it.
fn service_instance_id() -> &'static str {
    static INSTANCE_ID: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    INSTANCE_ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

/// Build the telemetry resource per the OTel resource conventions:
/// per-service `service.name` under the shared `signaldb` namespace, a
/// per-process instance id, and the stable `deployment.environment.name`
/// (config-sourced; the deprecated `deployment.environment` is not emitted).
fn build_resource(config: &Configuration, service_name: &str) -> Resource {
    use opentelemetry_semantic_conventions::attribute::{
        DEPLOYMENT_ENVIRONMENT_NAME, SERVICE_INSTANCE_ID, SERVICE_NAME, SERVICE_NAMESPACE,
        SERVICE_VERSION,
    };

    // Attributes go through `with_attributes` (which overrides the builder's
    // default-detector resource, e.g. `unknown_service`); `with_schema_url`
    // merges the other way around and would lose them.
    Resource::builder()
        .with_attributes(vec![
            KeyValue::new(SERVICE_NAME, service_name.to_string()),
            KeyValue::new(SERVICE_NAMESPACE, "signaldb"),
            KeyValue::new(SERVICE_VERSION, env!("CARGO_PKG_VERSION")),
            KeyValue::new(SERVICE_INSTANCE_ID, service_instance_id()),
            KeyValue::new(
                DEPLOYMENT_ENVIRONMENT_NAME,
                config.self_monitoring.environment.clone(),
            ),
        ])
        .with_schema_url(std::iter::empty::<KeyValue>(), SEMCONV_SCHEMA_URL)
        .build()
}

pub fn init_telemetry(config: &Configuration, service_name: &str) -> Result<Option<Telemetry>> {
    if !config.self_monitoring.enabled {
        return Ok(None);
    }

    let endpoint = &config.self_monitoring.endpoint;
    let metadata = build_metadata(config);

    let resource = build_resource(config, service_name);

    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_metadata(metadata.clone())
        .build()
        .context("Failed to build span exporter")?;
    let sampler = resolve_trace_sampler(
        config.self_monitoring.trace_sample_ratio,
        std::env::var("OTEL_TRACES_SAMPLER").ok(),
        std::env::var("OTEL_TRACES_SAMPLER_ARG").ok(),
    );
    let sampler_description = format!("{sampler:?}");
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_sampler(sampler)
        .with_batch_exporter(span_exporter)
        .build();

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_metadata(metadata.clone())
        .build()
        .context("Failed to build log exporter")?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();

    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .with_metadata(metadata)
        .build()
        .context("Failed to build metric exporter")?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();

    let metrics_handle = metrics::register_system_metrics(&meter_provider, service_name);

    // Install globals so instrumentation sites can use the OTel API without
    // threading providers through every call: W3C trace-context propagation
    // across Flight, `global::meter()` for application metrics, and
    // `global::tracer()` for manual spans.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );
    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_meter_provider(meter_provider.clone());
    // Bind application instruments to the freshly installed provider.
    let _ = app_metrics::app_metrics();

    Ok(Some(Telemetry {
        tracer_provider,
        logger_provider,
        meter_provider,
        sampler_description,
        _metrics_handle: metrics_handle,
    }))
}

/// Resolve the trace sampler from config and the standard OTel environment
/// variables (`OTEL_TRACES_SAMPLER` / `OTEL_TRACES_SAMPLER_ARG`), which take
/// precedence over `self_monitoring.trace_sample_ratio`.
///
/// When `OTEL_TRACES_SAMPLER` is unset the sampler is parent-based
/// (`ParentBased(TraceIdRatioBased(ratio))`) so a caller's sampled decision is
/// honored end-to-end and only unparented roots are ratio-sampled. Set
/// `OTEL_TRACES_SAMPLER=traceidratio` to force head-based sampling instead.
fn resolve_trace_sampler(
    config_ratio: f64,
    sampler_env: Option<String>,
    sampler_arg_env: Option<String>,
) -> opentelemetry_sdk::trace::Sampler {
    use opentelemetry_sdk::trace::Sampler;

    let ratio = sampler_arg_env
        .as_deref()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(config_ratio)
        .clamp(0.0, 1.0);

    match sampler_env.as_deref() {
        Some("always_on") => Sampler::AlwaysOn,
        Some("always_off") => Sampler::AlwaysOff,
        Some("parentbased_always_on") => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
        Some("parentbased_always_off") => Sampler::ParentBased(Box::new(Sampler::AlwaysOff)),
        Some("parentbased_traceidratio") => {
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
        }
        // Explicit opt-in to head-based ratio sampling, which ignores the
        // parent's sampled decision (standard OTel `traceidratio` semantics).
        Some("traceidratio") => Sampler::TraceIdRatioBased(ratio),
        // Unset: default to a parent-respecting sampler so an upstream caller's
        // sampled `traceparent` (and the whole query trace it roots) is always
        // kept, and only unparented root spans are ratio-sampled. Without this,
        // a bare ratio sampler drops ~`1 - ratio` of joined external traces and
        // defeats HTTP trace-context join at the query boundary.
        None => Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio))),
        // An unrecognized name falls back to the same parent-respecting
        // sampler as the unset default: a typo must not silently reintroduce
        // head-based sampling that drops joined caller traces.
        Some(other) => {
            tracing::warn!(
                sampler = %other,
                "Unsupported OTEL_TRACES_SAMPLER value; falling back to parentbased_traceidratio"
            );
            Sampler::ParentBased(Box::new(Sampler::TraceIdRatioBased(ratio)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_returns_none() {
        let config = Configuration::default();
        assert!(!config.self_monitoring.enabled);
        let result = init_telemetry(&config, "test-service").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_config_defaults() {
        let sm = crate::config::SelfMonitoringConfig::default();
        assert!(!sm.enabled);
        assert_eq!(sm.endpoint, "http://localhost:4317");
        assert_eq!(sm.tenant_id, "_system");
        assert_eq!(sm.dataset_id, "_monitoring");
        assert_eq!(sm.interval, std::time::Duration::from_secs(60));
        assert_eq!(sm.trace_sample_ratio, 0.1);
    }

    #[test]
    fn sampler_defaults_to_parent_based_config_ratio() {
        // Unset OTEL_TRACES_SAMPLER must default to a parent-respecting sampler
        // so an upstream caller's sampled `traceparent` is always kept and the
        // query trace it roots is never dropped by head sampling.
        let sampler = resolve_trace_sampler(0.1, None, None);
        assert_eq!(
            format!("{sampler:?}"),
            "ParentBased(TraceIdRatioBased(0.1))"
        );
    }

    #[test]
    fn explicit_traceidratio_stays_head_based() {
        // Setting OTEL_TRACES_SAMPLER=traceidratio is an explicit opt-in to
        // head-based sampling that ignores the parent decision.
        let sampler = resolve_trace_sampler(0.1, Some("traceidratio".into()), None);
        assert_eq!(format!("{sampler:?}"), "TraceIdRatioBased(0.1)");
    }

    #[test]
    fn sampler_arg_env_overrides_config_ratio() {
        let sampler = resolve_trace_sampler(0.1, Some("traceidratio".into()), Some("0.2".into()));
        assert_eq!(format!("{sampler:?}"), "TraceIdRatioBased(0.2)");

        // arg applies even when only the arg env is set; unset sampler stays
        // parent-based with the overridden ratio at the root.
        let sampler = resolve_trace_sampler(0.1, None, Some("0.5".into()));
        assert_eq!(
            format!("{sampler:?}"),
            "ParentBased(TraceIdRatioBased(0.5))"
        );
    }

    #[test]
    fn sampler_env_selects_always_on_off() {
        let on = resolve_trace_sampler(0.1, Some("always_on".into()), None);
        assert_eq!(format!("{on:?}"), "AlwaysOn");
        let off = resolve_trace_sampler(0.1, Some("always_off".into()), Some("0.9".into()));
        assert_eq!(format!("{off:?}"), "AlwaysOff");
    }

    #[test]
    fn sampler_env_parent_based_variants() {
        let s = resolve_trace_sampler(
            0.1,
            Some("parentbased_traceidratio".into()),
            Some("0.3".into()),
        );
        assert_eq!(format!("{s:?}"), "ParentBased(TraceIdRatioBased(0.3))");
        let s = resolve_trace_sampler(0.1, Some("parentbased_always_on".into()), None);
        assert_eq!(format!("{s:?}"), "ParentBased(AlwaysOn)");
    }

    #[test]
    fn sampler_ratio_is_clamped_and_bad_values_fall_back() {
        // Unset sampler stays parent-based; only the root ratio is clamped.
        let s = resolve_trace_sampler(5.0, None, None);
        assert_eq!(format!("{s:?}"), "ParentBased(TraceIdRatioBased(1.0))");
        let s = resolve_trace_sampler(0.1, None, Some("not-a-number".into()));
        assert_eq!(format!("{s:?}"), "ParentBased(TraceIdRatioBased(0.1))");
        // An explicit but unsupported sampler name falls back to the same
        // parent-respecting sampler as the unset default: a typo in
        // OTEL_TRACES_SAMPLER must not silently reintroduce head-based
        // sampling that drops joined caller traces.
        let s = resolve_trace_sampler(0.1, Some("bogus".into()), None);
        assert_eq!(format!("{s:?}"), "ParentBased(TraceIdRatioBased(0.1))");
    }

    #[test]
    fn resource_carries_semconv_service_identity() {
        let config = Configuration::default();
        let resource = build_resource(&config, "signaldb-querier");
        let get = |r: &opentelemetry_sdk::Resource, k: &'static str| {
            r.get(&opentelemetry::Key::from_static_str(k))
                .map(|v| v.to_string())
        };

        assert_eq!(
            get(&resource, "service.name").as_deref(),
            Some("signaldb-querier")
        );
        assert_eq!(
            get(&resource, "service.namespace").as_deref(),
            Some("signaldb")
        );
        assert_eq!(
            get(&resource, "service.version").as_deref(),
            Some(env!("CARGO_PKG_VERSION"))
        );
        assert_eq!(resource.schema_url(), Some(SEMCONV_SCHEMA_URL));

        // The instance id is a UUID, and stable within the process so the
        // (namespace, name, instance) triplet stays globally unique per run.
        let instance = get(&resource, "service.instance.id").expect("service.instance.id present");
        assert!(uuid::Uuid::parse_str(&instance).is_ok());
        let again = build_resource(&config, "signaldb-querier");
        assert_eq!(get(&again, "service.instance.id"), Some(instance));
    }

    #[test]
    fn resource_environment_is_config_sourced_stable_name() {
        let mut config = Configuration::default();
        config.self_monitoring.environment = "staging".to_string();
        let resource = build_resource(&config, "signaldb-router");

        // Stable attribute name, config-sourced value...
        assert_eq!(
            resource
                .get(&opentelemetry::Key::from_static_str(
                    "deployment.environment.name"
                ))
                .map(|v| v.to_string())
                .as_deref(),
            Some("staging")
        );
        // ...and the deprecated `deployment.environment` is gone.
        assert!(
            resource
                .get(&opentelemetry::Key::from_static_str(
                    "deployment.environment"
                ))
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_enabled_creates_telemetry() {
        let mut config = Configuration::default();
        config.self_monitoring.enabled = true;
        config.self_monitoring.endpoint = "http://127.0.0.1:4317".to_string();

        let result = init_telemetry(&config, "test-service").unwrap();
        assert!(result.is_some());

        // Drop without explicit shutdown — the providers will be dropped.
        // We skip calling shutdown() because the periodic exporter blocks
        // when the endpoint is unreachable.
        drop(result);
    }
}
