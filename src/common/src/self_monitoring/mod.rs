//! OTLP-based self-monitoring infrastructure for SignalDB services.
//!
//! Services instrument themselves via the OpenTelemetry SDK and export
//! telemetry through the normal OTLP write pipeline (dogfooding).

pub mod metrics;
pub mod suppress;

pub use suppress::{
    OtelExportFilter, SELF_MONITORING_DATASET, SELF_MONITORING_TENANT,
    SelfTelemetrySuppressionFilter, is_self_monitoring_tenant, self_telemetry_suppressed,
    suppress_self_telemetry, suppress_self_telemetry_sync,
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

fn build_metadata(config: &Configuration) -> MetadataMap {
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

pub fn init_telemetry(config: &Configuration, service_name: &str) -> Result<Option<Telemetry>> {
    if !config.self_monitoring.enabled {
        return Ok(None);
    }

    let endpoint = &config.self_monitoring.endpoint;
    let metadata = build_metadata(config);

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", "self-monitoring"),
        ])
        .build();

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
        Some("traceidratio") | None => Sampler::TraceIdRatioBased(ratio),
        Some(other) => {
            tracing::warn!(
                sampler = %other,
                "Unsupported OTEL_TRACES_SAMPLER value; falling back to traceidratio"
            );
            Sampler::TraceIdRatioBased(ratio)
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
    fn sampler_defaults_to_config_ratio() {
        let sampler = resolve_trace_sampler(0.1, None, None);
        assert_eq!(format!("{sampler:?}"), "TraceIdRatioBased(0.1)");
    }

    #[test]
    fn sampler_arg_env_overrides_config_ratio() {
        let sampler = resolve_trace_sampler(0.1, Some("traceidratio".into()), Some("0.2".into()));
        assert_eq!(format!("{sampler:?}"), "TraceIdRatioBased(0.2)");

        // arg applies even when only the arg env is set
        let sampler = resolve_trace_sampler(0.1, None, Some("0.5".into()));
        assert_eq!(format!("{sampler:?}"), "TraceIdRatioBased(0.5)");
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
        let s = resolve_trace_sampler(5.0, None, None);
        assert_eq!(format!("{s:?}"), "TraceIdRatioBased(1.0)");
        let s = resolve_trace_sampler(0.1, None, Some("not-a-number".into()));
        assert_eq!(format!("{s:?}"), "TraceIdRatioBased(0.1)");
        let s = resolve_trace_sampler(0.1, Some("bogus".into()), None);
        assert_eq!(format!("{s:?}"), "TraceIdRatioBased(0.1)");
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
