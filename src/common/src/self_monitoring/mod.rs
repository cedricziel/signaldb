//! OTLP-based self-monitoring infrastructure for SignalDB services.
//!
//! Services instrument themselves via the OpenTelemetry SDK and export
//! telemetry through the normal OTLP write pipeline (dogfooding).

pub mod metrics;

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
    _metrics_handle: metrics::MetricsHandle,
}

impl Telemetry {
    pub fn tracer_provider(&self) -> &SdkTracerProvider {
        &self.tracer_provider
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
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
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

    Ok(Some(Telemetry {
        tracer_provider,
        logger_provider,
        meter_provider,
        _metrics_handle: metrics_handle,
    }))
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
    }

    #[tokio::test]
    async fn test_enabled_creates_telemetry() {
        let mut config = Configuration::default();
        config.self_monitoring.enabled = true;
        config.self_monitoring.endpoint = "http://127.0.0.1:4317".to_string();

        let result = init_telemetry(&config, "test-service").unwrap();
        assert!(result.is_some());

        let telemetry = result.unwrap();
        telemetry.shutdown();
    }
}
