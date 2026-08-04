//! # Fleet
//!
//! Owns one OpenTelemetry SDK pipeline (tracer + logger + meter providers) per
//! service in the [topology](crate::topology). Each service is a distinct OTLP
//! resource, exactly as it would be in production — so a single distributed
//! trace naturally spans many resources, and metrics/logs carry the emitting
//! service's `service.name` + infrastructure attributes.
//!
//! All pipelines export to the same OTLP endpoint. Spans, logs and metrics are
//! correlated the way a real backend correlates them: shared `trace_id`
//! propagated across service boundaries (see [`crate::scenarios`]), logs emitted
//! inside the active span context, and histogram exemplars captured from that
//! same context.

use std::collections::HashMap;

use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter, UpDownCounter};
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;

use crate::topology::{self, Estate, Platform};

/// Explicit second-valued latency buckets shared by every duration histogram,
/// so heat-maps line up across services.
const LATENCY_BUCKETS_S: &[f64] = &[
    0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0,
];

/// The metric instruments every service carries. Which ones a given service
/// actually feeds depends on its role (a browser app records client latencies;
/// a Kafka consumer records `messaging.process.duration`), but they are created
/// uniformly so scenarios can reach for whichever fits.
pub struct Instruments {
    pub http_server_duration: Histogram<f64>,
    pub http_client_duration: Histogram<f64>,
    pub http_active_requests: UpDownCounter<i64>,
    pub rpc_client_duration: Histogram<f64>,
    pub db_client_duration: Histogram<f64>,
    pub messages_sent: Counter<u64>,
    pub messages_consumed: Counter<u64>,
    pub message_process_duration: Histogram<f64>,
    // Infrastructure metrics (host / Kubernetes), emitted once per tick.
    pub system_cpu_utilization: Gauge<f64>,
    pub system_memory_usage: Gauge<i64>,
    pub system_memory_utilization: Gauge<f64>,
    pub system_network_io: Counter<u64>,
    pub pod_cpu_usage: Gauge<f64>,
    pub pod_memory_usage: Gauge<i64>,
}

/// One service's full SDK pipeline plus its instruments.
pub struct ServiceTelemetry {
    pub name: &'static str,
    pub platform: Platform,
    pub tracer: opentelemetry_sdk::trace::Tracer,
    pub logger: opentelemetry_sdk::logs::SdkLogger,
    pub instruments: Instruments,
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
}

/// All service pipelines for the selected estates, keyed by `namespace/name`.
pub struct Fleet {
    services: HashMap<String, ServiceTelemetry>,
    /// Stable iteration order (for the per-tick infrastructure pass).
    order: Vec<String>,
}

impl Fleet {
    /// Builds SDK pipelines for every service in `estates`, all exporting to
    /// `endpoint`.
    pub fn build(endpoint: &str, estates: &[&'static Estate]) -> anyhow::Result<Self> {
        let mut services = HashMap::new();
        let mut order = Vec::new();

        for estate in estates {
            for spec in estate.services {
                let key = spec.key(estate);
                let resource = topology::build_resource(estate, spec);
                let telemetry = build_service(endpoint, spec.name, spec.platform, resource)?;
                order.push(key.clone());
                services.insert(key, telemetry);
            }
        }

        Ok(Self { services, order })
    }

    /// Looks up a service pipeline by its `namespace/name` key. Panics if the
    /// key is unknown — keys always come from the topology, so a miss is a bug.
    pub fn svc(&self, key: &str) -> &ServiceTelemetry {
        self.services
            .get(key)
            .unwrap_or_else(|| panic!("unknown service in fleet: {key}"))
    }

    /// Iterates every service pipeline in a stable order.
    pub fn iter(&self) -> impl Iterator<Item = &ServiceTelemetry> {
        self.order.iter().map(move |k| &self.services[k])
    }

    /// Flushes and shuts down every pipeline. Best-effort: logs failures but
    /// continues so one bad provider cannot strand the rest.
    pub fn shutdown(self) {
        for (key, svc) in self.services {
            if let Err(e) = svc.meter_provider.force_flush() {
                eprintln!("flush meter provider {key} failed: {e}");
            }
            if let Err(e) = svc.logger_provider.force_flush() {
                eprintln!("flush logger provider {key} failed: {e}");
            }
            if let Err(e) = svc.tracer_provider.shutdown() {
                eprintln!("shutdown tracer provider {key} failed: {e}");
            }
            if let Err(e) = svc.logger_provider.shutdown() {
                eprintln!("shutdown logger provider {key} failed: {e}");
            }
            if let Err(e) = svc.meter_provider.shutdown() {
                eprintln!("shutdown meter provider {key} failed: {e}");
            }
        }
    }
}

#[cfg(test)]
impl Fleet {
    /// Builds the fleet with a shared in-memory span exporter (and inert
    /// log/metric pipelines) so tests can inspect every finished span.
    pub(crate) fn build_in_memory(
        estates: &[&'static Estate],
    ) -> (Self, opentelemetry_sdk::trace::InMemorySpanExporter) {
        let exporter = opentelemetry_sdk::trace::InMemorySpanExporter::default();
        let mut services = HashMap::new();
        let mut order = Vec::new();

        for estate in estates {
            for spec in estate.services {
                let key = spec.key(estate);
                let resource = topology::build_resource(estate, spec);
                let tracer_provider = SdkTracerProvider::builder()
                    .with_resource(resource.clone())
                    .with_simple_exporter(exporter.clone())
                    .build();
                let tracer =
                    opentelemetry::trace::TracerProvider::tracer(&tracer_provider, spec.name);
                let logger_provider = SdkLoggerProvider::builder().with_resource(resource).build();
                let logger =
                    opentelemetry::logs::LoggerProvider::logger(&logger_provider, spec.name);
                let meter_provider = SdkMeterProvider::builder().build();
                let meter =
                    opentelemetry::metrics::MeterProvider::meter(&meter_provider, spec.name);
                let instruments = build_instruments(&meter);

                order.push(key.clone());
                services.insert(
                    key,
                    ServiceTelemetry {
                        name: spec.name,
                        platform: spec.platform,
                        tracer,
                        logger,
                        instruments,
                        tracer_provider,
                        logger_provider,
                        meter_provider,
                    },
                );
            }
        }

        (Self { services, order }, exporter)
    }
}

/// Builds a single service's tracer/logger/meter pipeline against `endpoint`.
fn build_service(
    endpoint: &str,
    name: &'static str,
    platform: Platform,
    resource: Resource,
) -> anyhow::Result<ServiceTelemetry> {
    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, name);

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();
    let logger = opentelemetry::logs::LoggerProvider::logger(&logger_provider, name);

    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();
    let meter = opentelemetry::metrics::MeterProvider::meter(&meter_provider, name);

    let instruments = build_instruments(&meter);

    Ok(ServiceTelemetry {
        name,
        platform,
        tracer,
        logger,
        instruments,
        tracer_provider,
        logger_provider,
        meter_provider,
    })
}

/// Creates every instrument for one service's meter.
fn build_instruments(meter: &Meter) -> Instruments {
    let seconds_hist = |name: &'static str, desc: &'static str| {
        meter
            .f64_histogram(name)
            .with_description(desc)
            .with_unit("s")
            .with_boundaries(LATENCY_BUCKETS_S.to_vec())
            .build()
    };

    Instruments {
        http_server_duration: seconds_hist(
            "http.server.request.duration",
            "Duration of inbound HTTP server requests",
        ),
        http_client_duration: seconds_hist(
            "http.client.request.duration",
            "Duration of outbound HTTP client requests",
        ),
        http_active_requests: meter
            .i64_up_down_counter("http.server.active_requests")
            .with_description("In-flight HTTP server requests")
            .with_unit("{request}")
            .build(),
        rpc_client_duration: seconds_hist("rpc.client.duration", "Duration of outbound RPC calls"),
        db_client_duration: seconds_hist(
            "db.client.operation.duration",
            "Duration of database client operations",
        ),
        messages_sent: meter
            .u64_counter("messaging.client.sent.messages")
            .with_description("Messages published to a broker")
            .with_unit("{message}")
            .build(),
        messages_consumed: meter
            .u64_counter("messaging.client.consumed.messages")
            .with_description("Messages consumed from a broker")
            .with_unit("{message}")
            .build(),
        message_process_duration: seconds_hist(
            "messaging.process.duration",
            "Duration of message processing",
        ),
        system_cpu_utilization: meter
            .f64_gauge("system.cpu.utilization")
            .with_description("Host CPU utilization (0..1)")
            .with_unit("1")
            .build(),
        system_memory_usage: meter
            .i64_gauge("system.memory.usage")
            .with_description("Host memory used")
            .with_unit("By")
            .build(),
        system_memory_utilization: meter
            .f64_gauge("system.memory.utilization")
            .with_description("Host memory utilization (0..1)")
            .with_unit("1")
            .build(),
        system_network_io: meter
            .u64_counter("system.network.io")
            .with_description("Host network bytes transferred")
            .with_unit("By")
            .build(),
        pod_cpu_usage: meter
            .f64_gauge("k8s.pod.cpu.usage")
            .with_description("Pod CPU usage in cores")
            .with_unit("{cpu}")
            .build(),
        pod_memory_usage: meter
            .i64_gauge("k8s.pod.memory.usage")
            .with_description("Pod memory working set")
            .with_unit("By")
            .build(),
    }
}
