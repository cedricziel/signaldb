use std::error::Error;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use opentelemetry::logs::{AnyValue, LogRecord, Logger, LoggerProvider, Severity};
use opentelemetry::metrics::{Counter, Histogram, MeterProvider, ObservableGauge, UpDownCounter};
use opentelemetry::trace::{Span, SpanKind, Status, TraceContextExt, Tracer, TracerProvider};
use opentelemetry::{Context, KeyValue};
use opentelemetry_otlp::{LogExporter, MetricExporter, SpanExporter, WithExportConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::logs::SdkLoggerProvider;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_semantic_conventions::attribute::{
    HTTP_REQUEST_METHOD, HTTP_RESPONSE_STATUS_CODE, URL_FULL,
};
use rand::Rng;

#[derive(Debug, Parser)]
#[command(author, version, about = "SignalDB OTLP signal producer")]
struct Cli {
    #[arg(long, default_value = "http://localhost:4317")]
    endpoint: String,

    #[arg(long, default_value = "all")]
    signals: String,

    #[arg(long, default_value_t = 5)]
    interval: u64,

    #[arg(long, default_value_t = 0)]
    count: u64,

    #[arg(long, default_value = "signal-producer")]
    service_name: String,
}

#[derive(Debug, Clone, Copy)]
enum SignalKind {
    Traces,
    Logs,
    Metrics,
    All,
}

impl FromStr for SignalKind {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.trim().to_ascii_lowercase().as_str() {
            "traces" => Ok(Self::Traces),
            "logs" => Ok(Self::Logs),
            "metrics" => Ok(Self::Metrics),
            "all" => Ok(Self::All),
            other => Err(format!(
                "invalid signal selector '{other}'. expected one of: traces, logs, metrics, all"
            )),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct SignalSelection {
    traces: bool,
    logs: bool,
    metrics: bool,
}

impl SignalSelection {
    fn parse(raw: &str) -> Result<Self, String> {
        let mut selected = Self {
            traces: false,
            logs: false,
            metrics: false,
        };

        for part in raw.split(',') {
            match SignalKind::from_str(part)? {
                SignalKind::Traces => selected.traces = true,
                SignalKind::Logs => selected.logs = true,
                SignalKind::Metrics => selected.metrics = true,
                SignalKind::All => {
                    selected.traces = true;
                    selected.logs = true;
                    selected.metrics = true;
                }
            }
        }

        if !selected.traces && !selected.logs && !selected.metrics {
            return Err("no valid signal types selected".to_string());
        }

        Ok(selected)
    }
}

struct MetricInstruments {
    request_counter: Counter<u64>,
    queue_depth: UpDownCounter<f64>,
    latency_histogram: Histogram<f64>,
    _cpu_usage_gauge: ObservableGauge<f64>,
    cpu_usage_milli: Arc<AtomicU64>,
}

struct Telemetry {
    tracer_provider: SdkTracerProvider,
    logger_provider: SdkLoggerProvider,
    meter_provider: SdkMeterProvider,
    tracer: opentelemetry_sdk::trace::Tracer,
    logger: opentelemetry_sdk::logs::SdkLogger,
    metrics: MetricInstruments,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_ansi(false)
        .compact()
        .init();

    let cli = Cli::parse();
    let selection = SignalSelection::parse(&cli.signals)?;

    eprintln!(
        "starting signal producer: endpoint={} signals={} interval={}s count={} service_name={}",
        cli.endpoint, cli.signals, cli.interval, cli.count, cli.service_name
    );

    let telemetry = init_telemetry(&cli.endpoint, &cli.service_name)?;
    let interval = Duration::from_secs(cli.interval.max(1));

    run_loop(selection, cli.count, interval, &telemetry).await;

    telemetry
        .meter_provider
        .force_flush()
        .map_err(|e| format!("failed to flush meter provider: {e}"))?;
    telemetry
        .logger_provider
        .force_flush()
        .map_err(|e| format!("failed to flush logger provider: {e}"))?;

    telemetry
        .tracer_provider
        .shutdown()
        .map_err(|e| format!("failed to shutdown tracer provider: {e}"))?;
    telemetry
        .logger_provider
        .shutdown()
        .map_err(|e| format!("failed to shutdown logger provider: {e}"))?;
    telemetry
        .meter_provider
        .shutdown()
        .map_err(|e| format!("failed to shutdown meter provider: {e}"))?;

    eprintln!("signal producer stopped");
    Ok(())
}

fn init_telemetry(endpoint: &str, service_name: &str) -> Result<Telemetry, Box<dyn Error>> {
    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", service_name.to_string()),
            KeyValue::new("service.version", env!("CARGO_PKG_VERSION")),
            KeyValue::new("deployment.environment", "development"),
        ])
        .build();

    let span_exporter = SpanExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let tracer_provider = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(span_exporter)
        .build();
    let tracer = tracer_provider.tracer("signal-producer-tracer");

    let log_exporter = LogExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let logger_provider = SdkLoggerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(log_exporter)
        .build();
    let logger = logger_provider.logger("signal-producer-logger");

    let metric_exporter = MetricExporter::builder()
        .with_tonic()
        .with_endpoint(endpoint.to_string())
        .build()?;
    let meter_provider = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(metric_exporter)
        .build();
    let meter = meter_provider.meter("signal-producer-meter");

    let request_counter = meter
        .u64_counter("signaldb.producer.requests_total")
        .with_description("Total synthetic requests generated")
        .build();
    let queue_depth = meter
        .f64_up_down_counter("signaldb.producer.queue_depth")
        .with_description("Synthetic queue depth")
        .build();
    let latency_histogram = meter
        .f64_histogram("signaldb.producer.request_latency_ms")
        .with_description("Synthetic request latency in milliseconds")
        .with_unit("ms")
        .build();

    let cpu_usage_milli = Arc::new(AtomicU64::new(0));
    let cpu_usage_source = Arc::clone(&cpu_usage_milli);
    let cpu_usage_gauge = meter
        .f64_observable_gauge("signaldb.producer.cpu_utilization")
        .with_description("Synthetic CPU utilization")
        .with_unit("1")
        .with_callback(move |observer| {
            let current = cpu_usage_source.load(Ordering::Relaxed) as f64 / 1000.0;
            observer.observe(
                current,
                &[KeyValue::new("host.name", "signal-producer-local")],
            );
        })
        .build();

    // TODO: Exponential histogram output depends on exporter/reader configuration.
    // The SDK histogram instrument is present, but explicit exponential histogram
    // control is not currently configured in this producer.

    // TODO: Summary metrics are not supported by the Rust OTel SDK meter API.
    // A future implementation can emit raw OTLP Summary data using opentelemetry-proto.

    Ok(Telemetry {
        tracer_provider,
        logger_provider,
        meter_provider,
        tracer,
        logger,
        metrics: MetricInstruments {
            request_counter,
            queue_depth,
            latency_histogram,
            _cpu_usage_gauge: cpu_usage_gauge,
            cpu_usage_milli,
        },
    })
}

async fn run_loop(
    selection: SignalSelection,
    count: u64,
    interval: Duration,
    telemetry: &Telemetry,
) {
    let mut iteration = 0_u64;

    loop {
        if count > 0 && iteration >= count {
            eprintln!("completed requested {} iterations", count);
            break;
        }

        iteration += 1;

        if selection.traces {
            generate_traces(&telemetry.tracer, iteration);
        }
        if selection.logs {
            generate_logs(&telemetry.logger, iteration);
        }
        if selection.metrics {
            generate_metrics(&telemetry.metrics, iteration);
        }

        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                eprintln!("received Ctrl+C, shutting down gracefully");
                break;
            }
            _ = tokio::time::sleep(interval) => {}
        }
    }
}

fn generate_traces(tracer: &opentelemetry_sdk::trace::Tracer, iteration: u64) {
    let mut rng = rand::rng();
    let request_id = format!("req-{iteration:06}");
    let user_id = format!("user-{:04}", rng.random_range(1_u64..5000_u64));
    let order_id = rng.random_range(1000_u64..9999_u64);
    let is_error = iteration.is_multiple_of(5);

    let mut root = tracer
        .span_builder("HTTP GET /api/orders/{id}")
        .with_kind(SpanKind::Server)
        .with_attributes(vec![
            KeyValue::new(HTTP_REQUEST_METHOD, "GET"),
            KeyValue::new(
                URL_FULL,
                format!("https://api.signaldb.dev/orders/{order_id}"),
            ),
            KeyValue::new("http.route", "/api/orders/{id}"),
            KeyValue::new("request.id", request_id.clone()),
            KeyValue::new("user.id", user_id),
        ])
        .start(tracer);

    root.add_event(
        "request.received",
        vec![
            KeyValue::new("network.protocol", "http"),
            KeyValue::new("network.transport", "tcp"),
        ],
    );

    let root_cx = Context::current_with_span(root);
    let mut inventory_client = tracer
        .span_builder("InventoryService.GetAvailability")
        .with_kind(SpanKind::Client)
        .with_attributes(vec![
            KeyValue::new("rpc.system", "grpc"),
            KeyValue::new("rpc.service", "inventory.v1.InventoryService"),
            KeyValue::new("rpc.method", "GetAvailability"),
        ])
        .start_with_context(tracer, &root_cx);

    inventory_client.add_event(
        "grpc.request",
        vec![
            KeyValue::new("peer.service", "inventory"),
            KeyValue::new("attempt", 1_i64),
        ],
    );

    let inventory_cx = Context::current_with_span(inventory_client);
    let mut db_span = tracer
        .span_builder("SELECT inventory_by_sku")
        .with_kind(SpanKind::Internal)
        .with_attributes(vec![
            KeyValue::new("db.system", "postgresql"),
            KeyValue::new("db.operation", "SELECT"),
            KeyValue::new("db.namespace", "inventory"),
        ])
        .start_with_context(tracer, &inventory_cx);

    if is_error {
        db_span.set_status(Status::error("query timeout while reading inventory"));
        db_span.set_attribute(KeyValue::new("db.timeout_ms", 250_i64));
        inventory_cx
            .span()
            .set_status(Status::error("inventory downstream failure"));
        root_cx
            .span()
            .set_status(Status::error("request failed due to dependency timeout"));
        root_cx
            .span()
            .set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 503_i64));
    } else {
        root_cx
            .span()
            .set_attribute(KeyValue::new(HTTP_RESPONSE_STATUS_CODE, 200_i64));
    }

    db_span.end();
    inventory_cx.span().end();
    root_cx.span().end();
}

fn generate_logs(logger: &opentelemetry_sdk::logs::SdkLogger, iteration: u64) {
    let severities = [
        (Severity::Trace, "TRACE", "Cache lookup started"),
        (
            Severity::Debug,
            "DEBUG",
            "Resolved feature flag from config",
        ),
        (Severity::Info, "INFO", "Request accepted for processing"),
        (
            Severity::Warn,
            "WARN",
            "Upstream latency exceeded threshold",
        ),
        (Severity::Error, "ERROR", "Failed to persist span batch"),
        (
            Severity::Fatal,
            "FATAL",
            "Circuit breaker opened for all requests",
        ),
    ];

    for (severity_number, severity_text, body) in severities {
        let mut record = logger.create_log_record();
        record.set_severity_number(severity_number);
        record.set_severity_text(severity_text);
        record.set_body(AnyValue::from(format!("{body} (iteration={iteration})")));
        record.add_attribute("log.source", "signal-producer");
        record.add_attribute("iteration", iteration as i64);
        record.add_attribute("component", "loadgen");
        logger.emit(record);
    }
}

fn generate_metrics(metrics: &MetricInstruments, iteration: u64) {
    let mut rng = rand::rng();
    let service_name = if iteration.is_multiple_of(2) {
        "checkout-service"
    } else {
        "payment-service"
    };

    let attrs = [
        KeyValue::new("service.name", service_name),
        KeyValue::new("deployment.environment", "dev"),
        KeyValue::new("region", "local"),
    ];

    let latency_ms = rng.random_range(5.0_f64..250.0_f64);
    let queue_delta = rng.random_range(-3.0_f64..5.0_f64);
    let cpu_utilization = rng.random_range(0.10_f64..0.95_f64);

    metrics.request_counter.add(1, &attrs);
    metrics.queue_depth.add(queue_delta, &attrs);
    metrics.latency_histogram.record(latency_ms, &attrs);
    metrics
        .cpu_usage_milli
        .store((cpu_utilization * 1000.0) as u64, Ordering::Relaxed);
}
