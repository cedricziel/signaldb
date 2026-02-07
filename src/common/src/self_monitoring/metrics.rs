//! System resource and service health gauges exported via OpenTelemetry.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{MeterProvider, ObservableGauge};
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub struct MetricsHandle {
    _cpu_gauge: ObservableGauge<f64>,
    _memory_gauge: ObservableGauge<f64>,
    _uptime_gauge: ObservableGauge<f64>,
}

pub fn register_system_metrics(
    meter_provider: &SdkMeterProvider,
    service_name: &str,
) -> MetricsHandle {
    let meter = meter_provider.meter("signaldb.self_monitoring");
    let svc = service_name.to_string();

    let cpu_usage = Arc::new(AtomicU64::new(0));
    let mem_usage = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let cpu_src = Arc::clone(&cpu_usage);
    let mem_src = Arc::clone(&mem_usage);

    std::thread::spawn({
        let cpu_usage = Arc::clone(&cpu_usage);
        let mem_usage = Arc::clone(&mem_usage);
        move || {
            use sysinfo::System;
            let mut sys = System::new();
            loop {
                sys.refresh_cpu_usage();
                sys.refresh_memory();

                let cpu = sys.global_cpu_usage() as f64;
                cpu_usage.store(cpu.to_bits(), Ordering::Relaxed);

                let mem_bytes = sys.used_memory();
                mem_usage.store(mem_bytes, Ordering::Relaxed);

                std::thread::sleep(std::time::Duration::from_secs(15));
            }
        }
    });

    let svc_cpu = svc.clone();
    let cpu_gauge = meter
        .f64_observable_gauge("signaldb.process.cpu_utilization")
        .with_description("Process CPU utilization percentage")
        .with_unit("1")
        .with_callback(move |observer| {
            let bits = cpu_src.load(Ordering::Relaxed);
            let value = f64::from_bits(bits);
            observer.observe(value, &[KeyValue::new("service.name", svc_cpu.clone())]);
        })
        .build();

    let svc_mem = svc.clone();
    let memory_gauge = meter
        .f64_observable_gauge("signaldb.process.memory_usage")
        .with_description("System used memory in bytes")
        .with_unit("By")
        .with_callback(move |observer| {
            let bytes = mem_src.load(Ordering::Relaxed) as f64;
            observer.observe(bytes, &[KeyValue::new("service.name", svc_mem.clone())]);
        })
        .build();

    let svc_up = svc;
    let uptime_gauge = meter
        .f64_observable_gauge("signaldb.process.uptime")
        .with_description("Service uptime in seconds")
        .with_unit("s")
        .with_callback(move |observer| {
            let uptime = start_time.elapsed().as_secs_f64();
            observer.observe(uptime, &[KeyValue::new("service.name", svc_up.clone())]);
        })
        .build();

    MetricsHandle {
        _cpu_gauge: cpu_gauge,
        _memory_gauge: memory_gauge,
        _uptime_gauge: uptime_gauge,
    }
}
