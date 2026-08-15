//! System resource and service health gauges exported via OpenTelemetry.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{MeterProvider, ObservableGauge};
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub struct MetricsHandle {
    _process_cpu_gauge: ObservableGauge<f64>,
    _process_memory_gauge: ObservableGauge<f64>,
    _system_cpu_gauge: ObservableGauge<f64>,
    _system_memory_gauge: ObservableGauge<f64>,
    _uptime_gauge: ObservableGauge<f64>,
}

/// A single snapshot of process- and host-scoped resource usage.
struct ResourceSample {
    /// Resident set size of this process, in bytes. Zero when the pid is
    /// unknown.
    process_memory_bytes: u64,
    /// CPU utilization of this process as a percentage of a single core
    /// (may exceed 100 on multi-core hosts). Zero when the pid is unknown.
    process_cpu_percent: f64,
    /// Host-wide used memory, in bytes (includes page cache/ARC on some
    /// platforms).
    system_memory_bytes: u64,
    /// System-wide CPU utilization percentage averaged across all cores
    /// (0-100).
    system_cpu_percent: f64,
}

/// Refreshes `sys` and reads both process-scoped and host-scoped resource
/// usage.
///
/// CPU values are computed from deltas between refreshes, so the first sample
/// after creating the [`sysinfo::System`] reports zero CPU.
fn sample_resources(sys: &mut sysinfo::System, pid: Option<sysinfo::Pid>) -> ResourceSample {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate};

    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let (process_memory_bytes, process_cpu_percent) = match pid {
        Some(pid) => {
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory().with_cpu(),
            );
            match sys.process(pid) {
                Some(process) => (process.memory(), process.cpu_usage() as f64),
                None => (0, 0.0),
            }
        }
        None => (0, 0.0),
    };

    ResourceSample {
        process_memory_bytes,
        process_cpu_percent,
        system_memory_bytes: sys.used_memory(),
        system_cpu_percent: sys.global_cpu_usage() as f64,
    }
}

pub fn register_system_metrics(
    meter_provider: &SdkMeterProvider,
    service_name: &str,
) -> MetricsHandle {
    let meter = meter_provider.meter("signaldb.self_monitoring");
    let svc = service_name.to_string();

    let process_cpu = Arc::new(AtomicU64::new(0));
    let process_mem = Arc::new(AtomicU64::new(0));
    let system_cpu = Arc::new(AtomicU64::new(0));
    let system_mem = Arc::new(AtomicU64::new(0));
    let start_time = Instant::now();

    let process_cpu_src = Arc::clone(&process_cpu);
    let process_mem_src = Arc::clone(&process_mem);
    let system_cpu_src = Arc::clone(&system_cpu);
    let system_mem_src = Arc::clone(&system_mem);

    std::thread::spawn(move || {
        let pid = match sysinfo::get_current_pid() {
            Ok(pid) => Some(pid),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "Failed to resolve current pid; process-scoped gauges will report zero"
                );
                None
            }
        };
        let mut sys = sysinfo::System::new();
        loop {
            let sample = sample_resources(&mut sys, pid);

            process_cpu.store(sample.process_cpu_percent.to_bits(), Ordering::Relaxed);
            process_mem.store(sample.process_memory_bytes, Ordering::Relaxed);
            system_cpu.store(sample.system_cpu_percent.to_bits(), Ordering::Relaxed);
            system_mem.store(sample.system_memory_bytes, Ordering::Relaxed);

            std::thread::sleep(std::time::Duration::from_secs(15));
        }
    });

    let svc_process_cpu = svc.clone();
    let process_cpu_gauge = meter
        .f64_observable_gauge("process.cpu.utilization")
        .with_description(
            "CPU utilization of this process as a percentage of a single core \
             (may exceed 100 on multi-core hosts)",
        )
        .with_unit("1")
        .with_callback(move |observer| {
            let value = f64::from_bits(process_cpu_src.load(Ordering::Relaxed));
            observer.observe(
                value,
                &[KeyValue::new("service.name", svc_process_cpu.clone())],
            );
        })
        .build();

    let svc_process_mem = svc.clone();
    let process_memory_gauge = meter
        .f64_observable_gauge("process.memory.usage")
        .with_description("Resident set size (RSS) of this process in bytes")
        .with_unit("By")
        .with_callback(move |observer| {
            let bytes = process_mem_src.load(Ordering::Relaxed) as f64;
            observer.observe(
                bytes,
                &[KeyValue::new("service.name", svc_process_mem.clone())],
            );
        })
        .build();

    let svc_system_cpu = svc.clone();
    let system_cpu_gauge = meter
        .f64_observable_gauge("system.cpu.utilization")
        .with_description("System-wide CPU utilization percentage averaged across all cores")
        .with_unit("1")
        .with_callback(move |observer| {
            let value = f64::from_bits(system_cpu_src.load(Ordering::Relaxed));
            observer.observe(
                value,
                &[KeyValue::new("service.name", svc_system_cpu.clone())],
            );
        })
        .build();

    let svc_system_mem = svc.clone();
    let system_memory_gauge = meter
        .f64_observable_gauge("system.memory.usage")
        .with_description(
            "Host-wide used memory in bytes (includes page cache/ARC on some platforms)",
        )
        .with_unit("By")
        .with_callback(move |observer| {
            let bytes = system_mem_src.load(Ordering::Relaxed) as f64;
            observer.observe(
                bytes,
                &[KeyValue::new("service.name", svc_system_mem.clone())],
            );
        })
        .build();

    let svc_up = svc;
    let uptime_gauge = meter
        .f64_observable_gauge("process.uptime")
        .with_description("Service uptime in seconds")
        .with_unit("s")
        .with_callback(move |observer| {
            let uptime = start_time.elapsed().as_secs_f64();
            observer.observe(uptime, &[KeyValue::new("service.name", svc_up.clone())]);
        })
        .build();

    MetricsHandle {
        _process_cpu_gauge: process_cpu_gauge,
        _process_memory_gauge: process_memory_gauge,
        _system_cpu_gauge: system_cpu_gauge,
        _system_memory_gauge: system_memory_gauge,
        _uptime_gauge: uptime_gauge,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sysinfo::System;

    /// Regression test for #759: `signaldb.process.memory_usage` reported the
    /// host's used memory (e.g. ~91 GiB with a large ZFS ARC) instead of the
    /// process RSS. The process-scoped value must come from the per-process
    /// source and be strictly smaller than host used memory.
    #[test]
    fn sample_resources_reports_process_rss_not_host_used_memory() {
        let mut sys = System::new();
        let pid = sysinfo::get_current_pid().ok();
        assert!(pid.is_some(), "current pid should be resolvable in tests");

        // Sample twice so CPU deltas can be computed; only the second sample
        // is asserted on.
        sample_resources(&mut sys, pid);
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let sample = sample_resources(&mut sys, pid);

        assert!(
            sample.process_memory_bytes > 0,
            "process RSS should be nonzero"
        );
        assert!(
            sample.system_memory_bytes > 0,
            "host used memory should be nonzero"
        );
        assert!(
            sample.process_memory_bytes < sample.system_memory_bytes,
            "process RSS ({} bytes) must be strictly smaller than host used memory ({} bytes)",
            sample.process_memory_bytes,
            sample.system_memory_bytes
        );
    }

    #[test]
    fn sample_resources_reports_finite_nonnegative_cpu() {
        let mut sys = System::new();
        let pid = sysinfo::get_current_pid().ok();

        sample_resources(&mut sys, pid);
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let sample = sample_resources(&mut sys, pid);

        assert!(sample.process_cpu_percent.is_finite());
        assert!(sample.process_cpu_percent >= 0.0);
        assert!(sample.system_cpu_percent.is_finite());
        assert!(sample.system_cpu_percent >= 0.0);
    }

    #[test]
    fn sample_resources_without_pid_reports_zero_process_values() {
        let mut sys = System::new();
        let sample = sample_resources(&mut sys, None);

        assert_eq!(sample.process_memory_bytes, 0);
        assert_eq!(sample.process_cpu_percent, 0.0);
    }

    /// Registration must export process-scoped gauges alongside the
    /// system-scoped gauges so host-level visibility is not lost.
    #[test]
    fn register_system_metrics_exports_process_and_system_gauges() {
        use opentelemetry_sdk::metrics::{InMemoryMetricExporter, SdkMeterProvider};

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        let _handle = register_system_metrics(&provider, "test-service");

        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();

        let names: Vec<String> = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics().map(|m| m.name().to_string()))
            .collect();

        // OTel semantic-convention names, not signaldb.*-namespaced: any
        // OTel-aware tool already recognizes process.cpu.utilization etc.
        // without SignalDB-specific configuration (issue #1211).
        for expected in [
            "process.cpu.utilization",
            "process.memory.usage",
            "system.cpu.utilization",
            "system.memory.usage",
            "process.uptime",
        ] {
            assert!(
                names.contains(&expected.to_string()),
                "expected gauge {expected} to be registered, got: {names:?}"
            );
        }
    }
}
