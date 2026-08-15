//! System resource and service health gauges exported via OpenTelemetry.
//!
//! Every instrument name, unit, and instrument kind matches its OTel
//! semantic-convention definition exactly (verified against
//! `open-telemetry/semantic-conventions`'s `model/process/metrics.yaml` and
//! `model/system/metrics.yaml`) so any OTel-aware tool recognizes them
//! without SignalDB-specific configuration (issue #1211). `process.cpu.
//! utilization`'s `cpu.mode` attribute is `required`, not just recommended —
//! `sysinfo` has no per-mode breakdown for a process (only one combined
//! percentage), so that one metric sources its raw counters from the OS
//! directly; see [`process_cpu_time_by_mode`].

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use opentelemetry::KeyValue;
use opentelemetry::metrics::{MeterProvider, ObservableGauge, ObservableUpDownCounter};
use opentelemetry_sdk::metrics::SdkMeterProvider;

pub struct MetricsHandle {
    _process_cpu_gauge: ObservableGauge<f64>,
    _process_memory_counter: ObservableUpDownCounter<f64>,
    _system_cpu_gauge: ObservableGauge<f64>,
    _system_memory_counter: ObservableUpDownCounter<f64>,
    _uptime_gauge: ObservableGauge<f64>,
}

/// Cumulative process CPU time by mode (`user`, `system`), in seconds, since
/// process start. `sysinfo` doesn't expose this breakdown for a process —
/// only a single combined percentage — so this reads the OS accounting API
/// directly. Always measures the *current* process: self-monitoring never
/// samples any other pid.
#[cfg(unix)]
fn process_cpu_time_by_mode() -> (f64, f64) {
    // SAFETY: `usage` is a plain-old-data struct the kernel fills in
    // entirely; zero-initializing before the call is the standard libc
    // pattern. RUSAGE_SELF always refers to the calling process, so the
    // call cannot fail for a bad target.
    let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, &mut usage) } != 0 {
        return (0.0, 0.0);
    }
    let secs = |tv: libc::timeval| tv.tv_sec as f64 + tv.tv_usec as f64 / 1_000_000.0;
    (secs(usage.ru_utime), secs(usage.ru_stime))
}

/// Windows stub for [`process_cpu_time_by_mode`]: not implemented, so the
/// process.cpu.utilization gauge reports zero for both modes on Windows
/// rather than a real value. Real support would read `GetProcessTimes`.
#[cfg(windows)]
fn process_cpu_time_by_mode() -> (f64, f64) {
    (0.0, 0.0)
}

/// A single snapshot of process- and host-scoped resource usage, excluding
/// per-mode process CPU time (sampled separately — see
/// [`process_cpu_time_by_mode`]).
struct ResourceSample {
    /// Resident set size of this process, in bytes. Zero when the pid is
    /// unknown.
    process_memory_bytes: u64,
    /// Host-wide used memory, in bytes (includes page cache/ARC on some
    /// platforms).
    system_memory_bytes: u64,
    /// System-wide CPU utilization ratio (0.0-1.0) averaged across all
    /// cores.
    system_cpu_ratio: f64,
    /// How long this process has been running, in seconds, sourced from the
    /// OS via `sysinfo`. Zero when the pid is unknown.
    process_uptime_secs: f64,
}

/// Refreshes `sys` and reads everything [`ResourceSample`] carries.
///
/// `system_cpu_ratio` is computed from deltas between refreshes, so the
/// first sample after creating the [`sysinfo::System`] reports zero.
fn sample_resources(sys: &mut sysinfo::System, pid: Option<sysinfo::Pid>) -> ResourceSample {
    use sysinfo::{ProcessRefreshKind, ProcessesToUpdate};

    sys.refresh_cpu_usage();
    sys.refresh_memory();

    let (process_memory_bytes, process_uptime_secs) = match pid {
        Some(pid) => {
            sys.refresh_processes_specifics(
                ProcessesToUpdate::Some(&[pid]),
                true,
                ProcessRefreshKind::nothing().with_memory(),
            );
            match sys.process(pid) {
                Some(process) => (process.memory(), process.run_time() as f64),
                None => (0, 0.0),
            }
        }
        None => (0, 0.0),
    };

    ResourceSample {
        process_memory_bytes,
        system_memory_bytes: sys.used_memory(),
        system_cpu_ratio: sys.global_cpu_usage() as f64 / 100.0,
        process_uptime_secs,
    }
}

pub fn register_system_metrics(
    meter_provider: &SdkMeterProvider,
    service_name: &str,
) -> MetricsHandle {
    let meter = meter_provider.meter("signaldb.self_monitoring");
    let svc = service_name.to_string();

    let process_cpu_user = Arc::new(AtomicU64::new(0));
    let process_cpu_system = Arc::new(AtomicU64::new(0));
    let process_mem = Arc::new(AtomicU64::new(0));
    let system_cpu = Arc::new(AtomicU64::new(0));
    let system_mem = Arc::new(AtomicU64::new(0));
    let uptime = Arc::new(AtomicU64::new(0));

    let process_cpu_user_src = Arc::clone(&process_cpu_user);
    let process_cpu_system_src = Arc::clone(&process_cpu_system);
    let process_mem_src = Arc::clone(&process_mem);
    let system_cpu_src = Arc::clone(&system_cpu);
    let system_mem_src = Arc::clone(&system_mem);
    let uptime_src = Arc::clone(&uptime);

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
        let num_cpus = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(1) as f64;
        // None on the first iteration: there's no prior sample to diff
        // against, so the ratio is undefined rather than zero-elapsed-time
        // nonsense — matches sysinfo's own "first CPU sample is zero"
        // convention for system_cpu_ratio.
        let mut prev_cpu_sample: Option<(f64, f64, Instant)> = None;
        loop {
            let sample = sample_resources(&mut sys, pid);
            let (user_secs, system_secs) = process_cpu_time_by_mode();
            let now = Instant::now();
            let (user_ratio, system_ratio) = match prev_cpu_sample {
                Some((prev_user, prev_system, prev_instant)) => {
                    let elapsed = now.duration_since(prev_instant).as_secs_f64();
                    if elapsed > 0.0 {
                        (
                            ((user_secs - prev_user) / (elapsed * num_cpus)).max(0.0),
                            ((system_secs - prev_system) / (elapsed * num_cpus)).max(0.0),
                        )
                    } else {
                        (0.0, 0.0)
                    }
                }
                None => (0.0, 0.0),
            };
            prev_cpu_sample = Some((user_secs, system_secs, now));

            process_cpu_user.store(user_ratio.to_bits(), Ordering::Relaxed);
            process_cpu_system.store(system_ratio.to_bits(), Ordering::Relaxed);
            process_mem.store(sample.process_memory_bytes, Ordering::Relaxed);
            system_cpu.store(sample.system_cpu_ratio.to_bits(), Ordering::Relaxed);
            system_mem.store(sample.system_memory_bytes, Ordering::Relaxed);
            uptime.store(sample.process_uptime_secs.to_bits(), Ordering::Relaxed);

            std::thread::sleep(std::time::Duration::from_secs(15));
        }
    });

    let svc_process_cpu = svc.clone();
    let process_cpu_gauge = meter
        .f64_observable_gauge("process.cpu.utilization")
        .with_description(
            "Difference in process CPU time since the last measurement, divided \
             by the elapsed time and the number of CPUs available to the process",
        )
        .with_unit("1")
        .with_callback(move |observer| {
            let svc_kv = KeyValue::new("service.name", svc_process_cpu.clone());
            observer.observe(
                f64::from_bits(process_cpu_user_src.load(Ordering::Relaxed)),
                &[svc_kv.clone(), KeyValue::new("cpu.mode", "user")],
            );
            observer.observe(
                f64::from_bits(process_cpu_system_src.load(Ordering::Relaxed)),
                &[svc_kv, KeyValue::new("cpu.mode", "system")],
            );
        })
        .build();

    let svc_process_mem = svc.clone();
    let process_memory_counter = meter
        .f64_observable_up_down_counter("process.memory.usage")
        .with_description("The amount of physical memory (RSS) in use by this process")
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
        .with_description("System-wide CPU utilization ratio averaged across all cores")
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
    let system_memory_counter = meter
        .f64_observable_up_down_counter("system.memory.usage")
        .with_description("Host-wide memory in use (includes page cache/ARC on some platforms)")
        .with_unit("By")
        .with_callback(move |observer| {
            let bytes = system_mem_src.load(Ordering::Relaxed) as f64;
            observer.observe(
                bytes,
                &[
                    KeyValue::new("service.name", svc_system_mem.clone()),
                    KeyValue::new("system.memory.state", "used"),
                ],
            );
        })
        .build();

    let svc_up = svc;
    let uptime_gauge = meter
        .f64_observable_gauge("process.uptime")
        .with_description("The time this process has been running, in seconds")
        .with_unit("s")
        .with_callback(move |observer| {
            let secs = f64::from_bits(uptime_src.load(Ordering::Relaxed));
            observer.observe(secs, &[KeyValue::new("service.name", svc_up.clone())]);
        })
        .build();

    MetricsHandle {
        _process_cpu_gauge: process_cpu_gauge,
        _process_memory_counter: process_memory_counter,
        _system_cpu_gauge: system_cpu_gauge,
        _system_memory_counter: system_memory_counter,
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
    fn sample_resources_reports_finite_nonnegative_system_cpu_ratio() {
        let mut sys = System::new();
        let pid = sysinfo::get_current_pid().ok();

        sample_resources(&mut sys, pid);
        std::thread::sleep(sysinfo::MINIMUM_CPU_UPDATE_INTERVAL);
        let sample = sample_resources(&mut sys, pid);

        assert!(sample.system_cpu_ratio.is_finite());
        assert!(sample.system_cpu_ratio >= 0.0);
        // A ratio (unit "1"), not a percentage — comfortably under 100 even
        // on a fully-loaded multi-core host, unlike the old 0-100+ scale.
        assert!(sample.system_cpu_ratio < 100.0);
    }

    #[test]
    fn sample_resources_reports_positive_uptime_for_a_live_pid() {
        let mut sys = System::new();
        let pid = sysinfo::get_current_pid().ok();
        let sample = sample_resources(&mut sys, pid);

        assert!(sample.process_uptime_secs.is_finite());
        assert!(sample.process_uptime_secs >= 0.0);
    }

    #[test]
    fn sample_resources_without_pid_reports_zero_process_values() {
        let mut sys = System::new();
        let sample = sample_resources(&mut sys, None);

        assert_eq!(sample.process_memory_bytes, 0);
        assert_eq!(sample.process_uptime_secs, 0.0);
    }

    /// Regression test for the semconv-alignment audit (issue #1211
    /// follow-up): process.cpu.utilization's per-mode breakdown must come
    /// from real, distinct user/system CPU time, not a single value copied
    /// into both modes.
    #[test]
    fn process_cpu_time_by_mode_reports_finite_nonnegative_and_monotonic_values() {
        let (user1, system1) = process_cpu_time_by_mode();
        assert!(user1.is_finite() && user1 >= 0.0);
        assert!(system1.is_finite() && system1 >= 0.0);

        // Burn a little CPU so the counters are guaranteed to have moved.
        let mut acc: u64 = 0;
        for i in 0..5_000_000u64 {
            acc = acc.wrapping_add(i);
        }
        std::hint::black_box(acc);

        let (user2, system2) = process_cpu_time_by_mode();
        // Cumulative counters (like process.cpu.time) never go backwards.
        assert!(user2 >= user1);
        assert!(system2 >= system1);
    }

    /// Registration must export process-scoped gauges alongside the
    /// system-scoped gauges so host-level visibility is not lost.
    #[test]
    fn register_system_metrics_exports_process_and_system_gauges() {
        use opentelemetry_sdk::metrics::InMemoryMetricExporter;
        use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};

        let exporter = InMemoryMetricExporter::default();
        let provider = SdkMeterProvider::builder()
            .with_periodic_exporter(exporter.clone())
            .build();

        let _handle = register_system_metrics(&provider, "test-service");

        provider.force_flush().unwrap();
        let finished = exporter.get_finished_metrics().unwrap();

        let metrics: Vec<_> = finished
            .iter()
            .flat_map(|rm| rm.scope_metrics())
            .flat_map(|sm| sm.metrics())
            .collect();
        let names: Vec<String> = metrics.iter().map(|m| m.name().to_string()).collect();

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
                "expected metric {expected} to be registered, got: {names:?}"
            );
        }

        let find = |name: &str| metrics.iter().find(|m| m.name() == name).unwrap();

        // process.cpu.utilization: one data point per cpu.mode (user,
        // system) — the attribute the OTel spec marks `required`.
        let AggregatedMetrics::F64(MetricData::Gauge(process_cpu)) =
            find("process.cpu.utilization").data()
        else {
            panic!("process.cpu.utilization should be an f64 Gauge");
        };
        let modes: Vec<Option<String>> = process_cpu
            .data_points()
            .map(|dp| {
                dp.attributes()
                    .find(|kv| kv.key.as_str() == "cpu.mode")
                    .map(|kv| kv.value.to_string())
            })
            .collect();
        assert!(
            modes.contains(&Some("user".to_string())),
            "expected a cpu.mode=user data point, got {modes:?}"
        );
        assert!(
            modes.contains(&Some("system".to_string())),
            "expected a cpu.mode=system data point, got {modes:?}"
        );

        // process.memory.usage / system.memory.usage: UpDownCounter
        // (Sum, non-monotonic), not Gauge — the OTel spec's instrument kind.
        for name in ["process.memory.usage", "system.memory.usage"] {
            let AggregatedMetrics::F64(MetricData::Sum(sum)) = find(name).data() else {
                panic!("{name} should be an f64 Sum (UpDownCounter)");
            };
            assert!(
                !sum.is_monotonic(),
                "{name} is an UpDownCounter — must not be monotonic"
            );
        }

        // system.memory.usage requires a system.memory.state attribute.
        let AggregatedMetrics::F64(MetricData::Sum(system_mem)) =
            find("system.memory.usage").data()
        else {
            panic!("system.memory.usage should be an f64 Sum");
        };
        let has_state = system_mem.data_points().any(|dp| {
            dp.attributes().any(|kv| {
                kv.key.as_str() == "system.memory.state" && kv.value.to_string() == "used"
            })
        });
        assert!(
            has_state,
            "expected a system.memory.state=used attribute on system.memory.usage"
        );
    }
}
