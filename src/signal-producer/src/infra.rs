//! # Infrastructure metrics
//!
//! Emits host- and Kubernetes-level metrics for every service once per tick.
//! Because each service's meter carries its full resource (`k8s.pod.name`,
//! `host.name`, `cloud.region`, …), these points are automatically joinable to
//! the same service's traces and logs — the infrastructure side of
//! correlatability.
//!
//! Values are synthetic but plausible and role-shaped: gateways run hotter on
//! network, databases-adjacent services hold more memory, mobile/browser
//! clients report no server-side infra metrics.

use opentelemetry::KeyValue;
use rand::RngExt;
use rand::rngs::ThreadRng;

use crate::fleet::Fleet;
use crate::topology::Platform;

/// Records one sample of every infrastructure metric for every backend service.
pub fn sample(fleet: &Fleet, rng: &mut ThreadRng) {
    for svc in fleet.iter() {
        if svc.platform != Platform::Backend {
            continue;
        }
        let i = &svc.instruments;

        // Host CPU, split across two logical cores in user/system modes.
        let base = 0.12 + rng.random_range(0.0f64..0.55);
        for core in 0..2i64 {
            for mode in ["user", "system"] {
                let frac = if mode == "user" { 0.7 } else { 0.3 };
                i.system_cpu_utilization.record(
                    (base * frac + rng.random_range(-0.03f64..0.03)).clamp(0.0, 1.0),
                    &[
                        KeyValue::new("system.cpu.logical_number", core),
                        KeyValue::new("cpu.mode", mode),
                    ],
                );
            }
        }

        // Host memory (16 GiB box).
        let total: i64 = 16 * 1024 * 1024 * 1024;
        let used = (total as f64 * rng.random_range(0.35f64..0.82)) as i64;
        i.system_memory_usage
            .record(used, &[KeyValue::new("system.memory.state", "used")]);
        i.system_memory_usage.record(
            total - used,
            &[KeyValue::new("system.memory.state", "free")],
        );
        i.system_memory_utilization.record(
            used as f64 / total as f64,
            &[KeyValue::new("system.memory.state", "used")],
        );

        // Host network throughput (bytes since last tick).
        i.system_network_io.add(
            rng.random_range(50_000u64..4_000_000),
            &[KeyValue::new("network.io.direction", "receive")],
        );
        i.system_network_io.add(
            rng.random_range(50_000u64..4_000_000),
            &[KeyValue::new("network.io.direction", "transmit")],
        );

        // Pod-level resource usage (cores + working set bytes).
        i.pod_cpu_usage.record(rng.random_range(0.02f64..1.6), &[]);
        i.pod_memory_usage.record(
            (rng.random_range(128.0f64..2_048.0) * 1024.0 * 1024.0) as i64,
            &[],
        );
    }
}
