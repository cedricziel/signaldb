//! Continuous profiling for SignalDB services.
//!
//! CPU profiles are captured with the Pyroscope agent's pprof-rs backend and
//! pushed to a Grafana Pyroscope server. Memory (heap) profiling is
//! available behind the `jemalloc-profiling` cargo feature: binaries built
//! with it install jemalloc as the global allocator and run Pyroscope's
//! jemalloc backend (requires `MALLOC_CONF=prof:true,prof_active:true` or the
//! `_RJEM_MALLOC_CONF` equivalent at runtime).
//!
//! Both are opt-in at runtime via the `[profiling]` config section and add
//! nothing when disabled.

use anyhow::{Context, Result};
use pyroscope::PyroscopeAgent;
use pyroscope::backend::{BackendConfig, PprofConfig, pprof_backend};
use pyroscope::pyroscope::{PyroscopeAgentBuilder, PyroscopeAgentRunning};

use crate::config::Configuration;

/// Spy name reported to the Pyroscope server.
const SPY_NAME: &str = "pyroscope-rs";

/// Handle to running profiling agents; stop via [`ProfilingHandle::shutdown`].
pub struct ProfilingHandle {
    agents: Vec<PyroscopeAgent<PyroscopeAgentRunning>>,
}

impl ProfilingHandle {
    /// Stop the profiling agent(s), flushing any pending profile data.
    pub fn shutdown(self) {
        for agent in self.agents {
            match agent.stop() {
                Ok(stopped) => stopped.shutdown(),
                Err(e) => tracing::warn!(error = %e, "Failed to stop Pyroscope agent"),
            }
        }
    }
}

/// Start continuous profiling when `[profiling] enabled = true`.
///
/// Returns `Ok(None)` when disabled. CPU profiling starts unconditionally
/// when enabled; memory profiling additionally requires
/// `memory_profiling = true` **and** the `jemalloc-profiling` build feature
/// (a warning is logged when configured without the feature).
pub fn init_profiling(
    config: &Configuration,
    service_name: &str,
) -> Result<Option<ProfilingHandle>> {
    if !config.profiling.enabled {
        return Ok(None);
    }

    let profiling = &config.profiling;
    let mut agents = Vec::new();

    let cpu_agent = PyroscopeAgentBuilder::new(
        profiling.pyroscope_url.as_str(),
        service_name,
        profiling.cpu_sample_rate,
        SPY_NAME,
        env!("CARGO_PKG_VERSION"),
        pprof_backend(
            PprofConfig {
                sample_rate: profiling.cpu_sample_rate,
            },
            BackendConfig::default(),
        ),
    )
    .tags(vec![
        ("service.name", service_name),
        ("deployment.environment", "self-monitoring"),
    ])
    .build()
    .context("Failed to build Pyroscope CPU agent")?
    .start()
    .context("Failed to start Pyroscope CPU agent")?;
    agents.push(cpu_agent);

    tracing::info!(
        pyroscope_url = %profiling.pyroscope_url,
        sample_rate = profiling.cpu_sample_rate,
        "Continuous CPU profiling started"
    );

    #[cfg(feature = "jemalloc-profiling")]
    if profiling.memory_profiling {
        let app_name = format!("{service_name}.memory");
        let memory_agent = PyroscopeAgentBuilder::new(
            profiling.pyroscope_url.as_str(),
            app_name.as_str(),
            profiling.cpu_sample_rate,
            SPY_NAME,
            env!("CARGO_PKG_VERSION"),
            pyroscope::backend::jemalloc::jemalloc_backend(),
        )
        .tags(vec![
            ("service.name", service_name),
            ("deployment.environment", "self-monitoring"),
        ])
        .build()
        .context("Failed to build Pyroscope jemalloc agent")?
        .start()
        .context("Failed to start Pyroscope jemalloc agent")?;
        agents.push(memory_agent);
        tracing::info!("Continuous heap profiling started (jemalloc)");
    }
    #[cfg(not(feature = "jemalloc-profiling"))]
    if profiling.memory_profiling {
        tracing::warn!(
            "memory_profiling is enabled but this binary was built without the \
             jemalloc-profiling feature; heap profiling is unavailable"
        );
    }

    Ok(Some(ProfilingHandle { agents }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_returns_none() {
        let config = Configuration::default();
        assert!(!config.profiling.enabled);
        let result = init_profiling(&config, "test-service").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn config_defaults() {
        let p = crate::config::ProfilingConfig::default();
        assert!(!p.enabled);
        assert_eq!(p.pyroscope_url, "http://localhost:4040");
        assert_eq!(p.cpu_sample_rate, 100);
        assert!(!p.memory_profiling);
    }
}
