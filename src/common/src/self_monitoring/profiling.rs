//! Continuous profiling for SignalDB services.
//!
//! CPU profiles are captured with the Pyroscope agent's pprof-rs backend and
//! pushed to a Grafana Pyroscope server. Memory (heap) profiling is
//! available behind the `jemalloc-profiling` cargo feature: binaries built
//! with it install jemalloc as the global allocator and run Pyroscope's
//! jemalloc backend (requires `MALLOC_CONF=prof:true,prof_active:true` or the
//! `_RJEM_MALLOC_CONF` equivalent at runtime).
//!
//! Separately, `[self_monitoring] profiles_enabled` captures CPU profiles
//! and exports them as OTLP profiles into SignalDB itself (see
//! [`super::self_profiling`]); `init_profiling` orchestrates both. All are
//! opt-in at runtime and add nothing when disabled.
//!
//! The Pyroscope agent's backends bind to pthreads and pprof-rs and only
//! support Linux/macOS, so on Windows this module compiles to a no-op stub
//! (see [`init_profiling`]) and the `pyroscope` dependency is dropped
//! entirely — see `src/common/Cargo.toml`.

#[cfg(not(target_os = "windows"))]
pub use platform::{ProfilingHandle, init_profiling};

#[cfg(target_os = "windows")]
pub use stub::{ProfilingHandle, init_profiling};

#[cfg(not(target_os = "windows"))]
mod platform {
    use anyhow::{Context, Result};
    use pyroscope::PyroscopeAgent;
    use pyroscope::backend::{BackendConfig, PprofConfig, pprof_backend};
    use pyroscope::pyroscope::{PyroscopeAgentBuilder, PyroscopeAgentRunning};

    use crate::config::Configuration;
    use crate::self_monitoring::self_profiling::{self, SelfProfilingHandle};

    /// Spy name reported to the Pyroscope server.
    const SPY_NAME: &str = "pyroscope-rs";

    /// Handle to running profiling agents; stop via [`ProfilingHandle::shutdown`].
    pub struct ProfilingHandle {
        agents: Vec<PyroscopeAgent<PyroscopeAgentRunning>>,
        self_profiler: Option<SelfProfilingHandle>,
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
            if let Some(self_profiler) = self.self_profiler {
                self_profiler.shutdown();
            }
        }
    }

    /// Start continuous profiling when `[profiling] enabled = true` and/or CPU
    /// self-profiling when `[self_monitoring] profiles_enabled = true`.
    ///
    /// Returns `Ok(None)` when both are disabled. The two are mutually
    /// exclusive (both drive the same SIGPROF-based sampler): when both are
    /// enabled, the explicitly configured external `[profiling]` agent wins
    /// and self-profiling is skipped with a warning.
    ///
    /// For the external agent, CPU profiling starts unconditionally when
    /// enabled; memory profiling additionally requires `memory_profiling =
    /// true` **and** the `jemalloc-profiling` build feature (a warning is
    /// logged when configured without the feature).
    pub fn init_profiling(
        config: &Configuration,
        service_name: &str,
    ) -> Result<Option<ProfilingHandle>> {
        if !config.profiling.enabled {
            let self_profiler = self_profiling::init_self_profiling(config, service_name)?;
            return Ok(self_profiler.map(|handle| ProfilingHandle {
                agents: Vec::new(),
                self_profiler: Some(handle),
            }));
        }
        if config.self_monitoring.profiles_enabled {
            tracing::warn!(
                "[profiling] and [self_monitoring].profiles_enabled are both set; they share \
                 one CPU sampler, so self-profiling is skipped in favor of the external \
                 Pyroscope agent"
            );
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
            let memory_agent_result = PyroscopeAgentBuilder::new(
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
            .context("Failed to build Pyroscope jemalloc agent")
            .and_then(|agent| {
                agent
                    .start()
                    .context("Failed to start Pyroscope jemalloc agent")
            });
            match memory_agent_result {
                Ok(memory_agent) => {
                    agents.push(memory_agent);
                    tracing::info!("Continuous heap profiling started (jemalloc)");
                }
                Err(e) => {
                    // Cleanly stop the already-running CPU agent before bailing.
                    ProfilingHandle {
                        agents,
                        self_profiler: None,
                    }
                    .shutdown();
                    return Err(e);
                }
            }
        }
        #[cfg(not(feature = "jemalloc-profiling"))]
        if profiling.memory_profiling {
            tracing::warn!(
                "memory_profiling is enabled but this binary was built without the \
                 jemalloc-profiling feature; heap profiling is unavailable"
            );
        }

        Ok(Some(ProfilingHandle {
            agents,
            self_profiler: None,
        }))
    }
}

#[cfg(target_os = "windows")]
mod stub {
    use anyhow::Result;

    use crate::config::Configuration;

    /// No-op profiling handle on platforms without Pyroscope agent support.
    pub struct ProfilingHandle;

    impl ProfilingHandle {
        /// No-op: profiling is never started on this platform.
        pub fn shutdown(self) {}
    }

    /// Continuous profiling is unsupported on Windows (the Pyroscope agent's
    /// backends require pthreads/pprof-rs), so this always returns `Ok(None)`.
    /// A warning is logged if `[profiling] enabled = true` or
    /// `[self_monitoring] profiles_enabled = true`.
    pub fn init_profiling(
        config: &Configuration,
        _service_name: &str,
    ) -> Result<Option<ProfilingHandle>> {
        if config.profiling.enabled || config.self_monitoring.profiles_enabled {
            tracing::warn!(
                "Continuous profiling is not supported on Windows; the [profiling] \
                 section and [self_monitoring].profiles_enabled are ignored"
            );
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::init_profiling;
    use crate::config::Configuration;

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
