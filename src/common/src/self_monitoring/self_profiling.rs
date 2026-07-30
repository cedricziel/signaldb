//! # CPU self-profiling as OTLP profiles
//!
//! Samples this process's CPU with pyroscope's pprof-rs backend (agent-free
//! API — no Pyroscope server involved) and exports each window as an OTLP
//! `profiles/v1development` request to SignalDB's own acceptor, under the
//! self-monitoring tenant/dataset. This makes SignalDB's fourth signal
//! self-hosting the same way traces/logs/metrics already are.
//!
//! Mutually exclusive with the external `[profiling]` Pyroscope agent: both
//! drive the same SIGPROF-based global sampler, so [`profiling::init_profiling`]
//! starts at most one of them.
//!
//! [`profiling::init_profiling`]: super::profiling::init_profiling

use std::collections::HashMap;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::collector::profiles::v1development::profiles_service_client::ProfilesServiceClient;
use opentelemetry_proto::tonic::common::v1::{AnyValue, InstrumentationScope, KeyValue, any_value};
use opentelemetry_proto::tonic::profiles::v1development::{
    Function, KeyValueAndUnit, Line, Location, Profile, ProfilesDictionary, ResourceProfiles,
    Sample, ScopeProfiles, Stack, ValueType,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use pyroscope::backend::{BackendConfig, PprofConfig, Report, ReportData, pprof_backend};
use tokio::sync::oneshot;
use tonic::transport::Channel;

use super::build_metadata;
use super::suppress::suppress_self_telemetry;
use crate::config::Configuration;

/// Handle to the running self-profiler; stop via
/// [`SelfProfilingHandle::shutdown`].
pub struct SelfProfilingHandle {
    shutdown_tx: oneshot::Sender<()>,
}

impl SelfProfilingHandle {
    /// Signal the export loop to flush a final (partial) window and stop the
    /// sampler. Best-effort: the flush races process exit, which is
    /// acceptable for a sampling profiler.
    pub fn shutdown(self) {
        let _ = self.shutdown_tx.send(());
    }
}

/// Metadata describing one profiling window, carried alongside the sampled
/// stacks into [`reports_to_otlp`].
pub(crate) struct ProfileWindow {
    pub service_name: String,
    pub sample_rate_hz: u32,
    pub start_unix_nanos: u64,
    pub duration_nanos: u64,
}

/// Start CPU self-profiling when `[self_monitoring] profiles_enabled = true`.
///
/// Returns `Ok(None)` when disabled. Must be called inside a Tokio runtime.
/// Deliberately gated on `profiles_enabled` alone (not
/// `self_monitoring.enabled`): the profiler needs none of the OTel SDK
/// machinery, only the endpoint and credentials from the same section.
pub fn init_self_profiling(
    config: &Configuration,
    service_name: &str,
) -> Result<Option<SelfProfilingHandle>> {
    let sm = &config.self_monitoring;
    if !sm.profiles_enabled {
        return Ok(None);
    }

    let backend = pprof_backend(
        PprofConfig {
            sample_rate: sm.profile_sample_rate_hz,
        },
        BackendConfig::default(),
    )
    .initialize()
    .map_err(|e| anyhow!("Failed to initialize CPU sampler: {e}"))?;
    // The loop only needs report()/shutdown(), both reachable through the
    // backend's shared inner handle, which moves into the task below.
    let sampler = backend.backend.clone();

    let channel = Channel::from_shared(sm.endpoint.clone())
        .context("Invalid self-monitoring endpoint")?
        .connect_lazy();
    let metadata = build_metadata(config);
    let mut client =
        ProfilesServiceClient::with_interceptor(channel, move |mut req: tonic::Request<()>| {
            for kv in metadata.iter() {
                if let tonic::metadata::KeyAndValueRef::Ascii(key, value) = kv {
                    req.metadata_mut().insert(key.clone(), value.clone());
                }
            }
            Ok(req)
        });

    let service_name = service_name.to_string();
    let sample_rate_hz = sm.profile_sample_rate_hz;
    let interval = sm.profile_interval;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        // interval() fires immediately once; consume that so the first
        // window actually spans `interval`.
        ticker.tick().await;

        let mut window_started_sys = SystemTime::now();
        let mut window_started = Instant::now();

        loop {
            let shutting_down = tokio::select! {
                _ = ticker.tick() => false,
                _ = &mut shutdown_rx => true,
            };

            let window = ProfileWindow {
                service_name: service_name.clone(),
                sample_rate_hz,
                start_unix_nanos: window_started_sys
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_nanos() as u64)
                    .unwrap_or_default(),
                duration_nanos: window_started.elapsed().as_nanos() as u64,
            };
            window_started_sys = SystemTime::now();
            window_started = Instant::now();

            // Symbolication in report() is milliseconds-scale; keep it off
            // the async worker threads.
            let collector = sampler.clone();
            let batch = tokio::task::spawn_blocking(move || {
                let mut guard = collector
                    .lock()
                    .map_err(|e| anyhow!("Sampler lock poisoned: {e}"))?;
                guard
                    .as_mut()
                    .ok_or_else(|| anyhow!("Sampler already shut down"))?
                    .report()
                    .map_err(|e| anyhow!("Failed to collect CPU profile: {e}"))
            })
            .await;

            match batch {
                Ok(Ok(batch)) => {
                    let reports = match batch.data {
                        ReportData::Reports(reports) => reports,
                        // The pprof backend always yields structured
                        // reports; raw pprof comes only from jemalloc.
                        ReportData::RawPprof(_) => Vec::new(),
                    };
                    if reports.iter().any(|r| !r.data.is_empty()) {
                        let request = reports_to_otlp(&reports, &window);
                        let export =
                            suppress_self_telemetry(async { client.export(request).await }).await;
                        if let Err(status) = export {
                            tracing::warn!(
                                error = %status,
                                "Self-profile export failed; retrying next window"
                            );
                        }
                    }
                }
                Ok(Err(e)) => tracing::warn!(error = %e, "Self-profile collection failed"),
                Err(e) => tracing::warn!(error = %e, "Self-profile collection task failed"),
            }

            if shutting_down {
                if let Ok(mut guard) = sampler.lock()
                    && let Some(backend) = guard.take()
                    && let Err(e) = { backend }.shutdown()
                {
                    tracing::warn!(error = %e, "Failed to shut down CPU sampler");
                }
                break;
            }
        }
    });

    tracing::info!(
        sample_rate_hz,
        interval = ?interval,
        "CPU self-profiling started (OTLP profiles to self-monitoring)"
    );
    Ok(Some(SelfProfilingHandle { shutdown_tx }))
}

/// Intern `value` into the dictionary string table, returning its index.
fn intern_string(strings: &mut Vec<String>, index: &mut HashMap<String, i32>, value: &str) -> i32 {
    if let Some(i) = index.get(value) {
        return *i;
    }
    let i = strings.len() as i32;
    strings.push(value.to_string());
    index.insert(value.to_string(), i);
    i
}

/// Convert sampled stack reports into an OTLP profiles export request.
///
/// Emits exactly the dictionary shape the ingest side
/// (`otlp_profiles_to_model`) resolves: `string_table[0]` is the empty
/// string, every other table carries a zero-value entry at index 0 (the
/// null-entry convention), stacks reference locations leaf-first (matching
/// pyroscope's frame order), and each sample's single value is
/// `count × period` nanoseconds of CPU time.
pub(crate) fn reports_to_otlp(
    reports: &[Report],
    window: &ProfileWindow,
) -> ExportProfilesServiceRequest {
    let mut strings: Vec<String> = vec![String::new()];
    let mut string_index: HashMap<String, i32> = HashMap::from([(String::new(), 0)]);
    let mut functions: Vec<Function> = vec![Function::default()];
    let mut function_index: HashMap<(i32, i32), i32> = HashMap::new();
    let mut locations: Vec<Location> = vec![Location::default()];
    let mut location_index: HashMap<(i32, i64), i32> = HashMap::new();
    let mut stacks: Vec<Stack> = vec![Stack::default()];
    let mut stack_index_map: HashMap<Vec<i32>, i32> = HashMap::new();
    let mut attributes: Vec<KeyValueAndUnit> = vec![KeyValueAndUnit::default()];
    let mut attribute_index: HashMap<String, i32> = HashMap::new();

    let period = 1_000_000_000_i64 / i64::from(window.sample_rate_hz.max(1));

    let mut samples = Vec::new();
    for report in reports {
        for (stack_trace, count) in report.iter() {
            let mut location_indices = Vec::with_capacity(stack_trace.frames.len());
            for frame in &stack_trace.frames {
                let name = frame.name.as_deref().unwrap_or("unknown");
                let filename = frame
                    .filename
                    .as_deref()
                    .or(frame.relative_path.as_deref())
                    .unwrap_or("");
                let line = i64::from(frame.line.unwrap_or(0));

                let name_strindex = intern_string(&mut strings, &mut string_index, name);
                let filename_strindex = intern_string(&mut strings, &mut string_index, filename);
                let func_key = (name_strindex, filename_strindex);
                let function_idx = *function_index.entry(func_key).or_insert_with(|| {
                    functions.push(Function {
                        name_strindex,
                        system_name_strindex: name_strindex,
                        filename_strindex,
                        start_line: 0,
                    });
                    (functions.len() - 1) as i32
                });

                let loc_key = (function_idx, line);
                let location_idx = *location_index.entry(loc_key).or_insert_with(|| {
                    locations.push(Location {
                        mapping_index: 0,
                        address: 0,
                        lines: vec![Line {
                            function_index: function_idx,
                            line,
                            column: 0,
                        }],
                        attribute_indices: Vec::new(),
                    });
                    (locations.len() - 1) as i32
                });
                location_indices.push(location_idx);
            }

            let stack_idx = *stack_index_map
                .entry(location_indices.clone())
                .or_insert_with(|| {
                    stacks.push(Stack { location_indices });
                    (stacks.len() - 1) as i32
                });

            let attribute_indices = match &stack_trace.thread_name {
                Some(thread_name) if !thread_name.is_empty() => {
                    let attr_idx =
                        *attribute_index
                            .entry(thread_name.clone())
                            .or_insert_with(|| {
                                let key_strindex =
                                    intern_string(&mut strings, &mut string_index, "thread.name");
                                attributes.push(KeyValueAndUnit {
                                    key_strindex,
                                    value: Some(AnyValue {
                                        value: Some(any_value::Value::StringValue(
                                            thread_name.clone(),
                                        )),
                                    }),
                                    unit_strindex: 0,
                                });
                                (attributes.len() - 1) as i32
                            });
                    vec![attr_idx]
                }
                _ => Vec::new(),
            };

            samples.push(Sample {
                stack_index: stack_idx,
                attribute_indices,
                link_index: 0,
                values: vec![*count as i64 * period],
                timestamps_unix_nano: Vec::new(),
            });
        }
    }
    // Report iteration order is a HashMap's; sort for a deterministic wire
    // payload (helps tests and dedup-friendly storage).
    samples.sort_by_key(|s| s.stack_index);

    let cpu_strindex = intern_string(&mut strings, &mut string_index, "cpu");
    let nanos_strindex = intern_string(&mut strings, &mut string_index, "nanoseconds");
    let value_type = ValueType {
        type_strindex: cpu_strindex,
        unit_strindex: nanos_strindex,
    };

    let profile = Profile {
        sample_type: Some(value_type),
        samples,
        time_unix_nano: window.start_unix_nanos,
        duration_nano: window.duration_nanos,
        period_type: Some(value_type),
        period,
        profile_id: uuid::Uuid::new_v4().into_bytes().to_vec(),
        ..Default::default()
    };

    let dictionary = ProfilesDictionary {
        location_table: locations,
        function_table: functions,
        string_table: strings,
        attribute_table: attributes,
        stack_table: stacks,
        ..Default::default()
    };

    ExportProfilesServiceRequest {
        resource_profiles: vec![ResourceProfiles {
            resource: Some(Resource {
                attributes: vec![
                    string_attr("service.name", &window.service_name),
                    string_attr("service.version", env!("CARGO_PKG_VERSION")),
                    string_attr("deployment.environment", "self-monitoring"),
                ],
                ..Default::default()
            }),
            scope_profiles: vec![ScopeProfiles {
                scope: Some(InstrumentationScope {
                    name: "signaldb-self-profiler".to_string(),
                    version: env!("CARGO_PKG_VERSION").to_string(),
                    ..Default::default()
                }),
                profiles: vec![profile],
                schema_url: String::new(),
            }],
            schema_url: String::new(),
        }],
        dictionary: Some(dictionary),
    }
}

fn string_attr(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(AnyValue {
            value: Some(any_value::Value::StringValue(value.to_string())),
        }),
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use pyroscope::backend::{BackendConfig, Report, StackFrame, StackTrace};

    use super::*;
    use crate::flight::conversion::conversion_profiles::{
        otlp_profiles_to_arrow, otlp_profiles_to_model,
    };
    use crate::profile::aggregate_profiles_to_flamegraph;

    fn frame(name: &str, file: &str, line: u32) -> StackFrame {
        StackFrame::new(
            None,
            Some(name.to_string()),
            Some(file.to_string()),
            None,
            None,
            Some(line),
        )
    }

    fn test_report() -> Report {
        let config = BackendConfig::default();
        // Leaf-first frames, matching the sampler's order.
        let hot = StackTrace::new(
            &config,
            None,
            None,
            Some("tokio-worker".to_string()),
            vec![
                frame("inner_hot", "hot.rs", 10),
                frame("main", "main.rs", 1),
            ],
        );
        let cold = StackTrace::new(
            &config,
            None,
            None,
            None,
            vec![
                frame("inner_cold", "cold.rs", 20),
                frame("main", "main.rs", 1),
            ],
        );
        Report::new(HashMap::from([(hot, 30), (cold, 10)]))
    }

    fn test_window() -> ProfileWindow {
        ProfileWindow {
            service_name: "signaldb-test".to_string(),
            sample_rate_hz: 100,
            start_unix_nanos: 1_700_000_000_000_000_000,
            duration_nanos: 60_000_000_000,
        }
    }

    #[test]
    fn dictionary_follows_null_entry_conventions() {
        let request = reports_to_otlp(&[test_report()], &test_window());
        let dict = request.dictionary.as_ref().unwrap();

        assert_eq!(dict.string_table[0], "");
        assert_eq!(dict.function_table[0], Function::default());
        assert_eq!(dict.location_table[0], Location::default());
        assert_eq!(dict.stack_table[0], Stack::default());
        assert_eq!(dict.attribute_table[0], KeyValueAndUnit::default());
        // Shared `main` frame interned once across both stacks.
        assert_eq!(dict.function_table.len(), 1 + 3);
        assert_eq!(dict.stack_table.len(), 1 + 2);
    }

    #[test]
    fn sample_values_are_count_times_period() {
        let request = reports_to_otlp(&[test_report()], &test_window());
        let profile = &request.resource_profiles[0].scope_profiles[0].profiles[0];

        assert_eq!(profile.period, 10_000_000); // 1e9 / 100 Hz
        let mut values: Vec<i64> = profile.samples.iter().map(|s| s.values[0]).collect();
        values.sort_unstable();
        assert_eq!(values, vec![100_000_000, 300_000_000]);
        assert_eq!(profile.profile_id.len(), 16);
    }

    #[test]
    fn round_trips_through_ingest_model_and_flamegraph() {
        let request = reports_to_otlp(&[test_report()], &test_window());

        let profiles = otlp_profiles_to_model(&request);
        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];
        assert_eq!(profile.service_name, "signaldb-test");
        assert_eq!(profile.sample_type.type_, "cpu");
        assert_eq!(profile.sample_type.unit, "nanoseconds");

        // Frames resolve leaf-first with symbol names intact.
        let leaf_names: Vec<&str> = profile
            .stacktraces
            .iter()
            .map(|s| s.frames[0].function_name.as_str())
            .collect();
        assert!(leaf_names.contains(&"inner_hot"));
        assert!(leaf_names.contains(&"inner_cold"));
        assert!(
            profile
                .stacktraces
                .iter()
                .all(|s| s.frames.last().unwrap().function_name == "main")
        );

        let flamegraph = aggregate_profiles_to_flamegraph(&profiles);
        assert_eq!(flamegraph.total, 400_000_000); // (30 + 10) samples × 10ms

        let batch = otlp_profiles_to_arrow(&request);
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);
    }

    #[test]
    fn missing_symbols_become_unknown() {
        let config = BackendConfig::default();
        let anonymous = StackTrace::new(
            &config,
            None,
            None,
            None,
            vec![StackFrame::new(None, None, None, None, None, None)],
        );
        let report = Report::new(HashMap::from([(anonymous, 1)]));

        let request = reports_to_otlp(&[report], &test_window());
        let profiles = otlp_profiles_to_model(&request);
        assert_eq!(
            profiles[0].stacktraces[0].frames[0].function_name,
            "unknown"
        );
    }

    #[test]
    fn thread_names_land_in_sample_attributes() {
        let request = reports_to_otlp(&[test_report()], &test_window());
        let profiles = otlp_profiles_to_model(&request);
        let with_thread: Vec<_> = profiles[0]
            .samples
            .iter()
            .filter_map(|s| s.attributes.as_ref())
            .collect();
        assert_eq!(with_thread.len(), 1);
        assert_eq!(with_thread[0]["thread.name"], "tokio-worker");
    }

    #[tokio::test]
    async fn disabled_returns_none() {
        let config = Configuration::default();
        assert!(!config.self_monitoring.profiles_enabled);
        let result = init_self_profiling(&config, "test-service").unwrap();
        assert!(result.is_none());
    }
}
