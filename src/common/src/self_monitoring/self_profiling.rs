//! # CPU and heap self-profiling as OTLP profiles
//!
//! Samples this process and exports each window as an OTLP
//! `profiles/v1development` request to SignalDB's own acceptor, under the
//! self-monitoring tenant/dataset — self-hosting profiles the same way
//! traces/logs/metrics already are.
//!
//! - **CPU** (`profiles_enabled`): pyroscope's pprof-rs backend (agent-free,
//!   no Pyroscope server), `cpu`/`nanoseconds`. Mutually exclusive with the
//!   external `[profiling]` agent (both use the SIGPROF sampler), so
//!   [`profiling::init_profiling`] starts at most one.
//! - **Heap** (`heap_profiles_enabled`, requires the `jemalloc-profiling`
//!   build feature): jemalloc live-heap dumps via `jemalloc_pprof`,
//!   symbolized in-process with `backtrace`, `inuse_space`/`bytes`. Uses no
//!   SIGPROF, so it runs alongside CPU or the external agent.
//!
//! Both feed one [`ProfileDict`] builder, which emits the OTLP dictionary
//! shape the ingest side (`otlp_profiles_to_model`) resolves.
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

/// Start self-profiling, exporting OTLP profiles into SignalDB itself.
///
/// `start_cpu` runs the CPU sampler (`[self_monitoring] profiles_enabled`,
/// unless the external `[profiling]` agent has claimed the SIGPROF sampler);
/// `heap_profiles_enabled` additionally exports a jemalloc heap profile each
/// window (requires the `jemalloc-profiling` build feature and jemalloc
/// profiling active at runtime). Returns `Ok(None)` when neither runs. Must
/// be called inside a Tokio runtime; needs no OTel SDK machinery, only the
/// endpoint and credentials from `[self_monitoring]`.
pub fn init_self_profiling(
    config: &Configuration,
    service_name: &str,
    start_cpu: bool,
) -> Result<Option<SelfProfilingHandle>> {
    let sm = &config.self_monitoring;

    let heap_requested = sm.heap_profiles_enabled;
    let heap_active = heap_requested && cfg!(feature = "jemalloc-profiling");
    if heap_requested && !heap_active {
        tracing::warn!(
            "[self_monitoring].heap_profiles_enabled is set but this binary was built \
             without the jemalloc-profiling feature; heap self-profiling is unavailable"
        );
    }
    if !start_cpu && !heap_active {
        return Ok(None);
    }

    // CPU sampler is optional: heap can run on its own.
    let cpu_sampler = if start_cpu {
        let backend = pprof_backend(
            PprofConfig {
                sample_rate: sm.profile_sample_rate_hz,
            },
            BackendConfig::default(),
        )
        .initialize()
        .map_err(|e| anyhow!("Failed to initialize CPU sampler: {e}"))?;
        // The loop only needs report()/shutdown(), reachable through the
        // backend's shared inner handle.
        Some(backend.backend.clone())
    } else {
        None
    };

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
        #[cfg(feature = "jemalloc-profiling")]
        if heap_active {
            // Turn on jemalloc's sampling profiler (a no-op if MALLOC_CONF
            // already set prof_active); PROF_CTL is None when the binary's
            // jemalloc lacks profiling support.
            jemalloc_pprof::activate_jemalloc_profiling().await;
        }

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

            if let Some(sampler) = &cpu_sampler {
                // Symbolication in report() is milliseconds-scale; keep it
                // off the async worker threads.
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
                            export_profile(&mut client, request, "cpu").await;
                        }
                    }
                    Ok(Err(e)) => tracing::warn!(error = %e, "CPU self-profile collection failed"),
                    Err(e) => tracing::warn!(error = %e, "CPU self-profile task failed"),
                }
            }

            #[cfg(feature = "jemalloc-profiling")]
            if heap_active {
                match collect_heap_profile(&window).await {
                    Ok(Some(request)) => export_profile(&mut client, request, "heap").await,
                    Ok(None) => {}
                    Err(e) => tracing::warn!(error = %e, "Heap self-profile collection failed"),
                }
            }

            if shutting_down {
                if let Some(sampler) = &cpu_sampler
                    && let Ok(mut guard) = sampler.lock()
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
        cpu = start_cpu,
        heap = heap_active,
        sample_rate_hz,
        interval = ?interval,
        "Self-profiling started (OTLP profiles to self-monitoring)"
    );
    Ok(Some(SelfProfilingHandle { shutdown_tx }))
}

/// Export one profile request, suppressing self-telemetry so the export's own
/// spans/logs are not re-ingested.
async fn export_profile(
    client: &mut ProfilesServiceClient<
        tonic::service::interceptor::InterceptedService<
            Channel,
            impl FnMut(tonic::Request<()>) -> Result<tonic::Request<()>, tonic::Status>,
        >,
    >,
    request: ExportProfilesServiceRequest,
    kind: &str,
) {
    let export = suppress_self_telemetry(async { client.export(request).await }).await;
    if let Err(status) = export {
        tracing::warn!(
            error = %status,
            kind,
            "Self-profile export failed; retrying next window"
        );
    }
}

/// Dump a jemalloc heap snapshot and convert it to an OTLP profile. Returns
/// `Ok(None)` when jemalloc profiling is unavailable (PROF_CTL absent) or the
/// snapshot has no samples.
#[cfg(feature = "jemalloc-profiling")]
async fn collect_heap_profile(
    window: &ProfileWindow,
) -> Result<Option<ExportProfilesServiceRequest>> {
    let Some(ctl) = jemalloc_pprof::PROF_CTL.as_ref() else {
        return Ok(None);
    };
    let profile = {
        let mut guard = ctl.lock().await;
        if !guard.activated() {
            return Ok(None);
        }
        // dump_profile writes a temp heap dump and parses it — fast enough to
        // hold the lock inline on this once-per-interval task.
        guard
            .dump_profile()
            .map_err(|e| anyhow!("jemalloc dump_profile failed: {e}"))?
    };
    if profile.stacks.is_empty() {
        return Ok(None);
    }
    Ok(Some(stackprofile_to_otlp(&profile, window)))
}

/// Convert a jemalloc `StackProfile` (raw addresses + weights) into an OTLP
/// `inuse_space`/bytes profile, symbolizing each address in-process. Frames
/// are leaf-first, matching the stack order the ingest side expects; each
/// sample's value is its live-bytes weight.
#[cfg(feature = "jemalloc-profiling")]
pub(crate) fn stackprofile_to_otlp(
    profile: &jemalloc_pprof::StackProfile,
    window: &ProfileWindow,
) -> ExportProfilesServiceRequest {
    use std::ffi::c_void;

    let mut dict = ProfileDict::new();
    for (stack, _annotation) in &profile.stacks {
        // Resolve each instruction address to symbol frames (backtrace
        // handles ASLR for the current process); expand inlined frames.
        let mut resolved: Vec<(String, String, i64)> = Vec::new();
        for &addr in &stack.addrs {
            let before = resolved.len();
            backtrace::resolve(addr as *mut c_void, |sym| {
                let name = sym.name().map(|n| n.to_string()).unwrap_or_default();
                let filename = sym
                    .filename()
                    .map(|p| p.to_string_lossy().into_owned())
                    .unwrap_or_default();
                let line = sym.lineno().map(i64::from).unwrap_or(0);
                resolved.push((name, filename, line));
            });
            if resolved.len() == before {
                // No debug info for this address; keep the raw address.
                resolved.push((format!("0x{addr:x}"), String::new(), 0));
            }
        }
        let frames: Vec<FrameInput<'_>> = resolved
            .iter()
            .map(|(name, filename, line)| FrameInput {
                name: if name.is_empty() { "unknown" } else { name },
                filename,
                line: *line,
            })
            .collect();
        dict.add_sample(&frames, stack.weight as i64, None);
    }
    dict.finish("inuse_space", "bytes", 0, window)
}

/// One resolved stack frame, leaf-first, feeding the dictionary builder.
pub(crate) struct FrameInput<'a> {
    pub name: &'a str,
    pub filename: &'a str,
    pub line: i64,
}

/// Builds the OTLP `ProfilesDictionary` + samples shared by the CPU and heap
/// producers, interning strings/functions/locations/stacks and emitting
/// exactly the shape the ingest side (`otlp_profiles_to_model`) resolves:
/// `string_table[0]` is the empty string, every other table carries a
/// zero-value entry at index 0 (the null-entry convention), and stacks
/// reference locations leaf-first.
struct ProfileDict {
    strings: Vec<String>,
    string_index: HashMap<String, i32>,
    functions: Vec<Function>,
    function_index: HashMap<(i32, i32), i32>,
    locations: Vec<Location>,
    location_index: HashMap<(i32, i64), i32>,
    stacks: Vec<Stack>,
    stack_index_map: HashMap<Vec<i32>, i32>,
    attributes: Vec<KeyValueAndUnit>,
    attribute_index: HashMap<String, i32>,
    samples: Vec<Sample>,
}

impl ProfileDict {
    fn new() -> Self {
        Self {
            strings: vec![String::new()],
            string_index: HashMap::from([(String::new(), 0)]),
            functions: vec![Function::default()],
            function_index: HashMap::new(),
            locations: vec![Location::default()],
            location_index: HashMap::new(),
            stacks: vec![Stack::default()],
            stack_index_map: HashMap::new(),
            attributes: vec![KeyValueAndUnit::default()],
            attribute_index: HashMap::new(),
            samples: Vec::new(),
        }
    }

    fn intern_string(&mut self, value: &str) -> i32 {
        if let Some(i) = self.string_index.get(value) {
            return *i;
        }
        let i = self.strings.len() as i32;
        self.strings.push(value.to_string());
        self.string_index.insert(value.to_string(), i);
        i
    }

    fn intern_stack(&mut self, frames: &[FrameInput<'_>]) -> i32 {
        let mut location_indices = Vec::with_capacity(frames.len());
        for frame in frames {
            let name_strindex = self.intern_string(frame.name);
            let filename_strindex = self.intern_string(frame.filename);
            let function_idx = match self.function_index.get(&(name_strindex, filename_strindex)) {
                Some(i) => *i,
                None => {
                    self.functions.push(Function {
                        name_strindex,
                        system_name_strindex: name_strindex,
                        filename_strindex,
                        start_line: 0,
                    });
                    let i = (self.functions.len() - 1) as i32;
                    self.function_index
                        .insert((name_strindex, filename_strindex), i);
                    i
                }
            };
            let loc_key = (function_idx, frame.line);
            let location_idx = match self.location_index.get(&loc_key) {
                Some(i) => *i,
                None => {
                    self.locations.push(Location {
                        mapping_index: 0,
                        address: 0,
                        lines: vec![Line {
                            function_index: function_idx,
                            line: frame.line,
                            column: 0,
                        }],
                        attribute_indices: Vec::new(),
                    });
                    let i = (self.locations.len() - 1) as i32;
                    self.location_index.insert(loc_key, i);
                    i
                }
            };
            location_indices.push(location_idx);
        }
        match self.stack_index_map.get(&location_indices) {
            Some(i) => *i,
            None => {
                self.stacks.push(Stack {
                    location_indices: location_indices.clone(),
                });
                let i = (self.stacks.len() - 1) as i32;
                self.stack_index_map.insert(location_indices, i);
                i
            }
        }
    }

    /// Intern a `thread.name` string attribute, returning its indices vector.
    fn thread_attribute(&mut self, thread_name: Option<&str>) -> Vec<i32> {
        match thread_name {
            Some(name) if !name.is_empty() => {
                if let Some(i) = self.attribute_index.get(name) {
                    return vec![*i];
                }
                let key_strindex = self.intern_string("thread.name");
                self.attributes.push(KeyValueAndUnit {
                    key_strindex,
                    value: Some(AnyValue {
                        value: Some(any_value::Value::StringValue(name.to_string())),
                    }),
                    unit_strindex: 0,
                });
                let i = (self.attributes.len() - 1) as i32;
                self.attribute_index.insert(name.to_string(), i);
                vec![i]
            }
            _ => Vec::new(),
        }
    }

    /// Add one sample: its leaf-first frames, a single value, and an optional
    /// thread name.
    fn add_sample(&mut self, frames: &[FrameInput<'_>], value: i64, thread_name: Option<&str>) {
        let stack_index = self.intern_stack(frames);
        let attribute_indices = self.thread_attribute(thread_name);
        self.samples.push(Sample {
            stack_index,
            attribute_indices,
            link_index: 0,
            values: vec![value],
            timestamps_unix_nano: Vec::new(),
        });
    }

    /// Finalize into an export request for one profile of the given type.
    fn finish(
        mut self,
        sample_type: &str,
        sample_unit: &str,
        period: i64,
        window: &ProfileWindow,
    ) -> ExportProfilesServiceRequest {
        // Sample map order is a HashMap's; sort for a deterministic payload.
        self.samples.sort_by_key(|s| s.stack_index);
        let type_strindex = self.intern_string(sample_type);
        let unit_strindex = self.intern_string(sample_unit);
        let value_type = ValueType {
            type_strindex,
            unit_strindex,
        };

        let profile = Profile {
            sample_type: Some(value_type),
            samples: self.samples,
            time_unix_nano: window.start_unix_nanos,
            duration_nano: window.duration_nanos,
            period_type: Some(value_type),
            period,
            profile_id: uuid::Uuid::new_v4().into_bytes().to_vec(),
            ..Default::default()
        };

        let dictionary = ProfilesDictionary {
            location_table: self.locations,
            function_table: self.functions,
            string_table: self.strings,
            attribute_table: self.attributes,
            stack_table: self.stacks,
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
}

/// Convert sampled CPU stack reports into an OTLP profiles export request.
///
/// Each sample's value is `count × period` nanoseconds of CPU time
/// (`cpu`/`nanoseconds`); frames are leaf-first, matching pyroscope's order.
pub(crate) fn reports_to_otlp(
    reports: &[Report],
    window: &ProfileWindow,
) -> ExportProfilesServiceRequest {
    let period = 1_000_000_000_i64 / i64::from(window.sample_rate_hz.max(1));
    let mut dict = ProfileDict::new();
    for report in reports {
        for (stack_trace, count) in report.iter() {
            let frames: Vec<FrameInput<'_>> = stack_trace
                .frames
                .iter()
                .map(|frame| FrameInput {
                    name: frame.name.as_deref().unwrap_or("unknown"),
                    filename: frame
                        .filename
                        .as_deref()
                        .or(frame.relative_path.as_deref())
                        .unwrap_or(""),
                    line: i64::from(frame.line.unwrap_or(0)),
                })
                .collect();
            dict.add_sample(
                &frames,
                *count as i64 * period,
                stack_trace.thread_name.as_deref(),
            );
        }
    }
    dict.finish("cpu", "nanoseconds", period, window)
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
        assert!(!config.self_monitoring.heap_profiles_enabled);
        // Neither CPU nor heap requested → nothing starts.
        let result = init_self_profiling(&config, "test-service", false).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn heap_conversion_produces_inuse_space_profile() {
        // A synthetic StackProfile-like input exercised through the shared
        // builder: two samples, weights become the single sample value, and
        // the sample type is inuse_space/bytes.
        let window = test_window();
        let mut dict = ProfileDict::new();
        dict.add_sample(
            &[
                FrameInput {
                    name: "alloc_hot",
                    filename: "a.rs",
                    line: 3,
                },
                FrameInput {
                    name: "main",
                    filename: "main.rs",
                    line: 1,
                },
            ],
            4096,
            None,
        );
        dict.add_sample(
            &[
                FrameInput {
                    name: "alloc_cold",
                    filename: "b.rs",
                    line: 9,
                },
                FrameInput {
                    name: "main",
                    filename: "main.rs",
                    line: 1,
                },
            ],
            1024,
            None,
        );
        let request = dict.finish("inuse_space", "bytes", 0, &window);

        let profiles = otlp_profiles_to_model(&request);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].sample_type.type_, "inuse_space");
        assert_eq!(profiles[0].sample_type.unit, "bytes");
        // Shared `main` frame interned once across both stacks.
        let dict = request.dictionary.as_ref().unwrap();
        assert_eq!(dict.function_table.len(), 1 + 3);
        let flamegraph = aggregate_profiles_to_flamegraph(&profiles);
        assert_eq!(flamegraph.total, 4096 + 1024);
    }
}
