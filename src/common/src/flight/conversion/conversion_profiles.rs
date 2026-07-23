//! # OTLP Profiles Conversion
//!
//! Converts OTLP `v1development` profile exports into the internal profile
//! model and Arrow RecordBatches using the Flight profile schema.
//!
//! OTLP profiles arrive with a request-level [`ProfilesDictionary`] (shared
//! string/function/location/stack/link/attribute tables). Conversion resolves
//! those indices eagerly so every stored row is self-contained: stack traces
//! and samples are denormalized to JSON, and the first span link is lifted
//! into dedicated `trace_id`/`span_id` columns for correlation queries.

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, BinaryArray, Int64Array, StringArray, UInt64Array};
use datafusion::arrow::record_batch::RecordBatch;
use opentelemetry_proto::tonic::collector::profiles::v1development::ExportProfilesServiceRequest;
use opentelemetry_proto::tonic::profiles::v1development::{
    Profile as OtlpProfile, ProfilesDictionary, ValueType as OtlpValueType,
};
use serde_json::Map;

use crate::flight::conversion::conversion_common::{
    extract_resource_json, extract_service_name, extract_value,
};
use crate::flight::schema::FlightSchemas;
use crate::model::profile::{Frame, Profile, ProfileLink, Sample, Stacktrace, ValueType};

use super::extract_scope_json;

/// Look up a string table entry; out-of-range and index 0 resolve to "".
fn resolve_string(dictionary: &ProfilesDictionary, index: i32) -> String {
    usize::try_from(index)
        .ok()
        .and_then(|i| dictionary.string_table.get(i))
        .cloned()
        .unwrap_or_default()
}

fn resolve_value_type(dictionary: &ProfilesDictionary, value_type: &OtlpValueType) -> ValueType {
    ValueType {
        type_: resolve_string(dictionary, value_type.type_strindex),
        unit: resolve_string(dictionary, value_type.unit_strindex),
    }
}

/// Resolve a stack table entry into leaf-first frames, expanding inlined
/// functions (one frame per line entry).
fn resolve_stacktrace(dictionary: &ProfilesDictionary, stack_index: i32) -> Stacktrace {
    let Some(stack) = usize::try_from(stack_index)
        .ok()
        .and_then(|i| dictionary.stack_table.get(i))
    else {
        return Stacktrace::default();
    };

    let mut frames = Vec::new();
    for location_index in &stack.location_indices {
        let Some(location) = usize::try_from(*location_index)
            .ok()
            .and_then(|i| dictionary.location_table.get(i))
        else {
            continue;
        };

        let mapping_filename = usize::try_from(location.mapping_index)
            .ok()
            .filter(|i| *i > 0)
            .and_then(|i| dictionary.mapping_table.get(i))
            .map(|mapping| resolve_string(dictionary, mapping.filename_strindex))
            .unwrap_or_default();

        if location.lines.is_empty() {
            // No symbol information; keep the raw address frame.
            frames.push(Frame {
                address: location.address,
                mapping_filename,
                ..Frame::default()
            });
            continue;
        }

        for line in &location.lines {
            let function = usize::try_from(line.function_index)
                .ok()
                .and_then(|i| dictionary.function_table.get(i));
            let (function_name, system_name, filename) = function
                .map(|f| {
                    (
                        resolve_string(dictionary, f.name_strindex),
                        resolve_string(dictionary, f.system_name_strindex),
                        resolve_string(dictionary, f.filename_strindex),
                    )
                })
                .unwrap_or_default();

            frames.push(Frame {
                function_name,
                system_name,
                filename,
                line: line.line,
                column: line.column,
                address: location.address,
                mapping_filename: mapping_filename.clone(),
            });
        }
    }

    Stacktrace { frames }
}

/// Resolve attribute table references into a JSON object; returns `None`
/// when there are no attributes.
fn resolve_attributes(
    dictionary: &ProfilesDictionary,
    attribute_indices: &[i32],
) -> Option<serde_json::Value> {
    if attribute_indices.is_empty() {
        return None;
    }

    let mut map = Map::new();
    for index in attribute_indices {
        let Some(attribute) = usize::try_from(*index)
            .ok()
            .filter(|i| *i > 0)
            .and_then(|i| dictionary.attribute_table.get(i))
        else {
            continue;
        };
        let key = resolve_string(dictionary, attribute.key_strindex);
        if key.is_empty() {
            continue;
        }
        map.insert(key, extract_value(&attribute.value));
    }

    if map.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(map))
    }
}

fn resolve_profile(
    dictionary: &ProfilesDictionary,
    otlp_profile: &OtlpProfile,
    service_name: &str,
    resource_json: &str,
    scope_json: &str,
) -> Profile {
    // Deduplicate stacks and links per profile, preserving first-seen order.
    let mut stack_local_indices: HashMap<i32, usize> = HashMap::new();
    let mut stacktraces = Vec::new();
    let mut link_local_indices: HashMap<i32, usize> = HashMap::new();
    let mut links = Vec::new();

    let mut samples = Vec::with_capacity(otlp_profile.samples.len());
    for otlp_sample in &otlp_profile.samples {
        let stacktrace_index = *stack_local_indices
            .entry(otlp_sample.stack_index)
            .or_insert_with(|| {
                stacktraces.push(resolve_stacktrace(dictionary, otlp_sample.stack_index));
                stacktraces.len() - 1
            });

        // link_table[0] is the null link by convention.
        let link_index = if otlp_sample.link_index > 0 {
            usize::try_from(otlp_sample.link_index)
                .ok()
                .and_then(|i| dictionary.link_table.get(i))
                .map(|link| {
                    *link_local_indices
                        .entry(otlp_sample.link_index)
                        .or_insert_with(|| {
                            let mut trace_id = [0u8; 16];
                            if link.trace_id.len() == 16 {
                                trace_id.copy_from_slice(&link.trace_id);
                            }
                            let mut span_id = [0u8; 8];
                            if link.span_id.len() == 8 {
                                span_id.copy_from_slice(&link.span_id);
                            }
                            links.push(ProfileLink { trace_id, span_id });
                            links.len() - 1
                        })
                })
        } else {
            None
        };

        samples.push(Sample {
            stacktrace_index,
            values: otlp_sample.values.clone(),
            timestamps_unix_nano: otlp_sample.timestamps_unix_nano.clone(),
            link_index,
            attributes: resolve_attributes(dictionary, &otlp_sample.attribute_indices),
        });
    }

    let mut profile_id = [0u8; 16];
    if otlp_profile.profile_id.len() == 16 {
        profile_id.copy_from_slice(&otlp_profile.profile_id);
    }

    let resource_attributes = serde_json::from_str(resource_json).ok();
    let scope_attributes = serde_json::from_str(scope_json).ok();

    Profile {
        profile_id,
        time_unix_nano: otlp_profile.time_unix_nano,
        duration_nano: otlp_profile.duration_nano,
        sample_type: otlp_profile
            .sample_type
            .as_ref()
            .map(|vt| resolve_value_type(dictionary, vt))
            .unwrap_or_default(),
        period_type: otlp_profile
            .period_type
            .as_ref()
            .map(|vt| resolve_value_type(dictionary, vt)),
        period: otlp_profile.period,
        service_name: service_name.to_string(),
        stacktraces,
        samples,
        links,
        resource_attributes,
        scope_attributes,
        attributes: resolve_attributes(dictionary, &otlp_profile.attribute_indices),
        dropped_attributes_count: otlp_profile.dropped_attributes_count,
    }
}

/// Convert an OTLP profiles export into the internal profile model,
/// resolving all dictionary references.
pub fn otlp_profiles_to_model(request: &ExportProfilesServiceRequest) -> Vec<Profile> {
    let default_dictionary = ProfilesDictionary::default();
    let dictionary = request.dictionary.as_ref().unwrap_or(&default_dictionary);

    let mut profiles = Vec::new();
    for resource_profiles in &request.resource_profiles {
        let resource_json = extract_resource_json(&resource_profiles.resource);
        let service_name = extract_service_name(&resource_profiles.resource);

        for scope_profiles in &resource_profiles.scope_profiles {
            let scope_json = extract_scope_json(&scope_profiles.scope);

            for otlp_profile in &scope_profiles.profiles {
                profiles.push(resolve_profile(
                    dictionary,
                    otlp_profile,
                    &service_name,
                    &resource_json,
                    &scope_json,
                ));
            }
        }
    }
    profiles
}

/// Convert internal profiles to an Arrow RecordBatch using the Flight
/// profile schema.
pub fn profiles_to_arrow(profiles: &[Profile]) -> RecordBatch {
    let schemas = FlightSchemas::new();
    let schema = schemas.profile_schema.clone();

    let mut profile_ids: Vec<&[u8]> = Vec::with_capacity(profiles.len());
    let mut times = Vec::with_capacity(profiles.len());
    let mut durations = Vec::with_capacity(profiles.len());
    let mut sample_type_types = Vec::with_capacity(profiles.len());
    let mut sample_type_units = Vec::with_capacity(profiles.len());
    let mut periods = Vec::with_capacity(profiles.len());
    let mut period_type_types = Vec::with_capacity(profiles.len());
    let mut period_type_units = Vec::with_capacity(profiles.len());
    let mut service_names = Vec::with_capacity(profiles.len());
    let mut stacktraces_jsons = Vec::with_capacity(profiles.len());
    let mut samples_jsons = Vec::with_capacity(profiles.len());
    let mut resource_jsons = Vec::with_capacity(profiles.len());
    let mut scope_jsons = Vec::with_capacity(profiles.len());
    let mut attributes_jsons = Vec::with_capacity(profiles.len());
    let mut trace_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(profiles.len());
    let mut span_ids: Vec<Option<Vec<u8>>> = Vec::with_capacity(profiles.len());

    for profile in profiles {
        profile_ids.push(&profile.profile_id[..]);
        times.push(profile.time_unix_nano);
        durations.push(profile.duration_nano);
        sample_type_types.push(profile.sample_type.type_.clone());
        sample_type_units.push(profile.sample_type.unit.clone());
        periods.push((profile.period != 0).then_some(profile.period));
        period_type_types.push(profile.period_type.as_ref().map(|vt| vt.type_.clone()));
        period_type_units.push(profile.period_type.as_ref().map(|vt| vt.unit.clone()));
        service_names.push(profile.service_name.clone());
        stacktraces_jsons
            .push(serde_json::to_string(&profile.stacktraces).unwrap_or_else(|_| "[]".to_string()));
        samples_jsons
            .push(serde_json::to_string(&profile.samples).unwrap_or_else(|_| "[]".to_string()));
        resource_jsons.push(profile.resource_attributes.as_ref().map(|v| v.to_string()));
        scope_jsons.push(profile.scope_attributes.as_ref().map(|v| v.to_string()));
        attributes_jsons.push(profile.attributes.as_ref().map(|v| v.to_string()));
        let link = profile.primary_link();
        trace_ids.push(link.map(|l| l.trace_id.to_vec()));
        span_ids.push(link.map(|l| l.span_id.to_vec()));
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(BinaryArray::from(profile_ids)),
        Arc::new(UInt64Array::from(times)),
        Arc::new(UInt64Array::from(durations)),
        Arc::new(StringArray::from(sample_type_types)),
        Arc::new(StringArray::from(sample_type_units)),
        Arc::new(Int64Array::from(periods)),
        Arc::new(StringArray::from(period_type_types)),
        Arc::new(StringArray::from(period_type_units)),
        Arc::new(StringArray::from(service_names)),
        Arc::new(StringArray::from(stacktraces_jsons)),
        Arc::new(StringArray::from(samples_jsons)),
        Arc::new(StringArray::from(resource_jsons)),
        Arc::new(StringArray::from(scope_jsons)),
        Arc::new(StringArray::from(attributes_jsons)),
        Arc::new(BinaryArray::from(
            trace_ids
                .iter()
                .map(|t| t.as_deref())
                .collect::<Vec<Option<&[u8]>>>(),
        )),
        Arc::new(BinaryArray::from(
            span_ids
                .iter()
                .map(|s| s.as_deref())
                .collect::<Vec<Option<&[u8]>>>(),
        )),
    ];

    RecordBatch::try_new(Arc::new(schema), columns)
        .expect("profile columns must match the Flight profile schema")
}

/// Convert an OTLP profiles export straight to an Arrow RecordBatch.
pub fn otlp_profiles_to_arrow(request: &ExportProfilesServiceRequest) -> RecordBatch {
    profiles_to_arrow(&otlp_profiles_to_model(request))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Array;
    use opentelemetry_proto::tonic::common::v1::{
        AnyValue, InstrumentationScope, KeyValue, any_value,
    };
    use opentelemetry_proto::tonic::profiles::v1development::{
        Function, KeyValueAndUnit, Line, Link, Location, Mapping, ResourceProfiles,
        Sample as OtlpSample, ScopeProfiles, Stack, ValueType as OtlpValueType,
    };
    use opentelemetry_proto::tonic::resource::v1::Resource;

    fn string_value(s: &str) -> Option<AnyValue> {
        Some(AnyValue {
            value: Some(any_value::Value::StringValue(s.to_string())),
        })
    }

    /// Build a request with a fully populated dictionary: two functions
    /// (main -> work), one stack, two samples sharing it, one span link.
    fn sample_request() -> ExportProfilesServiceRequest {
        let dictionary = ProfilesDictionary {
            string_table: vec![
                String::new(),              // 0: required empty string
                "cpu".to_string(),          // 1
                "nanoseconds".to_string(),  // 2
                "work".to_string(),         // 3
                "main".to_string(),         // 4
                "app.rs".to_string(),       // 5
                "/usr/bin/app".to_string(), // 6
                "thread.name".to_string(),  // 7
            ],
            function_table: vec![
                Function::default(), // 0: null function
                Function {
                    name_strindex: 3,
                    system_name_strindex: 3,
                    filename_strindex: 5,
                    start_line: 10,
                },
                Function {
                    name_strindex: 4,
                    system_name_strindex: 4,
                    filename_strindex: 5,
                    start_line: 1,
                },
            ],
            mapping_table: vec![
                Mapping::default(), // 0: null mapping
                Mapping {
                    filename_strindex: 6,
                    ..Mapping::default()
                },
            ],
            location_table: vec![
                Location::default(), // 0: null location
                Location {
                    mapping_index: 1,
                    address: 0x1000,
                    lines: vec![Line {
                        function_index: 1,
                        line: 42,
                        column: 3,
                    }],
                    attribute_indices: vec![],
                },
                Location {
                    mapping_index: 1,
                    address: 0x2000,
                    lines: vec![Line {
                        function_index: 2,
                        line: 7,
                        column: 0,
                    }],
                    attribute_indices: vec![],
                },
            ],
            stack_table: vec![
                Stack::default(), // 0: null stack
                Stack {
                    // leaf first: work called by main
                    location_indices: vec![1, 2],
                },
            ],
            link_table: vec![
                Link::default(), // 0: null link
                Link {
                    trace_id: vec![7; 16],
                    span_id: vec![9; 8],
                },
            ],
            attribute_table: vec![
                KeyValueAndUnit::default(), // 0: null attribute
                KeyValueAndUnit {
                    key_strindex: 7,
                    value: string_value("worker-1"),
                    unit_strindex: 0,
                },
            ],
        };

        let profile = OtlpProfile {
            sample_type: Some(OtlpValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            samples: vec![
                OtlpSample {
                    stack_index: 1,
                    values: vec![100],
                    link_index: 1,
                    attribute_indices: vec![1],
                    ..OtlpSample::default()
                },
                OtlpSample {
                    stack_index: 1,
                    values: vec![250],
                    ..OtlpSample::default()
                },
            ],
            time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 10_000_000_000,
            period_type: Some(OtlpValueType {
                type_strindex: 1,
                unit_strindex: 2,
            }),
            period: 10_000_000,
            profile_id: vec![1; 16],
            ..OtlpProfile::default()
        };

        ExportProfilesServiceRequest {
            resource_profiles: vec![ResourceProfiles {
                resource: Some(Resource {
                    attributes: vec![KeyValue {
                        key: "service.name".to_string(),
                        value: string_value("checkout"),
                        ..KeyValue::default()
                    }],
                    ..Resource::default()
                }),
                scope_profiles: vec![ScopeProfiles {
                    scope: Some(InstrumentationScope {
                        name: "profiler".to_string(),
                        ..InstrumentationScope::default()
                    }),
                    profiles: vec![profile],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
            dictionary: Some(dictionary),
        }
    }

    #[test]
    fn resolves_dictionary_into_self_contained_model() {
        let profiles = otlp_profiles_to_model(&sample_request());
        assert_eq!(profiles.len(), 1);
        let profile = &profiles[0];

        assert_eq!(profile.profile_id, [1; 16]);
        assert_eq!(profile.service_name, "checkout");
        assert_eq!(profile.sample_type.type_, "cpu");
        assert_eq!(profile.sample_type.unit, "nanoseconds");
        assert_eq!(profile.period, 10_000_000);

        // Two samples share one deduplicated stacktrace.
        assert_eq!(profile.samples.len(), 2);
        assert_eq!(profile.stacktraces.len(), 1);
        assert_eq!(profile.samples[0].stacktrace_index, 0);
        assert_eq!(profile.samples[1].stacktrace_index, 0);

        // Leaf-first frames with resolved symbols.
        let frames = &profile.stacktraces[0].frames;
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].function_name, "work");
        assert_eq!(frames[0].line, 42);
        assert_eq!(frames[0].mapping_filename, "/usr/bin/app");
        assert_eq!(frames[1].function_name, "main");

        // Span link resolved; only the first sample carries it.
        assert_eq!(profile.links.len(), 1);
        assert_eq!(profile.links[0].trace_id, [7; 16]);
        assert_eq!(profile.samples[0].link_index, Some(0));
        assert_eq!(profile.samples[1].link_index, None);

        // Sample attributes resolved from the attribute table.
        let attrs = profile.samples[0].attributes.as_ref().expect("attributes");
        assert_eq!(attrs["thread.name"], "worker-1");
        assert!(profile.samples[1].attributes.is_none());
    }

    #[test]
    fn converts_to_arrow_with_correlation_columns() {
        let batch = otlp_profiles_to_arrow(&sample_request());
        assert_eq!(batch.num_rows(), 1);
        assert_eq!(batch.num_columns(), 16);

        let schema = batch.schema();
        let trace_id_index = schema.index_of("trace_id").expect("trace_id column");
        let trace_ids = batch
            .column(trace_id_index)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .expect("binary array");
        assert_eq!(trace_ids.value(0), &[7u8; 16]);

        let service_index = schema.index_of("service_name").expect("service column");
        let services = batch
            .column(service_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        assert_eq!(services.value(0), "checkout");

        // Stacktraces roundtrip through JSON.
        let stacktraces_index = schema.index_of("stacktraces_json").expect("column");
        let stacktraces_json = batch
            .column(stacktraces_index)
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("string array");
        let stacktraces: Vec<Stacktrace> =
            serde_json::from_str(stacktraces_json.value(0)).expect("valid stacktrace JSON");
        assert_eq!(stacktraces[0].frames[0].function_name, "work");
    }

    #[test]
    fn empty_request_produces_empty_batch() {
        let batch = otlp_profiles_to_arrow(&ExportProfilesServiceRequest::default());
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 16);
    }

    #[test]
    fn missing_dictionary_yields_empty_stacks_not_panics() {
        let mut request = sample_request();
        request.dictionary = None;
        let profiles = otlp_profiles_to_model(&request);
        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].samples.len(), 2);
        assert!(profiles[0].stacktraces[0].frames.is_empty());
        assert!(profiles[0].links.is_empty());
        assert_eq!(profiles[0].sample_type.type_, "");
    }
}
