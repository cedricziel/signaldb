//! # Profile Data Model
//!
//! Internal representation of an OpenTelemetry profile after the OTLP
//! `v1development` dictionary encoding has been resolved. OTLP profiles
//! arrive with request-level lookup tables (strings, functions, locations,
//! stacks, links); this model is self-contained so a profile can be stored,
//! queried, and aggregated without carrying the dictionary along.
//!
//! Stack traces are kept leaf-first, matching the OTLP convention.

use serde::{Deserialize, Serialize};

/// The type and unit of sampled values, with dictionary indices resolved to
/// strings (e.g. `cpu` / `nanoseconds`, `alloc_space` / `bytes`).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValueType {
    #[serde(rename = "type")]
    pub type_: String,
    pub unit: String,
}

/// A single resolved stack frame.
///
/// One OTLP `Location` can expand to multiple frames when functions are
/// inlined; each inlined line becomes its own frame.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Frame {
    /// Human-readable function name; empty if unknown.
    pub function_name: String,
    /// System-level (e.g. mangled) function name; empty if unknown.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub system_name: String,
    /// Source file containing the function; empty if unknown.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub filename: String,
    /// Line number in the source file; 0 means unset.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub line: i64,
    /// Column number in the source file; 0 means unset.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub column: i64,
    /// Instruction address, when captured by a native profiler; 0 means unset.
    #[serde(default, skip_serializing_if = "is_zero_u64")]
    pub address: u64,
    /// Binary/library the address maps into (e.g. shared object path).
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub mapping_filename: String,
}

fn is_zero_i64(v: &i64) -> bool {
    *v == 0
}

fn is_zero_u64(v: &u64) -> bool {
    *v == 0
}

/// A resolved stack trace, leaf frame first.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stacktrace {
    pub frames: Vec<Frame>,
}

/// A pointer from a profile sample to a trace span.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProfileLink {
    pub trace_id: [u8; 16],
    pub span_id: [u8; 8],
}

/// A sampled observation: one stack trace with its recorded values and/or
/// timestamps.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Sample {
    /// Index into [`Profile::stacktraces`].
    pub stacktrace_index: usize,
    /// Sampled values; type and unit are given by [`Profile::sample_type`].
    pub values: Vec<i64>,
    /// Per-observation timestamps (UNIX epoch nanoseconds), if recorded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub timestamps_unix_nano: Vec<u64>,
    /// Index into [`Profile::links`] when this sample is tied to a span.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub link_index: Option<usize>,
    /// Sample attributes resolved from the dictionary, as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
}

/// A fully resolved profile, the unit of storage and querying.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Profile {
    /// Globally unique 16-byte identifier; all zeroes means unassigned.
    pub profile_id: [u8; 16],
    /// Collection time, UNIX epoch nanoseconds.
    pub time_unix_nano: u64,
    /// Duration of the profile in nanoseconds; 0 for instant profiles.
    pub duration_nano: u64,
    /// Type and unit of every `Sample::values` entry.
    pub sample_type: ValueType,
    /// The kind of events between sampled occurrences, if periodic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period_type: Option<ValueType>,
    /// The number of events between sampled occurrences; 0 means unset.
    #[serde(default, skip_serializing_if = "is_zero_i64")]
    pub period: i64,
    /// Service the profile was collected from (`service.name` resource
    /// attribute); empty if the resource does not carry one.
    pub service_name: String,
    /// Deduplicated stack traces referenced by [`Sample::stacktrace_index`].
    pub stacktraces: Vec<Stacktrace>,
    /// The sampled observations.
    pub samples: Vec<Sample>,
    /// Deduplicated span links referenced by [`Sample::link_index`].
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<ProfileLink>,
    /// Resource attributes as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource_attributes: Option<serde_json::Value>,
    /// Instrumentation scope attributes as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope_attributes: Option<serde_json::Value>,
    /// Profile-level attributes as a JSON object.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attributes: Option<serde_json::Value>,
    /// Number of attributes dropped at collection time.
    #[serde(default, skip_serializing_if = "is_zero_u32")]
    pub dropped_attributes_count: u32,
}

fn is_zero_u32(v: &u32) -> bool {
    *v == 0
}

impl Profile {
    /// The first trace/span link, used for primary trace correlation.
    pub fn primary_link(&self) -> Option<&ProfileLink> {
        self.links.first()
    }

    /// Sum of the first value of every sample — the profile's total, e.g.
    /// total sampled CPU nanoseconds. Timestamp-only samples count 1 per
    /// observation, per the OTLP sample-shape rules.
    pub fn total_value(&self) -> i64 {
        self.samples
            .iter()
            .map(|s| {
                s.values
                    .first()
                    .copied()
                    .unwrap_or(s.timestamps_unix_nano.len() as i64)
            })
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_profile() -> Profile {
        Profile {
            profile_id: [1; 16],
            time_unix_nano: 1_700_000_000_000_000_000,
            duration_nano: 10_000_000_000,
            sample_type: ValueType {
                type_: "cpu".to_string(),
                unit: "nanoseconds".to_string(),
            },
            period_type: Some(ValueType {
                type_: "cpu".to_string(),
                unit: "nanoseconds".to_string(),
            }),
            period: 10_000_000,
            service_name: "checkout".to_string(),
            stacktraces: vec![Stacktrace {
                frames: vec![
                    Frame {
                        function_name: "leaf".to_string(),
                        filename: "leaf.rs".to_string(),
                        line: 42,
                        ..Frame::default()
                    },
                    Frame {
                        function_name: "main".to_string(),
                        ..Frame::default()
                    },
                ],
            }],
            samples: vec![Sample {
                stacktrace_index: 0,
                values: vec![100, 200],
                link_index: Some(0),
                ..Sample::default()
            }],
            links: vec![ProfileLink {
                trace_id: [2; 16],
                span_id: [3; 8],
            }],
            ..Profile::default()
        }
    }

    #[test]
    fn profile_serde_roundtrip_preserves_all_fields() {
        let profile = sample_profile();
        let json = serde_json::to_string(&profile).expect("serialize");
        let back: Profile = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(profile, back);
    }

    #[test]
    fn default_profile_is_empty_and_serializes_compactly() {
        let profile = Profile::default();
        assert_eq!(profile.profile_id, [0; 16]);
        assert!(profile.samples.is_empty());
        let json = serde_json::to_value(&profile).expect("serialize");
        // Optional/empty fields are omitted from the JSON representation.
        assert!(json.get("links").is_none());
        assert!(json.get("period").is_none());
        assert!(json.get("attributes").is_none());
    }

    #[test]
    fn primary_link_returns_first_link() {
        let profile = sample_profile();
        let link = profile.primary_link().expect("link");
        assert_eq!(link.trace_id, [2; 16]);
        assert_eq!(link.span_id, [3; 8]);
        assert!(Profile::default().primary_link().is_none());
    }

    #[test]
    fn total_value_sums_first_values_and_counts_timestamp_only_samples() {
        let mut profile = sample_profile();
        assert_eq!(profile.total_value(), 100);

        // A timestamp-only sample counts each observation as value 1 per shape rules.
        profile.samples.push(Sample {
            stacktrace_index: 0,
            values: vec![],
            timestamps_unix_nano: vec![1, 2, 3],
            ..Sample::default()
        });
        assert_eq!(profile.total_value(), 103);
    }
}
