//! # Pyroscope API Types
//!
//! Response types for the Pyroscope-compatible HTTP API, mirroring the
//! wire format Grafana's Pyroscope datasource consumes (camelCase JSON).
//!
//! The central type is the flamebearer: an interned name table plus
//! delta-encoded per-level block quadruples (`format: "single"`) or
//! septuples carrying baseline/comparison pairs (`format: "double"`).

use serde::{Deserialize, Serialize};

/// Flamegraph payload in flamebearer encoding.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Flamebearer {
    /// Function name table referenced by block name indices.
    pub names: Vec<String>,
    /// One flat array per depth level. `format: "single"` uses
    /// `[offset_delta, total, self, name_index]` quadruples; `"double"`
    /// uses `[off_l, total_l, self_l, off_r, total_r, self_r, name_index]`.
    pub levels: Vec<Vec<i64>>,
    /// Total number of ticks (root width).
    pub num_ticks: i64,
    /// Largest self value of any block, for color scaling.
    pub max_self: i64,
}

/// Metadata describing how to interpret flamebearer values.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FlamebearerMetadata {
    /// `"single"` for one profile set, `"double"` for a diff.
    pub format: String,
    /// Profiler that produced the data, when known (e.g. "gospy").
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spy_name: Option<String>,
    /// Sample rate in Hz; 100 is the Pyroscope default for CPU profiles.
    pub sample_rate: u32,
    /// Value units (e.g. "samples", "objects", "bytes").
    pub units: String,
    /// Display name of the rendered profile/query.
    pub name: String,
}

/// Optional per-interval sample counts for the render timeline.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Timeline {
    /// Start of the timeline, unix seconds.
    pub start_time: i64,
    /// One aggregated value per interval.
    pub samples: Vec<u64>,
    /// Interval width in seconds.
    pub duration_delta: i64,
}

/// Response body of `GET /pyroscope/render` (and the diff variant).
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RenderResponse {
    pub flamebearer: Flamebearer,
    pub metadata: FlamebearerMetadata,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeline: Option<Timeline>,
    /// Total baseline ticks; present when `format` is `"double"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub left_ticks: Option<i64>,
    /// Total comparison ticks; present when `format` is `"double"`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub right_ticks: Option<i64>,
}

/// One entry of `GET /pyroscope/profile-types`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProfileType {
    /// Canonical ID: `{name}:{sample_type}:{sample_unit}`.
    #[serde(rename = "ID")]
    pub id: String,
    pub name: String,
    pub sample_type: String,
    pub sample_unit: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub period_type: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub period_unit: String,
}

impl ProfileType {
    /// Build a profile type from a `{sample_type}:{sample_unit}` pair as
    /// returned by the querier's discovery queries.
    pub fn from_type_unit(sample_type: &str, sample_unit: &str) -> Self {
        Self {
            id: format!("{sample_type}:{sample_type}:{sample_unit}"),
            name: sample_type.to_string(),
            sample_type: sample_type.to_string(),
            sample_unit: sample_unit.to_string(),
            period_type: String::new(),
            period_unit: String::new(),
        }
    }
}

/// Response body of the label-names / label-values endpoints.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LabelsResponse {
    pub names: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_response_serializes_camel_case() {
        let response = RenderResponse {
            flamebearer: Flamebearer {
                names: vec!["total".to_string()],
                levels: vec![vec![0, 100, 0, 0]],
                num_ticks: 100,
                max_self: 40,
            },
            metadata: FlamebearerMetadata {
                format: "single".to_string(),
                spy_name: None,
                sample_rate: 100,
                units: "samples".to_string(),
                name: "cpu".to_string(),
            },
            timeline: None,
            left_ticks: None,
            right_ticks: None,
        };

        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["flamebearer"]["numTicks"], 100);
        assert_eq!(json["flamebearer"]["maxSelf"], 40);
        assert_eq!(json["metadata"]["sampleRate"], 100);
        // Optional fields are omitted, not null.
        assert!(json.get("timeline").is_none());
        assert!(json.get("leftTicks").is_none());
        assert!(json["metadata"].get("spyName").is_none());
    }

    #[test]
    fn diff_response_carries_side_ticks() {
        let response = RenderResponse {
            metadata: FlamebearerMetadata {
                format: "double".to_string(),
                ..FlamebearerMetadata::default()
            },
            left_ticks: Some(175),
            right_ticks: Some(250),
            ..RenderResponse::default()
        };
        let json = serde_json::to_value(&response).expect("serialize");
        assert_eq!(json["leftTicks"], 175);
        assert_eq!(json["rightTicks"], 250);
    }

    #[test]
    fn profile_type_builds_canonical_id() {
        let profile_type = ProfileType::from_type_unit("cpu", "nanoseconds");
        assert_eq!(profile_type.id, "cpu:cpu:nanoseconds");
        let json = serde_json::to_value(&profile_type).expect("serialize");
        assert_eq!(json["ID"], "cpu:cpu:nanoseconds");
        assert_eq!(json["sampleType"], "cpu");
    }

    #[test]
    fn render_response_roundtrips() {
        let response = RenderResponse {
            flamebearer: Flamebearer {
                names: vec!["total".to_string(), "main".to_string()],
                levels: vec![vec![0, 10, 0, 0], vec![0, 10, 10, 1]],
                num_ticks: 10,
                max_self: 10,
            },
            metadata: FlamebearerMetadata {
                format: "single".to_string(),
                sample_rate: 100,
                units: "samples".to_string(),
                name: "cpu".to_string(),
                spy_name: None,
            },
            timeline: Some(Timeline {
                start_time: 1_700_000_000,
                samples: vec![5, 5],
                duration_delta: 10,
            }),
            left_ticks: None,
            right_ticks: None,
        };
        let json = serde_json::to_string(&response).expect("serialize");
        let back: RenderResponse = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(response, back);
    }
}
