//! # Eval sets
//!
//! Named, tenant- and dataset-scoped lists of test cases for offline
//! AI-agent evaluation (change: agent-offline-evals, design D7). A harness
//! pulls a set, runs its agent over each case's `input`, and reports
//! results back as `gen_ai.evaluation.result` records. Stored in the SQL
//! catalog (`eval_sets`, `eval_cases`); see [`store`] for persistence.

pub mod store;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub use store::{AppendCasesOutcome, MAX_CASES_PER_SET, StoreError, validate_name};

/// Where a case came from.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum EvalCaseSource {
    /// Captured from a production trace.
    Trace {
        /// W3C trace id: 32 hex characters, stored lower-case.
        trace_id: String,
    },
    /// Imported from an uploaded file.
    Upload,
    /// Written by hand.
    #[default]
    HandWritten,
}

/// One test case of an eval set.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalCase {
    /// Unique within the set; 1-128 characters.
    pub id: String,
    /// The input the agent under test receives.
    pub input: String,
    /// Expected tool trajectory, in call order. Empty when not checked.
    #[serde(default)]
    pub expected_tools: Vec<String>,
    /// Reference answer for evaluators that compare against one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
    #[serde(default)]
    pub source: EvalCaseSource,
}

/// Caller-supplied eval set: the body of a create/replace request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetSpec {
    /// Slug: lowercase letters, digits, `-`, `_` and `.`, starting with a
    /// letter or digit; 1-128 characters.
    pub name: String,
    /// The agent this set evaluates (`gen_ai.agent.name`).
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Cases, in order.
    #[serde(default)]
    pub cases: Vec<EvalCase>,
}

/// An eval set without its cases, as listed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetSummary {
    pub name: String,
    pub agent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub case_count: u64,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// How many of a set's cases came from each [`EvalCaseSource`] kind.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalCaseSourceCounts {
    pub trace: u64,
    pub upload: u64,
    pub hand_written: u64,
}

/// An eval set as the list returns it: the summary plus its cases' sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetListing {
    #[serde(flatten)]
    pub summary: EvalSetSummary,
    pub sources: EvalCaseSourceCounts,
}

/// A stored eval set with its cases in order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct EvalSetRecord {
    pub tenant_id: String,
    /// The dataset *name* the set belongs to.
    pub dataset: String,
    #[serde(flatten)]
    pub summary: EvalSetSummary,
    pub cases: Vec<EvalCase>,
}
