//! # Tenant OTTL processors
//!
//! Catalog-backed configuration for the per-tenant/per-dataset OTTL
//! transform rules applied at ingest (change: tenant-ottl-processors, design
//! D3). The uploaded statement list is stored verbatim; compilation into a
//! runnable `ottl::CompiledProgram` happens elsewhere (`ProcessorRegistry`).

pub mod store;

use serde::{Deserialize, Serialize};

pub use store::StoreError;

/// A stored processor row.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ProcessorRecord {
    pub tenant_id: String,
    pub name: String,
    /// The dataset *name* this processor applies to, or `None` for a
    /// tenant-wide rule.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    pub signal: String,
    pub enabled: bool,
    pub priority: i32,
    pub error_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub statements: Vec<String>,
    /// RFC3339 timestamp, as stored (`StoredRegistry` follows the same
    /// string-typed convention for the same reason: one dialect-agnostic
    /// decode path in the store, see `row_to_record`).
    pub created_at: String,
    pub updated_at: String,
}

/// Caller-supplied processor definition, without tenant scoping or
/// timestamps — the body of a create/replace request.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, utoipa::ToSchema)]
pub struct ProcessorSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dataset: Option<String>,
    pub signal: String,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default = "default_priority")]
    pub priority: i32,
    #[serde(default = "default_error_mode")]
    pub error_mode: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub statements: Vec<String>,
}

fn default_enabled() -> bool {
    true
}

fn default_priority() -> i32 {
    100
}

fn default_error_mode() -> String {
    "ignore".to_string()
}
