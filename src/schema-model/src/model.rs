//! Raw (unresolved) semantic-convention model types, mirroring the Weaver
//! YAML schema closely enough that upstream files and Weaver-authored custom
//! registries deserialize unchanged. Fields we do not interpret are kept in
//! `extra` maps so documents round-trip losslessly.

use std::collections::BTreeMap;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// A whole registry: manifest fields plus every group. This is the on-the-wire
/// shape for custom-registry uploads (JSON or YAML) and what a multi-file
/// `model/` tree is folded into.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RegistryDocument {
    /// Registry namespace (Weaver manifest `name`).
    pub name: String,
    /// Registry version (`1.43.0`); together with `name` identifies it.
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dependencies: Vec<Dependency>,
    #[serde(default)]
    pub groups: Vec<Group>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// A registry this one may `ref` into. Weaver manifests name dependencies by
/// `name` and/or `registry_path`/`schema_url`; SignalDB resolves them to a
/// namespace via [`Dependency::namespace`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct Dependency {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub registry_path: Option<String>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

impl Dependency {
    /// The namespace this dependency points at: an explicit `name`, else
    /// `otel` when the path/url is the upstream semantic conventions. Also
    /// accepts `otel@1.43.0`-style names by dropping the version.
    pub fn namespace(&self) -> Option<String> {
        if let Some(name) = &self.name {
            return Some(name.split('@').next().unwrap_or(name).to_string());
        }
        let upstream = |s: &str| {
            s.contains("opentelemetry.io/schemas")
                || s.contains("open-telemetry/semantic-conventions")
        };
        if self.registry_path.as_deref().is_some_and(upstream)
            || self.schema_url.as_deref().is_some_and(upstream)
        {
            return Some("otel".to_string());
        }
        None
    }
}

/// One `groups:` entry. `type` decides which fields matter; everything else is
/// carried in `extra`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Group {
    pub id: String,
    #[serde(rename = "type")]
    pub r#type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brief: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub display_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<Deprecated>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extends: Option<String>,
    /// Entity type name (`type: entity`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    /// `type: metric` fields.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub metric_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instrument: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entity_associations: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub attributes: Vec<AttributeSpec>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// An attribute inside a group: either a definition (`id`) or a reference to
/// one defined elsewhere (`ref`) with per-group overrides.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub struct AttributeSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub r#type: Option<AttributeType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brief: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub examples: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<Deprecated>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_level: Option<RequirementLevel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Attribute value type: a scalar/array/template name, or an enum.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum AttributeType {
    Named(String),
    Enum {
        members: Vec<EnumMember>,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
}

impl AttributeType {
    /// Canonical type name: the named type as written, or `enum`.
    pub fn name(&self) -> &str {
        match self {
            AttributeType::Named(n) => n,
            AttributeType::Enum { .. } => "enum",
        }
    }

    /// Whether this is a type the semconv schema knows.
    pub fn is_known(&self) -> bool {
        match self {
            AttributeType::Enum { .. } => true,
            AttributeType::Named(n) => KNOWN_TYPES.contains(&n.as_str()),
        }
    }
}

/// Type names permitted by the Weaver semconv schema (plus `enum`, which is
/// spelled structurally). `any`/`map`/`undefined` appear upstream.
pub const KNOWN_TYPES: [&str; 17] = [
    "string",
    "int",
    "double",
    "boolean",
    "string[]",
    "int[]",
    "double[]",
    "boolean[]",
    "template[string]",
    "template[int]",
    "template[double]",
    "template[boolean]",
    "template[string[]]",
    "any",
    "map",
    "map[]",
    "undefined",
];

/// One member of an enum attribute type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct EnumMember {
    pub id: String,
    pub value: serde_json::Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub brief: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub deprecated: Option<Deprecated>,
    #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
    #[schema(value_type = Object)]
    pub extra: BTreeMap<String, serde_json::Value>,
}

/// Deprecation marker: the structured `{reason, renamed_to, note}` form or the
/// legacy free-text form older registries use.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Deprecated {
    Structured {
        reason: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        renamed_to: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        note: Option<String>,
        #[serde(flatten, default, skip_serializing_if = "BTreeMap::is_empty")]
        extra: BTreeMap<String, serde_json::Value>,
    },
    Legacy(String),
}

/// Requirement level: a bare keyword or a keyword with a condition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum RequirementLevel {
    Keyword(String),
    Conditional(BTreeMap<String, serde_json::Value>),
}

impl RequirementLevel {
    /// The level keyword (`required`, `recommended`, `opt_in`,
    /// `conditionally_required`).
    pub fn keyword(&self) -> &str {
        match self {
            RequirementLevel::Keyword(k) => k,
            RequirementLevel::Conditional(map) => {
                map.keys().next().map(String::as_str).unwrap_or("")
            }
        }
    }
}

/// Role of an attribute within an entity.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    Identifying,
    Descriptive,
}

/// Errors reading a registry document.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    #[error("{path}: {source}")]
    Io {
        path: String,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: {message}")]
    Yaml { path: String, message: String },
    #[error("{0}")]
    Json(#[from] serde_json::Error),
}

/// A single model file: `groups:` plus whatever else (ignored).
#[derive(Deserialize)]
struct ModelFile {
    #[serde(default)]
    groups: Vec<Group>,
}

impl RegistryDocument {
    /// Parse a single-document registry from YAML text.
    pub fn from_yaml(text: &str) -> Result<Self, ParseError> {
        serde_norway::from_str(text).map_err(|e| ParseError::Yaml {
            path: "<document>".to_string(),
            message: e.to_string(),
        })
    }

    /// Parse a single-document registry from JSON text.
    pub fn from_json(text: &str) -> Result<Self, ParseError> {
        Ok(serde_json::from_str(text)?)
    }

    /// Fold a Weaver multi-file model tree (every `*.yaml`/`*.yml` under
    /// `dir`, recursively, that carries `groups:`) into one document named
    /// `name@version`. Manifest files without `groups` are ignored; the
    /// caller supplies identity because upstream manifests carry no version.
    /// Files are visited in sorted path order so output is deterministic.
    pub fn from_dir(name: &str, version: &str, dir: &Path) -> Result<Self, ParseError> {
        let mut files = Vec::new();
        collect_yaml_files(dir, &mut files)?;
        files.sort();
        let mut groups = Vec::new();
        let mut schema_url = None;
        let mut description = None;
        let mut dependencies = Vec::new();
        for path in files {
            let text = std::fs::read_to_string(&path).map_err(|e| ParseError::Io {
                path: path.display().to_string(),
                source: e,
            })?;
            let is_manifest = matches!(
                path.file_name().and_then(|f| f.to_str()),
                Some("manifest.yaml" | "registry_manifest.yaml")
            );
            if is_manifest {
                let manifest: Manifest =
                    serde_norway::from_str(&text).map_err(|e| ParseError::Yaml {
                        path: path.display().to_string(),
                        message: e.to_string(),
                    })?;
                schema_url = schema_url.or(manifest.schema_url);
                description = description.or(manifest.description);
                dependencies.extend(manifest.dependencies);
                continue;
            }
            let file: ModelFile = serde_norway::from_str(&text).map_err(|e| ParseError::Yaml {
                path: path.display().to_string(),
                message: e.to_string(),
            })?;
            groups.extend(file.groups);
        }
        Ok(RegistryDocument {
            name: name.to_string(),
            version: version.to_string(),
            schema_url,
            description,
            dependencies,
            groups,
            extra: BTreeMap::new(),
        })
    }
}

/// Weaver manifest file (`manifest.yaml` / `registry_manifest.yaml`).
#[derive(Deserialize)]
struct Manifest {
    #[serde(default)]
    schema_url: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    dependencies: Vec<Dependency>,
}

fn collect_yaml_files(dir: &Path, out: &mut Vec<std::path::PathBuf>) -> Result<(), ParseError> {
    let entries = std::fs::read_dir(dir).map_err(|e| ParseError::Io {
        path: dir.display().to_string(),
        source: e,
    })?;
    for entry in entries {
        let entry = entry.map_err(|e| ParseError::Io {
            path: dir.display().to_string(),
            source: e,
        })?;
        let path = entry.path();
        if path.is_dir() {
            collect_yaml_files(&path, out)?;
        } else if matches!(
            path.extension().and_then(|e| e.to_str()),
            Some("yaml" | "yml")
        ) {
            out.push(path);
        }
    }
    Ok(())
}
