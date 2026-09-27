//! Raw (unresolved) semantic-convention model types, mirroring the Weaver
//! YAML schema closely enough that upstream files and Weaver-authored custom
//! registries deserialize unchanged. Fields we do not interpret are kept in
//! `extra` maps so documents round-trip losslessly.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

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
    /// The namespace this dependency points at: an explicit `name` (with an
    /// `@version` suffix dropped), else the bundled upstream registry whose
    /// repository or schema URL it names (see [`UPSTREAM_NAMESPACES`]).
    pub fn namespace(&self) -> Option<String> {
        if let Some(name) = &self.name {
            return Some(name.split('@').next().unwrap_or(name).to_string());
        }
        let locations = [self.registry_path.as_deref(), self.schema_url.as_deref()];
        UPSTREAM_NAMESPACES
            .iter()
            .find(|(markers, _)| {
                locations
                    .iter()
                    .flatten()
                    .any(|loc| markers.iter().any(|m| loc.contains(m)))
            })
            .map(|(_, namespace)| namespace.to_string())
    }
}

/// Upstream repository / schema-URL markers and the bundled namespace they
/// map to. First match wins, so the GenAI repository (whose URLs contain the
/// core markers) is listed before core.
const UPSTREAM_NAMESPACES: [(&[&str], &str); 2] = [
    (
        &[
            "open-telemetry/semantic-conventions-genai",
            "opentelemetry.io/schemas/gen-ai",
        ],
        "otel-genai",
    ),
    (
        &[
            "open-telemetry/semantic-conventions",
            "opentelemetry.io/schemas",
        ],
        "otel",
    ),
];

/// One `groups:` entry. `type` decides which fields matter; everything else is
/// carried in `extra`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
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
    #[error(
        "{path}: unsupported file_format `{format}` (expected no `file_format` or `definition/2`)"
    )]
    UnsupportedFileFormat { path: String, format: String },
    #[error("{path}: model file has no `groups` key")]
    MissingGroups { path: String },
}

/// A v1 model file. `groups` is `Option` (rather than defaulting to empty) so
/// a file with no `groups:` key at all is distinguishable from `groups: []`.
#[derive(Deserialize)]
struct ModelFileV1 {
    groups: Option<Vec<Group>>,
}

/// Maps a YAML error to [`ParseError::Yaml`] naming `path`.
fn yaml_err(path: &Path) -> impl FnOnce(serde_norway::Error) -> ParseError + '_ {
    move |e| ParseError::Yaml {
        path: path.display().to_string(),
        message: e.to_string(),
    }
}

/// Lets callers holding a shared document (e.g. from `SchemaResolver::get`)
/// compare it against an owned one without dereferencing.
impl PartialEq<RegistryDocument> for Arc<RegistryDocument> {
    fn eq(&self, other: &RegistryDocument) -> bool {
        **self == *other
    }
}

/// Path reported in errors for a single-document upload.
const DOCUMENT: &str = "<document>";

/// Model layout of a registry file, from its `file_format` key.
enum Layout {
    Groups,
    DefinitionV2,
}

impl Layout {
    fn of(file_format: Option<&str>, path: &str) -> Result<Self, ParseError> {
        match file_format {
            None => Ok(Layout::Groups),
            Some("definition/2") => Ok(Layout::DefinitionV2),
            Some(other) => Err(ParseError::UnsupportedFileFormat {
                path: path.to_string(),
                format: other.to_string(),
            }),
        }
    }
}

/// Top-level keys of a `definition/2` model file ([`ModelFileV2`]'s fields).
const V2_SECTIONS: [&str; 8] = [
    "attributes",
    "attribute_groups",
    "metrics",
    "spans",
    "events",
    "entities",
    "span_refinements",
    "metric_refinements",
];

impl RegistryDocument {
    /// Parse a single-document registry from YAML text, in the `groups` or
    /// the `definition/2` layout (lowered to `groups`).
    pub fn from_yaml(text: &str) -> Result<Self, ParseError> {
        serde_norway::from_str::<Self>(text)
            .map_err(yaml_err(Path::new(DOCUMENT)))?
            .lower_layout()
    }

    /// Parse a single-document registry from JSON text, in the `groups` or
    /// the `definition/2` layout (lowered to `groups`).
    pub fn from_json(text: &str) -> Result<Self, ParseError> {
        serde_json::from_str::<Self>(text)?.lower_layout()
    }

    /// A `definition/2` document parses with its model sections in `extra`;
    /// lower them into `groups`.
    fn lower_layout(mut self) -> Result<Self, ParseError> {
        let file_format = self
            .extra
            .get("file_format")
            .map(|f| f.as_str().map_or_else(|| f.to_string(), str::to_owned));
        if let Layout::DefinitionV2 = Layout::of(file_format.as_deref(), DOCUMENT)? {
            self.extra.remove("file_format");
            let sections: serde_json::Map<_, _> = V2_SECTIONS
                .into_iter()
                .filter_map(|key| self.extra.remove_entry(key))
                .collect();
            let model: ModelFileV2 = serde_json::from_value(sections.into())?;
            self.groups
                .extend(lower_v2(vec![(format!("registry.{}", self.name), model)]));
        }
        Ok(self)
    }

    /// Append `other`'s groups and any dependency whose namespace `self` does
    /// not already declare. `self` keeps its identity, schema URL, and
    /// description.
    pub fn merge(&mut self, other: RegistryDocument) {
        self.groups.extend(other.groups);
        for dep in other.dependencies {
            let namespace = dep.namespace();
            if !self.dependencies.iter().any(|d| d.namespace() == namespace) {
                self.dependencies.push(dep);
            }
        }
    }

    /// Fold a Weaver multi-file model tree (every `*.yaml`/`*.yml` under
    /// `dir`, recursively) into one document named `name@version`. Model
    /// files are v1 (`groups:`) or `file_format: definition/2`, which is
    /// lowered to v1 groups; anything else is an error. The caller supplies
    /// identity because upstream manifests carry no version. Files are
    /// visited in sorted path order so output is deterministic.
    pub fn from_dir(name: &str, version: &str, dir: &Path) -> Result<Self, ParseError> {
        let mut files = Vec::new();
        collect_yaml_files(dir, &mut files)?;
        files.sort();
        let mut groups = Vec::new();
        let mut schema_url = None;
        let mut description = None;
        let mut dependencies = Vec::new();
        let mut v2_files = Vec::new();
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
                let manifest: Manifest = serde_norway::from_str(&text).map_err(yaml_err(&path))?;
                schema_url = schema_url.or(manifest.schema_url);
                description = description.or(manifest.description);
                dependencies.extend(manifest.dependencies);
                continue;
            }
            let value: serde_norway::Value =
                serde_norway::from_str(&text).map_err(yaml_err(&path))?;
            let file_format = value
                .get("file_format")
                .map(|f| f.as_str().map_or_else(|| format!("{f:?}"), str::to_owned));
            match Layout::of(file_format.as_deref(), &path.display().to_string())? {
                Layout::Groups => {
                    let file: ModelFileV1 =
                        serde_norway::from_value(value).map_err(yaml_err(&path))?;
                    groups.extend(file.groups.ok_or_else(|| ParseError::MissingGroups {
                        path: path.display().to_string(),
                    })?);
                }
                Layout::DefinitionV2 => {
                    let file: ModelFileV2 =
                        serde_norway::from_value(value).map_err(yaml_err(&path))?;
                    v2_files.push((synthetic_registry_id(dir, &path), file));
                }
            }
        }
        groups.extend(lower_v2(v2_files));
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

/// Weaver v2 (`file_format: definition/2`) model file. Every section is
/// optional; fields we do not model (e.g. `kind` on a span) round-trip
/// through the lowered [`Group`]'s `extra`.
#[derive(Deserialize)]
struct ModelFileV2 {
    #[serde(default)]
    attributes: Vec<V2Attr>,
    #[serde(default)]
    attribute_groups: Vec<V2AttributeGroup>,
    #[serde(default)]
    metrics: Vec<V2Metric>,
    #[serde(default)]
    spans: Vec<V2Span>,
    #[serde(default)]
    events: Vec<V2Named>,
    #[serde(default)]
    entities: Vec<V2Named>,
    #[serde(default)]
    span_refinements: Vec<V2Refinement>,
    #[serde(default)]
    metric_refinements: Vec<V2Refinement>,
}

/// A `definition/2` attribute: a definition keyed by `key` (v1's `id`), a
/// `ref` with overrides, or a `ref_group` splicing another group's list.
#[derive(Deserialize, Clone)]
struct V2Attr {
    #[serde(default)]
    key: Option<String>,
    #[serde(default)]
    ref_group: Option<String>,
    #[serde(flatten)]
    spec: AttributeSpec,
}

/// Fields every `definition/2` group-like entry shares with a v1 [`Group`].
#[derive(Deserialize)]
struct V2Common {
    #[serde(default)]
    brief: Option<String>,
    #[serde(default)]
    note: Option<String>,
    #[serde(default)]
    stability: Option<String>,
    #[serde(default)]
    deprecated: Option<Deprecated>,
    #[serde(default)]
    extends: Option<String>,
    #[serde(default)]
    attributes: Vec<V2Attr>,
    #[serde(flatten)]
    extra: BTreeMap<String, serde_json::Value>,
}

#[derive(Deserialize)]
struct V2AttributeGroup {
    id: String,
    #[serde(default)]
    display_name: Option<String>,
    #[serde(flatten)]
    common: V2Common,
}

#[derive(Deserialize)]
struct V2Metric {
    name: String,
    instrument: String,
    unit: String,
    #[serde(default)]
    entity_associations: Vec<String>,
    #[serde(flatten)]
    common: V2Common,
}

#[derive(Deserialize)]
struct V2Span {
    r#type: String,
    #[serde(flatten)]
    common: V2Common,
}

/// An `events:` or `entities:` entry, identified by `name`.
#[derive(Deserialize)]
struct V2Named {
    name: String,
    #[serde(flatten)]
    common: V2Common,
}

/// A `span_refinements:`/`metric_refinements:` entry: attributes layered
/// onto the span or metric named by `ref`.
#[derive(Deserialize)]
struct V2Refinement {
    id: String,
    r#ref: String,
    #[serde(flatten)]
    common: V2Common,
}

/// `Group` field names; a v2 key landing in `extra` under one of these would
/// be read back into the typed field, so it is kept as `v2_<key>` instead.
const GROUP_FIELDS: [&str; 14] = [
    "id",
    "type",
    "brief",
    "note",
    "display_name",
    "stability",
    "deprecated",
    "extends",
    "name",
    "metric_name",
    "instrument",
    "unit",
    "entity_associations",
    "attributes",
];

/// A v1 [`Group`] for one v2 entry, plus the entry's unexpanded attributes.
fn v2_group(id: String, r#type: &str, common: V2Common) -> (Group, Vec<V2Attr>) {
    let mut extra = common.extra;
    for field in GROUP_FIELDS {
        if let Some(value) = extra.remove(field) {
            extra.insert(format!("v2_{field}"), value);
        }
    }
    let group = Group {
        id,
        r#type: r#type.to_string(),
        brief: common.brief,
        note: common.note,
        stability: common.stability,
        deprecated: common.deprecated,
        extends: common.extends,
        extra,
        ..Group::default()
    };
    (group, common.attributes)
}

/// Lower every `definition/2` file of a tree to v1 groups. Lowered together
/// so a `ref_group` can name a group from any file: metrics are
/// `metric.<name>`, spans `span.<type>`, events `event.<name>`, entities
/// `entity.<name>`, and a file's top-level `attributes` a synthetic
/// `registry.<path>` group.
fn lower_v2(files: Vec<(String, ModelFileV2)>) -> Vec<Group> {
    let mut pending: Vec<(Group, Vec<V2Attr>)> = Vec::new();
    for (registry_id, file) in files {
        if !file.attributes.is_empty() {
            let group = Group {
                id: registry_id,
                r#type: "attribute_group".to_string(),
                ..Group::default()
            };
            pending.push((group, file.attributes));
        }
        for g in file.attribute_groups {
            let (mut group, attrs) = v2_group(g.id, "attribute_group", g.common);
            group.display_name = g.display_name;
            pending.push((group, attrs));
        }
        for m in file.metrics {
            let (mut group, attrs) = v2_group(format!("metric.{}", m.name), "metric", m.common);
            group.metric_name = Some(m.name);
            group.instrument = Some(m.instrument);
            group.unit = Some(m.unit);
            group.entity_associations = m.entity_associations;
            pending.push((group, attrs));
        }
        for s in file.spans {
            pending.push(v2_group(format!("span.{}", s.r#type), "span", s.common));
        }
        for (kind, items) in [("event", file.events), ("entity", file.entities)] {
            for e in items {
                let (mut group, attrs) = v2_group(format!("{kind}.{}", e.name), kind, e.common);
                group.name = Some(e.name);
                pending.push((group, attrs));
            }
        }
        for (base, items) in [
            ("span", file.span_refinements),
            ("metric", file.metric_refinements),
        ] {
            for r in items {
                let (mut group, attrs) = v2_group(r.id, "attribute_group", r.common);
                group.extends = Some(format!("{base}.{}", r.r#ref));
                pending.push((group, attrs));
            }
        }
    }

    let by_id: BTreeMap<&str, &[V2Attr]> = pending
        .iter()
        .map(|(group, attrs)| (group.id.as_str(), attrs.as_slice()))
        .collect();
    let expanded: Vec<Vec<AttributeSpec>> = pending
        .iter()
        .map(|(_, attrs)| expand_v2_attrs(attrs, &by_id, &mut BTreeSet::new()))
        .collect();
    pending
        .into_iter()
        .zip(expanded)
        .map(|((mut group, _), attributes)| {
            group.attributes = attributes;
            group
        })
        .collect()
}

/// Flatten a v2 attribute list into [`AttributeSpec`]s, splicing each
/// `ref_group` in place (recursively; a cycle splices nothing).
fn expand_v2_attrs<'a>(
    attrs: &'a [V2Attr],
    by_id: &BTreeMap<&str, &'a [V2Attr]>,
    visiting: &mut BTreeSet<&'a str>,
) -> Vec<AttributeSpec> {
    let mut out = Vec::new();
    for attr in attrs {
        match &attr.ref_group {
            Some(group_id) => {
                if let Some(inner) = by_id.get(group_id.as_str())
                    && visiting.insert(group_id)
                {
                    out.extend(expand_v2_attrs(inner, by_id, visiting));
                    visiting.remove(group_id.as_str());
                }
            }
            None => {
                let mut spec = attr.spec.clone();
                if attr.key.is_some() {
                    spec.id = attr.key.clone();
                }
                out.push(spec);
            }
        }
    }
    out
}

/// Deterministic synthetic `attribute_group` id for a file's top-level
/// `attributes:` list: `registry.<relative path, dots for separators, no
/// extension>` (e.g. `gen-ai/registry.yaml` -> `registry.gen-ai.registry`).
fn synthetic_registry_id(dir: &Path, path: &Path) -> String {
    let rel = path.strip_prefix(dir).unwrap_or(path).with_extension("");
    let stem = rel
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(".");
    format!("registry.{stem}")
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
