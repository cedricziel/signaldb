//! Resolution and validation: turn a [`RegistryDocument`] plus its
//! dependencies into flat definitions with reverse indexes, collecting every
//! rule violation with a JSON-pointer-like path so a tenant can fix their file.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::model::{
    AttributeSpec, AttributeType, Deprecated, EnumMember, Group, RegistryDocument, Role,
};

/// A validation failure at a location in the document.
#[derive(
    Debug, Clone, Serialize, Deserialize, PartialEq, Eq, thiserror::Error, utoipa::ToSchema,
)]
#[error("{path}: {message}")]
pub struct ValidationError {
    /// Path into the document (`groups[3].attributes[1].ref`).
    pub path: String,
    pub message: String,
}

/// Deprecation info in resolved form.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default, utoipa::ToSchema)]
pub struct DeprecatedInfo {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_to: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

impl From<&Deprecated> for DeprecatedInfo {
    fn from(d: &Deprecated) -> Self {
        match d {
            Deprecated::Structured {
                reason,
                renamed_to,
                note,
                ..
            } => DeprecatedInfo {
                reason: Some(reason.clone()),
                renamed_to: renamed_to.clone(),
                note: note.clone(),
            },
            Deprecated::Legacy(text) => DeprecatedInfo {
                reason: None,
                renamed_to: None,
                note: Some(text.clone()),
            },
        }
    }
}

/// A resolved attribute definition (one per `id` in the registry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct AttributeDef {
    /// Wire key (`k8s.pod.uid`).
    pub key: String,
    pub group_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub group_display_name: Option<String>,
    pub brief: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    /// Canonical type name (`string`, `int[]`, `template[string]`, `enum`).
    pub r#type: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub enum_members: Vec<EnumMember>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub examples: Vec<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<DeprecatedInfo>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_level: Option<String>,
}

/// An attribute referenced by an entity, with its role.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct EntityAttribute {
    pub key: String,
    pub role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_level: Option<String>,
}

/// A resolved entity type.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct EntityDef {
    /// Entity type name (`k8s.pod`).
    pub name: String,
    pub group_id: String,
    pub brief: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<DeprecatedInfo>,
    pub identifying: Vec<EntityAttribute>,
    pub descriptive: Vec<EntityAttribute>,
    /// Entity this one extends (bare name), if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub extends: Option<String>,
}

/// An attribute declared on a metric.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct MetricAttribute {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub requirement_level: Option<String>,
}

/// A resolved metric definition.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct MetricDef {
    pub name: String,
    pub group_id: String,
    pub brief: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
    pub instrument: String,
    pub unit: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub stability: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deprecated: Option<DeprecatedInfo>,
    pub attributes: Vec<MetricAttribute>,
    /// Entity type names this metric describes.
    pub entity_associations: Vec<String>,
}

/// An entity in which an attribute plays a role (reverse index entry).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct EntityRole {
    pub entity: String,
    pub role: Role,
}

/// A registry after resolution: flat definitions keyed by bare wire name plus
/// reverse indexes. Everything is serializable so `common`'s build script can
/// snapshot the bundled registries.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default, utoipa::ToSchema)]
pub struct ResolvedRegistry {
    pub namespace: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Namespaces this registry resolved refs against.
    #[serde(default)]
    pub dependencies: Vec<String>,
    pub attributes: BTreeMap<String, AttributeDef>,
    pub entities: BTreeMap<String, EntityDef>,
    pub metrics: BTreeMap<String, MetricDef>,
    /// Groups of types other than attribute_group/entity/metric, kept
    /// verbatim so the document round-trips.
    #[serde(default)]
    pub other_group_count: usize,
    #[serde(default)]
    entity_metrics: BTreeMap<String, Vec<String>>,
    #[serde(default)]
    attribute_entities: BTreeMap<String, Vec<EntityRole>>,
}

impl ResolvedRegistry {
    /// Metrics whose `entity_associations` name `entity`.
    pub fn metrics_for_entity(&self, entity: &str) -> impl Iterator<Item = &str> {
        self.entity_metrics
            .get(entity)
            .into_iter()
            .flatten()
            .map(String::as_str)
    }

    /// Entities in which `key` plays a role.
    pub fn entity_roles_for_attribute(&self, key: &str) -> impl Iterator<Item = &EntityRole> {
        self.attribute_entities.get(key).into_iter().flatten()
    }

    /// Entities extending `entity`.
    pub fn extensions_of(&self, entity: &str) -> impl Iterator<Item = &EntityDef> {
        self.entities
            .values()
            .filter(move |e| e.extends.as_deref() == Some(entity))
    }

    fn attribute_exists(&self, key: &str) -> bool {
        self.attributes.contains_key(key)
    }
}

/// Entry point for resolution + validation.
pub struct Registry;

impl Registry {
    /// Resolve `doc` against `deps` (already-resolved registries it may `ref`
    /// into; own definitions win, then deps in order). Returns the resolved
    /// registry or every violation found — never a partial result.
    pub fn resolve(
        doc: &RegistryDocument,
        deps: &[&ResolvedRegistry],
    ) -> Result<ResolvedRegistry, Vec<ValidationError>> {
        let mut errors = Vec::new();
        let mut out = ResolvedRegistry {
            namespace: doc.name.clone(),
            version: doc.version.clone(),
            schema_url: doc.schema_url.clone(),
            description: doc.description.clone(),
            dependencies: deps.iter().map(|d| d.namespace.clone()).collect(),
            ..Default::default()
        };

        if doc.name.trim().is_empty() {
            errors.push(err("name", "registry name (namespace) is required"));
        }
        if doc.version.trim().is_empty() {
            errors.push(err("version", "registry version is required"));
        }

        // Pass 1: group ids unique; collect every attribute *definition*
        // (`id`) from any group type, and entity/metric group shells.
        let mut group_ids = BTreeSet::new();
        for (gi, group) in doc.groups.iter().enumerate() {
            let gpath = format!("groups[{gi}]");
            if !group_ids.insert(group.id.as_str()) {
                errors.push(err(
                    &format!("{gpath}.id"),
                    format!("duplicate group id `{}`", group.id),
                ));
            }
            for (ai, attr) in group.attributes.iter().enumerate() {
                let apath = format!("{gpath}.attributes[{ai}]");
                match (&attr.id, &attr.r#ref) {
                    (Some(id), None) => {
                        if out.attributes.contains_key(id) {
                            errors.push(err(
                                &format!("{apath}.id"),
                                format!("duplicate attribute id `{id}`"),
                            ));
                            continue;
                        }
                        match attribute_def(group, attr, id, &apath) {
                            Ok(def) => {
                                out.attributes.insert(id.clone(), def);
                            }
                            Err(e) => errors.push(e),
                        }
                    }
                    (None, Some(_)) => {}
                    (Some(_), Some(_)) => {
                        errors.push(err(&apath, "attribute has both `id` and `ref`"));
                    }
                    (None, None) => {
                        errors.push(err(&apath, "attribute needs `id` or `ref`"));
                    }
                }
            }
        }

        // Pass 2: entities (with extends), metrics, and every `ref`.
        let lookup_attr = |key: &str, out: &ResolvedRegistry| -> bool {
            out.attribute_exists(key) || deps.iter().any(|d| d.attribute_exists(key))
        };
        let lookup_entity = |name: &str, out: &ResolvedRegistry| -> Option<EntityDef> {
            out.entities
                .get(name)
                .cloned()
                .or_else(|| deps.iter().find_map(|d| d.entities.get(name).cloned()))
        };

        // Entities first so metrics can validate associations against them.
        // `extends` may point at an entity later in the file, so iterate to a
        // fixed point (bounded by group count).
        let entity_groups: Vec<(usize, &Group)> = doc
            .groups
            .iter()
            .enumerate()
            .filter(|(_, g)| g.r#type == "entity")
            .collect();
        let mut pending: Vec<(usize, &Group)> = entity_groups.clone();
        let mut progress = true;
        while progress && !pending.is_empty() {
            progress = false;
            let mut still = Vec::new();
            for (gi, group) in pending {
                let gpath = format!("groups[{gi}]");
                let Some(name) = group.name.clone() else {
                    errors.push(err(&format!("{gpath}.name"), "entity group needs `name`"));
                    continue;
                };
                let parent = match &group.extends {
                    None => None,
                    Some(parent_id) => {
                        // `extends` names a *group id*; find its entity name.
                        let parent_name = doc
                            .groups
                            .iter()
                            .find(|g| &g.id == parent_id && g.r#type == "entity")
                            .and_then(|g| g.name.clone())
                            .or_else(|| {
                                deps.iter()
                                    .find_map(|d| {
                                        d.entities.values().find(|e| &e.group_id == parent_id)
                                    })
                                    .map(|e| e.name.clone())
                            });
                        match parent_name.and_then(|n| lookup_entity(&n, &out)) {
                            Some(p) => Some(p),
                            None => {
                                // Might be defined later in this doc; retry.
                                let later = doc
                                    .groups
                                    .iter()
                                    .any(|g| &g.id == parent_id && g.r#type == "entity");
                                if later {
                                    still.push((gi, group));
                                } else {
                                    errors.push(err(
                                        &format!("{gpath}.extends"),
                                        format!("`extends` target `{parent_id}` is not a known entity group"),
                                    ));
                                }
                                continue;
                            }
                        }
                    }
                };
                if out.entities.contains_key(&name) {
                    errors.push(err(
                        &format!("{gpath}.name"),
                        format!("duplicate entity name `{name}`"),
                    ));
                    continue;
                }
                let mut identifying = parent
                    .as_ref()
                    .map(|p| p.identifying.clone())
                    .unwrap_or_default();
                let mut descriptive = parent
                    .as_ref()
                    .map(|p| p.descriptive.clone())
                    .unwrap_or_default();
                for (ai, attr) in group.attributes.iter().enumerate() {
                    let apath = format!("{gpath}.attributes[{ai}]");
                    let Some(key) = attr.id.clone().or_else(|| attr.r#ref.clone()) else {
                        continue; // reported in pass 1
                    };
                    if attr.r#ref.is_some() && !lookup_attr(&key, &out) {
                        errors.push(err(
                            &format!("{apath}.ref"),
                            format!("unresolved ref `{key}`"),
                        ));
                        continue;
                    }
                    // Weaver defaults `role` to descriptive; several upstream
                    // entities (os, otel.scope, webengine) rely on that.
                    let role = attr.role.unwrap_or(Role::Descriptive);
                    let ea = EntityAttribute {
                        key: key.clone(),
                        role,
                        requirement_level: attr
                            .requirement_level
                            .as_ref()
                            .map(|r| r.keyword().to_string()),
                    };
                    match role {
                        Role::Identifying => {
                            if parent.is_some() && !identifying.iter().any(|a| a.key == key) {
                                errors.push(err(
                                    &format!("{apath}.role"),
                                    format!("entity `{name}` extends `{}` and may not add identifying attribute `{key}`", parent.as_ref().map(|p| p.name.as_str()).unwrap_or("")),
                                ));
                                continue;
                            }
                            if !identifying.iter().any(|a| a.key == key) {
                                identifying.push(ea);
                            }
                        }
                        Role::Descriptive => {
                            if !descriptive.iter().any(|a| a.key == key) {
                                descriptive.push(ea);
                            }
                        }
                    }
                }
                out.entities.insert(
                    name.clone(),
                    EntityDef {
                        name: name.clone(),
                        group_id: group.id.clone(),
                        brief: group.brief.clone().unwrap_or_default().trim().to_string(),
                        note: group.note.clone(),
                        stability: group.stability.clone(),
                        deprecated: group.deprecated.as_ref().map(Into::into),
                        identifying,
                        descriptive,
                        extends: parent.map(|p| p.name),
                    },
                );
                progress = true;
            }
            pending = still;
        }
        for (gi, _) in pending {
            errors.push(err(
                &format!("groups[{gi}].extends"),
                "circular `extends` chain",
            ));
        }

        // Metrics and ref checks in every group.
        for (gi, group) in doc.groups.iter().enumerate() {
            let gpath = format!("groups[{gi}]");
            match group.r#type.as_str() {
                "metric" => {
                    let name = match &group.metric_name {
                        Some(n) => n.clone(),
                        None => {
                            errors.push(err(
                                &format!("{gpath}.metric_name"),
                                "metric group needs `metric_name`",
                            ));
                            continue;
                        }
                    };
                    let instrument = match &group.instrument {
                        Some(i) if KNOWN_INSTRUMENTS.contains(&i.as_str()) => i.clone(),
                        Some(i) => {
                            errors.push(err(
                                &format!("{gpath}.instrument"),
                                format!("unknown instrument `{i}`"),
                            ));
                            continue;
                        }
                        None => {
                            errors.push(err(
                                &format!("{gpath}.instrument"),
                                format!("metric `{name}` needs `instrument`"),
                            ));
                            continue;
                        }
                    };
                    let Some(unit) = group.unit.clone() else {
                        errors.push(err(
                            &format!("{gpath}.unit"),
                            format!("metric `{name}` needs `unit`"),
                        ));
                        continue;
                    };
                    if out.metrics.contains_key(&name) {
                        errors.push(err(
                            &format!("{gpath}.metric_name"),
                            format!("duplicate metric name `{name}`"),
                        ));
                        continue;
                    }
                    let mut attributes = Vec::new();
                    // Inherited attributes via `extends` (attribute_group).
                    if let Some(parent_id) = &group.extends {
                        if let Some(parent) = doc.groups.iter().find(|g| &g.id == parent_id) {
                            for a in &parent.attributes {
                                if let Some(key) = a.id.clone().or_else(|| a.r#ref.clone()) {
                                    attributes.push(MetricAttribute {
                                        key,
                                        requirement_level: a
                                            .requirement_level
                                            .as_ref()
                                            .map(|r| r.keyword().to_string()),
                                    });
                                }
                            }
                        } else {
                            errors.push(err(
                                &format!("{gpath}.extends"),
                                format!("`extends` target `{parent_id}` not found"),
                            ));
                        }
                    }
                    for (ai, attr) in group.attributes.iter().enumerate() {
                        let apath = format!("{gpath}.attributes[{ai}]");
                        let Some(key) = attr.id.clone().or_else(|| attr.r#ref.clone()) else {
                            continue;
                        };
                        if attr.r#ref.is_some() && !lookup_attr(&key, &out) {
                            errors.push(err(
                                &format!("{apath}.ref"),
                                format!("unresolved ref `{key}`"),
                            ));
                            continue;
                        }
                        attributes.push(MetricAttribute {
                            key,
                            requirement_level: attr
                                .requirement_level
                                .as_ref()
                                .map(|r| r.keyword().to_string()),
                        });
                    }
                    for (ei, entity) in group.entity_associations.iter().enumerate() {
                        if lookup_entity(entity, &out).is_none() {
                            errors.push(err(
                                &format!("{gpath}.entity_associations[{ei}]"),
                                format!(
                                    "metric `{name}` is associated with unknown entity `{entity}`"
                                ),
                            ));
                        }
                    }
                    for entity in &group.entity_associations {
                        out.entity_metrics
                            .entry(entity.clone())
                            .or_default()
                            .push(name.clone());
                    }
                    out.metrics.insert(
                        name.clone(),
                        MetricDef {
                            name: name.clone(),
                            group_id: group.id.clone(),
                            brief: group.brief.clone().unwrap_or_default().trim().to_string(),
                            note: group.note.clone(),
                            instrument,
                            unit,
                            stability: group.stability.clone(),
                            deprecated: group.deprecated.as_ref().map(Into::into),
                            attributes,
                            entity_associations: group.entity_associations.clone(),
                        },
                    );
                }
                "entity" => {} // handled above
                other => {
                    if other != "attribute_group" {
                        out.other_group_count += 1;
                    }
                    if let Some(parent_id) = &group.extends
                        && !doc.groups.iter().any(|g| &g.id == parent_id)
                        && !deps.iter().any(|d| {
                            d.attributes.values().any(|a| &a.group_id == parent_id)
                                || d.metrics.values().any(|m| &m.group_id == parent_id)
                        })
                    {
                        errors.push(err(
                            &format!("{gpath}.extends"),
                            format!("`extends` target `{parent_id}` not found"),
                        ));
                    }
                    for (ai, attr) in group.attributes.iter().enumerate() {
                        if let Some(key) = &attr.r#ref
                            && !lookup_attr(key, &out)
                        {
                            errors.push(err(
                                &format!("{gpath}.attributes[{ai}].ref"),
                                format!("unresolved ref `{key}`"),
                            ));
                        }
                    }
                }
            }
        }

        // Reverse index attribute -> entity roles.
        for entity in out.entities.values() {
            for a in entity.identifying.iter().chain(entity.descriptive.iter()) {
                out.attribute_entities
                    .entry(a.key.clone())
                    .or_default()
                    .push(EntityRole {
                        entity: entity.name.clone(),
                        role: a.role,
                    });
            }
        }
        for v in out.entity_metrics.values_mut() {
            v.sort();
            v.dedup();
        }

        if errors.is_empty() {
            Ok(out)
        } else {
            Err(errors)
        }
    }
}

/// Instruments the semconv schema allows on a metric group.
pub const KNOWN_INSTRUMENTS: [&str; 4] = ["counter", "updowncounter", "gauge", "histogram"];

fn err(path: &str, message: impl Into<String>) -> ValidationError {
    ValidationError {
        path: path.to_string(),
        message: message.into(),
    }
}

fn attribute_def(
    group: &Group,
    attr: &AttributeSpec,
    id: &str,
    apath: &str,
) -> Result<AttributeDef, ValidationError> {
    let r#type = match &attr.r#type {
        Some(t) if t.is_known() => t,
        Some(t) => {
            return Err(err(
                &format!("{apath}.type"),
                format!("unknown attribute type `{}`", t.name()),
            ));
        }
        None => {
            return Err(err(
                &format!("{apath}.type"),
                format!("attribute `{id}` needs `type`"),
            ));
        }
    };
    if let Some(role) = attr.role
        && group.r#type != "entity"
    {
        let _ = role; // roles are only meaningful on entity groups; tolerated elsewhere
    }
    let enum_members = match r#type {
        AttributeType::Enum { members, .. } => members.clone(),
        AttributeType::Named(_) => Vec::new(),
    };
    let examples = match &attr.examples {
        None => Vec::new(),
        Some(serde_json::Value::Array(items)) => items.clone(),
        Some(v) => vec![v.clone()],
    };
    Ok(AttributeDef {
        key: id.to_string(),
        group_id: group.id.clone(),
        group_display_name: group.display_name.clone(),
        brief: attr.brief.clone().unwrap_or_default().trim().to_string(),
        note: attr.note.clone(),
        r#type: r#type.name().to_string(),
        enum_members,
        examples,
        stability: attr.stability.clone(),
        deprecated: attr.deprecated.as_ref().map(Into::into),
        requirement_level: attr
            .requirement_level
            .as_ref()
            .map(|r| r.keyword().to_string()),
    })
}
