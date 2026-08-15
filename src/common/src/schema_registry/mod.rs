//! # Schema registry
//!
//! Namespaced, versioned semantic-convention registries and the resolver that
//! answers "what is known about wire key X / entity type Y / metric name Z"
//! for a tenant.
//!
//! - **Bundled** registries (`otel` = vendored OpenTelemetry semconv at the
//!   self-monitoring pin, `signaldb` = `otel/registry/`) are embedded at build
//!   time (see `build.rs`) and are read-only.
//! - **Custom** registries are tenant-scoped Weaver-model documents stored in
//!   the catalog (`schema_registries`); see [`store`].
//! - [`SchemaResolver`] merges both with precedence tenant custom (namespace
//!   asc, version desc) → `signaldb` → `otel`; every hit is namespace-tagged
//!   and alternatives are never dropped.

mod store;

use std::collections::BTreeMap;
use std::sync::Arc;

use dashmap::DashMap;
use once_cell::sync::Lazy;
use schema_model::{
    AttributeDef, EntityDef, EntityRole, MetricDef, Registry, RegistryDocument, ResolvedRegistry,
    ValidationError,
};
use serde::{Deserialize, Serialize};

pub use store::{StoreError, StoredRegistry};

use crate::catalog::Catalog;

/// Where a registry comes from.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum RegistrySource {
    Bundled,
    Custom,
    Remote,
}

impl RegistrySource {
    pub fn as_str(&self) -> &'static str {
        match self {
            RegistrySource::Bundled => "bundled",
            RegistrySource::Custom => "custom",
            RegistrySource::Remote => "remote",
        }
    }
}

/// One bundled registry: the document (served verbatim) and its resolution,
/// shared behind `Arc`s so per-request views never copy the ~1k-group otel
/// document.
#[derive(Debug, Clone)]
pub struct BundledRegistry {
    pub document: Arc<RegistryDocument>,
    pub resolved: Arc<ResolvedRegistry>,
}

#[derive(Deserialize)]
struct SnapshotEntry {
    document: RegistryDocument,
    resolved: ResolvedRegistry,
}

#[derive(Deserialize)]
struct Snapshot {
    registries: Vec<SnapshotEntry>,
}

static BUNDLED: Lazy<Vec<BundledRegistry>> = Lazy::new(|| {
    let raw = include_str!(concat!(env!("OUT_DIR"), "/bundled_schema_registries.json"));
    let snapshot: Snapshot =
        serde_json::from_str(raw).expect("embedded schema-registry snapshot is valid JSON");
    snapshot
        .registries
        .into_iter()
        .map(|e| BundledRegistry {
            document: Arc::new(e.document),
            resolved: Arc::new(e.resolved),
        })
        .collect()
});

/// The bundled registries, in precedence order (`signaldb` before `otel`).
pub fn bundled_registries() -> &'static [BundledRegistry] {
    &BUNDLED
}

/// Whether `namespace` is a bundled (read-only, reserved) registry.
pub fn is_bundled(namespace: &str) -> bool {
    schema_model::is_reserved_namespace(namespace)
}

/// Registry list entry.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct RegistrySummary {
    pub namespace: String,
    pub version: String,
    pub source: RegistrySource,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub attribute_count: usize,
    pub entity_count: usize,
    pub metric_count: usize,
    pub read_only: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<String>,
}

/// Outcome of validating a document without storing it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct ValidationReport {
    pub namespace: String,
    pub version: String,
    pub errors: Vec<ValidationError>,
    pub attribute_count: usize,
    pub entity_count: usize,
    pub metric_count: usize,
}

/// A resolved attribute definition tagged with provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct AttributeHit {
    pub namespace: String,
    pub version: String,
    pub source: RegistrySource,
    #[serde(flatten)]
    pub def: AttributeDef,
    /// Entities (from any visible registry) in which this key plays a role,
    /// as `namespace/entity`-qualified roles.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entity_roles: Vec<QualifiedEntityRole>,
}

/// An entity role qualified by the registry that declares it.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, utoipa::ToSchema)]
pub struct QualifiedEntityRole {
    pub namespace: String,
    pub entity: String,
    pub role: schema_model::Role,
}

/// A resolved entity definition tagged with provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct EntityHit {
    pub namespace: String,
    pub version: String,
    pub source: RegistrySource,
    #[serde(flatten)]
    pub def: EntityDef,
    /// Metric names associated with this entity (from every visible registry).
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub metrics: Vec<String>,
    /// `namespace/entity` names of entities extending this one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extended_by: Vec<String>,
}

/// A resolved metric definition tagged with provenance.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, utoipa::ToSchema)]
pub struct MetricHit {
    pub namespace: String,
    pub version: String,
    pub source: RegistrySource,
    #[serde(flatten)]
    pub def: MetricDef,
}

/// Result of resolving one name: every hit in precedence order; `primary` is
/// the first.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Resolution<T> {
    pub key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub primary: Option<T>,
    pub hits: Vec<T>,
}

impl<T: Clone> Resolution<T> {
    fn new(key: &str, hits: Vec<T>) -> Self {
        Resolution {
            key: key.to_string(),
            primary: hits.first().cloned(),
            hits,
        }
    }
}

/// A registry visible to a tenant, in memory.
#[derive(Debug, Clone)]
struct Visible {
    source: RegistrySource,
    updated_at: Option<String>,
    document: Arc<RegistryDocument>,
    resolved: Arc<ResolvedRegistry>,
}

impl Visible {
    fn summary(&self) -> RegistrySummary {
        RegistrySummary {
            namespace: self.resolved.namespace.clone(),
            version: self.resolved.version.clone(),
            source: self.source,
            schema_url: self.resolved.schema_url.clone(),
            description: self.resolved.description.clone(),
            attribute_count: self.resolved.attributes.len(),
            entity_count: self.resolved.entities.len(),
            metric_count: self.resolved.metrics.len(),
            read_only: self.source != RegistrySource::Custom,
            updated_at: self.updated_at.clone(),
        }
    }
}

/// Resolves names against bundled + tenant custom registries.
#[derive(Clone)]
pub struct SchemaResolver {
    catalog: Catalog,
    /// Per-tenant custom registries in precedence order (namespace asc,
    /// version desc). Loaded lazily; dropped on every write.
    custom: Arc<DashMap<String, Arc<Vec<Visible>>>>,
}

impl SchemaResolver {
    pub fn new(catalog: Catalog) -> Self {
        SchemaResolver {
            catalog,
            custom: Arc::new(DashMap::new()),
        }
    }

    fn bundled_visible() -> Vec<Visible> {
        bundled_registries()
            .iter()
            .map(|b| Visible {
                source: RegistrySource::Bundled,
                updated_at: None,
                document: b.document.clone(),
                resolved: b.resolved.clone(),
            })
            .collect()
    }

    async fn custom_for(&self, tenant_id: &str) -> Result<Arc<Vec<Visible>>, StoreError> {
        if let Some(v) = self.custom.get(tenant_id) {
            return Ok(v.clone());
        }
        let mut rows = self.catalog.list_schema_registries(tenant_id).await?;
        rows.sort_by(|a, b| {
            a.namespace
                .cmp(&b.namespace)
                .then_with(|| version_cmp(&b.version, &a.version))
        });
        let visible: Vec<Visible> = rows
            .into_iter()
            .map(|r| Visible {
                source: RegistrySource::Custom,
                updated_at: Some(r.updated_at),
                document: Arc::new(r.document),
                resolved: Arc::new(r.resolved),
            })
            .collect();
        let arc = Arc::new(visible);
        self.custom.insert(tenant_id.to_string(), arc.clone());
        Ok(arc)
    }

    fn invalidate(&self, tenant_id: &str) {
        self.custom.remove(tenant_id);
    }

    /// Every registry visible to the tenant in precedence order.
    async fn visible(&self, tenant_id: &str) -> Result<Vec<Visible>, StoreError> {
        let mut all: Vec<Visible> = self.custom_for(tenant_id).await?.as_ref().clone();
        all.extend(Self::bundled_visible());
        Ok(all)
    }

    // ---- listing / documents -------------------------------------------

    pub async fn list(&self, tenant_id: &str) -> Result<Vec<RegistrySummary>, StoreError> {
        Ok(self
            .visible(tenant_id)
            .await?
            .iter()
            .map(Visible::summary)
            .collect())
    }

    pub async fn get(
        &self,
        tenant_id: &str,
        namespace: &str,
        version: &str,
    ) -> Result<Option<(RegistrySummary, RegistryDocument)>, StoreError> {
        Ok(self
            .visible(tenant_id)
            .await?
            .into_iter()
            .find(|v| v.resolved.namespace == namespace && v.resolved.version == version)
            .map(|v| (v.summary(), v.document.as_ref().clone())))
    }

    // ---- custom registry mutation --------------------------------------

    /// Resolve `doc` against its declared dependencies (bundled or the
    /// tenant's other custom registries; `otel` when none are declared).
    async fn resolve_document(
        &self,
        tenant_id: &str,
        doc: &RegistryDocument,
    ) -> Result<Result<ResolvedRegistry, Vec<ValidationError>>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let mut deps: Vec<Arc<ResolvedRegistry>> = Vec::new();
        let mut errors = Vec::new();
        if doc.dependencies.is_empty() {
            deps.extend(
                visible
                    .iter()
                    .filter(|v| v.resolved.namespace == "otel")
                    .map(|v| v.resolved.clone()),
            );
        } else {
            for (i, dep) in doc.dependencies.iter().enumerate() {
                let Some(ns) = dep.namespace() else {
                    errors.push(ValidationError {
                        path: format!("dependencies[{i}]"),
                        message: "dependency needs a `name` (or an upstream semconv registry_path)"
                            .into(),
                    });
                    continue;
                };
                // Newest visible version of that namespace, excluding the doc itself.
                match visible.iter().find(|v| {
                    v.resolved.namespace == ns
                        && !(v.resolved.namespace == doc.name && v.resolved.version == doc.version)
                }) {
                    Some(v) => deps.push(v.resolved.clone()),
                    None => errors.push(ValidationError {
                        path: format!("dependencies[{i}]"),
                        message: format!("unknown dependency namespace `{ns}`"),
                    }),
                }
            }
        }
        if !errors.is_empty() {
            return Ok(Err(errors));
        }
        let dep_refs: Vec<&ResolvedRegistry> = deps.iter().map(|d| d.as_ref()).collect();
        Ok(Registry::resolve(doc, &dep_refs))
    }

    /// Validate without storing.
    pub async fn validate(
        &self,
        tenant_id: &str,
        doc: &RegistryDocument,
    ) -> Result<ValidationReport, StoreError> {
        let mut errors = Vec::new();
        if is_bundled(&doc.name) {
            errors.push(ValidationError {
                path: "name".into(),
                message: format!(
                    "namespace `{}` is reserved for a bundled registry",
                    doc.name
                ),
            });
        }
        match self.resolve_document(tenant_id, doc).await? {
            Ok(resolved) => Ok(ValidationReport {
                namespace: doc.name.clone(),
                version: doc.version.clone(),
                errors,
                attribute_count: resolved.attributes.len(),
                entity_count: resolved.entities.len(),
                metric_count: resolved.metrics.len(),
            }),
            Err(mut errs) => {
                errors.append(&mut errs);
                Ok(ValidationReport {
                    namespace: doc.name.clone(),
                    version: doc.version.clone(),
                    errors,
                    attribute_count: 0,
                    entity_count: 0,
                    metric_count: 0,
                })
            }
        }
    }

    async fn resolve_for_write(
        &self,
        tenant_id: &str,
        doc: &RegistryDocument,
    ) -> Result<ResolvedRegistry, StoreError> {
        if is_bundled(&doc.name) {
            return Err(StoreError::ReservedNamespace(doc.name.clone()));
        }
        match self.resolve_document(tenant_id, doc).await? {
            Ok(r) => Ok(r),
            Err(errs) => Err(StoreError::Invalid(errs)),
        }
    }

    /// Create a custom registry at the document's `name@version`.
    pub async fn create(
        &self,
        tenant_id: &str,
        doc: &RegistryDocument,
    ) -> Result<RegistrySummary, StoreError> {
        let resolved = self.resolve_for_write(tenant_id, doc).await?;
        if self
            .catalog
            .get_schema_registry(tenant_id, &doc.name, &doc.version)
            .await?
            .is_some()
        {
            return Err(StoreError::AlreadyExists {
                namespace: doc.name.clone(),
                version: doc.version.clone(),
            });
        }
        let stored = self
            .catalog
            .insert_schema_registry(tenant_id, doc, &resolved)
            .await?;
        self.invalidate(tenant_id);
        Ok(summary_of(&stored))
    }

    /// Replace the custom registry at `namespace@version` with `doc` (whose
    /// identity must match). Atomic: the old document is served until the
    /// new one is validated and stored.
    pub async fn replace(
        &self,
        tenant_id: &str,
        namespace: &str,
        version: &str,
        doc: &RegistryDocument,
    ) -> Result<RegistrySummary, StoreError> {
        if is_bundled(namespace) {
            return Err(StoreError::ReadOnly {
                namespace: namespace.to_string(),
                version: version.to_string(),
            });
        }
        if doc.name != namespace || doc.version != version {
            return Err(StoreError::IdentityMismatch {
                path: format!("{namespace}@{version}"),
                document: format!("{}@{}", doc.name, doc.version),
            });
        }
        let resolved = self.resolve_for_write(tenant_id, doc).await?;
        let stored = self
            .catalog
            .replace_schema_registry(tenant_id, doc, &resolved)
            .await?;
        self.invalidate(tenant_id);
        Ok(summary_of(&stored))
    }

    /// Delete a custom registry. `Ok(false)` when it did not exist.
    pub async fn delete(
        &self,
        tenant_id: &str,
        namespace: &str,
        version: &str,
    ) -> Result<bool, StoreError> {
        if is_bundled(namespace) {
            return Err(StoreError::ReadOnly {
                namespace: namespace.to_string(),
                version: version.to_string(),
            });
        }
        let deleted = self
            .catalog
            .delete_schema_registry(tenant_id, namespace, version)
            .await?;
        self.invalidate(tenant_id);
        Ok(deleted)
    }

    // ---- resolution ------------------------------------------------------

    pub async fn resolve_attribute(
        &self,
        tenant_id: &str,
        key: &str,
    ) -> Result<Resolution<AttributeHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let roles = entity_roles(&visible, key);
        let hits = visible
            .iter()
            .filter_map(|v| {
                v.resolved.attributes.get(key).map(|def| AttributeHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                    entity_roles: roles.clone(),
                })
            })
            .collect();
        Ok(Resolution::new(key, hits))
    }

    pub async fn resolve_entity(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Resolution<EntityHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let metrics = metrics_for_entity(&visible, name);
        let extended_by = extensions_of(&visible, name);
        let hits = visible
            .iter()
            .filter_map(|v| {
                v.resolved.entities.get(name).map(|def| EntityHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                    metrics: metrics.clone(),
                    extended_by: extended_by.clone(),
                })
            })
            .collect();
        Ok(Resolution::new(name, hits))
    }

    pub async fn resolve_metric(
        &self,
        tenant_id: &str,
        name: &str,
    ) -> Result<Resolution<MetricHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let hits = visible
            .iter()
            .filter_map(|v| {
                v.resolved.metrics.get(name).map(|def| MetricHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                })
            })
            .collect();
        Ok(Resolution::new(name, hits))
    }

    // ---- prefix search ---------------------------------------------------

    /// Attributes whose key starts with `prefix`, precedence order across
    /// registries, one hit per `(namespace, key)`, at most `limit`.
    pub async fn search_attributes(
        &self,
        tenant_id: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<AttributeHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let mut out = Vec::new();
        for v in &visible {
            for (key, def) in prefix_range(&v.resolved.attributes, prefix) {
                if out.len() >= limit {
                    return Ok(out);
                }
                out.push(AttributeHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                    entity_roles: entity_roles(&visible, key),
                });
            }
        }
        Ok(out)
    }

    pub async fn search_entities(
        &self,
        tenant_id: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<EntityHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let mut out = Vec::new();
        for v in &visible {
            for (name, def) in prefix_range(&v.resolved.entities, prefix) {
                if out.len() >= limit {
                    return Ok(out);
                }
                out.push(EntityHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                    metrics: metrics_for_entity(&visible, name),
                    extended_by: extensions_of(&visible, name),
                });
            }
        }
        Ok(out)
    }

    pub async fn search_metrics(
        &self,
        tenant_id: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<MetricHit>, StoreError> {
        let visible = self.visible(tenant_id).await?;
        let mut out = Vec::new();
        for v in &visible {
            for (_, def) in prefix_range(&v.resolved.metrics, prefix) {
                if out.len() >= limit {
                    return Ok(out);
                }
                out.push(MetricHit {
                    namespace: v.resolved.namespace.clone(),
                    version: v.resolved.version.clone(),
                    source: v.source,
                    def: def.clone(),
                });
            }
        }
        Ok(out)
    }
}

fn summary_of(stored: &StoredRegistry) -> RegistrySummary {
    RegistrySummary {
        namespace: stored.namespace.clone(),
        version: stored.version.clone(),
        source: RegistrySource::Custom,
        schema_url: stored.resolved.schema_url.clone(),
        description: stored.resolved.description.clone(),
        attribute_count: stored.resolved.attributes.len(),
        entity_count: stored.resolved.entities.len(),
        metric_count: stored.resolved.metrics.len(),
        read_only: false,
        updated_at: Some(stored.updated_at.clone()),
    }
}

/// Keys of `map` starting with `prefix`, in key order.
fn prefix_range<'a, T>(
    map: &'a BTreeMap<String, T>,
    prefix: &str,
) -> impl Iterator<Item = (&'a str, &'a T)> + 'a {
    let prefix = prefix.to_string();
    map.range(prefix.clone()..)
        .take_while(move |(k, _)| k.starts_with(&prefix))
        .map(|(k, v)| (k.as_str(), v))
}

fn entity_roles(visible: &[Visible], key: &str) -> Vec<QualifiedEntityRole> {
    let mut out = Vec::new();
    for v in visible {
        for EntityRole { entity, role } in v.resolved.entity_roles_for_attribute(key) {
            let q = QualifiedEntityRole {
                namespace: v.resolved.namespace.clone(),
                entity: entity.clone(),
                role: *role,
            };
            if !out.contains(&q) {
                out.push(q);
            }
        }
    }
    out
}

fn metrics_for_entity(visible: &[Visible], name: &str) -> Vec<String> {
    let mut out: Vec<String> = Vec::new();
    for v in visible {
        for m in v.resolved.metrics_for_entity(name) {
            if !out.iter().any(|x| x == m) {
                out.push(m.to_string());
            }
        }
    }
    out
}

fn extensions_of(visible: &[Visible], name: &str) -> Vec<String> {
    let mut out = Vec::new();
    for v in visible {
        for e in v.resolved.extensions_of(name) {
            let q = format!("{}/{}", v.resolved.namespace, e.name);
            if !out.contains(&q) {
                out.push(q);
            }
        }
    }
    out
}

/// Compare versions numerically per dot-separated component when possible,
/// falling back to string order.
fn version_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let parse = |s: &str| -> Vec<Result<u64, String>> {
        s.split('.')
            .map(|p| p.parse::<u64>().map_err(|_| p.to_string()))
            .collect()
    };
    let (pa, pb) = (parse(a), parse(b));
    for (x, y) in pa.iter().zip(pb.iter()) {
        let ord = match (x, y) {
            (Ok(x), Ok(y)) => x.cmp(y),
            _ => format!("{x:?}").cmp(&format!("{y:?}")),
        };
        if ord != std::cmp::Ordering::Equal {
            return ord;
        }
    }
    pa.len().cmp(&pb.len())
}
