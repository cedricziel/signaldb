//! # schema-model
//!
//! The OpenTelemetry Weaver semantic-convention model as SignalDB consumes it:
//! a parser for registry documents (single-file JSON/YAML uploads or a
//! multi-file `model/` tree such as the vendored upstream conventions), a
//! resolver that turns a document plus its dependencies into flat, lookup-ready
//! attribute / entity / metric definitions with reverse indexes, and the subset
//! validator that guards custom registries on write.
//!
//! Group types `attribute_group`, `entity`, and `metric` are resolved; other
//! group types (`span`, `event`, `resource`, ...) and unknown fields round-trip
//! opaquely so an uploaded file is stored losslessly.
//!
//! This crate is deliberately dependency-light: it is used by `common` at
//! runtime and by its `build.rs` to embed the bundled registries.

mod model;
mod resolve;

pub use model::{
    AttributeSpec, AttributeType, Dependency, Deprecated, EnumMember, Group, ParseError,
    RegistryDocument, RequirementLevel, Role,
};
pub use resolve::{
    AttributeDef, DeprecatedInfo, EntityAttribute, EntityDef, EntityRole, MetricAttribute,
    MetricDef, Registry, ResolvedRegistry, ValidationError,
};

/// Namespaces owned by bundled registries; a custom registry may not claim
/// them in any tenant.
pub const RESERVED_NAMESPACES: [&str; 2] = ["otel", "signaldb"];

/// Whether `namespace` is reserved for a bundled registry.
pub fn is_reserved_namespace(namespace: &str) -> bool {
    RESERVED_NAMESPACES.contains(&namespace)
}
