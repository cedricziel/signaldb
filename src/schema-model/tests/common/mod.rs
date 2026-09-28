//! Loaders for the vendored registries, shared by this crate's test binaries.
#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use schema_model::{Registry, RegistryDocument, ResolvedRegistry};

pub fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

pub fn fixtures_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

/// A vendor tree's `VERSION`, which also names the directory holding `model/`.
fn vendored(tree: &str) -> (String, PathBuf) {
    let root = repo_root().join("vendor").join(tree);
    let version = std::fs::read_to_string(root.join("VERSION"))
        .unwrap_or_else(|e| panic!("vendor/{tree}/VERSION: {e}"))
        .trim()
        .to_string();
    let model = root.join(&version).join("model");
    (version, model)
}

pub fn otel_document() -> RegistryDocument {
    let (version, model) = vendored("otel-semconv");
    RegistryDocument::from_dir("otel", &version, &model).expect("parse vendored semconv model")
}

pub fn genai_document() -> RegistryDocument {
    let (commit, model) = vendored("otel-semconv-genai");
    RegistryDocument::from_dir("otel-genai", &commit[..7], &model)
        .expect("parse vendored GenAI model")
}

/// Resolve `doc` against `deps`, panicking with every error on failure.
pub fn resolve(doc: &RegistryDocument, deps: &[&ResolvedRegistry]) -> ResolvedRegistry {
    Registry::resolve(doc, deps).unwrap_or_else(|errs| {
        panic!(
            "{}@{} must resolve cleanly, got {} errors:\n{}",
            doc.name,
            doc.version,
            errs.len(),
            errs.iter()
                .map(|e| e.to_string())
                .collect::<Vec<_>>()
                .join("\n")
        )
    })
}

/// The resolved vendored `otel` registry, parsed once per test binary.
pub fn otel_resolved() -> ResolvedRegistry {
    static OTEL: OnceLock<ResolvedRegistry> = OnceLock::new();
    OTEL.get_or_init(|| resolve(&otel_document(), &[])).clone()
}
