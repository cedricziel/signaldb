//! Build script: embed the bundled schema registries.
//!
//! Parses the vendored OpenTelemetry semantic conventions
//! (`vendor/otel-semconv/<version>/model`) and SignalDB's own registry
//! (`otel/registry/`) with `schema-model`, resolves them, and writes one JSON
//! snapshot (documents + resolved definitions) into `OUT_DIR`, which
//! `common::schema_registry` includes with `include_str!`. Parse or
//! resolution errors in the vendored files fail the build instead of the
//! process; the runtime never touches these files.

use std::path::{Path, PathBuf};

use schema_model::{Registry, RegistryDocument};

fn main() {
    let manifest_dir =
        PathBuf::from(std::env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR"));
    let repo = manifest_dir.join("../..");
    let vendor = repo.join("vendor/otel-semconv");
    let signaldb_dir = repo.join("otel/registry");
    // Directory mtimes only change on add/remove, so name every file.
    for dir in [&vendor, &signaldb_dir] {
        println!("cargo:rerun-if-changed={}", dir.display());
        for file in walk(dir) {
            println!("cargo:rerun-if-changed={}", file.display());
        }
    }
    println!("cargo:rerun-if-changed=build.rs");

    let version = std::fs::read_to_string(vendor.join("VERSION"))
        .unwrap_or_else(|e| {
            panic!("vendor/otel-semconv/VERSION missing (run `cargo xtask vendor-semconv`): {e}")
        })
        .trim()
        .to_string();
    let otel_doc =
        RegistryDocument::from_dir("otel", &version, &vendor.join(&version).join("model"))
            .unwrap_or_else(|e| panic!("parse vendored semconv: {e}"));
    let otel = Registry::resolve(&otel_doc, &[]).unwrap_or_else(|errs| {
        panic!("vendored semconv failed to resolve: {}", join_errors(&errs))
    });

    let signaldb_schema_url = signaldb_schema_url(&signaldb_dir);
    // Exposed as `common::self_monitoring::SIGNALDB_SCHEMA_URL` so the
    // instrumentation scopes claim the same registry version this snapshot
    // bundles; release-please bumps the manifest with the crate version.
    println!("cargo:rustc-env=SIGNALDB_SCHEMA_URL={signaldb_schema_url}");
    let signaldb_version = signaldb_schema_url
        .rsplit('/')
        .next()
        .unwrap_or("0.0.0")
        .to_string();
    let signaldb_doc = RegistryDocument::from_dir("signaldb", &signaldb_version, &signaldb_dir)
        .unwrap_or_else(|e| panic!("parse otel/registry: {e}"));
    let signaldb = Registry::resolve(&signaldb_doc, &[&otel])
        .unwrap_or_else(|errs| panic!("otel/registry failed to resolve: {}", join_errors(&errs)));

    let snapshot = serde_json::json!({
        "registries": [
            { "document": otel_doc, "resolved": otel },
            { "document": signaldb_doc, "resolved": signaldb },
        ]
    });
    let out = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"))
        .join("bundled_schema_registries.json");
    std::fs::write(
        &out,
        serde_json::to_vec(&snapshot).expect("serialize snapshot"),
    )
    .unwrap_or_else(|e| panic!("write {}: {e}", out.display()));
}

/// The manifest's top-level `schema_url` (`https://cedricziel.github.io/signaldb/schemas/X.Y.Z`);
/// its last path segment is the registry version. Only the first
/// `schema_url:` line counts — the dependency entries below it are indented.
fn signaldb_schema_url(dir: &Path) -> String {
    let manifest = std::fs::read_to_string(dir.join("manifest.yaml"))
        .unwrap_or_else(|e| panic!("otel/registry/manifest.yaml: {e}"));
    manifest
        .lines()
        .find_map(|l| l.strip_prefix("schema_url:"))
        .map(|url| url.trim().to_string())
        .unwrap_or_else(|| panic!("otel/registry/manifest.yaml: no top-level schema_url"))
}

fn join_errors(errs: &[schema_model::ValidationError]) -> String {
    errs.iter()
        .map(|e| e.to_string())
        .collect::<Vec<_>>()
        .join("\n")
}

fn walk(dir: &Path) -> Vec<PathBuf> {
    let mut out = Vec::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if path.is_dir() {
                out.extend(walk(&path));
            } else {
                out.push(path);
            }
        }
    }
    out
}
