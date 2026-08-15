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

    let signaldb_version = signaldb_version(&signaldb_dir);
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

/// SignalDB's registry version is the last path segment of the manifest's
/// `schema_url` (`https://signaldb.dev/schemas/0.1.0` → `0.1.0`).
fn signaldb_version(dir: &Path) -> String {
    let manifest = std::fs::read_to_string(dir.join("manifest.yaml"))
        .unwrap_or_else(|e| panic!("otel/registry/manifest.yaml: {e}"));
    manifest
        .lines()
        .find_map(|l| l.trim().strip_prefix("schema_url:"))
        .and_then(|url| url.trim().rsplit('/').next())
        .map(str::to_string)
        .unwrap_or_else(|| "0.0.0".to_string())
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
