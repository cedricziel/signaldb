//! Build script: embed the bundled schema registries.
//!
//! Parses the vendored OpenTelemetry semantic conventions
//! (`vendor/otel-semconv/<version>/model`), the vendored GenAI conventions
//! (`vendor/otel-semconv-genai/<short-sha>/model`), and SignalDB's own
//! registry (`otel/registry/` plus `otel/registry-genai/`) with
//! `schema-model`, resolves them, and writes one JSON
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
    let vendor_genai = repo.join("vendor/otel-semconv-genai");
    let signaldb_dir = repo.join("otel/registry");
    let signaldb_genai_dir = repo.join("otel/registry-genai");
    // Directory mtimes only change on add/remove, so name every file.
    for dir in [&vendor, &vendor_genai, &signaldb_dir, &signaldb_genai_dir] {
        println!("cargo:rerun-if-changed={}", dir.display());
        for file in walk(dir) {
            println!("cargo:rerun-if-changed={}", file.display());
        }
    }
    println!("cargo:rerun-if-changed=build.rs");

    let version = vendored_version(&vendor, "vendor-semconv");
    let otel_doc =
        RegistryDocument::from_dir("otel", &version, &vendor.join(&version).join("model"))
            .unwrap_or_else(|e| panic!("parse vendored semconv: {e}"));
    let otel = Registry::resolve(&otel_doc, &[]).unwrap_or_else(|errs| {
        panic!("vendored semconv failed to resolve: {}", join_errors(&errs))
    });

    // Vendored by full commit SHA (no upstream release yet); the registry
    // version shown to tenants is the short SHA.
    let genai_commit = vendored_version(&vendor_genai, "vendor-semconv-genai");
    let genai_doc = RegistryDocument::from_dir(
        "otel-genai",
        genai_commit.get(..7).unwrap_or(&genai_commit),
        &vendor_genai.join(&genai_commit).join("model"),
    )
    .unwrap_or_else(|e| panic!("parse vendored GenAI semconv: {e}"));
    let genai = Registry::resolve(&genai_doc, &[&otel]).unwrap_or_else(|errs| {
        panic!(
            "vendored GenAI semconv failed to resolve: {}",
            join_errors(&errs)
        )
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
    let mut signaldb_doc = RegistryDocument::from_dir("signaldb", &signaldb_version, &signaldb_dir)
        .unwrap_or_else(|e| panic!("parse otel/registry: {e}"));
    let signaldb_genai_doc =
        RegistryDocument::from_dir("signaldb", &signaldb_version, &signaldb_genai_dir)
            .unwrap_or_else(|e| panic!("parse otel/registry-genai: {e}"));
    assert!(
        signaldb_genai_doc.dependencies.iter().any(|d| {
            d.namespace().as_deref() == Some("otel-genai")
                && d.registry_path
                    .as_deref()
                    .is_some_and(|p| p.contains(&format!("@{genai_commit}[")))
        }),
        "otel/registry-genai/manifest.yaml must depend on the vendored GenAI commit {genai_commit}"
    );
    signaldb_doc.merge(signaldb_genai_doc);
    // otel-genai first: its gen_ai.* definitions supersede core's deprecated shells.
    let signaldb = Registry::resolve(&signaldb_doc, &[&genai, &otel])
        .unwrap_or_else(|errs| panic!("otel/registry failed to resolve: {}", join_errors(&errs)));

    let mut registries = [
        (otel_doc, otel),
        (genai_doc, genai),
        (signaldb_doc, signaldb),
    ];
    registries.sort_by_key(|(doc, _)| {
        schema_model::RESERVED_NAMESPACES
            .iter()
            .position(|ns| *ns == doc.name)
            .unwrap_or_else(|| panic!("bundled registry `{}` is not reserved", doc.name))
    });
    let snapshot = serde_json::json!({
        "registries": registries
            .iter()
            .map(|(document, resolved)| serde_json::json!({ "document": document, "resolved": resolved }))
            .collect::<Vec<_>>(),
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

/// Contents of a vendor tree's `VERSION` file (the subdirectory holding its
/// `model/`), naming the xtask that writes it when it is missing.
fn vendored_version(dir: &Path, xtask: &str) -> String {
    std::fs::read_to_string(dir.join("VERSION"))
        .unwrap_or_else(|e| {
            panic!(
                "{}/VERSION missing (run `cargo xtask {xtask}`): {e}",
                dir.display()
            )
        })
        .trim()
        .to_string()
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
