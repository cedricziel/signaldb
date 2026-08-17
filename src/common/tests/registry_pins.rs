//! Drift gate between the code and the SignalDB convention registry
//! (`otel/registry/`): every `signaldb.*` attribute the workspace emits must
//! be declared in the registry, so `weaver registry check` (CI) and
//! live-check coverage stay meaningful. Complements the semconv-crate
//! constant pins in `self_monitoring::spans`.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

fn registry_yaml() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../otel/registry/signaldb.yaml");
    std::fs::read_to_string(&root).unwrap_or_else(|e| panic!("read {}: {e}", root.display()))
}

/// Every `- id: signaldb.*` entry declared in the registry YAML.
fn declared_registry_ids(registry_yaml: &str) -> BTreeSet<&str> {
    registry_yaml
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- id: "))
        .filter(|id| id.starts_with("signaldb."))
        .collect()
}

/// Extract every `signaldb.*` identifier in `content` that is used as a span
/// field: either a macro field assignment (`signaldb.foo.bar = ...`) or the
/// first argument to a `record("...")` call. Everything else -- prose,
/// comments, equality comparisons, file paths, metric-instrument names -- is
/// ignored, so a bare `signaldb.` mention in a doc comment can't be mistaken
/// for a real usage.
fn signaldb_span_field_usages(content: &str) -> BTreeSet<String> {
    let mut used = BTreeSet::new();
    for (i, _) in content.match_indices("signaldb.") {
        let tail = &content[i..];
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
            .unwrap_or(tail.len());
        let attr = tail[..end].trim_end_matches('.');
        if attr.matches('.').count() < 1 {
            continue;
        }
        // Only span-field usages: a macro field assignment (name followed by
        // `=`, but not `==`) or a `record` call with the name as its first
        // argument -- this skips metric instrument names, file names, and
        // prose in comments.
        let is_assignment = tail[end..].trim_start().starts_with('=')
            && !tail[end..].trim_start().starts_with("==");
        let is_record = content[..i].ends_with("record(\"");
        if is_assignment || is_record {
            used.insert(attr.to_string());
        }
    }
    used
}

/// Recursively collect every `.rs` file under `root`, skipping build and
/// frontend directories that are never scanned for span-field usage.
fn rust_files_under(root: &Path) -> Vec<PathBuf> {
    let mut files = Vec::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for entry in std::fs::read_dir(&dir).unwrap() {
            let path = entry.unwrap().path();
            let name = path.file_name().unwrap().to_string_lossy().to_string();
            if path.is_dir() {
                if name != "target" && name != "node_modules" && name != "ui" {
                    stack.push(path);
                }
                continue;
            }
            if path.extension().and_then(|e| e.to_str()) == Some("rs") {
                files.push(path);
            }
        }
    }
    files
}

/// Every `signaldb.*` field name that appears in workspace source must be
/// declared in the registry.
#[test]
fn emitted_signaldb_attributes_are_registered() {
    let registry = registry_yaml();
    let declared = declared_registry_ids(&registry);
    assert!(!declared.is_empty(), "registry declares no attributes?");

    // Scan workspace sources for `signaldb.` span-field usages.
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src");
    let mut used = BTreeSet::new();
    for path in rust_files_under(&src_root) {
        // Skip this file: the scanner unit tests below contain fixture
        // literals (e.g. signaldb.foo.bar) that are inputs to the heuristic,
        // not attributes the codebase emits.
        if path.ends_with("common/tests/registry_pins.rs") {
            continue;
        }
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        used.extend(signaldb_span_field_usages(&content));
    }

    // Span *names* in the signaldb namespace are not attributes.
    let span_names = ["signaldb.query.plan", "signaldb.query.execute"];
    let missing: Vec<_> = used
        .iter()
        .filter(|u| !span_names.contains(&u.as_str()))
        .filter(|u| !declared.contains(u.as_str()))
        .collect();

    assert!(
        missing.is_empty(),
        "signaldb.* fields used in code but missing from otel/registry/signaldb.yaml: {missing:?}"
    );
}

#[test]
fn scanner_matches_macro_field_assignment_form() {
    let content = r#"
        tracing::info_span!(
            "signaldb.job",
            signaldb.tenant.id = %tenant_id,
        );
    "#;

    let used = signaldb_span_field_usages(content);

    assert!(used.contains("signaldb.tenant.id"));
}

#[test]
fn scanner_matches_record_call_form() {
    let content = r#"span.record("signaldb.job.kind", job_kind);"#;

    let used = signaldb_span_field_usages(content);

    assert!(used.contains("signaldb.job.kind"));
}

#[test]
fn scanner_ignores_prose_mention_in_comment() {
    let content = "// See signaldb.tenant.id for the tenancy attribute.";

    let used = signaldb_span_field_usages(content);

    assert!(used.is_empty());
}

#[test]
fn scanner_ignores_equality_comparison() {
    // `==` is a comparison, not a field assignment -- must not be mistaken
    // for a span-field usage.
    let content = "if signaldb.tenant.id == expected_tenant {";

    let used = signaldb_span_field_usages(content);

    assert!(used.is_empty());
}

#[test]
fn scanner_ignores_single_segment_identifier() {
    // A bare "signaldb." with nothing after it (or no second segment) is not
    // a valid attribute name.
    let content = "let prefix = \"signaldb.\";";

    let used = signaldb_span_field_usages(content);

    assert!(used.is_empty());
}

#[test]
fn declared_registry_ids_extracts_signaldb_prefixed_entries_only() {
    let yaml = "\
groups:
  - id: registry.signaldb
    attributes:
      - id: signaldb.tenant.id
        type: string
      - id: signaldb.dataset.id
        type: string
      - id: rpc.method
        type: string
";

    let declared = declared_registry_ids(yaml);

    assert_eq!(
        declared,
        BTreeSet::from(["signaldb.tenant.id", "signaldb.dataset.id"])
    );
}

/// Map each `- id: signaldb.*` registry entry to its declared `type:`.
fn declared_registry_types(registry_yaml: &str) -> std::collections::BTreeMap<String, String> {
    let mut types = std::collections::BTreeMap::new();
    let mut current: Option<String> = None;
    for line in registry_yaml.lines() {
        let trimmed = line.trim();
        if let Some(id) = trimmed.strip_prefix("- id: ") {
            current = id.starts_with("signaldb.").then(|| id.to_string());
        } else if let Some(ty) = trimmed.strip_prefix("type: ")
            && let Some(id) = current.take()
        {
            types.insert(id, ty.to_string());
        }
    }
    types
}

/// Extract `(attribute, rhs)` pairs for every `signaldb.*` macro field
/// assignment and `record("signaldb...", value)` call in `content`. The rhs
/// is the expression text up to the closing delimiter of the field.
fn signaldb_field_assignments(content: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for (i, _) in content.match_indices("signaldb.") {
        let tail = &content[i..];
        let end = tail
            .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
            .unwrap_or(tail.len());
        let attr = tail[..end].trim_end_matches('.').to_string();
        if attr.matches('.').count() < 1 {
            continue;
        }
        let rest = tail[end..].trim_start();
        let rhs = if let Some(rhs) = rest.strip_prefix('=').filter(|r| !r.starts_with('=')) {
            rhs
        } else if content[..i].ends_with("record(\"") {
            rest.strip_prefix("\",").unwrap_or(rest)
        } else {
            continue;
        };
        let rhs_end = rhs.find([',', ';', '\n']).unwrap_or(rhs.len());
        out.push((attr, rhs[..rhs_end].trim().to_string()));
    }
    out
}

/// The tracing→OTel bridge visitors implement `record_i64` but not
/// `record_u64`, so `u64`/`usize` field values fall through to
/// `record_debug` and export as *strings* — silently violating the
/// registry's `type: int` declarations under `weaver registry live-check`.
/// Pin every int-typed registry attribute emission to an explicit `as i64`
/// cast (a deferred `Empty` field is fine: its later `record()` call is
/// scanned too).
#[test]
fn int_registry_attributes_are_emitted_as_i64() {
    let registry = registry_yaml();
    let int_attrs: BTreeSet<String> = declared_registry_types(&registry)
        .into_iter()
        .filter(|(_, ty)| ty == "int")
        .map(|(id, _)| id)
        .collect();
    assert!(
        !int_attrs.is_empty(),
        "registry declares no int attributes?"
    );

    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../src");
    let mut offending = Vec::new();
    for path in rust_files_under(&src_root) {
        if path.ends_with("common/tests/registry_pins.rs") {
            continue;
        }
        let content = std::fs::read_to_string(&path).unwrap_or_default();
        for (attr, rhs) in signaldb_field_assignments(&content) {
            if int_attrs.contains(&attr)
                && !(rhs.contains("as i64") || rhs.contains("Empty") || rhs.ends_with(")]"))
            {
                offending.push(format!("{}: {attr} = {rhs}", path.display()));
            }
        }
    }

    assert!(
        offending.is_empty(),
        "int-typed registry attributes emitted without an `as i64` cast \
         (u64/usize bridge as strings): {offending:#?}"
    );
}

/// The vendored OpenTelemetry semantic-conventions model
/// (`vendor/otel-semconv/`, the source of the bundled `otel` schema registry)
/// must be the same vintage the self-monitoring telemetry claims via
/// `SEMCONV_SCHEMA_URL`. Bump both together: change the constant, then run
/// `cargo xtask vendor-semconv`.
#[test]
fn vendored_semconv_matches_self_monitoring_pin() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../vendor/otel-semconv");
    let vendored = std::fs::read_to_string(root.join("VERSION"))
        .expect("vendor/otel-semconv/VERSION (run `cargo xtask vendor-semconv`)");
    let pinned = common::self_monitoring::SEMCONV_SCHEMA_URL
        .rsplit('/')
        .next()
        .expect("schema url ends in a version");
    assert_eq!(
        vendored.trim(),
        pinned,
        "vendored semconv drifted from the pin"
    );
    assert!(
        root.join(pinned).join("model").is_dir(),
        "vendor/otel-semconv/{pinned}/model missing"
    );
}

/// SignalDB's own registry version (`otel/registry/manifest.yaml`
/// `schema_url`) is bumped by release-please together with this crate, and
/// `build.rs` hands it to the code as `SIGNALDB_SCHEMA_URL`, which the
/// instrumentation scopes claim. All three must agree.
#[test]
fn signaldb_schema_url_tracks_crate_version_and_manifest() {
    let manifest = std::fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../otel/registry/manifest.yaml"),
    )
    .expect("otel/registry/manifest.yaml");
    let manifest_url = manifest
        .lines()
        .find_map(|l| l.strip_prefix("schema_url:"))
        .map(str::trim)
        .expect("top-level schema_url in manifest");

    let url = common::self_monitoring::SIGNALDB_SCHEMA_URL;
    assert_eq!(
        url, manifest_url,
        "SIGNALDB_SCHEMA_URL drifted from the manifest"
    );
    assert_eq!(
        url,
        format!("https://signaldb.dev/schemas/{}", env!("CARGO_PKG_VERSION")),
        "registry schema_url must carry the common crate version (release-please extra-files)"
    );
    // The manifest line sits inside release-please generic-updater markers.
    assert!(manifest.contains("# x-release-please-start-version"));
    assert!(manifest.contains("# x-release-please-end"));
}
