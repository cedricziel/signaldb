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
