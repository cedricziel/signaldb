//! Drift gate between the code and the SignalDB convention registry
//! (`otel/registry/`): every `signaldb.*` attribute the workspace emits must
//! be declared in the registry, so `weaver registry check` (CI) and
//! live-check coverage stay meaningful. Complements the semconv-crate
//! constant pins in `self_monitoring::spans`.

use std::collections::BTreeSet;
use std::path::Path;

fn registry_yaml() -> String {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../otel/registry/signaldb.yaml");
    std::fs::read_to_string(&root).unwrap_or_else(|e| panic!("read {}: {e}", root.display()))
}

/// Every `signaldb.*` field name that appears in workspace source must be
/// declared in the registry.
#[test]
fn emitted_signaldb_attributes_are_registered() {
    let registry = registry_yaml();
    let declared: BTreeSet<&str> = registry
        .lines()
        .filter_map(|l| l.trim().strip_prefix("- id: "))
        .filter(|id| id.starts_with("signaldb."))
        .collect();
    assert!(!declared.is_empty(), "registry declares no attributes?");

    // Scan workspace sources for `signaldb.` span-field usages.
    let src_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut used = BTreeSet::new();
    let mut stack = vec![src_root.join("src")];
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
            if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                continue;
            }
            let content = std::fs::read_to_string(&path).unwrap_or_default();
            for (i, _) in content.match_indices("signaldb.") {
                let tail = &content[i..];
                let end = tail
                    .find(|c: char| !(c.is_ascii_alphanumeric() || c == '.' || c == '_'))
                    .unwrap_or(tail.len());
                let attr = tail[..end].trim_end_matches('.');
                if attr.matches('.').count() < 1 {
                    continue;
                }
                // Only span-field usages: a macro field assignment
                // (name followed by `=`) or a `record` call with the name as
                // its first argument — this skips metric instrument names,
                // file names, and prose in comments.
                let is_assignment = tail[end..].trim_start().starts_with('=')
                    && !tail[end..].trim_start().starts_with("==");
                let is_record = content[..i].ends_with("record(\"");
                if is_assignment || is_record {
                    used.insert(attr.to_string());
                }
            }
        }
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
