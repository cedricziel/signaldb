//! Drift gate between the compactor's Prometheus exposition and the SignalDB
//! convention registry (`otel/registry/signaldb.yaml`).
//!
//! The compactor renders its metrics by hand rather than through the OTel SDK,
//! so nothing but this test stops the exported names and label sets from
//! drifting away from the registry that `weaver registry check` validates in
//! CI. Prometheus renders a dot-separated instrument name as underscores with
//! a `_total` suffix, and an attribute's dots as underscores — so
//! `compactor.jobs.failed{signaldb.table}` is scraped as
//! `compactor_jobs_failed_total{signaldb_table}`.
//!
//! Parsing is line-based, matching the idiom in `common`'s registry pins: the
//! workspace has no YAML dependency, and a full parser here would be more
//! machinery than the two shapes this file needs.

use compactor::http::ObservabilityState;
use compactor::metrics::{CompactionMetrics, CycleHealth, JobScope};
use compactor::orphan::metrics::OrphanMetrics;
use compactor::retention::metrics::RetentionMetrics;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::time::Duration;
use uuid::Uuid;

fn registry_yaml() -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../otel/registry/signaldb.yaml");
    std::fs::read_to_string(&path).unwrap_or_else(|e| panic!("read {}: {e}", path.display()))
}

/// Prometheus counter name → the attribute names its registry group declares.
///
/// Walks each `type: metric` group from its `metric_name:` to the start of the
/// next group, collecting every `- id:`/`- ref:` under it.
fn declared_metrics(yaml: &str) -> BTreeMap<String, BTreeSet<String>> {
    let mut metrics = BTreeMap::new();
    let mut current: Option<(String, BTreeSet<String>)> = None;

    for line in yaml.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("- id: ")
            && let Some((name, attrs)) = current.take()
        {
            metrics.insert(name, attrs);
        }
        if let Some(name) = trimmed.strip_prefix("metric_name: ") {
            current = Some((prometheus_counter_name(name.trim()), BTreeSet::new()));
        } else if let Some((_, attrs)) = current.as_mut() {
            for prefix in ["- ref: ", "- id: "] {
                if let Some(attr) = trimmed.strip_prefix(prefix) {
                    attrs.insert(prometheus_label_name(attr.trim()));
                }
            }
        }
    }
    if let Some((name, attrs)) = current {
        metrics.insert(name, attrs);
    }
    metrics
}

fn prometheus_counter_name(metric_name: &str) -> String {
    format!("{}_total", metric_name.replace('.', "_"))
}

fn prometheus_label_name(attribute: &str) -> String {
    attribute.replace('.', "_")
}

/// Every `name{labels} value` sample in the exposition, as (name, labels).
fn samples(rendered: &str) -> Vec<(String, BTreeSet<String>)> {
    rendered
        .lines()
        .filter(|line| !line.starts_with('#'))
        .filter_map(|line| {
            let (name, rest) = line.split_once('{')?;
            let labels = rest.split_once('}')?.0;
            let labels = labels
                .split(',')
                .filter_map(|pair| pair.split_once('=').map(|(k, _)| k.trim().to_string()))
                .collect();
            Some((name.to_string(), labels))
        })
        .collect()
}

fn rendered_state() -> String {
    let compaction = CompactionMetrics::new();
    let profiles = JobScope::new("_system", "_monitoring", "profiles");
    let traces = JobScope::new("acme", "production", "traces");

    compaction.record_job_start(&profiles);
    compaction.record_job_failure(&profiles, "resource_exhausted");
    compaction.record_job_start(&traces);
    compaction.record_job_success(&traces, 10, 1, 2048, 1024, Duration::from_millis(5));

    ObservabilityState::new(
        Uuid::nil(),
        compaction,
        RetentionMetrics::new(),
        OrphanMetrics::new(),
        CycleHealth::new(),
    )
    .render_prometheus()
}

/// Every compaction-job series the compactor exports must be declared in the
/// registry, labels included — otherwise `weaver registry check` is validating
/// a document that no longer describes what the process emits.
#[test]
fn exported_job_metrics_match_the_registry() {
    let declared = declared_metrics(&registry_yaml());
    assert!(
        !declared.is_empty(),
        "no `type: metric` groups found in the registry — the parser or the file changed shape"
    );

    for (name, labels) in samples(&rendered_state()) {
        let Some(declared_labels) = declared.get(&name) else {
            // Only the job families are registry-governed so far; the older
            // hand-rolled counters (cycle health, orphan cleanup) are not.
            assert!(
                !name.starts_with("compactor_jobs_"),
                "`{name}` is exported but not declared in otel/registry/signaldb.yaml"
            );
            continue;
        };
        for label in labels {
            assert!(
                declared_labels.contains(&label),
                "`{name}` exports label `{label}`, which its registry group does not declare \
                 (declared: {declared_labels:?})"
            );
        }
    }
}

/// The converse direction: a metric declared in the registry that nothing
/// exports is a promise the process does not keep.
#[test]
fn every_declared_job_metric_is_exported() {
    let declared = declared_metrics(&registry_yaml());
    let exported: BTreeSet<String> = samples(&rendered_state())
        .into_iter()
        .map(|(name, _)| name)
        .collect();

    for name in declared.keys() {
        assert!(
            exported.contains(name),
            "registry declares `{name}` but the compactor exports nothing under that name"
        );
    }
}
