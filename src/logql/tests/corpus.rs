//! Real-world LogQL corpus.
//!
//! Queries harvested verbatim from the official Grafana Loki
//! documentation (query/log_queries, metric_queries, query_examples, ip,
//! template_functions) and well-known community cheat sheets. They serve
//! two purposes: prove the parser accepts the breadth of syntax users
//! actually write, and pin down — honestly — the features it does not
//! support yet, so those gaps are visible and regression-guarded.

use logql::parse;

/// Queries that MUST parse with the current grammar. Grouped by feature
/// for readability; the assertion treats them as one flat set.
const SUPPORTED: &[&str] = &[
    // --- stream selectors: all operators, single/multiple matchers ---
    r#"{app="mysql",name="mysql-backup"}"#,
    r#"{name =~ "mysql.+"}"#,
    r#"{name !~ "mysql.+"}"#,
    r#"{name !~ `mysql-\d+`}"#,
    r#"{name =~ ".*mysql.*"}"#,
    r#"{name =~ "(?s).*mysql.*"}"#,
    r#"{service_name="nginx", status="500"}"#,
    r#"{job="mysql"}"#,
    r#"{instance=~"kafka-[23]",name="kafka"}"#,
    r#"{cluster="ops-tools1", namespace="loki-dev", job="loki-dev/query-frontend"}"#,
    // --- line filters: |=, !=, |~, !~, chained ---
    r#"{job="mysql"} |= "error""#,
    r#"{job="mysql"} |= "error" != "timeout""#,
    r#"{name="kafka"} |~ "tsdb-ops.*io:2003""#,
    r#"{name="cassandra"} |~ `error=\w+`"#,
    r#"{instance=~"kafka-[23]",name="kafka"} != "kafka.server:type=ReplicaManager""#,
    r#"{job="containerlogs"} |~ "(?i)(error|fail|lost|closed|panic|fatal|crash|password|authentication|denied)""#,
    r#"{job="containerlogs", container_name!~"^promtail.+"} |~ "(?i)(error|fail|lost)" != "ForgotPassword""#,
    r#"{job="containerlogs"} |~ "(too many requests|rate.limit)""#,
    r#"{job="security"} != "grafana_com" |= "session opened" != "sudo: ""#,
    r#"{job_name="myapp"} |= "3.180.71.3""#,
    // --- parser stages: json, logfmt, regexp, pattern, unpack ---
    r#"{job="mysql"} | json"#,
    r#"{job="varlogs"} | logfmt"#,
    r#"{job="mysql"} | pattern "<ip> - - <_> \"<method> <uri> <_>\" <status> <size> <_> \"<agent>\" <_>""#,
    r#"{job="mysql"} | regexp "(?P<method>\\w+) (?P<path>[\\w|/]+) \\((?P<status>\\d+?)\\) (?P<duration>.*)""#,
    r#"{job="mysql"} | unpack"#,
    r#"{job="security"} != "grafana_com" |= "session opened" != "sudo: " | regexp "(^(?P<user>\\S+ {1,2}){11})" | line_format "USER = {{.user}}""#,
    // --- json/logfmt with extraction expressions ---
    r#"{job="mysql"} | json first_server="servers[0]", ua="request.headers[\"User-Agent\"]""#,
    r#"{job="mysql"} | json server_list="servers", headers="request.headers""#,
    // --- label filter expressions: string/numeric/duration, and/or/comma ---
    r#"{container="query-frontend",namespace="loki-dev"} |= "metrics.go" | logfmt | duration > 10s and throughput_mb < 500"#,
    r#"{app="foo"} | logfmt | duration >= 20ms or method="GET""#,
    r#"{app="foo"} | logfmt | duration >= 20ms , method!~"2..""#,
    r#"{app="foo"} | logfmt | duration >= 20ms | method!~"2..""#,
    r#"{cluster="ops-tools1", namespace="loki-dev"} |= "metrics.go" != "out of order" | logfmt | duration > 30s or status_code != "200""#,
    // --- formatters: line_format, label_format (rename + template) ---
    r#"{job="mysql"} |= "error" | json | line_format "{{.err}}""#,
    r#"{job="mysql"} | json | line_format "{{.message}}" |= "error""#,
    r#"{container="frontend"} | logfmt | line_format "{{.query}} {{.duration}}""#,
    r#"{container="frontend"} | logfmt | line_format "{{.ip}} {{.status}} {{div .duration 1000}}""#,
    r#"{job="access_log"} | json | line_format `{{.http_request_headers_x_forwarded_for | default "-"}}`"#,
    r#"{job="loki/querier"} |= "finish in prometheus" | logfmt | line_format `{{ range $q := fromJson .queries }} {{ $q.query }} {{ end }}`"#,
    r#"{job="xyzlog"} | line_format `{{ __line__ | count "XYZ"}}`"#,
    r#"{job="loki/querier"} | label_format nowEpoch=`{{(unixEpoch now)}}`,createDateEpoch=`{{unixEpoch (toDate "2006-01-02" .createDate)}}` | label_format dateTimeDiff=`{{sub .nowEpoch .createDateEpoch}}` | dateTimeDiff > 86400"#,
    // --- drop with bare labels / __error__ ---
    r#"{job="varlogs"}|json|drop __error__"#,
    r#"{job="varlogs"}|json|drop level, source"#,
    // --- range aggregations ---
    r#"count_over_time({job="mysql"}[5m])"#,
    r#"count_over_time({job="mysql"}[5m] offset 5m)"#,
    r#"rate({job="mysql"} |= "error" != "timeout" [5m])"#,
    r#"bytes_over_time({job="mysql"}[5m])"#,
    r#"bytes_rate({job="mysql"}[5m])"#,
    r#"absent_over_time({job="mysql"}[5m])"#,
    // --- unwrap range aggregations, incl. conversions ---
    r#"sum_over_time({cluster="ops-tools1",container="loki-dev"} |= "metrics.go" | logfmt | unwrap bytes_processed [1m])"#,
    r#"avg_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"min_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"max_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"stddev_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"stdvar_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"first_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"last_over_time({app="api"} | json | unwrap duration_ms [5m])"#,
    r#"quantile_over_time(0.99, {container="ingress-nginx"} | json | __error__ = "" | unwrap request_time [1m]) by (path)"#,
    r#"quantile_over_time(0.99, {container="ingress-nginx"} | json | unwrap duration(request_time) [1m]) by (path)"#,
    r#"sum_over_time({container="loki"} | logfmt | unwrap bytes(bytes_processed) [1m])"#,
    r#"avg_over_time({app="api"} | logfmt | unwrap duration_seconds(latency) [5m])"#,
    // --- vector aggregations with by/without, topk/bottomk ---
    r#"sum(count_over_time({job="mysql"}[5m])) by (level)"#,
    r#"sum by (host) (rate({job="mysql"} |= "error" != "timeout" | json | duration > 10s [1m]))"#,
    r#"sum by (org_id) (sum_over_time({cluster="ops-tools1",container="loki-dev"} |= "metrics.go" | logfmt | unwrap bytes_processed [1m]))"#,
    r#"topk(10,sum(rate({region="us-east1"}[5m])) by (name))"#,
    r#"topk(10, sum by (app) (rate({namespace="production"}[5m])))"#,
    r#"bottomk(5, sum by (app) (rate({namespace="production"} |= "error" [5m])))"#,
    r#"sum by (job) (bytes_rate({job=~".+"}[5m]))"#,
    r#"sum without(app) (count_over_time({app="foo"}[1m]))"#,
    // --- binary / arithmetic operations and scalars ---
    r#"sum(rate({app="foo"}[5m])) * 2"#,
    r#"sum(rate({app="foo", level="warn"}[1m])) / sum(rate({app="foo", level="error"}[1m]))"#,
    // --- comparison operators, with & without bool ---
    r#"count_over_time({foo="bar"}[1m]) > 10"#,
    r#"count_over_time({foo="bar"}[1m]) > bool 10"#,
    r#"sum without(app) (count_over_time({app="foo"}[1m])) > sum without(app) (count_over_time({app="bar"}[1m]))"#,
    r#"sum without(app) (count_over_time({app="foo"}[1m])) > bool sum without(app) (count_over_time({app="bar"}[1m]))"#,
    // --- bytes literals in label filters ---
    r#"{app="foo"} | logfmt | duration > 1m and bytes_consumed > 20MB"#,
    r#"{app="foo"} | logfmt | size == 20KB"#,
    // --- parenthesized label filter (with bytes) ---
    r#"{app="foo"} | logfmt | ((duration >= 20ms or method="GET") and size <= 20KB)"#,
    // --- decolorize ---
    r#"{job="example"} | decolorize"#,
    // --- drop/keep with matcher expressions ---
    r#"{job="varlogs"}|json|drop level, method="GET""#,
    r#"{job="varlogs"}|json|keep level, app=~"some-api.*""#,
    // --- logfmt flags ---
    r#"{job="varlogs"} | logfmt --strict"#,
    r#"{job="varlogs"} | logfmt --keep-empty --strict host"#,
    // --- ip() line and label filters ---
    r#"{job_name="myapp"} |= ip("192.168.4.5/16")"#,
    r#"{job_name="myapp"} | logfmt | addr = ip("192.168.4.0/24") or addr = ip("10.10.15.0/24")"#,
];

/// Documented LogQL features the parser does NOT support yet. Each MUST
/// currently fail to parse; when support lands, move the entry into
/// `SUPPORTED` and the failing assertion here will flag it. The trailing
/// comment names the missing feature.
const KNOWN_UNSUPPORTED: &[&str] = &[
    r#"count_over_time({job="mysql"}[5m]) offset 5m"#, // offset after aggregation
    r#"rate(({job="mysql"} |= "error" != "timeout")[10s])"#, // parenthesized selector in range
    r#"avg(rate(({job="nginx"} |= "GET" | json | path="/home")[10s])) by (region)"#, // parenthesized selector in range
    r#"sum(count_over_time({namespace="traefik"}[5m])) or vector(0)"#, // vector() function
    r#"label_replace(sum(rate({job="api"}[5m])) by (instance), "host", "$1", "instance", "(.*):.*")"#, // label_replace()
    r#"{$label_name=~"$label_value"}"#, // Grafana template variables (interpolated before LogQL parsing)
];

#[test]
fn supported_corpus_parses() {
    let mut failures = Vec::new();
    for q in SUPPORTED {
        if let Err(e) = parse(q) {
            failures.push(format!("  {q}\n    -> {e}"));
        }
    }
    assert!(
        failures.is_empty(),
        "{} corpus queries failed to parse:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn known_unsupported_corpus_is_rejected() {
    let mut unexpectedly_ok = Vec::new();
    for q in KNOWN_UNSUPPORTED {
        if parse(q).is_ok() {
            unexpectedly_ok.push(*q);
        }
    }
    assert!(
        unexpectedly_ok.is_empty(),
        "queries now parse and should move to SUPPORTED:\n  {}",
        unexpectedly_ok.join("\n  ")
    );
}

#[test]
fn corpus_is_non_trivial() {
    // Guard against accidental truncation of the corpus.
    assert!(
        SUPPORTED.len() >= 80,
        "supported corpus shrank: {}",
        SUPPORTED.len()
    );
    assert!(!KNOWN_UNSUPPORTED.is_empty());
}
