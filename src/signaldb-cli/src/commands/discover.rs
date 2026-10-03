//! The `discover` command. Every subcommand goes through the Query IR's
//! `describe` stage (or `GET /api/v1/query/sources`) via `signaldb-sdk`, never
//! a Tempo/Loki/Prometheus/Pyroscope metadata endpoint, so they speak logical
//! dotted OTel names and are answered from the schema registry and maintained
//! statistics instead of a scan.
//!
//! `discover fields|values|sources` are the native surface.
//! `discover attributes --signal traces|logs|metrics [--tag NAME]` is the
//! signal-selected shorthand for `fields` (or `values` for one field when
//! `--tag` is given), and `discover metrics` lists metric names as the values
//! of the `metric.name` field. `--scope resource|span|intrinsic` (traces only)
//! narrows trace discovery to that attribute level; untyped keys carry no
//! level and are not listed under any scope.

use clap::{Args, Subcommand, ValueEnum};
use signaldb_sdk::types::{AttributeLevel, DiscoveredField, FieldOrigin, QueryIrResponse};

use super::query::{build_http_client, print_json_response};

/// Which signal to discover attributes for.
#[derive(Clone, Debug, ValueEnum)]
pub enum Signal {
    /// Trace attributes.
    Traces,
    /// Log attributes.
    Logs,
    /// Metric attributes.
    Metrics,
    /// Profile attributes.
    Profiles,
}

/// Attribute level to narrow trace discovery to. `--scope` is only valid with
/// `--signal traces`.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
}

impl TagScope {
    /// Whether a described field belongs to this scope: `resource` is the
    /// resource level, `span` the record level, and `intrinsic` a declared
    /// field that carries no attribute level.
    fn keeps(self, field: &DiscoveredField) -> bool {
        match self {
            TagScope::Resource => field.level == Some(AttributeLevel::Resource),
            TagScope::Span => field.level == Some(AttributeLevel::Record),
            TagScope::Intrinsic => field.origin == FieldOrigin::Declared && field.level.is_none(),
        }
    }

    /// The field a tag names at this scope: the level-qualified name the
    /// server lists when a key is typed at two levels. Intrinsics have no
    /// level to qualify by, so they have none.
    fn qualify(self, tag: &str) -> Option<String> {
        match self {
            TagScope::Resource => Some(format!("resource.{tag}")),
            TagScope::Span => Some(format!("span.{tag}")),
            TagScope::Intrinsic => None,
        }
    }
}

impl Signal {
    /// The Query IR source this signal's attributes live in.
    fn source(&self) -> &'static str {
        match self {
            Signal::Traces => "traces",
            Signal::Logs => "logs",
            Signal::Metrics => "metrics",
            Signal::Profiles => "profiles",
        }
    }
}

#[derive(Subcommand)]
pub enum DiscoverAction {
    /// List attribute/label names for a signal, or the values for one name
    Attributes(AttributesArgs),
    /// List distinct metric names (samples stored metric data)
    Metrics(MetricsArgs),
    /// List the queryable fields of a signal source (native surface)
    Fields(FieldsArgs),
    /// Suggest values for one field (native surface)
    Values(ValuesArgs),
    /// List the signal sources available to your tenant
    Sources(ConnectArgs),
}

#[derive(Args)]
pub struct FieldsArgs {
    /// The signal source to describe
    #[arg(long, default_value = "logs")]
    source: String,
    /// Range start (RFC3339, `now-1h`, or epoch nanoseconds)
    #[arg(long, default_value = "now-1h")]
    from: String,
    /// Range end
    #[arg(long, default_value = "now")]
    to: String,
    /// Maximum fields to return
    #[arg(long)]
    limit: Option<u64>,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct ValuesArgs {
    /// The signal source the field belongs to
    #[arg(long, default_value = "logs")]
    source: String,
    /// The logical field to suggest values for (a dotted OTel name)
    #[arg(long)]
    field: String,
    /// Range start (RFC3339, `now-1h`, or epoch nanoseconds)
    #[arg(long, default_value = "now-1h")]
    from: String,
    /// Range end
    #[arg(long, default_value = "now")]
    to: String,
    /// Maximum values to return
    #[arg(long)]
    limit: Option<u64>,
    /// Read the range's data to answer whenever no declared value set covers
    /// the field, rather than answering from maintained statistics (which
    /// cover one compacted partition). Without this the command answers from
    /// statistics or reports what would answer it instead of scanning.
    #[arg(long)]
    sample: bool,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// The range and bound of a `describe` read.
#[derive(Args)]
pub struct WindowArgs {
    /// Range start (RFC3339, `now-1h`, or epoch nanoseconds)
    #[arg(long, default_value = "now-1h")]
    from: String,
    /// Range end
    #[arg(long, default_value = "now")]
    to: String,
    /// Maximum fields, values or metric names to return
    #[arg(long)]
    limit: Option<u64>,
}

#[derive(Args)]
pub struct MetricsArgs {
    #[command(flatten)]
    window: WindowArgs,
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct AttributesArgs {
    /// Which signal to discover attributes for
    #[arg(long, value_enum, default_value = "traces")]
    signal: Signal,
    /// List the known values for this field, instead of field names
    #[arg(long)]
    tag: Option<String>,
    /// Narrow trace discovery to one attribute level (`resource`, `span`, or
    /// `intrinsic`: declared fields with no level). Lists only the fields at
    /// that level, or with `--tag` looks up the level-qualified field
    /// (`resource.<tag>` / `span.<tag>`; not valid for `intrinsic`). Only valid
    /// with `--signal traces`. Limits: untyped keys (no attribute level) and
    /// scope-level attributes are never listed under a scope; `--limit` counts
    /// the scoped fields; a qualified tag can
    /// land on an intrinsic (`span.kind`).
    #[arg(long, value_enum)]
    scope: Option<TagScope>,
    /// With `--tag`: read the range's data to answer whenever no declared
    /// value set covers the field, rather than answering from maintained
    /// statistics (which cover one compacted partition). Without this the
    /// command answers from statistics or reports what would answer it.
    #[arg(long)]
    sample: bool,
    #[command(flatten)]
    window: WindowArgs,
    #[command(flatten)]
    connect: ConnectArgs,
}

/// Router URL + tenant credential shared by the tenant-authenticated commands
/// (`discover`, `schema`, `admin schema`).
#[derive(Args)]
pub struct ConnectArgs {
    /// SignalDB router base URL
    #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
    pub(crate) url: String,
    /// API key for authentication
    #[arg(long, env = "SIGNALDB_API_KEY")]
    pub(crate) api_key: Option<String>,
    /// Tenant ID
    #[arg(long, env = "SIGNALDB_TENANT_ID")]
    pub(crate) tenant_id: Option<String>,
    /// Dataset ID
    #[arg(long, env = "SIGNALDB_DATASET_ID")]
    pub(crate) dataset_id: Option<String>,
}

impl ConnectArgs {
    pub(crate) fn build_client(&self) -> anyhow::Result<signaldb_sdk::Client> {
        build_http_client(
            &self.url,
            self.api_key.as_deref(),
            self.tenant_id.as_deref(),
            self.dataset_id.as_deref(),
        )
    }
}

impl DiscoverAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            DiscoverAction::Attributes(args) => args.run().await,
            DiscoverAction::Metrics(args) => run_metrics(&args).await,
            DiscoverAction::Fields(args) => args.run().await,
            DiscoverAction::Values(args) => args.run().await,
            DiscoverAction::Sources(connect) => run_sources(&connect).await,
        }
    }
}

/// The IR document for one `describe` request. Built as JSON and deserialized
/// into the generated request type, so the CLI states the document exactly as
/// the reference documents it.
fn describe_document(
    source: &str,
    from: &str,
    to: &str,
    stage: serde_json::Value,
) -> anyhow::Result<signaldb_sdk::types::QueryIrRequest> {
    let document = serde_json::json!({
        "irVersion": 4,
        "from": source,
        "range": { "from": from, "to": to },
        "result": "metadata",
        "pipeline": [ { "describe": stage } ]
    });
    Ok(serde_json::from_value(document)?)
}

/// The `describe` stage: a field's values when `field` is given, else the
/// source's fields.
fn describe_stage(field: Option<&str>, limit: Option<u64>, sample: bool) -> serde_json::Value {
    let mut stage = match field {
        Some(field) => serde_json::json!({ "target": "values", "field": field }),
        None => serde_json::json!({ "target": "fields" }),
    };
    if let Some(limit) = limit {
        stage["limit"] = serde_json::json!(limit);
    }
    if sample {
        stage["sample"] = serde_json::json!(true);
    }
    stage
}

/// Drops the fields of a `describe: fields` response outside `scope`, then
/// applies `limit`, so the limit counts scoped fields only.
fn retain_scope(response: &mut QueryIrResponse, scope: TagScope, limit: Option<u64>) {
    if let Some(metadata) = response.metadata.as_mut() {
        metadata.fields.retain(|f| scope.keeps(f));
        if let Some(limit) = limit.and_then(|l| usize::try_from(l).ok())
            && metadata.fields.len() > limit
        {
            metadata.fields.truncate(limit);
            metadata.truncated = true;
        }
    }
}

/// Sends one `describe` document through the generated SDK and prints the
/// response, narrowed to `scope` when one is given.
async fn describe_and_print(
    connect: &ConnectArgs,
    source: &str,
    (from, to): (&str, &str),
    stage: serde_json::Value,
    scope: Option<(TagScope, Option<u64>)>,
    what: &str,
) -> anyhow::Result<()> {
    let body = describe_document(source, from, to, stage)?;
    let client = connect.build_client()?;
    let result = client.query_ir().body(body).send().await.map(|r| {
        let mut response = r.into_inner();
        if let Some((scope, limit)) = scope {
            retain_scope(&mut response, scope, limit);
        }
        response
    });
    print_json_response(result, what)
}

impl FieldsArgs {
    async fn run(self) -> anyhow::Result<()> {
        let stage = describe_stage(None, self.limit, false);
        let range = (self.from.as_str(), self.to.as_str());
        describe_and_print(
            &self.connect,
            &self.source,
            range,
            stage,
            None,
            "discover fields",
        )
        .await
    }
}

impl ValuesArgs {
    async fn run(self) -> anyhow::Result<()> {
        if self.field.trim().is_empty() {
            anyhow::bail!("--field must name a logical field");
        }
        let stage = describe_stage(Some(&self.field), self.limit, self.sample);
        let range = (self.from.as_str(), self.to.as_str());
        describe_and_print(
            &self.connect,
            &self.source,
            range,
            stage,
            None,
            "discover values",
        )
        .await
    }
}

async fn run_sources(connect: &ConnectArgs) -> anyhow::Result<()> {
    let client = connect.build_client()?;
    let result = client.query_sources().send().await.map(|r| r.into_inner());
    print_json_response(result, "discover sources")
}

impl AttributesArgs {
    async fn run(self) -> anyhow::Result<()> {
        if self.scope.is_some() && !matches!(self.signal, Signal::Traces) {
            anyhow::bail!("--scope is only valid with --signal traces");
        }
        if self.tag.as_deref().is_some_and(|t| t.trim().is_empty()) {
            anyhow::bail!("--tag must name a field");
        }
        let field = match (self.tag.as_deref(), self.scope) {
            (Some(tag), Some(scope)) => Some(scope.qualify(tag).ok_or_else(|| {
                anyhow::anyhow!("--scope intrinsic cannot be combined with --tag")
            })?),
            (Some(tag), None) => Some(tag.to_string()),
            (None, _) => None,
        };
        let list_scope = self
            .scope
            .filter(|_| self.tag.is_none())
            .map(|scope| (scope, self.window.limit));
        let stage_limit = if list_scope.is_some() {
            None
        } else {
            self.window.limit
        };
        let stage = describe_stage(field.as_deref(), stage_limit, self.sample);
        let range = (self.window.from.as_str(), self.window.to.as_str());
        let source = self.signal.source();
        describe_and_print(
            &self.connect,
            source,
            range,
            stage,
            list_scope,
            "discover attributes",
        )
        .await
    }
}

/// Metric names are the values of the `metric.name` field. No declared value
/// set or maintained sketch covers them, so the listing samples stored metric
/// data in the range, bounded by the limit.
async fn run_metrics(args: &MetricsArgs) -> anyhow::Result<()> {
    let stage = describe_stage(Some("metric.name"), args.window.limit, true);
    let range = (args.window.from.as_str(), args.window.to.as_str());
    describe_and_print(
        &args.connect,
        "metrics",
        range,
        stage,
        None,
        "discover metrics",
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[derive(Parser)]
    struct TestCli {
        #[command(subcommand)]
        action: DiscoverAction,
    }

    #[test]
    fn fields_defaults_to_logs_over_the_last_hour() {
        let cli = TestCli::try_parse_from(["discover", "fields"]).expect("parses");
        let DiscoverAction::Fields(args) = cli.action else {
            panic!("expected Fields");
        };
        assert_eq!(args.source, "logs");
        assert_eq!(args.from, "now-1h");
        assert_eq!(args.to, "now");
        assert!(args.limit.is_none());
    }

    #[test]
    fn values_requires_a_field_and_does_not_sample_by_default() {
        assert!(
            TestCli::try_parse_from(["discover", "values"]).is_err(),
            "a value request without a field has nothing to answer"
        );
        let cli = TestCli::try_parse_from([
            "discover",
            "values",
            "--source",
            "traces",
            "--field",
            "http.route",
        ])
        .expect("parses");
        let DiscoverAction::Values(args) = cli.action else {
            panic!("expected Values");
        };
        assert_eq!(args.field, "http.route");
        assert!(
            !args.sample,
            "reading data must be something the user asked for"
        );
    }

    #[test]
    fn the_describe_document_is_what_the_reference_documents() {
        let stage =
            serde_json::json!({ "target": "values", "field": "http.route", "sample": true });
        let request = describe_document("traces", "now-6h", "now", stage).expect("builds");
        let value = serde_json::to_value(&request).expect("serializes");
        assert_eq!(value["irVersion"], 4);
        assert_eq!(value["from"], "traces");
        assert_eq!(value["result"], "metadata");
        assert_eq!(value["pipeline"][0]["describe"]["field"], "http.route");
        assert_eq!(value["pipeline"][0]["describe"]["sample"], true);
    }

    #[test]
    fn attributes_defaults_to_traces_signal() {
        let cli = TestCli::try_parse_from(["discover", "attributes"]).expect("parses");
        let DiscoverAction::Attributes(args) = cli.action else {
            panic!("expected Attributes");
        };
        assert!(matches!(args.signal, Signal::Traces));
        assert!(args.tag.is_none());
    }

    #[test]
    fn attributes_accepts_signal_and_tag() {
        let cli = TestCli::try_parse_from([
            "discover",
            "attributes",
            "--signal",
            "metrics",
            "--tag",
            "job",
        ])
        .expect("parses");
        let DiscoverAction::Attributes(args) = cli.action else {
            panic!("expected Attributes");
        };
        assert!(matches!(args.signal, Signal::Metrics));
        assert_eq!(args.tag.as_deref(), Some("job"));
    }

    #[test]
    fn metrics_subcommand_parses() {
        let cli = TestCli::try_parse_from(["discover", "metrics"]).expect("parses");
        assert!(matches!(cli.action, DiscoverAction::Metrics(_)));
    }

    #[test]
    fn rejects_unknown_signal() {
        assert!(TestCli::try_parse_from(["discover", "attributes", "--signal", "bogus"]).is_err());
    }

    /// Parses `discover attributes <extra>` aimed at a mock server.
    fn attributes_at(server: &mockito::ServerGuard, extra: &[&str]) -> AttributesArgs {
        let url = server.url();
        let mut argv = vec![
            "discover",
            "attributes",
            "--url",
            &url,
            "--api-key",
            "sk-test",
            "--tenant-id",
            "acme",
            "--dataset-id",
            "production",
        ];
        argv.extend_from_slice(extra);
        // `server.url()` is a temporary, so build the owned vector first.
        let argv: Vec<String> = argv.iter().map(|a| a.to_string()).collect();
        let DiscoverAction::Attributes(args) =
            TestCli::try_parse_from(argv).expect("parses").action
        else {
            panic!("expected Attributes");
        };
        args
    }

    /// A `describe: fields` answer for `traces` as the server lists it:
    /// declared intrinsics carry no level, keys the type authority has typed
    /// are `authority` with a level, a key typed at two levels is listed with
    /// source-aware qualifiers, and an untyped key is observed with no level.
    fn trace_fields() -> QueryIrResponse {
        serde_json::from_value(serde_json::json!({
            "result": "metadata",
            "window": { "start_ns": 0, "end_ns": 1 },
            "metadata": {
                "kind": "fields", "truncated": false,
                "cost": { "mode": "metadata", "window_scoped": false, "sampled": false, "approximate": false, "partial": false },
                "fields": [
                    { "name": "trace_id", "type": "string", "filterable": true, "origin": "declared" },
                    { "name": "duration", "type": "duration_ns", "filterable": true, "origin": "declared" },
                    { "name": "service.name", "type": "string", "filterable": true, "origin": "authority", "level": "resource" },
                    { "name": "http.route", "type": "string", "filterable": true, "origin": "authority", "level": "record" },
                    { "name": "resource.env", "type": "string", "filterable": true, "origin": "authority", "level": "resource" },
                    { "name": "span.env", "type": "string", "filterable": true, "origin": "authority", "level": "record" },
                    { "name": "untyped.key", "type": "string", "filterable": true, "origin": "observed" }
                ]
            }
        }))
        .expect("a describe answer")
    }

    fn kept(scope: TagScope) -> Vec<String> {
        let mut response = trace_fields();
        retain_scope(&mut response, scope, None);
        response
            .metadata
            .expect("metadata")
            .fields
            .into_iter()
            .map(|f| f.name)
            .collect()
    }

    #[test]
    fn scope_narrows_the_listed_trace_fields_by_level() {
        assert_eq!(kept(TagScope::Resource), ["service.name", "resource.env"]);
        assert_eq!(kept(TagScope::Span), ["http.route", "span.env"]);
        assert_eq!(kept(TagScope::Intrinsic), ["trace_id", "duration"]);
    }

    #[test]
    fn scope_limit_counts_scoped_fields_and_marks_truncation() {
        let mut response = trace_fields();
        retain_scope(&mut response, TagScope::Resource, Some(1));
        let metadata = response.metadata.expect("metadata");
        let names: Vec<_> = metadata.fields.into_iter().map(|f| f.name).collect();
        assert_eq!(names, ["service.name"]);
        assert!(metadata.truncated);
    }

    #[test]
    fn scope_qualifies_a_tag_with_its_level() {
        assert_eq!(
            TagScope::Resource.qualify("env").as_deref(),
            Some("resource.env")
        );
        assert_eq!(TagScope::Span.qualify("env").as_deref(), Some("span.env"));
        assert_eq!(TagScope::Intrinsic.qualify("duration"), None);
    }

    const DESCRIBE_EMPTY: &str = r#"{"result":"metadata","window":{"start_ns":0,"end_ns":1},"metadata":{"kind":"fields","fields":[],"truncated":false,"cost":{"mode":"metadata","window_scoped":false,"sampled":false,"approximate":false,"partial":false}}}"#;

    /// A mock of the Query IR endpoint that only matches a version-4 `describe`
    /// document with the given source, range and stage, sent with the tenant
    /// credential and dataset.
    async fn describe_mock(
        server: &mut mockito::ServerGuard,
        source: &str,
        range: (&str, &str),
        stage: serde_json::Value,
    ) -> mockito::Mock {
        server
            .mock("POST", "/api/v1/query")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .match_header("x-dataset-id", "production")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "irVersion": 4,
                "from": source,
                "range": { "from": range.0, "to": range.1 },
                "result": "metadata",
                "pipeline": [ { "describe": stage } ]
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(DESCRIBE_EMPTY)
            .create_async()
            .await
    }

    const HOUR: (&str, &str) = ("now-1h", "now");

    #[tokio::test]
    async fn attributes_without_a_tag_describe_the_signals_fields() {
        for signal in ["traces", "logs", "metrics", "profiles"] {
            let mut server = mockito::Server::new_async().await;
            let mock = describe_mock(
                &mut server,
                signal,
                HOUR,
                serde_json::json!({"target": "fields"}),
            )
            .await;
            attributes_at(&server, &["--signal", signal])
                .run()
                .await
                .expect("discover attributes succeeds");
            mock.assert_async().await;
        }
    }

    #[tokio::test]
    async fn attributes_with_a_tag_describe_values_and_only_sample_on_request() {
        let mut server = mockito::Server::new_async().await;
        let mock = describe_mock(
            &mut server,
            "traces",
            HOUR,
            serde_json::json!({"target": "values", "field": "service.name"}),
        )
        .await;
        attributes_at(&server, &["--tag", "service.name"])
            .run()
            .await
            .expect("discover attributes succeeds");
        mock.assert_async().await;

        let mut server = mockito::Server::new_async().await;
        let mock = describe_mock(
            &mut server,
            "traces",
            ("now-6h", "now"),
            serde_json::json!({"target": "values", "field": "service.name", "limit": 5, "sample": true}),
        )
        .await;
        attributes_at(
            &server,
            &[
                "--tag",
                "service.name",
                "--sample",
                "--limit",
                "5",
                "--from",
                "now-6h",
            ],
        )
        .run()
        .await
        .expect("discover attributes succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn attributes_scope_with_a_tag_describes_the_qualified_field() {
        let mut server = mockito::Server::new_async().await;
        let mock = describe_mock(
            &mut server,
            "traces",
            HOUR,
            serde_json::json!({"target": "values", "field": "span.env"}),
        )
        .await;
        attributes_at(&server, &["--tag", "env", "--scope", "span"])
            .run()
            .await
            .expect("discover attributes succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn discover_metrics_samples_the_metric_name_values_over_the_last_hour() {
        let mut server = mockito::Server::new_async().await;
        let mock = describe_mock(
            &mut server,
            "metrics",
            HOUR,
            serde_json::json!({"target": "values", "field": "metric.name", "limit": 50, "sample": true}),
        )
        .await;
        let url = server.url();
        let argv = [
            "discover",
            "metrics",
            "--url",
            &url,
            "--api-key",
            "sk-test",
            "--tenant-id",
            "acme",
            "--dataset-id",
            "production",
            "--limit",
            "50",
        ];
        let DiscoverAction::Metrics(args) = TestCli::try_parse_from(argv).expect("parses").action
        else {
            panic!("expected Metrics");
        };
        run_metrics(&args).await.expect("discover metrics succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn a_403_fails_the_command() {
        let mut server = mockito::Server::new_async().await;
        let _mock = server
            .mock("POST", "/api/v1/query")
            .with_status(403)
            .with_header("content-type", "application/json")
            .with_body(r#"{"error":"forbidden","errorType":"forbidden","status":"error"}"#)
            .create_async()
            .await;
        let err = attributes_at(&server, &[])
            .run()
            .await
            .expect_err("a 403 is an error");
        assert!(
            err.to_string().contains("discover attributes failed"),
            "{err}"
        );
    }

    #[tokio::test]
    async fn intrinsic_scope_with_a_tag_and_a_blank_tag_are_rejected() {
        // No request is issued for either.
        let server = mockito::Server::new_async().await;
        let err = attributes_at(&server, &["--tag", "kind", "--scope", "intrinsic"])
            .run()
            .await
            .expect_err("intrinsic has no level to qualify by");
        assert!(err.to_string().contains("intrinsic"), "{err}");
        let err = attributes_at(&server, &["--tag", "  "])
            .run()
            .await
            .expect_err("a blank tag names nothing");
        assert!(err.to_string().contains("--tag"), "{err}");
    }

    #[tokio::test]
    async fn scope_on_a_non_traces_signal_is_rejected() {
        // No request is issued: --scope has no meaning for logs/metrics.
        let server = mockito::Server::new_async().await;
        let err = attributes_at(&server, &["--signal", "logs", "--scope", "resource"])
            .run()
            .await
            .expect_err("scope on --signal logs must be rejected");
        assert!(err.to_string().contains("traces"), "got {err}");
    }
}
