//! The `discover` command: attribute/label and metric-name discovery via
//! `signaldb-sdk`, mirroring the MCP server's `discover_attributes` /
//! `discover_metrics` tools (see the `client-surface-parity` capability).
//!
//! `signaldb-cli discover attributes --signal traces|logs|metrics [--tag NAME]`
//! lists tag/label names, or the known values for one name when `--tag` is
//! given. `signaldb-cli discover metrics` lists distinct metric names (the
//! values of Prometheus's reserved `__name__` label).
//!
//! `--scope resource|span|intrinsic` (traces only) restricts tag discovery
//! to one scope, routing through the Tempo v2 discovery endpoints
//! (`search_tags_v2`/`search_tag_values_v2`) instead of v1.

use clap::{Args, Subcommand, ValueEnum};

use super::query::{build_http_client, print_json_response};

/// Which signal to discover attributes for.
#[derive(Clone, Debug, ValueEnum)]
pub enum Signal {
    /// Tempo trace attributes (tags).
    Traces,
    /// Loki log labels.
    Logs,
    /// Prometheus metric labels.
    Metrics,
}

/// Trace tag scope (Tempo v2 discovery only). `--scope` is only valid with
/// `--signal traces`.
#[derive(Clone, Copy, Debug, ValueEnum)]
pub enum TagScope {
    Resource,
    Span,
    Intrinsic,
}

impl TagScope {
    fn into_sdk(self) -> signaldb_sdk::types::TagScope {
        match self {
            TagScope::Resource => signaldb_sdk::types::TagScope::Resource,
            TagScope::Span => signaldb_sdk::types::TagScope::Span,
            TagScope::Intrinsic => signaldb_sdk::types::TagScope::Intrinsic,
        }
    }

    /// The v2 scoped tag name (`resource.<tag>`, `span.<tag>`, or the bare
    /// `<tag>` for `intrinsic`) that `search_tag_values_v2` expects.
    fn scoped_tag_name(self, tag: &str) -> String {
        match self {
            TagScope::Resource => format!("resource.{tag}"),
            TagScope::Span => format!("span.{tag}"),
            TagScope::Intrinsic => tag.to_string(),
        }
    }
}

#[derive(Subcommand)]
pub enum DiscoverAction {
    /// List attribute/label names for a signal, or the values for one name
    Attributes(AttributesArgs),
    /// List distinct metric names
    Metrics(ConnectArgs),
}

#[derive(Args)]
pub struct AttributesArgs {
    /// Which signal to discover attributes for
    #[arg(long, value_enum, default_value = "traces")]
    signal: Signal,
    /// List the known values for this attribute/label name, instead of names
    #[arg(long)]
    tag: Option<String>,
    /// Restrict trace tag discovery to one scope, routing through the Tempo
    /// v2 discovery endpoints instead of v1. Only valid with `--signal traces`.
    #[arg(long, value_enum)]
    scope: Option<TagScope>,
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
            DiscoverAction::Metrics(connect) => run_metrics(&connect).await,
        }
    }
}

impl AttributesArgs {
    async fn run(self) -> anyhow::Result<()> {
        if self.scope.is_some() && !matches!(self.signal, Signal::Traces) {
            anyhow::bail!("--scope is only valid with --signal traces");
        }
        let client = self.connect.build_client()?;
        // Each backend returns its own response type (Tempo's tag endpoints are
        // typed; Loki/Prometheus's are permissive JSON), so each arm maps and
        // prints its own result rather than unifying into one match expression.
        match (self.signal, self.tag, self.scope) {
            (Signal::Traces, Some(tag), Some(scope)) => {
                let result = client
                    .search_tag_values_v2()
                    .tag_name(scope.scoped_tag_name(&tag))
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Traces, Some(tag), None) => {
                let result = client
                    .search_tag_values()
                    .tag_name(tag)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Traces, None, Some(scope)) => {
                let result = client
                    .search_tags_v2()
                    .scope(scope.into_sdk())
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Traces, None, None) => {
                let result = client.search_tags().send().await.map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Logs, Some(name), _) => {
                let result = client
                    .logql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Logs, None, _) => {
                let result = client.logql_labels().send().await.map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Metrics, Some(name), _) => {
                let result = client
                    .promql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Metrics, None, _) => {
                let result = client.promql_labels().send().await.map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
        }
    }
}

async fn run_metrics(connect: &ConnectArgs) -> anyhow::Result<()> {
    let client = connect.build_client()?;
    let result = client
        .promql_label_values()
        .name("__name__")
        .send()
        .await
        .map(|r| r.into_inner());
    print_json_response(result, "discover metrics")
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

    #[tokio::test]
    async fn attributes_against_unreachable_endpoint_errors() {
        // A runtime failure must surface as an error so the process exits
        // non-zero. Port 1 is not listenable.
        let args = AttributesArgs {
            signal: Signal::Traces,
            tag: None,
            scope: None,
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: None,
                tenant_id: None,
                dataset_id: None,
            },
        };
        assert!(args.run().await.is_err());
    }

    #[tokio::test]
    async fn discover_attributes_dispatches_per_signal_via_sdk() {
        let mut server = mockito::Server::new_async().await;
        let tags_mock = server
            .mock("GET", "/tempo/api/search/tag/service.name/values")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tagValues":["api"]}"#)
            .create_async()
            .await;

        let args = AttributesArgs {
            signal: Signal::Traces,
            tag: Some("service.name".to_string()),
            scope: None,
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        args.run().await.expect("discover attributes succeeds");
        tags_mock.assert_async().await;
    }

    #[tokio::test]
    async fn discover_metrics_queries_prometheus_name_label() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/prometheus/api/v1/label/__name__/values")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"status":"success","data":["up","http_requests_total"]}"#)
            .create_async()
            .await;

        let connect = ConnectArgs {
            url: server.url(),
            api_key: Some("sk-test".to_string()),
            tenant_id: Some("acme".to_string()),
            dataset_id: None,
        };
        run_metrics(&connect)
            .await
            .expect("discover metrics succeeds");
        mock.assert_async().await;
    }

    #[test]
    fn attributes_accepts_scope() {
        let cli = TestCli::try_parse_from([
            "discover",
            "attributes",
            "--signal",
            "traces",
            "--scope",
            "resource",
        ])
        .expect("parses");
        let DiscoverAction::Attributes(args) = cli.action else {
            panic!("expected Attributes");
        };
        assert!(matches!(args.scope, Some(TagScope::Resource)));
    }

    #[test]
    fn rejects_unknown_scope() {
        assert!(TestCli::try_parse_from(["discover", "attributes", "--scope", "bogus"]).is_err());
    }

    #[tokio::test]
    async fn scope_without_tag_dispatches_to_v2_tags() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("GET", "/tempo/api/v2/search/tags")
            .match_query(mockito::Matcher::UrlEncoded(
                "scope".to_string(),
                "resource".to_string(),
            ))
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"scopes":[{"scope":"resource","tags":["service.name"]}]}"#)
            .create_async()
            .await;

        let args = AttributesArgs {
            signal: Signal::Traces,
            tag: None,
            scope: Some(TagScope::Resource),
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        args.run().await.expect("discover attributes succeeds");
        mock.assert_async().await;
    }

    #[tokio::test]
    async fn scope_with_tag_dispatches_to_v2_tag_values_with_scoped_name() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock(
                "GET",
                "/tempo/api/v2/search/tag/resource.service.name/values",
            )
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"tagValues":[{"tag":"resource.service.name","value":"checkout"}]}"#)
            .create_async()
            .await;

        let args = AttributesArgs {
            signal: Signal::Traces,
            tag: Some("service.name".to_string()),
            scope: Some(TagScope::Resource),
            connect: ConnectArgs {
                url: server.url(),
                api_key: Some("sk-test".to_string()),
                tenant_id: Some("acme".to_string()),
                dataset_id: None,
            },
        };
        args.run().await.expect("discover attributes succeeds");
        mock.assert_async().await;
    }

    #[test]
    fn intrinsic_scope_produces_the_bare_tag_name() {
        assert_eq!(TagScope::Intrinsic.scoped_tag_name("status"), "status");
        assert_eq!(
            TagScope::Resource.scoped_tag_name("service.name"),
            "resource.service.name"
        );
        assert_eq!(
            TagScope::Span.scoped_tag_name("http.route"),
            "span.http.route"
        );
    }

    #[tokio::test]
    async fn scope_on_a_non_traces_signal_is_rejected() {
        // No mock server needed: the command must reject before issuing any
        // request, since --scope (Tempo v2) has no meaning for logs/metrics.
        let args = AttributesArgs {
            signal: Signal::Logs,
            tag: None,
            scope: Some(TagScope::Resource),
            connect: ConnectArgs {
                url: "http://127.0.0.1:1".to_string(),
                api_key: None,
                tenant_id: None,
                dataset_id: None,
            },
        };
        let err = args
            .run()
            .await
            .expect_err("scope on --signal logs must be rejected");
        assert!(err.to_string().contains("traces"), "got {err}");
    }
}
