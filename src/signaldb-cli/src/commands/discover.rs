//! The `discover` command: attribute/label and metric-name discovery via
//! `signaldb-sdk`, mirroring the MCP server's `discover_attributes` /
//! `discover_metrics` tools (see the `client-surface-parity` capability).
//!
//! `signaldb-cli discover attributes --signal traces|logs|metrics [--tag NAME]`
//! lists tag/label names, or the known values for one name when `--tag` is
//! given. `signaldb-cli discover metrics` lists distinct metric names (the
//! values of Prometheus's reserved `__name__` label).

use clap::{Args, Subcommand, ValueEnum};

use super::query::build_http_client;

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
    #[command(flatten)]
    connect: ConnectArgs,
}

#[derive(Args)]
pub struct ConnectArgs {
    /// SignalDB router base URL
    #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
    url: String,
    /// API key for authentication
    #[arg(long, env = "SIGNALDB_API_KEY")]
    api_key: Option<String>,
    /// Tenant ID
    #[arg(long, env = "SIGNALDB_TENANT_ID")]
    tenant_id: Option<String>,
    /// Dataset ID
    #[arg(long, env = "SIGNALDB_DATASET_ID")]
    dataset_id: Option<String>,
}

impl ConnectArgs {
    fn build_client(&self) -> anyhow::Result<signaldb_sdk::Client> {
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
        let client = self.connect.build_client()?;
        // Each backend returns its own response type (Tempo's tag endpoints are
        // typed; Loki/Prometheus's are permissive JSON), so each arm maps and
        // prints its own result rather than unifying into one match expression.
        match (self.signal, self.tag) {
            (Signal::Traces, Some(tag)) => {
                let result = client
                    .search_tag_values()
                    .tag_name(tag)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Traces, None) => {
                let result = client.search_tags().send().await.map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Logs, Some(name)) => {
                let result = client
                    .logql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Logs, None) => {
                let result = client.logql_labels().send().await.map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Metrics, Some(name)) => {
                let result = client
                    .promql_label_values()
                    .name(name)
                    .send()
                    .await
                    .map(|r| r.into_inner());
                print_json_response(result, "discover attributes")
            }
            (Signal::Metrics, None) => {
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

/// Print a JSON response to stdout, or turn an SDK error into an `anyhow`
/// error with context so the process exits non-zero with a diagnostic on
/// stderr (mirrors `query::print_json_response`).
fn print_json_response<T, E>(result: Result<T, E>, what: &str) -> anyhow::Result<()>
where
    T: serde::Serialize,
    E: std::fmt::Display,
{
    match result {
        Ok(value) => {
            println!("{}", serde_json::to_string_pretty(&value)?);
            Ok(())
        }
        Err(e) => anyhow::bail!("{what} failed: {e}"),
    }
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
}
