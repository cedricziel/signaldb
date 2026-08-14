//! The `query` command: one command, one required language flag.
//!
//! `signaldb query --sql|--promql|--logql|--traceql '<q>'`, plus `--ir` for a
//! native Query IR JSON document (`POST /api/v1/query`). The language flag
//! determines the signal, the transport, and the output shape. All paths go
//! through `signaldb-sdk` — the CLI holds no Flight or HTTP client of its own
//! (see the `client-surface-parity` capability).

use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use arrow::util::pretty::pretty_format_batches;
use clap::Args;
use signaldb_sdk::types::{QueryIrRequest, QueryIrResponse};
use signaldb_sdk::{Client, QueryClient};

/// One query in one language. Exactly one language flag is required.
#[derive(Args)]
#[command(group(
    clap::ArgGroup::new("lang")
        .required(true)
        .multiple(false)
        .args(["sql", "promql", "logql", "traceql", "ir"])
))]
pub struct QueryArgs {
    /// The query string (SQL/PromQL/LogQL/TraceQL), or — with `--ir` — the IR
    /// JSON document. Omit with `--ir` to read the document from `--file` or
    /// stdin.
    query: Option<String>,

    /// Run as SQL over Arrow Flight; returns tabular rows.
    #[arg(long)]
    sql: bool,
    /// Run as PromQL; returns native Prometheus JSON.
    #[arg(long)]
    promql: bool,
    /// Run as LogQL; returns native Loki JSON.
    #[arg(long)]
    logql: bool,
    /// Run as TraceQL; returns native Tempo JSON.
    #[arg(long)]
    traceql: bool,
    /// Execute a native Query IR JSON document (`POST /api/v1/query`).
    #[arg(long)]
    ir: bool,

    /// With `--ir`: read the IR document from a file instead of the
    /// argument/stdin.
    #[arg(long, short = 'f', requires = "ir")]
    file: Option<PathBuf>,

    /// Router base URL (used by `--promql`/`--logql`/`--traceql`/`--ir`).
    #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
    url: String,
    /// Flight endpoint URL (used by `--sql`).
    #[arg(
        long,
        env = "SIGNALDB_FLIGHT_URL",
        default_value = "http://localhost:50053"
    )]
    flight_url: String,
    /// API key for authentication.
    #[arg(long, env = "SIGNALDB_API_KEY")]
    api_key: Option<String>,
    /// Tenant ID.
    #[arg(long, env = "SIGNALDB_TENANT_ID")]
    tenant_id: Option<String>,
    /// Dataset ID.
    #[arg(long, env = "SIGNALDB_DATASET_ID")]
    dataset_id: Option<String>,
    /// Output format for `--sql` (the native JSON languages ignore this).
    #[arg(long, default_value = "table")]
    format: OutputFormat,
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    /// Pretty-printed table.
    Table,
    /// Newline-delimited JSON (one object per row, pipe-friendly).
    Json,
    /// Comma-separated values with header row.
    Csv,
}

impl QueryArgs {
    pub async fn run(self) -> anyhow::Result<()> {
        // Query IR takes a JSON document (argument, --file, or stdin) rather
        // than a query string, so it is handled before the string-language path.
        if self.ir {
            return self.run_ir().await;
        }

        let query = self.query.clone().ok_or_else(|| {
            anyhow::anyhow!("a query string is required (pass it as the positional argument)")
        })?;

        if self.sql {
            self.run_sql(&query).await
        } else if self.promql {
            let v = self
                .http_client()?
                .promql_query()
                .query(&query)
                .send()
                .await;
            print_json_response(v.map(|r| r.into_inner()), "promql_query")
        } else if self.logql {
            let v = self.http_client()?.logql_query().query(&query).send().await;
            print_json_response(v.map(|r| r.into_inner()), "logql_query")
        } else if self.traceql {
            let v = self.http_client()?.search().q(&query).send().await;
            print_json_response(v.map(|r| r.into_inner()), "tempo search")
        } else {
            // clap's required ArgGroup guarantees exactly one flag is set.
            unreachable!("a language flag is required")
        }
    }

    /// Build the SDK HTTP client, carrying bearer/tenant/dataset on every
    /// request so all generated calls are authenticated.
    fn http_client(&self) -> anyhow::Result<Client> {
        build_http_client(
            &self.url,
            self.api_key.as_deref(),
            self.tenant_id.as_deref(),
            self.dataset_id.as_deref(),
        )
    }

    async fn run_ir(&self) -> anyhow::Result<()> {
        let ir_text = read_ir_document(self.query.clone(), self.file.as_deref())?;
        let request: QueryIrRequest = serde_json::from_str(&ir_text)
            .map_err(|e| anyhow::anyhow!("invalid IR document: {e}"))?;
        let response = submit_ir(
            &self.url,
            self.api_key.as_deref(),
            self.tenant_id.as_deref(),
            self.dataset_id.as_deref(),
            request,
        )
        .await?;
        println!("{}", serde_json::to_string_pretty(&response)?);
        Ok(())
    }

    async fn run_sql(&self, query: &str) -> anyhow::Result<()> {
        let mut client = QueryClient::new(&self.flight_url);
        if let Some(key) = &self.api_key {
            client = client.with_api_key(key);
        }
        if let Some(tenant) = &self.tenant_id {
            client = client.with_tenant(tenant);
        }
        if let Some(dataset) = &self.dataset_id {
            client = client.with_dataset(dataset);
        }

        let batches = client.sql(query).await?;
        if batches.is_empty() || batches.iter().all(|b| b.num_rows() == 0) {
            eprintln!("No results.");
            return Ok(());
        }
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

        match self.format {
            OutputFormat::Table => {
                let formatted = pretty_format_batches(&batches)?;
                println!("{formatted}");
                eprintln!("{total_rows} row(s) returned.");
            }
            OutputFormat::Json => {
                let mut writer = arrow::json::LineDelimitedWriter::new(std::io::stdout());
                for batch in &batches {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }
            OutputFormat::Csv => {
                let mut writer = arrow::csv::WriterBuilder::new()
                    .with_header(true)
                    .build(std::io::stdout());
                for batch in &batches {
                    writer.write(batch)?;
                }
            }
        }
        Ok(())
    }
}

/// Build an SDK HTTP client that carries bearer/tenant/dataset headers on every
/// request, mirroring how the MCP server constructs it.
///
/// `pub(crate)` so the `discover` command (tenant-authenticated like `query`)
/// builds its client the same way instead of duplicating header setup.
pub(crate) fn build_http_client(
    url: &str,
    api_key: Option<&str>,
    tenant_id: Option<&str>,
    dataset_id: Option<&str>,
) -> anyhow::Result<Client> {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
    let base_url = url.trim_end_matches('/').to_string();
    let mut headers = HeaderMap::new();
    if let Some(key) = api_key {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {key}"))
                .context("invalid API key for Authorization header")?,
        );
    }
    if let Some(tenant) = tenant_id {
        headers.insert(
            "x-tenant-id",
            HeaderValue::from_str(tenant).context("invalid tenant id")?,
        );
    }
    if let Some(dataset) = dataset_id {
        headers.insert(
            "x-dataset-id",
            HeaderValue::from_str(dataset).context("invalid dataset id")?,
        );
    }
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(60))
        .build()
        .context("building HTTP client")?;
    Ok(Client::new_with_client(&base_url, http))
}

/// Read the IR document from the argument, else a file, else stdin.
fn read_ir_document(
    query: Option<String>,
    file: Option<&std::path::Path>,
) -> anyhow::Result<String> {
    if let Some(q) = query {
        return Ok(q);
    }
    if let Some(path) = file {
        return std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("failed to read IR document from {}: {e}", path.display())
        });
    }
    let mut buf = String::new();
    std::io::stdin()
        .read_to_string(&mut buf)
        .map_err(|e| anyhow::anyhow!("failed to read IR document from stdin: {e}"))?;
    if buf.trim().is_empty() {
        anyhow::bail!("no IR document provided (pass it as an argument, --file, or on stdin)");
    }
    Ok(buf)
}

/// Submit a Query IR request via the generated SDK and return the envelope.
async fn submit_ir(
    url: &str,
    api_key: Option<&str>,
    tenant_id: Option<&str>,
    dataset_id: Option<&str>,
    request: QueryIrRequest,
) -> anyhow::Result<QueryIrResponse> {
    let client = build_http_client(url, api_key, tenant_id, dataset_id)?;
    let response = client
        .query_ir()
        .body(request)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("IR query failed: {e}"))?;
    Ok(response.into_inner())
}

/// Print a native-JSON query response to stdout, or turn an SDK error into an
/// `anyhow` error with context so the process exits non-zero with a diagnostic
/// on stderr.
pub(crate) fn print_json_response<T, E>(result: Result<T, E>, what: &str) -> anyhow::Result<()>
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

    fn sample_ir() -> &'static str {
        r#"{
            "irVersion": 1,
            "from": "logs",
            "range": { "from": "now-1h", "to": "now" },
            "result": "rows",
            "fields": ["service_name"],
            "pipeline": [
                { "where": { "field": "service.name", "op": "eq", "value": "api" } }
            ]
        }"#
    }

    #[test]
    fn read_ir_document_prefers_argument_then_file() {
        // Argument wins.
        assert_eq!(
            read_ir_document(Some("{\"x\":1}".to_string()), None).unwrap(),
            "{\"x\":1}"
        );
        // Falls back to a file.
        let mut path = std::env::temp_dir();
        path.push("signaldb-cli-ir-test.json");
        std::fs::write(&path, sample_ir()).unwrap();
        let text = read_ir_document(None, Some(path.as_path())).unwrap();
        assert!(text.contains("\"from\": \"logs\""));
        let _ = std::fs::remove_file(&path);
    }

    // The CLI reads an IR query and submits it via the generated SDK, returning
    // the enveloped result (no hand-written HTTP).
    #[tokio::test]
    async fn ir_query_submits_via_sdk_and_returns_envelope() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/query")
            .match_header("authorization", "Bearer sk-test")
            .match_header("x-tenant-id", "acme")
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{
                    "result": "rows",
                    "window": { "start_ns": 0, "end_ns": 3600000000000 },
                    "columns": [{ "name": "service_name", "type": "string" }],
                    "rows": [["api"], ["api"]]
                }"#,
            )
            .create_async()
            .await;

        let request: QueryIrRequest = serde_json::from_str(sample_ir()).unwrap();
        let response = submit_ir(&server.url(), Some("sk-test"), Some("acme"), None, request)
            .await
            .expect("IR query succeeds");

        mock.assert_async().await;
        assert_eq!(response.result, "rows");
        assert_eq!(response.rows.len(), 2);
        assert_eq!(response.window.end_ns, 3_600_000_000_000);
    }

    #[tokio::test]
    async fn ir_query_accepts_v2_heatmap_without_a_dedicated_command() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/query")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "irVersion": 2, "result": "heatmap"
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(r#"{"result":"heatmap","window":{"start_ns":0,"end_ns":60},"heatmap":{"x":{"step_ns":60,"align":"epoch"},"y":{"of":"duration","type":"duration_ns","bounds":[1],"overflow":true},"value":"count","cells":[{"time_bucket_ns":0,"duration_bucket":0,"count":2}]}}"#)
            .create_async().await;
        let request: QueryIrRequest = serde_json::from_value(serde_json::json!({
            "irVersion": 2, "from": "traces", "range": { "from": "0", "to": "60" },
            "result": "heatmap", "pipeline": [{ "heatmap": {
                "x": { "step": "1m", "align": "epoch" },
                "y": { "of": "duration", "bounds": ["1ms"], "overflow": true },
                "value": { "fn": "count", "as": "count" }
            }}]
        }))
        .unwrap();
        let response = submit_ir(&server.url(), None, None, None, request)
            .await
            .unwrap();
        mock.assert_async().await;
        assert_eq!(response.result, "heatmap");
        assert_eq!(response.heatmap.expect("heatmap envelope").value, "count");
    }

    /// profile-payload-access task 5.2 — the generic `query-ir` command
    /// forwards a `flamegraph` envelope document unchanged; no dedicated CLI
    /// flag or subcommand is needed since it already forwards an arbitrary
    /// IR document (mirroring the heatmap and profiles precedents above).
    #[tokio::test]
    async fn ir_query_accepts_the_flamegraph_envelope_without_a_dedicated_command() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/query")
            .match_body(mockito::Matcher::PartialJson(serde_json::json!({
                "from": "profiles", "result": "flamegraph"
            })))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"result":"flamegraph","window":{"start_ns":0,"end_ns":60},"flamegraph":{"names":["main"],"levels":[[0,10,10,0]],"total":10,"max_self":10,"truncated":false}}"#,
            )
            .create_async()
            .await;
        let request: QueryIrRequest = serde_json::from_value(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "0", "to": "60" },
            "result": "flamegraph",
            "pipeline": [{ "where": { "field": "profile.id", "op": "eq", "value": "abc" } }]
        }))
        .unwrap();
        let response = submit_ir(&server.url(), None, None, None, request)
            .await
            .unwrap();
        mock.assert_async().await;
        assert_eq!(response.result, "flamegraph");
        assert_eq!(response.flamegraph.expect("flamegraph envelope").total, 10);
    }

    #[tokio::test]
    async fn ir_query_forwards_profiles_without_a_dedicated_transport() {
        let mut server = mockito::Server::new_async().await;
        let mock = server
            .mock("POST", "/api/v1/query")
            .match_body(mockito::Matcher::PartialJson(
                serde_json::json!({ "from": "profiles" }),
            ))
            .with_status(200)
            .with_header("content-type", "application/json")
            .with_body(
                r#"{"result":"rows","window":{"start_ns":0,"end_ns":60},"columns":[],"rows":[]}"#,
            )
            .create_async()
            .await;
        let request: QueryIrRequest = serde_json::from_value(serde_json::json!({
            "irVersion": 1, "from": "profiles", "range": { "from": "0", "to": "60" },
            "result": "rows", "pipeline": []
        }))
        .unwrap();
        submit_ir(&server.url(), None, None, None, request)
            .await
            .unwrap();
        mock.assert_async().await;
    }

    fn sql_args(flight_url: &str, query: Option<&str>) -> QueryArgs {
        QueryArgs {
            query: query.map(str::to_string),
            sql: true,
            promql: false,
            logql: false,
            traceql: false,
            ir: false,
            file: None,
            url: "http://localhost:3000".to_string(),
            flight_url: flight_url.to_string(),
            api_key: None,
            tenant_id: None,
            dataset_id: None,
            format: OutputFormat::Table,
        }
    }

    #[tokio::test]
    async fn sql_against_unreachable_endpoint_errors() {
        // A runtime failure must surface as an error so the process exits
        // non-zero (main maps Err → exit 1). Port 1 is not listenable.
        let args = sql_args("http://127.0.0.1:1", Some("SELECT 1"));
        assert!(args.run().await.is_err());
    }

    #[tokio::test]
    async fn string_language_requires_a_query() {
        // --promql with no positional and no --ir: run() rejects at runtime.
        let mut args = sql_args("http://127.0.0.1:1", None);
        args.sql = false;
        args.promql = true;
        assert!(args.run().await.is_err());
    }
}
