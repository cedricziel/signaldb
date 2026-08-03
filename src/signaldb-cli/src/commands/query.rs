use std::io::Read;
use std::path::PathBuf;
use std::time::Duration;

use arrow::util::pretty::pretty_format_batches;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use clap::Subcommand;
use futures::{StreamExt, TryStreamExt};
use signaldb_sdk::types::{QueryIrRequest, QueryIrResponse};
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

#[derive(Subcommand)]
pub enum QueryAction {
    /// Execute a native Query IR document against SignalDB (`POST /api/v1/query`)
    Ir {
        /// The IR query document as a JSON string. Omit to read from `--file`
        /// or, failing that, stdin.
        query: Option<String>,

        /// Read the IR query document from a file instead of the argument/stdin.
        #[arg(long, short = 'f')]
        file: Option<PathBuf>,

        /// Router HTTP URL (the base for the generated SDK client).
        #[arg(long, env = "SIGNALDB_URL", default_value = "http://localhost:3000")]
        url: String,

        /// API key for authentication.
        #[arg(long, env = "SIGNALDB_API_KEY")]
        api_key: Option<String>,

        /// Tenant ID.
        #[arg(long, env = "SIGNALDB_TENANT_ID")]
        tenant_id: Option<String>,

        /// Dataset ID.
        #[arg(long, env = "SIGNALDB_DATASET_ID")]
        dataset_id: Option<String>,
    },
    /// Execute a SQL query against SignalDB
    Sql {
        /// SQL query to execute
        sql: String,

        /// Flight service URL
        #[arg(
            long,
            env = "SIGNALDB_FLIGHT_URL",
            default_value = "http://localhost:50053"
        )]
        flight_url: String,

        /// API key for authentication
        #[arg(long, env = "SIGNALDB_API_KEY")]
        api_key: Option<String>,

        /// Tenant ID
        #[arg(long, env = "SIGNALDB_TENANT_ID")]
        tenant_id: Option<String>,

        /// Dataset ID
        #[arg(long, env = "SIGNALDB_DATASET_ID")]
        dataset_id: Option<String>,

        /// Output format
        #[arg(long, default_value = "table")]
        format: OutputFormat,
    },
}

#[derive(Clone, Debug, clap::ValueEnum)]
pub enum OutputFormat {
    /// Pretty-printed table
    Table,
    /// Newline-delimited JSON (one object per row, pipe-friendly)
    Json,
    /// Comma-separated values with header row
    Csv,
}

impl QueryAction {
    pub async fn run(self) -> anyhow::Result<()> {
        match self {
            QueryAction::Ir {
                query,
                file,
                url,
                api_key,
                tenant_id,
                dataset_id,
            } => {
                let ir_text = read_ir_document(query, file.as_deref())?;
                let request: QueryIrRequest = serde_json::from_str(&ir_text)
                    .map_err(|e| anyhow::anyhow!("invalid IR document: {e}"))?;

                let response = submit_ir(
                    &url,
                    api_key.as_deref(),
                    tenant_id.as_deref(),
                    dataset_id.as_deref(),
                    request,
                )
                .await?;

                println!("{}", serde_json::to_string_pretty(&response)?);
                Ok(())
            }
            QueryAction::Sql {
                sql,
                flight_url,
                api_key,
                tenant_id,
                dataset_id,
                format,
            } => {
                let endpoint = Endpoint::from_shared(flight_url.clone())
                    .map_err(|e| anyhow::anyhow!("Invalid flight URL '{flight_url}': {e}"))?
                    .timeout(Duration::from_secs(30))
                    .connect_timeout(Duration::from_secs(10));

                let channel = endpoint.connect().await.map_err(|e| {
                    anyhow::anyhow!(
                        "Failed to connect to SignalDB at {flight_url}: {e}\n  \
                         Is the server running? Try: ./scripts/run-dev.sh"
                    )
                })?;
                let mut client = FlightServiceClient::new(channel);

                let ticket = Ticket {
                    ticket: sql.as_bytes().to_vec().into(),
                };
                let mut request = tonic::Request::new(ticket);

                if let Some(key) = &api_key {
                    let value = MetadataValue::try_from(format!("Bearer {key}"))?;
                    request.metadata_mut().insert("authorization", value);
                }
                if let Some(tenant) = &tenant_id {
                    let value = MetadataValue::try_from(tenant.as_str())?;
                    request.metadata_mut().insert("x-tenant-id", value);
                }
                if let Some(dataset) = &dataset_id {
                    let value = MetadataValue::try_from(dataset.as_str())?;
                    request.metadata_mut().insert("x-dataset-id", value);
                }

                let response = client
                    .do_get(request)
                    .await
                    .map_err(|status| anyhow::anyhow!("{}", format_flight_error(&status)))?;
                let flight_stream = response.into_inner();
                let mut batch_stream = FlightRecordBatchStream::new_from_flight_data(
                    flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
                );

                let mut batches = Vec::new();
                while let Some(result) = batch_stream.next().await {
                    batches.push(result?);
                }

                if batches.is_empty() {
                    eprintln!("No results.");
                    return Ok(());
                }

                let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();

                match format {
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
    }
}

/// Read the IR query document from the argument, a file, or stdin (in that
/// order of precedence).
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

/// Submit an IR document via the generated SDK client and return the enveloped
/// result. Uses only the generated client — no hand-written HTTP.
async fn submit_ir(
    url: &str,
    api_key: Option<&str>,
    tenant_id: Option<&str>,
    dataset_id: Option<&str>,
    request: QueryIrRequest,
) -> anyhow::Result<QueryIrResponse> {
    use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};

    // The generated SDK bakes absolute paths; the base is the router root.
    let base_url = url.trim_end_matches('/').to_string();
    let mut headers = HeaderMap::new();
    if let Some(key) = api_key {
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {key}"))?,
        );
    }
    if let Some(tenant) = tenant_id {
        headers.insert("x-tenant-id", HeaderValue::from_str(tenant)?);
    }
    if let Some(dataset) = dataset_id {
        headers.insert("x-dataset-id", HeaderValue::from_str(dataset)?);
    }
    let http = reqwest::Client::builder()
        .default_headers(headers)
        .timeout(Duration::from_secs(60))
        .build()?;
    let client = signaldb_sdk::Client::new_with_client(&base_url, http);

    let response = client
        .query_ir()
        .body(request)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("IR query failed: {e}"))?;
    Ok(response.into_inner())
}

fn format_flight_error(status: &tonic::Status) -> String {
    let msg = status.message().trim();
    match status.code() {
        tonic::Code::Unavailable => {
            format!("Service unavailable: {msg}\n  No query service is reachable.")
        }
        tonic::Code::Internal => {
            if let Some(detail) = msg.strip_prefix("Query execution failed: ") {
                format!("Query failed: {detail}")
            } else {
                format!("Server error: {msg}")
            }
        }
        tonic::Code::InvalidArgument => format!("Invalid query: {msg}"),
        tonic::Code::NotFound => format!("Not found: {msg}"),
        tonic::Code::Unauthenticated => {
            format!("Authentication required: {msg}\n  Use --api-key or set SIGNALDB_API_KEY.")
        }
        tonic::Code::PermissionDenied => {
            format!("Permission denied: {msg}\n  Check your API key and tenant access.")
        }
        _ => format!("{}: {msg}", status.code()),
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

    // Task 8.1 — the CLI reads an IR query and submits it via the generated SDK,
    // returning the enveloped result (no hand-written HTTP).
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
}
