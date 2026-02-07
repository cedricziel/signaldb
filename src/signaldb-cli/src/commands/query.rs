use std::time::Duration;

use arrow::util::pretty::pretty_format_batches;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use clap::Subcommand;
use futures::{StreamExt, TryStreamExt};
use tonic::metadata::MetadataValue;
use tonic::transport::Endpoint;

#[derive(Subcommand)]
pub enum QueryAction {
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
