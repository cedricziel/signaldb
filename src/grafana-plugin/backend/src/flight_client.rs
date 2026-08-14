//! Flight client for connecting to SignalDB router.

use arrow::datatypes::Schema;
use arrow::record_batch::RecordBatch;
use arrow_flight::Ticket;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::flight_service_client::FlightServiceClient;
use futures::TryStreamExt;
use futures::stream::StreamExt;
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};

/// Authentication context for Flight requests.
#[derive(Debug, Clone, Default)]
pub struct AuthContext {
    pub api_key: Option<String>,
    pub tenant_id: Option<String>,
    pub dataset_id: Option<String>,
}

/// Error type for Flight client operations.
#[derive(Debug, thiserror::Error)]
pub enum FlightClientError {
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Query failed: {0}")]
    Query(String),
    #[error("Data decode failed: {0}")]
    Decode(String),
}

/// Insert a single metadata header on a Flight request, logging (rather than
/// failing the request) if the value isn't valid ASCII metadata.
fn insert_header(request: &mut tonic::Request<Ticket>, name: &'static str, value: String) {
    match MetadataValue::try_from(value) {
        Ok(value) => {
            request.metadata_mut().insert(name, value);
        }
        Err(e) => {
            tracing::warn!("Failed to set {name} header: {e}");
        }
    }
}

/// Flight client for SignalDB.
#[derive(Debug)]
pub struct SignalDBFlightClient {
    client: FlightServiceClient<Channel>,
}

impl SignalDBFlightClient {
    /// Connect to a SignalDB Flight service.
    pub async fn connect(url: &str, timeout_secs: u32) -> Result<Self, FlightClientError> {
        let endpoint = Endpoint::from_shared(url.to_string())
            .map_err(|e| FlightClientError::Connection(e.to_string()))?
            .timeout(Duration::from_secs(timeout_secs as u64))
            .connect_timeout(Duration::from_secs(10));

        let channel = endpoint
            .connect()
            .await
            .map_err(|e| FlightClientError::Connection(e.to_string()))?;

        let client = FlightServiceClient::new(channel);

        tracing::info!("Connected to SignalDB Flight service at {url}");

        Ok(Self { client })
    }

    /// Execute a query with optional authentication headers and return record batches.
    pub async fn query_with_auth(
        &mut self,
        ticket: &str,
        auth: Option<&AuthContext>,
    ) -> Result<(Vec<RecordBatch>, Arc<Schema>), FlightClientError> {
        let ticket = Ticket {
            ticket: ticket.as_bytes().to_vec().into(),
        };

        let mut request = tonic::Request::new(ticket);

        // Add authentication headers if provided
        if let Some(ctx) = auth {
            if let Some(key) = &ctx.api_key {
                insert_header(&mut request, "authorization", format!("Bearer {key}"));
            }
            if let Some(tenant) = &ctx.tenant_id {
                insert_header(&mut request, "x-tenant-id", tenant.clone());
            }
            if let Some(dataset) = &ctx.dataset_id {
                insert_header(&mut request, "x-dataset-id", dataset.clone());
            }
        }

        let response = self
            .client
            .do_get(request)
            .await
            .map_err(|e| FlightClientError::Query(e.to_string()))?;

        // Use FlightRecordBatchStream to properly decode FlightData messages
        let flight_stream = response.into_inner();
        let mut record_batch_stream = FlightRecordBatchStream::new_from_flight_data(
            flight_stream.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e))),
        );

        let mut batches = Vec::new();
        let mut schema: Option<Arc<Schema>> = None;

        // Iterate over the stream, handling errors explicitly
        while let Some(result) = record_batch_stream.next().await {
            match result {
                Ok(batch) => {
                    // Capture schema from first batch
                    if schema.is_none() {
                        schema = Some(batch.schema());
                    }
                    batches.push(batch);
                }
                Err(e) => {
                    return Err(FlightClientError::Decode(format!(
                        "Failed to decode record batch: {e}"
                    )));
                }
            }
        }

        // If we have batches but no schema captured, get it from the stream
        let final_schema = schema
            .or_else(|| record_batch_stream.schema().cloned())
            .unwrap_or_else(|| Arc::new(Schema::empty()));

        tracing::debug!(
            "Query returned {} batches with {} total rows",
            batches.len(),
            batches.iter().map(|b| b.num_rows()).sum::<usize>()
        );

        Ok((batches, final_schema))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_display_includes_variant_prefix_and_inner_message() {
        assert_eq!(
            FlightClientError::Connection("test error".to_string()).to_string(),
            "Connection failed: test error"
        );
        assert_eq!(
            FlightClientError::Query("query failed".to_string()).to_string(),
            "Query failed: query failed"
        );
        assert_eq!(
            FlightClientError::Decode("decode error".to_string()).to_string(),
            "Data decode failed: decode error"
        );
    }

    #[test]
    fn test_error_debug() {
        let err = FlightClientError::Connection("test".to_string());
        let debug_str = format!("{err:?}");
        assert!(debug_str.contains("Connection"));
    }

    #[tokio::test]
    async fn test_connect_invalid_url() {
        // Test with an invalid URL
        let result = SignalDBFlightClient::connect("not-a-valid-url", 5).await;
        assert!(result.is_err());
        if let Err(FlightClientError::Connection(msg)) = result {
            assert!(!msg.is_empty());
        } else {
            panic!("Expected Connection error");
        }
    }

    #[tokio::test]
    async fn test_connect_unreachable_host() {
        // Test with a valid URL format but unreachable host
        // Use a non-routable IP to ensure fast failure
        let result = SignalDBFlightClient::connect("http://10.255.255.1:50053", 1).await;
        assert!(result.is_err());
        if let Err(FlightClientError::Connection(_)) = result {
            // Expected
        } else {
            panic!("Expected Connection error");
        }
    }
}
