//! Flight client for connecting to SignalDB router.

use arrow_flight::Ticket;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::ipc::reader::StreamReader;
use datafusion::arrow::record_batch::RecordBatch;
use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;
use tonic::transport::{Channel, Endpoint};

/// Error type for Flight client operations.
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum FlightClientError {
    #[error("Connection failed: {0}")]
    Connection(String),
    #[error("Query failed: {0}")]
    Query(String),
    #[error("Data decode failed: {0}")]
    Decode(String),
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

    /// Execute a query and return record batches.
    pub async fn query(
        &mut self,
        ticket: &str,
    ) -> Result<(Vec<RecordBatch>, Arc<Schema>), FlightClientError> {
        let ticket = Ticket {
            ticket: ticket.as_bytes().to_vec().into(),
        };

        let request = tonic::Request::new(ticket);
        let response = self
            .client
            .do_get(request)
            .await
            .map_err(|e| FlightClientError::Query(e.to_string()))?;

        let mut stream = response.into_inner();
        let mut schema: Option<Arc<Schema>> = None;
        let mut batches = Vec::new();
        let mut ipc_data = Vec::new();

        // Collect all flight data messages
        while let Some(flight_data) = stream
            .message()
            .await
            .map_err(|e| FlightClientError::Query(e.to_string()))?
        {
            // Schema message (header has data, body is empty)
            if !flight_data.data_header.is_empty() {
                // Accumulate the IPC message
                ipc_data.extend_from_slice(&flight_data.data_header);
            }
            // Data batch message (body has data)
            if !flight_data.data_body.is_empty() {
                ipc_data.extend_from_slice(&flight_data.data_body);
            }
        }

        // If we collected IPC data, try to decode it as a stream
        if !ipc_data.is_empty() {
            let cursor = Cursor::new(&ipc_data);
            match StreamReader::try_new(cursor, None) {
                Ok(reader) => {
                    schema = Some(reader.schema());
                    for batch in reader.flatten() {
                        batches.push(batch);
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to decode IPC stream: {e}");
                }
            }
        }

        let final_schema = schema.unwrap_or_else(|| Arc::new(Schema::empty()));

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
    fn test_error_display_connection() {
        let err = FlightClientError::Connection("test error".to_string());
        assert!(err.to_string().contains("Connection failed"));
        assert!(err.to_string().contains("test error"));
    }

    #[test]
    fn test_error_display_query() {
        let err = FlightClientError::Query("query failed".to_string());
        assert!(err.to_string().contains("Query failed"));
        assert!(err.to_string().contains("query failed"));
    }

    #[test]
    fn test_error_display_decode() {
        let err = FlightClientError::Decode("decode error".to_string());
        assert!(err.to_string().contains("Data decode failed"));
        assert!(err.to_string().contains("decode error"));
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

    #[test]
    fn test_client_debug() {
        // This test just verifies the Debug impl compiles
        // We can't actually create a client without a connection
        let _ = format!("{:?}", FlightClientError::Connection("test".to_string()));
    }
}
