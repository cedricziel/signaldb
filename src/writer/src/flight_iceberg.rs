use crate::processor::WalProcessor;
use crate::schema_transform::{
    FlightMetadata, determine_wal_operation, extract_flight_metadata, transform_trace_v1_to_v2,
};
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
};
use bytes::Bytes;
use common::config::Configuration;
use common::flight::schema::FlightSchemas;
use common::wal::{Wal, WalOperation, record_batch_to_bytes};
use datafusion::arrow::datatypes::SchemaRef;
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStore;
use std::sync::Arc;
use tokio::sync::Mutex;
use tonic::{Request, Response, Status};

/// Enhanced Flight service that uses Iceberg table writer instead of direct Parquet writes
/// This demonstrates the integration of the new Iceberg-based processor
pub struct IcebergWriterFlightService {
    processor: Arc<Mutex<WalProcessor>>,
    wal: Arc<Wal>,
    #[allow(dead_code)]
    schemas: FlightSchemas,
}

impl IcebergWriterFlightService {
    /// Create a new IcebergWriterFlightService with Iceberg-based processing
    pub fn new(config: Configuration, object_store: Arc<dyn ObjectStore>, wal: Arc<Wal>) -> Self {
        let processor = WalProcessor::new(wal.clone(), config, object_store);

        Self {
            processor: Arc::new(Mutex::new(processor)),
            wal,
            schemas: FlightSchemas::new(),
        }
    }

    /// Start the background WAL processing loop
    pub async fn start_background_processing(&self) -> Result<(), anyhow::Error> {
        let processor = self.processor.clone();

        tokio::spawn(async move {
            loop {
                let mut processor_guard = processor.lock().await;
                if let Err(e) = processor_guard.process_pending_entries().await {
                    log::error!("Background WAL processing error: {e}");
                }
                drop(processor_guard);

                // Process every 5 seconds
                tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            }
        });

        Ok(())
    }
}

#[tonic::async_trait]
impl FlightService for IcebergWriterFlightService {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _request: Request<tonic::Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        let resp = HandshakeResponse {
            protocol_version: 0,
            payload: Bytes::new(),
        };
        let out = stream::once(async move { Ok(resp) }).boxed();
        Ok(Response::new(out))
    }

    type ListFlightsStream = BoxStream<'static, Result<arrow_flight::FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _request: Request<arrow_flight::Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        // No predefined flights
        let out = stream::empty().boxed();
        Ok(Response::new(out))
    }

    async fn get_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::FlightInfo>, Status> {
        Err(Status::unimplemented("get_flight_info not supported"))
    }

    async fn get_schema(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("get_schema not supported"))
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        let mut inbound = request.into_inner();
        let mut data_vec = Vec::new();
        let mut flight_metadata: Option<FlightMetadata> = None;
        let mut schema_ref: Option<SchemaRef> = None;

        while let Some(msg) = inbound.next().await {
            let d = msg.map_err(|e| Status::internal(e.to_string()))?;

            // Extract full metadata from the first FlightData message (which contains metadata)
            if flight_metadata.is_none() && !d.app_metadata.is_empty() {
                match extract_flight_metadata(&d.app_metadata) {
                    Ok(metadata) => {
                        log::info!(
                            "Received data - schema: {}, signal: {:?}, target: {:?}",
                            metadata.schema_version,
                            metadata.signal_type,
                            metadata.target_table
                        );
                        flight_metadata = Some(metadata);
                    }
                    Err(e) => {
                        log::warn!("Failed to extract metadata: {e}, using defaults");
                        flight_metadata = Some(FlightMetadata {
                            schema_version: "v1".to_string(),
                            signal_type: Some("traces".to_string()),
                            target_table: None,
                        });
                    }
                }
            }

            data_vec.push(d);
        }

        if data_vec.is_empty() {
            return Err(Status::invalid_argument("No FlightData received"));
        }

        // Convert FlightData stream into Arrow RecordBatches
        let batches =
            flight_data_to_batches(&data_vec).map_err(|e| Status::internal(e.to_string()))?;

        // Determine WAL operation from metadata
        let wal_operation = if let Some(ref metadata) = flight_metadata {
            determine_wal_operation(metadata.signal_type.as_deref())
        } else {
            WalOperation::WriteTraces // Default fallback
        };

        log::debug!("Using WAL operation: {wal_operation:?}");

        // Transform batches if needed based on schema version
        let transformed_batches = if let Some(ref metadata) = flight_metadata {
            if metadata.schema_version == "v1" && metadata.signal_type.as_deref() == Some("traces")
            {
                // Transform v1 to v2 for traces only
                let mut transformed = Vec::new();
                for batch in batches {
                    match transform_trace_v1_to_v2(batch) {
                        Ok(transformed_batch) => {
                            if schema_ref.is_none() {
                                schema_ref = Some(transformed_batch.schema());
                            }
                            transformed.push(transformed_batch);
                        }
                        Err(e) => {
                            return Err(Status::internal(format!(
                                "Schema transformation failed: {e}"
                            )));
                        }
                    }
                }
                transformed
            } else {
                batches
            }
        } else {
            batches
        };

        // Write all batches to WAL first for durability
        let mut wal_entry_ids = Vec::new();

        // Serialize FlightMetadata to JSON for WAL storage (for writer routing)
        let metadata_json = flight_metadata.as_ref().map(|metadata| {
            serde_json::to_string(&serde_json::json!({
                "schema_version": metadata.schema_version,
                "signal_type": metadata.signal_type,
                "target_table": metadata.target_table,
            }))
            .unwrap_or_default()
        });

        for batch in &transformed_batches {
            // Serialize RecordBatch for WAL storage
            let batch_bytes = record_batch_to_bytes(batch)
                .map_err(|e| Status::internal(format!("Failed to serialize batch: {e}")))?;

            // Write to WAL with correct operation type determined from metadata
            // Pass metadata to enable proper table routing (e.g., metrics_exponential_histogram)
            let entry_id = self
                .wal
                .append(wal_operation.clone(), batch_bytes, metadata_json.clone())
                .await
                .map_err(|e| Status::internal(format!("Failed to write to WAL: {e}")))?;

            wal_entry_ids.push(entry_id);
        }

        // Force flush WAL to ensure durability before acknowledging
        self.wal
            .flush()
            .await
            .map_err(|e| Status::internal(format!("Failed to flush WAL: {e}")))?;

        // Process entries using the Iceberg processor
        // This is done synchronously to ensure consistency, but could be made async
        let mut processor = self.processor.lock().await;
        for entry_id in wal_entry_ids {
            match processor.process_single_entry(entry_id).await {
                Ok(_) => {
                    log::debug!("Successfully processed WAL entry {entry_id} via Iceberg");
                }
                Err(e) => {
                    log::error!("Failed to process WAL entry {entry_id} via Iceberg: {e}");
                    return Err(Status::internal(format!(
                        "Failed to process via Iceberg: {e}"
                    )));
                }
            }
        }

        let result = PutResult {
            app_metadata: Bytes::new(),
        };
        let out = stream::once(async move { Ok(result) }).boxed();
        Ok(Response::new(out))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;
    async fn do_get(
        &self,
        _request: Request<arrow_flight::Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("do_get not supported"))
    }
    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> Result<Response<arrow_flight::PollInfo>, Status> {
        Err(Status::unimplemented("poll_flight_info not supported"))
    }
    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;
    async fn do_exchange(
        &self,
        _request: Request<tonic::Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("do_exchange not supported"))
    }
    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;
    async fn do_action(
        &self,
        _request: Request<arrow_flight::Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("do_action not supported"))
    }
    type ListActionsStream = BoxStream<'static, Result<arrow_flight::ActionType, Status>>;
    async fn list_actions(
        &self,
        _request: Request<arrow_flight::Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        let out = stream::empty().boxed();
        Ok(Response::new(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::Configuration;
    use common::wal::WalConfig;
    use object_store::memory::InMemory;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_iceberg_flight_service_creation() {
        let temp_dir = tempdir().unwrap();
        let config = Configuration::default();
        let object_store = Arc::new(InMemory::new());
        let wal_config = WalConfig {
            wal_dir: temp_dir.path().to_path_buf(),
            max_segment_size: 1024 * 1024, // 1MB
            max_buffer_entries: 1000,
            flush_interval_secs: 5,
            tenant_id: "test-tenant".to_string(),
            dataset_id: "test-dataset".to_string(),
            retention_secs: 3600,
            cleanup_interval_secs: 300,
            compaction_threshold: 0.5,
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());

        let service = IcebergWriterFlightService::new(config, object_store, wal);

        // Verify service was created successfully
        assert!(service.processor.lock().await.get_stats().active_writers == 0);
    }
}
