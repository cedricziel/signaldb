use crate::processor::WalProcessor;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
};
use bytes::Bytes;
use common::config::Configuration;
use common::flight::schema::FlightSchemas;
use common::wal::{Wal, WalOperation, record_batch_to_bytes};
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
        while let Some(msg) = inbound.next().await {
            let d = msg.map_err(|e| Status::internal(e.to_string()))?;
            data_vec.push(d);
        }
        if data_vec.is_empty() {
            return Err(Status::invalid_argument("No FlightData received"));
        }

        // Convert FlightData stream into Arrow RecordBatches
        let batches =
            flight_data_to_batches(&data_vec).map_err(|e| Status::internal(e.to_string()))?;

        // Write all batches to WAL first for durability
        let mut wal_entry_ids = Vec::new();
        for batch in &batches {
            // Serialize RecordBatch for WAL storage
            let batch_bytes = record_batch_to_bytes(batch)
                .map_err(|e| Status::internal(format!("Failed to serialize batch: {e}")))?;

            // Write to WAL - this ensures durability
            let entry_id = self
                .wal
                .append(WalOperation::WriteTraces, batch_bytes) // TODO: Determine operation type from metadata
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
        };
        let wal = Arc::new(Wal::new(wal_config).await.unwrap());

        let service = IcebergWriterFlightService::new(config, object_store, wal);

        // Verify service was created successfully
        assert!(service.processor.lock().await.get_stats().active_writers == 0);
    }
}
