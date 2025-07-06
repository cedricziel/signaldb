use crate::write_batch_to_object_store;
use arrow_flight::flight_service_server::FlightService;
use arrow_flight::utils::flight_data_to_batches;
use arrow_flight::{
    FlightData, FlightDescriptor, HandshakeRequest, HandshakeResponse, PutResult, SchemaResult,
};
use bytes::Bytes;
use common::flight::schema::FlightSchemas;
use common::wal::{Wal, WalOperation, record_batch_to_bytes};
use futures::StreamExt;
use futures::stream::{self, BoxStream};
use object_store::ObjectStore;
use std::sync::Arc;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// Flight service that ingests RecordBatches and writes them as Parquet files
pub struct WriterFlightService {
    object_store: Arc<dyn ObjectStore>,
    wal: Arc<Wal>,
    #[allow(dead_code)]
    schemas: FlightSchemas,
}

impl WriterFlightService {
    /// Create a new WriterFlightService with shared object store and WAL
    pub fn new(object_store: Arc<dyn ObjectStore>, wal: Arc<Wal>) -> Self {
        Self {
            object_store,
            wal,
            schemas: FlightSchemas::new(),
        }
    }
}

#[tonic::async_trait]
impl FlightService for WriterFlightService {
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

        // Write all batches to WAL first for durability (WAL serves as the buffer)
        let mut wal_entry_ids = Vec::new();
        for batch in &batches {
            // Serialize RecordBatch for WAL storage
            let batch_bytes = record_batch_to_bytes(batch)
                .map_err(|e| Status::internal(format!("Failed to serialize batch: {e}")))?;

            // Write to WAL - this ensures durability and serves as our buffer
            let entry_id = self
                .wal
                .append(WalOperation::WriteTraces, batch_bytes)
                .await
                .map_err(|e| Status::internal(format!("Failed to write to WAL: {e}")))?;

            wal_entry_ids.push(entry_id);
        }

        // Force flush WAL to ensure durability before acknowledging
        self.wal
            .flush()
            .await
            .map_err(|e| Status::internal(format!("Failed to flush WAL: {e}")))?;

        // Process entries from WAL to object store (synchronous for now)
        for (batch, entry_id) in batches.iter().zip(wal_entry_ids.iter()) {
            let path = format!("batch/{}.parquet", Uuid::new_v4());

            match write_batch_to_object_store(self.object_store.clone(), &path, batch.clone()).await
            {
                Ok(_) => {
                    // Mark WAL entry as processed after successful write
                    if let Err(e) = self.wal.mark_processed(*entry_id).await {
                        log::warn!("Failed to mark WAL entry {entry_id} as processed: {e}");
                    }
                    log::debug!("Successfully wrote batch {entry_id} to object store at {path}");
                }
                Err(e) => {
                    log::error!("Failed to write batch {entry_id} to object store: {e}");
                    return Err(Status::internal(format!(
                        "Failed to write to object store: {e}"
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
