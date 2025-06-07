use std::sync::Arc;
// Flight protocol imports
use arrow_flight::client::FlightClient;
use arrow_flight::utils::batches_to_flight_data;
use futures::{stream, StreamExt, TryStreamExt};

use common::flight::conversion::otlp_metrics_to_arrow;
use messaging::{messages::batch::BatchWrapper, Message, MessagingBackend};
use opentelemetry_proto::tonic::collector::metrics::v1::{
    metrics_service_server::MetricsService, ExportMetricsServiceRequest,
    ExportMetricsServiceResponse,
};
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

pub struct MetricsAcceptorService<Q: MessagingBackend + 'static> {
    queue: Arc<Mutex<Q>>,
    flight_client: Option<Arc<Mutex<FlightClient>>>,
}

impl<Q: MessagingBackend + 'static> MetricsAcceptorService<Q> {
    /// Legacy constructor using only in-memory queue
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self {
            queue,
            flight_client: None,
        }
    }
    /// Constructor with Flight client for forwarding telemetry
    pub fn new_with_flight(queue: Arc<Mutex<Q>>, flight_client: Arc<Mutex<FlightClient>>) -> Self {
        Self {
            queue,
            flight_client: Some(flight_client),
        }
    }
}

#[async_trait]
impl<Q: MessagingBackend + 'static> MetricsService for MetricsAcceptorService<Q> {
    async fn export(
        &self,
        request: Request<ExportMetricsServiceRequest>,
    ) -> Result<Response<ExportMetricsServiceResponse>, Status> {
        log::info!("Got a metrics request: {:?}", request);

        let req = request.into_inner();

        // Convert OTLP to Arrow RecordBatch
        let record_batch = otlp_metrics_to_arrow(&req);
        // Clone for Flight protocol if configured
        let flight_batch = record_batch.clone();

        // Create a batch wrapper from the record batch
        let batch_wrapper = BatchWrapper::from(record_batch);
        let message = Message::Batch(batch_wrapper);

        let queue = self.queue.lock().await;

        // Send the message
        let _ = queue
            .send_message("arrow-metrics", message)
            .await
            .map_err(|e| {
                log::error!("Failed to publish arrow metrics message: {:?}", e);
                e
            });

        // Forward via Flight protocol if configured
        if let Some(flight_client) = &self.flight_client {
            let schema = flight_batch.schema();
            let flight_data =
                batches_to_flight_data(schema.as_ref(), vec![flight_batch]).unwrap_or_default();
            let mut client = flight_client.lock().await;
            let mut results = client
                .do_put(stream::iter(flight_data.into_iter().map(Ok)))
                .await
                .unwrap_or_else(|e| {
                    log::error!("Flight do_put failed for metrics: {:?}", e);
                    futures::stream::empty().boxed()
                });
            while let Some(res) = results.next().await {
                if let Err(e) = res {
                    log::error!("Flight put result error for metrics: {:?}", e);
                }
            }
        }
        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
pub struct MockMetricsAcceptorService {
    pub handle_export_calls: tokio::sync::Mutex<Vec<ExportMetricsServiceRequest>>,
}

#[cfg(test)]
impl MockMetricsAcceptorService {
    pub fn new() -> Self {
        Self {
            handle_export_calls: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn export(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> ExportMetricsServiceResponse {
        self.handle_export_calls.lock().await.push(request);

        ExportMetricsServiceResponse {
            partial_success: None,
        }
    }
}
