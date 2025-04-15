use std::sync::Arc;

use arrow_array::RecordBatch;
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
}

impl<Q: MessagingBackend + 'static> MetricsAcceptorService<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
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

        Ok(Response::new(ExportMetricsServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
pub struct MockMetricsAcceptorService {
    pub handle_export_calls: std::sync::Mutex<Vec<ExportMetricsServiceRequest>>,
}

#[cfg(test)]
impl MockMetricsAcceptorService {
    pub fn new() -> Self {
        Self {
            handle_export_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn export(
        &self,
        request: ExportMetricsServiceRequest,
    ) -> ExportMetricsServiceResponse {
        self.handle_export_calls.lock().unwrap().push(request);

        ExportMetricsServiceResponse {
            partial_success: None,
        }
    }
}
