use std::sync::Arc;

use arrow_array::RecordBatch;
use common::flight::conversion::otlp_logs_to_arrow;
use messaging::{messages::batch::BatchWrapper, Message, MessagingBackend};
use opentelemetry_proto::tonic::collector::logs::v1::{
    logs_service_server::LogsService, ExportLogsServiceRequest, ExportLogsServiceResponse,
};
use tokio::sync::Mutex;
use tonic::{async_trait, Request, Response, Status};

pub struct LogAcceptorService<Q: MessagingBackend + 'static> {
    queue: Arc<Mutex<Q>>,
}

impl<Q: MessagingBackend + 'static> LogAcceptorService<Q> {
    pub fn new(queue: Arc<Mutex<Q>>) -> Self {
        Self { queue }
    }
}

#[async_trait]
impl<Q: MessagingBackend + 'static> LogsService for LogAcceptorService<Q> {
    async fn export(
        &self,
        request: Request<ExportLogsServiceRequest>,
    ) -> Result<Response<ExportLogsServiceResponse>, Status> {
        log::info!("Got a logs request: {:?}", request);

        let req = request.into_inner();

        // Convert OTLP to Arrow RecordBatch
        let record_batch = otlp_logs_to_arrow(&req);

        // Create a batch wrapper from the record batch
        let batch_wrapper = BatchWrapper::from(record_batch);
        let message = Message::Batch(batch_wrapper);

        let queue = self.queue.lock().await;

        // Send the message
        let _ = queue
            .send_message("arrow-logs", message)
            .await
            .map_err(|e| {
                log::error!("Failed to publish arrow logs message: {:?}", e);
                e
            });

        Ok(Response::new(ExportLogsServiceResponse {
            partial_success: None,
        }))
    }
}

#[cfg(test)]
pub struct MockLogAcceptorService {
    pub handle_export_calls: std::sync::Mutex<Vec<ExportLogsServiceRequest>>,
}

#[cfg(test)]
impl MockLogAcceptorService {
    pub fn new() -> Self {
        Self {
            handle_export_calls: std::sync::Mutex::new(Vec::new()),
        }
    }

    pub async fn export(&self, request: ExportLogsServiceRequest) -> ExportLogsServiceResponse {
        self.handle_export_calls.lock().unwrap().push(request);

        ExportLogsServiceResponse {
            partial_success: None,
        }
    }
}
