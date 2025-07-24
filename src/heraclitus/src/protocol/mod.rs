use crate::{
    config::AuthConfig, error::Result, metrics::Metrics, state::StateManager, storage::BatchWriter,
};
use std::sync::Arc;
use tokio::net::TcpStream;

mod api_versions;
mod create_topics;
mod describe_groups;
mod fetch;
mod find_coordinator;
mod handler;
mod heartbeat;
mod init_producer_id;
mod join_group;
mod kafka_protocol;
mod leave_group;
pub mod list_offsets;
mod message_set;
mod metadata;
mod offset_commit;
mod offset_fetch;
mod produce;
mod record_batch;
mod record_batch_builder;
mod request;
mod response;
mod sasl_authenticate;
mod sasl_handshake;
mod sync_group;

pub use fetch::{FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicResponse};
pub use handler::ConnectionHandler;
pub use produce::{ProduceRequest, ProduceResponse};
pub use record_batch::{Record, RecordBatch, RecordHeader};
pub use record_batch_builder::{CompressionType, RecordBatchBuilder};
pub use request::{KafkaRequest, RequestType};
pub use response::KafkaResponse;

#[derive(Clone)]
pub struct ProtocolHandler {
    state_manager: Arc<StateManager>,
    batch_writer: Arc<BatchWriter>,
    port: u16,
    auth_config: Arc<AuthConfig>,
    metrics: Arc<Metrics>,
}

impl ProtocolHandler {
    pub fn new(
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        port: u16,
        auth_config: Arc<AuthConfig>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            state_manager,
            batch_writer,
            port,
            auth_config,
            metrics,
        }
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        let handler = ConnectionHandler::new(
            socket,
            self.state_manager.clone(),
            self.batch_writer.clone(),
            self.port,
            self.auth_config.clone(),
            self.metrics.clone(),
        );
        handler.run().await
    }
}
