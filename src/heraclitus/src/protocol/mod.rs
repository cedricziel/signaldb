use crate::{error::Result, state::StateManager, storage::BatchWriter};
use std::sync::Arc;
use tokio::net::TcpStream;

mod api_versions;
mod fetch;
mod find_coordinator;
mod handler;
mod heartbeat;
mod join_group;
mod kafka_protocol;
pub mod list_offsets;
mod message_set;
mod metadata;
mod produce;
mod record_batch;
mod record_batch_builder;
mod request;
mod response;
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
}

impl ProtocolHandler {
    pub fn new(
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        port: u16,
    ) -> Self {
        Self {
            state_manager,
            batch_writer,
            port,
        }
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        let handler = ConnectionHandler::new(
            socket,
            self.state_manager.clone(),
            self.batch_writer.clone(),
            self.port,
        );
        handler.run().await
    }
}
