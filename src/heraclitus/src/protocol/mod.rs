use crate::{error::Result, state::StateManager, storage::BatchWriter};
use std::sync::Arc;
use tokio::net::TcpStream;

mod handler;
mod kafka_protocol;
mod metadata;
mod produce;
mod request;
mod response;

pub use handler::ConnectionHandler;
pub use produce::{ProduceRequest, ProduceResponse};
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
