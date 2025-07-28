use crate::{
    config::{AuthConfig, CompressionConfig, TopicConfig},
    error::Result,
    metrics::Metrics,
    protocol_v2::ConnectionHandler,
    state::StateManager,
    storage::{BatchWriter, MessageReader},
};
use std::sync::Arc;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct ProtocolConfig {
    pub auth_config: Arc<AuthConfig>,
    pub topic_config: Arc<TopicConfig>,
    pub compression_config: Arc<CompressionConfig>,
}

#[derive(Clone)]
pub struct ProtocolHandler {
    state_manager: Arc<StateManager>,
    batch_writer: Arc<BatchWriter>,
    message_reader: Arc<MessageReader>,
    port: u16,
    metrics: Arc<Metrics>,
    config: ProtocolConfig,
}

impl ProtocolHandler {
    pub fn new(
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        message_reader: Arc<MessageReader>,
        port: u16,
        metrics: Arc<Metrics>,
        config: ProtocolConfig,
    ) -> Self {
        Self {
            state_manager,
            batch_writer,
            message_reader,
            port,
            metrics,
            config,
        }
    }

    pub async fn handle_connection(&self, socket: TcpStream) -> Result<()> {
        let handler = ConnectionHandler::new(
            socket,
            self.state_manager.clone(),
            self.batch_writer.clone(),
            self.message_reader.clone(),
            self.port,
            self.config.auth_config.clone(),
            self.metrics.clone(),
            self.config.topic_config.clone(),
            self.config.compression_config.clone(),
        );
        handler.run().await
    }
}
