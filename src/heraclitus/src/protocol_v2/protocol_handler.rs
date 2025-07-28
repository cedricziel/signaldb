use crate::{
    config::{AuthConfig, TopicConfig},
    error::Result,
    metrics::Metrics,
    protocol_v2::ConnectionHandler,
    state::StateManager,
    storage::BatchWriter,
};
use std::sync::Arc;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct ProtocolHandler {
    state_manager: Arc<StateManager>,
    batch_writer: Arc<BatchWriter>,
    port: u16,
    auth_config: Arc<AuthConfig>,
    metrics: Arc<Metrics>,
    topic_config: Arc<TopicConfig>,
}

impl ProtocolHandler {
    pub fn new(
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        port: u16,
        auth_config: Arc<AuthConfig>,
        metrics: Arc<Metrics>,
        topic_config: Arc<TopicConfig>,
    ) -> Self {
        Self {
            state_manager,
            batch_writer,
            port,
            auth_config,
            metrics,
            topic_config,
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
            self.topic_config.clone(),
        );
        handler.run().await
    }
}
