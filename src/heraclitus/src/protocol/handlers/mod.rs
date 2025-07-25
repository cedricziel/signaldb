use crate::{
    config::AuthConfig,
    error::Result,
    metrics::Metrics,
    protocol::{kafka_protocol::*, request::KafkaRequest},
    state::StateManager,
    storage::BatchWriter,
};
use bytes::{BufMut, BytesMut};
use std::sync::Arc;

/// Trait for handling Kafka API requests
#[async_trait::async_trait]
pub trait ApiHandler: Send + Sync {
    /// Handle a request and return the response body
    async fn handle(&self, request: &KafkaRequest, context: &mut HandlerContext)
    -> Result<Vec<u8>>;

    /// Get the API key this handler is responsible for
    fn api_key(&self) -> i16;

    /// Check if this handler supports the given API version
    fn supports_version(&self, version: i16) -> bool;
}

/// Context passed to handlers containing shared state and utilities
pub struct HandlerContext {
    pub state_manager: Arc<StateManager>,
    pub batch_writer: Arc<BatchWriter>,
    pub metrics: Arc<Metrics>,
    pub auth_config: Arc<AuthConfig>,
    pub authenticated: bool,
    pub username: Option<String>,
    pub port: u16,
    // Track consumer group membership for cleanup
    pub consumer_groups: Vec<(String, String)>, // (group_id, member_id)
}

/// Trait for building Kafka responses with proper headers
pub trait ResponseBuilder {
    /// Build a complete response with the appropriate header version
    fn build_response(&self, request: &KafkaRequest, response_body: Vec<u8>) -> Vec<u8>;

    /// Build an error response with the appropriate header version
    fn build_error_response(&self, request: &KafkaRequest, error_code: i16) -> Vec<u8>;
}

/// Default implementation of ResponseBuilder
pub struct DefaultResponseBuilder;

impl ResponseBuilder for DefaultResponseBuilder {
    fn build_response(&self, request: &KafkaRequest, response_body: Vec<u8>) -> Vec<u8> {
        let mut response = BytesMut::new();

        // Write size placeholder
        response.put_i32(0);

        // Write response header based on whether the API uses flexible versions
        if uses_flexible_version(request.api_key, request.api_version) {
            write_response_header_v1(&mut response, request.correlation_id);
        } else {
            write_response_header(&mut response, request.correlation_id);
        }

        // Write response body
        response.extend_from_slice(&response_body);

        // Update size
        let size = (response.len() - 4) as i32;
        response[0..4].copy_from_slice(&size.to_be_bytes());

        response.to_vec()
    }

    fn build_error_response(&self, request: &KafkaRequest, error_code: i16) -> Vec<u8> {
        let mut response_body = BytesMut::new();

        // Most error responses just contain the error code
        // Some APIs have different error response formats, but this covers the common case
        response_body.put_i16(error_code);

        self.build_response(request, response_body.to_vec())
    }
}

// Re-export the uses_flexible_version function
use super::handler::uses_flexible_version;

// Handler modules
pub mod admin;
pub mod auth;
pub mod consumer_group;
pub mod fetch;
pub mod metadata;
pub mod produce;
pub mod registry;

// Re-export handlers
pub use admin::{CreateTopicsHandler, DeleteTopicsHandler};
pub use auth::{ApiVersionsHandler, SaslAuthenticateHandler, SaslHandshakeHandler};
pub use consumer_group::{
    DescribeGroupsHandler, FindCoordinatorHandler, HeartbeatHandler, JoinGroupHandler,
    LeaveGroupHandler, ListGroupsHandler, OffsetCommitHandler, OffsetFetchHandler,
    SyncGroupHandler,
};
pub use fetch::FetchHandler;
pub use fetch::ListOffsetsHandler;
pub use metadata::MetadataHandler;
pub use produce::InitProducerIdHandler;
pub use produce::ProduceHandler;
pub use registry::HandlerRegistry;
