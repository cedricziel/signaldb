use super::{ApiHandler, HandlerContext};
use crate::error::Result;
use crate::protocol::request::KafkaRequest;
use std::collections::HashMap;
use std::sync::Arc;

pub struct HandlerRegistry {
    handlers: HashMap<i16, Arc<dyn ApiHandler>>,
}

impl HandlerRegistry {
    pub fn new() -> Self {
        let mut handlers: HashMap<i16, Arc<dyn ApiHandler>> = HashMap::new();

        // Register all handlers
        handlers.insert(0, Arc::new(super::produce::ProduceHandler));
        handlers.insert(1, Arc::new(super::fetch::FetchHandler));
        handlers.insert(2, Arc::new(super::fetch::ListOffsetsHandler));
        handlers.insert(3, Arc::new(super::metadata::MetadataHandler));
        handlers.insert(8, Arc::new(super::consumer_group::OffsetCommitHandler));
        handlers.insert(9, Arc::new(super::consumer_group::OffsetFetchHandler));
        handlers.insert(10, Arc::new(super::consumer_group::FindCoordinatorHandler));
        handlers.insert(11, Arc::new(super::consumer_group::JoinGroupHandler));
        handlers.insert(12, Arc::new(super::consumer_group::HeartbeatHandler));
        handlers.insert(13, Arc::new(super::consumer_group::LeaveGroupHandler));
        handlers.insert(14, Arc::new(super::consumer_group::SyncGroupHandler));
        handlers.insert(15, Arc::new(super::consumer_group::DescribeGroupsHandler));
        handlers.insert(16, Arc::new(super::consumer_group::ListGroupsHandler));
        handlers.insert(18, Arc::new(super::auth::ApiVersionsHandler));
        handlers.insert(19, Arc::new(super::admin::CreateTopicsHandler));
        handlers.insert(20, Arc::new(super::admin::DeleteTopicsHandler));
        handlers.insert(22, Arc::new(super::produce::InitProducerIdHandler));
        handlers.insert(36, Arc::new(super::auth::SaslHandshakeHandler));
        handlers.insert(37, Arc::new(super::auth::SaslAuthenticateHandler));

        Self { handlers }
    }

    pub async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        match self.handlers.get(&request.api_key) {
            Some(handler) => {
                // Check version support
                if !handler.supports_version(request.api_version) {
                    return Err(crate::error::HeraclitusError::UnsupportedVersion(
                        request.api_key,
                        request.api_version,
                    ));
                }

                handler.handle(request, context).await
            }
            None => Err(crate::error::HeraclitusError::UnsupportedApiKey(
                request.api_key,
            )),
        }
    }

    pub fn get_handler(&self, api_key: i16) -> Option<&Arc<dyn ApiHandler>> {
        self.handlers.get(&api_key)
    }
}

impl Default for HandlerRegistry {
    fn default() -> Self {
        Self::new()
    }
}
