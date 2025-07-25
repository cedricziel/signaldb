use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        create_topics::{CreateTopicResponse, CreateTopicsRequest, CreateTopicsResponse},
        delete_topics::{
            DeleteTopicsRequest, DeleteTopicsResponse, TopicDeletionError as DeleteTopicResponse,
        },
        kafka_protocol::*,
        request::KafkaRequest,
    },
};
use tracing::{debug, error, info};

pub struct CreateTopicsHandler;

#[async_trait::async_trait]
impl ApiHandler for CreateTopicsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling create topics request v{}", request.api_version);

        // Track create topics requests
        let api_key_str = request.api_key.to_string();
        let api_version_str = request.api_version.to_string();
        let api_name =
            crate::metrics::protocol::ProtocolMetrics::api_name(request.api_key).to_string();
        context
            .metrics
            .protocol
            .request_count
            .with_label_values(&[&api_key_str, &api_name, &api_version_str])
            .inc();

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let create_request = CreateTopicsRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "CreateTopics request: {} topics, timeout={}ms, validate_only={}",
            create_request.topics.len(),
            create_request.timeout_ms,
            create_request.validate_only
        );

        let mut topic_results = Vec::new();

        for topic in &create_request.topics {
            // Validate topic name
            if topic.name.is_empty() || topic.name.contains(' ') {
                topic_results.push(CreateTopicResponse {
                    name: topic.name.clone(),
                    error_code: ERROR_INVALID_TOPIC_EXCEPTION,
                    error_message: Some("Invalid topic name".to_string()),
                    num_partitions: -1,
                    replication_factor: -1,
                    configs: vec![],
                });
                continue;
            }

            // Set default partitions if not specified
            let num_partitions = if topic.num_partitions <= 0 {
                1
            } else {
                topic.num_partitions
            };
            let replication_factor = if topic.replication_factor <= 0 {
                1
            } else {
                topic.replication_factor
            };

            if create_request.validate_only {
                // Just validate, don't create
                topic_results.push(CreateTopicResponse {
                    name: topic.name.clone(),
                    error_code: ERROR_NONE,
                    error_message: None,
                    num_partitions,
                    replication_factor,
                    configs: vec![],
                });
            } else {
                // Actually create the topic
                let topic_metadata = crate::state::TopicMetadata {
                    name: topic.name.clone(),
                    partitions: num_partitions,
                    replication_factor,
                    config: std::collections::HashMap::new(),
                    created_at: chrono::Utc::now(),
                };

                match context
                    .state_manager
                    .metadata()
                    .create_topic(topic_metadata)
                    .await
                {
                    Ok(()) => {
                        info!(
                            "Created topic '{}' with {} partitions",
                            topic.name, num_partitions
                        );
                        topic_results.push(CreateTopicResponse {
                            name: topic.name.clone(),
                            error_code: ERROR_NONE,
                            error_message: None,
                            num_partitions,
                            replication_factor,
                            configs: vec![],
                        });
                    }
                    Err(e) => {
                        let error_code = if e.to_string().contains("already exists") {
                            ERROR_TOPIC_ALREADY_EXISTS
                        } else {
                            ERROR_UNKNOWN_SERVER_ERROR
                        };

                        error!("Failed to create topic '{}': {}", topic.name, e);
                        topic_results.push(CreateTopicResponse {
                            name: topic.name.clone(),
                            error_code,
                            error_message: Some(e.to_string()),
                            num_partitions: -1,
                            replication_factor: -1,
                            configs: vec![],
                        });
                    }
                }
            }
        }

        let response = CreateTopicsResponse {
            throttle_time_ms: 0,
            topics: topic_results,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        19 // CreateTopics
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=7).contains(&version)
    }
}

pub struct DeleteTopicsHandler;

#[async_trait::async_trait]
impl ApiHandler for DeleteTopicsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling delete topics request v{}", request.api_version);

        // Track delete topics requests
        let api_key_str = request.api_key.to_string();
        let api_version_str = request.api_version.to_string();
        let api_name =
            crate::metrics::protocol::ProtocolMetrics::api_name(request.api_key).to_string();
        context
            .metrics
            .protocol
            .request_count
            .with_label_values(&[&api_key_str, &api_name, &api_version_str])
            .inc();

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let delete_request = DeleteTopicsRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "DeleteTopics request: topics={:?}, timeout={}ms",
            delete_request.topic_names, delete_request.timeout_ms
        );

        let mut responses = Vec::new();

        for topic_name in &delete_request.topic_names {
            match context
                .state_manager
                .metadata()
                .delete_topic(topic_name)
                .await
            {
                Ok(()) => {
                    info!("Deleted topic '{}'", topic_name);
                    responses.push(DeleteTopicResponse {
                        topic_name: topic_name.clone(),
                        error_code: ERROR_NONE,
                        error_message: None,
                    });
                }
                Err(e) => {
                    let error_code = if e.to_string().contains("not found") {
                        ERROR_TOPIC_NOT_FOUND
                    } else {
                        ERROR_UNKNOWN
                    };

                    error!("Failed to delete topic '{}': {}", topic_name, e);
                    responses.push(DeleteTopicResponse {
                        topic_name: topic_name.clone(),
                        error_code,
                        error_message: Some(e.to_string()),
                    });
                }
            }
        }

        let response = DeleteTopicsResponse {
            throttle_time_ms: 0,
            topic_errors: responses,
        };

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        20 // DeleteTopics
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=6).contains(&version)
    }
}
