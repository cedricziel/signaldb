use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        kafka_protocol::*,
        metadata::{
            BrokerMetadata, MetadataRequest, MetadataResponse, PartitionMetadata,
            TopicMetadata as ProtoTopicMetadata,
        },
        request::KafkaRequest,
    },
};
use tracing::{debug, error, info};

pub struct MetadataHandler;

#[async_trait::async_trait]
impl ApiHandler for MetadataHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling metadata request v{}", request.api_version);

        // Track metadata requests
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
        let metadata_request = MetadataRequest::parse(&mut cursor, request.api_version)?;

        debug!("Metadata request: topics={:?}", metadata_request.topics);

        // Get broker info
        let broker_host = "localhost".to_string();
        let broker_port = context.port;

        // Build topic metadata
        let mut topics = Vec::new();

        match &metadata_request.topics {
            Some(topic_names) => {
                // Specific topics requested
                for topic_name in topic_names {
                    match context.state_manager.metadata().get_topic(topic_name).await {
                        Ok(Some(topic_meta)) => {
                            topics.push(ProtoTopicMetadata {
                                error_code: ERROR_NONE,
                                name: topic_meta.name.clone(),
                                topic_id: None,
                                is_internal: false,
                                partitions: (0..topic_meta.partitions)
                                    .map(|partition_id| PartitionMetadata {
                                        error_code: ERROR_NONE,
                                        partition_id,
                                        leader: 0,
                                        replicas: vec![0],
                                        isr: vec![0],
                                    })
                                    .collect(),
                                topic_authorized_operations: -1,
                            });
                        }
                        Ok(None) => {
                            // Topic doesn't exist
                            if true {
                                // Auto-create topics by default
                                // Auto-create topic with default settings
                                let topic_metadata = crate::state::TopicMetadata {
                                    name: topic_name.clone(),
                                    partitions: 1,
                                    replication_factor: 1,
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
                                        info!("Auto-created topic: {}", topic_name);
                                        topics.push(ProtoTopicMetadata {
                                            error_code: ERROR_NONE,
                                            name: topic_name.clone(),
                                            topic_id: None,
                                            is_internal: false,
                                            partitions: vec![PartitionMetadata {
                                                error_code: ERROR_NONE,
                                                partition_id: 0,
                                                leader: 0,
                                                replicas: vec![0],
                                                isr: vec![0],
                                            }],
                                            topic_authorized_operations: -1,
                                        });
                                    }
                                    Err(e) => {
                                        error!("Failed to auto-create topic {}: {}", topic_name, e);
                                        topics.push(ProtoTopicMetadata {
                                            error_code: ERROR_TOPIC_NOT_FOUND,
                                            name: topic_name.clone(),
                                            topic_id: None,
                                            is_internal: false,
                                            partitions: vec![],
                                            topic_authorized_operations: -1,
                                        });
                                    }
                                }
                            } else {
                                topics.push(ProtoTopicMetadata {
                                    error_code: ERROR_TOPIC_NOT_FOUND,
                                    name: topic_name.clone(),
                                    topic_id: None,
                                    is_internal: false,
                                    partitions: vec![],
                                    topic_authorized_operations: -1,
                                });
                            }
                        }
                        Err(e) => {
                            error!("Failed to get topic metadata for {}: {}", topic_name, e);
                            topics.push(ProtoTopicMetadata {
                                error_code: ERROR_UNKNOWN,
                                name: topic_name.clone(),
                                topic_id: None,
                                is_internal: false,
                                partitions: vec![],
                                topic_authorized_operations: -1,
                            });
                        }
                    }
                }
            }
            None => {
                // All topics requested
                match context.state_manager.metadata().list_topics().await {
                    Ok(topic_names) => {
                        for topic_name in topic_names {
                            match context
                                .state_manager
                                .metadata()
                                .get_topic(&topic_name)
                                .await
                            {
                                Ok(Some(topic_meta)) => {
                                    topics.push(ProtoTopicMetadata {
                                        error_code: ERROR_NONE,
                                        name: topic_meta.name.clone(),
                                        topic_id: None,
                                        is_internal: false,
                                        partitions: (0..topic_meta.partitions)
                                            .map(|partition_id| PartitionMetadata {
                                                error_code: ERROR_NONE,
                                                partition_id,
                                                leader: 0,
                                                replicas: vec![0],
                                                isr: vec![0],
                                            })
                                            .collect(),
                                        topic_authorized_operations: -1,
                                    });
                                }
                                Ok(None) => {
                                    // Skip topics that were deleted between list and get
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to get topic metadata for {}: {}",
                                        topic_name, e
                                    );
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!("Failed to list topics: {}", e);
                    }
                }
            }
        }

        let response = MetadataResponse {
            throttle_time_ms: 0,
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: broker_host,
                port: broker_port as i32,
            }],
            cluster_id: None,
            controller_id: 0,
            topics,
        };

        info!(
            "Metadata response: {} brokers, {} topics",
            response.brokers.len(),
            response.topics.len()
        );

        // Encode response
        let response_body = response.encode(request.api_version)?;
        info!(
            "Metadata response v{} encoded: {} bytes",
            request.api_version,
            response_body.len()
        );

        Ok(response_body)
    }

    fn api_key(&self) -> i16 {
        3 // Metadata
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=12).contains(&version)
    }
}
