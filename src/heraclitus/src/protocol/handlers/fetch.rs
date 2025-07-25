use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        fetch::{FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicResponse},
        kafka_protocol::*,
        list_offsets::{
            ListOffsetsPartitionResponse, ListOffsetsRequest, ListOffsetsResponse,
            ListOffsetsTopicResponse,
        },
        request::KafkaRequest,
    },
};
use tracing::{debug, error, info, warn};

pub struct FetchHandler;

#[async_trait::async_trait]
impl ApiHandler for FetchHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling fetch request v{}", request.api_version);

        // Track fetch requests
        context
            .metrics
            .protocol
            .request_count
            .with_label_values(&["fetch"])
            .inc();

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let fetch_request = FetchRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "Fetch request: max_wait={}, min_bytes={}, max_bytes={}, topics={}",
            fetch_request.max_wait_ms,
            fetch_request.min_bytes,
            fetch_request.max_bytes,
            fetch_request.topics.len()
        );

        // Process each topic
        let mut topic_responses = Vec::new();
        let mut _total_bytes_fetched = 0;

        for topic_data in &fetch_request.topics {
            let mut partition_responses = Vec::new();

            // Check if topic exists
            match context
                .state_manager
                .metadata()
                .get_topic(&topic_data.topic)
                .await
            {
                Ok(Some(_topic_meta)) => {
                    // Process each partition
                    for partition_data in &topic_data.partitions {
                        // For now, we only support partition 0
                        if partition_data.partition != 0 {
                            partition_responses.push(FetchPartitionResponse {
                                partition: partition_data.partition,
                                error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                                high_watermark: -1,
                                last_stable_offset: -1,
                                log_start_offset: 0,
                                aborted_transactions: vec![],
                                preferred_read_replica: -1,
                                records: None,
                            });
                            continue;
                        }

                        // Create message reader
                        let message_reader = crate::storage::MessageReader::new(
                            context.batch_writer.object_store(),
                            context.batch_writer.layout().clone(),
                        );

                        // Fetch records from storage
                        match message_reader
                            .read_messages(
                                &topic_data.topic,
                                0,
                                partition_data.fetch_offset,
                                fetch_request.max_bytes as usize,
                            )
                            .await
                        {
                            Ok(messages) => {
                                // Get high watermark
                                let high_watermark = if messages.is_empty() {
                                    partition_data.fetch_offset
                                } else {
                                    messages
                                        .last()
                                        .map(|m| m.offset + 1)
                                        .unwrap_or(partition_data.fetch_offset)
                                };

                                // Convert messages to RecordBatch
                                let mut builder = crate::protocol::RecordBatchBuilder::new(
                                    crate::protocol::CompressionType::None,
                                );
                                for message in messages {
                                    builder.add_message(message);
                                }
                                // Build and encode the record batch to bytes
                                let encoded = builder.build()?;
                                let bytes_in_partition = encoded.len();
                                _total_bytes_fetched += bytes_in_partition;

                                debug!(
                                    "Fetched {} bytes from topic '{}' partition 0, offset={}, high_watermark={}",
                                    bytes_in_partition,
                                    topic_data.topic,
                                    partition_data.fetch_offset,
                                    high_watermark
                                );

                                partition_responses.push(FetchPartitionResponse {
                                    partition: 0,
                                    error_code: ERROR_NONE,
                                    high_watermark,
                                    last_stable_offset: high_watermark,
                                    log_start_offset: 0,
                                    aborted_transactions: vec![],
                                    preferred_read_replica: -1,
                                    records: Some(encoded),
                                });
                            }
                            Err(e) => {
                                error!(
                                    "Failed to fetch from topic '{}' partition 0: {}",
                                    topic_data.topic, e
                                );
                                partition_responses.push(FetchPartitionResponse {
                                    partition: 0,
                                    error_code: ERROR_UNKNOWN,
                                    high_watermark: -1,
                                    last_stable_offset: -1,
                                    log_start_offset: 0,
                                    aborted_transactions: vec![],
                                    preferred_read_replica: -1,
                                    records: None,
                                });
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Topic doesn't exist
                    for partition_data in &topic_data.partitions {
                        partition_responses.push(FetchPartitionResponse {
                            partition: partition_data.partition,
                            error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: 0,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: None,
                        });
                    }
                }
                Err(e) => {
                    error!("Failed to check topic '{}': {}", topic_data.topic, e);
                    for partition_data in &topic_data.partitions {
                        partition_responses.push(FetchPartitionResponse {
                            partition: partition_data.partition,
                            error_code: ERROR_UNKNOWN,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: 0,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: None,
                        });
                    }
                }
            }

            topic_responses.push(FetchTopicResponse {
                topic: topic_data.topic.clone(),
                partitions: partition_responses,
            });
        }

        // Update metrics - removed as these counters don't exist in ProtocolMetrics

        // Build response
        let response = FetchResponse {
            throttle_time_ms: 0,
            error_code: ERROR_NONE,
            session_id: 0,
            responses: topic_responses,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;
        Ok(response_body)
    }

    fn api_key(&self) -> i16 {
        1 // Fetch
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=12).contains(&version)
    }
}

pub struct ListOffsetsHandler;

#[async_trait::async_trait]
impl ApiHandler for ListOffsetsHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling list offsets request v{}", request.api_version);

        // Parse request
        let mut cursor = std::io::Cursor::new(&request.body[..]);
        let list_offsets_request = ListOffsetsRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "ListOffsets request: replica_id={}, topics={}",
            list_offsets_request.replica_id,
            list_offsets_request.topics.len()
        );

        // Process each topic
        let mut topic_responses = Vec::new();

        for topic_data in &list_offsets_request.topics {
            let mut partition_responses = Vec::new();

            // Check if topic exists
            match context
                .state_manager
                .metadata()
                .get_topic(&topic_data.name)
                .await
            {
                Ok(Some(_topic_meta)) => {
                    // Process each partition
                    for partition_data in &topic_data.partitions {
                        // For now, we only support partition 0
                        if partition_data.partition_index != 0 {
                            partition_responses.push(ListOffsetsPartitionResponse {
                                partition_index: partition_data.partition_index,
                                error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                                old_style_offsets: vec![],
                                timestamp: -1,
                                offset: -1,
                            });
                            continue;
                        }

                        // Get offset based on timestamp
                        let offset = if partition_data.timestamp == -2 {
                            // -2 means earliest offset
                            0
                        } else if partition_data.timestamp == -1 {
                            // -1 means latest offset
                            match context
                                .state_manager
                                .messages()
                                .get_high_water_mark(&topic_data.name, 0)
                                .await
                            {
                                Ok(hwm) => hwm,
                                Err(e) => {
                                    error!(
                                        "Failed to get high watermark for topic '{}': {}",
                                        topic_data.name, e
                                    );
                                    0
                                }
                            }
                        } else {
                            // Specific timestamp - not implemented yet
                            warn!("Timestamp-based offset lookup not implemented");
                            0
                        };

                        partition_responses.push(ListOffsetsPartitionResponse {
                            partition_index: 0,
                            error_code: if offset >= 0 {
                                ERROR_NONE
                            } else {
                                ERROR_OFFSET_NOT_AVAILABLE
                            },
                            old_style_offsets: vec![],
                            timestamp: partition_data.timestamp,
                            offset,
                        });
                    }
                }
                Ok(None) => {
                    // Topic doesn't exist
                    for partition_data in &topic_data.partitions {
                        partition_responses.push(ListOffsetsPartitionResponse {
                            partition_index: partition_data.partition_index,
                            error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                            old_style_offsets: vec![],
                            timestamp: -1,
                            offset: -1,
                        });
                    }
                }
                Err(e) => {
                    error!("Failed to check topic '{}': {}", topic_data.name, e);
                    for partition_data in &topic_data.partitions {
                        partition_responses.push(ListOffsetsPartitionResponse {
                            partition_index: partition_data.partition_index,
                            error_code: ERROR_UNKNOWN,
                            old_style_offsets: vec![],
                            timestamp: -1,
                            offset: -1,
                        });
                    }
                }
            }

            topic_responses.push(ListOffsetsTopicResponse {
                name: topic_data.name.clone(),
                partitions: partition_responses,
            });
        }

        // Build response
        let response = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: topic_responses,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;
        Ok(response_body)
    }

    fn api_key(&self) -> i16 {
        2 // ListOffsets
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=7).contains(&version)
    }
}
