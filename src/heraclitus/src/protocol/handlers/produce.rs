use super::{ApiHandler, HandlerContext};
use crate::{
    error::Result,
    protocol::{
        init_producer_id::{InitProducerIdRequest, InitProducerIdResponse},
        kafka_protocol::*,
        produce::{
            ProducePartitionResponse, ProduceRequest, ProduceResponse, ProduceTopicResponse,
        },
        request::KafkaRequest,
    },
};
use tracing::{debug, error, info, warn};

pub struct ProduceHandler;

#[async_trait::async_trait]
impl ApiHandler for ProduceHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling produce request v{}", request.api_version);

        // Track produce requests
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
        let produce_request = ProduceRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "Produce request: acks={}, timeout={}, topics={}",
            produce_request.acks,
            produce_request.timeout_ms,
            produce_request.topics.len()
        );

        // Validate acks value
        if produce_request.acks != 0 && produce_request.acks != 1 && produce_request.acks != -1 {
            warn!("Invalid acks value: {}", produce_request.acks);
            let error_response = ProduceResponse {
                throttle_time_ms: 0,
                responses: vec![],
            };
            return error_response.encode(request.api_version);
        }

        // Track metrics
        let mut _total_records = 0;
        let mut _total_bytes = 0;

        // Process each topic
        let mut topic_responses = Vec::new();
        for topic_data in &produce_request.topics {
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
                            partition_responses.push(ProducePartitionResponse {
                                partition_index: partition_data.partition_index,
                                error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                            continue;
                        }

                        if !partition_data.records.is_empty() {
                            // Parse the raw record batch bytes
                            match crate::protocol::RecordBatch::parse(&partition_data.records) {
                                Ok(record_batch) => {
                                    // Track metrics
                                    let record_count = record_batch.records.len();
                                    _total_records += record_count;
                                    _total_bytes += partition_data.records.len();

                                    // Get base offset for this batch
                                    let base_offset = match context
                                        .state_manager
                                        .messages()
                                        .assign_offsets(&topic_data.name, 0, record_count)
                                        .await
                                    {
                                        Ok(offset) => offset,
                                        Err(e) => {
                                            error!("Failed to assign offsets: {}", e);
                                            partition_responses.push(ProducePartitionResponse {
                                                partition_index: 0,
                                                error_code: ERROR_UNKNOWN,
                                                base_offset: -1,
                                                log_append_time_ms: -1,
                                                log_start_offset: 0,
                                            });
                                            continue;
                                        }
                                    };

                                    // Convert RecordBatch to individual KafkaMessages and write them
                                    let mut write_errors = false;
                                    let _now = chrono::Utc::now().timestamp_millis();

                                    for (i, record) in record_batch.records.iter().enumerate() {
                                        let headers = record
                                            .headers
                                            .iter()
                                            .map(|h| (h.key.clone(), h.value.clone()))
                                            .collect();

                                        let message = crate::storage::KafkaMessage {
                                            topic: topic_data.name.clone(),
                                            partition: 0,
                                            offset: base_offset + i as i64,
                                            key: record.key.clone(),
                                            value: record.value.clone(),
                                            headers,
                                            timestamp: record_batch.base_timestamp
                                                + record.timestamp_delta,
                                            producer_id: Some(record_batch.producer_id),
                                            producer_epoch: Some(record_batch.producer_epoch),
                                            sequence: Some(record_batch.base_sequence + i as i32),
                                        };

                                        if let Err(e) = context.batch_writer.write(message).await {
                                            error!("Failed to write message: {}", e);
                                            write_errors = true;
                                            break;
                                        }
                                    }

                                    if !write_errors {
                                        info!(
                                            "Wrote batch to topic '{}' partition 0: {} records, base_offset={}",
                                            topic_data.name, record_count, base_offset
                                        );

                                        partition_responses.push(ProducePartitionResponse {
                                            partition_index: 0,
                                            error_code: ERROR_NONE,
                                            base_offset,
                                            log_append_time_ms: -1,
                                            log_start_offset: 0,
                                        });
                                    } else {
                                        partition_responses.push(ProducePartitionResponse {
                                            partition_index: 0,
                                            error_code: ERROR_UNKNOWN,
                                            base_offset: -1,
                                            log_append_time_ms: -1,
                                            log_start_offset: 0,
                                        });
                                    }
                                }
                                Err(e) => {
                                    // Failed to parse record batch
                                    error!(
                                        "Failed to parse record batch for topic '{}' partition {}: {}",
                                        topic_data.name, partition_data.partition_index, e
                                    );
                                    partition_responses.push(ProducePartitionResponse {
                                        partition_index: 0,
                                        error_code: ERROR_INVALID_REQUEST,
                                        base_offset: -1,
                                        log_append_time_ms: -1,
                                        log_start_offset: 0,
                                    });
                                }
                            }
                        } else {
                            // No records for this partition
                            partition_responses.push(ProducePartitionResponse {
                                partition_index: partition_data.partition_index,
                                error_code: ERROR_NONE,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: 0,
                            });
                        }
                    }
                }
                Ok(None) => {
                    // Topic doesn't exist - create it automatically
                    info!("Auto-creating topic: {}", topic_data.name);
                    let topic_metadata = crate::state::TopicMetadata {
                        name: topic_data.name.clone(),
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
                        Ok(_) => {
                            info!("Successfully created topic: {}", topic_data.name);
                            // Now process the partitions as if the topic existed
                            for partition_data in &topic_data.partitions {
                                // For now, we only support partition 0
                                if partition_data.partition_index != 0 {
                                    partition_responses.push(ProducePartitionResponse {
                                        partition_index: partition_data.partition_index,
                                        error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                                        base_offset: -1,
                                        log_append_time_ms: -1,
                                        log_start_offset: 0,
                                    });
                                    continue;
                                }

                                if !partition_data.records.is_empty() {
                                    // Parse the raw record batch bytes
                                    match crate::protocol::RecordBatch::parse(
                                        &partition_data.records,
                                    ) {
                                        Ok(record_batch) => {
                                            // Track metrics
                                            let record_count = record_batch.records.len();
                                            _total_records += record_count;
                                            _total_bytes += partition_data.records.len();

                                            // Get base offset for this batch
                                            let base_offset = match context
                                                .state_manager
                                                .messages()
                                                .assign_offsets(&topic_data.name, 0, record_count)
                                                .await
                                            {
                                                Ok(offset) => offset,
                                                Err(e) => {
                                                    error!("Failed to assign offsets: {}", e);
                                                    partition_responses.push(
                                                        ProducePartitionResponse {
                                                            partition_index: 0,
                                                            error_code: ERROR_UNKNOWN,
                                                            base_offset: -1,
                                                            log_append_time_ms: -1,
                                                            log_start_offset: 0,
                                                        },
                                                    );
                                                    continue;
                                                }
                                            };

                                            // Convert RecordBatch to individual KafkaMessages and write them
                                            let mut write_errors = false;
                                            let _now = chrono::Utc::now().timestamp_millis();

                                            for (i, record) in
                                                record_batch.records.iter().enumerate()
                                            {
                                                let headers = record
                                                    .headers
                                                    .iter()
                                                    .map(|h| (h.key.clone(), h.value.clone()))
                                                    .collect();

                                                let message = crate::storage::KafkaMessage {
                                                    topic: topic_data.name.clone(),
                                                    partition: 0,
                                                    offset: base_offset + i as i64,
                                                    key: record.key.clone(),
                                                    value: record.value.clone(),
                                                    headers,
                                                    timestamp: record_batch.base_timestamp
                                                        + record.timestamp_delta,
                                                    producer_id: Some(record_batch.producer_id),
                                                    producer_epoch: Some(
                                                        record_batch.producer_epoch,
                                                    ),
                                                    sequence: Some(
                                                        record_batch.base_sequence + i as i32,
                                                    ),
                                                };

                                                if let Err(e) =
                                                    context.batch_writer.write(message).await
                                                {
                                                    error!("Failed to write message: {}", e);
                                                    write_errors = true;
                                                    break;
                                                }
                                            }

                                            if !write_errors {
                                                info!(
                                                    "Wrote batch to topic '{}' partition 0: {} records, base_offset={}",
                                                    topic_data.name, record_count, base_offset
                                                );

                                                partition_responses.push(
                                                    ProducePartitionResponse {
                                                        partition_index: 0,
                                                        error_code: ERROR_NONE,
                                                        base_offset,
                                                        log_append_time_ms: -1,
                                                        log_start_offset: 0,
                                                    },
                                                );
                                            } else {
                                                partition_responses.push(
                                                    ProducePartitionResponse {
                                                        partition_index: 0,
                                                        error_code: ERROR_UNKNOWN,
                                                        base_offset: -1,
                                                        log_append_time_ms: -1,
                                                        log_start_offset: 0,
                                                    },
                                                );
                                            }
                                        }
                                        Err(e) => {
                                            // Failed to parse record batch
                                            error!(
                                                "Failed to parse record batch for topic '{}' partition {}: {}",
                                                topic_data.name, partition_data.partition_index, e
                                            );
                                            partition_responses.push(ProducePartitionResponse {
                                                partition_index: 0,
                                                error_code: ERROR_INVALID_REQUEST,
                                                base_offset: -1,
                                                log_append_time_ms: -1,
                                                log_start_offset: 0,
                                            });
                                        }
                                    }
                                } else {
                                    // No records for this partition
                                    partition_responses.push(ProducePartitionResponse {
                                        partition_index: partition_data.partition_index,
                                        error_code: ERROR_NONE,
                                        base_offset: -1,
                                        log_append_time_ms: -1,
                                        log_start_offset: 0,
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to create topic '{}': {}", topic_data.name, e);
                            for partition_data in &topic_data.partitions {
                                partition_responses.push(ProducePartitionResponse {
                                    partition_index: partition_data.partition_index,
                                    error_code: ERROR_UNKNOWN,
                                    base_offset: -1,
                                    log_append_time_ms: -1,
                                    log_start_offset: 0,
                                });
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to check topic '{}': {}", topic_data.name, e);
                    for partition_data in &topic_data.partitions {
                        partition_responses.push(ProducePartitionResponse {
                            partition_index: partition_data.partition_index,
                            error_code: ERROR_UNKNOWN,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: 0,
                        });
                    }
                }
            }

            topic_responses.push(ProduceTopicResponse {
                name: topic_data.name.clone(),
                partitions: partition_responses,
            });
        }

        // Update metrics
        // Update metrics - removed as these counters don't exist in ProtocolMetrics

        // Build response
        let response = ProduceResponse {
            throttle_time_ms: 0,
            responses: topic_responses,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;
        Ok(response_body)
    }

    fn api_key(&self) -> i16 {
        0 // Produce
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=9).contains(&version)
    }
}

pub struct InitProducerIdHandler;

#[async_trait::async_trait]
impl ApiHandler for InitProducerIdHandler {
    async fn handle(
        &self,
        request: &KafkaRequest,
        context: &mut HandlerContext,
    ) -> Result<Vec<u8>> {
        info!("Handling InitProducerId request v{}", request.api_version);

        // Track InitProducerId requests
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
        let init_request = InitProducerIdRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "InitProducerId request: transactional_id={:?}, timeout={}ms",
            init_request.transactional_id, init_request.transaction_timeout_ms
        );

        // Check if transactional producer
        if init_request.transactional_id.is_some() {
            warn!("Transactional producers not supported");
            let response = InitProducerIdResponse::error(ERROR_NOT_COORDINATOR);
            return response.encode(request.api_version);
        }

        // Allocate a producer ID for idempotent producer
        // For now, we'll use a simple timestamp-based ID
        let producer_id = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
        info!(
            "Allocated producer_id={} for idempotent producer",
            producer_id
        );

        // Save initial producer state
        let producer_state = crate::state::producer::ProducerState {
            producer_id,
            producer_epoch: 0,
            sequence_numbers: std::collections::HashMap::new(),
        };
        context
            .state_manager
            .producers()
            .save_producer_state(&producer_state)
            .await?;

        // Build response
        let response = InitProducerIdResponse::new(producer_id, 0);

        // Encode response
        response.encode(request.api_version)
    }

    fn api_key(&self) -> i16 {
        22 // InitProducerId
    }

    fn supports_version(&self, version: i16) -> bool {
        (0..=4).contains(&version)
    }
}
