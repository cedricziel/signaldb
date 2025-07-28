use crate::{
    config::{AuthConfig, TopicConfig},
    error::{HeraclitusError, Result},
    metrics::Metrics,
    protocol_v2::KafkaProtocolHandler,
    state::{StateManager, TopicMetadata},
    storage::{BatchWriter, MessageReader},
};
use bytes::{Buf, BytesMut};
use kafka_protocol::messages::{ApiKey, RequestKind, ResponseKind};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{error, info, warn};

pub struct ConnectionHandler {
    socket: TcpStream,
    state_manager: Arc<StateManager>,
    batch_writer: Arc<BatchWriter>,
    message_reader: Arc<MessageReader>,
    port: u16,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
    _auth_config: Arc<AuthConfig>,
    _authenticated: bool,
    _username: Option<String>,
    metrics: Arc<Metrics>,
    topic_config: Arc<TopicConfig>,
}

impl ConnectionHandler {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        socket: TcpStream,
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        message_reader: Arc<MessageReader>,
        port: u16,
        auth_config: Arc<AuthConfig>,
        metrics: Arc<Metrics>,
        topic_config: Arc<TopicConfig>,
    ) -> Self {
        // Track new connection
        metrics.connection.active_connections.inc();
        metrics.connection.total_connections.inc();

        Self {
            socket,
            state_manager,
            batch_writer,
            message_reader,
            port,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            _auth_config: auth_config.clone(),
            _authenticated: !auth_config.enabled, // If auth is disabled, consider connection authenticated
            _username: None,
            metrics,
            topic_config,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let peer_addr = self.socket.peer_addr().ok();
        info!(
            "Starting Kafka protocol handler for new connection from {:?}",
            peer_addr
        );
        let connection_start = std::time::Instant::now();
        let mut request_count = 0;

        loop {
            // Try to read a complete frame
            match self.read_frame().await {
                Ok(Some(frame)) => {
                    request_count += 1;
                    info!(
                        "Received request #{} after {}ms, frame size: {} bytes",
                        request_count,
                        connection_start.elapsed().as_millis(),
                        frame.len()
                    );

                    // Process the request and get response
                    match self.process_request(frame).await {
                        Ok(response) => {
                            // Send response back to client
                            if let Err(e) = self.write_frame(response).await {
                                error!("Failed to write response: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to process request: {}", e);
                            break;
                        }
                    }
                }
                Ok(None) => {
                    info!(
                        "Client disconnected after {} requests over {}ms",
                        request_count,
                        connection_start.elapsed().as_millis()
                    );
                    break;
                }
                Err(e) => {
                    error!("Failed to read frame: {}", e);
                    break;
                }
            }
        }

        // Track connection close
        self.metrics.connection.active_connections.dec();

        Ok(())
    }

    async fn read_frame(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            // Check if we have a complete frame
            if self.read_buffer.len() >= 4 {
                let frame_size = (&self.read_buffer[..4]).get_i32() as usize;
                info!(
                    "Read buffer has {} bytes, frame size is {}",
                    self.read_buffer.len(),
                    frame_size
                );

                if self.read_buffer.len() >= 4 + frame_size {
                    // We have a complete frame
                    self.read_buffer.advance(4); // Skip frame size
                    let frame = self.read_buffer.split_to(frame_size).to_vec();
                    info!("Extracted complete frame of {} bytes", frame.len());
                    // Log first few bytes of the frame for debugging
                    if frame.len() >= 4 {
                        info!(
                            "Frame header: api_key={}, api_version={}",
                            i16::from_be_bytes([frame[0], frame[1]]),
                            i16::from_be_bytes([frame[2], frame[3]])
                        );
                    }
                    return Ok(Some(frame));
                }
            }

            // Read more data
            let n = self.socket.read_buf(&mut self.read_buffer).await?;
            info!(
                "Read {} bytes from socket, buffer now has {} bytes",
                n,
                self.read_buffer.len()
            );
            if n == 0 {
                // EOF
                info!("Socket EOF detected");
                return Ok(None);
            }
        }
    }

    async fn write_frame(&mut self, data: Vec<u8>) -> Result<()> {
        self.write_buffer.clear();

        // Write frame size
        self.write_buffer
            .extend_from_slice(&(data.len() as i32).to_be_bytes());
        // Write frame data
        self.write_buffer.extend_from_slice(&data);

        info!(
            "Writing response: {} bytes (frame size: {}, data: {})",
            self.write_buffer.len(),
            data.len(),
            self.write_buffer.len() - 4
        );
        self.socket.write_all(&self.write_buffer).await?;
        self.socket.flush().await?;
        info!("Response sent successfully");

        Ok(())
    }

    async fn process_request(&mut self, frame: Vec<u8>) -> Result<Vec<u8>> {
        // Parse the request using kafka-protocol
        let (header, request) = KafkaProtocolHandler::parse_request(&frame).await?;

        // Track metrics
        let api_key_str = header.request_api_key.to_string();
        let api_version_str = header.request_api_version.to_string();
        let api_name = match ApiKey::try_from(header.request_api_key) {
            Ok(key) => format!("{key:?}"),
            Err(_) => "Unknown".to_string(),
        };

        self.metrics
            .protocol
            .request_count
            .with_label_values(&[&api_key_str, &api_name, &api_version_str])
            .inc();

        // Handle the request
        let response = match request {
            RequestKind::ApiVersions(_) => {
                info!("Handling ApiVersions request");
                ResponseKind::ApiVersions(KafkaProtocolHandler::create_api_versions_response())
            }
            RequestKind::Metadata(req) => {
                info!("Handling Metadata request for topics: {:?}", req.topics);
                self.handle_metadata(req).await?
            }
            RequestKind::Produce(req) => {
                info!("Handling Produce request");
                self.handle_produce(req).await?
            }
            RequestKind::Fetch(req) => {
                info!("Handling Fetch request");
                self.handle_fetch(req).await?
            }
            RequestKind::FindCoordinator(req) => {
                info!("Handling FindCoordinator request");
                self.handle_find_coordinator(req).await?
            }
            RequestKind::JoinGroup(req) => {
                info!("Handling JoinGroup request");
                self.handle_join_group(req).await?
            }
            RequestKind::SyncGroup(req) => {
                info!("Handling SyncGroup request");
                self.handle_sync_group(req).await?
            }
            RequestKind::Heartbeat(req) => {
                info!("Handling Heartbeat request");
                self.handle_heartbeat(req).await?
            }
            RequestKind::LeaveGroup(req) => {
                info!("Handling LeaveGroup request");
                self.handle_leave_group(req).await?
            }
            RequestKind::OffsetCommit(req) => {
                info!("Handling OffsetCommit request");
                self.handle_offset_commit(req).await?
            }
            RequestKind::OffsetFetch(req) => {
                info!("Handling OffsetFetch request");
                self.handle_offset_fetch(req).await?
            }
            RequestKind::ListOffsets(req) => {
                info!("Handling ListOffsets request");
                self.handle_list_offsets(req).await?
            }
            _ => {
                warn!("Unsupported request type: {:?}", header.request_api_key);
                return Err(HeraclitusError::Protocol(format!(
                    "Unsupported API key: {}",
                    header.request_api_key
                )));
            }
        };

        // Encode the response
        KafkaProtocolHandler::encode_response(&header, response).await
    }

    async fn handle_metadata(
        &self,
        req: kafka_protocol::messages::MetadataRequest,
    ) -> Result<ResponseKind> {
        use kafka_protocol::messages::metadata_response::{
            MetadataResponseBroker, MetadataResponsePartition, MetadataResponseTopic,
        };

        let mut topic_responses = vec![];

        // Handle requested topics
        if let Some(requested_topics) = &req.topics {
            if requested_topics.is_empty() {
                // Empty list means client wants metadata for all topics
                info!("Client requested metadata for all topics");

                // Get all existing topics
                let all_topic_names = self.state_manager.metadata().list_topics().await?;
                info!("Found {} existing topics", all_topic_names.len());

                for topic_name in all_topic_names {
                    // Get topic metadata
                    if let Some(topic_metadata) =
                        self.state_manager.metadata().get_topic(&topic_name).await?
                    {
                        // Build partition responses
                        let mut partitions = vec![];
                        for partition_id in 0..topic_metadata.partitions {
                            partitions.push(
                                MetadataResponsePartition::default()
                                    .with_error_code(0)
                                    .with_partition_index(partition_id)
                                    .with_leader_id(kafka_protocol::messages::BrokerId(1))
                                    .with_leader_epoch(0)
                                    .with_replica_nodes(vec![kafka_protocol::messages::BrokerId(1)])
                                    .with_isr_nodes(vec![kafka_protocol::messages::BrokerId(1)])
                                    .with_offline_replicas(vec![]),
                            );
                        }

                        topic_responses.push(
                            MetadataResponseTopic::default()
                                .with_error_code(0)
                                .with_name(Some(kafka_protocol::messages::TopicName(
                                    kafka_protocol::protocol::StrBytes::from_string(
                                        topic_metadata.name,
                                    ),
                                )))
                                .with_is_internal(false)
                                .with_partitions(partitions),
                        );
                    }
                }
            } else {
                // Specific topics requested
                info!(
                    "Client requested metadata for {} specific topics",
                    requested_topics.len()
                );
                for topic_req in requested_topics {
                    let topic_name = match &topic_req.name {
                        Some(name) => name.as_str(),
                        None => {
                            info!("Topic request with no name, skipping");
                            continue; // Skip if no topic name
                        }
                    };
                    info!("Processing metadata request for topic: '{}'", topic_name);

                    // Try to get existing topic
                    let topic_metadata =
                        match self.state_manager.metadata().get_topic(topic_name).await? {
                            Some(metadata) => metadata,
                            None => {
                                // Check if we should auto-create the topic
                                if req.allow_auto_topic_creation
                                    && self.topic_config.auto_create_topics_enable
                                {
                                    // Auto-create the topic
                                    let new_topic = TopicMetadata {
                                        name: topic_name.to_string(),
                                        partitions: self.topic_config.default_num_partitions,
                                        replication_factor: self
                                            .topic_config
                                            .default_replication_factor,
                                        config: std::collections::HashMap::new(),
                                        created_at: chrono::Utc::now(),
                                    };

                                    self.state_manager
                                        .metadata()
                                        .create_topic(new_topic.clone())
                                        .await?;
                                    info!(
                                        "Auto-created topic '{}' with {} partitions",
                                        topic_name, new_topic.partitions
                                    );
                                    new_topic
                                } else {
                                    // Topic doesn't exist and can't be auto-created
                                    topic_responses.push(
                                        MetadataResponseTopic::default()
                                            .with_error_code(3) // UNKNOWN_TOPIC
                                            .with_name(topic_req.name.clone())
                                            .with_is_internal(false)
                                            .with_partitions(vec![]),
                                    );
                                    continue;
                                }
                            }
                        };

                    // Build partition responses
                    let mut partitions = vec![];
                    for partition_id in 0..topic_metadata.partitions {
                        partitions.push(
                            MetadataResponsePartition::default()
                                .with_error_code(0)
                                .with_partition_index(partition_id)
                                .with_leader_id(kafka_protocol::messages::BrokerId(1))
                                .with_leader_epoch(0)
                                .with_replica_nodes(vec![kafka_protocol::messages::BrokerId(1)])
                                .with_isr_nodes(vec![kafka_protocol::messages::BrokerId(1)])
                                .with_offline_replicas(vec![]),
                        );
                    }

                    topic_responses.push(
                        MetadataResponseTopic::default()
                            .with_error_code(0)
                            .with_name(Some(kafka_protocol::messages::TopicName(
                                kafka_protocol::protocol::StrBytes::from_string(
                                    topic_metadata.name,
                                ),
                            )))
                            .with_is_internal(false)
                            .with_partitions(partitions),
                    );
                }
            }
        }

        let response = kafka_protocol::messages::MetadataResponse::default()
            .with_throttle_time_ms(0)
            .with_brokers(vec![
                MetadataResponseBroker::default()
                    .with_node_id(kafka_protocol::messages::BrokerId(1))
                    .with_host(kafka_protocol::protocol::StrBytes::from_static_str(
                        "127.0.0.1",
                    ))
                    .with_port(self.port as i32)
                    .with_rack(None),
            ])
            .with_cluster_id(Some(kafka_protocol::protocol::StrBytes::from_static_str(
                "heraclitus",
            )))
            .with_controller_id(kafka_protocol::messages::BrokerId(1))
            .with_topics(topic_responses.clone());

        info!(
            "Metadata response: {} topics, broker at 127.0.0.1:{}",
            topic_responses.len(),
            self.port
        );

        Ok(ResponseKind::Metadata(response))
    }

    async fn handle_produce(
        &mut self,
        req: kafka_protocol::messages::ProduceRequest,
    ) -> Result<ResponseKind> {
        use crate::storage::{KafkaMessage, KafkaMessageBatch};
        use kafka_protocol::messages::produce_response::{
            PartitionProduceResponse, TopicProduceResponse,
        };
        use kafka_protocol::records::RecordBatchDecoder;

        let mut topic_responses = vec![];
        let mut message_batch = KafkaMessageBatch::new();

        // Process each topic's data
        for topic_data in &req.topic_data {
            let topic_name = topic_data.name.as_str();
            let mut partition_responses = vec![];

            // Process each partition's data
            for partition_data in &topic_data.partition_data {
                let partition_index = partition_data.index;
                let mut base_offset = 0i64;
                let mut assigned_base_offset = None;
                let mut partition_error_code = 0i16;

                // Extract messages from the records
                if let Some(records_bytes) = &partition_data.records {
                    // Decode the record batch
                    let mut records_buf = records_bytes.clone();
                    match RecordBatchDecoder::decode_with_custom_compression(
                        &mut records_buf,
                        Some(decompress_record_batch_data),
                    ) {
                        Ok(record_set) => {
                            // Assign offsets for this batch if not already done
                            if assigned_base_offset.is_none() {
                                let record_count = record_set.records.len();
                                if record_count > 0 {
                                    base_offset = self
                                        .state_manager
                                        .messages()
                                        .assign_offsets(topic_name, partition_index, record_count)
                                        .await
                                        .unwrap_or(0);
                                    assigned_base_offset = Some(base_offset);
                                }
                            }

                            // Process each record in the batch
                            for record in &record_set.records {
                                let offset = base_offset;
                                base_offset += 1;

                                let message = KafkaMessage {
                                    topic: topic_name.to_string(),
                                    partition: partition_index,
                                    offset,
                                    timestamp: record.timestamp,
                                    key: record.key.clone().map(|k| k.to_vec()),
                                    value: record
                                        .value
                                        .clone()
                                        .map(|v| v.to_vec())
                                        .unwrap_or_default(),
                                    headers: record
                                        .headers
                                        .iter()
                                        .map(|(k, v)| {
                                            (
                                                k.as_str().to_string(),
                                                v.clone().map(|v| v.to_vec()).unwrap_or_default(),
                                            )
                                        })
                                        .collect(),
                                    producer_id: if record.producer_id
                                        == kafka_protocol::records::NO_PRODUCER_ID
                                    {
                                        None
                                    } else {
                                        Some(record.producer_id)
                                    },
                                    producer_epoch: if record.producer_epoch
                                        == kafka_protocol::records::NO_PRODUCER_EPOCH
                                    {
                                        None
                                    } else {
                                        Some(record.producer_epoch)
                                    },
                                    sequence: if record.sequence
                                        == kafka_protocol::records::NO_SEQUENCE
                                    {
                                        None
                                    } else {
                                        Some(record.sequence)
                                    },
                                };

                                message_batch.add(message);
                            }
                        }
                        Err(e) => {
                            warn!("Failed to decode record batch: {e}");
                            partition_error_code = 1; // OFFSET_OUT_OF_RANGE or similar
                        }
                    }
                }

                // Create partition response
                let response_base_offset: i64 = assigned_base_offset.unwrap_or_default();

                let partition_resp = PartitionProduceResponse::default()
                    .with_index(partition_index)
                    .with_error_code(partition_error_code)
                    .with_base_offset(response_base_offset)
                    .with_log_append_time_ms(-1)
                    .with_log_start_offset(0);

                partition_responses.push(partition_resp);
            }

            let topic_resp = TopicProduceResponse::default()
                .with_name(topic_data.name.clone())
                .with_partition_responses(partition_responses);

            topic_responses.push(topic_resp);
        }

        // Write messages to storage
        for message in message_batch.messages {
            self.batch_writer.write(message).await?;
        }

        let response = kafka_protocol::messages::ProduceResponse::default()
            .with_responses(topic_responses)
            .with_throttle_time_ms(0);

        Ok(ResponseKind::Produce(response))
    }

    async fn handle_fetch(
        &self,
        req: kafka_protocol::messages::FetchRequest,
    ) -> Result<ResponseKind> {
        use kafka_protocol::messages::fetch_response::{FetchableTopicResponse, PartitionData};

        let mut topic_responses = vec![];

        for topic_req in &req.topics {
            let topic_name = topic_req.topic.as_str();
            let mut partition_responses = vec![];

            for partition_req in &topic_req.partitions {
                let partition_id = partition_req.partition;
                let fetch_offset = partition_req.fetch_offset;

                info!(
                    "Fetch request for topic '{}' partition {} starting at offset {}",
                    topic_name, partition_id, fetch_offset
                );

                // Get the high water mark for this partition
                let high_water_mark = self
                    .state_manager
                    .messages()
                    .get_high_water_mark(topic_name, partition_id)
                    .await
                    .unwrap_or(0);

                // Retrieve messages from storage
                let messages = match self
                    .message_reader
                    .read_messages(
                        topic_name,
                        partition_id,
                        fetch_offset,
                        100, // Max messages per fetch
                    )
                    .await
                {
                    Ok(msgs) => msgs,
                    Err(e) => {
                        warn!("Failed to read messages: {e}");
                        vec![]
                    }
                };

                let record_batch = if messages.is_empty() {
                    None
                } else {
                    // For now, create a simple record batch manually
                    // This is a temporary implementation until we figure out the correct kafka-protocol API
                    info!(
                        "Found {} messages to return in fetch response",
                        messages.len()
                    );

                    // Create a minimal record batch with the messages
                    // Format: [baseOffset][batchLength][partitionLeaderEpoch][magic][crc][attributes][lastOffsetDelta][firstTimestamp][maxTimestamp][producerId][producerEpoch][baseSequence][records]
                    let mut batch_bytes = Vec::new();

                    // Base offset (8 bytes)
                    batch_bytes.extend_from_slice(&messages[0].offset.to_be_bytes());

                    // Placeholder for batch length (4 bytes) - will update later
                    let batch_length_pos = batch_bytes.len();
                    batch_bytes.extend_from_slice(&0i32.to_be_bytes());

                    // Partition leader epoch (4 bytes)
                    batch_bytes.extend_from_slice(&(-1i32).to_be_bytes());

                    // Magic byte (1 byte) - version 2
                    batch_bytes.push(2);

                    // CRC placeholder (4 bytes)
                    batch_bytes.extend_from_slice(&0u32.to_be_bytes());

                    // Attributes (2 bytes) - no compression
                    batch_bytes.extend_from_slice(&0u16.to_be_bytes());

                    // Last offset delta (4 bytes)
                    let last_offset_delta =
                        (messages.last().unwrap().offset - messages[0].offset) as i32;
                    batch_bytes.extend_from_slice(&last_offset_delta.to_be_bytes());

                    // First timestamp (8 bytes)
                    batch_bytes.extend_from_slice(&messages[0].timestamp.to_be_bytes());

                    // Max timestamp (8 bytes)
                    let max_timestamp = messages.iter().map(|m| m.timestamp).max().unwrap();
                    batch_bytes.extend_from_slice(&max_timestamp.to_be_bytes());

                    // Producer ID (8 bytes)
                    batch_bytes
                        .extend_from_slice(&messages[0].producer_id.unwrap_or(-1).to_be_bytes());

                    // Producer epoch (2 bytes)
                    batch_bytes
                        .extend_from_slice(&messages[0].producer_epoch.unwrap_or(-1).to_be_bytes());

                    // Base sequence (4 bytes)
                    batch_bytes
                        .extend_from_slice(&messages[0].sequence.unwrap_or(-1).to_be_bytes());

                    // Record count (4 bytes)
                    batch_bytes.extend_from_slice(&(messages.len() as i32).to_be_bytes());

                    // Records - simplified encoding
                    for msg in &messages {
                        // Record attributes (1 byte)
                        batch_bytes.push(0);

                        // Timestamp delta (varlong)
                        let timestamp_delta = msg.timestamp - messages[0].timestamp;
                        Self::write_varint(&mut batch_bytes, timestamp_delta);

                        // Offset delta (varint)
                        let offset_delta = (msg.offset - messages[0].offset) as i64;
                        Self::write_varint(&mut batch_bytes, offset_delta);

                        // Key length and key
                        if let Some(key) = &msg.key {
                            Self::write_varint(&mut batch_bytes, key.len() as i64);
                            batch_bytes.extend_from_slice(key);
                        } else {
                            Self::write_varint(&mut batch_bytes, -1);
                        }

                        // Value length and value
                        Self::write_varint(&mut batch_bytes, msg.value.len() as i64);
                        batch_bytes.extend_from_slice(&msg.value);

                        // Headers array length
                        Self::write_varint(&mut batch_bytes, msg.headers.len() as i64);

                        // Headers
                        for (k, v) in &msg.headers {
                            Self::write_varint(&mut batch_bytes, k.len() as i64);
                            batch_bytes.extend_from_slice(k.as_bytes());
                            Self::write_varint(&mut batch_bytes, v.len() as i64);
                            batch_bytes.extend_from_slice(v);
                        }
                    }

                    // Update batch length
                    let batch_length = (batch_bytes.len() - batch_length_pos - 4) as i32;
                    batch_bytes[batch_length_pos..batch_length_pos + 4]
                        .copy_from_slice(&batch_length.to_be_bytes());

                    Some(bytes::Bytes::from(batch_bytes))
                };

                let partition_resp = PartitionData::default()
                    .with_partition_index(partition_id)
                    .with_error_code(0)
                    .with_high_watermark(high_water_mark)
                    .with_last_stable_offset(high_water_mark)
                    .with_log_start_offset(0)
                    .with_records(record_batch);

                partition_responses.push(partition_resp);
            }

            let topic_resp = FetchableTopicResponse::default()
                .with_topic(topic_req.topic.clone())
                .with_partitions(partition_responses);

            topic_responses.push(topic_resp);
        }

        let response = kafka_protocol::messages::FetchResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_session_id(req.session_id)
            .with_responses(topic_responses);

        Ok(ResponseKind::Fetch(response))
    }

    async fn handle_find_coordinator(
        &self,
        _req: kafka_protocol::messages::FindCoordinatorRequest,
    ) -> Result<ResponseKind> {
        // Return this broker as the coordinator
        let response = kafka_protocol::messages::FindCoordinatorResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_error_message(None)
            .with_node_id(kafka_protocol::messages::BrokerId(1))
            .with_host(kafka_protocol::protocol::StrBytes::from_static_str(
                "127.0.0.1",
            ))
            .with_port(self.port as i32);

        Ok(ResponseKind::FindCoordinator(response))
    }

    async fn handle_join_group(
        &mut self,
        req: kafka_protocol::messages::JoinGroupRequest,
    ) -> Result<ResponseKind> {
        // For now, simple implementation - just accept the join
        let member_id = if req.member_id.is_empty() {
            // Generate new member ID
            kafka_protocol::protocol::StrBytes::from_string(format!(
                "consumer-{}-{}",
                req.group_id.as_str(),
                uuid::Uuid::new_v4()
            ))
        } else {
            req.member_id.clone()
        };

        let response = kafka_protocol::messages::JoinGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_generation_id(1)
            .with_protocol_type(Some(kafka_protocol::protocol::StrBytes::from_static_str(
                "consumer",
            )))
            .with_protocol_name(Some(kafka_protocol::protocol::StrBytes::from_static_str(
                "range",
            )))
            .with_leader(member_id.clone())
            .with_member_id(member_id)
            .with_members(vec![]);

        Ok(ResponseKind::JoinGroup(response))
    }

    async fn handle_sync_group(
        &self,
        req: kafka_protocol::messages::SyncGroupRequest,
    ) -> Result<ResponseKind> {
        // For now, implement a simple partition assignment
        // In a real implementation, this would be based on the group protocol
        // and the assignments provided by the group leader

        info!(
            "SyncGroup request for group {} member {}",
            req.group_id.as_str(),
            req.member_id.as_str()
        );

        // For now, return an empty assignment to debug the consumer flow
        // A proper implementation would parse the leader's assignment
        let assignment_bytes = Vec::new();

        let response = kafka_protocol::messages::SyncGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_protocol_type(Some(kafka_protocol::protocol::StrBytes::from_static_str(
                "consumer",
            )))
            .with_protocol_name(Some(kafka_protocol::protocol::StrBytes::from_static_str(
                "range",
            )))
            .with_assignment(bytes::Bytes::from(assignment_bytes));

        Ok(ResponseKind::SyncGroup(response))
    }

    async fn handle_heartbeat(
        &self,
        _req: kafka_protocol::messages::HeartbeatRequest,
    ) -> Result<ResponseKind> {
        let response = kafka_protocol::messages::HeartbeatResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0);

        Ok(ResponseKind::Heartbeat(response))
    }

    async fn handle_leave_group(
        &self,
        _req: kafka_protocol::messages::LeaveGroupRequest,
    ) -> Result<ResponseKind> {
        let response = kafka_protocol::messages::LeaveGroupResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0);

        Ok(ResponseKind::LeaveGroup(response))
    }

    async fn handle_offset_commit(
        &mut self,
        _req: kafka_protocol::messages::OffsetCommitRequest,
    ) -> Result<ResponseKind> {
        // For now, just acknowledge the commit
        let response = kafka_protocol::messages::OffsetCommitResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(vec![]);

        Ok(ResponseKind::OffsetCommit(response))
    }

    async fn handle_offset_fetch(
        &self,
        _req: kafka_protocol::messages::OffsetFetchRequest,
    ) -> Result<ResponseKind> {
        // Return no committed offsets for now
        let response = kafka_protocol::messages::OffsetFetchResponse::default()
            .with_throttle_time_ms(0)
            .with_error_code(0)
            .with_topics(vec![]);

        Ok(ResponseKind::OffsetFetch(response))
    }

    async fn handle_list_offsets(
        &self,
        req: kafka_protocol::messages::ListOffsetsRequest,
    ) -> Result<ResponseKind> {
        use kafka_protocol::messages::list_offsets_response::{
            ListOffsetsPartitionResponse, ListOffsetsTopicResponse,
        };

        // Build response for each topic/partition
        let mut topic_responses = vec![];

        for topic_req in &req.topics {
            let mut partition_responses = vec![];

            for partition_req in &topic_req.partitions {
                // For now, return offset 0 for earliest, 1000 for latest
                let offset = if partition_req.timestamp == -2 {
                    0 // Earliest
                } else {
                    1000 // Latest
                };

                let partition_resp = ListOffsetsPartitionResponse::default()
                    .with_partition_index(partition_req.partition_index)
                    .with_error_code(0)
                    .with_timestamp(0)
                    .with_offset(offset);

                partition_responses.push(partition_resp);
            }

            let topic_resp = ListOffsetsTopicResponse::default()
                .with_name(topic_req.name.clone())
                .with_partitions(partition_responses);

            topic_responses.push(topic_resp);
        }

        let response = kafka_protocol::messages::ListOffsetsResponse::default()
            .with_throttle_time_ms(0)
            .with_topics(topic_responses);

        Ok(ResponseKind::ListOffsets(response))
    }

    fn write_varint(buffer: &mut Vec<u8>, value: i64) {
        // Zigzag encode signed to unsigned
        let mut encoded = if value < 0 {
            (((-value) << 1) - 1) as u64
        } else {
            (value << 1) as u64
        };

        // Write varint
        while encoded >= 0x80 {
            buffer.push((encoded as u8) | 0x80);
            encoded >>= 7;
        }
        buffer.push(encoded as u8);
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        // Cleanup is handled in the run() method
    }
}

// Helper function for decompressing record batch data
fn decompress_record_batch_data(
    compressed_buffer: &mut bytes::Bytes,
    compression: kafka_protocol::records::Compression,
) -> anyhow::Result<bytes::Bytes> {
    match compression {
        kafka_protocol::records::Compression::None => Ok(compressed_buffer.to_vec().into()),
        _ => {
            // TODO: Implement compression support
            anyhow::bail!("Compression {:?} not yet implemented", compression)
        }
    }
}
