use crate::{error::Result, state::StateManager, storage::BatchWriter};
use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

pub struct ConnectionHandler {
    socket: TcpStream,
    state_manager: Arc<StateManager>,
    batch_writer: Arc<BatchWriter>,
    port: u16,
    read_buffer: BytesMut,
    write_buffer: BytesMut,
}

impl ConnectionHandler {
    pub fn new(
        socket: TcpStream,
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        port: u16,
    ) -> Self {
        Self {
            socket,
            state_manager,
            batch_writer,
            port,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting Kafka protocol handler for new connection");

        loop {
            // Try to read a complete frame
            match self.read_frame().await {
                Ok(Some(frame)) => {
                    // Process the request and get response
                    match self.process_request(frame).await {
                        Ok(response) => {
                            // Send response back to client
                            if let Err(e) = self.write_frame(response).await {
                                error!("Failed to write response: {e}");
                                return Err(e);
                            }
                        }
                        Err(e) => {
                            error!("Failed to process request: {e}");
                            // Try to send error response
                            let error_response = self.create_error_response(e);
                            if let Err(write_err) = self.write_frame(error_response).await {
                                error!("Failed to write error response: {write_err}");
                                return Err(write_err);
                            }
                        }
                    }
                }
                Ok(None) => {
                    // Connection closed by client
                    info!("Client closed connection");
                    return Ok(());
                }
                Err(e) => {
                    error!("Failed to read frame: {e}");
                    return Err(e);
                }
            }
        }
    }

    /// Read a complete Kafka frame from the socket
    /// Returns None if the connection is closed
    async fn read_frame(&mut self) -> Result<Option<Vec<u8>>> {
        loop {
            // Check if we have a complete frame in the buffer
            if self.read_buffer.len() >= 4 {
                // Peek at the frame length
                let mut cursor = Cursor::new(&self.read_buffer[..]);
                let frame_length = cursor.get_i32() as usize;

                // Check if we have the complete frame
                if self.read_buffer.len() >= 4 + frame_length {
                    // Extract the frame
                    self.read_buffer.advance(4); // Skip length prefix
                    let frame = self.read_buffer.split_to(frame_length).to_vec();
                    return Ok(Some(frame));
                }
            }

            // Read more data from socket
            let n = self.socket.read_buf(&mut self.read_buffer).await?;
            if n == 0 {
                // Connection closed
                if !self.read_buffer.is_empty() {
                    warn!("Connection closed with incomplete frame in buffer");
                }
                return Ok(None);
            }
            debug!("Read {n} bytes from socket");
        }
    }

    /// Write a frame to the socket
    async fn write_frame(&mut self, data: Vec<u8>) -> Result<()> {
        // Clear write buffer
        self.write_buffer.clear();

        // Write frame length
        self.write_buffer
            .extend_from_slice(&(data.len() as i32).to_be_bytes());

        // Write frame data
        self.write_buffer.extend_from_slice(&data);

        // Send to socket
        self.socket.write_all(&self.write_buffer).await?;
        self.socket.flush().await?;

        debug!("Wrote frame of {} bytes", data.len());
        Ok(())
    }

    /// Process a Kafka request and return the response
    async fn process_request(&mut self, frame: Vec<u8>) -> Result<Vec<u8>> {
        // Parse request header
        let mut cursor = Cursor::new(&frame[..]);
        let header = crate::protocol::kafka_protocol::read_request_header(&mut cursor)?;

        info!(
            "Processing request: api_key={}, api_version={}, correlation_id={}, client_id={:?}",
            header.api_key, header.api_version, header.correlation_id, header.client_id
        );

        // Get remaining bytes as request body
        let body_start = cursor.position() as usize;
        let body = frame[body_start..].to_vec();

        // Create request object
        let request = crate::protocol::request::KafkaRequest::new(
            header.api_key,
            header.api_version,
            header.correlation_id,
            header.client_id,
            body,
        )?;

        // Dispatch to appropriate handler
        self.handle_request(request).await
    }

    /// Handle a specific Kafka request type
    async fn handle_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::request::RequestType;

        // For now, return unsupported operation error for all requests
        // This will be implemented incrementally
        match request.request_type {
            RequestType::ApiVersions => {
                debug!("Handling API versions request");
                self.handle_api_versions_request(request).await
            }
            RequestType::Metadata => {
                debug!("Handling metadata request");
                self.handle_metadata_request(request).await
            }
            RequestType::Produce => {
                debug!("Handling produce request");
                self.handle_produce_request(request).await
            }
            RequestType::Fetch => {
                debug!("Handling fetch request");
                self.handle_fetch_request(request).await
            }
            _ => {
                warn!(
                    "Request type {:?} not yet implemented",
                    request.request_type
                );
                self.create_unsupported_response(request.correlation_id)
            }
        }
    }

    /// Create an error response for a given error
    fn create_error_response(&self, _error: crate::error::HeraclitusError) -> Vec<u8> {
        // For now, return empty response
        // TODO: Implement proper error response based on request context
        vec![]
    }

    /// Create an unsupported operation response
    fn create_unsupported_response(&self, correlation_id: i32) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;

        let mut response = BytesMut::new();

        // Write response header
        write_response_header(&mut response, correlation_id);

        // Write error code for unsupported operation
        response.extend_from_slice(&ERROR_UNSUPPORTED_VERSION.to_be_bytes());

        Ok(response.to_vec())
    }

    /// Handle API versions request
    async fn handle_api_versions_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::api_versions::{ApiVersionsRequest, ApiVersionsResponse};
        use crate::protocol::kafka_protocol::*;

        // Parse API versions request
        let mut cursor = Cursor::new(&request.body[..]);
        let _api_req = ApiVersionsRequest::parse(&mut cursor, request.api_version)?;

        // Build response
        let response = ApiVersionsResponse::new();

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle metadata request
    async fn handle_metadata_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::metadata::{
            BrokerMetadata, MetadataRequest, MetadataResponse, PartitionMetadata,
            TopicMetadata as ProtoTopicMetadata,
        };

        // Parse metadata request
        let mut cursor = Cursor::new(&request.body[..]);
        let metadata_req = MetadataRequest::parse(&mut cursor, request.api_version)?;

        debug!("Metadata request for topics: {:?}", metadata_req.topics);

        // Build broker metadata
        // Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        let broker_host = "127.0.0.1".to_string();
        let broker_port = self.port;

        // Get topics from state manager
        let mut topics = Vec::new();

        match &metadata_req.topics {
            Some(topic_names) => {
                // Specific topics requested
                for topic_name in topic_names {
                    match self.state_manager.metadata().get_topic(topic_name).await {
                        Ok(Some(topic_meta)) => {
                            topics.push(ProtoTopicMetadata {
                                error_code: ERROR_NONE,
                                name: topic_meta.name.clone(),
                                partitions: (0..topic_meta.partitions)
                                    .map(|partition_id| PartitionMetadata {
                                        error_code: ERROR_NONE,
                                        partition_id,
                                        leader: 0, // This broker is the leader for all partitions
                                        replicas: vec![0],
                                        isr: vec![0],
                                    })
                                    .collect(),
                            });
                        }
                        Ok(None) => {
                            // Topic not found - auto-create if enabled
                            // TODO: Make this configurable via auto.create.topics.enable
                            info!("Auto-creating topic {} from metadata request", topic_name);

                            // Create topic with 1 partition for simplicity
                            use crate::state::TopicMetadata;
                            use std::collections::HashMap;
                            let new_metadata = TopicMetadata {
                                name: topic_name.clone(),
                                partitions: 1,
                                replication_factor: 1,
                                config: HashMap::new(),
                                created_at: chrono::Utc::now(),
                            };

                            match self
                                .state_manager
                                .metadata()
                                .create_topic(new_metadata.clone())
                                .await
                            {
                                Ok(_) => {
                                    topics.push(ProtoTopicMetadata {
                                        error_code: ERROR_NONE,
                                        name: topic_name.clone(),
                                        partitions: vec![PartitionMetadata {
                                            error_code: ERROR_NONE,
                                            partition_id: 0,
                                            leader: 0,
                                            replicas: vec![0],
                                            isr: vec![0],
                                        }],
                                    });
                                }
                                Err(e) => {
                                    error!("Failed to auto-create topic {}: {}", topic_name, e);
                                    topics.push(ProtoTopicMetadata {
                                        error_code: ERROR_TOPIC_NOT_FOUND,
                                        name: topic_name.clone(),
                                        partitions: vec![],
                                    });
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to get topic metadata for {}: {}", topic_name, e);
                            topics.push(ProtoTopicMetadata {
                                error_code: ERROR_UNKNOWN,
                                name: topic_name.clone(),
                                partitions: vec![],
                            });
                        }
                    }
                }
            }
            None => {
                // All topics requested
                match self.state_manager.metadata().list_topics().await {
                    Ok(topic_names) => {
                        for topic_name in topic_names {
                            match self.state_manager.metadata().get_topic(&topic_name).await {
                                Ok(Some(topic_meta)) => {
                                    topics.push(ProtoTopicMetadata {
                                        error_code: ERROR_NONE,
                                        name: topic_meta.name.clone(),
                                        partitions: (0..topic_meta.partitions)
                                            .map(|partition_id| PartitionMetadata {
                                                error_code: ERROR_NONE,
                                                partition_id,
                                                leader: 0,
                                                replicas: vec![0],
                                                isr: vec![0],
                                            })
                                            .collect(),
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
            brokers: vec![BrokerMetadata {
                node_id: 0,
                host: broker_host,
                port: broker_port as i32,
            }],
            topics,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle produce request
    async fn handle_produce_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::produce::{
            ProducePartitionResponse, ProduceRequest, ProduceResponse, ProduceTopicResponse,
        };
        use crate::storage::KafkaMessage;

        // Parse produce request
        let mut cursor = Cursor::new(&request.body[..]);
        let produce_req = ProduceRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "Produce request: acks={}, timeout_ms={}, topics={}",
            produce_req.acks,
            produce_req.timeout_ms,
            produce_req.topics.len()
        );

        // Validate acks value
        if produce_req.acks < -1 || produce_req.acks > 1 {
            return self.create_error_produce_response(
                request.correlation_id,
                ERROR_INVALID_REQUIRED_ACKS,
            );
        }

        // Process each topic
        let mut topic_responses = Vec::new();

        for topic_data in produce_req.topics {
            let topic_name = &topic_data.name;

            // Check if topic exists (auto-create if needed)
            let topic_metadata = match self.state_manager.metadata().get_topic(topic_name).await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => {
                    // Auto-create topic with default partitions
                    info!("Auto-creating topic {}", topic_name);

                    // Create topic with 1 partition for simplicity
                    use crate::state::TopicMetadata;
                    use std::collections::HashMap;
                    let new_metadata = TopicMetadata {
                        name: topic_name.clone(),
                        partitions: 1,
                        replication_factor: 1,
                        config: HashMap::new(),
                        created_at: chrono::Utc::now(),
                    };

                    match self
                        .state_manager
                        .metadata()
                        .create_topic(new_metadata.clone())
                        .await
                    {
                        Ok(_) => new_metadata,
                        Err(e) => {
                            error!("Failed to auto-create topic {}: {}", topic_name, e);
                            topic_responses.push(ProduceTopicResponse {
                                name: topic_name.clone(),
                                partitions: vec![ProducePartitionResponse {
                                    partition_index: 0,
                                    error_code: ERROR_UNKNOWN,
                                    base_offset: -1,
                                    log_append_time_ms: -1,
                                    log_start_offset: -1,
                                }],
                            });
                            continue;
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get topic metadata: {}", e);
                    topic_responses.push(ProduceTopicResponse {
                        name: topic_name.clone(),
                        partitions: vec![ProducePartitionResponse {
                            partition_index: 0,
                            error_code: ERROR_UNKNOWN,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: -1,
                        }],
                    });
                    continue;
                }
            };

            // Process each partition
            let mut partition_responses = Vec::new();

            for partition_data in topic_data.partitions {
                let partition_index = partition_data.partition_index;

                // Validate partition exists
                if partition_index < 0 || partition_index >= topic_metadata.partitions {
                    partition_responses.push(ProducePartitionResponse {
                        partition_index,
                        error_code: ERROR_TOPIC_NOT_FOUND,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                    });
                    continue;
                }

                // Parse record batch
                if partition_data.records.is_empty() {
                    // Empty batch
                    partition_responses.push(ProducePartitionResponse {
                        partition_index,
                        error_code: ERROR_NONE,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                    });
                    continue;
                }

                // Parse the RecordBatch v2 format
                let record_batch =
                    match crate::protocol::RecordBatch::parse(&partition_data.records) {
                        Ok(batch) => batch,
                        Err(e) => {
                            error!("Failed to parse record batch: {}", e);
                            partition_responses.push(ProducePartitionResponse {
                                partition_index,
                                error_code: ERROR_INVALID_REQUEST,
                                base_offset: -1,
                                log_append_time_ms: -1,
                                log_start_offset: -1,
                            });
                            continue;
                        }
                    };

                // Validate producer ID/epoch if present
                if record_batch.producer_id >= 0 {
                    let valid = self
                        .state_manager
                        .messages()
                        .validate_producer_sequence(
                            record_batch.producer_id,
                            record_batch.producer_epoch,
                            topic_name,
                            partition_index,
                            record_batch.base_sequence,
                        )
                        .await?;

                    if !valid {
                        partition_responses.push(ProducePartitionResponse {
                            partition_index,
                            error_code: ERROR_INVALID_REQUEST,
                            base_offset: -1,
                            log_append_time_ms: -1,
                            log_start_offset: -1,
                        });
                        continue;
                    }
                }

                // Assign offsets for all messages in the batch
                let message_count = record_batch.records.len();
                if message_count == 0 {
                    partition_responses.push(ProducePartitionResponse {
                        partition_index,
                        error_code: ERROR_NONE,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: 0,
                    });
                    continue;
                }

                let base_offset = self
                    .state_manager
                    .messages()
                    .assign_offsets(topic_name, partition_index, message_count)
                    .await?;

                // Convert records to KafkaMessages and write them
                let mut write_errors = false;
                for (i, record) in record_batch.records.iter().enumerate() {
                    let offset = base_offset + i as i64;
                    let timestamp = record_batch.record_timestamp(record);
                    let headers = crate::protocol::RecordBatch::headers_map(record);

                    let message = KafkaMessage {
                        topic: topic_name.clone(),
                        partition: partition_index,
                        offset,
                        timestamp,
                        key: record.key.clone(),
                        value: record.value.clone(),
                        headers,
                        producer_id: if record_batch.producer_id >= 0 {
                            Some(record_batch.producer_id)
                        } else {
                            None
                        },
                        producer_epoch: if record_batch.producer_id >= 0 {
                            Some(record_batch.producer_epoch)
                        } else {
                            None
                        },
                        sequence: if record_batch.producer_id >= 0 {
                            Some(record_batch.base_sequence + i as i32)
                        } else {
                            None
                        },
                    };

                    if let Err(e) = self.batch_writer.write(message).await {
                        error!("Failed to write message: {}", e);
                        write_errors = true;
                        break;
                    }
                }

                if write_errors {
                    partition_responses.push(ProducePartitionResponse {
                        partition_index,
                        error_code: ERROR_UNKNOWN,
                        base_offset: -1,
                        log_append_time_ms: -1,
                        log_start_offset: -1,
                    });
                    continue;
                }

                // Get log start offset
                let log_start_offset = self
                    .state_manager
                    .messages()
                    .get_log_start_offset(topic_name, partition_index)
                    .await
                    .unwrap_or(0);

                partition_responses.push(ProducePartitionResponse {
                    partition_index,
                    error_code: ERROR_NONE,
                    base_offset,
                    log_append_time_ms: record_batch.max_timestamp,
                    log_start_offset,
                });
            }

            topic_responses.push(ProduceTopicResponse {
                name: topic_name.clone(),
                partitions: partition_responses,
            });
        }

        let response = ProduceResponse {
            responses: topic_responses,
            throttle_time_ms: 0,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Create an error produce response
    fn create_error_produce_response(
        &self,
        correlation_id: i32,
        error_code: i16,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;

        let mut response = BytesMut::new();

        // Write response header
        write_response_header(&mut response, correlation_id);

        // Write empty topics array
        response.put_i32(0);

        // Write throttle_time_ms
        response.put_i32(0);

        // Write the global error code
        response.put_i16(error_code);

        Ok(response.to_vec())
    }

    /// Handle fetch request
    async fn handle_fetch_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::fetch::{
            FetchPartitionResponse, FetchRequest, FetchResponse, FetchTopicResponse,
        };
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::{CompressionType, RecordBatchBuilder};
        use crate::storage::MessageReader;

        // Parse fetch request
        let mut cursor = Cursor::new(&request.body[..]);
        let fetch_req = FetchRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "Fetch request: replica_id={}, max_wait_ms={}, min_bytes={}, topics={}",
            fetch_req.replica_id,
            fetch_req.max_wait_ms,
            fetch_req.min_bytes,
            fetch_req.topics.len()
        );

        // Create message reader
        let message_reader = MessageReader::new(
            self.batch_writer.object_store(),
            self.batch_writer.layout().clone(),
        );

        // Process each topic
        let mut topic_responses = Vec::new();
        let mut total_bytes = 0;

        for fetch_topic in fetch_req.topics {
            let topic_name = &fetch_topic.topic;

            // Check if topic exists
            let topic_metadata = match self.state_manager.metadata().get_topic(topic_name).await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => {
                    // Topic not found
                    topic_responses.push(FetchTopicResponse {
                        topic: topic_name.clone(),
                        partitions: vec![FetchPartitionResponse {
                            partition: 0,
                            error_code: ERROR_TOPIC_NOT_FOUND,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: -1,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: vec![],
                        }],
                    });
                    continue;
                }
                Err(e) => {
                    error!("Failed to get topic metadata: {}", e);
                    topic_responses.push(FetchTopicResponse {
                        topic: topic_name.clone(),
                        partitions: vec![FetchPartitionResponse {
                            partition: 0,
                            error_code: ERROR_UNKNOWN,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            log_start_offset: -1,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: vec![],
                        }],
                    });
                    continue;
                }
            };

            // Process each partition
            let mut partition_responses = Vec::new();

            for fetch_partition in fetch_topic.partitions {
                let partition_index = fetch_partition.partition;

                // Validate partition exists
                if partition_index < 0 || partition_index >= topic_metadata.partitions {
                    partition_responses.push(FetchPartitionResponse {
                        partition: partition_index,
                        error_code: ERROR_UNKNOWN,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        log_start_offset: -1,
                        aborted_transactions: vec![],
                        preferred_read_replica: -1,
                        records: vec![],
                    });
                    continue;
                }

                // Get partition info
                let high_watermark = self
                    .state_manager
                    .messages()
                    .get_high_water_mark(topic_name, partition_index)
                    .await
                    .unwrap_or(0);

                let log_start_offset = self
                    .state_manager
                    .messages()
                    .get_log_start_offset(topic_name, partition_index)
                    .await
                    .unwrap_or(0);

                // Check if fetch offset is valid
                if fetch_partition.fetch_offset > high_watermark {
                    // Offset out of range
                    partition_responses.push(FetchPartitionResponse {
                        partition: partition_index,
                        error_code: ERROR_NONE, // Kafka returns NONE for offset out of range in fetch
                        high_watermark,
                        last_stable_offset: high_watermark,
                        log_start_offset,
                        aborted_transactions: vec![],
                        preferred_read_replica: -1,
                        records: vec![],
                    });
                    continue;
                }

                // Calculate how many messages to fetch
                let remaining_bytes = fetch_req.max_bytes.saturating_sub(total_bytes as i32);
                if remaining_bytes <= 0 {
                    // We've hit the max bytes limit
                    partition_responses.push(FetchPartitionResponse {
                        partition: partition_index,
                        error_code: ERROR_NONE,
                        high_watermark,
                        last_stable_offset: high_watermark,
                        log_start_offset,
                        aborted_transactions: vec![],
                        preferred_read_replica: -1,
                        records: vec![],
                    });
                    continue;
                }

                // Read messages from storage
                let max_messages = (fetch_partition.partition_max_bytes / 100).max(1) as usize; // Rough estimate
                debug!(
                    "Fetching messages: topic={}, partition={}, offset={}, max_messages={}",
                    topic_name, partition_index, fetch_partition.fetch_offset, max_messages
                );
                let messages = match message_reader
                    .read_messages(
                        topic_name,
                        partition_index,
                        fetch_partition.fetch_offset,
                        max_messages,
                    )
                    .await
                {
                    Ok(messages) => {
                        debug!("Read {} messages from storage", messages.len());
                        messages
                    }
                    Err(e) => {
                        error!("Failed to read messages: {}", e);
                        partition_responses.push(FetchPartitionResponse {
                            partition: partition_index,
                            error_code: ERROR_UNKNOWN,
                            high_watermark,
                            last_stable_offset: high_watermark,
                            log_start_offset,
                            aborted_transactions: vec![],
                            preferred_read_replica: -1,
                            records: vec![],
                        });
                        continue;
                    }
                };

                // Build RecordBatch from messages
                let records = if messages.is_empty() {
                    vec![]
                } else {
                    let mut builder = RecordBatchBuilder::new(CompressionType::None);
                    builder.add_messages(messages);
                    match builder.build() {
                        Ok(batch) => batch,
                        Err(e) => {
                            error!("Failed to build record batch: {}", e);
                            vec![]
                        }
                    }
                };

                total_bytes += records.len();

                partition_responses.push(FetchPartitionResponse {
                    partition: partition_index,
                    error_code: ERROR_NONE,
                    high_watermark,
                    last_stable_offset: high_watermark,
                    log_start_offset,
                    aborted_transactions: vec![],
                    preferred_read_replica: -1,
                    records,
                });
            }

            topic_responses.push(FetchTopicResponse {
                topic: topic_name.clone(),
                partitions: partition_responses,
            });
        }

        let response = FetchResponse {
            throttle_time_ms: 0,
            error_code: ERROR_NONE,
            session_id: 0, // No session support yet
            responses: topic_responses,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }
}
