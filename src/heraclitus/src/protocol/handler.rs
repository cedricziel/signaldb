use crate::{
    config::AuthConfig, error::Result, metrics::Metrics, state::StateManager, storage::BatchWriter,
};
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
    auth_config: Arc<AuthConfig>,
    authenticated: bool,
    username: Option<String>,
    metrics: Arc<Metrics>,
}

impl ConnectionHandler {
    pub fn new(
        socket: TcpStream,
        state_manager: Arc<StateManager>,
        batch_writer: Arc<BatchWriter>,
        port: u16,
        auth_config: Arc<AuthConfig>,
        metrics: Arc<Metrics>,
    ) -> Self {
        // Track new connection
        metrics.connection.active_connections.inc();
        metrics.connection.total_connections.inc();

        Self {
            socket,
            state_manager,
            batch_writer,
            port,
            read_buffer: BytesMut::with_capacity(8192),
            write_buffer: BytesMut::with_capacity(8192),
            auth_config: auth_config.clone(),
            authenticated: !auth_config.enabled, // If auth is disabled, consider connection authenticated
            username: None,
            metrics,
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
                            self.metrics.connection.connection_errors.inc();
                            // Try to send error response
                            let error_response = self.create_error_response(e);
                            if let Err(write_err) = self.write_frame(error_response).await {
                                error!("Failed to write error response: {write_err}");
                                self.metrics.connection.connection_errors.inc();
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
                    self.metrics.connection.connection_errors.inc();
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
        use std::time::Instant;

        // Track request metrics
        let start_time = Instant::now();
        let request_type = request.request_type.clone();
        self.metrics
            .protocol
            .request_count
            .with_label_values(&[&request_type.to_string()])
            .inc();

        // Check authentication for non-exempt requests
        if self.auth_config.enabled && !self.authenticated {
            match request.request_type {
                RequestType::ApiVersions
                | RequestType::SaslHandshake
                | RequestType::SaslAuthenticate => {
                    // These requests are allowed before authentication
                }
                _ => {
                    // All other requests require authentication
                    warn!("Unauthenticated request: {:?}", request.request_type);
                    return self.create_auth_error_response(request.correlation_id);
                }
            }
        }

        let result = match request.request_type {
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
            RequestType::ListOffsets => {
                debug!("Handling list offsets request");
                self.handle_list_offsets_request(request).await
            }
            RequestType::FindCoordinator => {
                debug!("Handling find coordinator request");
                self.handle_find_coordinator_request(request).await
            }
            RequestType::JoinGroup => {
                debug!("Handling join group request");
                self.handle_join_group_request(request).await
            }
            RequestType::SyncGroup => {
                debug!("Handling sync group request");
                self.handle_sync_group_request(request).await
            }
            RequestType::Heartbeat => {
                debug!("Handling heartbeat request");
                self.handle_heartbeat_request(request).await
            }
            RequestType::OffsetCommit => {
                debug!("Handling offset commit request");
                self.handle_offset_commit_request(request).await
            }
            RequestType::OffsetFetch => {
                debug!("Handling offset fetch request");
                self.handle_offset_fetch_request(request).await
            }
            RequestType::LeaveGroup => {
                debug!("Handling leave group request");
                self.handle_leave_group_request(request).await
            }
            RequestType::SaslHandshake => {
                debug!("Handling SASL handshake request");
                self.handle_sasl_handshake_request(request).await
            }
            RequestType::SaslAuthenticate => {
                debug!("Handling SASL authenticate request");
                self.handle_sasl_authenticate_request(request).await
            }
        };

        // Track request latency
        let elapsed = start_time.elapsed();
        self.metrics
            .protocol
            .request_duration
            .with_label_values(&[&request_type.to_string()])
            .observe(elapsed.as_secs_f64());

        result
    }

    /// Create an error response for a given error
    fn create_error_response(&self, _error: crate::error::HeraclitusError) -> Vec<u8> {
        // For now, return empty response
        // TODO: Implement proper error response based on request context
        vec![]
    }

    /// Create an unsupported operation response
    #[allow(dead_code)]
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
        info!("Produce request body length: {}", request.body.len());
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
                    debug!("Empty record batch for partition {}", partition_index);
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
                info!(
                    "Parsing record batch of {} bytes for partition {}",
                    partition_data.records.len(),
                    partition_index
                );

                // Log first few bytes to understand the format
                if !partition_data.records.is_empty() {
                    let preview_len = std::cmp::min(16, partition_data.records.len());
                    let preview: Vec<String> = partition_data.records[..preview_len]
                        .iter()
                        .map(|b| format!("{b:02x}"))
                        .collect();
                    info!(
                        "First {} bytes of record batch: {}",
                        preview_len,
                        preview.join(" ")
                    );
                }

                // Try to parse as RecordBatch v2 first, then fall back to MessageSet
                let record_batch =
                    match crate::protocol::RecordBatch::parse(&partition_data.records) {
                        Ok(batch) => batch,
                        Err(e) => {
                            // Try parsing as old MessageSet format
                            info!("RecordBatch parse failed: {}, trying MessageSet format", e);

                            match crate::protocol::message_set::MessageSet::parse(
                                &partition_data.records,
                            ) {
                                Ok(message_set) => {
                                    info!(
                                        "Successfully parsed as MessageSet with {} messages",
                                        message_set.messages.len()
                                    );
                                    // Convert MessageSet to RecordBatch
                                    match message_set.to_record_batch() {
                                        Ok(batch) => batch,
                                        Err(e) => {
                                            error!(
                                                "Failed to convert MessageSet to RecordBatch: {}",
                                                e
                                            );
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
                                }
                                Err(e) => {
                                    error!(
                                        "Failed to parse as both RecordBatch and MessageSet: {}",
                                        e
                                    );
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

    /// Handle list offsets request
    async fn handle_list_offsets_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::list_offsets::{
            EARLIEST_TIMESTAMP, LATEST_TIMESTAMP, ListOffsetsPartitionResponse, ListOffsetsRequest,
            ListOffsetsResponse, ListOffsetsTopicResponse,
        };

        // Parse list offsets request
        let mut cursor = Cursor::new(&request.body[..]);
        let list_offsets_req = ListOffsetsRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "List offsets request: replica_id={}, topics={}",
            list_offsets_req.replica_id,
            list_offsets_req.topics.len()
        );

        // Process each topic
        let mut topic_responses = Vec::new();

        for topic in list_offsets_req.topics {
            let topic_name = &topic.name;

            // Check if topic exists
            let topic_metadata = match self.state_manager.metadata().get_topic(topic_name).await {
                Ok(Some(metadata)) => metadata,
                Ok(None) => {
                    // Topic not found
                    let mut partition_responses = Vec::new();
                    for partition in topic.partitions {
                        partition_responses.push(ListOffsetsPartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: ERROR_TOPIC_NOT_FOUND,
                            timestamp: partition.timestamp,
                            offset: -1,
                            old_style_offsets: vec![],
                        });
                    }
                    topic_responses.push(ListOffsetsTopicResponse {
                        name: topic_name.clone(),
                        partitions: partition_responses,
                    });
                    continue;
                }
                Err(e) => {
                    error!("Failed to get topic metadata: {}", e);
                    let mut partition_responses = Vec::new();
                    for partition in topic.partitions {
                        partition_responses.push(ListOffsetsPartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: ERROR_UNKNOWN,
                            timestamp: partition.timestamp,
                            offset: -1,
                            old_style_offsets: vec![],
                        });
                    }
                    topic_responses.push(ListOffsetsTopicResponse {
                        name: topic_name.clone(),
                        partitions: partition_responses,
                    });
                    continue;
                }
            };

            // Process each partition
            let mut partition_responses = Vec::new();

            for partition in topic.partitions {
                let partition_index = partition.partition_index;

                // Validate partition exists
                if partition_index < 0 || partition_index >= topic_metadata.partitions {
                    partition_responses.push(ListOffsetsPartitionResponse {
                        partition_index,
                        error_code: ERROR_UNKNOWN_TOPIC_OR_PARTITION,
                        timestamp: partition.timestamp,
                        offset: -1,
                        old_style_offsets: vec![],
                    });
                    continue;
                }

                // Get the requested offset based on timestamp
                let offset = match partition.timestamp {
                    EARLIEST_TIMESTAMP => {
                        // Get the earliest available offset (log start offset)
                        self.state_manager
                            .messages()
                            .get_log_start_offset(topic_name, partition_index)
                            .await
                            .unwrap_or(0)
                    }
                    LATEST_TIMESTAMP => {
                        // Get the latest offset (high water mark)
                        self.state_manager
                            .messages()
                            .get_high_water_mark(topic_name, partition_index)
                            .await
                            .unwrap_or(0)
                    }
                    timestamp => {
                        // For specific timestamp, find the first offset with timestamp >= requested
                        // For now, we'll return an error as we don't have timestamp-based indexing
                        warn!(
                            "Timestamp-based offset lookup not yet implemented: {}",
                            timestamp
                        );
                        -1
                    }
                };

                if offset >= 0 {
                    partition_responses.push(ListOffsetsPartitionResponse {
                        partition_index,
                        error_code: ERROR_NONE,
                        timestamp: partition.timestamp,
                        offset,
                        old_style_offsets: if request.api_version == 0 {
                            vec![offset]
                        } else {
                            vec![]
                        },
                    });
                } else {
                    partition_responses.push(ListOffsetsPartitionResponse {
                        partition_index,
                        error_code: ERROR_OFFSET_NOT_AVAILABLE,
                        timestamp: partition.timestamp,
                        offset: -1,
                        old_style_offsets: vec![],
                    });
                }
            }

            topic_responses.push(ListOffsetsTopicResponse {
                name: topic_name.clone(),
                partitions: partition_responses,
            });
        }

        let response = ListOffsetsResponse {
            throttle_time_ms: 0,
            topics: topic_responses,
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle find coordinator request
    async fn handle_find_coordinator_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::find_coordinator::{FindCoordinatorRequest, FindCoordinatorResponse};
        use crate::protocol::kafka_protocol::*;

        // Parse find coordinator request
        let mut cursor = Cursor::new(&request.body[..]);
        let find_coord_req = FindCoordinatorRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "FindCoordinator request: key={}, key_type={}",
            find_coord_req.key, find_coord_req.key_type
        );

        // For now, we only support group coordinators (key_type = 0)
        if find_coord_req.key_type != 0 {
            let response = FindCoordinatorResponse::error(
                ERROR_INVALID_REQUEST,
                "Only group coordinators are supported".to_string(),
            );
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            return Ok(full_response.to_vec());
        }

        // Return ourselves as the coordinator
        // Get our advertised address
        let host = self.socket.local_addr()?.ip().to_string();
        let port = self.port as i32;

        let response = FindCoordinatorResponse::new(0, host, port);

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle join group request
    async fn handle_join_group_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::join_group::{JoinGroupMember, JoinGroupRequest, JoinGroupResponse};
        use crate::protocol::kafka_protocol::*;
        use crate::state::{ConsumerGroupMember, ConsumerGroupState};
        use std::collections::HashMap;

        // Parse join group request
        let mut cursor = Cursor::new(&request.body[..]);
        let join_req = JoinGroupRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "JoinGroup request: group_id={}, member_id={}, protocol_type={}, protocols={:?}",
            join_req.group_id,
            join_req.member_id,
            join_req.protocol_type,
            join_req
                .protocols
                .iter()
                .map(|p| &p.name)
                .collect::<Vec<_>>()
        );

        // Get or create consumer group
        let mut group_state = match self
            .state_manager
            .consumer_groups()
            .get_group(&join_req.group_id)
            .await?
        {
            Some(state) => state,
            None => {
                // Create new group
                ConsumerGroupState {
                    group_id: join_req.group_id.clone(),
                    generation_id: 0,
                    protocol_type: join_req.protocol_type.clone(),
                    protocol: None,
                    leader: None,
                    members: HashMap::new(),
                }
            }
        };

        // Generate member ID if empty (new member)
        let member_id = if join_req.member_id.is_empty() {
            format!("consumer-{}-{}", join_req.group_id, uuid::Uuid::new_v4())
        } else {
            join_req.member_id.clone()
        };

        // Check if this is a rebalance or new join
        let is_new_member = !group_state.members.contains_key(&member_id);

        // Add/update member
        let client_host = self.socket.peer_addr()?.ip().to_string();
        group_state.members.insert(
            member_id.clone(),
            ConsumerGroupMember {
                member_id: member_id.clone(),
                client_id: request.client_id.clone().unwrap_or_default(),
                client_host,
                session_timeout_ms: join_req.session_timeout_ms,
                rebalance_timeout_ms: join_req.rebalance_timeout_ms,
                last_heartbeat_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as i64,
            },
        );

        // If group is empty or new member joined, trigger rebalance
        if is_new_member || group_state.leader.is_none() {
            // Increment generation
            group_state.generation_id += 1;

            // Select leader (first member)
            if group_state.leader.is_none() {
                group_state.leader = Some(member_id.clone());
            }

            // Select common protocol (for now, just use the first one)
            if !join_req.protocols.is_empty() {
                group_state.protocol = Some(join_req.protocols[0].name.clone());
            }
        }

        // Save group state
        self.state_manager
            .consumer_groups()
            .save_group(&group_state)
            .await?;

        // Build response
        let response = if Some(&member_id) == group_state.leader.as_ref() {
            // Leader gets member list
            let members: Vec<JoinGroupMember> = join_req
                .protocols
                .iter()
                .map(|protocol| JoinGroupMember {
                    member_id: member_id.clone(),
                    group_instance_id: join_req.group_instance_id.clone(),
                    metadata: protocol.metadata.clone(),
                })
                .collect();

            JoinGroupResponse::new_leader(
                group_state.generation_id,
                group_state.protocol.unwrap_or_default(),
                group_state.leader.unwrap_or_default(),
                member_id,
                members,
            )
        } else {
            // Followers don't get member list
            JoinGroupResponse::new_follower(
                group_state.generation_id,
                group_state.protocol.unwrap_or_default(),
                group_state.leader.unwrap_or_default(),
                member_id,
            )
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle sync group request
    async fn handle_sync_group_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::sync_group::{
            ConsumerProtocolAssignment, SyncGroupRequest, SyncGroupResponse,
        };
        use std::collections::HashMap;

        // Parse sync group request
        let mut cursor = Cursor::new(&request.body[..]);
        let sync_req = SyncGroupRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "SyncGroup request: group_id={}, generation_id={}, member_id={}, assignments={}",
            sync_req.group_id,
            sync_req.generation_id,
            sync_req.member_id,
            sync_req.group_assignments.len()
        );

        // Get the consumer group state
        let group_state = match self
            .state_manager
            .consumer_groups()
            .get_group(&sync_req.group_id)
            .await?
        {
            Some(state) => state,
            None => {
                // Group doesn't exist
                let response = SyncGroupResponse::error(ERROR_UNKNOWN_MEMBER_ID);
                let response_body = response.encode(request.api_version)?;
                let mut full_response = BytesMut::new();
                write_response_header(&mut full_response, request.correlation_id);
                full_response.extend_from_slice(&response_body);
                return Ok(full_response.to_vec());
            }
        };

        // Validate generation ID
        if sync_req.generation_id != group_state.generation_id {
            let response = SyncGroupResponse::error(ERROR_ILLEGAL_GENERATION);
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            return Ok(full_response.to_vec());
        }

        // Check if member exists
        if !group_state.members.contains_key(&sync_req.member_id) {
            let response = SyncGroupResponse::error(ERROR_UNKNOWN_MEMBER_ID);
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            return Ok(full_response.to_vec());
        }

        // For the leader, store the assignments
        let assignment = if Some(&sync_req.member_id) == group_state.leader.as_ref()
            && !sync_req.group_assignments.is_empty()
        {
            // Leader is providing assignments for all members
            // For now, we'll just return the assignment for this member
            // In a full implementation, we'd store all assignments and return them to respective members
            sync_req
                .group_assignments
                .get(&sync_req.member_id)
                .cloned()
                .unwrap_or_default()
        } else {
            // For followers or if no assignments provided, create a simple assignment
            // In a real implementation, followers would wait for the leader's assignment
            // For now, we'll create a basic assignment

            // Get all topics the group is interested in
            let topics = self.state_manager.metadata().list_topics().await?;

            if !topics.is_empty() {
                // Simple assignment: give this member partition 0 of the first topic
                let mut topic_partitions = HashMap::new();
                topic_partitions.insert(topics[0].clone(), vec![0]);

                let assignment = ConsumerProtocolAssignment::new(topic_partitions);
                assignment.encode()
            } else {
                Vec::new()
            }
        };

        // Create response
        let response = SyncGroupResponse::new(assignment);

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    async fn handle_heartbeat_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::heartbeat::{HeartbeatRequest, HeartbeatResponse};
        use crate::protocol::kafka_protocol::{
            ERROR_COORDINATOR_NOT_AVAILABLE, ERROR_ILLEGAL_GENERATION, ERROR_UNKNOWN_MEMBER_ID,
            write_response_header,
        };

        let mut cursor = Cursor::new(&request.body[..]);
        let heartbeat_req = HeartbeatRequest::parse(&mut cursor, request.api_version)?;

        // Get consumer group state
        let mut group_state = match self
            .state_manager
            .consumer_groups()
            .get_group(&heartbeat_req.group_id)
            .await?
        {
            Some(state) => state,
            None => {
                // Group doesn't exist
                let response = HeartbeatResponse::error(ERROR_COORDINATOR_NOT_AVAILABLE);
                let response_body = response.encode(request.api_version)?;
                let mut full_response = BytesMut::new();
                write_response_header(&mut full_response, request.correlation_id);
                full_response.extend_from_slice(&response_body);
                return Ok(full_response.to_vec());
            }
        };

        // Validate generation ID
        if heartbeat_req.generation_id != group_state.generation_id {
            let response = HeartbeatResponse::error(ERROR_ILLEGAL_GENERATION);
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            return Ok(full_response.to_vec());
        }

        // Check if member exists
        if let Some(member) = group_state.members.get_mut(&heartbeat_req.member_id) {
            // Update last heartbeat timestamp
            member.last_heartbeat_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            // Save updated group state
            self.state_manager
                .consumer_groups()
                .save_group(&group_state)
                .await?;

            // Check if rebalance is in progress
            // For now, we'll assume no rebalance is in progress
            // In a full implementation, we'd track rebalance state

            let response = HeartbeatResponse::success();
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            Ok(full_response.to_vec())
        } else {
            // Member doesn't exist
            let response = HeartbeatResponse::error(ERROR_UNKNOWN_MEMBER_ID);
            let response_body = response.encode(request.api_version)?;
            let mut full_response = BytesMut::new();
            write_response_header(&mut full_response, request.correlation_id);
            full_response.extend_from_slice(&response_body);
            Ok(full_response.to_vec())
        }
    }

    async fn handle_offset_commit_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::write_response_header;
        use crate::protocol::offset_commit::{
            OffsetCommitRequest, OffsetCommitResponse, OffsetCommitResponsePartition,
            OffsetCommitResponseTopic,
        };

        let mut cursor = Cursor::new(&request.body[..]);
        let commit_req = OffsetCommitRequest::parse(&mut cursor, request.api_version)?;

        // For v0, we don't have generation/member validation
        // For v1+, we should validate the consumer group membership
        if request.api_version >= 1 {
            // Get consumer group state
            let group_state = match self
                .state_manager
                .consumer_groups()
                .get_group(&commit_req.group_id)
                .await?
            {
                Some(state) => state,
                None => {
                    // Group doesn't exist - return NOT_COORDINATOR error
                    let response = OffsetCommitResponse::error_all(
                        &commit_req.topics,
                        crate::protocol::kafka_protocol::ERROR_NOT_COORDINATOR,
                    );
                    let response_body = response.encode(request.api_version)?;
                    let mut full_response = BytesMut::new();
                    write_response_header(&mut full_response, request.correlation_id);
                    full_response.extend_from_slice(&response_body);
                    return Ok(full_response.to_vec());
                }
            };

            // Validate generation ID
            if commit_req.generation_id != group_state.generation_id {
                let response = OffsetCommitResponse::error_all(
                    &commit_req.topics,
                    crate::protocol::kafka_protocol::ERROR_ILLEGAL_GENERATION,
                );
                let response_body = response.encode(request.api_version)?;
                let mut full_response = BytesMut::new();
                write_response_header(&mut full_response, request.correlation_id);
                full_response.extend_from_slice(&response_body);
                return Ok(full_response.to_vec());
            }

            // Check if member exists
            if !group_state.members.contains_key(&commit_req.member_id) {
                let response = OffsetCommitResponse::error_all(
                    &commit_req.topics,
                    crate::protocol::kafka_protocol::ERROR_UNKNOWN_MEMBER_ID,
                );
                let response_body = response.encode(request.api_version)?;
                let mut full_response = BytesMut::new();
                write_response_header(&mut full_response, request.correlation_id);
                full_response.extend_from_slice(&response_body);
                return Ok(full_response.to_vec());
            }
        }

        // Prepare offsets to save
        let mut offsets_to_save = Vec::new();
        let mut response_topics = Vec::new();

        for topic in &commit_req.topics {
            let mut response_partitions = Vec::new();

            for partition in &topic.partitions {
                // Save the offset
                offsets_to_save.push((
                    topic.name.clone(),
                    partition.partition_index,
                    partition.committed_offset,
                    partition.metadata.clone(),
                ));

                // Add success response for this partition
                response_partitions.push(OffsetCommitResponsePartition {
                    partition_index: partition.partition_index,
                    error_code: 0, // SUCCESS
                });
            }

            response_topics.push(OffsetCommitResponseTopic {
                name: topic.name.clone(),
                partitions: response_partitions,
            });
        }

        // Save all offsets
        self.state_manager
            .consumer_groups()
            .save_offsets(&commit_req.group_id, &offsets_to_save)
            .await?;

        // Create success response
        let response = OffsetCommitResponse::new(response_topics);
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    async fn handle_offset_fetch_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::write_response_header;
        use crate::protocol::offset_fetch::{
            OffsetFetchRequest, OffsetFetchResponse, OffsetFetchResponsePartition,
            OffsetFetchResponseTopic,
        };

        let mut cursor = Cursor::new(&request.body[..]);
        let fetch_req = OffsetFetchRequest::parse(&mut cursor, request.api_version)?;

        // Get all committed offsets for the group
        let group_offsets = self
            .state_manager
            .consumer_groups()
            .get_offsets(&fetch_req.group_id)
            .await?;

        let mut response_topics = Vec::new();

        if let Some(requested_topics) = &fetch_req.topics {
            // Client requested specific topics/partitions
            for topic in requested_topics {
                let mut response_partitions = Vec::new();

                for &partition_index in &topic.partition_indexes {
                    let partition_response = if let Some(offset_info) =
                        group_offsets.get(&(topic.name.clone(), partition_index))
                    {
                        // We have a committed offset for this partition
                        OffsetFetchResponsePartition::success(
                            partition_index,
                            offset_info.offset,
                            offset_info.metadata.clone(),
                        )
                    } else {
                        // No committed offset for this partition
                        OffsetFetchResponsePartition::no_offset(partition_index)
                    };

                    response_partitions.push(partition_response);
                }

                response_topics.push(OffsetFetchResponseTopic {
                    name: topic.name.clone(),
                    partitions: response_partitions,
                });
            }
        } else {
            // Client wants all committed offsets for the group
            // Group by topic name
            let mut topics_map: std::collections::HashMap<
                String,
                std::collections::HashMap<i32, &crate::state::consumer_group::TopicPartitionOffset>,
            > = std::collections::HashMap::new();

            for ((topic, partition), offset_info) in &group_offsets {
                topics_map
                    .entry(topic.clone())
                    .or_default()
                    .insert(*partition, offset_info);
            }

            // Convert to response format
            for (topic_name, partitions) in topics_map {
                let mut response_partitions = Vec::new();

                for (partition_index, offset_info) in partitions {
                    response_partitions.push(OffsetFetchResponsePartition::success(
                        partition_index,
                        offset_info.offset,
                        offset_info.metadata.clone(),
                    ));
                }

                // Sort partitions by index for consistent output
                response_partitions.sort_by_key(|p| p.partition_index);

                response_topics.push(OffsetFetchResponseTopic {
                    name: topic_name,
                    partitions: response_partitions,
                });
            }

            // Sort topics by name for consistent output
            response_topics.sort_by(|a, b| a.name.cmp(&b.name));
        }

        // Create success response
        let response = OffsetFetchResponse::new(response_topics);
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    async fn handle_leave_group_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::{
            ERROR_COORDINATOR_NOT_AVAILABLE, write_response_header,
        };
        use crate::protocol::leave_group::{LeaveGroupRequest, LeaveGroupResponse};

        let mut cursor = Cursor::new(&request.body[..]);
        let leave_req = LeaveGroupRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "LeaveGroup request: group_id={}, member_id={}, batch_size={}",
            leave_req.group_id,
            leave_req.member_id,
            leave_req.members.as_ref().map(|m| m.len()).unwrap_or(1)
        );

        // Get consumer group state
        let mut group_state = match self
            .state_manager
            .consumer_groups()
            .get_group(&leave_req.group_id)
            .await?
        {
            Some(state) => state,
            None => {
                // Group doesn't exist
                let response = LeaveGroupResponse::error(ERROR_COORDINATOR_NOT_AVAILABLE);
                let response_body = response.encode(request.api_version)?;
                let mut full_response = BytesMut::new();
                write_response_header(&mut full_response, request.correlation_id);
                full_response.extend_from_slice(&response_body);
                return Ok(full_response.to_vec());
            }
        };

        // Handle different API versions
        if request.api_version <= 2 {
            // v0-v2: single member leaving
            if !group_state.members.contains_key(&leave_req.member_id) {
                // Member doesn't exist - this is actually OK for LeaveGroup
                debug!(
                    "Member {} not found in group {}, already left",
                    leave_req.member_id, leave_req.group_id
                );
            } else {
                // Remove the member
                group_state.members.remove(&leave_req.member_id);
                info!(
                    "Member {} left group {}",
                    leave_req.member_id, leave_req.group_id
                );

                // If the leader left, elect a new leader
                if Some(&leave_req.member_id) == group_state.leader.as_ref() {
                    group_state.leader = group_state.members.keys().next().cloned();
                    if let Some(ref new_leader) = group_state.leader {
                        info!("New group leader elected: {}", new_leader);
                    }
                }

                // If this was the last member, reset the group
                if group_state.members.is_empty() {
                    info!("Group {} is now empty", leave_req.group_id);
                    group_state.generation_id += 1;
                    group_state.leader = None;
                    group_state.protocol = None;
                } else {
                    // Trigger rebalance for remaining members
                    group_state.generation_id += 1;
                    info!(
                        "Rebalance triggered for group {} (generation {})",
                        leave_req.group_id, group_state.generation_id
                    );
                }
            }
        } else {
            // v3+: batch member leaving
            if let Some(ref members) = leave_req.members {
                let mut removed_count = 0;
                let mut leader_left = false;

                for member in members {
                    if group_state.members.remove(&member.member_id).is_some() {
                        removed_count += 1;
                        info!(
                            "Member {} left group {}",
                            member.member_id, leave_req.group_id
                        );

                        // Check if the leader left
                        if Some(&member.member_id) == group_state.leader.as_ref() {
                            leader_left = true;
                        }
                    }
                }

                // If the leader left, elect a new leader
                if leader_left {
                    group_state.leader = group_state.members.keys().next().cloned();
                    if let Some(ref new_leader) = group_state.leader {
                        info!("New group leader elected: {}", new_leader);
                    }
                }

                // If members were removed, update group state
                if removed_count > 0 {
                    if group_state.members.is_empty() {
                        info!("Group {} is now empty", leave_req.group_id);
                        group_state.generation_id += 1;
                        group_state.leader = None;
                        group_state.protocol = None;
                    } else {
                        // Trigger rebalance for remaining members
                        group_state.generation_id += 1;
                        info!(
                            "Rebalance triggered for group {} (generation {}), {} members removed",
                            leave_req.group_id, group_state.generation_id, removed_count
                        );
                    }
                }
            }
        }

        // Save updated group state
        self.state_manager
            .consumer_groups()
            .save_group(&group_state)
            .await?;

        // Create success response
        let response = LeaveGroupResponse::success();
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Create an authentication error response
    fn create_auth_error_response(&self, correlation_id: i32) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;

        let mut response = BytesMut::new();

        // Write response header
        write_response_header(&mut response, correlation_id);

        // Write error code for authentication required
        response.extend_from_slice(&ERROR_SASL_AUTHENTICATION_FAILED.to_be_bytes());

        Ok(response.to_vec())
    }

    /// Handle SASL handshake request
    async fn handle_sasl_handshake_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::sasl_handshake::{SaslHandshakeRequest, SaslHandshakeResponse};

        // Parse request
        let mut cursor = Cursor::new(&request.body[..]);
        let handshake_req = SaslHandshakeRequest::parse(&mut cursor, request.api_version)?;

        debug!(
            "SASL handshake request: mechanism={}",
            handshake_req.mechanism
        );

        // Check if the requested mechanism is supported
        let response = if handshake_req.mechanism == self.auth_config.mechanism {
            // Return the supported mechanisms (just PLAIN for now)
            SaslHandshakeResponse::new(vec![self.auth_config.mechanism.clone()])
        } else {
            // Unsupported mechanism
            SaslHandshakeResponse::error(ERROR_UNSUPPORTED_SASL_MECHANISM)
        };

        // Encode response
        let response_body = response.encode(request.api_version)?;

        // Build complete response with header
        let mut full_response = BytesMut::new();
        write_response_header(&mut full_response, request.correlation_id);
        full_response.extend_from_slice(&response_body);

        Ok(full_response.to_vec())
    }

    /// Handle SASL authenticate request
    async fn handle_sasl_authenticate_request(
        &mut self,
        request: crate::protocol::request::KafkaRequest,
    ) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;
        use crate::protocol::sasl_authenticate::{
            SaslAuthenticateRequest, SaslAuthenticateResponse, parse_plain_auth_bytes,
        };

        // Parse request
        let mut cursor = Cursor::new(&request.body[..]);
        let auth_req = SaslAuthenticateRequest::parse(&mut cursor, request.api_version)?;

        debug!("SASL authenticate request received");

        // Handle PLAIN mechanism authentication
        let response = if self.auth_config.mechanism == "PLAIN" {
            match parse_plain_auth_bytes(&auth_req.auth_bytes) {
                Ok((username, password)) => {
                    // Check credentials
                    if let (Some(expected_user), Some(expected_pass)) = (
                        &self.auth_config.plain_username,
                        &self.auth_config.plain_password,
                    ) {
                        if &username == expected_user && &password == expected_pass {
                            // Authentication successful
                            self.authenticated = true;
                            self.username = Some(username);
                            info!("Client authenticated as user: {}", expected_user);
                            SaslAuthenticateResponse::success()
                        } else {
                            warn!("Authentication failed for user: {}", username);
                            SaslAuthenticateResponse::error(
                                ERROR_SASL_AUTHENTICATION_FAILED,
                                "Authentication failed".to_string(),
                            )
                        }
                    } else {
                        // No credentials configured
                        error!("SASL/PLAIN authentication enabled but no credentials configured");
                        SaslAuthenticateResponse::error(
                            ERROR_SASL_AUTHENTICATION_FAILED,
                            "Server misconfiguration".to_string(),
                        )
                    }
                }
                Err(e) => {
                    warn!("Failed to parse SASL/PLAIN auth bytes: {}", e);
                    SaslAuthenticateResponse::error(
                        ERROR_SASL_AUTHENTICATION_FAILED,
                        "Invalid auth format".to_string(),
                    )
                }
            }
        } else {
            // Unsupported mechanism (shouldn't happen if handshake was successful)
            SaslAuthenticateResponse::error(
                ERROR_UNSUPPORTED_SASL_MECHANISM,
                "Unsupported mechanism".to_string(),
            )
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

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        // Decrement active connections counter
        self.metrics.connection.active_connections.dec();
    }
}
