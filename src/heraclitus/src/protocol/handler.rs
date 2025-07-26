use crate::{
    config::AuthConfig,
    error::Result,
    metrics::Metrics,
    protocol::handlers::{
        DefaultResponseBuilder, HandlerContext, HandlerRegistry, ResponseBuilder,
    },
    state::StateManager,
    storage::BatchWriter,
};
use bytes::{Buf, BytesMut};
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
    // Track consumer group membership for cleanup
    consumer_groups: Vec<(String, String)>, // (group_id, member_id)
    handler_registry: Arc<HandlerRegistry>,
    response_builder: Arc<dyn ResponseBuilder + Send + Sync>,
}

/// Determine if an API uses flexible versions and requires response header v1
pub fn uses_flexible_version(api_key: i16, api_version: i16) -> bool {
    match api_key {
        // ApiVersions uses flexible versions from v4+ (not v3!)
        18 => api_version >= 4,
        // Metadata uses flexible versions from v9+
        3 => api_version >= 9,
        // CreateTopics uses flexible versions from v5+
        19 => api_version >= 5,
        // InitProducerId uses flexible versions from v2+
        22 => api_version >= 2,
        // OffsetCommit uses flexible versions from v8+
        8 => api_version >= 8,
        // OffsetFetch uses flexible versions from v6+
        9 => api_version >= 6,
        // Heartbeat uses flexible versions from v4+
        12 => api_version >= 4,
        // JoinGroup uses flexible versions from v6+
        11 => api_version >= 6,
        // SyncGroup uses flexible versions from v4+
        14 => api_version >= 4,
        // LeaveGroup uses flexible versions from v4+
        13 => api_version >= 4,
        // ListGroups uses flexible versions from v3+
        16 => api_version >= 3,
        // DescribeGroups uses flexible versions from v5+
        15 => api_version >= 5,
        // ListOffsets uses flexible versions from v4+
        2 => api_version >= 4,
        // Produce uses flexible versions from v9+
        0 => api_version >= 9,
        // Fetch uses flexible versions from v12+
        1 => api_version >= 12,
        _ => false,
    }
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
            consumer_groups: Vec::new(),
            handler_registry: Arc::new(HandlerRegistry::new()),
            response_builder: Arc::new(DefaultResponseBuilder),
        }
    }

    pub async fn run(mut self) -> Result<()> {
        info!("Starting Kafka protocol handler for new connection");
        let connection_start = std::time::Instant::now();
        let mut request_count = 0;

        loop {
            // Try to read a complete frame
            match self.read_frame().await {
                Ok(Some(frame)) => {
                    request_count += 1;
                    debug!(
                        "Received request #{} after {}ms",
                        request_count,
                        connection_start.elapsed().as_millis()
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
                            // Try to send error response
                            let error_response = self.create_error_response(e);
                            if let Err(write_err) = self.write_frame(error_response).await {
                                error!("Failed to write error response: {}", write_err);
                                break;
                            }
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

        // Clean up consumer group memberships
        for (group_id, member_id) in &self.consumer_groups {
            info!(
                "Cleaning up membership for group_id={}, member_id={}",
                group_id, member_id
            );
            // Leave the group on disconnect
            // This is a best-effort cleanup - errors are logged but not fatal
            if let Ok(Some(mut group_state)) = self
                .state_manager
                .consumer_groups()
                .get_group(group_id)
                .await
            {
                if group_state.members.remove(member_id).is_some() {
                    // If this was the leader, select a new one
                    if group_state.leader.as_ref() == Some(member_id) {
                        group_state.leader = group_state.members.keys().next().cloned();
                    }

                    if let Err(e) = self
                        .state_manager
                        .consumer_groups()
                        .save_group(&group_state)
                        .await
                    {
                        error!("Failed to save group state after cleanup: {}", e);
                    }
                }
            }
        }

        // Track disconnection
        self.metrics.connection.active_connections.dec();

        Ok(())
    }

    /// Read a complete frame from the socket
    async fn read_frame(&mut self) -> Result<Option<bytes::Bytes>> {
        loop {
            // Check if we have a complete frame
            if self.read_buffer.len() >= 4 {
                // Peek at the frame length
                let mut cursor = Cursor::new(&self.read_buffer[..]);
                let frame_length = cursor.get_i32() as usize;

                // Check if we have the complete frame
                if self.read_buffer.len() >= 4 + frame_length {
                    // Extract the frame
                    self.read_buffer.advance(4); // Skip length prefix
                    let frame = self.read_buffer.split_to(frame_length).freeze();
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
        info!("Writing response frame: size={} bytes", data.len());
        info!(
            "Frame data first 32 bytes: {:02x?}",
            &data[..32.min(data.len())]
        );

        // Clear write buffer
        self.write_buffer.clear();

        // Write frame length
        let frame_length = data.len() as i32;
        self.write_buffer
            .extend_from_slice(&frame_length.to_be_bytes());

        info!(
            "Frame length: {} (0x{:08x}), bytes: {:02x?}",
            frame_length,
            frame_length,
            &frame_length.to_be_bytes()
        );

        // Write frame data
        self.write_buffer.extend_from_slice(&data);

        // Log complete frame for debugging
        if self.write_buffer.len() < 300 {
            // Log small frames completely
            let hex_bytes: String = self
                .write_buffer
                .iter()
                .enumerate()
                .map(|(i, b)| {
                    if i % 16 == 0 && i > 0 {
                        format!("\n  {i:04x}: {b:02x}")
                    } else if i % 16 == 0 {
                        format!("  {i:04x}: {b:02x}")
                    } else {
                        format!(" {b:02x}")
                    }
                })
                .collect::<String>();
            info!("Complete frame hex dump:\n{}", hex_bytes);
        }

        // Send to socket
        self.socket.write_all(&self.write_buffer).await?;
        self.socket.flush().await?;

        info!(
            "Successfully sent response: total_size={} bytes (4 byte header + {} byte payload)",
            self.write_buffer.len(),
            data.len()
        );
        Ok(())
    }

    /// Process a Kafka request and return the response
    async fn process_request(&mut self, frame: bytes::Bytes) -> Result<Vec<u8>> {
        // Log the raw request frame for debugging
        info!(
            "Raw request frame ({} bytes): {:02x?}",
            frame.len(),
            &frame[..32.min(frame.len())]
        );

        // Parse request header
        let mut cursor = Cursor::new(&frame[..]);
        let header = crate::protocol::kafka_protocol::read_request_header(&mut cursor)?;

        // Get remaining bytes as request body
        let body_start = cursor.position() as usize;
        let body = frame.slice(body_start..);

        info!(
            "Processing request: api_key={} ({}), api_version={}, correlation_id={}, client_id={:?}, body_len={}",
            header.api_key,
            match header.api_key {
                0 => "Produce",
                1 => "Fetch",
                3 => "Metadata",
                18 => "ApiVersions",
                _ => "Other",
            },
            header.api_version,
            header.correlation_id,
            header.client_id,
            body.len()
        );

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
        let api_key_str = request.api_key.to_string();
        let api_version_str = request.api_version.to_string();
        self.metrics
            .protocol
            .request_count
            .with_label_values(&[&api_key_str, &request_type.to_string(), &api_version_str])
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

        // Create handler context
        let mut context = HandlerContext {
            state_manager: self.state_manager.clone(),
            batch_writer: self.batch_writer.clone(),
            metrics: self.metrics.clone(),
            auth_config: self.auth_config.clone(),
            authenticated: self.authenticated,
            username: self.username.clone(),
            port: self.port,
            consumer_groups: self.consumer_groups.clone(),
        };

        // Dispatch to handler registry
        let result = self.handler_registry.handle(&request, &mut context).await;

        // Update authentication state from context
        self.authenticated = context.authenticated;
        self.username = context.username.clone();
        self.consumer_groups = context.consumer_groups.clone();

        // Log handler result
        match &result {
            Ok(response_body) => {
                info!(
                    "Handler returned response for api_key={}, correlation_id={}, response_body_len={}",
                    request.api_key,
                    request.correlation_id,
                    response_body.len()
                );
            }
            Err(e) => {
                error!(
                    "Handler error for api_key={}, correlation_id={}: {:?}",
                    request.api_key, request.correlation_id, e
                );
            }
        }

        // Track request latency
        let elapsed = start_time.elapsed();
        self.metrics
            .protocol
            .request_duration
            .with_label_values(&[&api_key_str, &request_type.to_string(), &api_version_str])
            .observe(elapsed.as_secs_f64());

        match result {
            Ok(response_body) => {
                info!(
                    "Handler returned raw response body of {} bytes for api_key={}, first 8 bytes: {:02x?}",
                    response_body.len(),
                    request.api_key,
                    &response_body[..8.min(response_body.len())]
                );

                // Build complete response with header
                let complete_response = self
                    .response_builder
                    .build_response(&request, response_body);

                info!(
                    "ResponseBuilder created response of {} bytes, first 8 bytes: {:02x?}",
                    complete_response.len(),
                    &complete_response[..8.min(complete_response.len())]
                );

                Ok(complete_response)
            }
            Err(e) => {
                error!("Error handling request: {}", e);

                // Special case for ApiVersions: always respond with v0 format on error
                if request.api_key == 18 {
                    // ApiVersions errors must use v0 format for compatibility
                    let v0_request = crate::protocol::request::KafkaRequest::new(
                        18,
                        0, // Force v0
                        request.correlation_id,
                        request.client_id.clone(),
                        request.body.clone(),
                    )?;

                    // Return UNSUPPORTED_VERSION error for version mismatch
                    let error_code = match &e {
                        crate::error::HeraclitusError::UnsupportedVersion(_, _) => {
                            crate::protocol::kafka_protocol::ERROR_UNSUPPORTED_VERSION
                        }
                        _ => crate::protocol::kafka_protocol::ERROR_UNKNOWN_SERVER_ERROR,
                    };

                    return Ok(self
                        .response_builder
                        .build_error_response(&v0_request, error_code));
                }

                // For other APIs, use the requested version format
                Ok(self.response_builder.build_error_response(
                    &request,
                    crate::protocol::kafka_protocol::ERROR_UNKNOWN_SERVER_ERROR,
                ))
            }
        }
    }

    /// Create an error response for a given error
    fn create_error_response(&self, _error: crate::error::HeraclitusError) -> Vec<u8> {
        // For now, return empty response
        vec![]
    }

    /// Create an authentication error response
    fn create_auth_error_response(&self, correlation_id: i32) -> Result<Vec<u8>> {
        use crate::protocol::kafka_protocol::*;

        // Create a minimal request object for response building
        let request = crate::protocol::request::KafkaRequest::new(
            0, // API key doesn't matter for error response
            0, // API version doesn't matter
            correlation_id,
            None,
            bytes::Bytes::new(),
        )?;

        Ok(self
            .response_builder
            .build_error_response(&request, ERROR_SASL_AUTHENTICATION_FAILED))
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        debug!("Dropping connection handler");
    }
}
