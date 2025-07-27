// Protocol comparison test using HeraclitusAgent directly
// This compares Kafka's protocol responses with Heraclitus's responses

use bytes::{BufMut, BytesMut};
use heraclitus::{HeraclitusAgent, HeraclitusConfig};
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use testcontainers::{ContainerAsync, runners::AsyncRunner};
use testcontainers_modules::kafka::apache::Kafka;
use tokio::time::sleep;

// Protocol analyzer module for parsing Kafka protocol messages
mod protocol_analyzer {
    use bytes::Buf;

    #[derive(Debug, Clone, PartialEq)]
    pub struct ProtocolMessage {
        pub frame_size: i32,
        pub correlation_id: i32,
        pub body: Vec<u8>,
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ApiVersionsResponse {
        pub error_code: i16,
        pub api_versions: Vec<ApiVersion>,
        pub throttle_time_ms: Option<i32>, // Only in v1+
    }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ApiVersion {
        pub api_key: i16,
        pub min_version: i16,
        pub max_version: i16,
    }

    pub fn parse_protocol_message(data: &[u8]) -> Result<ProtocolMessage, String> {
        if data.len() < 8 {
            return Err("Message too short".to_string());
        }

        let mut cursor = std::io::Cursor::new(data);

        let frame_size = cursor.get_i32();
        let correlation_id = cursor.get_i32();

        let body = cursor.into_inner()[8..].to_vec();

        Ok(ProtocolMessage {
            frame_size,
            correlation_id,
            body,
        })
    }

    pub fn parse_api_versions_response_v0(body: &[u8]) -> Result<ApiVersionsResponse, String> {
        let mut cursor = std::io::Cursor::new(body);

        // Error code (2 bytes)
        if cursor.remaining() < 2 {
            return Err("Not enough data for error code".to_string());
        }
        let error_code = cursor.get_i16();

        // Array length (4 bytes)
        if cursor.remaining() < 4 {
            return Err("Not enough data for array length".to_string());
        }
        let array_len = cursor.get_i32();

        // Parse API versions
        let mut api_versions = Vec::new();
        for _ in 0..array_len {
            if cursor.remaining() < 6 {
                return Err("Not enough data for API version entry".to_string());
            }

            let api_key = cursor.get_i16();
            let min_version = cursor.get_i16();
            let max_version = cursor.get_i16();

            api_versions.push(ApiVersion {
                api_key,
                min_version,
                max_version,
            });
        }

        // v0 doesn't have throttle time

        Ok(ApiVersionsResponse {
            error_code,
            api_versions,
            throttle_time_ms: None,
        })
    }
}

/// Captures the raw TCP protocol exchange for a given request
fn capture_protocol_exchange(
    host: &str,
    port: u16,
    request_bytes: &[u8],
) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    let mut stream = TcpStream::connect(format!("{host}:{port}"))?;
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    stream.set_write_timeout(Some(Duration::from_secs(5)))?;

    // Send request
    stream.write_all(request_bytes)?;
    stream.flush()?;

    // Read response
    let mut response = Vec::new();

    // First read the frame size (4 bytes)
    let mut frame_size_buf = [0u8; 4];
    std::io::Read::read_exact(&mut stream, &mut frame_size_buf)?;
    response.extend_from_slice(&frame_size_buf);

    let frame_size = i32::from_be_bytes(frame_size_buf) as usize;

    // Read the frame data
    let mut frame_data = vec![0u8; frame_size];
    std::io::Read::read_exact(&mut stream, &mut frame_data)?;
    response.extend_from_slice(&frame_data);

    Ok(response)
}

/// Creates an ApiVersions request (v0) with the given correlation ID
fn create_api_versions_request_v0(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut request = BytesMut::new();

    // API key (18 for ApiVersions)
    request.put_i16(18);
    // API version (0)
    request.put_i16(0);
    // Correlation ID
    request.put_i32(correlation_id);
    // Client ID (nullable string)
    let client_id_bytes = client_id.as_bytes();
    request.put_i16(client_id_bytes.len() as i16);
    request.put_slice(client_id_bytes);

    // No request body for ApiVersions v0

    // Wrap in frame
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    frame.to_vec()
}

/// Creates an ApiVersions request (v3) with client software info
fn create_api_versions_request_v3(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut request = BytesMut::new();

    // API key (18 for ApiVersions)
    request.put_i16(18);
    // API version (3)
    request.put_i16(3);
    // Correlation ID
    request.put_i32(correlation_id);
    // Client ID (nullable string)
    let client_id_bytes = client_id.as_bytes();
    request.put_i16(client_id_bytes.len() as i16);
    request.put_slice(client_id_bytes);

    // Request body for v3 (uses flexible encoding)
    let mut body = BytesMut::new();

    // Client software name (compact string)
    body.put_u8(8); // length + 1 for "rdkafka"
    body.put_slice(b"rdkafka");

    // Client software version (compact string)
    body.put_u8(12); // length + 1 for "librdkafka"
    body.put_slice(b"librdkafka");

    // Tagged fields (empty)
    body.put_u8(0);

    request.extend_from_slice(&body);

    // Wrap in frame
    let mut frame = BytesMut::new();
    frame.put_i32(request.len() as i32);
    frame.extend_from_slice(&request);

    frame.to_vec()
}

async fn find_available_port() -> Result<u16, Box<dyn std::error::Error>> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    drop(listener);
    Ok(port)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_api_versions_v0_protocol_direct() {
        // Start Kafka container for comparison
        let kafka_container: ContainerAsync<Kafka> = Kafka::default()
            .start()
            .await
            .expect("Failed to start Kafka container");
        let kafka_port = kafka_container
            .get_host_port_ipv4(testcontainers_modules::kafka::apache::KAFKA_PORT)
            .await
            .expect("Failed to get Kafka port");

        // Find available port for Heraclitus
        let heraclitus_port = find_available_port()
            .await
            .expect("Failed to find available port");

        // Create HeraclitusAgent configuration
        let config = HeraclitusConfig {
            kafka_port: heraclitus_port,
            http_port: find_available_port()
                .await
                .expect("Failed to find available port"),
            storage: heraclitus::config::StorageConfig {
                path: "memory://".to_string(),
            },
            metrics: heraclitus::config::MetricsConfig {
                enabled: false,
                ..Default::default()
            },
            ..Default::default()
        };

        // Start HeraclitusAgent in background
        let agent_handle = tokio::spawn(async move {
            match HeraclitusAgent::new(config).await {
                Ok(agent) => {
                    println!("HeraclitusAgent started successfully");
                    if let Err(e) = agent.run().await {
                        eprintln!("HeraclitusAgent error: {e}");
                    }
                }
                Err(e) => {
                    eprintln!("Failed to create HeraclitusAgent: {e}");
                }
            }
        });

        // Wait for services to be ready
        sleep(Duration::from_secs(5)).await;

        // Create ApiVersions request v0
        let correlation_id = 1;
        let client_id = "protocol-test";
        let request_v0 = create_api_versions_request_v0(correlation_id, client_id);

        println!("\n=== Testing ApiVersions v0 ===");
        println!("Sending request: {request_v0:02x?}");

        // Send to Kafka
        let kafka_response = capture_protocol_exchange("127.0.0.1", kafka_port, &request_v0)
            .expect("Failed to get Kafka response");
        println!(
            "\nKafka response ({} bytes): {:02x?}",
            kafka_response.len(),
            &kafka_response[..std::cmp::min(100, kafka_response.len())]
        );

        // Send to Heraclitus
        let heraclitus_response =
            capture_protocol_exchange("127.0.0.1", heraclitus_port, &request_v0)
                .expect("Failed to get Heraclitus response");
        println!(
            "\nHeraclitus response ({} bytes): {:02x?}",
            heraclitus_response.len(),
            &heraclitus_response[..std::cmp::min(100, heraclitus_response.len())]
        );

        // Compare frame sizes
        let kafka_frame_size = i32::from_be_bytes([
            kafka_response[0],
            kafka_response[1],
            kafka_response[2],
            kafka_response[3],
        ]);
        let heraclitus_frame_size = i32::from_be_bytes([
            heraclitus_response[0],
            heraclitus_response[1],
            heraclitus_response[2],
            heraclitus_response[3],
        ]);

        println!("\nFrame sizes:");
        println!("  Kafka: {kafka_frame_size} bytes");
        println!("  Heraclitus: {heraclitus_frame_size} bytes");

        // Parse and compare API versions
        if let Ok(kafka_msg) = protocol_analyzer::parse_protocol_message(&kafka_response) {
            if let Ok(kafka_api_resp) =
                protocol_analyzer::parse_api_versions_response_v0(&kafka_msg.body)
            {
                println!(
                    "\nKafka API versions: {} total",
                    kafka_api_resp.api_versions.len()
                );
                for api in &kafka_api_resp.api_versions[..5] {
                    println!(
                        "  API {}: v{}-v{}",
                        api.api_key, api.min_version, api.max_version
                    );
                }
            }
        }

        if let Ok(heraclitus_msg) = protocol_analyzer::parse_protocol_message(&heraclitus_response)
        {
            if let Ok(heraclitus_api_resp) =
                protocol_analyzer::parse_api_versions_response_v0(&heraclitus_msg.body)
            {
                println!(
                    "\nHeraclitus API versions: {} total",
                    heraclitus_api_resp.api_versions.len()
                );
                for api in &heraclitus_api_resp.api_versions[..5] {
                    println!(
                        "  API {}: v{}-v{}",
                        api.api_key, api.min_version, api.max_version
                    );
                }
            }
        }

        // Now test ApiVersions v3 (what rdkafka uses)
        println!("\n\n=== Testing ApiVersions v3 ===");
        let request_v3 = create_api_versions_request_v3(2, "rdkafka");
        println!("Sending request: {request_v3:02x?}");

        // Send v3 to Heraclitus
        match capture_protocol_exchange("127.0.0.1", heraclitus_port, &request_v3) {
            Ok(response) => {
                println!(
                    "\nHeraclitus v3 response ({} bytes): {:02x?}",
                    response.len(),
                    &response[..std::cmp::min(100, response.len())]
                );
            }
            Err(e) => {
                println!("\nHeraclitus v3 response error: {e}");
            }
        }

        // Cleanup
        agent_handle.abort();
    }
}
