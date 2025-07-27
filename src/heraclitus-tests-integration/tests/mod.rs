// Integration tests for Heraclitus
// These tests verify protocol compatibility and functionality

// TCP-based tests - Direct protocol implementation tests
mod tcp;

// RdKafka-based tests - Client library compatibility tests
mod rdkafka;

// Cloud storage integration tests
#[cfg(feature = "minio-integration")]
mod minio_integration;

// Shared test utilities
pub mod test_utils {
    use std::net::TcpStream;
    use std::time::Duration;

    /// Wait for a service to be ready on the given address
    pub fn wait_for_service(addr: &str, timeout: Duration) -> Result<(), String> {
        let start = std::time::Instant::now();

        loop {
            if TcpStream::connect(addr).is_ok() {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(format!(
                    "Service at {addr} did not become ready within {timeout:?}"
                ));
            }

            std::thread::sleep(Duration::from_millis(100));
        }
    }

    /// Common test configuration
    pub struct TestConfig {
        pub kafka_port: u16,
        pub http_port: u16,
    }

    impl Default for TestConfig {
        fn default() -> Self {
            Self {
                kafka_port: 9092,
                http_port: 9093,
            }
        }
    }
}
