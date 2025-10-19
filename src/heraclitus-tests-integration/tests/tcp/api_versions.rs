use anyhow::Result;
use bytes::Buf;
use heraclitus_tests_integration::{
    HeraclitusTestContext, MinioTestContext, find_available_port, init_test_tracing,
};
use std::io::Cursor;
use tokio::net::TcpStream;

use super::helpers::{send_api_versions_request, send_api_versions_request_v3};

#[tokio::test]
async fn test_api_versions_request() -> Result<()> {
    init_test_tracing();

    // Start MinIO
    let minio = MinioTestContext::new("heraclitus-api-test").await?;

    // Find available port and start Heraclitus
    let kafka_port = find_available_port().await?;
    let _heraclitus = HeraclitusTestContext::new(&minio, kafka_port).await?;

    // Connect to Heraclitus
    let mut stream = TcpStream::connect(format!("127.0.0.1:{kafka_port}")).await?;

    // Test 1: Send API versions request (v0)
    let response = send_api_versions_request(&mut stream, 1).await?;

    // Parse the response
    let mut cursor = Cursor::new(&response[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID

    let error_code = cursor.get_i16();
    println!("API versions response error code: {error_code}");
    assert_eq!(error_code, 0, "API versions request should succeed");

    let api_count = cursor.get_i32();
    println!("Number of supported APIs: {api_count}");
    assert!(api_count > 0, "Should support at least one API");

    // Parse API entries
    for i in 0..api_count {
        let api_key = cursor.get_i16();
        let min_version = cursor.get_i16();
        let max_version = cursor.get_i16();
        println!("API {i}: key={api_key}, min_version={min_version}, max_version={max_version}");
    }

    // Test 2: Send API versions request (v3) with client info
    let response = send_api_versions_request_v3(&mut stream, 2, "heraclitus-test", "0.1.0").await?;

    // Parse the response
    let mut cursor = Cursor::new(&response[..]);
    Buf::advance(&mut cursor, 4); // Skip correlation ID

    let error_code = cursor.get_i16();
    println!("API versions v3 response error code: {error_code}");
    assert_eq!(error_code, 0, "API versions v3 request should succeed");

    Ok(())
}
