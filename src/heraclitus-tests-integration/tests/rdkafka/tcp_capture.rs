// TCP capture test to see what rdkafka is sending

use anyhow::Result;
use rdkafka::config::ClientConfig;
use rdkafka::producer::BaseProducer;
use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::time::sleep;

#[tokio::test]
async fn test_tcp_capture() -> Result<()> {
    println!("Starting TCP capture test");

    // Start a simple TCP server that logs everything
    let listener = TcpListener::bind("127.0.0.1:39092").await?;
    println!("TCP server listening on port 39092");

    // Start server task
    let _server_task = tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, addr)) => {
                    println!("New connection from {addr}");

                    tokio::spawn(async move {
                        let mut buffer = vec![0u8; 4096];
                        let mut total_bytes = 0;

                        loop {
                            match socket.read(&mut buffer).await {
                                Ok(0) => {
                                    println!("Connection closed after {total_bytes} bytes");
                                    break;
                                }
                                Ok(n) => {
                                    total_bytes += n;
                                    println!("Received {n} bytes (total: {total_bytes})");
                                    println!("Data: {:?}", &buffer[..n]);

                                    // Try to parse as Kafka frame
                                    if n >= 4 {
                                        let frame_size = i32::from_be_bytes([
                                            buffer[0], buffer[1], buffer[2], buffer[3],
                                        ]);
                                        println!("Frame size: {frame_size}");

                                        if n >= 8 {
                                            let api_key =
                                                i16::from_be_bytes([buffer[4], buffer[5]]);
                                            let api_version =
                                                i16::from_be_bytes([buffer[6], buffer[7]]);
                                            println!(
                                                "API key: {api_key}, API version: {api_version}"
                                            );
                                        }
                                    }
                                }
                                Err(e) => {
                                    eprintln!("Read error: {e}");
                                    break;
                                }
                            }
                        }
                    });
                }
                Err(e) => {
                    eprintln!("Accept error: {e}");
                }
            }
        }
    });

    // Give server time to start
    sleep(Duration::from_millis(100)).await;

    // Create rdkafka producer
    println!("Creating rdkafka producer");
    let _producer: BaseProducer = ClientConfig::new()
        .set("bootstrap.servers", "127.0.0.1:39092")
        .set("message.timeout.ms", "5000")
        .set("debug", "broker,protocol")
        .set("socket.timeout.ms", "2000")
        .set("request.timeout.ms", "2000")
        .create()
        .expect("Failed to create producer");

    println!("Created producer, waiting for connection");

    // Wait a bit to see what rdkafka sends
    sleep(Duration::from_secs(3)).await;

    println!("Test complete");

    Ok(())
}
