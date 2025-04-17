use common::discovery::{register, deregister, watch, Instance};
use futures::StreamExt;
use ntest::timeout;
use std::env;
use testcontainers_modules::{nats::Nats, testcontainers::ContainerAsync};

/// Helper to start a NATS container and set NATS_URL env.
async fn start_nats() -> ContainerAsync<Nats> {
    // Start NATS container
    let container = Nats::default()
        .start()
        .await
        .expect("Failed to start NATS container");
    // Allow NATS to initialize
    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    // Configure NATS_URL for discovery
    let host = container.get_host().await.unwrap();
    let port = container.get_host_port_ipv4(4222).await.unwrap();
    env::set_var("NATS_URL", format!("{}:{}", host, port));
    container
}

#[tokio::test]
#[timeout(15000)]
async fn test_watch_register_and_deregister_nats() {
    // Start NATS and configure environment
    let _container = start_nats().await;

    // Begin watching for 'acceptor' role
    let mut stream = watch("acceptor");

    // Initial snapshot: no instances
    let list0 = stream.next().await.expect("Expected initial snapshot");
    assert!(list0.is_empty(), "Initial instances should be empty");

    // Register first instance
    let inst1 = Instance {
        id: "id1".into(),
        host: "host1".into(),
        port: 4317,
    };
    register("acceptor", inst1.clone())
        .await
        .expect("Failed to register inst1");
    let list1 = stream.next().await.expect("Expected list after inst1 register");
    assert_eq!(list1, vec![inst1.clone()]);

    // Register second instance
    let inst2 = Instance {
        id: "id2".into(),
        host: "host2".into(),
        port: 4318,
    };
    register("acceptor", inst2.clone())
        .await
        .expect("Failed to register inst2");
    let mut list2 = stream.next().await.expect("Expected list after inst2 register");
    // Sort for deterministic comparison
    list2.sort_by(|a, b| a.id.cmp(&b.id));
    let mut expected = vec![inst1.clone(), inst2.clone()];
    expected.sort_by(|a, b| a.id.cmp(&b.id));
    assert_eq!(list2, expected);

    // Deregister first instance
    deregister("acceptor", &inst1.id)
        .await
        .expect("Failed to deregister inst1");
    let list3 = stream.next().await.expect("Expected list after inst1 deregister");
    assert_eq!(list3, vec![inst2.clone()]);

    // Deregister second instance
    deregister("acceptor", &inst2.id)
        .await
        .expect("Failed to deregister inst2");
    let list4 = stream.next().await.expect("Expected list after inst2 deregister");
    assert!(list4.is_empty(), "Instances should be empty after deregister");
}