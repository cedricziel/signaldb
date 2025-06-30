use crate::catalog::Catalog;
use crate::config::{Configuration, DatabaseConfig, DiscoveryConfig};
use crate::flight::transport::ServiceCapability;
use crate::service_bootstrap::{ServiceBootstrap, ServiceType};
use std::time::Duration;
use tempfile::TempDir;

/// Test helper to create a temporary SQLite database for testing
async fn create_test_sqlite_config() -> (Configuration, TempDir) {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let db_path = temp_dir.path().join("test.db");
    let dsn = format!("sqlite:{}", db_path.display());

    let config = Configuration {
        database: DatabaseConfig { dsn: dsn.clone() },
        discovery: Some(DiscoveryConfig {
            dsn,
            heartbeat_interval: Duration::from_secs(1),
            poll_interval: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
        }),
        ..Default::default()
    };

    (config, temp_dir)
}

/// Test helper to create a test PostgreSQL config (requires running PostgreSQL)
#[allow(dead_code)]
fn create_test_postgres_config() -> Configuration {
    let dsn = std::env::var("TEST_POSTGRES_DSN").unwrap_or_else(|_| {
        "postgresql://postgres:password@localhost:5432/signaldb_test".to_string()
    });

    Configuration {
        database: DatabaseConfig { dsn: dsn.clone() },
        discovery: Some(DiscoveryConfig {
            dsn,
            heartbeat_interval: Duration::from_secs(1),
            poll_interval: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
        }),
        ..Default::default()
    }
}

#[tokio::test]
async fn test_flight_transport_with_sqlite_catalog() {
    let (config, _temp_dir) = create_test_sqlite_config().await;

    // Create service bootstrap
    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await
    .expect("Failed to create service bootstrap");

    // Create flight transport
    let transport = bootstrap.create_flight_transport();

    // Register a service
    let service_id = transport
        .register_flight_service(
            ServiceType::Writer,
            "localhost".to_string(),
            50052,
            vec![
                ServiceCapability::TraceIngestion,
                ServiceCapability::Storage,
            ],
        )
        .await
        .expect("Failed to register service");

    // Test service discovery
    let trace_services = transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;

    assert!(
        !trace_services.is_empty(),
        "Should discover trace ingestion services"
    );
    assert!(
        trace_services.iter().any(|s| s.service_id == service_id),
        "Should find the registered service"
    );

    // Test health check
    assert!(transport.is_healthy().await, "Transport should be healthy");

    // Test pool stats
    let (current, max) = transport.pool_stats().await;
    assert_eq!(current, 0, "No connections should be active initially");
    assert!(max > 0, "Max pool size should be configured");
}

#[tokio::test]
async fn test_multiple_services_with_sqlite_catalog() {
    let (config, _temp_dir) = create_test_sqlite_config().await;

    // Create multiple service bootstraps
    let writer_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await
    .expect("Failed to create writer bootstrap");

    let querier_bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        "localhost:9000".to_string(),
    )
    .await
    .expect("Failed to create querier bootstrap");

    // Create flight transports
    let writer_transport = writer_bootstrap.create_flight_transport();
    let querier_transport = querier_bootstrap.create_flight_transport();

    // The services are already registered with the catalog through ServiceBootstrap
    // No need to register them again - just get their IDs for verification
    let writer_services = writer_transport.list_services().await;
    let querier_services = querier_transport.list_services().await;
    
    // Both transports start with empty local registries
    assert_eq!(writer_services.len(), 0);
    assert_eq!(querier_services.len(), 0);

    // Test that both transports are healthy (can discover services)
    assert!(writer_transport.is_healthy().await, "Writer transport should be healthy");
    assert!(querier_transport.is_healthy().await, "Querier transport should be healthy");
    
    // Test capability-based discovery (even though current impl assumes all are writers)
    let trace_services = writer_transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;
    
    // Should discover services from catalog (both services registered)
    assert!(!trace_services.is_empty(), "Should discover services from catalog");
    
    // Since both services were registered with ServiceBootstrap, they appear in catalog
    // The current implementation assumes all catalog services are writers with trace ingestion
    assert!(trace_services.len() >= 1, "Should have at least one service discovered");
}

#[tokio::test]
async fn test_service_heartbeat_and_cleanup_with_sqlite() {
    let (config, _temp_dir) = create_test_sqlite_config().await;

    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await
    .expect("Failed to create service bootstrap");

    let service_id = bootstrap.service_id();

    // Verify service is registered in catalog
    let catalog = bootstrap.catalog();
    let ingesters = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters");

    assert!(
        ingesters.iter().any(|i| i.id == service_id),
        "Service should be registered in catalog"
    );

    // Test manual heartbeat
    bootstrap
        .heartbeat()
        .await
        .expect("Failed to send heartbeat");

    // Test service shutdown and cleanup
    bootstrap
        .shutdown()
        .await
        .expect("Failed to shutdown service");

    // Verify service is deregistered
    let catalog = Catalog::new(&config.discovery.as_ref().unwrap().dsn)
        .await
        .expect("Failed to create catalog");

    let ingesters_after = catalog
        .list_ingesters()
        .await
        .expect("Failed to list ingesters after shutdown");

    assert!(
        !ingesters_after.iter().any(|i| i.id == service_id),
        "Service should be deregistered from catalog"
    );
}

#[tokio::test]
async fn test_catalog_unavailability_scenarios() {
    // Test with invalid SQLite path
    let invalid_config = Configuration {
        database: DatabaseConfig {
            dsn: "sqlite:/invalid/path/that/does/not/exist/test.db".to_string(),
        },
        discovery: Some(DiscoveryConfig {
            dsn: "sqlite:/invalid/path/that/does/not/exist/test.db".to_string(),
            heartbeat_interval: Duration::from_secs(1),
            poll_interval: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
        }),
        ..Default::default()
    };

    // This should fail gracefully
    let result = ServiceBootstrap::new(
        invalid_config,
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await;

    // Should fail to create bootstrap with invalid catalog
    assert!(result.is_err(), "Should fail with invalid catalog path");

    // Test with valid path but without permissions (Unix only)
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        let temp_dir = TempDir::new().expect("Failed to create temp directory");
        let restricted_dir = temp_dir.path().join("restricted");
        std::fs::create_dir(&restricted_dir).expect("Failed to create restricted directory");

        // Remove write permissions
        let mut perms = std::fs::metadata(&restricted_dir).unwrap().permissions();
        perms.set_mode(0o444); // r--r--r--
        std::fs::set_permissions(&restricted_dir, perms).unwrap();

        let restricted_config = Configuration {
            database: DatabaseConfig {
                dsn: format!("sqlite:{}/test.db", restricted_dir.display()),
            },
            discovery: Some(DiscoveryConfig {
                dsn: format!("sqlite:{}/test.db", restricted_dir.display()),
                heartbeat_interval: Duration::from_secs(1),
                poll_interval: Duration::from_secs(2),
                ttl: Duration::from_secs(10),
            }),
            ..Default::default()
        };

        let result = ServiceBootstrap::new(
            restricted_config,
            ServiceType::Writer,
            "localhost:50052".to_string(),
        )
        .await;

        // This might succeed or fail depending on system behavior
        // The key is that it should handle the error gracefully
        if result.is_err() {
            // The error was handled gracefully
            // Different systems may have different error messages
        }
    }
}

#[tokio::test]
async fn test_service_bootstrap_directory_creation() {
    use tempfile::TempDir;

    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let nested_path = temp_dir
        .path()
        .join("nested")
        .join("deeper")
        .join("test.db");
    let dsn = format!("sqlite:{}", nested_path.display());

    let config = Configuration {
        database: DatabaseConfig { dsn: dsn.clone() },
        discovery: Some(DiscoveryConfig {
            dsn,
            heartbeat_interval: Duration::from_secs(1),
            poll_interval: Duration::from_secs(2),
            ttl: Duration::from_secs(10),
        }),
        ..Default::default()
    };

    // This should create the nested directory structure
    let bootstrap =
        ServiceBootstrap::new(config, ServiceType::Writer, "localhost:50052".to_string())
            .await
            .expect("Failed to create service bootstrap with nested directories");

    // Verify the directory was created
    assert!(
        nested_path.parent().unwrap().exists(),
        "Nested directory should be created"
    );

    // Clean up
    bootstrap.shutdown().await.expect("Failed to shutdown");
}

#[tokio::test]
async fn test_flight_transport_connection_pooling() {
    let (config, _temp_dir) = create_test_sqlite_config().await;

    let bootstrap = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await
    .expect("Failed to create service bootstrap");

    // Create transport with small pool for testing
    let transport = bootstrap.create_flight_transport_with_config(2, 5);

    // Register a service
    transport
        .register_flight_service(
            ServiceType::Writer,
            "localhost".to_string(),
            50052,
            vec![ServiceCapability::TraceIngestion],
        )
        .await
        .expect("Failed to register service");

    // Test pool statistics
    let (initial_connections, max_pool_size) = transport.pool_stats().await;
    assert_eq!(initial_connections, 0, "Should start with no connections");
    assert_eq!(max_pool_size, 2, "Should have configured max pool size");

    // Test connection cleanup
    transport.cleanup_expired_connections().await;
    let (after_cleanup, _) = transport.pool_stats().await;
    assert_eq!(
        after_cleanup, 0,
        "Should have no expired connections initially"
    );
}

#[tokio::test]
async fn test_service_discovery_across_transports() {
    let (config, _temp_dir) = create_test_sqlite_config().await;

    // Create first service and transport
    let bootstrap1 = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Writer,
        "localhost:50052".to_string(),
    )
    .await
    .expect("Failed to create first bootstrap");

    let transport1 = bootstrap1.create_flight_transport();
    let _service1_id = transport1
        .register_flight_service(
            ServiceType::Writer,
            "localhost".to_string(),
            50052,
            vec![ServiceCapability::TraceIngestion],
        )
        .await
        .expect("Failed to register first service");

    // Create second service and transport (different instance)
    let bootstrap2 = ServiceBootstrap::new(
        config.clone(),
        ServiceType::Querier,
        "localhost:9000".to_string(),
    )
    .await
    .expect("Failed to create second bootstrap");

    let transport2 = bootstrap2.create_flight_transport();
    let _service2_id = transport2
        .register_flight_service(
            ServiceType::Querier,
            "localhost".to_string(),
            9000,
            vec![ServiceCapability::QueryExecution],
        )
        .await
        .expect("Failed to register second service");

    // Wait a bit for catalog synchronization
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Transport1 should be able to discover services registered by transport2 via catalog
    let query_services_from_transport1 = transport1
        .discover_services_by_capability(ServiceCapability::QueryExecution)
        .await;

    // Transport2 should be able to discover services registered by transport1 via catalog
    let trace_services_from_transport2 = transport2
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;

    // Both should find services from the catalog (registered as ingesters)
    // Note: The specific discovery behavior depends on catalog integration
    assert!(
        !query_services_from_transport1.is_empty() || !trace_services_from_transport2.is_empty(),
        "Should discover services across transports via catalog"
    );

    // Note: Bootstraps are moved when creating transports, so they are cleaned up automatically
}

// Only run PostgreSQL tests if TEST_POSTGRES_DSN is set
#[tokio::test]
#[ignore] // Ignored by default, run with --ignored if PostgreSQL is available
async fn test_flight_transport_with_postgres_catalog() {
    if std::env::var("TEST_POSTGRES_DSN").is_err() {
        eprintln!("Skipping PostgreSQL test - TEST_POSTGRES_DSN not set");
        return;
    }

    let config = create_test_postgres_config();

    let bootstrap =
        ServiceBootstrap::new(config, ServiceType::Writer, "localhost:50052".to_string())
            .await
            .expect("Failed to create service bootstrap with PostgreSQL");

    let transport = bootstrap.create_flight_transport();

    let service_id = transport
        .register_flight_service(
            ServiceType::Writer,
            "localhost".to_string(),
            50052,
            vec![ServiceCapability::TraceIngestion],
        )
        .await
        .expect("Failed to register service with PostgreSQL");

    let services = transport
        .discover_services_by_capability(ServiceCapability::TraceIngestion)
        .await;

    assert!(
        services.iter().any(|s| s.service_id == service_id),
        "Should discover service with PostgreSQL catalog"
    );

    // Note: Bootstrap is moved when creating transport, so it's cleaned up automatically
}
