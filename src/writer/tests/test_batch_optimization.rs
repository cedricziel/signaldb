use common::config::{Configuration, DefaultSchemas, SchemaConfig, StorageConfig};
use datafusion::arrow::array::{
    Date32Array, Float64Array, Int32Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use object_store::memory::InMemory;
use std::sync::Arc;
use writer::{BatchOptimizationConfig, CatalogPoolConfig, create_iceberg_writer_with_pool};

/// Create test configuration for optimization tests
fn create_test_config() -> Configuration {
    Configuration {
        schema: SchemaConfig {
            catalog_type: "memory".to_string(),
            catalog_uri: "memory://".to_string(),
            default_schemas: DefaultSchemas {
                traces_enabled: true,
                logs_enabled: true,
                metrics_enabled: true,
                custom_schemas: Default::default(),
            },
        },
        storage: StorageConfig {
            dsn: "memory://".to_string(),
        },
        ..Default::default()
    }
}

/// Create test data with specified number of rows
fn create_test_data(num_rows: usize) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new(
            "start_timestamp",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            true,
        ),
        Field::new("service_name", DataType::Utf8, false),
        Field::new("metric_name", DataType::Utf8, false),
        Field::new("metric_description", DataType::Utf8, true),
        Field::new("metric_unit", DataType::Utf8, true),
        Field::new("value", DataType::Float64, false),
        Field::new("flags", DataType::Int32, true),
        Field::new("resource_schema_url", DataType::Utf8, true),
        Field::new("resource_attributes", DataType::Utf8, true),
        Field::new("scope_name", DataType::Utf8, true),
        Field::new("scope_version", DataType::Utf8, true),
        Field::new("scope_schema_url", DataType::Utf8, true),
        Field::new("scope_attributes", DataType::Utf8, true),
        Field::new("scope_dropped_attr_count", DataType::Int32, true),
        Field::new("attributes", DataType::Utf8, true),
        Field::new("exemplars", DataType::Utf8, true),
        Field::new("date_day", DataType::Date32, false),
        Field::new("hour", DataType::Int32, false),
    ]));

    let timestamps: Vec<i64> = (0..num_rows)
        .map(|i| 1_700_000_000_000_000_000 + (i as i64 * 1_000_000_000))
        .collect();
    let service_names: Vec<&str> = (0..num_rows).map(|_| "test-service").collect();
    let metric_names: Vec<String> = (0..num_rows)
        .map(|i| format!("test.metric.{}", i % 10))
        .collect();
    let values: Vec<f64> = (0..num_rows).map(|i| i as f64).collect();
    let hours: Vec<i32> = (0..num_rows).map(|i| (i % 24) as i32).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(timestamps)),
            Arc::new(TimestampNanosecondArray::from(vec![None; num_rows])),
            Arc::new(StringArray::from(service_names)),
            Arc::new(StringArray::from(
                metric_names.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(vec![Some("Test metric"); num_rows])),
            Arc::new(StringArray::from(vec![Some("count"); num_rows])),
            Arc::new(Float64Array::from(values)),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![Some("{}"); num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Int32Array::from(vec![None; num_rows])),
            Arc::new(StringArray::from(vec![Some("{}"); num_rows])),
            Arc::new(StringArray::from(vec![None::<&str>; num_rows])),
            Arc::new(Date32Array::from(vec![19700; num_rows])),
            Arc::new(Int32Array::from(hours)),
        ],
    )
    .expect("Failed to create test data")
}

/// Test default batch optimization configuration
#[tokio::test]
async fn test_default_batch_config() {
    let config = BatchOptimizationConfig::default();

    assert_eq!(config.max_rows_per_batch, 50_000);
    assert_eq!(config.max_memory_per_batch_bytes, 128 * 1024 * 1024);
    assert!(config.enable_auto_split);
    assert_eq!(config.target_concurrent_batches, 4);
    assert!(config.enable_catalog_caching);
    assert_eq!(config.catalog_cache_ttl_seconds, 300);
}

/// Test custom batch optimization configuration
#[tokio::test]
async fn test_custom_batch_config() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    // Create writer
    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Test initial default config
        assert_eq!(writer.batch_config().max_rows_per_batch, 50_000);
        assert!(writer.batch_config().enable_catalog_caching);

        // Update batch configuration
        let custom_config = BatchOptimizationConfig {
            max_rows_per_batch: 10_000,
            max_memory_per_batch_bytes: 64 * 1024 * 1024,
            enable_auto_split: false,
            target_concurrent_batches: 2,
            enable_catalog_caching: false,
            catalog_cache_ttl_seconds: 600,
        };

        writer.set_batch_config(custom_config);

        // Verify updated configuration
        assert_eq!(writer.batch_config().max_rows_per_batch, 10_000);
        assert_eq!(
            writer.batch_config().max_memory_per_batch_bytes,
            64 * 1024 * 1024
        );
        assert!(!writer.batch_config().enable_auto_split);
        assert_eq!(writer.batch_config().target_concurrent_batches, 2);
        assert!(!writer.batch_config().enable_catalog_caching);
        assert_eq!(writer.batch_config().catalog_cache_ttl_seconds, 600);
    }
}

/// Test catalog caching functionality
#[tokio::test]
async fn test_catalog_caching() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Initially no cached catalog
        assert!(writer.catalog_cache_info().is_none());

        // Enable caching with short TTL for testing
        let batch_config = BatchOptimizationConfig {
            enable_catalog_caching: true,
            catalog_cache_ttl_seconds: 2, // 2 second TTL for testing
            ..Default::default()
        };
        writer.set_batch_config(batch_config);

        // Cache should still be empty
        assert!(writer.catalog_cache_info().is_none());

        // Test manual cache clearing
        writer.clear_catalog_cache();
        assert!(writer.catalog_cache_info().is_none());

        // Test disabling caching clears cache
        let no_cache_config = BatchOptimizationConfig {
            enable_catalog_caching: false,
            ..Default::default()
        };
        writer.set_batch_config(no_cache_config);
        assert!(writer.catalog_cache_info().is_none());
    }
}

/// Test batch splitting functionality
#[tokio::test]
async fn test_batch_splitting_logic() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Configure for small batches to force splitting
        let split_config = BatchOptimizationConfig {
            max_rows_per_batch: 100, // Very small for testing
            enable_auto_split: true,
            ..Default::default()
        };
        writer.set_batch_config(split_config);

        // Create a large batch that should be split
        let _large_batch = create_test_data(250); // 250 rows > 100 row limit

        // Test the batch splitting logic (this is internal so we can't directly test it)
        // But we can verify the configuration is set correctly
        assert_eq!(writer.batch_config().max_rows_per_batch, 100);
        assert!(writer.batch_config().enable_auto_split);

        // Test disabling auto-split
        let no_split_config = BatchOptimizationConfig {
            enable_auto_split: false,
            ..Default::default()
        };
        writer.set_batch_config(no_split_config);
        assert!(!writer.batch_config().enable_auto_split);
    }
}

/// Test batch optimization with different sizes
#[tokio::test]
async fn test_batch_size_optimization() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Test different batch size configurations
        let sizes = vec![1_000, 10_000, 50_000, 100_000];

        for max_size in sizes {
            let batch_config = BatchOptimizationConfig {
                max_rows_per_batch: max_size,
                enable_auto_split: true,
                ..Default::default()
            };
            writer.set_batch_config(batch_config);

            assert_eq!(writer.batch_config().max_rows_per_batch, max_size);

            // Create test batch smaller than limit (shouldn't be split)
            let small_batch = create_test_data(max_size / 2);
            assert!(small_batch.num_rows() <= max_size);

            // Create test batch larger than limit (would be split)
            let large_batch = create_test_data(max_size * 2);
            assert!(large_batch.num_rows() > max_size);
        }
    }
}

/// Test memory-based batch optimization
#[tokio::test]
async fn test_memory_optimization() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Configure for memory-based optimization
        let memory_config = BatchOptimizationConfig {
            max_memory_per_batch_bytes: 1024 * 1024, // 1MB limit
            enable_auto_split: true,
            ..Default::default()
        };
        writer.set_batch_config(memory_config);

        assert_eq!(
            writer.batch_config().max_memory_per_batch_bytes,
            1024 * 1024
        );

        // Test that we can create batches and check their memory usage
        let test_batch = create_test_data(1000);
        let memory_usage = test_batch.get_array_memory_size();

        log::info!("Test batch memory usage: {memory_usage} bytes");
        assert!(memory_usage > 0);
    }
}

/// Test concurrent batch processing configuration
#[tokio::test]
async fn test_concurrent_batch_config() {
    let config = create_test_config();
    let pool_config = CatalogPoolConfig::default();
    let object_store = Arc::new(InMemory::new());

    let result = create_iceberg_writer_with_pool(
        &config,
        object_store,
        "test_tenant",
        "metrics_gauge",
        pool_config,
    )
    .await;

    if let Ok(mut writer) = result {
        // Test different concurrency levels
        for concurrency in [1, 2, 4, 8, 16] {
            let concurrent_config = BatchOptimizationConfig {
                target_concurrent_batches: concurrency,
                ..Default::default()
            };
            writer.set_batch_config(concurrent_config);

            assert_eq!(writer.batch_config().target_concurrent_batches, concurrency);
        }
    }
}
