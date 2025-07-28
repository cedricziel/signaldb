use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeraclitusConfig {
    #[serde(default = "default_kafka_port")]
    pub kafka_port: u16,

    #[serde(default = "default_http_port")]
    pub http_port: u16,

    #[serde(default)]
    pub storage: StorageConfig,

    #[serde(default)]
    pub state: StateConfig,

    #[serde(default)]
    pub batching: BatchingConfig,

    #[serde(default)]
    pub cache: CacheConfig,

    #[serde(default)]
    pub auth: AuthConfig,

    #[serde(default)]
    pub metrics: MetricsConfig,

    #[serde(default)]
    pub performance: PerformanceConfig,

    #[serde(default)]
    pub topics: TopicConfig,

    #[serde(default = "default_shutdown_timeout_sec")]
    pub shutdown_timeout_sec: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageConfig {
    #[serde(default = "default_storage_path")]
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default = "default_state_prefix")]
    pub prefix: String,

    #[serde(default = "default_metadata_cache_ttl")]
    pub metadata_cache_ttl_sec: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BatchingConfig {
    #[serde(default = "default_max_batch_size")]
    pub max_batch_size: usize,

    #[serde(default = "default_max_batch_messages")]
    pub max_batch_messages: usize,

    #[serde(default = "default_flush_interval_ms")]
    pub flush_interval_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_size")]
    pub metadata_cache_size: usize,

    #[serde(default = "default_cache_ttl")]
    pub cache_ttl_sec: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_auth_mechanism")]
    pub mechanism: String,

    pub plain_username: Option<String>,
    pub plain_password: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    #[serde(default = "default_metrics_prefix")]
    pub prefix: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PerformanceConfig {
    #[serde(default = "default_socket_send_buffer")]
    pub socket_send_buffer_bytes: Option<usize>,

    #[serde(default = "default_socket_recv_buffer")]
    pub socket_recv_buffer_bytes: Option<usize>,

    #[serde(default = "default_tcp_nodelay")]
    pub tcp_nodelay: bool,

    #[serde(default = "default_tcp_keepalive")]
    pub tcp_keepalive: Option<u64>,

    #[serde(default = "default_buffer_pool_size")]
    pub buffer_pool_size: usize,

    #[serde(default = "default_compression_level")]
    pub compression_level: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TopicConfig {
    #[serde(default = "default_auto_create_topics_enable")]
    pub auto_create_topics_enable: bool,

    #[serde(default = "default_num_partitions")]
    pub default_num_partitions: i32,

    #[serde(default = "default_replication_factor")]
    pub default_replication_factor: i16,
}

// Default implementations
impl Default for HeraclitusConfig {
    fn default() -> Self {
        Self {
            kafka_port: default_kafka_port(),
            http_port: default_http_port(),
            storage: StorageConfig::default(),
            state: StateConfig::default(),
            batching: BatchingConfig::default(),
            cache: CacheConfig::default(),
            auth: AuthConfig::default(),
            metrics: MetricsConfig::default(),
            performance: PerformanceConfig::default(),
            topics: TopicConfig::default(),
            shutdown_timeout_sec: default_shutdown_timeout_sec(),
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            path: default_storage_path(),
        }
    }
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            prefix: default_state_prefix(),
            metadata_cache_ttl_sec: default_metadata_cache_ttl(),
        }
    }
}

impl Default for BatchingConfig {
    fn default() -> Self {
        Self {
            max_batch_size: default_max_batch_size(),
            max_batch_messages: default_max_batch_messages(),
            flush_interval_ms: default_flush_interval_ms(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            metadata_cache_size: default_cache_size(),
            cache_ttl_sec: default_cache_ttl(),
        }
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            mechanism: default_auth_mechanism(),
            plain_username: None,
            plain_password: None,
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            enabled: default_metrics_enabled(),
            prefix: default_metrics_prefix(),
        }
    }
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            socket_send_buffer_bytes: default_socket_send_buffer(),
            socket_recv_buffer_bytes: default_socket_recv_buffer(),
            tcp_nodelay: default_tcp_nodelay(),
            tcp_keepalive: default_tcp_keepalive(),
            buffer_pool_size: default_buffer_pool_size(),
            compression_level: default_compression_level(),
        }
    }
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            auto_create_topics_enable: default_auto_create_topics_enable(),
            default_num_partitions: default_num_partitions(),
            default_replication_factor: default_replication_factor(),
        }
    }
}

// Default value functions
fn default_kafka_port() -> u16 {
    9092
}
fn default_http_port() -> u16 {
    9093
}
fn default_storage_path() -> String {
    "/var/lib/heraclitus".to_string()
}
fn default_state_prefix() -> String {
    "heraclitus".to_string()
}
fn default_metadata_cache_ttl() -> u64 {
    300
} // 5 minutes
fn default_max_batch_size() -> usize {
    1_048_576
} // 1MB
fn default_max_batch_messages() -> usize {
    1000
}
fn default_flush_interval_ms() -> u64 {
    100
}
fn default_cache_size() -> usize {
    1000
}
fn default_cache_ttl() -> u64 {
    60
}
fn default_auth_mechanism() -> String {
    "PLAIN".to_string()
}
fn default_metrics_enabled() -> bool {
    true
}
fn default_metrics_prefix() -> String {
    "heraclitus".to_string()
}
fn default_socket_send_buffer() -> Option<usize> {
    Some(131_072)
} // 128KB
fn default_socket_recv_buffer() -> Option<usize> {
    Some(131_072)
} // 128KB
fn default_tcp_nodelay() -> bool {
    true
}
fn default_tcp_keepalive() -> Option<u64> {
    Some(60)
} // 60 seconds
fn default_buffer_pool_size() -> usize {
    1000
}
fn default_compression_level() -> i32 {
    6
} // Default compression level
fn default_shutdown_timeout_sec() -> u64 {
    30
}
fn default_auto_create_topics_enable() -> bool {
    true
}
fn default_num_partitions() -> i32 {
    1
}
fn default_replication_factor() -> i16 {
    1
}
