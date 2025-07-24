use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeraclitusConfig {
    #[serde(default = "default_kafka_port")]
    pub kafka_port: u16,

    #[serde(default = "default_http_port")]
    pub http_port: u16,

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

    #[serde(default = "default_shutdown_timeout_sec")]
    pub shutdown_timeout_sec: u64,

    #[serde(skip)]
    pub common_config: common::config::Configuration,
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

    #[serde(default = "default_max_batch_bytes")]
    pub max_batch_bytes: usize,

    #[serde(default = "default_max_batch_delay_ms")]
    pub max_batch_delay_ms: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_segment_cache_size_mb")]
    pub segment_cache_size_mb: usize,

    #[serde(default = "default_prefetch_segments")]
    pub prefetch_segments: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuthConfig {
    #[serde(default)]
    pub enabled: bool,

    #[serde(default = "default_auth_mechanism")]
    pub mechanism: String,

    #[serde(default)]
    pub plain_username: Option<String>,

    #[serde(default)]
    pub plain_password: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_enabled")]
    pub enabled: bool,

    #[serde(default = "default_metrics_prefix")]
    pub prefix: String,
}

impl Default for HeraclitusConfig {
    fn default() -> Self {
        Self {
            kafka_port: default_kafka_port(),
            http_port: default_http_port(),
            state: StateConfig::default(),
            batching: BatchingConfig::default(),
            cache: CacheConfig::default(),
            auth: AuthConfig::default(),
            metrics: MetricsConfig::default(),
            shutdown_timeout_sec: default_shutdown_timeout_sec(),
            common_config: common::config::Configuration::default(),
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
            max_batch_bytes: default_max_batch_bytes(),
            max_batch_delay_ms: default_max_batch_delay_ms(),
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            segment_cache_size_mb: default_segment_cache_size_mb(),
            prefetch_segments: default_prefetch_segments(),
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

impl HeraclitusConfig {
    pub fn batch_delay_duration(&self) -> Duration {
        Duration::from_millis(self.batching.max_batch_delay_ms)
    }

    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.state.metadata_cache_ttl_sec)
    }

    /// Create HeraclitusConfig from common Configuration
    pub fn from_common_config(config: common::config::Configuration) -> Self {
        let heraclitus_config = &config.heraclitus;
        Self {
            kafka_port: heraclitus_config.kafka_port,
            http_port: default_http_port(), // Use default for now
            state: StateConfig {
                prefix: heraclitus_config.state_prefix.clone(),
                metadata_cache_ttl_sec: 60, // Default 60 seconds
            },
            batching: BatchingConfig {
                max_batch_size: heraclitus_config.batch_size,
                max_batch_bytes: 10 * 1024 * 1024, // 10MB default
                max_batch_delay_ms: heraclitus_config.batch_timeout_ms,
            },
            cache: CacheConfig {
                segment_cache_size_mb: (heraclitus_config.segment_size_mb as usize) * 16, // Cache 16 segments
                prefetch_segments: 3,
            },
            auth: AuthConfig::default(),       // Use defaults for now
            metrics: MetricsConfig::default(), // Use defaults for now
            shutdown_timeout_sec: default_shutdown_timeout_sec(),
            common_config: config,
        }
    }
}

fn default_kafka_port() -> u16 {
    9092
}

fn default_state_prefix() -> String {
    "heraclitus/".to_string()
}

fn default_metadata_cache_ttl() -> u64 {
    60
}

fn default_max_batch_size() -> usize {
    10000
}

fn default_max_batch_bytes() -> usize {
    10 * 1024 * 1024 // 10MB
}

fn default_max_batch_delay_ms() -> u64 {
    100
}

fn default_segment_cache_size_mb() -> usize {
    1024 // 1GB
}

fn default_prefetch_segments() -> usize {
    3
}

fn default_auth_mechanism() -> String {
    "PLAIN".to_string()
}

fn default_http_port() -> u16 {
    9093
}

fn default_metrics_enabled() -> bool {
    true
}

fn default_metrics_prefix() -> String {
    "heraclitus_".to_string()
}

fn default_shutdown_timeout_sec() -> u64 {
    30
}
