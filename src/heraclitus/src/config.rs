use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HeraclitusConfig {
    #[serde(default = "default_enabled")]
    pub enabled: bool,

    #[serde(default = "default_kafka_port")]
    pub kafka_port: u16,

    #[serde(default)]
    pub state: StateConfig,

    #[serde(default)]
    pub batching: BatchingConfig,

    #[serde(default)]
    pub cache: CacheConfig,

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

impl Default for HeraclitusConfig {
    fn default() -> Self {
        Self {
            enabled: default_enabled(),
            kafka_port: default_kafka_port(),
            state: StateConfig::default(),
            batching: BatchingConfig::default(),
            cache: CacheConfig::default(),
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

impl HeraclitusConfig {
    pub fn batch_delay_duration(&self) -> Duration {
        Duration::from_millis(self.batching.max_batch_delay_ms)
    }

    pub fn metadata_cache_ttl(&self) -> Duration {
        Duration::from_secs(self.state.metadata_cache_ttl_sec)
    }
}

fn default_enabled() -> bool {
    false
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
