use crate::error::Result;
use bytes::Bytes;
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Partition assignment strategies
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PartitionStrategy {
    /// Round-robin across partitions
    RoundRobin,
    /// Hash-based partitioning using message key
    KeyHash,
    /// Sticky partitioning for better batching
    Sticky,
    /// Manual partition assignment
    Manual,
}

/// Optimized partitioner for Kafka producers
pub struct Partitioner {
    strategy: PartitionStrategy,
    partition_count: Arc<RwLock<HashMap<String, i32>>>,
    round_robin_counter: Arc<RwLock<HashMap<String, u64>>>,
    sticky_partitions: Arc<RwLock<HashMap<String, StickyPartitionInfo>>>,
}

#[derive(Debug)]
struct StickyPartitionInfo {
    current_partition: i32,
    batch_count: usize,
    last_rotation: std::time::Instant,
}

impl Partitioner {
    pub fn new(strategy: PartitionStrategy) -> Self {
        Self {
            strategy,
            partition_count: Arc::new(RwLock::new(HashMap::new())),
            round_robin_counter: Arc::new(RwLock::new(HashMap::new())),
            sticky_partitions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Update partition count for a topic
    pub fn update_partition_count(&self, topic: &str, count: i32) {
        self.partition_count.write().insert(topic.to_string(), count);
    }

    /// Get partition for a message
    pub fn partition(
        &self,
        topic: &str,
        key: Option<&Bytes>,
        partition: Option<i32>,
    ) -> Result<i32> {
        // Manual partition assignment takes precedence
        if let Some(p) = partition {
            return Ok(p);
        }

        let partition_count = self
            .partition_count
            .read()
            .get(topic)
            .copied()
            .unwrap_or(1);

        if partition_count <= 0 {
            return Err(crate::error::HeraclitusError::Protocol(format!(
                "Invalid partition count for topic {}: {}",
                topic, partition_count
            )));
        }

        match self.strategy {
            PartitionStrategy::Manual => Ok(0), // Default to first partition
            PartitionStrategy::RoundRobin => self.round_robin_partition(topic, partition_count),
            PartitionStrategy::KeyHash => self.key_hash_partition(key, partition_count),
            PartitionStrategy::Sticky => self.sticky_partition(topic, key, partition_count),
        }
    }

    fn round_robin_partition(&self, topic: &str, partition_count: i32) -> Result<i32> {
        let mut counters = self.round_robin_counter.write();
        let counter = counters.entry(topic.to_string()).or_insert(0);
        let partition = (*counter % partition_count as u64) as i32;
        *counter = counter.wrapping_add(1);
        Ok(partition)
    }

    fn key_hash_partition(&self, key: Option<&Bytes>, partition_count: i32) -> Result<i32> {
        match key {
            Some(k) if !k.is_empty() => {
                // Use murmur2 hash for compatibility with Java client
                let hash = murmur2_32(k, 0x9747b28c);
                Ok((hash.abs() % partition_count) as i32)
            }
            _ => {
                // No key, use random partition
                use rand::Rng;
                Ok(rand::thread_rng().gen_range(0..partition_count))
            }
        }
    }

    fn sticky_partition(
        &self,
        topic: &str,
        key: Option<&Bytes>,
        partition_count: i32,
    ) -> Result<i32> {
        // If we have a key, use key-based partitioning
        if let Some(k) = key {
            if !k.is_empty() {
                return self.key_hash_partition(Some(k), partition_count);
            }
        }

        let mut sticky_map = self.sticky_partitions.write();
        let sticky_info = sticky_map
            .entry(topic.to_string())
            .or_insert_with(|| StickyPartitionInfo {
                current_partition: 0,
                batch_count: 0,
                last_rotation: std::time::Instant::now(),
            });

        // Rotate partition after certain conditions
        let should_rotate = sticky_info.batch_count >= 100 || // After 100 batches
            sticky_info.last_rotation.elapsed() > std::time::Duration::from_millis(100); // Or after 100ms

        if should_rotate {
            sticky_info.current_partition = (sticky_info.current_partition + 1) % partition_count;
            sticky_info.batch_count = 0;
            sticky_info.last_rotation = std::time::Instant::now();
        }

        sticky_info.batch_count += 1;
        Ok(sticky_info.current_partition)
    }

    /// Get load distribution statistics
    pub fn get_stats(&self) -> PartitionStats {
        PartitionStats {
            round_robin_counters: self.round_robin_counter.read().clone(),
            sticky_info: self
                .sticky_partitions
                .read()
                .iter()
                .map(|(k, v)| (k.clone(), v.current_partition, v.batch_count))
                .collect(),
        }
    }
}

#[derive(Debug)]
pub struct PartitionStats {
    pub round_robin_counters: HashMap<String, u64>,
    pub sticky_info: Vec<(String, i32, usize)>,
}

/// Murmur2 32-bit hash implementation (compatible with Java client)
pub fn murmur2_32(data: &[u8], seed: u32) -> i32 {
    let m: u32 = 0x5bd1e995;
    let r = 24;

    let mut h = seed ^ (data.len() as u32);
    let mut offset = 0;

    while offset + 4 <= data.len() {
        let mut k = u32::from_le_bytes([
            data[offset],
            data[offset + 1],
            data[offset + 2],
            data[offset + 3],
        ]);

        k = k.wrapping_mul(m);
        k ^= k >> r;
        k = k.wrapping_mul(m);

        h = h.wrapping_mul(m);
        h ^= k;

        offset += 4;
    }

    // Handle remaining bytes
    if offset < data.len() {
        let remaining = &data[offset..];
        for (i, &byte) in remaining.iter().enumerate() {
            h ^= (byte as u32) << (i * 8);
        }
        h = h.wrapping_mul(m);
    }

    h ^= h >> 13;
    h = h.wrapping_mul(m);
    h ^= h >> 15;

    h as i32
}

/// Optimized batch partitioning for multiple messages
pub struct BatchPartitioner {
    partitioner: Partitioner,
    cache: RwLock<HashMap<Vec<u8>, i32>>, // Cache key -> partition mapping
}

impl BatchPartitioner {
    pub fn new(strategy: PartitionStrategy) -> Self {
        Self {
            partitioner: Partitioner::new(strategy),
            cache: RwLock::new(HashMap::new()),
        }
    }

    /// Partition a batch of messages efficiently
    pub fn partition_batch(
        &self,
        topic: &str,
        messages: &[(Option<Bytes>, Bytes)], // (key, value) pairs
    ) -> Result<HashMap<i32, Vec<usize>>> {
        let mut partition_map: HashMap<i32, Vec<usize>> = HashMap::new();

        for (idx, (key, _value)) in messages.iter().enumerate() {
            let partition = if let Some(k) = key {
                // Check cache first
                let cache_key = k.to_vec();
                if let Some(&cached_partition) = self.cache.read().get(&cache_key) {
                    cached_partition
                } else {
                    let partition = self.partitioner.partition(topic, Some(k), None)?;
                    // Cache the result
                    self.cache.write().insert(cache_key, partition);
                    partition
                }
            } else {
                self.partitioner.partition(topic, None, None)?
            };

            partition_map
                .entry(partition)
                .or_insert_with(Vec::new)
                .push(idx);
        }

        Ok(partition_map)
    }

    /// Clear the partition cache
    pub fn clear_cache(&self) {
        self.cache.write().clear();
    }

    /// Get cache size
    pub fn cache_size(&self) -> usize {
        self.cache.read().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_round_robin_partitioner() {
        let partitioner = Partitioner::new(PartitionStrategy::RoundRobin);
        partitioner.update_partition_count("test-topic", 3);

        let partitions: Vec<_> = (0..6)
            .map(|_| partitioner.partition("test-topic", None, None).unwrap())
            .collect();

        assert_eq!(partitions, vec![0, 1, 2, 0, 1, 2]);
    }

    #[test]
    fn test_key_hash_partitioner() {
        let partitioner = Partitioner::new(PartitionStrategy::KeyHash);
        partitioner.update_partition_count("test-topic", 10);

        let key1 = Bytes::from("key1");
        let key2 = Bytes::from("key2");

        // Same key should always go to same partition
        let p1 = partitioner.partition("test-topic", Some(&key1), None).unwrap();
        let p2 = partitioner.partition("test-topic", Some(&key1), None).unwrap();
        assert_eq!(p1, p2);

        // Different keys should (likely) go to different partitions
        let p3 = partitioner.partition("test-topic", Some(&key2), None).unwrap();
        // This might occasionally fail due to hash collisions, but very unlikely
        assert_ne!(p1, p3);
    }

    #[test]
    fn test_murmur2_hash() {
        // Test vectors from Kafka's Java implementation
        assert_eq!(murmur2_32(b"", 0x9747b28c), -1617704563);
        assert_eq!(murmur2_32(b"test", 0x9747b28c), -632113869);
        assert_eq!(murmur2_32(b"hello", 0x9747b28c), -1846442199);
    }
}