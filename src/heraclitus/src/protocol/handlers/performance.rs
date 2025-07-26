use crate::config::PerformanceConfig;
use bytes::BytesMut;
use parking_lot::RwLock;
use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{debug, info};

/// Adaptive buffer pool for reducing allocations
pub struct BufferPool {
    _config: Arc<PerformanceConfig>,
    pools: Vec<RwLock<Vec<BytesMut>>>,
    size_classes: Vec<usize>,
}

impl BufferPool {
    pub fn new(config: Arc<PerformanceConfig>) -> Self {
        // Create size classes: 4KB, 16KB, 64KB, 256KB, 1MB, 4MB, 16MB
        let size_classes = vec![
            4 * 1024,
            16 * 1024,
            64 * 1024,
            256 * 1024,
            1024 * 1024,
            4 * 1024 * 1024,
            16 * 1024 * 1024, // 16MB max
        ];

        let pools = size_classes
            .iter()
            .map(|_| RwLock::new(Vec::with_capacity(16)))
            .collect();

        Self {
            _config: config,
            pools,
            size_classes,
        }
    }

    /// Get a buffer that can hold at least `min_size` bytes
    pub fn acquire(&self, min_size: usize) -> BytesMut {
        // Find the appropriate size class
        let size_class_idx = self
            .size_classes
            .iter()
            .position(|&size| size >= min_size)
            .unwrap_or(self.size_classes.len() - 1);

        let size_class = self.size_classes[size_class_idx];

        // Try to get from pool
        if let Some(mut buffer) = self.pools[size_class_idx].write().pop() {
            buffer.clear();
            debug!("Reused buffer from pool: size_class={}", size_class);
            return buffer;
        }

        // Allocate new buffer
        debug!("Allocated new buffer: size_class={}", size_class);
        BytesMut::with_capacity(size_class)
    }

    /// Return a buffer to the pool
    pub fn release(&self, mut buffer: BytesMut) {
        let capacity = buffer.capacity();

        // Find appropriate pool
        if let Some(size_class_idx) = self.size_classes.iter().position(|&size| capacity <= size) {
            let mut pool = self.pools[size_class_idx].write();
            if pool.len() < 32 {
                // Limit pool size
                buffer.clear();
                pool.push(buffer);
                debug!(
                    "Returned buffer to pool: size_class={}",
                    self.size_classes[size_class_idx]
                );
            }
        }
    }
}

/// Compression worker pool for offloading compression from I/O threads
pub struct CompressionPool {
    tx: mpsc::Sender<CompressionTask>,
}

struct CompressionTask {
    data: bytes::Bytes,
    compression_type: crate::protocol::record_batch::CompressionType,
    response_tx: tokio::sync::oneshot::Sender<Result<bytes::Bytes, String>>,
}

impl CompressionPool {
    pub fn new(num_threads: usize) -> Self {
        let (tx, rx) = mpsc::channel::<CompressionTask>(1000);
        let rx = Arc::new(tokio::sync::Mutex::new(rx));

        // Spawn worker threads
        for i in 0..num_threads {
            let task_rx = rx.clone();
            std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("Failed to create compression runtime");

                runtime.block_on(async move {
                    info!("Compression worker {} started", i);
                    loop {
                        let task = {
                            let mut rx_guard = task_rx.lock().await;
                            rx_guard.recv().await
                        };
                        match task {
                            Some(task) => {
                                let result = Self::compress(task.data, task.compression_type);
                                let _ = task.response_tx.send(result);
                            }
                            None => break,
                        }
                    }
                    info!("Compression worker {} stopped", i);
                });
            });
        }

        Self { tx }
    }

    /// Compress data asynchronously
    pub async fn compress_async(
        &self,
        data: bytes::Bytes,
        compression_type: crate::protocol::record_batch::CompressionType,
    ) -> Result<bytes::Bytes, String> {
        let (response_tx, response_rx) = tokio::sync::oneshot::channel();

        let task = CompressionTask {
            data,
            compression_type,
            response_tx,
        };

        self.tx
            .send(task)
            .await
            .map_err(|_| "Compression pool shutdown")?;

        response_rx
            .await
            .map_err(|_| "Compression task cancelled")?
    }

    fn compress(
        data: bytes::Bytes,
        compression_type: crate::protocol::record_batch::CompressionType,
    ) -> Result<bytes::Bytes, String> {
        use crate::protocol::record_batch::CompressionType;

        match compression_type {
            CompressionType::None => Ok(data),
            CompressionType::Gzip => {
                use flate2::Compression;
                use flate2::write::GzEncoder;
                use std::io::Write;

                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder
                    .write_all(&data)
                    .map_err(|e| format!("Gzip compression failed: {e}"))?;
                let compressed = encoder
                    .finish()
                    .map_err(|e| format!("Gzip compression failed: {e}"))?;
                Ok(bytes::Bytes::from(compressed))
            }
            CompressionType::Snappy => {
                let compressed = snap::raw::Encoder::new()
                    .compress_vec(&data)
                    .map_err(|e| format!("Snappy compression failed: {e}"))?;
                Ok(bytes::Bytes::from(compressed))
            }
            CompressionType::Lz4 => {
                let compressed = lz4::block::compress(&data, None, false)
                    .map_err(|e| format!("LZ4 compression failed: {e}"))?;
                Ok(bytes::Bytes::from(compressed))
            }
            CompressionType::Zstd => {
                let compressed = zstd::encode_all(data.as_ref(), 3)
                    .map_err(|e| format!("Zstd compression failed: {e}"))?;
                Ok(bytes::Bytes::from(compressed))
            }
        }
    }
}

/// Socket options optimizer
pub async fn optimize_socket(
    socket: &tokio::net::TcpStream,
    config: &PerformanceConfig,
) -> std::io::Result<()> {
    // Set TCP_NODELAY
    socket.set_nodelay(config.tcp_nodelay)?;

    // Set socket buffer sizes using socket2
    let sock_ref = socket2::SockRef::from(socket);

    if let Some(size) = config.socket_send_buffer_bytes {
        sock_ref.set_send_buffer_size(size)?;
        debug!("Set socket send buffer size to {}", size);
    }

    if let Some(size) = config.socket_recv_buffer_bytes {
        sock_ref.set_recv_buffer_size(size)?;
        debug!("Set socket receive buffer size to {}", size);
    }

    // Enable TCP keepalive
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(60))
        .with_interval(std::time::Duration::from_secs(10));
    sock_ref.set_tcp_keepalive(&keepalive)?;

    Ok(())
}

/// Metrics for monitoring buffer pool performance
pub struct BufferPoolMetrics {
    pub allocations: prometheus::IntCounter,
    pub reuses: prometheus::IntCounter,
    pub pool_size: prometheus::IntGaugeVec,
}

impl BufferPoolMetrics {
    pub fn new(registry: &prometheus::Registry) -> Self {
        let allocations = prometheus::register_int_counter_with_registry!(
            "heraclitus_buffer_pool_allocations_total",
            "Total number of buffer allocations",
            registry
        )
        .unwrap();

        let reuses = prometheus::register_int_counter_with_registry!(
            "heraclitus_buffer_pool_reuses_total",
            "Total number of buffer reuses from pool",
            registry
        )
        .unwrap();

        let pool_size = prometheus::register_int_gauge_vec_with_registry!(
            "heraclitus_buffer_pool_size",
            "Current size of buffer pools by size class",
            &["size_class"],
            registry
        )
        .unwrap();

        Self {
            allocations,
            reuses,
            pool_size,
        }
    }
}
