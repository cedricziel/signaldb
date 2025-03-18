mod parquet;
pub use parquet::write_batch_to_object_store;

use anyhow::Result;
use async_trait::async_trait;
use common::model::trace::Trace;
use std::path::Path;

#[async_trait]
pub trait Storage: Send + Sync {
    async fn store_trace(&self, trace: &Trace) -> Result<()>;
    async fn list_traces(&self) -> Result<Vec<String>>;
    async fn get_trace(&self, id: &str) -> Result<Trace>;
}

pub struct LocalStorage {
    path: std::path::PathBuf,
}

impl LocalStorage {
    pub fn new(path: &Path) -> Self {
        Self {
            path: path.to_path_buf(),
        }
    }
}

#[async_trait]
impl Storage for LocalStorage {
    async fn store_trace(&self, trace: &Trace) -> Result<()> {
        let trace_path = self.path.join(format!("{}.json", trace.spans[0].trace_id));
        tokio::fs::write(trace_path, serde_json::to_string(trace)?).await?;
        Ok(())
    }

    async fn list_traces(&self) -> Result<Vec<String>> {
        let mut traces = Vec::new();
        let mut entries = tokio::fs::read_dir(&self.path).await?;
        while let Some(entry) = entries.next_entry().await? {
            if let Some(file_name) = entry.file_name().to_str() {
                if file_name.ends_with(".json") {
                    traces.push(file_name[..file_name.len() - 5].to_string());
                }
            }
        }
        Ok(traces)
    }

    async fn get_trace(&self, id: &str) -> Result<Trace> {
        let trace_path = self.path.join(format!("{}.json", id));
        let content = tokio::fs::read_to_string(trace_path).await?;
        Ok(serde_json::from_str(&content)?)
    }
}
