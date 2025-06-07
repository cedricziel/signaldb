use datafusion::arrow::record_batch::RecordBatch;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq)]
pub struct BatchWrapper {
    #[serde(skip)]
    pub batch: Option<RecordBatch>,
}

impl From<RecordBatch> for BatchWrapper {
    fn from(batch: RecordBatch) -> Self {
        Self { batch: Some(batch) }
    }
}
