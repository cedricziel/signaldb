pub enum DataSetType {
    Metrics,
    Logs,
    Traces,
}

pub enum DataStore {
    InMemory,
    Disk,
    S3,
}

/// A dataset represents a collection of signals of a specific type.
pub struct DataSet {
    pub data_type: DataSetType,
    pub store: DataStore,
}

impl DataSet {
    pub fn new(data_type: DataSetType, store: DataStore) -> Self {
        Self { data_type, store }
    }
}
