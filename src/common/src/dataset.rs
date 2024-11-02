use std::fmt::Display;

pub enum DataSetType {
    Metrics,
    Logs,
    Traces,
}

impl Display for DataSetType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataSetType::Metrics => write!(f, "metrics"),
            DataSetType::Logs => write!(f, "logs"),
            DataSetType::Traces => write!(f, "traces"),
        }
    }
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

    pub fn get_path() -> String {
        "src/common/src/dataset.rs".to_string()
    }
}
