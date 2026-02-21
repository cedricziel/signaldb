//! Iceberg table operations for retention management
//!
//! Provides snapshot management, manifest reading, and partition operations
//! for the compactor's retention enforcement features.

pub mod manifest;
pub mod partition;
pub mod snapshot;

pub use manifest::{ManifestFileInfo, ManifestReader};
pub use partition::{PartitionInfo, PartitionManager};
pub use snapshot::{SnapshotInfo, SnapshotManager};
