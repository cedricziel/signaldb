use std::sync::Arc;

use anyhow::Result;
use arrow_array::RecordBatch;
use datafusion::parquet::{
    arrow::{async_writer::ParquetObjectWriter, AsyncArrowWriter},
    file::properties::{WriterProperties, WriterVersion},
};
use object_store::{path::Path, ObjectStore};

/// Write a RecordBatch to object storage in Parquet format
pub async fn write_batch_to_object_store(
    object_store: Arc<dyn ObjectStore>,
    path: &str,
    batch: RecordBatch,
) -> Result<()> {
    let path = Path::from(path);

    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    let schema = batch.schema();

    let object_store_writer = ParquetObjectWriter::new(object_store.clone(), path);

    let mut arrow_writer = AsyncArrowWriter::try_new(object_store_writer, schema, Some(props))
        .expect("Error creating parquet writer");

    arrow_writer.write(&batch).await?;
    arrow_writer.close().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch as ArrowRecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use object_store::local::LocalFileSystem;
    use std::{fs, path::PathBuf};

    #[tokio::test]
    async fn test_write_batch() -> Result<()> {
        // Setup test directory
        let test_dir = PathBuf::from("./test_data");
        if test_dir.exists() {
            fs::remove_dir_all(&test_dir)?;
        }
        fs::create_dir_all(&test_dir)?;

        // Create test batch
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let batch = ArrowRecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
        )?;

        // Write batch
        let object_store = Arc::new(LocalFileSystem::new_with_prefix("./test_data")?);
        write_batch_to_object_store(object_store, "test.parquet", batch).await?;

        // Verify file exists
        assert!(test_dir.join("test.parquet").exists());

        // Cleanup
        fs::remove_dir_all(&test_dir)?;

        Ok(())
    }
}
