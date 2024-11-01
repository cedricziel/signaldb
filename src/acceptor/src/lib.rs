pub mod services;

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema};
use parquet::{
    arrow::AsyncArrowWriter,
    file::properties::{WriterProperties, WriterVersion},
};

pub fn get_parquet_writer() -> AsyncArrowWriter<Vec<u8>> {
    let writer = Vec::new();
    let schema = Schema::new(vec![
        Field::new("c1", DataType::Int32, false),
        Field::new("c2", DataType::Utf8, false),
        Field::new("c3", DataType::Float64, false),
    ]);

    println!("get_parquet_writer");
    let props = WriterProperties::builder()
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .build();

    AsyncArrowWriter::try_new(writer, Arc::new(schema), Some(props))
        .expect("Error creating parquet writer")
}
