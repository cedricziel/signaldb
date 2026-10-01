//! Parquet-with-warm-index file builders shared by [`super::prefilter`]'s
//! and [`super::table`]'s test modules, so the two don't duplicate the
//! Arrow/Parquet writer boilerplate for a bloom-filtered `attr_index`
//! column.

use std::sync::Arc;

use common::attrs::typed::HomeValue;
use common::attrs::warm_index::{WARM_INDEX_COLUMN, encode_token};
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Int64Array, Int64Builder, ListBuilder, MapBuilder, RecordBatch,
    StringBuilder,
};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::parquet::file::properties::WriterProperties;
use datafusion::parquet::schema::types::ColumnPath;

/// Writes `schema`/`batch` to an in-memory Parquet file, returning its bytes.
pub(crate) fn write_parquet(
    schema: Arc<Schema>,
    batch: RecordBatch,
    props: Option<WriterProperties>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buf, schema, props).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buf
}

/// Enables a bloom filter on [`WARM_INDEX_COLUMN`]'s list leaf, one row group
/// per row (matching what the writer configures for an opted-in table).
pub(crate) fn warm_index_writer_properties(ndv: u64) -> WriterProperties {
    let leaf = ColumnPath::new(vec![
        WARM_INDEX_COLUMN.to_string(),
        "list".to_string(),
        "item".to_string(),
    ]);
    WriterProperties::builder()
        .set_max_row_group_row_count(Some(1))
        .set_column_bloom_filter_enabled(leaf.clone(), true)
        .set_column_bloom_filter_max_ndv(leaf, ndv)
        .build()
}

fn warm_index_field() -> Field {
    Field::new(
        WARM_INDEX_COLUMN,
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        true,
    )
}

fn warm_index_array(rows: &[&[&[u8]]]) -> ArrayRef {
    let item_field = Arc::new(Field::new("item", DataType::Binary, true));
    let mut builder = ListBuilder::new(BinaryBuilder::new()).with_field(item_field);
    for row in rows {
        builder.append_value(row.iter().map(|token| Some(*token)));
    }
    Arc::new(builder.finish())
}

/// A one-column `attr_index: List<Binary>` batch with a bloom filter on the
/// list leaf — for [`super::prefilter`]'s tests, which probe the index
/// directly and don't need a real typed-home column alongside it.
pub(crate) fn write_warm_index_file(rows: &[&[&[u8]]], ndv: u64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![warm_index_field()]));
    let batch = RecordBatch::try_new(schema.clone(), vec![warm_index_array(rows)]).unwrap();
    write_parquet(schema, batch, Some(warm_index_writer_properties(ndv)))
}

/// A file with no `attr_index` column at all — always kept by the prefilter,
/// and never wrapped by `WarmIndexTable::maybe_wrap`.
pub(crate) fn write_file_without_warm_index() -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));
    let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
    write_parquet(schema, batch, None)
}

/// Writes `count` files (`f0.parquet`, `f1.parquet`, ...) to `store`, with
/// `bytes_for(i)` as each one's content, returning their `PartitionedFile`s.
pub(crate) async fn make_files(
    store: &Arc<dyn object_store::ObjectStore>,
    count: u32,
    mut bytes_for: impl FnMut(u32) -> Vec<u8>,
) -> Vec<datafusion::datasource::listing::PartitionedFile> {
    use datafusion::datasource::listing::PartitionedFile;
    use object_store::ObjectStoreExt;
    use object_store::path::Path;

    let mut files = Vec::new();
    for i in 0..count {
        let bytes = bytes_for(i);
        let path = Path::from(format!("f{i}.parquet"));
        store.put(&path, bytes.clone().into()).await.unwrap();
        files.push(PartitionedFile::new(path.to_string(), bytes.len() as u64));
    }
    files
}

/// One row's typed-home value, one canonical type per file — enough for
/// [`super::table`]'s end-to-end tests, which only ever compare a single
/// home column at a time.
pub(crate) enum HomeRow<'a> {
    Int(i64),
    Str(&'a str),
}

/// A one-row file carrying both a real typed-home value (a `Map<Utf8, ..>`
/// column named `home_column`, so `get_field(home_column, key)` evaluates
/// correctly) and the warm-index token that value implies — what the writer
/// would produce for an opted-in table. `id` distinguishes rows once
/// multiple files are scanned together.
pub(crate) fn write_home_file(id: i64, home_column: &str, key: &str, row: HomeRow<'_>) -> Vec<u8> {
    let id_field = Field::new("id", DataType::Int64, false);
    let id_array: ArrayRef = Arc::new(Int64Array::from(vec![id]));

    let (map_array, token): (ArrayRef, Vec<Vec<u8>>) = match row {
        HomeRow::Int(v) => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
            builder.keys().append_value(key);
            builder.values().append_value(v);
            builder.append(true).unwrap();
            (
                Arc::new(builder.finish()),
                encode_token(key, HomeValue::Int(v)).into_iter().collect(),
            )
        }
        HomeRow::Str(v) => {
            let mut builder = MapBuilder::new(None, StringBuilder::new(), StringBuilder::new());
            builder.keys().append_value(key);
            builder.values().append_value(v);
            builder.append(true).unwrap();
            (
                Arc::new(builder.finish()),
                encode_token(key, HomeValue::Str(v)).into_iter().collect(),
            )
        }
    };
    let map_field = Field::new(home_column, map_array.data_type().clone(), true);
    let token_refs: Vec<&[u8]> = token.iter().map(Vec::as_slice).collect();
    let attr_index = warm_index_array(&[token_refs.as_slice()]);

    let schema = Arc::new(Schema::new(vec![id_field, map_field, warm_index_field()]));
    let batch =
        RecordBatch::try_new(schema.clone(), vec![id_array, map_array, attr_index]).unwrap();
    write_parquet(schema, batch, Some(warm_index_writer_properties(100)))
}
