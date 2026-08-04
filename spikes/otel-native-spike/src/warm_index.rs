//! # Warm typed containment index (spike 0.1)
//!
//! Prototype + proof for the "warm tier" requirement of the otel-native-schema
//! change: a derived, *typed* generalization of today's untyped `attr_tokens`
//! column that prunes row groups / files for an unpromoted `key = value`
//! predicate — without stringifying typed literals and without relying on any
//! per-key statistics inside a Parquet MAP (Parquet keeps none).
//!
//! ## Today's mechanism (prior art, `src/common`, `src/writer`, `src/querier`)
//!
//! The writer derives `attr_tokens: List<Utf8>`, one `key=value` string token
//! per attribute, and enables a Parquet bloom filter on the list leaf
//! (`attr_tokens.list.item`). The querier ANDs
//! `array_has(attr_tokens, 'k=v')` onto equality predicates. That token is
//! string-only: an int attribute must be stringified, and `status_code=200`
//! (int) collides with a string attribute whose value is the text `"200"`.
//!
//! ## Typed generalization
//!
//! We keep the "fuse key+value into one bloom leaf value" idea but make it
//! **per canonical type** and **binary**, so the value bytes are the *typed*
//! canonical encoding, never a stringification:
//!
//! ```text
//! token = key_utf8 || 0x1F (unit separator) || canonical_value_bytes
//! ```
//!
//! - `attr_tok_str : List<Binary>` — value bytes = UTF-8 of the string value.
//! - `attr_tok_int : List<Binary>` — value bytes = `i64::to_be_bytes` (8 bytes).
//! - (`attr_tok_f64`, `attr_tok_bool` generalize identically; the spike proves
//!   str + int, the two the task requires.)
//!
//! One column per canonical type means the int token for
//! `http.response.status_code = 200` and any string token are in different
//! bloom filters entirely — no cross-type collision, and the int literal is
//! matched as 8 big-endian bytes, never as the text "200".
//!
//! ### Why this cannot cause false negatives
//!
//! A bloom answers "definitely absent" or "maybe present". The only way to get
//! a false *negative* (wrongly skip a file that does contain the pair) is if the
//! same `(key, typed value)` hashed to different bytes at write time vs query
//! time. The encoding is a pure function of `(key, value)`, so write and query
//! produce identical bytes — deterministic, no false negatives. Two *different*
//! `(key, value)` pairs colliding to the same bytes would only ever cause a
//! false *positive* (an extra file scanned), which the bloom already tolerates
//! and which the exact predicate over the typed map still filters out.

use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result};
use arrow::array::{
    ArrayRef, BinaryBuilder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames, RecordBatch,
    StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{ReaderProperties, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use parquet::schema::types::ColumnPath;

/// Unit separator between the key and the canonical value bytes in a token.
/// `0x1F` never appears in an attribute key (keys are dotted identifiers), so
/// the key/value split is unambiguous even though we do not length-prefix.
const SEP: u8 = 0x1F;

/// Canonical warm-index column names, per canonical type.
pub const TOK_STR_COLUMN: &str = "attr_tok_str";
pub const TOK_INT_COLUMN: &str = "attr_tok_int";

/// The typed-MAP homes (the negative control: bare maps do NOT prune).
pub const STR_MAP_COLUMN: &str = "attr_str_map";
pub const INT_MAP_COLUMN: &str = "attr_int_map";

/// Encode a `(key, string value)` containment token.
pub fn encode_str_token(key: &str, value: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1 + value.len());
    out.extend_from_slice(key.as_bytes());
    out.push(SEP);
    out.extend_from_slice(value.as_bytes());
    out
}

/// Encode a `(key, i64 value)` containment token. The value is the canonical
/// 8-byte big-endian encoding — the typed literal is NEVER stringified.
pub fn encode_int_token(key: &str, value: i64) -> Vec<u8> {
    let mut out = Vec::with_capacity(key.len() + 1 + 8);
    out.extend_from_slice(key.as_bytes());
    out.push(SEP);
    out.extend_from_slice(&value.to_be_bytes());
    out
}

/// One synthetic row: a handful of string attributes and int attributes.
/// In a real signal these come from resource/scope/record attribute scopes.
#[derive(Clone, Debug, Default)]
pub struct Row {
    pub id: i64,
    pub str_attrs: Vec<(String, String)>,
    pub int_attrs: Vec<(String, i64)>,
}

/// Arrow schema for a spike "file": an id, the two typed MAP homes (cold tier /
/// negative control), and the two per-type warm-index token lists.
pub fn spike_schema() -> Arc<Schema> {
    // List<Binary> with the element field explicitly named `item`, so the
    // Parquet leaf path is `<col>.list.item` — exactly the leaf the product
    // code (bloom_filter_property_for_attr_tokens) targets on the Iceberg path.
    let str_tok = Field::new(
        TOK_STR_COLUMN,
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        true,
    );
    let int_tok = Field::new(
        TOK_INT_COLUMN,
        DataType::List(Arc::new(Field::new("item", DataType::Binary, true))),
        true,
    );

    let str_map = Field::new(
        STR_MAP_COLUMN,
        map_type(DataType::Utf8),
        false, // maps are non-null; entries may be empty
    );
    let int_map = Field::new(INT_MAP_COLUMN, map_type(DataType::Int64), false);

    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        str_map,
        int_map,
        str_tok,
        int_tok,
    ]))
}

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    }
}

fn map_type(value: DataType) -> DataType {
    let names = map_field_names();
    DataType::Map(
        Arc::new(Field::new(
            names.entry,
            DataType::Struct(
                vec![
                    Field::new(names.key, DataType::Utf8, false),
                    Field::new(names.value, value, true),
                ]
                .into(),
            ),
            false,
        )),
        false,
    )
}

/// Build a `RecordBatch` for one file from its rows: fill both typed MAP homes
/// and both per-type warm-index token lists.
pub fn build_batch(rows: &[Row]) -> Result<RecordBatch> {
    let schema = spike_schema();

    let mut id = Int64Builder::new();

    let names = map_field_names();
    let mut str_map = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut int_map = MapBuilder::new(Some(names), StringBuilder::new(), Int64Builder::new());

    let mut str_tok = ListBuilder::new(BinaryBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Binary,
        true,
    )));
    let mut int_tok = ListBuilder::new(BinaryBuilder::new()).with_field(Arc::new(Field::new(
        "item",
        DataType::Binary,
        true,
    )));

    for row in rows {
        id.append_value(row.id);

        for (k, v) in &row.str_attrs {
            str_map.keys().append_value(k);
            str_map.values().append_value(v);
        }
        str_map.append(true)?;

        for (k, v) in &row.int_attrs {
            int_map.keys().append_value(k);
            int_map.values().append_value(*v);
        }
        int_map.append(true)?;

        // Warm-index tokens, deduplicated per row (mirrors the product code).
        let mut seen: HashSet<Vec<u8>> = HashSet::new();
        for (k, v) in &row.str_attrs {
            let tok = encode_str_token(k, v);
            if seen.insert(tok.clone()) {
                str_tok.values().append_value(&tok);
            }
        }
        str_tok.append(true);

        seen.clear();
        for (k, v) in &row.int_attrs {
            let tok = encode_int_token(k, *v);
            if seen.insert(tok.clone()) {
                int_tok.values().append_value(&tok);
            }
        }
        int_tok.append(true);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(id.finish()),
        Arc::new(str_map.finish()),
        Arc::new(int_map.finish()),
        Arc::new(str_tok.finish()),
        Arc::new(int_tok.finish()),
    ];

    // Derive the schema from the finished arrays' concrete datatypes so the
    // Map/List field naming + nullability always match exactly (the builders
    // are the source of truth for the physical shape). `spike_schema()` documents
    // the intended shape; this keeps `try_new` from failing on a nullability nit.
    let names = [
        "id",
        STR_MAP_COLUMN,
        INT_MAP_COLUMN,
        TOK_STR_COLUMN,
        TOK_INT_COLUMN,
    ];
    let nullable = [false, false, false, true, true];
    let fields: Vec<Field> = columns
        .iter()
        .zip(names)
        .zip(nullable)
        .map(|((arr, name), null)| Field::new(name, arr.data_type().clone(), null))
        .collect();
    let derived = Arc::new(Schema::new(fields));
    let _ = schema; // documented intent; derived schema is authoritative

    RecordBatch::try_new(derived, columns).context("build_batch")
}

/// Writer properties: small row groups + a Parquet bloom filter on BOTH
/// warm-index list leaves. Deliberately NO bloom on the MAP columns — the
/// negative control depends on the map having no prunable structure.
pub fn writer_properties(row_group_size: usize, bloom_fpp: f64) -> WriterProperties {
    let str_leaf = ColumnPath::new(vec![
        TOK_STR_COLUMN.to_string(),
        "list".to_string(),
        "item".to_string(),
    ]);
    let int_leaf = ColumnPath::new(vec![
        TOK_INT_COLUMN.to_string(),
        "list".to_string(),
        "item".to_string(),
    ]);

    WriterProperties::builder()
        .set_max_row_group_size(row_group_size)
        .set_column_bloom_filter_enabled(str_leaf.clone(), true)
        .set_column_bloom_filter_fpp(str_leaf, bloom_fpp)
        .set_column_bloom_filter_enabled(int_leaf.clone(), true)
        .set_column_bloom_filter_fpp(int_leaf, bloom_fpp)
        .build()
}

/// Write one file's rows to a Parquet file with warm-index blooms enabled.
pub fn write_file(path: &Path, rows: &[Row], row_group_size: usize, bloom_fpp: f64) -> Result<()> {
    let batch = build_batch(rows)?;
    let file = std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
    let props = writer_properties(row_group_size, bloom_fpp);
    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(())
}

/// Which warm-index leaf a probe targets.
#[derive(Clone, Copy, Debug)]
pub enum TokenColumn {
    Str,
    Int,
}

impl TokenColumn {
    fn column_name(self) -> &'static str {
        match self {
            TokenColumn::Str => TOK_STR_COLUMN,
            TokenColumn::Int => TOK_INT_COLUMN,
        }
    }
}

/// Per-file result of the manual bloom pre-filter (what a custom TableProvider /
/// row-group pruning hook in layer 4.3 would compute).
#[derive(Clone, Debug, Default)]
pub struct FilePruneResult {
    pub row_groups_total: usize,
    /// Row groups whose leaf bloom says "maybe present" (must be scanned).
    pub row_groups_kept: usize,
    /// True if any row group survives → the whole file must be opened.
    pub file_kept: bool,
    /// Total bytes of bloom filter data in this file (both warm-index leaves).
    pub bloom_bytes: u64,
    /// Total on-disk size of the file.
    pub file_bytes: u64,
}

/// The manual bloom pre-filter: read the file footer with bloom reading
/// enabled, and for each row group probe the target warm-index leaf bloom for
/// the encoded token. Returns which row groups survive and bloom-size overhead.
///
/// This is exactly the work a custom file/row-group pruning hook must do,
/// because DataFusion 54.1 will NOT do it for an `array_has(list, lit)`
/// predicate (see the findings doc / demo output).
pub fn prefilter_file(path: &Path, column: TokenColumn, token: &[u8]) -> Result<FilePruneResult> {
    let file = std::fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let file_bytes = file.metadata()?.len();

    // parquet 58.4: bloom reading moved onto ReaderProperties.
    let options = ReadOptionsBuilder::new()
        .with_reader_properties(
            ReaderProperties::builder()
                .set_read_bloom_filter(true)
                .build(),
        )
        .build();
    let reader = SerializedFileReader::new_with_options(file, options)?;
    let meta = reader.metadata();

    // Resolve the leaf column index for `<col>.list.item` once.
    let leaf_path = format!("{}.list.item", column.column_name());
    let schema_descr = meta.file_metadata().schema_descr();
    let leaf_idx = (0..schema_descr.num_columns())
        .find(|&i| schema_descr.column(i).path().string() == leaf_path)
        .with_context(|| format!("leaf column {leaf_path} not found"))?;

    let mut result = FilePruneResult {
        row_groups_total: meta.num_row_groups(),
        ..Default::default()
    };

    // Bloom-size overhead: sum the bloom filter chunk lengths for the leaf.
    for rg in 0..meta.num_row_groups() {
        let col_chunk = meta.row_group(rg).column(leaf_idx);
        if let Some(len) = col_chunk.bloom_filter_length() {
            result.bloom_bytes += len as u64;
        }
    }

    for rg in 0..meta.num_row_groups() {
        let rg_reader = reader.get_row_group(rg)?;
        let keep = match rg_reader.get_column_bloom_filter(leaf_idx) {
            // "maybe present" → must scan; "definitely absent" → prune.
            Some(sbbf) => sbbf.check(token),
            // No bloom on this row group → conservatively keep (cannot prune).
            None => true,
        };
        if keep {
            result.row_groups_kept += 1;
        }
    }
    result.file_kept = result.row_groups_kept > 0;
    result.file_bytes = file_bytes;
    Ok(result)
}

/// Ground truth: does the file actually contain the pair? Used only to assert
/// the bloom pre-filter never produces a false negative.
pub fn file_contains(
    rows: &[Row],
    column: TokenColumn,
    key: &str,
    str_val: &str,
    int_val: i64,
) -> bool {
    rows.iter().any(|r| match column {
        TokenColumn::Str => r.str_attrs.iter().any(|(k, v)| k == key && v == str_val),
        TokenColumn::Int => r.int_attrs.iter().any(|(k, v)| k == key && *v == int_val),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tokens_are_deterministic_and_typed() {
        // Same (key,value) → identical bytes (no false negatives).
        assert_eq!(
            encode_int_token("http.response.status_code", 200),
            encode_int_token("http.response.status_code", 200)
        );
        // Int token is NOT the stringification: it embeds 8 big-endian bytes.
        let int_tok = encode_int_token("k", 200);
        assert_eq!(&int_tok[int_tok.len() - 8..], &200i64.to_be_bytes());
        // The int-200 token and the string-"200" token differ, so an int
        // predicate cannot collide with a string attribute valued "200".
        assert_ne!(encode_int_token("k", 200), encode_str_token("k", "200"));
    }

    #[test]
    fn batch_roundtrips_through_parquet_with_blooms() -> Result<()> {
        let rows = vec![
            Row {
                id: 0,
                str_attrs: vec![("host".into(), "a.example.com".into())],
                int_attrs: vec![("status".into(), 200)],
            },
            Row {
                id: 1,
                str_attrs: vec![("host".into(), "b.example.com".into())],
                int_attrs: vec![("status".into(), 404)],
            },
        ];
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("f.parquet");
        write_file(&path, &rows, 1, 0.01)?;

        // Present int pair → kept; absent int pair → pruned.
        let present = prefilter_file(&path, TokenColumn::Int, &encode_int_token("status", 200))?;
        assert!(present.file_kept);
        assert!(present.bloom_bytes > 0);

        let absent = prefilter_file(&path, TokenColumn::Int, &encode_int_token("status", 999))?;
        assert!(!absent.file_kept, "999 is absent, file must prune");
        Ok(())
    }
}
