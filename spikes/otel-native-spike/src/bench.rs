//! # Attribute-storage benchmark on real hive data (spike 0.3)
//!
//! Materializes the four attribute-storage layouts the `otel-native-schema`
//! change proposes, from a bounded, deterministic slice of **real** production
//! parquet (`.data/spike/hive`), at two file-size regimes, and measures
//! storage, write cost, query cost and pruning for each.
//!
//! | variant | layout                                                                   |
//! | ------- | ------------------------------------------------------------------------ |
//! | `V0`    | legacy: one `Map<Utf8,Utf8>` holding every attribute stringified          |
//! | `V1`    | typed per-type maps (`str`/`int`/`double`/`bool`) + `Binary` CBOR residue |
//! | `V2`    | `V1` + two promoted top-level typed columns (stats + bloom)               |
//! | `V3`    | `V1` + the warm typed containment index of spike 0.1 (list-leaf blooms)   |
//!
//! Everything is derived from the same in-memory [`SourceRow`] slice, so the
//! variants differ **only** in physical layout. Two synthetic perturbations are
//! injected (both documented in the findings doc, both necessary because the
//! real population has no natural specimen — see `spike/data-characterization.md`):
//!
//! 1. a **conflicted key** `bench.conflicted` — canonical `int`, 5% off-type
//!    values that are the *numeric-looking string* `"42"`, so the legacy
//!    string map cannot tell them apart from a real `42` while the typed map
//!    can (the off-type values land in the residue);
//! 2. **planted rare values** in two row clusters, so "files pruned %" has a
//!    known ground truth in both file-size regimes.
//!
//! The residue is a top-level `Binary` column holding one CBOR array of
//! `(key, type_tag, raw)` triples per row (null when a row has no residue) —
//! deliberately not a `Map<String,Binary>`, which nulls through the
//! datafusion-iceberg provider (see `coexistence.rs`). All reads here go
//! through **plain parquet + DataFusion**, never the iceberg provider, so the
//! measurement isolates layout cost from provider behaviour.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use arrow::array::{
    Array, ArrayRef, AsArray, BinaryBuilder, BooleanBuilder, Float64Builder, Int64Builder,
    ListBuilder, MapArray, MapBuilder, MapFieldNames, RecordBatch, StringBuilder,
};
use arrow::compute::cast;
use arrow::datatypes::{DataType, Field, Int64Type, Schema, TimeUnit};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

use crate::warm_index::{TOK_INT_COLUMN, TOK_STR_COLUMN, encode_int_token, encode_str_token};

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

/// Single seed for every random decision in the benchmark.
pub const SEED: u64 = 0x0742_454E_4348_3033;

/// Rows taken from `_system/_monitoring/traces` (span + resource attributes).
pub const TRACE_ROWS: usize = 150_000;
/// Rows taken from `_system/_monitoring/logs` (log + resource attributes).
pub const LOG_ROWS: usize = 50_000;

/// Synthesized conflicted key: canonical int, 5% off-type numeric-looking string.
pub const CONFLICTED_KEY: &str = "bench.conflicted";
/// The int value probed by query class 5 — also the *text* of the off-type values.
pub const CONFLICTED_PROBE: i64 = 42;
/// Fraction of rows whose `bench.conflicted` value is the off-type string "42".
pub const CONFLICTED_OFF_TYPE_FRACTION: f64 = 0.05;

/// Promoted keys for V2 (one string, one int — both real hive keys).
pub const PROMOTED_STR_KEY: &str = "target";
pub const PROMOTED_INT_KEY: &str = "thread.id";
pub const PROMOTED_STR_COLUMN: &str = "attr_target";
pub const PROMOTED_INT_COLUMN: &str = "attr_thread_id";

/// Unpromoted int key probed by query classes 2 and 4.
pub const RARE_INT_KEY: &str = "busy_ns";
/// Planted rare values (absent from the real data by construction).
pub const RARE_STR_VALUE: &str = "bench.rare.target";
pub const RARE_INT_VALUE: i64 = 424_242_424_242;
pub const RARE_THREAD_ID: i64 = 987_654_321;
/// Two clusters of planted rows (indices into the combined source slice), so
/// pruning ground truth is >1 file in both regimes.
pub const PLANT_CLUSTERS: [(usize, usize); 2] = [(40_000, 10), (120_000, 10)];

/// Bloom filter false-positive rate, matching spike 0.1.
pub const BLOOM_FPP: f64 = 0.01;

/// A file-size regime: how many rows go into each output parquet file.
#[derive(Clone, Copy, Debug)]
pub struct Regime {
    pub name: &'static str,
    pub rows_per_file: usize,
}

/// Mirrors the real hive distribution (38–132 rows/file, one row group each).
pub const SMALL_FLUSH: Regime = Regime {
    name: "small-flush",
    rows_per_file: 100,
};
/// What compaction would produce.
pub const COMPACTED: Regime = Regime {
    name: "compacted",
    rows_per_file: 50_000,
};

/// The four layouts under test.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Variant {
    /// Today's layout: one `Map<Utf8,Utf8>`.
    V0Legacy,
    /// Per-type maps + CBOR residue.
    V1Typed,
    /// V1 + promoted typed columns.
    V2Promoted,
    /// V1 + warm typed containment index.
    V3Warm,
}

impl Variant {
    pub const ALL: [Variant; 4] = [
        Variant::V0Legacy,
        Variant::V1Typed,
        Variant::V2Promoted,
        Variant::V3Warm,
    ];

    pub fn tag(self) -> &'static str {
        match self {
            Variant::V0Legacy => "V0",
            Variant::V1Typed => "V1",
            Variant::V2Promoted => "V2",
            Variant::V3Warm => "V3",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Variant::V0Legacy => "V0 legacy string map",
            Variant::V1Typed => "V1 typed maps + residue",
            Variant::V2Promoted => "V2 typed + promoted cols",
            Variant::V3Warm => "V3 typed + warm index",
        }
    }
}

// ---------------------------------------------------------------------------
// Source rows (real data)
// ---------------------------------------------------------------------------

/// One signal record reduced to what the benchmark varies: a timestamp, two
/// scalar columns kept identical across variants, and the merged attribute set
/// exactly as it exists today (stringified key/value pairs).
#[derive(Clone, Debug)]
pub struct SourceRow {
    pub ts_nanos: i64,
    pub service: String,
    /// `span_name` for traces, `body` for logs.
    pub name: String,
    pub attrs: Vec<(String, String)>,
    /// Keys whose *OTLP* value type is string even though the stringified text
    /// may look numeric. The real data is already stringified, so this is the
    /// side-channel that lets the synthesized conflicted key carry a genuine
    /// off-type value ("42"-the-string vs 42-the-int); it is empty for every
    /// row that comes straight from hive.
    pub forced_str: Vec<String>,
}

/// Which real table to read, and which record-level attribute map it uses.
pub struct SourceSpec {
    pub dir: PathBuf,
    pub record_attr_col: &'static str,
    pub name_col: &'static str,
    pub budget: usize,
}

/// Recursively collect parquet files, sorted — the read order is deterministic.
pub fn parquet_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(d) = stack.pop() {
        for entry in std::fs::read_dir(&d).with_context(|| format!("read_dir {}", d.display()))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.extension().is_some_and(|e| e == "parquet") {
                out.push(path);
            }
        }
    }
    out.sort();
    Ok(out)
}

fn utf8_column(batch: &RecordBatch, name: &str) -> Result<ArrayRef> {
    let col = batch
        .column_by_name(name)
        .ok_or_else(|| anyhow!("column {name} missing"))?;
    Ok(cast(col, &DataType::Utf8)?)
}

fn map_column<'a>(batch: &'a RecordBatch, name: &str) -> Option<&'a MapArray> {
    // A present-but-wrongly-typed column is a schema surprise worth failing
    // loudly on, not silently skipping (review follow-up on PR #960).
    match batch.column_by_name(name) {
        None => None,
        Some(c) => match c.as_any().downcast_ref::<MapArray>() {
            Some(m) => Some(m),
            None => panic!(
                "column {name} exists but is {:?}, not a MapArray — source schema mismatch",
                c.data_type()
            ),
        },
    }
}

fn push_map_entries(map: &MapArray, row: usize, into: &mut Vec<(String, String)>) -> Result<()> {
    if map.is_null(row) {
        return Ok(());
    }
    let entries = map.value(row);
    let keys = entries
        .column(0)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| anyhow!("map keys are not Utf8"))?;
    let vals = entries
        .column(1)
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .ok_or_else(|| anyhow!("map values are not Utf8"))?;
    for i in 0..entries.len() {
        if keys.is_null(i) {
            continue;
        }
        let k = keys.value(i);
        let v = if vals.is_null(i) { "" } else { vals.value(i) };
        match into.iter_mut().find(|(ek, _)| ek == k) {
            // Record-level attributes are pushed after resource-level ones and
            // win on collision, mirroring OTel scope precedence.
            Some(slot) => slot.1 = v.to_string(),
            None => into.push((k.to_string(), v.to_string())),
        }
    }
    Ok(())
}

/// Read a bounded, deterministic slice of a real hive table.
pub fn load_source(spec: &SourceSpec) -> Result<(Vec<SourceRow>, usize)> {
    let files = parquet_files(&spec.dir)?;
    let mut rows: Vec<SourceRow> = Vec::with_capacity(spec.budget);
    let mut files_read = 0usize;

    for path in &files {
        if rows.len() >= spec.budget {
            break;
        }
        files_read += 1;
        let file = File::open(path).with_context(|| format!("open {}", path.display()))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
            .with_batch_size(8192)
            .build()?;
        for batch in reader {
            let batch = batch?;
            let ts = cast(
                batch
                    .column_by_name("timestamp")
                    .ok_or_else(|| anyhow!("timestamp column missing"))?,
                &DataType::Timestamp(TimeUnit::Nanosecond, None),
            )?;
            let ts = cast(&ts, &DataType::Int64)?;
            let ts = ts.as_primitive::<Int64Type>();
            let svc = utf8_column(&batch, "service_name")?;
            let svc = svc.as_string::<i32>();
            let name = utf8_column(&batch, spec.name_col)?;
            let name = name.as_string::<i32>();
            let res_attrs = map_column(&batch, "resource_attributes");
            let rec_attrs = map_column(&batch, spec.record_attr_col);

            for r in 0..batch.num_rows() {
                if rows.len() >= spec.budget {
                    break;
                }
                let mut attrs = Vec::new();
                if let Some(m) = res_attrs {
                    push_map_entries(m, r, &mut attrs)?;
                }
                if let Some(m) = rec_attrs {
                    push_map_entries(m, r, &mut attrs)?;
                }
                rows.push(SourceRow {
                    ts_nanos: if ts.is_null(r) { 0 } else { ts.value(r) },
                    service: if svc.is_null(r) {
                        String::new()
                    } else {
                        svc.value(r).to_string()
                    },
                    name: if name.is_null(r) {
                        String::new()
                    } else {
                        name.value(r).to_string()
                    },
                    attrs,
                    forced_str: Vec::new(),
                });
            }
        }
    }
    Ok((rows, files_read))
}

// ---------------------------------------------------------------------------
// Typing (what the attribute registry would decide)
// ---------------------------------------------------------------------------

/// Canonical storage class for an attribute key.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Canon {
    Str,
    Int,
    Double,
    Bool,
    /// Complex/JSON-shaped values — no typed home, always residue.
    Residue,
}

impl Canon {
    pub fn tag(self) -> &'static str {
        match self {
            Canon::Str => "str",
            Canon::Int => "int",
            Canon::Double => "double",
            Canon::Bool => "bool",
            Canon::Residue => "residue",
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct KeyStat {
    pub n: usize,
    pub n_str: usize,
    pub n_int: usize,
    pub n_double: usize,
    pub n_bool: usize,
    pub n_json: usize,
    pub canon: Option<Canon>,
}

impl KeyStat {
    /// Fraction of values that do NOT match the canonical class (→ residue).
    pub fn off_type_fraction(&self) -> f64 {
        let winner = match self.canon {
            Some(Canon::Str) => self.n_str,
            Some(Canon::Int) => self.n_int,
            Some(Canon::Double) => self.n_double,
            Some(Canon::Bool) => self.n_bool,
            _ => self.n_json,
        };
        if self.n == 0 {
            0.0
        } else {
            1.0 - winner as f64 / self.n as f64
        }
    }
}

fn classify(value: &str) -> Canon {
    if value.eq_ignore_ascii_case("true") || value.eq_ignore_ascii_case("false") {
        return Canon::Bool;
    }
    if value.parse::<i64>().is_ok() {
        return Canon::Int;
    }
    if value.parse::<f64>().is_ok() && value.chars().any(|c| c == '.' || c == 'e' || c == 'E') {
        return Canon::Double;
    }
    let t = value.trim_start();
    if t.starts_with('{') || t.starts_with('[') {
        return Canon::Residue;
    }
    Canon::Str
}

/// Infer one canonical class per key by majority vote, exactly as a registry
/// would: the winning class gets a typed home, everything else is residue.
pub fn infer_types(rows: &[SourceRow]) -> BTreeMap<String, KeyStat> {
    let mut stats: BTreeMap<String, KeyStat> = BTreeMap::new();
    for row in rows {
        for (k, v) in &row.attrs {
            let e = stats.entry(k.clone()).or_default();
            e.n += 1;
            let class = if row.forced_str.iter().any(|f| f == k) {
                Canon::Str
            } else {
                classify(v)
            };
            match class {
                Canon::Str => e.n_str += 1,
                Canon::Int => e.n_int += 1,
                Canon::Double => e.n_double += 1,
                Canon::Bool => e.n_bool += 1,
                Canon::Residue => e.n_json += 1,
            }
        }
    }
    for stat in stats.values_mut() {
        // Deterministic argmax with a fixed tie-break order.
        let candidates = [
            (stat.n_str, Canon::Str),
            (stat.n_int, Canon::Int),
            (stat.n_double, Canon::Double),
            (stat.n_bool, Canon::Bool),
            (stat.n_json, Canon::Residue),
        ];
        let mut best = candidates[0];
        for c in candidates.iter().skip(1) {
            if c.0 > best.0 {
                best = *c;
            }
        }
        stat.canon = Some(best.1);
    }
    stats
}

/// The resolved layout: key → canonical class (what the registry would serve).
#[derive(Clone, Debug)]
pub struct Layout {
    pub types: BTreeMap<String, Canon>,
    pub stats: BTreeMap<String, KeyStat>,
}

impl Layout {
    pub fn from_rows(rows: &[SourceRow]) -> Self {
        let stats = infer_types(rows);
        let types = stats
            .iter()
            .map(|(k, s)| (k.clone(), s.canon.unwrap_or(Canon::Str)))
            .collect();
        Layout { types, stats }
    }

    pub fn canon(&self, key: &str) -> Canon {
        self.types.get(key).copied().unwrap_or(Canon::Residue)
    }

    pub fn count(&self, canon: Canon) -> usize {
        self.types.values().filter(|c| **c == canon).count()
    }
}

// ---------------------------------------------------------------------------
// Synthetic perturbations
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct PlantReport {
    pub planted_rows: usize,
    pub conflicted_int_rows: usize,
    pub conflicted_off_type_rows: usize,
    /// Rows whose `bench.conflicted` int value equals [`CONFLICTED_PROBE`].
    pub conflicted_probe_matches: usize,
}

fn set_attr(row: &mut SourceRow, key: &str, value: String) {
    match row.attrs.iter_mut().find(|(k, _)| k == key) {
        Some(slot) => slot.1 = value,
        None => row.attrs.push((key.to_string(), value)),
    }
}

/// Inject the two synthetic perturbations into the source rows, in place.
pub fn plant(rows: &mut [SourceRow]) -> PlantReport {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut report = PlantReport::default();

    // 1. conflicted key: 95% int, 5% the off-type numeric-looking string "42".
    for row in rows.iter_mut() {
        if rng.random::<f64>() < CONFLICTED_OFF_TYPE_FRACTION {
            set_attr(row, CONFLICTED_KEY, format!("{CONFLICTED_PROBE}"));
            row.forced_str.push(CONFLICTED_KEY.to_string());
            report.conflicted_off_type_rows += 1;
        } else {
            let v: i64 = rng.random_range(1..=100);
            if v == CONFLICTED_PROBE {
                report.conflicted_probe_matches += 1;
            }
            set_attr(row, CONFLICTED_KEY, v.to_string());
            report.conflicted_int_rows += 1;
        }
    }

    // 2. planted rare values in two clusters (known pruning ground truth).
    for (start, len) in PLANT_CLUSTERS {
        for idx in start..start + len {
            let Some(row) = rows.get_mut(idx) else {
                continue;
            };
            set_attr(row, PROMOTED_STR_KEY, RARE_STR_VALUE.to_string());
            set_attr(row, PROMOTED_INT_KEY, RARE_THREAD_ID.to_string());
            set_attr(row, RARE_INT_KEY, RARE_INT_VALUE.to_string());
            report.planted_rows += 1;
        }
    }
    report
}

/// The file indices (per regime) that actually carry the planted rows — the
/// ground truth a pruning strategy must not miss.
pub fn planted_files(regime: Regime) -> Vec<usize> {
    let mut set: Vec<usize> = PLANT_CLUSTERS
        .iter()
        .flat_map(|(start, len)| (*start..*start + *len).map(|i| i / regime.rows_per_file))
        .collect();
    set.sort_unstable();
    set.dedup();
    set
}

// ---------------------------------------------------------------------------
// Residue encoding
// ---------------------------------------------------------------------------

/// CBOR-encode a row's residue as an array of `(key, type_tag, raw)` triples.
pub fn encode_residue(entries: &[(&str, &str, &str)]) -> Result<Vec<u8>> {
    let mut buf = Vec::with_capacity(64);
    ciborium::into_writer(entries, &mut buf).context("cbor encode residue")?;
    Ok(buf)
}

/// Decode what [`encode_residue`] wrote.
pub fn decode_residue(bytes: &[u8]) -> Result<Vec<(String, String, String)>> {
    ciborium::from_reader(bytes).context("cbor decode residue")
}

// ---------------------------------------------------------------------------
// Layout materialization
// ---------------------------------------------------------------------------

fn map_field_names() -> MapFieldNames {
    MapFieldNames {
        entry: "entries".to_string(),
        key: "keys".to_string(),
        value: "values".to_string(),
    }
}

fn list_item_field() -> Arc<Field> {
    Arc::new(Field::new("item", DataType::Binary, true))
}

/// Build the `RecordBatch` for one output file in the given layout.
pub fn build_batch(
    variant: Variant,
    rows: &[SourceRow],
    first_id: i64,
    layout: &Layout,
) -> Result<RecordBatch> {
    let names = map_field_names();

    let mut id = Int64Builder::new();
    let mut ts = Int64Builder::new();
    let mut service = StringBuilder::new();
    let mut name = StringBuilder::new();

    // V0
    let mut legacy = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    // V1+
    let mut str_map = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        StringBuilder::new(),
    );
    let mut int_map = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        Int64Builder::new(),
    );
    let mut double_map = MapBuilder::new(
        Some(names.clone()),
        StringBuilder::new(),
        Float64Builder::new(),
    );
    let mut bool_map = MapBuilder::new(Some(names), StringBuilder::new(), BooleanBuilder::new());
    let mut residue = BinaryBuilder::new();
    // V2
    let mut prom_str = StringBuilder::new();
    let mut prom_int = Int64Builder::new();
    // V3
    let mut tok_str = ListBuilder::new(BinaryBuilder::new()).with_field(list_item_field());
    let mut tok_int = ListBuilder::new(BinaryBuilder::new()).with_field(list_item_field());

    for (i, row) in rows.iter().enumerate() {
        id.append_value(first_id + i as i64);
        ts.append_value(row.ts_nanos);
        service.append_value(&row.service);
        name.append_value(&row.name);

        if variant == Variant::V0Legacy {
            for (k, v) in &row.attrs {
                legacy.keys().append_value(k);
                legacy.values().append_value(v);
            }
            legacy.append(true)?;
            continue;
        }

        let mut residue_entries: Vec<(&str, &str, &str)> = Vec::new();
        let mut prom_str_val: Option<&str> = None;
        let mut prom_int_val: Option<i64> = None;
        let mut seen_str: HashSet<Vec<u8>> = HashSet::new();
        let mut seen_int: HashSet<Vec<u8>> = HashSet::new();
        let promote = variant == Variant::V2Promoted;

        for (k, v) in &row.attrs {
            // An OTLP-string value under an int-canonical key is off-type and
            // goes to the residue, however numeric its text looks.
            let canon = if row.forced_str.iter().any(|f| f == k) {
                Canon::Str
            } else {
                layout.canon(k)
            };
            let canon = match (canon, layout.canon(k)) {
                (Canon::Str, Canon::Str) => Canon::Str,
                (Canon::Str, _) => Canon::Residue,
                (c, _) => c,
            };
            match canon {
                Canon::Int => match v.parse::<i64>() {
                    Ok(n) => {
                        if promote && k == PROMOTED_INT_KEY {
                            prom_int_val = Some(n);
                        } else {
                            int_map.keys().append_value(k);
                            int_map.values().append_value(n);
                        }
                        if variant == Variant::V3Warm {
                            let tok = encode_int_token(k, n);
                            if seen_int.insert(tok.clone()) {
                                tok_int.values().append_value(&tok);
                            }
                        }
                    }
                    // Off-type value for an int key → residue (the design's rule).
                    Err(_) => residue_entries.push((k.as_str(), "str", v.as_str())),
                },
                Canon::Double => match v.parse::<f64>() {
                    Ok(n) => {
                        double_map.keys().append_value(k);
                        double_map.values().append_value(n);
                    }
                    Err(_) => residue_entries.push((k.as_str(), "str", v.as_str())),
                },
                Canon::Bool => match v.to_ascii_lowercase().parse::<bool>() {
                    Ok(b) => {
                        bool_map.keys().append_value(k);
                        bool_map.values().append_value(b);
                    }
                    Err(_) => residue_entries.push((k.as_str(), "str", v.as_str())),
                },
                Canon::Str => {
                    if promote && k == PROMOTED_STR_KEY {
                        prom_str_val = Some(v.as_str());
                    } else {
                        str_map.keys().append_value(k);
                        str_map.values().append_value(v);
                    }
                    if variant == Variant::V3Warm {
                        let tok = encode_str_token(k, v);
                        if seen_str.insert(tok.clone()) {
                            tok_str.values().append_value(&tok);
                        }
                    }
                }
                Canon::Residue => residue_entries.push((k.as_str(), "json", v.as_str())),
            }
        }

        str_map.append(true)?;
        int_map.append(true)?;
        double_map.append(true)?;
        bool_map.append(true)?;
        if residue_entries.is_empty() {
            residue.append_null();
        } else {
            residue.append_value(encode_residue(&residue_entries)?);
        }
        if promote {
            match prom_str_val {
                Some(v) => prom_str.append_value(v),
                None => prom_str.append_null(),
            }
            prom_int.append_option(prom_int_val);
        }
        if variant == Variant::V3Warm {
            tok_str.append(true);
            tok_int.append(true);
        }
    }

    let mut columns: Vec<(&str, ArrayRef, bool)> = vec![
        ("id", Arc::new(id.finish()) as ArrayRef, false),
        ("ts_nanos", Arc::new(ts.finish()) as ArrayRef, false),
        ("service_name", Arc::new(service.finish()) as ArrayRef, true),
        ("name", Arc::new(name.finish()) as ArrayRef, true),
    ];
    if variant == Variant::V0Legacy {
        columns.push(("attributes", Arc::new(legacy.finish()), false));
    } else {
        columns.push(("attributes_str", Arc::new(str_map.finish()), false));
        columns.push(("attributes_int", Arc::new(int_map.finish()), false));
        columns.push(("attributes_double", Arc::new(double_map.finish()), false));
        columns.push(("attributes_bool", Arc::new(bool_map.finish()), false));
        columns.push(("attributes_residue", Arc::new(residue.finish()), true));
    }
    if variant == Variant::V2Promoted {
        columns.push((PROMOTED_STR_COLUMN, Arc::new(prom_str.finish()), true));
        columns.push((PROMOTED_INT_COLUMN, Arc::new(prom_int.finish()), true));
    }
    if variant == Variant::V3Warm {
        columns.push((TOK_STR_COLUMN, Arc::new(tok_str.finish()), true));
        columns.push((TOK_INT_COLUMN, Arc::new(tok_int.finish()), true));
    }

    // Derive the schema from the finished arrays so Map/List field naming and
    // nullability always match the builders (same approach as warm_index).
    let fields: Vec<Field> = columns
        .iter()
        .map(|(n, arr, null)| Field::new(*n, arr.data_type().clone(), *null))
        .collect();
    let arrays: Vec<ArrayRef> = columns.into_iter().map(|(_, a, _)| a).collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).context("build_batch")
}

/// Writer properties per variant: ZSTD + dictionary (matching hive), one row
/// group per file (matching hive), blooms only where the variant defines them.
///
/// `bloom_ndv` sets the expected distinct-value count used to size the bloom
/// filters. When it is `None`, parquet-rs resolves the NDV to the row group's
/// **row** count — which under-sizes a containment index by the
/// attributes-per-row factor, because a warm-index row contributes one token
/// per attribute, not one token per row.
pub fn writer_props(
    variant: Variant,
    rows_per_file: usize,
    bloom_ndv: Option<u64>,
) -> Result<WriterProperties> {
    let mut b = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
        .set_max_row_group_row_count(Some(rows_per_file.max(1)));
    match variant {
        Variant::V2Promoted => {
            for c in [PROMOTED_STR_COLUMN, PROMOTED_INT_COLUMN] {
                let path = ColumnPath::new(vec![c.to_string()]);
                b = b
                    .set_column_bloom_filter_enabled(path.clone(), true)
                    .set_column_bloom_filter_fpp(path.clone(), BLOOM_FPP);
                if let Some(ndv) = bloom_ndv {
                    b = b.set_column_bloom_filter_ndv(path, ndv);
                }
            }
        }
        Variant::V3Warm => {
            for c in [TOK_STR_COLUMN, TOK_INT_COLUMN] {
                let path =
                    ColumnPath::new(vec![c.to_string(), "list".to_string(), "item".to_string()]);
                b = b
                    .set_column_bloom_filter_enabled(path.clone(), true)
                    .set_column_bloom_filter_fpp(path.clone(), BLOOM_FPP);
                if let Some(ndv) = bloom_ndv {
                    b = b.set_column_bloom_filter_ndv(path, ndv);
                }
            }
        }
        _ => {}
    }
    Ok(b.build())
}

/// Storage + write-cost result for one (variant × regime).
#[derive(Clone, Debug, Default)]
pub struct WriteStats {
    pub files: usize,
    pub total_bytes: u64,
    pub footer_bytes: u64,
    pub bloom_bytes: u64,
    pub build_write_ms: f64,
    pub rows: usize,
    pub paths: Vec<PathBuf>,
}

impl WriteStats {
    pub fn footer_pct(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            100.0 * self.footer_bytes as f64 / self.total_bytes as f64
        }
    }
    pub fn bloom_pct(&self) -> f64 {
        if self.total_bytes == 0 {
            0.0
        } else {
            100.0 * self.bloom_bytes as f64 / self.total_bytes as f64
        }
    }
    pub fn bytes_per_row(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.rows as f64
        }
    }
    pub fn ns_per_row(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.build_write_ms * 1e6 / self.rows as f64
        }
    }
    pub fn path_strings(&self) -> Vec<String> {
        self.paths
            .iter()
            .map(|p| p.to_string_lossy().to_string())
            .collect()
    }
}

/// Parquet footer size: the thrift metadata plus the trailing `[len][PAR1]`.
pub fn footer_bytes(path: &Path) -> Result<u64> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::End(-8))?;
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    if &buf[4..] != b"PAR1" {
        bail!("{} is not a parquet file", path.display());
    }
    Ok(u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as u64 + 8)
}

/// Sum the bloom-filter chunk lengths across every column of every row group.
pub fn bloom_bytes(path: &Path) -> Result<u64> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path)?)?;
    let meta = reader.metadata().clone();
    let mut total = 0u64;
    for rg in 0..meta.num_row_groups() {
        for col in meta.row_group(rg).columns() {
            if let Some(len) = col.bloom_filter_length() {
                total += len as u64;
            }
        }
    }
    Ok(total)
}

/// Materialize one (variant × regime) as parquet under `out_dir`.
///
/// `dir_suffix` distinguishes alternative configurations of the same variant
/// (used by the bloom-sizing experiment); `bloom_ndv` is passed to
/// [`writer_props`].
pub fn materialize(
    variant: Variant,
    regime: Regime,
    rows: &[SourceRow],
    layout: &Layout,
    out_dir: &Path,
    bloom_ndv: Option<u64>,
    dir_suffix: &str,
) -> Result<WriteStats> {
    let dir = out_dir.join(regime.name).join(format!(
        "{}{dir_suffix}",
        variant.tag().to_ascii_lowercase()
    ));
    if dir.exists() {
        std::fs::remove_dir_all(&dir)?;
    }
    std::fs::create_dir_all(&dir)?;

    let props = writer_props(variant, regime.rows_per_file, bloom_ndv)?;
    let mut stats = WriteStats {
        rows: rows.len(),
        ..Default::default()
    };

    let start = Instant::now();
    for (fi, chunk) in rows.chunks(regime.rows_per_file).enumerate() {
        let batch = build_batch(variant, chunk, (fi * regime.rows_per_file) as i64, layout)?;
        let path = dir.join(format!("part-{fi:05}.parquet"));
        let file = File::create(&path)?;
        let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props.clone()))?;
        writer.write(&batch)?;
        writer.close()?;
        stats.paths.push(path);
    }
    stats.build_write_ms = start.elapsed().as_secs_f64() * 1e3;

    for path in &stats.paths {
        stats.files += 1;
        stats.total_bytes += std::fs::metadata(path)?.len();
        stats.footer_bytes += footer_bytes(path)?;
        stats.bloom_bytes += bloom_bytes(path)?;
    }
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Query harness (plain parquet + DataFusion)
// ---------------------------------------------------------------------------

/// Scan-level counters pulled out of the executed physical plan.
///
/// **DataFusion 54.1 caveat:** the pruning counters are
/// `MetricValue::PruningMetrics` (displayed as `N total -> M matched`), and
/// `MetricsSet::sum_by_name` deliberately ignores that variant — `as_usize()`
/// returns 0 for it. Reading these with `sum_by_name` (as spike 0.1's demo
/// does) therefore reports 0 pruning even when pruning happened. They must be
/// matched structurally, which is what [`walk_metrics`] does.
#[derive(Default, Debug, Clone, Copy)]
pub struct PruneMetrics {
    pub row_groups_pruned_statistics: usize,
    pub row_groups_pruned_bloom_filter: usize,
    pub files_ranges_pruned_statistics: usize,
    pub pushdown_rows_pruned: usize,
    pub files_opened: usize,
    pub bytes_scanned: usize,
    pub output_rows: usize,
}

fn walk_metrics(plan: &Arc<dyn ExecutionPlan>, acc: &mut PruneMetrics) {
    if let Some(set) = plan.metrics() {
        let m: MetricsSet = set.aggregate_by_name();
        let get = |name: &str| m.sum_by_name(name).map(|v| v.as_usize()).unwrap_or(0);
        acc.pushdown_rows_pruned += get("pushdown_rows_pruned");
        acc.files_opened += get("files_opened");
        acc.bytes_scanned += get("bytes_scanned");
        for metric in m.iter() {
            if let MetricValue::PruningMetrics {
                name,
                pruning_metrics,
            } = metric.value()
            {
                let pruned = pruning_metrics.pruned();
                match name.as_ref() {
                    "row_groups_pruned_statistics" => acc.row_groups_pruned_statistics += pruned,
                    "row_groups_pruned_bloom_filter" => {
                        acc.row_groups_pruned_bloom_filter += pruned
                    }
                    "files_ranges_pruned_statistics" => {
                        acc.files_ranges_pruned_statistics += pruned
                    }
                    _ => {}
                }
            }
        }
    }
    for child in plan.children() {
        walk_metrics(child, acc);
    }
}

/// A session configured the way the querier reads these files.
pub fn session() -> SessionContext {
    let mut cfg = SessionConfig::new();
    {
        let opts = cfg.options_mut();
        opts.execution.parquet.bloom_filter_on_read = true;
        opts.execution.parquet.pushdown_filters = true;
        opts.execution.parquet.reorder_filters = true;
    }
    SessionContext::new_with_config(cfg)
}

/// Outcome of one timed query.
#[derive(Clone, Debug, Default)]
pub struct ScanResult {
    pub median_ms: f64,
    /// Rows returned (or the `count(*)` value for counting queries).
    pub rows: i64,
    pub metrics: PruneMetrics,
    pub files_offered: usize,
    /// Milliseconds spent in the warm-index pre-filter (V3 only).
    pub prefilter_ms: f64,
    /// Files handed to DataFusion after the pre-filter (== offered when none).
    pub files_scanned: usize,
    pub files_total: usize,
}

impl ScanResult {
    /// Percentage of the variant's files that never reached DataFusion.
    pub fn files_pruned_pct(&self) -> f64 {
        if self.files_total == 0 {
            0.0
        } else {
            100.0 * (self.files_total - self.files_scanned) as f64 / self.files_total as f64
        }
    }
    /// Wall time including the pre-filter probe.
    pub fn total_ms(&self) -> f64 {
        self.median_ms + self.prefilter_ms
    }
}

async fn run_once(
    ctx: &SessionContext,
    paths: &[String],
    schema: &Schema,
    sql: &str,
) -> Result<(f64, i64, PruneMetrics)> {
    let start = Instant::now();
    let _ = ctx.deregister_table("t");
    if paths.is_empty() {
        return Ok((
            start.elapsed().as_secs_f64() * 1e3,
            0,
            PruneMetrics::default(),
        ));
    }
    // The schema is passed explicitly: inferring it would read every file's
    // footer before the query even starts, which would dominate (and distort)
    // the small-flush regime's timings.
    let df = ctx
        .read_parquet(paths.to_vec(), ParquetReadOptions::default().schema(schema))
        .await?;
    ctx.register_table("t", df.into_view())?;
    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    let ms = start.elapsed().as_secs_f64() * 1e3;

    let batch_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
    // If the query is a single-row count, surface the counted value.
    let counted = if batches.len() == 1
        && batches[0].num_rows() == 1
        && batches[0].num_columns() == 1
        && matches!(batches[0].column(0).data_type(), DataType::Int64)
    {
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    } else {
        batch_rows as i64
    };
    let mut m = PruneMetrics::default();
    walk_metrics(&plan, &mut m);
    m.output_rows = batch_rows;
    Ok((ms, counted, m))
}

fn median(mut v: Vec<f64>) -> f64 {
    if v.is_empty() {
        return f64::NAN;
    }
    v.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    v[v.len() / 2]
}

/// Run `sql` against `paths` `reps` times; median wall time, last run's counts.
pub async fn timed_query(
    ctx: &SessionContext,
    paths: &[String],
    schema: &Schema,
    sql: &str,
    reps: usize,
    files_total: usize,
) -> Result<ScanResult> {
    let mut times = Vec::with_capacity(reps);
    let mut last = (0.0, 0i64, PruneMetrics::default());
    for _ in 0..reps {
        last = run_once(ctx, paths, schema, sql).await?;
        times.push(last.0);
    }
    Ok(ScanResult {
        median_ms: median(times),
        rows: last.1,
        metrics: last.2,
        files_offered: paths.len(),
        prefilter_ms: 0.0,
        files_scanned: paths.len(),
        files_total,
    })
}

/// Warm-index pre-filter over a file set (the layer-4.3 hook of spike 0.1):
/// the surviving paths plus the probe cost (median of `reps` passes).
pub fn prefilter(
    paths: &[PathBuf],
    column: crate::warm_index::TokenColumn,
    token: &[u8],
    reps: usize,
) -> Result<(Vec<String>, f64)> {
    let mut times = Vec::with_capacity(reps);
    let mut kept = Vec::new();
    for _ in 0..reps.max(1) {
        let start = Instant::now();
        kept = Vec::new();
        for p in paths {
            if crate::warm_index::prefilter_file(p, column, token)?.file_kept {
                kept.push(p.to_string_lossy().to_string());
            }
        }
        times.push(start.elapsed().as_secs_f64() * 1e3);
    }
    Ok((kept, median(times)))
}

// ---------------------------------------------------------------------------
// Residue decode cost
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct ResidueStats {
    pub fetch_ms: f64,
    pub decode_ms: f64,
    pub rows: usize,
    pub entries: usize,
    pub bytes: usize,
}

impl ResidueStats {
    pub fn ns_per_row(&self) -> f64 {
        if self.rows == 0 {
            0.0
        } else {
            self.decode_ms * 1e6 / self.rows as f64
        }
    }
    pub fn ns_per_entry(&self) -> f64 {
        if self.entries == 0 {
            0.0
        } else {
            self.decode_ms * 1e6 / self.entries as f64
        }
    }
}

/// Fetch every non-null residue payload and CBOR-decode it, timing the two
/// phases separately (IO+scan vs parse).
pub async fn residue_cost(
    ctx: &SessionContext,
    paths: &[String],
    schema: &Schema,
) -> Result<ResidueStats> {
    let mut stats = ResidueStats::default();
    let start = Instant::now();
    let _ = ctx.deregister_table("t");
    let df = ctx
        .read_parquet(paths.to_vec(), ParquetReadOptions::default().schema(schema))
        .await?;
    ctx.register_table("t", df.into_view())?;
    let batches = ctx
        .sql("SELECT attributes_residue FROM t WHERE attributes_residue IS NOT NULL")
        .await?
        .collect()
        .await?;
    stats.fetch_ms = start.elapsed().as_secs_f64() * 1e3;

    let decode_start = Instant::now();
    for batch in &batches {
        let col = batch.column(0).as_binary::<i32>();
        for i in 0..col.len() {
            if col.is_null(i) {
                continue;
            }
            let bytes = col.value(i);
            let decoded = decode_residue(bytes)?;
            stats.rows += 1;
            stats.entries += decoded.len();
            stats.bytes += bytes.len();
            std::hint::black_box(&decoded);
        }
    }
    stats.decode_ms = decode_start.elapsed().as_secs_f64() * 1e3;
    Ok(stats)
}

// ---------------------------------------------------------------------------
// Registry-lookup microbenchmark (write path, no DataFusion)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct RegistryStats {
    pub shape: String,
    pub batch_rows: usize,
    pub lookups: usize,
    pub ns_per_lookup: f64,
    pub lookups_per_sec: f64,
}

fn stat(shape: &str, batch_rows: usize, lookups: usize, start: Instant) -> RegistryStats {
    let secs = start.elapsed().as_secs_f64();
    RegistryStats {
        shape: shape.to_string(),
        batch_rows,
        lookups,
        ns_per_lookup: secs * 1e9 / lookups as f64,
        lookups_per_sec: lookups as f64 / secs,
    }
}

/// Simulate the write-path type resolver over `batches` batches of
/// `batch_rows` rows, each row carrying every key in `keys`, at ~100% hit rate.
///
/// Three cache shapes, all realistic implementations of the same resolver:
///
/// - `hoisted per-scope`: the `(tenant,dataset,signal,level)` scope is resolved
///   **once per batch** into a `&HashMap<String, Canon>`; each attribute is a
///   single `&str` lookup with no allocation.
/// - `global composite key`: one global map keyed by the full 5-tuple, so every
///   attribute lookup must build an owned key.
/// - `shared RwLock`: `hoisted`, but the map is shared across writer threads
///   behind an `RwLock` — one read guard per batch.
pub fn registry_bench(keys: &[String], batch_rows: usize, batches: usize) -> Vec<RegistryStats> {
    let tenant = "_system";
    let dataset = "_monitoring";
    let signal = "traces";
    let level = "record";

    let scope: HashMap<String, Canon> = keys
        .iter()
        .enumerate()
        .map(|(i, k)| {
            (
                k.clone(),
                match i % 3 {
                    0 => Canon::Str,
                    1 => Canon::Int,
                    _ => Canon::Double,
                },
            )
        })
        .collect();
    let mut per_scope: HashMap<(String, String, String, String), HashMap<String, Canon>> =
        HashMap::new();
    per_scope.insert(
        (
            tenant.to_string(),
            dataset.to_string(),
            signal.to_string(),
            level.to_string(),
        ),
        scope.clone(),
    );
    let composite: HashMap<(String, String, String, String, String), Canon> = keys
        .iter()
        .map(|k| {
            (
                (
                    tenant.to_string(),
                    dataset.to_string(),
                    signal.to_string(),
                    level.to_string(),
                    k.clone(),
                ),
                Canon::Str,
            )
        })
        .collect();
    let shared = RwLock::new(scope);

    let lookups = batches * batch_rows * keys.len();
    let mut out = Vec::new();

    // --- hoisted per-scope map ---------------------------------------------
    let start = Instant::now();
    let mut hits = 0usize;
    for _ in 0..batches {
        let scope_key = (
            tenant.to_string(),
            dataset.to_string(),
            signal.to_string(),
            level.to_string(),
        );
        let map = per_scope.get(&scope_key).expect("scope present");
        for _ in 0..batch_rows {
            for k in keys {
                if map.get(k.as_str()).is_some() {
                    hits += 1;
                }
            }
        }
    }
    std::hint::black_box(hits);
    out.push(stat("hoisted per-scope", batch_rows, lookups, start));

    // --- global composite key ----------------------------------------------
    let start = Instant::now();
    let mut hits = 0usize;
    for _ in 0..batches {
        for _ in 0..batch_rows {
            for k in keys {
                let key = (
                    tenant.to_string(),
                    dataset.to_string(),
                    signal.to_string(),
                    level.to_string(),
                    k.clone(),
                );
                if composite.get(&key).is_some() {
                    hits += 1;
                }
            }
        }
    }
    std::hint::black_box(hits);
    out.push(stat("global composite key", batch_rows, lookups, start));

    // --- shared RwLock, one read guard per batch ---------------------------
    let start = Instant::now();
    let mut hits = 0usize;
    for _ in 0..batches {
        let guard = shared.read().expect("rwlock");
        for _ in 0..batch_rows {
            for k in keys {
                if guard.get(k.as_str()).is_some() {
                    hits += 1;
                }
            }
        }
    }
    std::hint::black_box(hits);
    out.push(stat(
        "shared RwLock (1 guard/batch)",
        batch_rows,
        lookups,
        start,
    ));

    out
}

/// Cold path: every lookup misses and must insert under a write lock (what a
/// registry upsert costs the first time a key is seen).
pub fn registry_cold_miss(keys_to_insert: usize) -> RegistryStats {
    let shared: RwLock<HashMap<String, Canon>> = RwLock::new(HashMap::new());
    let start = Instant::now();
    for i in 0..keys_to_insert {
        let key = format!("bench.cold.key.{i}");
        let known = shared.read().expect("rwlock").get(key.as_str()).copied();
        if known.is_none() {
            shared.write().expect("rwlock").insert(key, Canon::Str);
        }
    }
    stat("cold miss (read+insert)", 1, keys_to_insert, start)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn median_of_empty_is_nan_not_panic() {
        assert!(median(Vec::new()).is_nan());
        assert_eq!(median(vec![3.0, 1.0, 2.0]), 2.0);
    }

    #[test]
    fn classify_matches_registry_rules() {
        assert_eq!(classify("42"), Canon::Int);
        assert_eq!(classify("4.2"), Canon::Double);
        assert_eq!(classify("true"), Canon::Bool);
        assert_eq!(classify("{\"a\":1}"), Canon::Residue);
        assert_eq!(classify("[1,2]"), Canon::Residue);
        assert_eq!(classify("hello"), Canon::Str);
    }

    #[test]
    fn residue_roundtrips() -> Result<()> {
        let entries = vec![("k", "json", "[1,2]"), ("other", "str", "42")];
        let bytes = encode_residue(&entries)?;
        let back = decode_residue(&bytes)?;
        assert_eq!(back.len(), 2);
        assert_eq!(back[0].0, "k");
        assert_eq!(back[1].2, "42");
        Ok(())
    }

    #[test]
    fn planted_files_differ_per_regime() {
        assert_eq!(planted_files(SMALL_FLUSH), vec![400, 1200]);
        assert_eq!(planted_files(COMPACTED), vec![0, 2]);
    }
}
