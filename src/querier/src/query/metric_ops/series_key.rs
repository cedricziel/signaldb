//! The series identity the metric operators group by: the stored `series_id`,
//! or, where a row lacks one, a fingerprint of the columns it is derived from.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, StringBuilder};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::util::display::{ArrayFormatter, FormatOptions};
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::functions::core::expr_fn::coalesce;
use datafusion::logical_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility, col,
    lit,
};
use datafusion::prelude::ident;

/// Columns a `series_id` is derived from (metric, resource, scope, point
/// attributes), matched by name or as a typed-attribute container prefix.
const IDENTITY: [&str; 9] = [
    "metric_name",
    "metric_type",
    "service_name",
    "resource_identity",
    "resource_attributes",
    "scope_name",
    "scope_version",
    "scope_attributes",
    "attributes",
];

fn is_identity(name: &str) -> bool {
    IDENTITY
        .iter()
        .any(|id| name == *id || name.strip_prefix(id).is_some_and(|r| r.starts_with('_')))
        || name.starts_with("label_")
}

/// `series_id`, falling back to a fingerprint of the identity columns in
/// `schema` when it is null or the table has no such column.
pub(super) fn series_key(schema: &DFSchema) -> Expr {
    let names: Vec<&String> = schema.fields().iter().map(|f| f.name()).collect();
    let identity: Vec<Expr> = names
        .iter()
        .filter(|n| is_identity(n))
        .map(|n| ident(n.as_str()))
        .collect();
    let derived = if identity.is_empty() {
        lit("~")
    } else {
        ScalarUDF::new_from_impl(Fingerprint {
            signature: Signature::variadic_any(Volatility::Immutable),
        })
        .call(identity)
    };
    if names.iter().any(|n| *n == "series_id") {
        coalesce(vec![col("series_id"), derived])
    } else {
        derived
    }
}

#[derive(Debug, PartialEq, Eq, Hash)]
struct Fingerprint {
    signature: Signature,
}

impl ScalarUDFImpl for Fingerprint {
    fn name(&self) -> &str {
        "series_fingerprint"
    }
    fn signature(&self) -> &Signature {
        &self.signature
    }
    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let arrays = args
            .args
            .iter()
            .map(|a| a.to_array(args.number_rows))
            .collect::<Result<Vec<ArrayRef>>>()?;
        let options = FormatOptions::default().with_null("\u{0}");
        let formatters = arrays
            .iter()
            .map(|a| ArrayFormatter::try_new(a.as_ref(), &options))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let mut out = StringBuilder::new();
        for row in 0..args.number_rows {
            let mut h = DefaultHasher::new();
            for f in &formatters {
                f.value(row).to_string().hash(&mut h);
            }
            out.append_value(format!("~{:016x}", h.finish()));
        }
        Ok(ColumnarValue::Array(Arc::new(out.finish())))
    }
}
