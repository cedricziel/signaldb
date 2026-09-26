//! Column names of the typed attribute layout: each attribute container
//! (`span_attributes`, `resource_attributes`, ...) is stored as one typed map
//! per [`CanonicalType`] plus a binary residue for values that have no typed
//! home (off-type, array, kvlist, bytes).

use crate::schema::type_authority::CanonicalType;

/// The typed-home column of `container` that stores values of `canonical`,
/// e.g. `span_attributes_int`.
pub fn home_column(container: &str, canonical: CanonicalType) -> String {
    let suffix = match canonical {
        CanonicalType::String => "str",
        CanonicalType::Int64 => "int",
        CanonicalType::Float64 => "double",
        CanonicalType::Bool => "bool",
    };
    format!("{container}_{suffix}")
}

/// The binary-residue column of `container`.
pub fn residue_column(container: &str) -> String {
    format!("{container}_residue")
}

/// The `schemas.toml` field type that declares a container in the typed
/// layout; the parser expands it into [`typed_fields`].
pub const TYPED_ATTRIBUTES_TYPE: &str = "typed_attributes";

/// All five columns of `container` with their `schemas.toml` field types: the
/// four typed homes, then the residue.
pub fn typed_fields(container: &str) -> [(String, &'static str); 5] {
    [
        (
            home_column(container, CanonicalType::String),
            "map<string,string>",
        ),
        (
            home_column(container, CanonicalType::Int64),
            "map<string,long>",
        ),
        (
            home_column(container, CanonicalType::Float64),
            "map<string,double>",
        ),
        (
            home_column(container, CanonicalType::Bool),
            "map<string,boolean>",
        ),
        (residue_column(container), "binary"),
    ]
}

/// All five column names of `container`, in [`typed_fields`] order.
pub fn typed_columns(container: &str) -> [String; 5] {
    typed_fields(container).map(|(name, _)| name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_columns_name_the_four_homes_then_the_residue() {
        assert_eq!(
            typed_columns("span_attributes"),
            [
                "span_attributes_str",
                "span_attributes_int",
                "span_attributes_double",
                "span_attributes_bool",
                "span_attributes_residue",
            ]
        );
    }
}
