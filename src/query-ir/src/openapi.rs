//! OpenAPI helpers for the stage grammar (the `openapi` feature).

use utoipa::openapi::{
    RefOr,
    schema::{AdditionalProperties, Schema},
};

/// Closes every inline object variant of a `oneOf` to unknown keys
/// (`additionalProperties: false`), as the parser does. utoipa honours
/// `deny_unknown_fields` only on named structs, so neither an externally
/// tagged enum's single-key wrappers nor an untagged enum's struct variants
/// get it from the derive.
pub fn close_object_variants(schema: &mut RefOr<Schema>) {
    let RefOr::T(Schema::OneOf(one_of)) = schema else {
        return;
    };
    for variant in &mut one_of.items {
        if let RefOr::T(Schema::Object(object)) = variant {
            object.additional_properties = Some(Box::new(AdditionalProperties::FreeForm(false)));
        }
    }
}
