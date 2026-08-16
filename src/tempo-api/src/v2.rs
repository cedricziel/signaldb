use serde::{Deserialize, Serialize};

/// GET /api/v2/search/tag/.service.name/values
///
/// Todo: Add types to values
///
/// See <https://grafana.com/docs/tempo/latest/api_docs/#search-tag-values-v2>
// `#[schema(as = ...)]` disambiguates the OpenAPI component name from the
// v1 types of the same bare name (`tempo_api::TagSearchResponse` etc.) —
// utoipa registers schemas by type name alone, so without it the two
// `TagSearchResponse`s (v1 flat list, v2 scoped groups) would collide.
#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
#[schema(as = tempo_api::v2::TagValuesResponse)]
pub struct TagValuesResponse {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<TagWithValue>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
#[schema(as = tempo_api::v2::TagWithValue)]
pub struct TagWithValue {
    pub tag: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
#[schema(as = tempo_api::v2::TagSearchResponse)]
pub struct TagSearchResponse {
    pub scopes: Vec<TagSearchScope>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug, utoipa::ToSchema)]
#[schema(as = tempo_api::v2::TagSearchScope)]
pub struct TagSearchScope {
    pub scope: String,
    pub tags: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn tag_values_response_serializes_to_tag_values_wire_field() {
        let response = TagValuesResponse {
            tag_values: vec![TagWithValue {
                tag: "service.name".to_string(),
                value: "checkout".to_string(),
            }],
        };

        assert_eq!(
            serde_json::to_value(&response).unwrap(),
            json!({
                "tagValues": [
                    {"tag": "service.name", "value": "checkout"}
                ]
            })
        );
    }
}
