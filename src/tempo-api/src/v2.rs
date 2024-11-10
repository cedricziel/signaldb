use std::collections::HashMap;

use serde::{Deserialize, Serialize};

/// GET /api/v2/search/tag/.service.name/values
///
/// Todo: Add types to values
///
/// See https://grafana.com/docs/tempo/latest/api_docs/#search-tag-values-v2
#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TagValuesResponse {
    #[serde(rename = "tagValues")]
    pub tag_values: Vec<TagWithValue>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TagWithValue {
    pub tag: String,
    pub value: String,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TagSearchResponse {
    pub scopes: Vec<TagSearchScope>,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub struct TagSearchScope {
    pub scope: String,
    pub tags: Vec<String>,
}

mod tests {
    use super::*;

    #[test]
    fn test_serde_tag_values_response() {
        let tag_values_response = TagValuesResponse {
            tag_values: Vec::new(),
        };

        let json = serde_json::to_string(&tag_values_response).unwrap();
        let deserialized: TagValuesResponse = serde_json::from_str(&json).unwrap();

        assert_eq!(tag_values_response, deserialized);
    }
}
