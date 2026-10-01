//! # Result-column aliases for logical field names
//!
//! A logical field name is dotted and OTel-native (`service.name`,
//! `http.request.method`), which an engine reads as a table qualifier. Every
//! stage that materializes a logical field as its own result column — an
//! `aggregate` group key, a curated projection — aliases it through
//! [`safe_ident`], so the querier that builds the column and the router that
//! reads it back agree on one spelling without either guessing.

/// The result-column name a logical field takes: every character that is not
/// ASCII alphanumeric or `_` becomes `_`. Case is preserved, so the column
/// keeps the spelling the document used.
pub fn safe_ident(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::safe_ident;

    #[test]
    fn dots_become_underscores_and_case_is_preserved() {
        assert_eq!(safe_ident("service.name"), "service_name");
        assert_eq!(safe_ident("statusCode"), "statusCode");
        assert_eq!(safe_ident("k8s.pod/name"), "k8s_pod_name");
    }
}
