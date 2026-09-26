use crate::schema::logical::{AttributeLevel, LogicalType};

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CanonicalType {
    String,
    Int64,
    Float64,
    Bool,
}

impl From<CanonicalType> for LogicalType {
    fn from(value: CanonicalType) -> Self {
        match value {
            CanonicalType::String => LogicalType::String,
            CanonicalType::Int64 => LogicalType::Int64,
            CanonicalType::Float64 => LogicalType::Float64,
            CanonicalType::Bool => LogicalType::Bool,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservedKind {
    String,
    Int64,
    Float64,
    Bool,
    Bytes,
    Array,
    KvList,
    Empty,
}

impl ObservedKind {
    pub fn canonical(self) -> Option<CanonicalType> {
        match self {
            ObservedKind::String => Some(CanonicalType::String),
            ObservedKind::Int64 => Some(CanonicalType::Int64),
            ObservedKind::Float64 => Some(CanonicalType::Float64),
            ObservedKind::Bool => Some(CanonicalType::Bool),
            ObservedKind::Bytes
            | ObservedKind::Array
            | ObservedKind::KvList
            | ObservedKind::Empty => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TypeSource {
    Config,
    Semconv,
    Observed,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct SchemaUrls<'a> {
    pub resource: Option<&'a str>,
    pub scope: Option<&'a str>,
}

impl<'a> SchemaUrls<'a> {
    fn non_empty(url: Option<&'a str>) -> Option<&'a str> {
        url.filter(|url| !url.is_empty())
    }

    /// The schema_url that applies as a semconv hint source for `level`.
    /// Resource-level attributes only ever see the resource url (OTLP has
    /// no scope-scoped hint for them); scope- and record-level attributes
    /// prefer the scope url, falling back to the resource url.
    pub fn applicable_for(&self, level: AttributeLevel) -> Option<&'a str> {
        match level {
            AttributeLevel::Resource => Self::non_empty(self.resource),
            AttributeLevel::Scope | AttributeLevel::Record => {
                Self::non_empty(self.scope).or_else(|| Self::non_empty(self.resource))
            }
        }
    }
}

pub trait TypeHints {
    fn hint(&self, schema_url: &str, key: &str) -> Option<CanonicalType>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Resolution<'a> {
    pub canonical: CanonicalType,
    pub source: TypeSource,
    pub hint_schema_url: Option<&'a str>,
}

pub fn resolve<'a>(
    config: Option<CanonicalType>,
    hints: &dyn TypeHints,
    urls: SchemaUrls<'a>,
    level: AttributeLevel,
    key: &str,
    observed: ObservedKind,
) -> Option<Resolution<'a>> {
    if let Some(canonical) = config {
        return Some(Resolution {
            canonical,
            source: TypeSource::Config,
            hint_schema_url: None,
        });
    }

    if let Some(schema_url) = urls.applicable_for(level)
        && let Some(canonical) = hints.hint(schema_url, key)
    {
        return Some(Resolution {
            canonical,
            source: TypeSource::Semconv,
            hint_schema_url: Some(schema_url),
        });
    }

    observed.canonical().map(|canonical| Resolution {
        canonical,
        source: TypeSource::Observed,
        hint_schema_url: None,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Placement {
    Home(CanonicalType),
    Residue { off_type: bool },
}

pub fn place(canonical: Option<CanonicalType>, observed: ObservedKind) -> Placement {
    match (canonical, observed.canonical()) {
        (Some(canonical), Some(observed_canonical)) if canonical == observed_canonical => {
            Placement::Home(canonical)
        }
        (Some(_), Some(_)) => Placement::Residue { off_type: true },
        _ => Placement::Residue { off_type: false },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const RESOURCE_URL: &str = "https://resource.example.com/v1";
    const SCOPE_URL: &str = "https://scope.example.com/v1";

    struct NoHints;
    impl TypeHints for NoHints {
        fn hint(&self, _schema_url: &str, _key: &str) -> Option<CanonicalType> {
            None
        }
    }

    /// Declares `service.name` as `canonical` under exactly one schema_url.
    struct HintAt(&'static str, CanonicalType);
    impl TypeHints for HintAt {
        fn hint(&self, schema_url: &str, key: &str) -> Option<CanonicalType> {
            (schema_url == self.0 && key == "service.name").then_some(self.1)
        }
    }

    const BOTH_URLS: SchemaUrls<'static> = SchemaUrls {
        resource: Some(RESOURCE_URL),
        scope: Some(SCOPE_URL),
    };

    fn resolve_service_name<'a>(
        config: Option<CanonicalType>,
        hints: &dyn TypeHints,
        urls: SchemaUrls<'a>,
        level: AttributeLevel,
        observed: ObservedKind,
    ) -> Option<Resolution<'a>> {
        resolve(config, hints, urls, level, "service.name", observed)
    }

    #[test]
    fn config_override_beats_semconv_hint_and_observed_type() {
        let resolution = resolve_service_name(
            Some(CanonicalType::String),
            &HintAt(RESOURCE_URL, CanonicalType::Int64),
            BOTH_URLS,
            AttributeLevel::Resource,
            ObservedKind::Bool,
        );

        assert_eq!(
            resolution,
            Some(Resolution {
                canonical: CanonicalType::String,
                source: TypeSource::Config,
                hint_schema_url: None,
            })
        );
    }

    #[test]
    fn semconv_hint_beats_observed_and_records_its_schema_url() {
        let resolution = resolve_service_name(
            None,
            &HintAt(RESOURCE_URL, CanonicalType::Int64),
            BOTH_URLS,
            AttributeLevel::Resource,
            ObservedKind::Bool,
        );

        assert_eq!(
            resolution,
            Some(Resolution {
                canonical: CanonicalType::Int64,
                source: TypeSource::Semconv,
                hint_schema_url: Some(RESOURCE_URL),
            })
        );
    }

    #[test]
    fn missing_empty_or_unknown_schema_url_falls_through_to_observed() {
        for urls in [
            SchemaUrls::default(),
            SchemaUrls {
                resource: Some(""),
                scope: Some(""),
            },
            SchemaUrls {
                resource: Some("https://unknown.example.com/v1"),
                scope: None,
            },
        ] {
            let resolution = resolve_service_name(
                None,
                &HintAt(RESOURCE_URL, CanonicalType::Int64),
                urls,
                AttributeLevel::Record,
                ObservedKind::Bool,
            );

            assert_eq!(
                resolution,
                Some(Resolution {
                    canonical: CanonicalType::Bool,
                    source: TypeSource::Observed,
                    hint_schema_url: None,
                }),
                "{urls:?}"
            );
        }
    }

    #[test]
    fn scope_url_beats_resource_url_for_scope_and_record_fields() {
        for level in [AttributeLevel::Scope, AttributeLevel::Record] {
            assert_eq!(BOTH_URLS.applicable_for(level), Some(SCOPE_URL));
        }
        let resource_only = SchemaUrls {
            resource: Some(RESOURCE_URL),
            scope: Some(""),
        };
        assert_eq!(
            resource_only.applicable_for(AttributeLevel::Record),
            Some(RESOURCE_URL)
        );
    }

    #[test]
    fn resource_level_field_ignores_scope_url() {
        assert_eq!(
            BOTH_URLS.applicable_for(AttributeLevel::Resource),
            Some(RESOURCE_URL)
        );
        assert_eq!(
            SchemaUrls {
                resource: None,
                scope: Some(SCOPE_URL),
            }
            .applicable_for(AttributeLevel::Resource),
            None
        );
    }

    #[test]
    fn first_observed_non_scalar_establishes_no_canonical_type() {
        for observed in [
            ObservedKind::Array,
            ObservedKind::KvList,
            ObservedKind::Bytes,
            ObservedKind::Empty,
        ] {
            let resolution = resolve_service_name(
                None,
                &NoHints,
                SchemaUrls::default(),
                AttributeLevel::Record,
                observed,
            );
            assert_eq!(resolution, None, "{observed:?}");
        }
    }

    #[test]
    fn matching_scalar_places_in_the_home() {
        assert_eq!(
            place(Some(CanonicalType::Int64), ObservedKind::Int64),
            Placement::Home(CanonicalType::Int64)
        );
    }

    #[test]
    fn off_type_scalar_goes_to_residue_marked_off_type() {
        assert_eq!(
            place(Some(CanonicalType::Int64), ObservedKind::String),
            Placement::Residue { off_type: true }
        );
    }

    #[test]
    fn int_into_a_float_field_is_off_type_never_coerced() {
        assert_eq!(
            place(Some(CanonicalType::Float64), ObservedKind::Int64),
            Placement::Residue { off_type: true }
        );
    }

    #[test]
    fn non_scalars_are_always_residue_without_off_type() {
        for observed in [
            ObservedKind::Array,
            ObservedKind::KvList,
            ObservedKind::Bytes,
            ObservedKind::Empty,
        ] {
            for canonical in [Some(CanonicalType::String), None] {
                assert_eq!(
                    place(canonical, observed),
                    Placement::Residue { off_type: false }
                );
            }
        }
    }
}
