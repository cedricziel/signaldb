pub mod auth;
pub mod catalog;
pub mod catalog_manager;
pub mod cli;
pub mod config;
pub mod dataset;
pub mod flight;
pub mod iceberg;
pub mod model;
pub mod profile;
pub mod ratelimit;
pub mod schema;
pub mod self_monitoring;
pub mod service_bootstrap;
pub mod storage;
pub mod storage_usage;
pub mod tenant_api;
pub mod wal;

#[cfg(any(test, feature = "testing"))]
pub mod testing;

pub use catalog_manager::CatalogManager;

#[cfg(test)]
mod otlp_profiles_proto_tests {
    use opentelemetry_proto::tonic::collector::profiles::v1development::{
        ExportProfilesServiceRequest, ExportProfilesServiceResponse,
    };
    use opentelemetry_proto::tonic::profiles::v1development::{
        Profile, ProfilesData, ProfilesDictionary,
    };

    #[test]
    fn profiles_proto_types_are_available() {
        let request = ExportProfilesServiceRequest::default();
        assert!(request.resource_profiles.is_empty());

        let response = ExportProfilesServiceResponse::default();
        assert!(response.partial_success.is_none());

        let data = ProfilesData::default();
        assert!(data.dictionary.is_none());

        let dictionary = ProfilesDictionary::default();
        assert!(dictionary.string_table.is_empty());

        let profile = Profile::default();
        assert!(profile.profile_id.is_empty());
        assert_eq!(profile.time_unix_nano, 0);
    }
}
