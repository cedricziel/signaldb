use std::vec;

use tempo_api::tempopb::{
    querier_server::Querier, trace_by_id_response::Status, SearchBlockRequest, SearchRequest,
    SearchResponse, SearchTagValuesRequest, SearchTagValuesResponse, SearchTagValuesV2Response,
    SearchTagsRequest, SearchTagsResponse, SearchTagsV2Response, SearchTagsV2Scope, TagValue,
    TraceByIdRequest, TraceByIdResponse,
};
use tonic::Response;

#[derive(Debug, Default)]
pub struct SignalDBQuerier {}

#[tonic::async_trait]
impl Querier for SignalDBQuerier {
    #[tracing::instrument]
    async fn find_trace_by_id(
        &self,
        request: tonic::Request<TraceByIdRequest>,
    ) -> Result<tonic::Response<TraceByIdResponse>, tonic::Status> {
        let response = TraceByIdResponse {
            trace: None,
            metrics: None,
            status: Status::Complete as i32,
            message: "Hello, World!".to_string(),
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn search_recent(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let response = SearchResponse {
            traces: vec![],
            metrics: None,
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn search_block(
        &self,
        request: tonic::Request<SearchBlockRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        // Implement the method logic here
        unimplemented!()
    }

    #[tracing::instrument]
    async fn search_tags(
        &self,
        request: tonic::Request<SearchTagsRequest>,
    ) -> Result<tonic::Response<SearchTagsResponse>, tonic::Status> {
        let response = SearchTagsResponse {
            metrics: None,
            tag_names: vec!["my_tag".to_string()],
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn search_tags_v2(
        &self,
        request: tonic::Request<SearchTagsRequest>,
    ) -> Result<tonic::Response<SearchTagsV2Response>, tonic::Status> {
        let response = SearchTagsV2Response {
            metrics: None,
            scopes: vec![SearchTagsV2Scope {
                name: "resource".to_string(),
                tags: vec!["my_tag".to_string()],
            }],
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn search_tag_values(
        &self,
        request: tonic::Request<SearchTagValuesRequest>,
    ) -> Result<tonic::Response<SearchTagValuesResponse>, tonic::Status> {
        let response = SearchTagValuesResponse {
            tag_values: vec!["my_tag".to_string()],
            metrics: None,
        };

        Ok(Response::new(response))
    }

    #[tracing::instrument]
    async fn search_tag_values_v2(
        &self,
        request: tonic::Request<SearchTagValuesRequest>,
    ) -> Result<tonic::Response<SearchTagValuesV2Response>, tonic::Status> {
        let response = SearchTagValuesV2Response {
            tag_values: vec![TagValue {
                r#type: "my_tag".to_string(),
                value: "Hello, World!".to_string(),
            }],
            metrics: None,
        };
        Ok(Response::new(response))
    }
}
