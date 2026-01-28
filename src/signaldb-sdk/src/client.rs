use crate::SdkError;

/// HTTP client for the SignalDB Admin API
pub struct SignalDbClient {
    base_url: String,
    admin_key: String,
    http: reqwest::Client,
}

impl SignalDbClient {
    /// Create a new client pointing at the given base URL with the admin API key
    pub fn new(base_url: &str, admin_key: &str) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            admin_key: admin_key.to_string(),
            http: reqwest::Client::new(),
        }
    }

    /// Send a GET request and deserialize the response
    pub(crate) async fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
    ) -> Result<T, SdkError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .http
            .get(&url)
            .bearer_auth(&self.admin_key)
            .send()
            .await?;
        handle_response(resp).await
    }

    /// Send a POST request with a JSON body and deserialize the response
    pub(crate) async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, SdkError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .http
            .post(&url)
            .bearer_auth(&self.admin_key)
            .json(body)
            .send()
            .await?;
        handle_response(resp).await
    }

    /// Send a PUT request with a JSON body and deserialize the response
    pub(crate) async fn put<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T, SdkError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .http
            .put(&url)
            .bearer_auth(&self.admin_key)
            .json(body)
            .send()
            .await?;
        handle_response(resp).await
    }

    /// Send a DELETE request, expecting no response body
    pub(crate) async fn delete(&self, path: &str) -> Result<(), SdkError> {
        let url = format!("{}{path}", self.base_url);
        let resp = self
            .http
            .delete(&url)
            .bearer_auth(&self.admin_key)
            .send()
            .await?;

        if resp.status().is_success() {
            Ok(())
        } else {
            let status = resp.status().as_u16();
            let text = resp.text().await.unwrap_or_default();
            let message = serde_json::from_str::<signaldb_api::ApiError>(&text)
                .map(|e| e.message)
                .unwrap_or(text);
            Err(SdkError::Api { status, message })
        }
    }
}

async fn handle_response<T: serde::de::DeserializeOwned>(
    resp: reqwest::Response,
) -> Result<T, SdkError> {
    if resp.status().is_success() {
        let body = resp.text().await?;
        Ok(serde_json::from_str(&body)?)
    } else {
        let status = resp.status().as_u16();
        let text = resp.text().await.unwrap_or_default();
        let message = serde_json::from_str::<signaldb_api::ApiError>(&text)
            .map(|e| e.message)
            .unwrap_or(text);
        Err(SdkError::Api { status, message })
    }
}
