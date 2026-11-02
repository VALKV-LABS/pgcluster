use anyhow::Result;
use std::time::Duration;

/// Simple HTTP client for talking to the pgcluster REST API.
///
/// Sends the key in `PGCLUSTER_API_KEY`, when set, as a bearer token.
pub struct ApiClient {
    base_url: String,
    client: reqwest::Client,
    api_key: Option<String>,
}

impl ApiClient {
    pub fn new(api_addr: &str) -> Self {
        Self::with_key(api_addr, std::env::var("PGCLUSTER_API_KEY").ok())
    }

    pub fn with_key(api_addr: &str, api_key: Option<String>) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(5))
            .build()
            .expect("build HTTP client");
        Self {
            base_url: format!("http://{}", api_addr),
            client,
            api_key: api_key.filter(|k| !k.is_empty()),
        }
    }

    fn authorize(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.api_key {
            Some(key) => request.bearer_auth(key),
            None => request,
        }
    }

    pub async fn get<T: serde::de::DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.get(&url));
        let resp = request.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn post<B: serde::Serialize, T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.post(&url)).json(body);
        let resp = request.send().await?.error_for_status()?;
        Ok(resp.json().await?)
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = format!("{}{}", self.base_url, path);
        let request = self.authorize(self.client.delete(&url));
        request.send().await?.error_for_status()?;
        Ok(())
    }
}
