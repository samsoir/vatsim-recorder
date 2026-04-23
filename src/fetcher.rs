use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use reqwest::Client;
use serde::Deserialize;
use std::time::{Duration, Instant};
use tracing::{info, warn};

use crate::vatsim::DataFeed;

pub const DEFAULT_STATUS_URL: &str = "https://status.vatsim.net/status.json";
pub const DEFAULT_DATA_URL: &str = "https://data.vatsim.net/v3/vatsim-data.json";

pub struct Fetch {
    pub raw: Vec<u8>,
    pub feed: DataFeed,
    pub duration_ms: i64,
    pub fetch_ts: DateTime<Utc>,
}

pub struct Fetcher {
    client: Client,
    data_urls: Vec<String>,
    next_idx: usize,
}

#[derive(Debug, Deserialize)]
struct StatusDoc {
    data: StatusData,
}

#[derive(Debug, Deserialize)]
struct StatusData {
    v3: Vec<String>,
}

impl Fetcher {
    /// Build a fetcher. Resolves the list of data endpoints by calling the
    /// status endpoint; on failure falls back to `fallback_data_url`.
    pub async fn new(status_url: &str, fallback_data_url: &str) -> Result<Self> {
        let client = Client::builder()
            .user_agent(concat!("vatsim-recorder/", env!("CARGO_PKG_VERSION")))
            .connect_timeout(Duration::from_secs(10))
            .timeout(Duration::from_secs(30))
            .build()?;

        let data_urls = match fetch_status(&client, status_url).await {
            Ok(urls) if !urls.is_empty() => {
                info!(count = urls.len(), "resolved data URLs from status endpoint");
                urls
            }
            Ok(_) => {
                warn!("status endpoint returned empty v3 list; using fallback");
                vec![fallback_data_url.to_string()]
            }
            Err(e) => {
                warn!(error = %e, "status endpoint failed; using fallback data URL");
                vec![fallback_data_url.to_string()]
            }
        };

        Ok(Self { client, data_urls, next_idx: 0 })
    }

    /// Fetch a single snapshot. Tries each known data URL in round-robin
    /// order until one succeeds or all fail for this tick.
    pub async fn fetch_once(&mut self) -> Result<Fetch> {
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..self.data_urls.len() {
            let url = self.data_urls[self.next_idx].clone();
            self.next_idx = (self.next_idx + 1) % self.data_urls.len();
            let start = Instant::now();
            let fetch_ts = Utc::now();
            match fetch_data(&self.client, &url).await {
                Ok((raw, feed)) => {
                    return Ok(Fetch {
                        raw,
                        feed,
                        duration_ms: start.elapsed().as_millis() as i64,
                        fetch_ts,
                    });
                }
                Err(e) => {
                    warn!(url = %url, error = %e, "fetch failed; trying next URL");
                    last_err = Some(e);
                }
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow::anyhow!("no data URLs configured")))
    }
}

async fn fetch_status(client: &Client, url: &str) -> Result<Vec<String>> {
    let doc: StatusDoc = client.get(url).send().await?.error_for_status()?.json().await?;
    Ok(doc.data.v3)
}

async fn fetch_data(client: &Client, url: &str) -> Result<(Vec<u8>, DataFeed)> {
    let resp = client.get(url).send().await.context("http get")?.error_for_status()?;
    let raw = resp.bytes().await.context("read body")?.to_vec();
    let feed: DataFeed = serde_json::from_slice(&raw).context("parse json")?;
    Ok((raw, feed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use httpmock::prelude::*;

    const FIXTURE: &str = include_str!("../tests/fixtures/vatsim-data.json");

    #[tokio::test(flavor = "current_thread")]
    async fn uses_status_endpoint_when_available() {
        let server = MockServer::start_async().await;
        let status_mock = server.mock_async(|when, then| {
            when.method(GET).path("/status.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(r#"{"data":{"v3":["URL_PLACEHOLDER"]}}"#.replace(
                    "URL_PLACEHOLDER",
                    &format!("{}/data.json", server.base_url()),
                ));
        }).await;
        let data_mock = server.mock_async(|when, then| {
            when.method(GET).path("/data.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(FIXTURE);
        }).await;

        let mut fetcher = Fetcher::new(
            &format!("{}/status.json", server.base_url()),
            "http://unused.example/",
        ).await.unwrap();
        let fetched = fetcher.fetch_once().await.unwrap();

        status_mock.assert_async().await;
        data_mock.assert_async().await;
        assert_eq!(fetched.feed.pilots.len(), 2);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn falls_back_when_status_fails() {
        let server = MockServer::start_async().await;
        let data_mock = server.mock_async(|when, then| {
            when.method(GET).path("/data.json");
            then.status(200)
                .header("content-type", "application/json")
                .body(FIXTURE);
        }).await;

        let mut fetcher = Fetcher::new(
            "http://127.0.0.1:1/status.json", // unreachable
            &format!("{}/data.json", server.base_url()),
        ).await.unwrap();
        let fetched = fetcher.fetch_once().await.unwrap();
        data_mock.assert_async().await;
        assert_eq!(fetched.feed.controllers.len(), 1);
    }
}
