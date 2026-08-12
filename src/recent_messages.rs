use crate::config::{Config, DEFAULT_RECENT_MESSAGES_LIMIT};
use anyhow::{anyhow, Context, Result};
use reqwest::Url;
use serde::Deserialize;
use std::time::Duration;
use tracing::warn;

pub(crate) const RECENT_MESSAGES_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone)]
pub struct RecentMessagesClient {
    enabled: bool,
    base_url: Option<Url>,
    limit: usize,
    client: reqwest::Client,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecentMessagesRuntimeSummary {
    pub enabled: bool,
    pub base_url: Option<String>,
    pub limit: usize,
    pub warnings: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct RecentMessagesResponse {
    pub messages: Vec<String>,
    pub error: Option<String>,
    pub error_code: Option<String>,
}

impl RecentMessagesClient {
    pub fn from_config(config: &Config) -> Self {
        Self::from_summary(Self::summary_from_config(config))
    }

    pub fn from_summary(summary: RecentMessagesRuntimeSummary) -> Self {
        crate::install_rustls_crypto_provider();

        for warning in &summary.warnings {
            warn!("{warning}");
        }

        let base_url = summary
            .base_url
            .as_deref()
            .and_then(|url| Url::parse(url).ok());
        Self {
            enabled: summary.enabled && base_url.is_some(),
            base_url,
            limit: summary.limit,
            client: reqwest::Client::builder()
                .timeout(RECENT_MESSAGES_TIMEOUT)
                .build()
                .expect("failed to build recent-messages HTTP client"),
        }
    }

    pub fn summary_from_config(config: &Config) -> RecentMessagesRuntimeSummary {
        let mut warnings = Vec::new();
        let base_url = match normalize_base_url(&config.recent_messages_url) {
            Ok(url) => Some(url),
            Err(error) => {
                warnings.push(format!(
                    "recentMessagesUrl is invalid; recent-message backfill is disabled: {error}"
                ));
                None
            }
        };
        let limit = (config.recent_messages_limit > 0)
            .then_some(config.recent_messages_limit)
            .unwrap_or(DEFAULT_RECENT_MESSAGES_LIMIT);

        if config.recent_messages_enabled && base_url.is_none() {
            warnings.push(
                "recentMessagesEnabled is set but recentMessagesUrl is unusable; recent-message backfill is disabled".to_string(),
            );
        }

        RecentMessagesRuntimeSummary {
            enabled: config.recent_messages_enabled,
            base_url,
            limit,
            warnings,
        }
    }

    pub fn enabled(&self) -> bool {
        self.enabled
    }

    pub async fn fetch(&self, channel_login: &str) -> Result<RecentMessagesResponse> {
        let mut url = self
            .base_url
            .clone()
            .context("recent-message backfill is disabled")?;
        url.path_segments_mut()
            .map_err(|_| anyhow!("recent-messages URL cannot contain path segments"))?
            .push(&channel_login.to_ascii_lowercase());
        url.query_pairs_mut()
            .append_pair("limit", &self.limit.to_string());

        self.client
            .get(url)
            .send()
            .await
            .context("recent-messages request failed")?
            .error_for_status()
            .context("recent-messages request returned an error status")?
            .json::<RecentMessagesResponse>()
            .await
            .context("recent-messages response was not valid JSON")
    }
}

fn normalize_base_url(value: &str) -> Result<String, String> {
    let trimmed = value.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return Err("empty URL".to_string());
    }

    let parsed = Url::parse(trimmed).map_err(|error| error.to_string())?;
    match parsed.scheme() {
        "http" | "https" => Ok(parsed.to_string().trim_end_matches('/').to_string()),
        scheme => Err(format!("unsupported scheme {scheme}")),
    }
}

#[cfg(test)]
mod tests {
    use super::{RecentMessagesClient, RecentMessagesRuntimeSummary};
    use crate::config::{Config, DEFAULT_RECENT_MESSAGES_LIMIT, DEFAULT_RECENT_MESSAGES_URL};
    use axum::{
        extract::{Path, Query},
        http::StatusCode,
        response::IntoResponse,
        routing::get,
        Json, Router,
    };
    use std::collections::HashMap;
    use tokio::net::TcpListener;

    fn config(enabled: bool, url: &str, limit: usize) -> Config {
        serde_json::from_value(serde_json::json!({
            "clickhouseUrl": "http://clickhouse:8123",
            "clickhouseDb": "rustlog",
            "clickhouseUsername": "rustlog",
            "clickhousePassword": "password",
            "channels": [],
            "clientID": "client-id",
            "clientSecret": "client-secret",
            "admins": [],
            "optOut": {},
            "adminAPIKey": "admin-key",
            "recentMessagesEnabled": enabled,
            "recentMessagesUrl": url,
            "recentMessagesLimit": limit
        }))
        .unwrap()
    }

    #[test]
    fn defaults_to_disabled_with_robotty_url() {
        let config: Config = serde_json::from_value(serde_json::json!({
            "clickhouseUrl": "http://clickhouse:8123",
            "clickhouseDb": "rustlog",
            "channels": [],
            "clientID": "client-id",
            "clientSecret": "client-secret",
            "admins": [],
            "optOut": {}
        }))
        .unwrap();
        let summary = RecentMessagesClient::summary_from_config(&config);

        assert!(!summary.enabled);
        assert_eq!(
            summary.base_url.as_deref(),
            Some(DEFAULT_RECENT_MESSAGES_URL)
        );
        assert_eq!(summary.limit, DEFAULT_RECENT_MESSAGES_LIMIT);
        assert!(summary.warnings.is_empty());
    }

    #[test]
    fn normalizes_configured_url_and_limit() {
        let config = config(true, " https://example.test/api/v2/recent-messages/ ", 25);
        let summary = RecentMessagesClient::summary_from_config(&config);

        assert!(summary.enabled);
        assert_eq!(
            summary.base_url.as_deref(),
            Some("https://example.test/api/v2/recent-messages")
        );
        assert_eq!(summary.limit, 25);
    }

    #[test]
    fn rejects_non_http_url_and_defaults_invalid_limit() {
        let config = config(true, "file:///tmp/messages", 0);
        let summary = RecentMessagesClient::summary_from_config(&config);

        assert!(summary.enabled);
        assert!(summary.base_url.is_none());
        assert_eq!(summary.limit, DEFAULT_RECENT_MESSAGES_LIMIT);
        assert!(!summary.warnings.is_empty());
        assert!(!RecentMessagesClient::from_summary(summary).enabled());
    }

    #[tokio::test]
    async fn fetch_uses_lowercase_channel_and_configured_limit() {
        async fn handler(
            Path(channel): Path<String>,
            Query(query): Query<HashMap<String, String>>,
        ) -> impl IntoResponse {
            assert_eq!(channel, "channelone");
            assert_eq!(query.get("limit").map(String::as_str), Some("25"));
            Json(serde_json::json!({
                "messages": ["raw message"],
                "error": null,
                "error_code": null
            }))
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new().route("/api/v2/recent-messages/{channel}", get(handler)),
            )
            .await
        });
        let client = RecentMessagesClient::from_summary(RecentMessagesRuntimeSummary {
            enabled: true,
            base_url: Some(format!("http://{address}/api/v2/recent-messages")),
            limit: 25,
            warnings: Vec::new(),
        });

        let response = client.fetch("ChannelOne").await.unwrap();
        assert_eq!(response.messages, ["raw message"]);
        server.abort();
    }

    #[tokio::test]
    async fn fetch_rejects_error_status_and_malformed_json() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(
                listener,
                Router::new()
                    .route(
                        "/api/v2/recent-messages/status",
                        get(|| async { StatusCode::BAD_GATEWAY }),
                    )
                    .route("/api/v2/recent-messages/json", get(|| async { "not json" })),
            )
            .await
        });
        let client = RecentMessagesClient::from_summary(RecentMessagesRuntimeSummary {
            enabled: true,
            base_url: Some(format!("http://{address}/api/v2/recent-messages")),
            limit: 800,
            warnings: Vec::new(),
        });

        assert!(client.fetch("status").await.is_err());
        assert!(client.fetch("json").await.is_err());
        server.abort();
    }
}
