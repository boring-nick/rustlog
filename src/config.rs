use anyhow::Context;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{collections::HashSet, sync::RwLock};
use tracing::info;

pub(crate) const DEFAULT_RECENT_MESSAGES_URL: &str =
    "https://recent-messages.robotty.de/api/v2/recent-messages";
pub(crate) const DEFAULT_RECENT_MESSAGES_LIMIT: usize = 800;

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    pub clickhouse_url: String,
    pub clickhouse_db: String,
    pub clickhouse_username: Option<String>,
    pub clickhouse_password: Option<String>,
    #[serde(default = "clickhouse_flush_interval")]
    pub clickhouse_flush_interval: u64,
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    #[serde(default)]
    pub recent_messages_enabled: bool,
    #[serde(default = "default_recent_messages_url")]
    pub recent_messages_url: String,
    #[serde(default = "default_recent_messages_limit")]
    pub recent_messages_limit: usize,
    pub channels: RwLock<HashSet<String>>,
    #[serde(rename = "clientID")]
    pub client_id: String,
    pub client_secret: String,
    pub admins: Vec<String>,
    #[serde(default)]
    pub opt_out: DashMap<String, bool>,
    #[serde(rename = "adminAPIKey")]
    pub admin_api_key: Option<String>,
    #[serde(skip)]
    config_path: Option<std::path::PathBuf>,
}

impl Config {
    pub fn load(config_path: &std::path::Path) -> anyhow::Result<Self> {
        let contents = fs::read_to_string(config_path)
            .with_context(|| format!("Failed to load config from {}", config_path.display()))?;
        let mut s: Self = serde_json::from_str(&contents).context("Config deserializtion error")?;
        s.config_path = Some(config_path.to_owned());
        Ok(s)
    }

    pub fn save(&self) -> anyhow::Result<()> {
        info!("Updating config");
        let json = serde_json::to_string_pretty(self)?;
        fs::write(
            self.config_path
                .as_ref()
                .expect("config path should always be available"),
            json,
        )?;

        Ok(())
    }
}

fn default_listen_address() -> String {
    String::from("0.0.0.0:8025")
}

fn clickhouse_flush_interval() -> u64 {
    10
}

fn default_recent_messages_url() -> String {
    DEFAULT_RECENT_MESSAGES_URL.to_string()
}

fn default_recent_messages_limit() -> usize {
    DEFAULT_RECENT_MESSAGES_LIMIT
}

#[cfg(test)]
mod tests {
    use super::{Config, DEFAULT_RECENT_MESSAGES_LIMIT, DEFAULT_RECENT_MESSAGES_URL};
    use serde_json::json;

    fn base_config() -> serde_json::Value {
        json!({
            "clickhouseUrl": "http://clickhouse:8123",
            "clickhouseDb": "rustlog",
            "clickhouseUsername": "rustlog",
            "clickhousePassword": "password",
            "channels": [],
            "clientID": "client-id",
            "clientSecret": "client-secret",
            "admins": [],
            "optOut": {},
            "adminAPIKey": "admin-key"
        })
    }

    #[test]
    fn recent_messages_defaults_match_documented_values() {
        let config: Config = serde_json::from_value(base_config()).unwrap();

        assert!(!config.recent_messages_enabled);
        assert_eq!(config.recent_messages_url, DEFAULT_RECENT_MESSAGES_URL);
        assert_eq!(config.recent_messages_limit, DEFAULT_RECENT_MESSAGES_LIMIT);
    }

    #[test]
    fn recent_messages_fields_round_trip() {
        let mut value = base_config();
        value["recentMessagesEnabled"] = json!(true);
        value["recentMessagesUrl"] = json!("https://example.test/recent");
        value["recentMessagesLimit"] = json!(25);

        let config: Config = serde_json::from_value(value).unwrap();
        let serialized = serde_json::to_value(&config).unwrap();

        assert_eq!(serialized["recentMessagesEnabled"], json!(true));
        assert_eq!(
            serialized["recentMessagesUrl"],
            json!("https://example.test/recent")
        );
        assert_eq!(serialized["recentMessagesLimit"], json!(25));
    }
}
