use anyhow::Context;
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::fs;
use std::{collections::HashSet, sync::RwLock};
use tracing::info;

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
        fs::write(self.config_path.as_ref().expect("config path should always be available"), json)?;

        Ok(())
    }
}

fn default_listen_address() -> String {
    String::from("0.0.0.0:8025")
}

fn clickhouse_flush_interval() -> u64 {
    10
}
